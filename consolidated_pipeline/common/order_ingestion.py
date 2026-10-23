# Databricks notebook source
"""Exactly-once order ingestion: Auto Loader micro-batches and date-range backfills.

Why file-level idempotency instead of Delta's ``txnVersion``: txnVersion silently skips
a write when a checkpoint is reset and batch ids restart at zero. Here Bronze remembers
which (file path, modification time) it already holds, so a retried batch, a reset
checkpoint or a replayed file can never duplicate or drop rows. Silver and Gold are
MERGEs on business keys, so they are idempotent on their own.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F

try:
    split_valid_and_quarantined  # already defined when this runs via %run after order_quality
except NameError:
    from consolidated_pipeline.common.order_quality import (
        parse_order_date,
        persist_quality_results,
        split_valid_and_quarantined,
    )

RAW_COLUMNS = ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"]
FILE_COLUMNS = ["file_path", "file_name", "file_size", "file_modification_time"]
FILE_KEYS = ["file_path", "file_modification_time"]  # what makes a landed file the same file again
SILVER_COLUMNS = [*RAW_COLUMNS, "read_timestamp", "file_name", "file_size", "product_code"]
SILVER_KEYS = ["order_placement_date", "order_id", "product_code", "customer_id"]
GOLD_KEYS = ["date", "order_id", "product_code", "customer_code"]
PARENT_KEYS = ["date", "product_code", "customer_code"]


def _persist(frame: DataFrame) -> DataFrame:
    """Cache when the compute allows it. Databricks serverless (including Free Edition) rejects persist()."""
    try:
        return frame.persist()
    except Exception:
        return frame


def _unpersist(frame: DataFrame) -> None:
    try:
        frame.unpersist()
    except Exception:
        pass


class SchemaContractError(RuntimeError):
    """The landed files lack columns the order contract requires."""


# --------------------------------------------------------------------------- reading


def with_file_metadata(frame: DataFrame) -> DataFrame:
    return frame.select(
        "*",
        "_metadata.file_path",
        "_metadata.file_name",
        "_metadata.file_size",
        "_metadata.file_modification_time",
    ).withColumn("read_timestamp", F.current_timestamp())


def read_raw_batch(spark: SparkSession, path: str) -> DataFrame:
    """Batch read shaped like an Auto Loader micro-batch: every column a string, plus file metadata."""
    return with_file_metadata(spark.read.options(header=True, inferSchema=False).csv(path))


def build_orders_stream(spark: SparkSession, *, landing_path: str, schema_path: str, reader: str = "cloudFiles"):
    """Streaming source for landed order files.

    ``cloudFiles`` is Databricks Auto Loader. Columns stay strings so a day of ISO-only
    dates cannot be inferred as DATE and break the append (the failure the old
    ``inferSchema`` ingestion had). New source columns are added to the stored schema
    (``addNewColumns``); the stream stops once on the change and the job retry restarts
    it with the wider schema. ``csv`` is the open-source file source used by local tests.
    """
    if reader == "cloudFiles":
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(landing_path)
        )
    elif reader == "csv":
        schema = ", ".join(f"{name} string" for name in RAW_COLUMNS)
        stream = spark.readStream.schema(schema).option("header", "true").csv(landing_path)
    else:
        raise ValueError(f"unknown reader: {reader!r}")
    return with_file_metadata(stream)


def run_orders_stream(spark, process_batch, *, landing_path, checkpoint_path, schema_path, reader="cloudFiles") -> dict:
    """Drain whatever has landed since the last run, then stop. No new files means zero batches."""
    stream = build_orders_stream(spark, landing_path=landing_path, schema_path=schema_path, reader=reader)
    query = (
        stream.writeStream.foreachBatch(lambda batch, batch_id: process_batch(batch.sparkSession, batch, batch_id))
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    progress = query.recentProgress
    return {"batches": sum(1 for p in progress if p["numInputRows"] > 0), "input_rows": sum(p["numInputRows"] for p in progress)}


# --------------------------------------------------------------------------- writing


def batch_run_id(batch: DataFrame) -> str:
    """Same files in -> same id out, so a retried batch replaces its own quarantine and metrics rows."""
    files = sorted(
        (row["file_path"], str(row["file_modification_time"]))
        for row in batch.select("file_path", "file_modification_time").distinct().collect()
    )
    return "batch-" + hashlib.sha256(json.dumps(files).encode()).hexdigest()[:24]


def ingest_bronze(spark: SparkSession, batch: DataFrame, table: str, source_keys: list[str] | None = None) -> int:
    """Append only rows whose source unit Bronze does not already hold. Returns the rows appended.

    ``source_keys`` identify a unit of source data: a landed file by default, a Kafka message
    (topic, partition, offset, timestamp) for streaming.
    """
    keys = source_keys or FILE_KEYS
    fresh = batch
    if spark.catalog.tableExists(table):
        held = spark.table(table).select(*keys).distinct()
        fresh = batch.join(held, on=keys, how="left_anti")
    appended = fresh.count()
    if appended:
        (
            fresh.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(table)
        )
    return appended


def _latest_per_key(frame: DataFrame, keys: list[str]) -> DataFrame:
    """One row per merge key (a later file corrects an earlier one) so MERGE never sees ambiguity."""
    order = [F.col("read_timestamp").desc(), F.col("file_name").desc()] if "read_timestamp" in frame.columns else [F.lit(1)]
    ranked = frame.withColumn("_rank", F.row_number().over(Window.partitionBy(*keys).orderBy(*order)))
    return ranked.filter("_rank = 1").drop("_rank")


def _upsert(
    spark: SparkSession,
    frame: DataFrame,
    table: str,
    keys: list[str],
    compare: list[str],
    *,
    bound_column: str | None = None,
    use_bounds: bool = True,
) -> None:
    """MERGE that rewrites a matched row only when a business value changed.

    A replay therefore produces no updates, so Change Data Feed shows real changes only
    and the original ingestion metadata (read_timestamp, file_name) is preserved.

    With ``bound_column`` the MERGE condition also carries the batch's min/max of that column,
    so Delta can skip every file whose statistics fall outside it instead of scanning the table.
    """
    if not spark.catalog.tableExists(table):
        frame.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(table)
        return
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    if bound_column and use_bounds:
        low, high = frame.agg(F.min(bound_column), F.max(bound_column)).first()
        if low is not None:
            condition += f" AND target.{bound_column} BETWEEN '{low}' AND '{high}'"
    changed = " OR ".join(f"NOT (target.{column} <=> source.{column})" for column in compare)
    (
        DeltaTable.forName(spark, table).alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll(condition=changed)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _replace_range(spark: SparkSession, frame: DataFrame, table: str, predicate: str) -> None:
    writer = frame.write.format("delta").option("delta.enableChangeDataFeed", "true")
    if spark.catalog.tableExists(table):
        writer.mode("overwrite").option("replaceWhere", predicate).saveAsTable(table)
    else:
        writer.mode("overwrite").saveAsTable(table)


def _gold_rows(silver_rows: DataFrame) -> DataFrame:
    return silver_rows.select(
        "order_id",
        F.col("order_placement_date").alias("date"),
        F.col("customer_id").alias("customer_code"),
        "product_code",
        "product_id",
        F.col("order_qty").alias("sold_quantity"),
    )


def refresh_parent_months(
    spark: SparkSession,
    months: list[date],
    *,
    child_fact_table: str,
    parent_fact_table: str,
    child_customers_table: str,
    delete_stale: bool,
) -> None:
    """Recompute monthly totals from the child's daily fact and upsert them into the shared fact.

    Totals are recomputed, never incremented, so replays converge. With ``delete_stale``
    (backfills) child-owned parent rows whose month no longer has any child orders are removed;
    the parent company's own rows are never touched.
    """
    if not months:
        return
    in_months = None
    for month in months:  # plain range predicates on the raw column let Delta skip files
        following = date(month.year + month.month // 12, month.month % 12 + 1, 1)
        clause = (F.col("date") >= F.lit(month)) & (F.col("date") < F.lit(following))
        in_months = clause if in_months is None else in_months | clause
    recomputed = (
        spark.table(child_fact_table)
        .filter(in_months)
        .withColumn("date", F.trunc("date", "MM"))
        .groupBy("date", "product_code", "customer_code")
        .agg(F.sum("sold_quantity").alias("sold_quantity"))
        .withColumn("_delete", F.lit(False))
    )
    if not spark.catalog.tableExists(parent_fact_table):
        recomputed.drop("_delete").write.format("delta").option("delta.enableChangeDataFeed", "true").mode(
            "overwrite"
        ).saveAsTable(parent_fact_table)
        return

    source = recomputed
    if delete_stale:
        child_codes = spark.table(child_customers_table).select(F.col("customer_id").cast("string").alias("customer_code"))
        stale = (
            spark.table(parent_fact_table)
            .filter(F.col("date").isin(months))
            .join(child_codes, on="customer_code", how="left_semi")
            .join(recomputed.select(*PARENT_KEYS), on=PARENT_KEYS, how="left_anti")
            .select(*PARENT_KEYS)
            .withColumn("sold_quantity", F.lit(None).cast("double"))
            .withColumn("_delete", F.lit(True))
        )
        source = recomputed.unionByName(stale)
    condition = " AND ".join(f"target.{key} = source.{key}" for key in PARENT_KEYS)
    (
        DeltaTable.forName(spark, parent_fact_table).alias("target")
        .merge(source.alias("source"), condition)
        .whenMatchedDelete(condition="source._delete")
        .whenMatchedUpdate(set={"sold_quantity": "source.sold_quantity"})
        .whenNotMatchedInsert(
            condition="NOT source._delete",
            values={"date": "source.date", "product_code": "source.product_code",
                    "customer_code": "source.customer_code", "sold_quantity": "source.sold_quantity"},
        )
        .execute()
    )


def _month_starts(frame: DataFrame, column: str) -> list[date]:
    rows = frame.select(F.trunc(column, "MM").alias("month")).distinct().collect()
    return sorted(row["month"] for row in rows if row["month"] is not None)


def _tables(catalog: str, bronze: str, silver: str, gold: str, bronze_table_name: str) -> dict[str, str]:
    return {
        "bronze": f"{catalog}.{bronze}.{bronze_table_name}",
        "products": f"{catalog}.{silver}.products",
        "customers": f"{catalog}.{silver}.customers",
        "silver": f"{catalog}.{silver}.orders",
        "gold": f"{catalog}.{gold}.sb_fact_orders",
        "parent": f"{catalog}.{gold}.fact_orders",
    }


# --------------------------------------------------------------------------- entry points


def process_order_batch(
    spark: SparkSession,
    batch: DataFrame,
    *,
    catalog: str,
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    bronze_table_name: str = "orders_raw",
    load_type: str = "autoloader",
    max_order_qty: float | None = None,
    use_merge_bounds: bool = True,
    source_keys: list[str] | None = None,
    run_id: str | None = None,
) -> dict:
    """Bronze -> Silver -> Gold for one micro-batch. Safe to run again on the same batch.

    ``source_keys`` and ``run_id`` let a non-file source (Kafka) supply its own idempotency key and a
    deterministic id for the batch; by default both are derived from the landed files.
    """
    if batch.isEmpty():
        return {"status": "empty"}
    names = _tables(catalog, bronze_schema, silver_schema, gold_schema, bronze_table_name)
    batch = _persist(batch)
    try:
        appended = ingest_bronze(spark, batch, names["bronze"], source_keys)
        missing = [column for column in RAW_COLUMNS if column not in batch.columns]
        if missing:
            # Raw rows are already preserved in Bronze; fail visibly rather than guess.
            raise SchemaContractError(f"landed files are missing required columns: {missing}")

        valid, quarantined = split_valid_and_quarantined(batch, spark.table(names["products"]), max_order_qty=max_order_qty)
        quality = persist_quality_results(
            spark, catalog=catalog, silver_schema=silver_schema, load_type=load_type,
            raw=batch, valid=valid, quarantined=quarantined, run_id=run_id or batch_run_id(batch),
        )
        silver_rows = _latest_per_key(valid.select(*SILVER_COLUMNS), SILVER_KEYS)
        _upsert(spark, silver_rows, names["silver"], SILVER_KEYS, ["product_id", "order_qty"],
                bound_column="order_placement_date", use_bounds=use_merge_bounds)
        gold_rows = _gold_rows(silver_rows)
        _upsert(spark, _latest_per_key(gold_rows, GOLD_KEYS), names["gold"], GOLD_KEYS, ["product_id", "sold_quantity"],
                bound_column="date", use_bounds=use_merge_bounds)
        refresh_parent_months(
            spark, _month_starts(silver_rows, "order_placement_date"),
            child_fact_table=names["gold"], parent_fact_table=names["parent"],
            child_customers_table=names["customers"], delete_stale=False,
        )
    finally:
        _unpersist(batch)
    return {"status": "processed", "bronze_rows_appended": appended, **quality}


def backfill_orders(
    spark: SparkSession,
    *,
    catalog: str,
    start_date: date,
    end_date: date,
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    bronze_table_name: str = "orders_raw",
    max_order_qty: float | None = None,
) -> dict:
    """Re-derive Silver, Gold and the shared monthly fact for [start_date, end_date] from Bronze.

    Replace semantics (``replaceWhere``), not upsert: a row that the current rules no longer
    accept disappears from Silver and Gold and appears in quarantine. Re-running with the same
    arguments leaves every table unchanged. Rows whose order date cannot be parsed belong to
    no date range and are left alone.
    """
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    names = _tables(catalog, bronze_schema, silver_schema, gold_schema, bronze_table_name)
    in_range = (
        spark.table(names["bronze"])
        .withColumn("_parsed", parse_order_date(F.col("order_placement_date")))
        .filter(F.col("_parsed").between(F.lit(start_date), F.lit(end_date)))
        .drop("_parsed")
    )
    valid, quarantined = split_valid_and_quarantined(in_range, spark.table(names["products"]), max_order_qty=max_order_qty)
    window = f"'{start_date.isoformat()}' AND '{end_date.isoformat()}'"
    options = hashlib.sha256(json.dumps([max_order_qty]).encode()).hexdigest()[:8]
    quality = persist_quality_results(
        spark, catalog=catalog, silver_schema=silver_schema, load_type="backfill",
        raw=in_range, valid=valid, quarantined=quarantined,
        run_id=f"backfill-{start_date}-{end_date}-{options}",
        replace_where=f"order_placement_date BETWEEN {window}",
    )
    silver_rows = _latest_per_key(valid.select(*SILVER_COLUMNS), SILVER_KEYS)
    _replace_range(spark, silver_rows, names["silver"], f"order_placement_date BETWEEN {window}")
    _replace_range(spark, _latest_per_key(_gold_rows(silver_rows), GOLD_KEYS), names["gold"], f"date BETWEEN {window}")

    first_month, last_month = start_date.replace(day=1), end_date.replace(day=1)
    months, cursor = [], first_month
    while cursor <= last_month:
        months.append(cursor)
        cursor = date(cursor.year + cursor.month // 12, cursor.month % 12 + 1, 1)
    refresh_parent_months(
        spark, months, child_fact_table=names["gold"], parent_fact_table=names["parent"],
        child_customers_table=names["customers"], delete_stale=True,
    )
    return {"start_date": str(start_date), "end_date": str(end_date), **quality}

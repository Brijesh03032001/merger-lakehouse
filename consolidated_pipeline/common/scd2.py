# Databricks notebook source
"""Reusable Delta Lake Slowly Changing Dimension Type 2 merge."""

from __future__ import annotations

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F


def _attribute_hash(columns: list[str]):
    values = [F.coalesce(F.col(column).cast("string"), F.lit("__NULL__")) for column in columns]
    return F.sha2(F.concat_ws("||", *values), 256)


def _assert_single_current_version(
    spark: SparkSession,
    target_table: str,
    business_key: str,
) -> None:
    duplicate_current = (
        spark.table(target_table)
        .filter(F.col("is_current"))
        .groupBy(business_key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_current:
        raise RuntimeError(
            f"SCD2 invariant failed for {target_table}: multiple current versions "
            f"exist for {business_key}"
        )


def apply_scd2(
    spark: SparkSession,
    *,
    source: DataFrame,
    target_table: str,
    business_key: str,
    tracked_columns: list[str],
    effective_at: datetime | None = None,
) -> dict[str, int | str]:
    """Insert new dimension versions and expire changed current versions.

    The source must contain one logical row per business key. Replaying the same
    source is idempotent because rows with an unchanged attribute hash are not
    staged for the Delta merge.
    """
    if business_key in tracked_columns:
        raise ValueError("business_key must not also be a tracked column")
    missing = ({business_key, *tracked_columns} - set(source.columns))
    if missing:
        raise ValueError(f"source is missing required columns: {sorted(missing)}")

    effective_at = effective_at or datetime.now(timezone.utc).replace(tzinfo=None)
    prepared = (
        source
        .select(business_key, *tracked_columns)
        .dropDuplicates([business_key])
        .withColumn("attribute_hash", _attribute_hash(tracked_columns))
    )

    if not spark.catalog.tableExists(target_table):
        initial = (
            prepared
            .withColumn("dimension_sk", F.expr("uuid()"))
            .withColumn("effective_from", F.lit(effective_at).cast("timestamp"))
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )
        inserted = initial.count()
        (
            initial.write.format("delta")
            .option("delta.enableChangeDataFeed", "true")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
        _assert_single_current_version(spark, target_table, business_key)
        return {"target_table": target_table, "inserted": inserted, "expired": 0, "unchanged": 0}

    current = (
        spark.table(target_table)
        .filter(F.col("is_current"))
        .select(
            F.col(business_key),
            F.col("attribute_hash").alias("_current_attribute_hash"),
        )
    )
    compared = prepared.join(current, on=business_key, how="left")
    changed = compared.filter(
        F.col("_current_attribute_hash").isNull()
        | (F.col("attribute_hash") != F.col("_current_attribute_hash"))
    ).drop("_current_attribute_hash")

    source_count = prepared.count()
    changed_count = changed.count()
    unchanged_count = source_count - changed_count
    if changed_count == 0:
        return {
            "target_table": target_table,
            "inserted": 0,
            "expired": 0,
            "unchanged": unchanged_count,
        }

    existing_changes = changed.join(current.select(business_key), on=business_key, how="inner")
    expired_count = existing_changes.count()
    versioned = (
        changed
        .withColumn("dimension_sk", F.expr("uuid()"))
        .withColumn("effective_from", F.lit(effective_at).cast("timestamp"))
        .withColumn("effective_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    # Existing changed keys are staged twice: once to expire the active row and
    # once with a null merge key to force insertion of the new current version.
    merge_key = f"_merge_{business_key}"
    expire_rows = (
        existing_changes
        .withColumn("effective_from", F.lit(effective_at).cast("timestamp"))
        .withColumn(merge_key, F.col(business_key))
    )
    business_key_type = prepared.schema[business_key].dataType
    insert_rows = versioned.withColumn(merge_key, F.lit(None).cast(business_key_type))
    staged = expire_rows.unionByName(insert_rows, allowMissingColumns=True)

    target = DeltaTable.forName(spark, target_table)
    insert_values = {column: f"source.{column}" for column in versioned.columns}
    (
        target.alias("target")
        .merge(
            staged.alias("source"),
            f"target.{business_key} = source.{merge_key} AND target.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                "is_current": "false",
                "effective_to": "source.effective_from",
            }
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    _assert_single_current_version(spark, target_table, business_key)
    return {
        "target_table": target_table,
        "inserted": changed_count,
        "expired": expired_count,
        "unchanged": unchanged_count,
    }


# Databricks notebook source
"""Pipeline health checks: reconciliation, SCD2 integrity, freshness and a valid-rate gate.

Each check compares two things the pipeline already promises are equal, so a failure
means a real defect or a missed load, not a tuned threshold. Results are persisted to
``silver.pipeline_check_results`` and the notebook fails the task on any ``fail``,
which is what raises the Databricks job alert.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

try:
    split_valid_and_quarantined  # already defined when this runs via %run after order_quality
except NameError:
    from consolidated_pipeline.common.order_quality import split_valid_and_quarantined

PASS, WARN, FAIL = "pass", "warn", "fail"
LOAD_TYPES = ("full", "incremental", "autoloader", "kafka")  # backfills re-evaluate history; they are not a daily rate


@dataclass
class CheckResult:
    name: str
    status: str
    observed: str
    expected: str
    detail: str = ""


def _close(a: float | None, b: float | None, tolerance: float) -> bool:
    a, b = a or 0.0, b or 0.0
    return abs(a - b) <= tolerance * max(1.0, abs(a), abs(b))


def check_run_accounting(spark: SparkSession, metrics_table: str) -> CheckResult:
    """Every quality run must account for each raw row exactly once: raw = valid + quarantined."""
    broken = spark.table(metrics_table).filter(F.col("raw_rows") != F.col("valid_rows") + F.col("quarantined_rows")).count()
    return CheckResult("run_accounting", PASS if broken == 0 else FAIL, f"{broken} runs off", "0 runs off",
                       "raw_rows == valid_rows + quarantined_rows for every row of order_quality_metrics")


def check_valid_rate(spark: SparkSession, metrics_table: str, min_valid_rate: float) -> CheckResult:
    latest = (
        spark.table(metrics_table).filter(F.col("load_type").isin(*LOAD_TYPES))
        .orderBy(F.col("measured_at").desc()).limit(1).collect()
    )
    if not latest:
        return CheckResult("valid_rate", WARN, "no runs", f">= {min_valid_rate}", "no quality metrics recorded yet")
    rate = latest[0]["valid_rate"]
    return CheckResult("valid_rate", PASS if rate >= min_valid_rate else FAIL, f"{rate:.4f}", f">= {min_valid_rate}",
                       f"latest {latest[0]['load_type']} run: {latest[0]['valid_rows']} valid of {latest[0]['raw_rows']}")


def check_silver_gold(spark: SparkSession, silver: str, gold: str, tolerance: float = 1e-9) -> list[CheckResult]:
    s = spark.table(silver).agg(F.count("*").alias("n"), F.sum("order_qty").alias("q")).first()
    g = spark.table(gold).agg(F.count("*").alias("n"), F.sum("sold_quantity").alias("q")).first()
    return [
        CheckResult("silver_gold_row_count", PASS if s["n"] == g["n"] else FAIL, str(g["n"]), str(s["n"]),
                    "gold.sb_fact_orders must hold one row per silver.orders row"),
        CheckResult("silver_gold_quantity", PASS if _close(s["q"], g["q"], tolerance) else FAIL,
                    str(g["q"]), str(s["q"]), "total quantity must match between silver.orders and gold.sb_fact_orders"),
    ]


def check_daily_vs_monthly(spark: SparkSession, daily: str, shared: str, customers: str, tolerance: float = 1e-9) -> CheckResult:
    """The shared monthly fact must equal the child's daily fact, restricted to the child's customers."""
    child_codes = spark.table(customers).select(F.col("customer_id").cast("string").alias("customer_code"))
    d = spark.table(daily).agg(F.sum("sold_quantity").alias("q")).first()["q"]
    m = (
        spark.table(shared).join(child_codes, on="customer_code", how="left_semi")
        .agg(F.sum("sold_quantity").alias("q")).first()["q"]
    )
    return CheckResult("daily_vs_monthly_quantity", PASS if _close(d, m, tolerance) else FAIL, str(m), str(d),
                       "monthly fact_orders (child customers) vs daily sb_fact_orders")


RECOMPUTE_COLUMNS = ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty", "read_timestamp", "file_name"]


def _bronze_union(spark: SparkSession, tables: list[str]) -> DataFrame | None:
    """Raw order rows from every existing Bronze table, as strings, so history split across the batch
    notebooks' ``orders`` and Auto Loader's ``orders_raw`` is judged as one source."""
    frames = [
        spark.table(table).select(*[F.col(c).cast("string").alias(c) if c != "read_timestamp" else F.col(c) for c in RECOMPUTE_COLUMNS])
        for table in tables
        if spark.catalog.tableExists(table)
    ]
    if not frames:
        return None
    union = frames[0]
    for frame in frames[1:]:
        union = union.unionByName(frame)
    return union


def check_bronze_recompute(
    spark: SparkSession, bronze_tables: list[str], products: str, gold: str, max_order_qty: float | None, tolerance: float = 1e-9
) -> list[CheckResult]:
    """Independently re-derive Gold from Bronze with the same contract and compare row count and quantity."""
    bronze = _bronze_union(spark, bronze_tables)
    if bronze is None:
        return [CheckResult("bronze_gold_recompute", WARN, "skipped", "-", f"none of {bronze_tables} exist")]
    valid, _ = split_valid_and_quarantined(bronze, spark.table(products), max_order_qty=max_order_qty)
    key = ["order_placement_date", "order_id", "product_code", "customer_id"]
    latest = valid.withColumn(
        "_rank", F.row_number().over(Window.partitionBy(*key).orderBy(F.col("read_timestamp").desc(), F.col("file_name").desc()))
    ).filter("_rank = 1")
    r = latest.agg(F.count("*").alias("n"), F.sum("order_qty").alias("q")).first()
    g = spark.table(gold).agg(F.count("*").alias("n"), F.sum("sold_quantity").alias("q")).first()
    return [
        CheckResult("bronze_gold_row_count", PASS if r["n"] == g["n"] else FAIL, str(g["n"]), str(r["n"]),
                    "rows re-derived from Bronze vs gold.sb_fact_orders"),
        CheckResult("bronze_gold_quantity", PASS if _close(r["q"], g["q"], tolerance) else FAIL, str(g["q"]), str(r["q"]),
                    "quantity re-derived from Bronze vs gold.sb_fact_orders"),
    ]


def check_scd2(spark: SparkSession, table: str, key: str) -> CheckResult:
    """One current version per key and no gaps or overlaps between consecutive versions."""
    name = f"scd2_integrity_{table.rsplit('.', 1)[-1]}"
    if not spark.catalog.tableExists(table):
        return CheckResult(name, WARN, "skipped", "-", f"{table} does not exist")
    versions = spark.table(table)
    ordered = Window.partitionBy(key).orderBy("effective_from")
    problems = (
        versions.withColumn("_next_from", F.lead("effective_from").over(ordered))
        .filter(
            ((F.col("_next_from").isNotNull()) & (F.col("effective_to") != F.col("_next_from")))
            | (F.col("is_current") & F.col("effective_to").isNotNull())
            | (~F.col("is_current") & F.col("effective_to").isNull())
        ).count()
    )
    multiple_current = versions.filter("is_current").groupBy(key).count().filter("count > 1").count()
    no_current = versions.groupBy(key).agg(F.max(F.col("is_current").cast("int")).alias("c")).filter("c = 0").count()
    total = problems + multiple_current + no_current
    return CheckResult(name, PASS if total == 0 else FAIL, f"{total} violations", "0 violations",
                       f"interval problems={problems}, keys with several current={multiple_current}, keys with none={no_current}")


def check_orphans(spark: SparkSession, fact: str, customers: str, products: str) -> list[CheckResult]:
    fact_frame = spark.table(fact)
    orphan_customers = (
        fact_frame.select(F.col("customer_code")).distinct()
        .join(spark.table(customers).select(F.col("customer_id").cast("string").alias("customer_code")), "customer_code", "left_anti")
        .count()
    )
    orphan_products = (
        fact_frame.select("product_code").distinct()
        .join(spark.table(products).select("product_code"), "product_code", "left_anti").count()
    )
    return [
        CheckResult("fact_customer_orphans", PASS if orphan_customers == 0 else WARN, str(orphan_customers), "0",
                    "customer codes in gold.sb_fact_orders with no row in silver.customers"),
        CheckResult("fact_product_orphans", PASS if orphan_products == 0 else FAIL, str(orphan_products), "0",
                    "product codes in gold.sb_fact_orders with no row in silver.products"),
    ]


def check_xref_collisions(spark: SparkSession, xref: str) -> CheckResult:
    """The shared fact cannot say which company a code came from, so a code used by both would double-count."""
    name = f"xref_code_collisions_{xref.rsplit('.', 1)[-1]}"
    if not spark.catalog.tableExists(xref):
        return CheckResult(name, PASS, "n/a", "-", f"{xref} does not exist: entity resolution has not run")
    clashes = spark.table(xref).groupBy("source_code").agg(F.countDistinct("source_system").alias("n")).filter("n > 1").count()
    return CheckResult(name, PASS if clashes == 0 else FAIL, f"{clashes} codes", "0 codes",
                       "customer/product codes present in both the parent and child systems")


def check_freshness(
    spark: SparkSession, table: str, column: str, max_age_hours: float, now: datetime | None = None
) -> CheckResult:
    """Fail when the newest ingested row is older than the cadence allows, i.e. a load was missed or is late."""
    name = f"freshness_{table.rsplit('.', 1)[-1]}"
    newest = spark.table(table).agg(F.max(column).alias("newest")).first()["newest"]
    if newest is None:
        return CheckResult(name, FAIL, "no data", f"<= {max_age_hours}h old", f"{table} is empty")
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    age = (now - newest.replace(tzinfo=None)).total_seconds() / 3600
    return CheckResult(name, PASS if age <= max_age_hours else FAIL, f"{age:.1f}h old", f"<= {max_age_hours}h old",
                       f"newest {column} = {newest}")


def run_checks(
    spark: SparkSession,
    *,
    catalog: str,
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    bronze_table: str = "orders,orders_raw,orders_kafka",
    max_order_qty: float | None = None,
    min_valid_rate: float = 0.6,
    max_age_hours: float = 30.0,
    recompute_from_bronze: bool = True,
    now: datetime | None = None,
) -> list[CheckResult]:
    s = lambda name: f"{catalog}.{silver_schema}.{name}"
    g = lambda name: f"{catalog}.{gold_schema}.{name}"
    results = [
        check_run_accounting(spark, s("order_quality_metrics")),
        check_valid_rate(spark, s("order_quality_metrics"), min_valid_rate),
        *check_silver_gold(spark, s("orders"), g("sb_fact_orders")),
        check_daily_vs_monthly(spark, g("sb_fact_orders"), g("fact_orders"), s("customers")),
        *check_orphans(spark, g("sb_fact_orders"), s("customers"), s("products")),
        check_scd2(spark, g("sb_dim_customers_history"), "customer_id"),
        check_scd2(spark, g("sb_dim_products_history"), "product_id"),
        check_freshness(spark, s("orders"), "read_timestamp", max_age_hours, now),
        check_xref_collisions(spark, g("xref_customers")),
        check_xref_collisions(spark, g("xref_products")),
    ]
    if recompute_from_bronze:
        bronze_tables = [f"{catalog}.{bronze_schema}.{name.strip()}" for name in bronze_table.split(",") if name.strip()]
        results += check_bronze_recompute(spark, bronze_tables, s("products"), g("sb_fact_orders"), max_order_qty)
    return results


def persist_check_results(spark: SparkSession, results: list[CheckResult], *, catalog: str, silver_schema: str = "silver", gold_schema: str = "gold") -> str:
    run_id = str(uuid4())
    rows = [(run_id, r.name, r.status, r.observed, r.expected, r.detail) for r in results]
    frame = spark.createDataFrame(rows, "check_run_id string, check_name string, status string, observed string, expected string, detail string")
    (
        frame.withColumn("checked_at", F.current_timestamp()).write.format("delta")
        .option("mergeSchema", "true").mode("append").saveAsTable(f"{catalog}.{silver_schema}.pipeline_check_results")
    )
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{gold_schema}.vw_data_health AS
        SELECT check_name, status, observed, expected, detail, checked_at
        FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY check_name ORDER BY checked_at DESC) AS rn
          FROM {catalog}.{silver_schema}.pipeline_check_results
        ) WHERE rn = 1
    """)
    return run_id


def enforce(results: list[CheckResult], *, fail_on_warn: bool = False) -> None:
    """Raise so the Databricks task fails and the job's failure notification fires."""
    blocking = {FAIL, WARN} if fail_on_warn else {FAIL}
    failed = [r for r in results if r.status in blocking]
    if failed:
        lines = "; ".join(f"{r.name}: observed {r.observed}, expected {r.expected}" for r in failed)
        raise RuntimeError(f"Data quality gate failed ({len(failed)} checks): {lines}")

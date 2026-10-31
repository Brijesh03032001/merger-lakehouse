"""Health checks must pass on a healthy pipeline and each fail for the defect it exists to catch."""

import itertools
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from consolidated_pipeline.common import quality_checks as qc

pytestmark = [pytest.mark.spark, pytest.mark.slow]

_copies = itertools.count(1)
COPIED = {
    "bronze": ["orders"],
    "silver": ["orders", "customers", "products", "order_quality_metrics"],
    "gold": ["sb_fact_orders", "fact_orders", "sb_dim_customers_history", "sb_dim_products_history"],
}


@pytest.fixture()
def sandbox(pipeline):
    """A throwaway copy of the healthy pipeline's tables that a test may damage."""
    spark, catalog, prefix = pipeline.spark, pipeline.catalog, f"qc{next(_copies)}"
    schemas = {layer: f"{prefix}_{layer}" for layer in COPIED}
    for layer, tables in COPIED.items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schemas[layer]}")
        if layer == "bronze":  # the other ingestion paths' tables exist when their tests ran first
            tables = [*tables, *[n for n in ("orders_raw", "orders_kafka") if spark.catalog.tableExists(f"{catalog}.bronze.{n}")]]
        for table in tables:
            spark.sql(f"CREATE TABLE {catalog}.{schemas[layer]}.{table} USING DELTA AS SELECT * FROM {catalog}.{layer}.{table}")

    def checks(**overrides):
        kwargs = dict(catalog=catalog, bronze_schema=schemas["bronze"], silver_schema=schemas["silver"],
                      gold_schema=schemas["gold"])
        return {r.name: r for r in qc.run_checks(spark, **{**kwargs, **overrides})}

    def sql(statement):
        spark.sql(statement.format(s=f"{catalog}.{schemas['silver']}", g=f"{catalog}.{schemas['gold']}",
                                   b=f"{catalog}.{schemas['bronze']}"))

    def table(layer, name):
        return spark.table(f"{catalog}.{schemas[layer]}.{name}")

    def append_variant(layer, name, **overrides):
        """Append a copy of one existing row with some columns replaced (by column name, not position)."""
        row = table(layer, name).limit(1)
        for column, value in overrides.items():
            row = row.withColumn(column, F.lit(value))
        row.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schemas[layer]}.{name}")

    def first(layer, name, column):
        return table(layer, name).agg(F.min(column)).first()[0]

    return type("Sandbox", (), {"checks": staticmethod(checks), "sql": staticmethod(sql), "pipeline": pipeline,
                                "schemas": schemas, "table": staticmethod(table),
                                "append_variant": staticmethod(append_variant), "first": staticmethod(first)})


def _statuses(results):
    return {name: result.status for name, result in results.items()}


def test_a_healthy_pipeline_passes_every_check(sandbox):
    results = sandbox.checks()
    assert {n: s for n, s in _statuses(results).items() if s != qc.PASS} == {}, {
        n: (r.observed, r.expected) for n, r in results.items() if r.status != qc.PASS
    }
    assert {"bronze_gold_row_count", "silver_gold_quantity", "daily_vs_monthly_quantity", "freshness_orders"} <= set(results)


def test_a_lost_gold_row_is_caught_by_both_reconciliations(sandbox):
    victim = sandbox.first("gold", "sb_fact_orders", "order_id")
    sandbox.sql(f"DELETE FROM {{g}}.sb_fact_orders WHERE order_id = '{victim}'")
    statuses = _statuses(sandbox.checks())
    assert statuses["silver_gold_row_count"] == qc.FAIL
    assert statuses["bronze_gold_row_count"] == qc.FAIL
    assert statuses["bronze_gold_quantity"] == qc.FAIL


def test_a_changed_quantity_is_caught_even_when_row_counts_match(sandbox):
    victim = sandbox.first("gold", "sb_fact_orders", "order_id")
    sandbox.sql(f"UPDATE {{g}}.sb_fact_orders SET sold_quantity = sold_quantity + 1 WHERE order_id = '{victim}'")
    statuses = _statuses(sandbox.checks())
    assert statuses["silver_gold_row_count"] == qc.PASS
    assert statuses["silver_gold_quantity"] == qc.FAIL
    assert statuses["bronze_gold_quantity"] == qc.FAIL
    assert statuses["daily_vs_monthly_quantity"] == qc.FAIL


def test_a_drifting_shared_monthly_fact_is_caught(sandbox):
    child_codes = ", ".join(repr(r["customer_id"]) for r in sandbox.table("silver", "customers").collect())
    sandbox.sql(f"UPDATE {{g}}.fact_orders SET sold_quantity = sold_quantity * 2 WHERE customer_code IN ({child_codes})")
    assert _statuses(sandbox.checks())["daily_vs_monthly_quantity"] == qc.FAIL


def test_scd2_double_current_versions_are_caught(sandbox):
    current = sandbox.table("gold", "sb_dim_customers_history").filter("is_current").limit(1)
    current.withColumn("dimension_sk", F.lit("duplicate-current")).write.format("delta").mode("append").saveAsTable(
        f"{sandbox.pipeline.catalog}.{sandbox.schemas['gold']}.sb_dim_customers_history"
    )
    results = sandbox.checks()
    assert results["scd2_integrity_sb_dim_customers_history"].status == qc.FAIL
    assert results["scd2_integrity_sb_dim_products_history"].status == qc.PASS


def test_scd2_gap_between_versions_is_caught(sandbox):
    """An expired version must end exactly when its successor begins."""
    key = sandbox.first("gold", "sb_dim_customers_history", "customer_id")
    sandbox.sql(f"""UPDATE {{g}}.sb_dim_customers_history SET is_current = false, effective_to = current_timestamp()
                    WHERE customer_id = '{key}'""")
    current = sandbox.table("gold", "sb_dim_customers_history").filter(f"customer_id = '{key}'").limit(1)
    current.withColumn("dimension_sk", F.lit("successor")).withColumn("is_current", F.lit(True)).withColumn(
        "effective_to", F.lit(None).cast("timestamp")
    ).withColumn("effective_from", F.expr("current_timestamp() + INTERVAL 1 DAY")).write.format("delta").mode(
        "append"
    ).saveAsTable(f"{sandbox.pipeline.catalog}.{sandbox.schemas['gold']}.sb_dim_customers_history")
    assert sandbox.checks()["scd2_integrity_sb_dim_customers_history"].status == qc.FAIL


def test_stale_data_fails_freshness_and_empty_data_fails_too(sandbox):
    late = datetime.utcnow() + timedelta(hours=100)
    assert _statuses(sandbox.checks(now=late))["freshness_orders"] == qc.FAIL
    sandbox.sql("DELETE FROM {s}.orders")
    assert sandbox.checks()["freshness_orders"].observed == "no data"


def test_valid_rate_gate_uses_the_latest_daily_run_not_backfills(sandbox):
    sandbox.sql("""INSERT INTO {s}.order_quality_metrics VALUES
                   ('daily', 'incremental', current_timestamp() + INTERVAL 1 DAY, 100, 90, 10, 0.9)""")
    assert _statuses(sandbox.checks(min_valid_rate=0.99))["valid_rate"] == qc.FAIL
    assert _statuses(sandbox.checks(min_valid_rate=0.6))["valid_rate"] == qc.PASS
    sandbox.sql("""INSERT INTO {s}.order_quality_metrics VALUES
                   ('bf', 'backfill', current_timestamp() + INTERVAL 2 DAY, 100, 1, 99, 0.01)""")
    assert _statuses(sandbox.checks(min_valid_rate=0.6))["valid_rate"] == qc.PASS, \
        "a backfill re-evaluating history must not trip the daily gate"


def test_run_accounting_catches_rows_that_vanish_between_stages(sandbox):
    sandbox.sql("""INSERT INTO {s}.order_quality_metrics VALUES
                   ('bad', 'incremental', current_timestamp(), 100, 60, 30, 0.6)""")
    assert _statuses(sandbox.checks())["run_accounting"] == qc.FAIL


def test_orphans_warn_for_customers_but_fail_for_products(sandbox):
    sandbox.append_variant("gold", "sb_fact_orders", order_id="ORPHAN-C", customer_code="1")
    statuses = _statuses(sandbox.checks(recompute_from_bronze=False))
    assert statuses["fact_customer_orphans"] == qc.WARN
    assert statuses["fact_product_orphans"] == qc.PASS
    sandbox.append_variant("gold", "sb_fact_orders", order_id="ORPHAN-P", product_code="no-such-product")
    assert _statuses(sandbox.checks(recompute_from_bronze=False))["fact_product_orphans"] == qc.FAIL


def test_code_collisions_between_companies_are_caught(sandbox):
    spark, catalog, gold = sandbox.pipeline.spark, sandbox.pipeline.catalog, sandbox.schemas["gold"]
    spark.sql(f"""CREATE TABLE {catalog}.{gold}.xref_customers (source_system string, source_code string, unified_id string)
                  USING DELTA""")
    spark.sql(f"INSERT INTO {catalog}.{gold}.xref_customers VALUES ('parent','5','P-5'), ('child','6','C-6')")
    assert _statuses(sandbox.checks(recompute_from_bronze=False))["xref_code_collisions_xref_customers"] == qc.PASS
    spark.sql(f"INSERT INTO {catalog}.{gold}.xref_customers VALUES ('child','5','C-5')")
    assert _statuses(sandbox.checks(recompute_from_bronze=False))["xref_code_collisions_xref_customers"] == qc.FAIL


def test_enforce_raises_only_on_failures_unless_warnings_are_blocking():
    ok, warn, bad = qc.CheckResult("a", qc.PASS, "1", "1"), qc.CheckResult("b", qc.WARN, "2", "0"), qc.CheckResult("c", qc.FAIL, "3", "0")
    qc.enforce([ok, warn])
    with pytest.raises(RuntimeError, match="c: observed 3"):
        qc.enforce([ok, warn, bad])
    with pytest.raises(RuntimeError, match="b: observed 2"):
        qc.enforce([ok, warn], fail_on_warn=True)


def test_notebook_persists_results_publishes_the_health_view_and_passes(pipeline):
    """Counts what this test's own runs add: other tests may already have run the gate on the shared state."""
    spark, catalog = pipeline.spark, pipeline.catalog
    table = f"{catalog}.silver.pipeline_check_results"
    before = spark.table(table).count() if spark.catalog.tableExists(table) else 0
    result = pipeline.run("5_quality/1_run_quality_checks.py")
    assert "[FAIL]" not in result.stdout
    added = spark.table(table).count() - before
    health = spark.table(f"{catalog}.gold.vw_data_health")
    assert added == health.count() and added >= 12, "one stored result per check, and the view shows one row per check"
    pipeline.run("5_quality/1_run_quality_checks.py")
    assert spark.table(f"{catalog}.gold.vw_data_health").count() == health.count(), "the view keeps one row per check"
    assert spark.table(table).count() == before + 2 * added

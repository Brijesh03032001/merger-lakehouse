"""Exactly-once ingestion and backfill, tested against the failure modes the batch notebooks have."""

import csv
import itertools
from datetime import date

import pytest
from pyspark.sql import functions as F

from consolidated_pipeline.common import order_ingestion as oi

pytestmark = [pytest.mark.spark, pytest.mark.slow]

_ids = itertools.count(1)


@pytest.fixture(scope="module")
def lake(pipeline, tmp_path_factory):
    """Isolated schemas holding only the dimensions the ingestion code reads."""
    spark, catalog = pipeline.spark, pipeline.catalog
    for schema in ("ing_bronze", "ing_silver", "ing_gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    for table in ("products", "customers"):
        spark.table(f"{catalog}.silver.{table}").write.format("delta").mode("overwrite").saveAsTable(
            f"{catalog}.ing_silver.{table}"
        )
    parent_rows = spark.table(f"{catalog}.gold.fact_orders").join(
        spark.table(f"{catalog}.silver.customers").select(F.col("customer_id").cast("string").alias("customer_code")),
        on="customer_code", how="left_anti",
    )
    parent_rows.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.ing_gold.fact_orders")
    pipeline.kwargs = dict(bronze_schema="ing_bronze", silver_schema="ing_silver", gold_schema="ing_gold")
    pipeline.parent_row_count = parent_rows.count()
    return pipeline


def _ids_for(lake):
    scenario = lake.scenario
    return scenario.child_customers[0]["customer_id"], scenario.child_products[0]["product_id"]


def _write(directory, name, rows):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    with path.open("w", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(oi.RAW_COLUMNS)
        writer.writerows(rows)
    return path


def _row(lake, order_id=None, when="2026-01-05", qty="5.0", customer=None):
    customer_id, product_id = _ids_for(lake)
    return [order_id or f"T{next(_ids):06d}", when, customer or customer_id, product_id, qty]


def _count(lake, schema, table):
    return lake.spark.table(f"{lake.catalog}.{schema}.{table}").count()


def _batch(lake, path):
    return oi.read_raw_batch(lake.spark, str(path))


def _process(lake, batch, **extra):
    return oi.process_order_batch(lake.spark, batch, catalog=lake.catalog, **lake.kwargs, **extra)


def _fingerprint(lake, schema, table, order_by):
    frame = lake.spark.table(f"{lake.catalog}.{schema}.{table}")
    frame = frame.drop("quarantined_at") if "quarantined_at" in frame.columns else frame
    return sorted(tuple(map(str, row)) for row in frame.orderBy(*order_by).collect())


def test_empty_batch_is_a_no_op_not_a_crash(lake):
    empty = lake.spark.createDataFrame([], "order_id string").withColumn("file_path", F.lit("x"))
    assert _process(lake, empty) == {"status": "empty"}


def test_iso_only_dates_no_longer_break_bronze(lake, tmp_path):
    """Regression: inferSchema typed an all-ISO day as DATE and the append into the string column failed."""
    first = _write(tmp_path / "a", "orders_a.csv", [_row(lake, when="2026-01-05"), _row(lake, when="2026-01-06")])
    outcome = _process(lake, _batch(lake, first))
    assert outcome["status"] == "processed" and outcome["valid_rows"] == 2
    second = _write(tmp_path / "b", "orders_b.csv", [_row(lake, when="Saturday, January 10, 2026")])
    assert _process(lake, _batch(lake, second))["valid_rows"] == 1
    bronze = lake.spark.table(f"{lake.catalog}.ing_bronze.orders_raw")
    assert dict(bronze.dtypes)["order_placement_date"] == "string"


def test_processing_the_same_batch_twice_changes_nothing(lake, tmp_path):
    rows = [_row(lake), _row(lake), _row(lake, customer="NOT-A-CUSTOMER"), _row(lake, qty="N/A")]
    path = _write(tmp_path, "orders_replay.csv", rows)
    _process(lake, _batch(lake, path))
    snapshot = {
        "bronze": _count(lake, "ing_bronze", "orders_raw"),
        "silver": _fingerprint(lake, "ing_silver", "orders", ["order_id"]),
        "gold": _fingerprint(lake, "ing_gold", "sb_fact_orders", ["order_id"]),
        "quarantine": _count(lake, "ing_silver", "quarantine_orders"),
        "metrics": _count(lake, "ing_silver", "order_quality_metrics"),
        "parent": _fingerprint(lake, "ing_gold", "fact_orders", ["date", "customer_code", "product_code"]),
    }
    replay = _process(lake, _batch(lake, path))
    assert replay["bronze_rows_appended"] == 0
    assert snapshot["bronze"] == _count(lake, "ing_bronze", "orders_raw")
    assert snapshot["silver"] == _fingerprint(lake, "ing_silver", "orders", ["order_id"])
    assert snapshot["gold"] == _fingerprint(lake, "ing_gold", "sb_fact_orders", ["order_id"])
    assert snapshot["quarantine"] == _count(lake, "ing_silver", "quarantine_orders")
    assert snapshot["metrics"] == _count(lake, "ing_silver", "order_quality_metrics")
    assert snapshot["parent"] == _fingerprint(lake, "ing_gold", "fact_orders", ["date", "customer_code", "product_code"])


def test_a_later_file_corrects_an_earlier_one_without_duplicating(lake, tmp_path):
    order_id = "CORRECT-1"
    _process(lake, _batch(lake, _write(tmp_path / "x", "o1.csv", [_row(lake, order_id=order_id, qty="10.0")])))
    _process(lake, _batch(lake, _write(tmp_path / "y", "o2.csv", [_row(lake, order_id=order_id, qty="12.0")])))
    silver = lake.spark.table(f"{lake.catalog}.ing_silver.orders").filter(f"order_id = '{order_id}'").collect()
    assert [row["order_qty"] for row in silver] == [12.0]


def test_conflicting_duplicates_inside_one_batch_do_not_break_merge(lake, tmp_path):
    """Two valid rows sharing a merge key but not a quantity used to make MERGE ambiguous."""
    order_id = "CONFLICT-1"
    path = _write(tmp_path, "o.csv", [_row(lake, order_id=order_id, qty="3.0"), _row(lake, order_id=order_id, qty="4.0")])
    _process(lake, _batch(lake, path))
    _process(lake, _batch(lake, _write(tmp_path / "again", "o2.csv", [_row(lake, order_id=order_id, qty="6.0")])))
    rows = lake.spark.table(f"{lake.catalog}.ing_silver.orders").filter(f"order_id = '{order_id}'").collect()
    assert len(rows) == 1 and rows[0]["order_qty"] == 6.0


def test_schema_drift_lands_in_bronze_and_leaves_silver_alone(lake, tmp_path):
    directory = tmp_path / "drift"
    directory.mkdir()
    path = directory / "orders_drift.csv"
    with path.open("w", newline="") as target:
        writer = csv.writer(target)
        writer.writerow([*oi.RAW_COLUMNS, "promo_code"])
        writer.writerow([*_row(lake, order_id="DRIFT-1"), "SUMMER"])
    silver_columns = lake.spark.table(f"{lake.catalog}.ing_silver.orders").columns
    outcome = _process(lake, _batch(lake, path))
    assert outcome["valid_rows"] == 1
    bronze = lake.spark.table(f"{lake.catalog}.ing_bronze.orders_raw")
    assert "promo_code" in bronze.columns
    assert bronze.filter("order_id = 'DRIFT-1'").first()["promo_code"] == "SUMMER"
    assert lake.spark.table(f"{lake.catalog}.ing_silver.orders").columns == silver_columns


def test_missing_required_column_fails_loudly_after_preserving_raw_rows(lake, tmp_path):
    directory = tmp_path / "broken"
    directory.mkdir()
    path = directory / "orders_broken.csv"
    path.write_text("order_id,customer_id\nBROKEN-1,1\n")
    with pytest.raises(oi.SchemaContractError):
        _process(lake, _batch(lake, path))
    held = lake.spark.table(f"{lake.catalog}.ing_bronze.orders_raw").filter("order_id = 'BROKEN-1'").count()
    assert held == 1


def test_shared_fact_keeps_the_parent_companys_rows(lake):
    parent_customers = lake.spark.table(f"{lake.catalog}.ing_gold.fact_orders")
    child_ids = [r["customer_id"] for r in lake.spark.table(f"{lake.catalog}.ing_silver.customers").collect()]
    own = parent_customers.filter(~F.col("customer_code").isin(child_ids)).count()
    assert own == lake.parent_row_count


def test_backfill_applies_a_new_rule_to_history_and_converges(lake, tmp_path):
    lake_args = dict(catalog=lake.catalog, **lake.kwargs)
    rows = [_row(lake, order_id=f"BF-{i}", when="2026-02-10", qty=str(float(q))) for i, q in enumerate([5, 50, 500, 5000])]
    other_month = _row(lake, order_id="BF-OTHER", when="2026-03-10", qty="9000.0")
    _process(lake, _batch(lake, _write(tmp_path, "bf.csv", [*rows, other_month])))
    assert lake.spark.table(f"{lake.catalog}.ing_silver.orders").filter("order_id LIKE 'BF-%'").count() == 5

    first = oi.backfill_orders(
        lake.spark, start_date=date(2026, 2, 1), end_date=date(2026, 2, 28), max_order_qty=100.0, **lake_args
    )
    assert first["raw_rows"] == 4 and first["valid_rows"] == 2 and first["quarantined_rows"] == 2
    silver = {r["order_id"]: r["order_qty"] for r in lake.spark.table(f"{lake.catalog}.ing_silver.orders").filter("order_id LIKE 'BF-%'").collect()}
    assert silver == {"BF-0": 5.0, "BF-1": 50.0, "BF-OTHER": 9000.0}, "other months must be untouched"
    reasons = {
        r["order_id"]: r["quarantine_reasons"]
        for r in lake.spark.table(f"{lake.catalog}.ing_silver.quarantine_orders").filter("order_id IN ('BF-2','BF-3')").collect()
    }
    assert reasons == {"BF-2": ["order_qty_above_limit"], "BF-3": ["order_qty_above_limit"]}

    gold = lake.spark.table(f"{lake.catalog}.ing_gold.sb_fact_orders").filter("order_id LIKE 'BF-%'").count()
    assert gold == 3
    customer_id, product_id = _ids_for(lake)
    product_code = lake.spark.table(f"{lake.catalog}.ing_silver.products").filter(f"product_id = '{product_id}'").first()["product_code"]
    feb = lake.spark.table(f"{lake.catalog}.ing_gold.fact_orders").filter(
        f"date = DATE'2026-02-01' AND customer_code = '{customer_id}' AND product_code = '{product_code}'"
    ).first()
    assert feb["sold_quantity"] == 55.0, "monthly total must reflect only the rows that survived the new rule"

    tables = [("ing_silver", "orders", ["order_id"]), ("ing_gold", "sb_fact_orders", ["order_id"]),
              ("ing_gold", "fact_orders", ["date", "customer_code", "product_code"]),
              ("ing_silver", "quarantine_orders", ["order_id", "pipeline_run_id"])]
    before = [_fingerprint(lake, s, t, o) for s, t, o in tables]
    oi.backfill_orders(lake.spark, start_date=date(2026, 2, 1), end_date=date(2026, 2, 28), max_order_qty=100.0, **lake_args)
    after = [_fingerprint(lake, s, t, o) for s, t, o in tables]
    assert before == after, "a second identical backfill must leave every table exactly as it was"


def test_backfill_can_remove_a_months_shared_row_entirely(lake, tmp_path):
    lake_args = dict(catalog=lake.catalog, **lake.kwargs)
    customer_id, _ = _ids_for(lake)
    path = _write(tmp_path, "gone.csv", [_row(lake, order_id="GONE-1", when="2026-04-12", qty="7000.0")])
    _process(lake, _batch(lake, path))
    shared = f"{lake.catalog}.ing_gold.fact_orders"
    assert lake.spark.table(shared).filter(f"date = DATE'2026-04-01' AND customer_code = '{customer_id}'").count() == 1
    oi.backfill_orders(lake.spark, start_date=date(2026, 4, 1), end_date=date(2026, 4, 30), max_order_qty=100.0, **lake_args)
    assert lake.spark.table(shared).filter(f"date = DATE'2026-04-01' AND customer_code = '{customer_id}'").count() == 0
    own = lake.spark.table(shared).filter(~F.col("customer_code").isin(
        [r["customer_id"] for r in lake.spark.table(f"{lake.catalog}.ing_silver.customers").collect()])).count()
    assert own == lake.parent_row_count, "the parent company's rows survive a backfill"


def test_backfill_rejects_an_inverted_range(lake):
    with pytest.raises(ValueError):
        oi.backfill_orders(lake.spark, catalog=lake.catalog, start_date=date(2026, 2, 2), end_date=date(2026, 2, 1), **lake.kwargs)


def test_caching_is_best_effort_so_serverless_compute_still_works():
    """Databricks serverless rejects persist(); ingestion must carry on uncached rather than fail."""
    class Serverless:
        def persist(self):
            raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute.")

        def unpersist(self):
            raise RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS]")

    frame = Serverless()
    assert oi._persist(frame) is frame
    oi._unpersist(frame)  # must not raise

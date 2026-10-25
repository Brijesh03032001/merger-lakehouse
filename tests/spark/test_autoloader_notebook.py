"""The streaming notebook's wiring (availableNow + checkpoint + foreachBatch), on Spark's csv file source.

Databricks Auto Loader (cloudFiles) cannot run outside Databricks, so only the reader differs here.
Everything downstream of the reader is the production code path.
"""

import csv
import shutil

import pytest

from consolidated_pipeline.common import order_ingestion as oi

pytestmark = [pytest.mark.spark, pytest.mark.slow]

NOTEBOOK = "3_fact_data_processing/3_autoloader_orders.py"


@pytest.fixture(scope="module")
def stream(pipeline, tmp_path_factory):
    root = tmp_path_factory.mktemp("stream_root")
    pipeline.stream_root = root
    pipeline.stream_run = lambda: pipeline.run(NOTEBOOK, source_root=str(root), reader="csv", data_source="orders")
    return pipeline


def _land(stream, name, rows):
    landing = stream.stream_root / "orders" / "landing"
    landing.mkdir(parents=True, exist_ok=True)
    with (landing / name).open("w", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(oi.RAW_COLUMNS)
        writer.writerows(rows)


def _rows(stream, prefix, count, when):
    customer = stream.scenario.child_customers[1]["customer_id"]
    product = stream.scenario.child_products[1]["product_id"]
    return [[f"{prefix}-{i}", when, customer, product, "4.0"] for i in range(count)]


def _silver_count(stream, prefix):
    return stream.spark.table(f"{stream.catalog}.silver.orders").filter(f"order_id LIKE '{prefix}-%'").count()


def test_a_day_with_no_new_files_succeeds(stream):
    """The batch notebook raised PATH_NOT_FOUND here and failed the scheduled job."""
    result = stream.stream_run()
    assert "'batches': 0" in result.stdout


def test_new_files_are_ingested_once(stream):
    _land(stream, "orders_1.csv", _rows(stream, "AL1", 3, "2026-01-05"))
    result = stream.stream_run()
    assert "'batches': 1" in result.stdout
    assert _silver_count(stream, "AL1") == 3
    stream.stream_run()
    assert _silver_count(stream, "AL1") == 3


def test_a_reset_checkpoint_cannot_double_ingest(stream):
    bronze = f"{stream.catalog}.bronze.orders_raw"
    before = stream.spark.table(bronze).count()
    shutil.rmtree(stream.stream_root / "orders" / "_autoloader" / "checkpoint")
    stream.stream_run()
    assert stream.spark.table(bronze).count() == before, "Bronze already held those files"
    assert _silver_count(stream, "AL1") == 3


def test_files_arriving_later_are_picked_up_by_the_next_run(stream):
    _land(stream, "orders_2.csv", _rows(stream, "AL2", 2, "Saturday, January 10, 2026"))
    stream.stream_run()
    assert _silver_count(stream, "AL2") == 2
    assert _silver_count(stream, "AL1") == 3


def test_shared_monthly_fact_matches_the_daily_fact(stream):
    spark, catalog = stream.spark, stream.catalog
    child_ids = [r["customer_id"] for r in spark.table(f"{catalog}.silver.customers").collect()]
    daily = spark.sql(
        f"""SELECT trunc(date,'MM') d, product_code, customer_code, SUM(sold_quantity) q
            FROM {catalog}.gold.sb_fact_orders GROUP BY 1,2,3"""
    ).collect()
    shared = {
        (r["date"], r["product_code"], r["customer_code"]): r["sold_quantity"]
        for r in spark.table(f"{catalog}.gold.fact_orders").filter(f"customer_code IN ({','.join(repr(c) for c in child_ids)})").collect()
    }
    assert {(r["d"], r["product_code"], r["customer_code"]): r["q"] for r in daily} == shared

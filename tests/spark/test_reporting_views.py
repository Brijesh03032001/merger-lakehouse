"""Point-in-time joins, unified cross-company rollups and time travel."""

from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

import consolidated_pipeline.common.entity_resolution as er
from consolidated_pipeline.common import point_in_time as pit
from consolidated_pipeline.common.scd2 import apply_scd2

pytestmark = [pytest.mark.spark, pytest.mark.slow]

CUSTOMER_COLUMNS = ["customer_name", "city", "customer", "market", "platform", "channel"]
PRODUCT_COLUMNS = ["product_code", "division", "category", "product", "variant"]


@pytest.fixture(scope="module")
def pit_lake(pipeline):
    """A tiny warehouse with business-dated SCD2 history: Acme moves Pune -> Mumbai on 2025-09-15."""
    spark, catalog = pipeline.spark, pipeline.catalog
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.pit_gold")

    def customer(city):
        return spark.createDataFrame(
            [("c1", "Acme", city, f"Acme-{city}", "India", "Retail", "Direct")],
            "customer_id string, customer_name string, city string, customer string, market string, platform string, channel string",
        ).select("customer_id", *CUSTOMER_COLUMNS)

    def product(category):
        return spark.createDataFrame(
            [("p1", "code-p1", "Bars", category, "Protein Bar", "Choc")],
            "product_id string, product_code string, division string, category string, product string, variant string",
        ).select("product_id", *PRODUCT_COLUMNS)

    target = f"{catalog}.pit_gold.sb_dim_customers_history"
    apply_scd2(spark, source=customer("Pune"), target_table=target, business_key="customer_id",
               tracked_columns=CUSTOMER_COLUMNS, effective_at=datetime(2025, 1, 1))
    apply_scd2(spark, source=customer("Mumbai"), target_table=target, business_key="customer_id",
               tracked_columns=CUSTOMER_COLUMNS, effective_at=datetime(2025, 9, 15, 10, 30))
    apply_scd2(spark, source=product("Protein Bars"), target_table=f"{catalog}.pit_gold.sb_dim_products_history",
               business_key="product_id", tracked_columns=PRODUCT_COLUMNS, effective_at=datetime(2025, 1, 1))
    orders = [("A", date(2024, 12, 1)), ("B", date(2025, 9, 14)), ("C", date(2025, 9, 15)), ("D", date(2025, 12, 1))]
    spark.createDataFrame(
        [(oid, d, "c1", "code-p1", "p1", 1.0) for oid, d in orders],
        "order_id string, date date, customer_code string, product_code string, product_id string, sold_quantity double",
    ).write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.pit_gold.sb_fact_orders")
    pit.create_pit_views(spark, catalog=catalog, gold="pit_gold")
    return pipeline


def test_each_order_sees_the_customer_as_it_was_that_day(pit_lake):
    rows = {r["order_id"]: r for r in pit_lake.spark.table(f"{pit_lake.catalog}.pit_gold.vw_fact_orders_pit").collect()}
    assert rows["A"]["city_as_of"] == "Pune", "orders before the first load resolve to the first version (back-dated)"
    assert rows["B"]["city_as_of"] == "Pune"
    assert rows["C"]["city_as_of"] == "Mumbai", "a change on day D applies from D onward"
    assert rows["D"]["city_as_of"] == "Mumbai"
    assert rows["A"]["category_as_of"] == "Protein Bars"


def test_the_as_of_join_neither_drops_nor_duplicates_orders(pit_lake):
    catalog = pit_lake.catalog
    view = pit_lake.spark.table(f"{catalog}.pit_gold.vw_fact_orders_pit")
    assert view.count() == pit_lake.spark.table(f"{catalog}.pit_gold.sb_fact_orders").count() == 4
    assert view.filter("city_as_of IS NULL").count() == 0


def test_current_state_tables_would_have_rewritten_history(pit_lake):
    """Why the SCD2 join matters: the current-state city is wrong for every order before the move."""
    current = (
        pit_lake.spark.table(f"{pit_lake.catalog}.pit_gold.sb_dim_customers_history").filter("is_current").first()["city"]
    )
    as_of = {r["order_id"]: r["city_as_of"] for r in pit_lake.spark.table(f"{pit_lake.catalog}.pit_gold.vw_fact_orders_pit").collect()}
    assert current == "Mumbai" and as_of["A"] != current


# ----------------------------------------------------------------------- unified views


@pytest.fixture(scope="module")
def unified(pipeline):
    pipeline.run("4_entity_resolution/1_resolve_entities.py", entity="customers")
    pipeline.run("4_entity_resolution/1_resolve_entities.py", entity="products")
    pipeline.run("6_reporting/1_reporting_views.py", merger_date="2025-09-01")
    return pipeline


def _tbl(p, name):
    return p.spark.table(f"{p.catalog}.gold.{name}")


def test_matched_customers_collapse_to_one_unified_customer(unified):
    xref = _tbl(unified, "xref_customers").collect()
    matched = sum(1 for r in xref if r["source_system"] == "child" and r["match_status"] in er.MATCHED_STATUSES)
    assert _tbl(unified, "vw_customers_unified").count() == len(xref) - matched
    overlap = _tbl(unified, "vw_customer_overlap").first()
    assert overlap["in_both_companies"] == matched
    assert overlap["parent_only"] + overlap["child_only"] + overlap["in_both_companies"] == overlap["unified_customers"]


def test_unified_label_and_city_come_from_the_parent_when_matched(unified):
    row = _tbl(unified, "vw_customers_unified").filter(F.size("source_systems") == 2).first()
    parent_code = next(c for c in row["member_codes"] if c.startswith("70"))
    parent_label = _tbl(unified, "dim_customers").filter(f"customer_code = '{parent_code}'").first()["customer"]
    assert row["customer"] == parent_label
    assert row["city"] == er.split_name_city(parent_label)[1]


def test_region_rollup_conserves_every_unit_sold(unified):
    region = _tbl(unified, "vw_orders_by_region_month")
    total_by_region = region.agg(F.sum("sold_quantity")).first()[0]
    total_in_fact = _tbl(unified, "fact_orders").agg(F.sum("sold_quantity")).first()[0]
    assert total_by_region == pytest.approx(total_in_fact), "the unified join must not drop or duplicate quantity"
    split = region.agg(F.sum("parent_quantity") + F.sum("child_quantity"), F.sum("sold_quantity")).first()
    assert split[0] == pytest.approx(split[1])
    assert {r["period"] for r in region.select("period").distinct().collect()} == {"pre_merger", "post_merger"}


def test_a_matched_customers_orders_from_both_companies_roll_up_together(unified):
    spark = unified.spark
    xref = _tbl(unified, "xref_customers")
    both = (
        xref.groupBy("unified_id").agg(F.countDistinct("source_system").alias("systems"))
        .filter("systems = 2").first()["unified_id"]
    )
    members = [r["source_code"] for r in xref.filter(f"unified_id = '{both}'").collect()]
    expected = (
        _tbl(unified, "fact_orders").filter(F.col("customer_code").isin(members)).agg(F.sum("sold_quantity")).first()[0]
    )
    rolled = (
        _tbl(unified, "fact_orders").join(xref.select("source_code", "unified_id"), F.col("customer_code") == F.col("source_code"))
        .filter(f"unified_id = '{both}'").agg(F.sum("sold_quantity")).first()[0]
    )
    assert rolled == pytest.approx(expected) and expected > 0


def test_history_retention_is_extended_for_time_travel(unified):
    properties = dict(unified.spark.sql(f"SHOW TBLPROPERTIES {unified.catalog}.gold.sb_fact_orders").collect())
    assert properties["delta.deletedFileRetentionDuration"] == "interval 90 days"


# ----------------------------------------------------------------------- time travel


def test_time_travel_reproduces_an_earlier_report(pit_lake):
    spark, table = pit_lake.spark, f"{pit_lake.catalog}.pit_gold.tt_fact"
    spark.createDataFrame([("x", 10.0)], "k string, q double").write.format("delta").mode("overwrite").saveAsTable(table)
    version = spark.sql(f"DESCRIBE HISTORY {table}").agg(F.max("version")).first()[0]
    spark.sql(f"UPDATE {table} SET q = 99.0")
    spark.sql(f"INSERT INTO {table} VALUES ('y', 5.0)")
    assert spark.table(table).agg(F.sum("q")).first()[0] == 104.0
    then = pit.table_as_of(spark, table, version=version)
    assert then.agg(F.sum("q")).first()[0] == 10.0 and then.count() == 1
    stamp = spark.sql(f"DESCRIBE HISTORY {table}").filter(f"version = {version}").first()["timestamp"]
    from datetime import timezone

    # PySpark returns a naive local-time datetime; converting it to an aware one keeps the instant exact.
    assert pit.table_as_of(spark, table, timestamp=stamp.astimezone(timezone.utc)).count() == 1


def test_time_travel_requires_exactly_one_anchor(pit_lake):
    with pytest.raises(ValueError):
        pit.table_as_of(pit_lake.spark, "t")
    with pytest.raises(ValueError):
        pit.table_as_of(pit_lake.spark, "t", timestamp="2025-01-01", version=1)

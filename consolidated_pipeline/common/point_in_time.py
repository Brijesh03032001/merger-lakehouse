# Databricks notebook source
"""Point-in-time and unified reporting views over the SCD2 history and the entity cross-reference.

Two questions the plain current-state tables cannot answer:
- "What did this customer / product look like when the order was placed?" (SCD2 as-of join)
- "What are sales per real customer and region across both companies?" (cross-reference rollup)
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession

# City -> conformed hierarchy shared by both companies. Reference data, not derived from sales.
GEOGRAPHY = [
    ("Bengaluru", "Karnataka", "South", "India"),
    ("Hyderabad", "Telangana", "South", "India"),
    ("Chennai", "Tamil Nadu", "South", "India"),
    ("Mumbai", "Maharashtra", "West", "India"),
    ("Pune", "Maharashtra", "West", "India"),
    ("Ahmedabad", "Gujarat", "West", "India"),
    ("New Delhi", "Delhi", "North", "India"),
    ("Kolkata", "West Bengal", "East", "India"),
]

FAR_PAST = "TIMESTAMP'1900-01-01 00:00:00'"
FAR_FUTURE = "TIMESTAMP'9999-12-31 00:00:00'"


def _pit_dimension(gold: str, history: str, key: str, view: str) -> str:
    """Validity window per version. The first version is back-dated so orders that predate the
    pipeline's first load still resolve to it; later versions start when the change was observed."""
    return f"""
    CREATE OR REPLACE VIEW {gold}.{view} AS
    SELECT h.*,
           CASE WHEN h.effective_from = MIN(h.effective_from) OVER (PARTITION BY h.{key})
                THEN {FAR_PAST} ELSE h.effective_from END AS valid_from,
           COALESCE(h.effective_to, {FAR_FUTURE}) AS valid_to
    FROM {gold}.{history} h
    """


def create_pit_views(spark: SparkSession, *, catalog: str, gold: str = "gold") -> None:
    g = f"{catalog}.{gold}"
    spark.sql(_pit_dimension(g, "sb_dim_customers_history", "customer_id", "vw_dim_customers_pit"))
    spark.sql(_pit_dimension(g, "sb_dim_products_history", "product_id", "vw_dim_products_pit"))

    # A change on day D applies to orders from D onward (day granularity).
    spark.sql(f"""
    CREATE OR REPLACE VIEW {g}.vw_fact_orders_pit AS
    SELECT f.order_id, f.date, f.customer_code, f.product_code, f.sold_quantity,
           c.customer AS customer_as_of, c.city AS city_as_of, c.channel AS channel_as_of,
           p.product AS product_as_of, p.category AS category_as_of, p.division AS division_as_of
    FROM {g}.sb_fact_orders f
    LEFT JOIN {g}.vw_dim_customers_pit c
      ON c.customer_id = f.customer_code
     AND f.date >= CAST(c.valid_from AS DATE) AND f.date < CAST(c.valid_to AS DATE)
    LEFT JOIN {g}.vw_dim_products_pit p
      ON p.product_id = f.product_id
     AND f.date >= CAST(p.valid_from AS DATE) AND f.date < CAST(p.valid_to AS DATE)
    """)


def create_unified_views(spark: SparkSession, *, catalog: str, merger_date: date, gold: str = "gold") -> None:
    g = f"{catalog}.{gold}"
    spark.createDataFrame(GEOGRAPHY, "city string, state string, region string, country string").write.format(
        "delta"
    ).mode("overwrite").saveAsTable(f"{g}.dim_geography")

    # One row per real customer. A matched child customer takes its parent's identity and label.
    spark.sql(f"""
    CREATE OR REPLACE VIEW {g}.vw_customers_unified AS
    SELECT x.unified_id,
           max_by(d.customer, IF(x.source_system = 'parent', 1, 0)) AS customer,
           NULLIF(regexp_extract(max_by(d.customer, IF(x.source_system = 'parent', 1, 0)), '-([^-]+)$', 1), '') AS city,
           max_by(d.channel, IF(x.source_system = 'parent', 1, 0)) AS channel,
           array_sort(collect_set(x.source_system)) AS source_systems,
           array_sort(collect_set(x.source_code)) AS member_codes
    FROM {g}.xref_customers x
    JOIN {g}.dim_customers d ON d.customer_code = x.source_code
    GROUP BY x.unified_id
    """)

    spark.sql(f"""
    CREATE OR REPLACE VIEW {g}.vw_orders_by_region_month AS
    SELECT f.date AS month,
           CASE WHEN f.date < DATE'{merger_date.isoformat()}' THEN 'pre_merger' ELSE 'post_merger' END AS period,
           COALESCE(geo.country, 'Unknown') AS country,
           COALESCE(geo.region, 'Unknown') AS region,
           COALESCE(geo.state, 'Unknown') AS state,
           SUM(f.sold_quantity) AS sold_quantity,
           SUM(IF(x.source_system = 'parent', f.sold_quantity, 0)) AS parent_quantity,
           SUM(IF(x.source_system = 'child', f.sold_quantity, 0)) AS child_quantity,
           COUNT(DISTINCT x.unified_id) AS unified_customers
    FROM {g}.fact_orders f
    JOIN {g}.xref_customers x ON x.source_code = f.customer_code
    JOIN {g}.vw_customers_unified u ON u.unified_id = x.unified_id
    LEFT JOIN {g}.dim_geography geo ON geo.city = u.city
    GROUP BY 1, 2, 3, 4, 5
    """)

    spark.sql(f"""
    CREATE OR REPLACE VIEW {g}.vw_customer_overlap AS
    SELECT COUNT(*) AS unified_customers,
           SUM(IF(array_contains(source_systems, 'parent') AND array_contains(source_systems, 'child'), 1, 0)) AS in_both_companies,
           SUM(IF(source_systems = array('parent'), 1, 0)) AS parent_only,
           SUM(IF(source_systems = array('child'), 1, 0)) AS child_only
    FROM {g}.vw_customers_unified
    """)



def keep_history_for_time_travel(spark: SparkSession, *, catalog: str, gold: str = "gold") -> None:
    """Time travel needs history that survives VACUUM; the 7-day default is too short for "as of last quarter"."""
    g = f"{catalog}.{gold}"
    for table in ("sb_fact_orders", "fact_orders"):
        spark.sql(
            f"ALTER TABLE {g}.{table} SET TBLPROPERTIES ("
            "'delta.logRetentionDuration' = 'interval 90 days', "
            "'delta.deletedFileRetentionDuration' = 'interval 90 days')"
        )


def create_reporting_views(spark: SparkSession, *, catalog: str, merger_date: date, gold: str = "gold") -> None:
    create_pit_views(spark, catalog=catalog, gold=gold)
    create_unified_views(spark, catalog=catalog, merger_date=merger_date, gold=gold)
    keep_history_for_time_travel(spark, catalog=catalog, gold=gold)


def table_as_of(
    spark: SparkSession, table: str, *, timestamp: str | datetime | None = None, version: int | None = None
) -> DataFrame:
    """Read a Delta table as it was at a past timestamp or version (exactly one must be given).

    A ``datetime`` is rendered with an explicit UTC offset, so the result does not depend on the
    session or machine time zone (a naive ``datetime`` is taken to be UTC). A plain string is passed
    through as written: include an offset such as ``2026-01-31 23:59:59+00:00``.
    """
    if (timestamp is None) == (version is None):
        raise ValueError("pass exactly one of timestamp or version")
    if isinstance(timestamp, datetime):
        aware = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
        timestamp = aware.astimezone(timezone.utc).isoformat(sep=" ", timespec="milliseconds")
    clause = f"TIMESTAMP AS OF '{timestamp}'" if timestamp is not None else f"VERSION AS OF {int(version)}"
    return spark.sql(f"SELECT * FROM {table} {clause}")

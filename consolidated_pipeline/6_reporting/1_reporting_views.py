# Databricks notebook source
"""
Reporting Views: Point-in-Time and Unified Across Both Companies

Purpose:
- Answer "what did the customer/product look like when the order was placed" from SCD2 history
- Roll both companies up to one customer and one region hierarchy via the entity cross-reference
- Keep enough Delta history for "as of last quarter" time-travel reports

Tech:
- Delta Lake views, time travel, SCD2 validity windows
"""

# COMMAND ----------

# MAGIC %run ../1_setup/utilities

# COMMAND ----------

# MAGIC %run ../common/point_in_time

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("merger_date", "2025-09-01", "Acquisition date (YYYY-MM-DD) splitting pre/post-merger periods")

catalog = dbutils.widgets.get("catalog")
merger_date = date.fromisoformat(dbutils.widgets.get("merger_date"))

# COMMAND ----------

create_reporting_views(spark, catalog=catalog, merger_date=merger_date, gold=gold_schema)

# COMMAND ----------

display(spark.table(f"{catalog}.{gold_schema}.vw_customer_overlap"))

# COMMAND ----------

display(spark.table(f"{catalog}.{gold_schema}.vw_orders_by_region_month").orderBy("month", "region"))

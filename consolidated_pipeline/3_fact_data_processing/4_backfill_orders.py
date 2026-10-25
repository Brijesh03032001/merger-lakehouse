# Databricks notebook source
"""
Order Backfill – Re-derive a Date Range from Bronze

Purpose:
- Rebuild Silver, Gold and the shared monthly fact for any order-date range from Bronze
- Apply corrected quality rules to history: rows that no longer pass leave Silver and Gold
  and land in quarantine with a reason code
- Safe to re-run: the same range and rules always converge to the same tables

Tech:
- Delta Lake replaceWhere (replace, not upsert), MERGE with tombstones for the shared fact
"""

# COMMAND ----------

# MAGIC %run ../1_setup/utilities

# COMMAND ----------

# MAGIC %run ../common/order_quality

# COMMAND ----------

# MAGIC %run ../common/order_ingestion

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("start_date", "2024-01-01", "First order date to rebuild (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2025-12-31", "Last order date to rebuild (YYYY-MM-DD)")
dbutils.widgets.text("bronze_table", "orders_raw", "Bronze table (orders_raw for Auto Loader, orders for the batch notebooks)")
dbutils.widgets.text("max_order_qty", "", "Reject quantities above this value (blank = no ceiling)")

catalog = dbutils.widgets.get("catalog")
start_date = date.fromisoformat(dbutils.widgets.get("start_date"))
end_date = date.fromisoformat(dbutils.widgets.get("end_date"))
bronze_table = dbutils.widgets.get("bronze_table")
max_order_qty = float(dbutils.widgets.get("max_order_qty")) if dbutils.widgets.get("max_order_qty") else None

# COMMAND ----------

outcome = backfill_orders(
    spark, catalog=catalog, start_date=start_date, end_date=end_date,
    bronze_schema=bronze_schema, silver_schema=silver_schema, gold_schema=gold_schema,
    bronze_table_name=bronze_table, max_order_qty=max_order_qty,
)
print("Backfill:", outcome)

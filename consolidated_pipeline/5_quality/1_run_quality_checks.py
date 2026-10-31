# Databricks notebook source
"""
Pipeline Health Gate

Purpose:
- Reconcile Bronze, Silver and Gold (row counts and quantity totals must agree)
- Verify SCD2 history integrity and a minimum valid-row rate
- Fail when the newest data is older than the load cadence allows (missed or late load)
- Persist every result so the health view shows the latest status per check

Tech:
- PySpark, Delta Lake
- Failing the task is the alert: attach email/webhook notifications to the job
"""

# COMMAND ----------

# MAGIC %run ../1_setup/utilities

# COMMAND ----------

# MAGIC %run ../common/order_quality

# COMMAND ----------

# MAGIC %run ../common/quality_checks

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("bronze_table", "orders,orders_raw,orders_kafka", "Bronze tables to re-derive Gold from (comma-separated; missing ones are skipped)")
dbutils.widgets.text("max_order_qty", "", "Quantity ceiling used by the load (blank = none)")
dbutils.widgets.text("min_valid_rate", "0.6", "Fail below this valid-row rate")
dbutils.widgets.text("max_age_hours", "30", "Fail when the newest row is older than this")
dbutils.widgets.text("recompute_from_bronze", "true", "Re-derive Gold from Bronze and compare (true | false)")
dbutils.widgets.text("fail_on_warn", "false", "Treat warnings as failures (true | false)")

catalog = dbutils.widgets.get("catalog")
max_order_qty_text = dbutils.widgets.get("max_order_qty")

# COMMAND ----------

results = run_checks(
    spark,
    catalog=catalog,
    bronze_schema=bronze_schema,
    silver_schema=silver_schema,
    gold_schema=gold_schema,
    bronze_table=dbutils.widgets.get("bronze_table"),
    max_order_qty=float(max_order_qty_text) if max_order_qty_text else None,
    min_valid_rate=float(dbutils.widgets.get("min_valid_rate")),
    max_age_hours=float(dbutils.widgets.get("max_age_hours")),
    recompute_from_bronze=dbutils.widgets.get("recompute_from_bronze").lower() == "true",
)
run_id = persist_check_results(spark, results, catalog=catalog, silver_schema=silver_schema, gold_schema=gold_schema)
for result in results:
    print(f"[{result.status.upper():4}] {result.name}: observed {result.observed}, expected {result.expected}")

# COMMAND ----------

display(spark.table(f"{catalog}.{gold_schema}.vw_data_health"))

# COMMAND ----------

enforce(results, fail_on_warn=dbutils.widgets.get("fail_on_warn").lower() == "true")

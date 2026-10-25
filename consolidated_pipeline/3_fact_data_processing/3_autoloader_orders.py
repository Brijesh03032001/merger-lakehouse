# Databricks notebook source
"""
Incremental Orders via Auto Loader

Purpose:
- Pick up newly landed order files exactly once, tracked by an Auto Loader checkpoint
- Succeed on days with no new files (the batch notebook fails on an empty landing folder)
- Keep Bronze as raw strings and adapt to new source columns without code changes
- Run each micro-batch through the shared quality contract into Silver, Gold and the shared monthly fact

Tech:
- Databricks Auto Loader (cloudFiles), Structured Streaming availableNow
- Delta Lake MERGE; file-level idempotent Bronze (common/order_ingestion.py)
"""

# COMMAND ----------

# MAGIC %run ../1_setup/utilities

# COMMAND ----------

# MAGIC %run ../common/order_quality

# COMMAND ----------

# MAGIC %run ../common/order_ingestion

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")
dbutils.widgets.text("source_root", "s3://sportsbar-analytics", "Source Root URI")
dbutils.widgets.text("reader", "cloudFiles", "cloudFiles (Databricks) | csv (open-source Spark)")
dbutils.widgets.text("max_order_qty", "", "Reject quantities above this value (blank = no ceiling)")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
source_root = dbutils.widgets.get("source_root").rstrip("/")
reader = dbutils.widgets.get("reader")
max_order_qty = float(dbutils.widgets.get("max_order_qty")) if dbutils.widgets.get("max_order_qty") else None

base_path = f"{source_root}/{data_source}"
landing_path = f"{base_path}/landing/"
checkpoint_path = f"{base_path}/_autoloader/checkpoint"
schema_path = f"{base_path}/_autoloader/schema"
dbutils.fs.mkdirs(landing_path)

# COMMAND ----------

def process(session, batch, batch_id):
    outcome = process_order_batch(
        session, batch, catalog=catalog, bronze_schema=bronze_schema,
        silver_schema=silver_schema, gold_schema=gold_schema, max_order_qty=max_order_qty,
    )
    print(f"batch {batch_id}:", outcome)


summary = run_orders_stream(
    spark, process, landing_path=landing_path, checkpoint_path=checkpoint_path,
    schema_path=schema_path, reader=reader,
)
print("Auto Loader run:", summary)

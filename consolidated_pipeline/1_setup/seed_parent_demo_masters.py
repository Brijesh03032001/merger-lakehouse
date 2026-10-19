# Databricks notebook source
"""
Seed Parent-Company Demo Masters

Purpose:
- Load a parent company's customers, products and monthly orders into the shared
  Gold tables so the acquired company has someone to be merged with
- Production already has these tables; run this only for the portfolio demo

Tech:
- PySpark, Delta Lake MERGE (idempotent: safe to re-run)
"""

# COMMAND ----------

import re

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("source_root", "s3://sportsbar-analytics", "Source Root URI")

catalog = dbutils.widgets.get("catalog")
source_root = dbutils.widgets.get("source_root").rstrip("/")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Invalid Unity Catalog identifier: {catalog!r}")

# COMMAND ----------

def read_csv(prefix: str):
    return spark.read.options(header=True).csv(f"{source_root}/{prefix}/*.csv")


def upsert(frame, table: str, keys: list[str]) -> None:
    if not spark.catalog.tableExists(table):
        frame.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(table)
        return
    condition = " AND ".join(f"target.{key} = source.{key}" for key in keys)
    (
        DeltaTable.forName(spark, table).alias("target")
        .merge(frame.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

customers = read_csv("parent_customers").select(
    F.col("customer_code").cast("string"), "customer", "market", "platform", "channel"
)
upsert(customers, f"{catalog}.gold.dim_customers", ["customer_code"])

# COMMAND ----------

products = read_csv("parent_products").select(
    F.col("product_code").cast("string"), "division", "category", "product", "variant"
)
upsert(products, f"{catalog}.gold.dim_products", ["product_code"])

# COMMAND ----------

facts = read_csv("parent_fact_orders").select(
    F.to_date("date").alias("date"),
    F.col("product_code").cast("string"),
    F.col("customer_code").cast("string"),
    F.col("sold_quantity").cast("double"),
)
upsert(facts, f"{catalog}.gold.fact_orders", ["date", "product_code", "customer_code"])

# COMMAND ----------

for table in ("dim_customers", "dim_products", "fact_orders"):
    print(table, spark.table(f"{catalog}.gold.{table}").count())

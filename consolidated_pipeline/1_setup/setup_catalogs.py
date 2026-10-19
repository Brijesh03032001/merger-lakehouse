# Databricks notebook source
"""
Catalog and Schema Setup

Purpose:
- Initialize Unity Catalog structure for FMCG data warehouse
- Create Bronze, Silver, and Gold schemas following medallion architecture
- Ensure idempotent catalog and schema creation
- Establish namespace foundation for all downstream pipelines

Tech:
- Databricks Unity Catalog
- SQL DDL
- Databricks Jobs compatible
"""

import re

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
catalog = dbutils.widgets.get("catalog")

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Invalid Unity Catalog identifier: {catalog!r}")

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`bronze`.`landing`")

print(f"Catalog ready: {catalog} (bronze, silver, gold; bronze.landing volume)")

# Databricks notebook source
"""
Date Dimension Table Creation

Purpose:
- Generate a date dimension table at monthly grain
- Create surrogate keys and time-based attributes
- Support fact table aggregations and time-based analytics
- Build reusable calendar table for the FMCG data warehouse

Tech:
- PySpark SQL
- Delta Lake
- Databricks Jobs compatible
"""

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

import re
from datetime import date

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **Define start and end dates**

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("start_date", "2024-01-01", "Start Date")
dbutils.widgets.text("end_date", "2025-12-01", "End Date")

catalog = dbutils.widgets.get("catalog")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Invalid Unity Catalog identifier: {catalog!r}")
start = date.fromisoformat(start_date)
end = date.fromisoformat(end_date)
if start > end:
    raise ValueError("start_date must be on or before end_date")

# COMMAND ----------

# 1️⃣ Generate one row per month start between start_date and end_date
df = (
    spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 month
            )
        ) AS month_start_date
    """)
)


# 2️⃣ Add useful analytics columns
df = (
    df
    # Surrogate key at month grain
    .withColumn("date_key", F.date_format("month_start_date", "yyyyMM").cast("int"))
    .withColumn("year", F.year("month_start_date"))
    .withColumn("month_name", F.date_format("month_start_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("month_start_date", "MMM"))
    .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("month_start_date")))
    .withColumn("year_quarter", F.concat(F.col("year"), F.lit("-Q"), F.quarter("month_start_date")))
)

# COMMAND ----------

display(df)

# COMMAND ----------

# Save as a table
df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{catalog}.gold.dim_date")

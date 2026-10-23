# Databricks notebook source
"""
Full Load Fact Processing – Orders

Purpose:
- Perform initial historical load of order data from S3 landing zone
- Apply data quality checks and normalization
- Build staging tables for full data refresh
- Aggregate daily data to monthly grain
- Load into Gold fact table with full refresh

Tech:
- PySpark
- Delta Lake (MERGE, ACID)
- Databricks Jobs compatible
"""

# COMMAND ----------

# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# MAGIC %run ../1_setup/utilities

# COMMAND ----------

# MAGIC %run ../common/order_quality

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")
dbutils.widgets.text("source_root", "s3://sportsbar-analytics", "Source Root URI")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")
source_root = dbutils.widgets.get("source_root").rstrip("/")

base_path = f'{source_root}/{data_source}'
landing_path = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"
print("Base Path: ", base_path)
print("Landing Path: ", landing_path)
print("Processed Path: ", processed_path)


# define the tables
bronze_table = f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

# DBTITLE 1,Write Delta Table
df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("append") \
 .saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Moving files from source to processed directory

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_orders_raw = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders_raw.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# Apply the shared contract. Invalid values are preserved in
# silver.quarantine_orders instead of being replaced by sentinel IDs or dropped.
df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_joined, df_quarantined = split_valid_and_quarantined(df_orders_raw, df_products)

quality_metrics = persist_quality_results(
    spark,
    catalog=catalog,
    silver_schema=silver_schema,
    load_type="full",
    raw=df_orders_raw,
    valid=df_joined,
    quarantined=df_quarantined,
)
print("Order quality metrics:", quality_metrics)

# Aggregate run-level metrics are appended to silver.order_quality_metrics.
display(
    df_quarantined
    .select(F.explode("quarantine_reasons").alias("reason"))
    .groupBy("reason")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

# check what's the maximum and minimum date
df_joined.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Validated orders with resolved products**

# COMMAND ----------

df_joined.show(5)

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

df_gold.show(2)

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema" , "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging with Parent company

# COMMAND ----------

# MAGIC %md
# MAGIC - Note: We want data for monthly level but child data is on daily level

# COMMAND ----------

# MAGIC %md
# MAGIC **Full Load**

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

df_monthly = (
    df_child
    # 1. Get month start date (e.g., 2025-11-30 → 2025-11-01)
    .withColumn("month_start", F.trunc("date", "MM"))   # or F.date_trunc("month", "date").cast("date")

    # 2.Group at monthly grain by month_start + product_code + customer_code
    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )

    # 3. Rename month_start back to `date` to match your target schema
    .withColumnRenamed("month_start", "date")
)

df_monthly.show(5, truncate=False)

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

parent_fact_table = f"{catalog}.{gold_schema}.fact_orders"
if not spark.catalog.tableExists(parent_fact_table):
    (
        df_monthly.write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .saveAsTable(parent_fact_table)
    )
else:
    gold_parent_delta = DeltaTable.forName(spark, parent_fact_table)
    (
        gold_parent_delta.alias("parent_gold").merge(
            df_monthly.alias("child_gold"),
            "parent_gold.date = child_gold.date AND "
            "parent_gold.product_code = child_gold.product_code AND "
            "parent_gold.customer_code = child_gold.customer_code",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    )

# COMMAND ----------


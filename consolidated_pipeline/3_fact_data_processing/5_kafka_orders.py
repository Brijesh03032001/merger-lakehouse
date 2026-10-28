# Databricks notebook source
"""
Orders from Kafka

Purpose:
- Consume order messages from a Kafka topic into the same Bronze -> Silver -> Gold path as the file flows
- Process each message exactly once: Bronze remembers (topic, partition, offset, timestamp), so retries,
  a reset checkpoint or a replayed offset range cannot duplicate or drop rows
- Quarantine malformed messages with their payload; keep fields the contract ignores in Bronze

Tech:
- Structured Streaming Kafka source, availableNow (scheduled drain) or processingTime (continuous)
- Delta Lake MERGE; common/kafka_ingestion.py and common/order_ingestion.py
"""

# COMMAND ----------

# MAGIC %run ../1_setup/utilities

# COMMAND ----------

# MAGIC %run ../common/order_quality

# COMMAND ----------

# MAGIC %run ../common/order_ingestion

# COMMAND ----------

# MAGIC %run ../common/kafka_ingestion

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("source_root", "s3://sportsbar-analytics", "Root URI for the checkpoint")
dbutils.widgets.text("bootstrap_servers", "", "Kafka bootstrap servers, host:port[,host:port]")
dbutils.widgets.text("topic", "orders", "Topic")
dbutils.widgets.text("starting_offsets", "earliest", "Where a NEW checkpoint starts (earliest | latest)")
dbutils.widgets.text("max_offsets_per_trigger", "", "Cap on messages per micro-batch (blank = no cap)")
dbutils.widgets.text("trigger", "availableNow", "availableNow | processingTime=30 seconds")
dbutils.widgets.text("fail_on_data_loss", "true", "Stop if retention deleted offsets we never read (true | false)")
dbutils.widgets.text("security_protocol", "PLAINTEXT", "PLAINTEXT | SSL | SASL_SSL | SASL_PLAINTEXT")
dbutils.widgets.text("sasl_mechanism", "PLAIN", "PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512")
dbutils.widgets.text("secret_scope", "", "Secret scope holding kafka_username and kafka_password (SASL only)")
dbutils.widgets.text("jaas_module_prefix", "kafkashaded.", "kafkashaded. on Databricks, blank on open-source Spark")
dbutils.widgets.text("max_order_qty", "", "Reject quantities above this value (blank = no ceiling)")
dbutils.widgets.text("reader", "kafka", "kafka | parquet (Kafka-shaped files, for tests without a broker)")
dbutils.widgets.text("files_path", "", "Kafka-shaped parquet folder (reader=parquet only)")

catalog = dbutils.widgets.get("catalog")
source_root = dbutils.widgets.get("source_root").rstrip("/")
reader = dbutils.widgets.get("reader")
max_order_qty = float(dbutils.widgets.get("max_order_qty")) if dbutils.widgets.get("max_order_qty") else None
checkpoint_path = f"{source_root}/orders/_kafka/checkpoint"
BRONZE_TABLE = "orders_kafka"

# COMMAND ----------

options = None
if reader == "kafka":
    scope = dbutils.widgets.get("secret_scope")
    options = kafka_options(
        dbutils.widgets.get("bootstrap_servers"),
        dbutils.widgets.get("topic"),
        starting_offsets=dbutils.widgets.get("starting_offsets"),
        max_offsets_per_trigger=int(dbutils.widgets.get("max_offsets_per_trigger") or 0) or None,
        fail_on_data_loss=dbutils.widgets.get("fail_on_data_loss").lower() == "true",
        security_protocol=dbutils.widgets.get("security_protocol"),
        sasl_mechanism=dbutils.widgets.get("sasl_mechanism"),
        username=dbutils.secrets.get(scope, "kafka_username") if scope else None,
        password=dbutils.secrets.get(scope, "kafka_password") if scope else None,
        jaas_module_prefix=dbutils.widgets.get("jaas_module_prefix"),
    )

# COMMAND ----------

def process(session, batch, batch_id):
    outcome = process_order_batch(
        session, batch, catalog=catalog, bronze_schema=bronze_schema, silver_schema=silver_schema,
        gold_schema=gold_schema, bronze_table_name=BRONZE_TABLE, load_type="kafka",
        max_order_qty=max_order_qty, source_keys=KAFKA_KEYS, run_id=kafka_batch_run_id(batch),
    )
    print(f"batch {batch_id}:", outcome)


summary = run_orders_kafka_stream(
    spark, process, options=options, checkpoint_path=checkpoint_path,
    trigger=dbutils.widgets.get("trigger"), reader=reader, files_path=dbutils.widgets.get("files_path") or None,
)
print("Kafka run:", summary)

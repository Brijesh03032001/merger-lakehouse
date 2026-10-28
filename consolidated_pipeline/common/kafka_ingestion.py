# Databricks notebook source
"""Kafka source for orders: messages become the same micro-batches the file path produces.

A message is JSON with the five order fields. Everything downstream (Bronze, the quality contract,
Silver, Gold, the shared monthly fact) is ``order_ingestion.process_order_batch``, unchanged.

Idempotency key: (topic, partition, offset, Kafka timestamp). The timestamp is in the key because a
topic that is deleted and recreated restarts at offset 0; on offsets alone the new messages would be
mistaken for ones Bronze already holds and silently dropped. A message re-read after a retry or a
reset checkpoint has the same timestamp, so it is skipped as it should be.
"""

from __future__ import annotations

import hashlib
import json
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, functions as F

try:
    RAW_COLUMNS  # already defined when this runs via %run after order_ingestion
except NameError:
    from consolidated_pipeline.common.order_ingestion import RAW_COLUMNS

KAFKA_KEYS = ["source_topic", "source_partition", "source_offset", "source_timestamp"]
KAFKA_RAW_SCHEMA = "key binary, value binary, topic string, partition int, offset long, timestamp timestamp, timestampType int"
ORDER_JSON_SCHEMA = ", ".join(f"{column} string" for column in RAW_COLUMNS)

_LOGIN_MODULES = {
    "PLAIN": "org.apache.kafka.common.security.plain.PlainLoginModule",
    "SCRAM-SHA-256": "org.apache.kafka.common.security.scram.ScramLoginModule",
    "SCRAM-SHA-512": "org.apache.kafka.common.security.scram.ScramLoginModule",
}


def _jaas_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def kafka_options(
    bootstrap_servers: str,
    topic: str,
    *,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: int | None = None,
    fail_on_data_loss: bool = True,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str = "PLAIN",
    username: str | None = None,
    password: str | None = None,
    jaas_module_prefix: str = "",
) -> dict[str, str]:
    """Reader options. ``jaas_module_prefix`` is ``kafkashaded.`` on Databricks, whose Kafka client is
    relocated, and empty on open-source Spark. ``starting_offsets`` applies only to a new checkpoint."""
    if not bootstrap_servers.strip():
        raise ValueError("bootstrap_servers is empty: set the Kafka bootstrap servers for this environment")
    if not topic.strip():
        raise ValueError("topic is empty")
    options = {
        "kafka.bootstrap.servers": bootstrap_servers.strip(),
        "subscribe": topic.strip(),
        "startingOffsets": starting_offsets,
        # True stops the run if retention deleted offsets we never read, instead of silently skipping them.
        "failOnDataLoss": str(fail_on_data_loss).lower(),
    }
    if max_offsets_per_trigger:
        options["maxOffsetsPerTrigger"] = str(int(max_offsets_per_trigger))
    if security_protocol != "PLAINTEXT":
        options["kafka.security.protocol"] = security_protocol
        if security_protocol.startswith("SASL"):
            if sasl_mechanism not in _LOGIN_MODULES:
                raise ValueError(f"unsupported SASL mechanism {sasl_mechanism!r}; use one of {sorted(_LOGIN_MODULES)}")
            if not username or not password:
                raise ValueError("SASL needs a username and password (read them from a secret scope)")
            options["kafka.sasl.mechanism"] = sasl_mechanism
            options["kafka.sasl.jaas.config"] = (
                f'{jaas_module_prefix}{_LOGIN_MODULES[sasl_mechanism]} required '
                f'username="{_jaas_quote(username)}" password="{_jaas_quote(password)}";'
            )
    return options


def orders_from_kafka(raw: DataFrame) -> DataFrame:
    """Shape Kafka records like the file path's micro-batches.

    ``file_name`` is a sortable ``kafka:topic:partition:offset`` and ``read_timestamp`` is the Kafka
    timestamp, so a later message corrects an earlier one exactly as a later file does. The whole
    payload is kept in ``raw_payload``, so fields the contract ignores are never lost. A message that
    yields none of the five order fields (garbage, empty, or the source renamed every field) is flagged
    ``is_malformed`` and quarantined with its payload instead of vanishing.
    """
    text = F.col("value").cast("string")
    fields = F.from_json(text, ORDER_JSON_SCHEMA)
    framed = raw.select(
        *[fields[column].alias(column) for column in RAW_COLUMNS],
        F.col("timestamp").alias("read_timestamp"),
        F.concat(
            F.lit("kafka:"), F.col("topic"), F.lit(":"),
            F.lpad(F.col("partition").cast("string"), 5, "0"), F.lit(":"),
            F.lpad(F.col("offset").cast("string"), 20, "0"),
        ).alias("file_name"),
        F.octet_length(F.col("value")).cast("long").alias("file_size"),
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("timestamp").alias("source_timestamp"),
        text.alias("raw_payload"),
    )
    no_order_fields = reduce(lambda a, b: a & b, [F.col(column).isNull() for column in RAW_COLUMNS])
    return framed.withColumn("is_malformed", no_order_fields)


def kafka_batch_run_id(batch: DataFrame) -> str:
    """Same offset ranges in -> same id out, so a retried batch replaces its own quarantine and metrics rows."""
    ranges = sorted(
        (row["source_topic"], row["source_partition"], row["lo"], row["hi"], str(row["first"]), str(row["last"]))
        for row in batch.groupBy("source_topic", "source_partition").agg(
            F.min("source_offset").alias("lo"), F.max("source_offset").alias("hi"),
            F.min("source_timestamp").alias("first"), F.max("source_timestamp").alias("last"),
        ).collect()
    )
    return "kafka-" + hashlib.sha256(json.dumps(ranges).encode()).hexdigest()[:24]


def build_orders_kafka_stream(
    spark: SparkSession, options: dict[str, str] | None = None, *, reader: str = "kafka", files_path: str | None = None
) -> DataFrame:
    """``kafka`` is the real source. ``parquet`` streams Kafka-shaped rows from files so the rest of the
    wiring (checkpoint, trigger, foreachBatch, idempotency) can be tested without a broker."""
    if reader == "kafka":
        raw = spark.readStream.format("kafka").options(**(options or {})).load()
    elif reader == "parquet":
        raw = spark.readStream.schema(KAFKA_RAW_SCHEMA).parquet(files_path)
    else:
        raise ValueError(f"unknown reader: {reader!r}")
    return orders_from_kafka(raw)


def run_orders_kafka_stream(
    spark: SparkSession,
    process_batch,
    *,
    options: dict[str, str] | None,
    checkpoint_path: str,
    trigger: str = "availableNow",
    reader: str = "kafka",
    files_path: str | None = None,
) -> dict:
    """``availableNow`` drains what is there and stops (schedule it). ``processingTime=30 seconds`` runs
    continuously; it never returns, so use it only for a long-running streaming job."""
    stream = build_orders_kafka_stream(spark, options, reader=reader, files_path=files_path)
    writer = stream.writeStream.foreachBatch(lambda batch, batch_id: process_batch(batch.sparkSession, batch, batch_id)).option(
        "checkpointLocation", checkpoint_path
    )
    if trigger == "availableNow":
        writer = writer.trigger(availableNow=True)
    elif trigger.startswith("processingTime="):
        writer = writer.trigger(processingTime=trigger.split("=", 1)[1])
    else:
        raise ValueError(f"trigger must be 'availableNow' or 'processingTime=<interval>', got {trigger!r}")
    query = writer.start()
    query.awaitTermination()
    progress = query.recentProgress
    return {"batches": sum(1 for p in progress if p["numInputRows"] > 0), "input_rows": sum(p["numInputRows"] for p in progress)}

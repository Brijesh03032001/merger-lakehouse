"""The Kafka source against a REAL broker. Skipped unless KAFKA_BOOTSTRAP_SERVERS is set.

    make kafka-up
    make test-kafka

STATUS: written but never run. No broker was available when it was written, so the first run is the
first proof that `format("kafka")`, the checkpoint and the notebook work together. The same logic is
covered without a broker by test_kafka_ingestion.py; this file only adds the real source.
"""

import json
import os
import shutil
import uuid
from types import SimpleNamespace

import pytest

pytestmark = [
    pytest.mark.spark,
    pytest.mark.slow,
    pytest.mark.skipif(not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"), reason="set KAFKA_BOOTSTRAP_SERVERS (make kafka-up)"),
]


@pytest.fixture(scope="module")
def broker(pipeline, tmp_path_factory):
    servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = f"orders-it-{uuid.uuid4().hex[:8]}"
    root = tmp_path_factory.mktemp("broker_root")
    customer = pipeline.scenario.child_customers[2]["customer_id"]
    product = pipeline.scenario.child_products[2]["product_id"]

    def produce(values):
        rows = [(f"k{i}", v) for i, v in enumerate(values)]
        (
            pipeline.spark.createDataFrame(rows, "key string, value string")
            .write.format("kafka").option("kafka.bootstrap.servers", servers).option("topic", topic).save()
        )

    def message(order_id, qty="3.0"):
        return json.dumps({"order_id": order_id, "order_placement_date": "2026-02-02", "customer_id": customer,
                           "product_id": product, "order_qty": qty})

    def ingest(**extra):
        return pipeline.run("3_fact_data_processing/5_kafka_orders.py", source_root=str(root), bootstrap_servers=servers,
                            topic=topic, reader="kafka", jaas_module_prefix="", **extra)

    def silver_count(prefix):
        return pipeline.spark.table(f"{pipeline.catalog}.silver.orders").filter(f"order_id LIKE '{prefix}-%'").count()

    return SimpleNamespace(root=root, produce=produce, message=message, ingest=ingest, silver_count=silver_count, spark=pipeline.spark,
                           catalog=pipeline.catalog)


def test_messages_flow_to_silver_and_a_second_run_is_a_no_op(broker):
    broker.produce([broker.message(f"BR1-{i}") for i in range(5)] + ["not json"])
    broker.ingest()
    assert broker.silver_count("BR1") == 5
    assert "'batches': 0" in broker.ingest().stdout


def test_a_reset_checkpoint_re_reads_the_topic_without_duplicating(broker):
    before = broker.spark.table(f"{broker.catalog}.bronze.orders_kafka").count()
    shutil.rmtree(broker.root / "orders" / "_kafka" / "checkpoint")
    broker.ingest(starting_offsets="earliest")
    assert broker.spark.table(f"{broker.catalog}.bronze.orders_kafka").count() == before
    assert broker.silver_count("BR1") == 5


def test_a_micro_batch_cap_splits_the_work_without_changing_the_result(broker):
    broker.produce([broker.message(f"BR2-{i}") for i in range(7)])
    result = broker.ingest(max_offsets_per_trigger="2")
    assert broker.silver_count("BR2") == 7
    assert "'batches': 1" not in result.stdout, "a cap of 2 messages must have produced several batches"


def test_a_missing_topic_fails_loudly(pipeline, tmp_path):
    with pytest.raises(Exception):
        pipeline.run("3_fact_data_processing/5_kafka_orders.py", source_root=str(tmp_path), reader="kafka",
                     bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], topic=f"does-not-exist-{uuid.uuid4().hex}",
                     jaas_module_prefix="")

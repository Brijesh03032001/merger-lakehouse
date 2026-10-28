"""Kafka path: message parsing, malformed handling, per-message idempotency and the streaming wiring.

No broker is involved. Records are built in the shape Spark's Kafka source emits (key, value, topic,
partition, offset, timestamp), so everything after the source is the production code. The one thing
these tests cannot show is the `format("kafka")` reader itself talking to a real broker.
"""

import itertools
import json
import shutil
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from consolidated_pipeline.common import kafka_ingestion as ki
from consolidated_pipeline.common import order_ingestion as oi

pytestmark = [pytest.mark.spark, pytest.mark.slow]

_ids = itertools.count(1)
BASE = datetime(2026, 1, 5, 8, 0, 0)


@pytest.fixture(scope="module")
def lake(pipeline):
    """Isolated schemas holding only the dimensions the ingestion code reads."""
    spark, catalog = pipeline.spark, pipeline.catalog
    for schema in ("kf_bronze", "kf_silver", "kf_gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    for table in ("products", "customers"):
        spark.table(f"{catalog}.silver.{table}").write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.kf_silver.{table}")
    pipeline.kwargs = dict(
        bronze_schema="kf_bronze", silver_schema="kf_silver", gold_schema="kf_gold",
        bronze_table_name="orders_kafka", load_type="kafka", source_keys=ki.KAFKA_KEYS,
    )
    return pipeline


def _ids_for(lake):
    return lake.scenario.child_customers[0]["customer_id"], lake.scenario.child_products[0]["product_id"]


def _msg(lake, order_id=None, qty="5.0", when="2026-01-05", **overrides):
    customer, product = _ids_for(lake)
    body = {"order_id": order_id or f"K{next(_ids):06d}", "order_placement_date": when,
            "customer_id": customer, "product_id": product, "order_qty": qty}
    body.update(overrides)
    return json.dumps({k: v for k, v in body.items() if v is not None})


def _records(lake, values, *, topic="orders", partition=0, first_offset=0, start=BASE):
    """Kafka-shaped rows. A value of None is a tombstone; bytes pass through untouched."""
    rows = []
    for index, value in enumerate(values):
        raw = value if isinstance(value, bytes) or value is None else value.encode()
        offset = first_offset + index  # the timestamp is a function of the offset, as for a real record
        rows.append((b"k", raw, topic, partition, offset, start + timedelta(seconds=offset), 0))
    return lake.spark.createDataFrame(rows, ki.KAFKA_RAW_SCHEMA)


def _process(lake, raw):
    batch = ki.orders_from_kafka(raw)
    return oi.process_order_batch(lake.spark, batch, catalog=lake.catalog, run_id=ki.kafka_batch_run_id(batch), **lake.kwargs)


def _table(lake, schema, name):
    return lake.spark.table(f"{lake.catalog}.{schema}.{name}")


def _fingerprint(lake, schema, name, order_by):
    frame = _table(lake, schema, name)
    frame = frame.drop(*[c for c in ("quarantined_at", "measured_at") if c in frame.columns])
    return sorted(tuple(map(str, row)) for row in frame.orderBy(*order_by).collect())


# ----------------------------------------------------------------------------- parsing


def test_a_message_becomes_an_order_row_with_traceable_lineage(lake):
    payload = _msg(lake, order_id="PARSE-1", qty=7)  # a JSON number, not a string
    row = ki.orders_from_kafka(_records(lake, [payload], topic="orders", partition=2, first_offset=41)).first()
    assert (row["order_id"], row["order_qty"], row["is_malformed"]) == ("PARSE-1", "7", False)
    assert row["file_name"] == "kafka:orders:00002:00000000000000000041"
    assert row["file_size"] == len(payload.encode())
    assert row["read_timestamp"] == BASE + timedelta(seconds=41)  # the test helper stamps offset seconds
    assert row["raw_payload"] == payload


def test_file_names_sort_in_offset_order(lake):
    names = [r["file_name"] for r in ki.orders_from_kafka(_records(lake, [_msg(lake)] * 12, first_offset=5)).collect()]
    assert sorted(names) == names, "'...10' must not sort before '...9': later messages must win ties"


@pytest.mark.parametrize(
    "value",
    [b"this is not json", b"", None, b"[1,2,3]", b'{"order_id": broken', b'{"id":"X","date":"2026-01-05","qty":"3"}'],
    ids=["garbage", "empty", "tombstone", "json-array", "truncated", "renamed-fields"],
)
def test_messages_with_no_order_fields_are_malformed(lake, value):
    row = ki.orders_from_kafka(_records(lake, [value])).first()
    assert row["is_malformed"] is True


def test_a_message_with_some_fields_is_not_malformed_it_fails_the_contract_with_specific_reasons(lake):
    row = ki.orders_from_kafka(_records(lake, [json.dumps({"order_id": "PART-1", "customer_id": "789403"})])).first()
    assert row["is_malformed"] is False


# ----------------------------------------------------------------------------- end to end


def test_valid_malformed_and_partial_messages_are_each_routed_correctly(lake):
    customer, _ = _ids_for(lake)
    values = [
        _msg(lake, order_id="E2E-OK"),
        b"definitely not json",
        json.dumps({"order_id": "E2E-PARTIAL", "customer_id": customer}),
        _msg(lake, order_id="E2E-NEG", qty="-4"),
    ]
    outcome = _process(lake, _records(lake, values, first_offset=100))
    assert outcome["status"] == "processed" and outcome["valid_rows"] == 1 and outcome["quarantined_rows"] == 3
    silver = {r["order_id"] for r in _table(lake, "kf_silver", "orders").collect()}
    assert silver == {"E2E-OK"}
    quarantine = {(r["raw_payload"], tuple(sorted(r["quarantine_reasons"]))) for r in _table(lake, "kf_silver", "quarantine_orders").collect()}
    garbage = next(reasons for payload, reasons in quarantine if payload == "definitely not json")
    assert "malformed_message" in garbage, "unparseable messages are quarantined with their payload, not dropped"
    partial = next(reasons for payload, reasons in quarantine if payload and "E2E-PARTIAL" in payload)
    assert "malformed_message" not in partial and "invalid_order_date" in partial and "invalid_product_id" in partial
    bronze = _table(lake, "kf_bronze", "orders_kafka")
    assert bronze.count() == 4, "every message is kept in Bronze, including the bad ones"


def test_fields_the_contract_ignores_are_kept_in_bronze_and_do_not_reach_silver(lake):
    silver_columns = _table(lake, "kf_silver", "orders").columns
    _process(lake, _records(lake, [_msg(lake, order_id="EXTRA-1", promo_code="SUMMER", channel="web")], first_offset=200))
    bronze = _table(lake, "kf_bronze", "orders_kafka").filter("raw_payload LIKE '%EXTRA-1%'").first()
    assert json.loads(bronze["raw_payload"])["promo_code"] == "SUMMER"
    assert _table(lake, "kf_silver", "orders").columns == silver_columns


def test_replaying_the_same_batch_changes_nothing(lake):
    raw = _records(lake, [_msg(lake), _msg(lake), b"junk"], first_offset=300)
    _process(lake, raw)
    tables = [("kf_silver", "orders", ["order_id"]), ("kf_gold", "sb_fact_orders", ["order_id"]),
              ("kf_silver", "quarantine_orders", ["pipeline_run_id", "file_name"]),
              ("kf_silver", "order_quality_metrics", ["pipeline_run_id"]),
              ("kf_gold", "fact_orders", ["date", "customer_code", "product_code"])]
    before = [_fingerprint(lake, s, t, o) for s, t, o in tables]
    bronze_before = _table(lake, "kf_bronze", "orders_kafka").count()
    replay = _process(lake, raw)
    assert replay["bronze_rows_appended"] == 0
    assert _table(lake, "kf_bronze", "orders_kafka").count() == bronze_before
    assert before == [_fingerprint(lake, s, t, o) for s, t, o in tables]


def test_overlapping_offset_ranges_append_only_the_new_messages(lake):
    """After a checkpoint reset or a different maxOffsetsPerTrigger, a batch can re-read offsets already held."""
    values = [_msg(lake, order_id=f"OVL-{i}") for i in range(10)]
    _process(lake, _records(lake, values[:6], first_offset=400))
    outcome = _process(lake, _records(lake, values[2:10], first_offset=402))  # offsets 402..409, 402..405 already held
    assert outcome["bronze_rows_appended"] == 4
    assert _table(lake, "kf_silver", "orders").filter("order_id LIKE 'OVL-%'").count() == 10


def test_a_recreated_topic_is_new_data_not_a_replay(lake):
    """Same topic, partition and offsets, different timestamps: offsets alone would drop these as 'already seen'."""
    first = _records(lake, [_msg(lake, order_id="TOP-OLD")], topic="orders-recreated", first_offset=0, start=BASE)
    second = _records(lake, [_msg(lake, order_id="TOP-NEW")], topic="orders-recreated", first_offset=0, start=BASE + timedelta(days=30))
    _process(lake, first)
    outcome = _process(lake, second)
    assert outcome["bronze_rows_appended"] == 1
    assert {r["order_id"] for r in _table(lake, "kf_silver", "orders").filter("order_id LIKE 'TOP-%'").collect()} == {"TOP-OLD", "TOP-NEW"}


def test_the_later_message_wins_a_correction_within_a_batch_and_across_batches(lake):
    order = "CORR-1"
    _process(lake, _records(lake, [_msg(lake, order_id=order, qty="10.0"), _msg(lake, order_id=order, qty="12.0")], first_offset=500))
    row = _table(lake, "kf_silver", "orders").filter(f"order_id = '{order}'").collect()
    assert [r["order_qty"] for r in row] == [12.0], "two messages for one order in a batch: the later offset wins"
    _process(lake, _records(lake, [_msg(lake, order_id=order, qty="15.0")], first_offset=502, start=BASE + timedelta(hours=1)))
    row = _table(lake, "kf_silver", "orders").filter(f"order_id = '{order}'").collect()
    assert [r["order_qty"] for r in row] == [15.0]
    assert _table(lake, "kf_gold", "sb_fact_orders").filter(f"order_id = '{order}'").first()["sold_quantity"] == 15.0


def test_an_empty_batch_is_a_no_op(lake):
    empty = ki.orders_from_kafka(_records(lake, []))
    assert oi.process_order_batch(lake.spark, empty, catalog=lake.catalog, **lake.kwargs) == {"status": "empty"}


# ----------------------------------------------------------------------------- reader options


def test_default_options_read_from_the_beginning_and_refuse_to_skip_lost_data():
    options = ki.kafka_options("broker:9092", "orders")
    assert options == {"kafka.bootstrap.servers": "broker:9092", "subscribe": "orders",
                       "startingOffsets": "earliest", "failOnDataLoss": "true"}


def test_micro_batch_size_and_data_loss_policy_are_configurable():
    options = ki.kafka_options("b:9092", "orders", starting_offsets="latest", max_offsets_per_trigger=50_000, fail_on_data_loss=False)
    assert options["maxOffsetsPerTrigger"] == "50000" and options["failOnDataLoss"] == "false"
    assert options["startingOffsets"] == "latest"


def test_sasl_credentials_become_a_jaas_line_with_the_databricks_shaded_class():
    options = ki.kafka_options("b:9093", "orders", security_protocol="SASL_SSL", username="svc", password="p@ss",
                               jaas_module_prefix="kafkashaded.")
    assert options["kafka.security.protocol"] == "SASL_SSL" and options["kafka.sasl.mechanism"] == "PLAIN"
    assert options["kafka.sasl.jaas.config"] == (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="svc" password="p@ss";')


def test_scram_uses_its_own_login_module_and_jaas_values_are_escaped():
    options = ki.kafka_options("b:9093", "t", security_protocol="SASL_SSL", sasl_mechanism="SCRAM-SHA-512",
                               username="u", password='we"ird\\pw')
    line = options["kafka.sasl.jaas.config"]
    assert "scram.ScramLoginModule" in line and 'password="we\\"ird\\\\pw"' in line


@pytest.mark.parametrize(
    "kwargs",
    [{"bootstrap_servers": " "}, {"topic": ""}, {"security_protocol": "SASL_SSL"},
     {"security_protocol": "SASL_SSL", "username": "u", "password": "p", "sasl_mechanism": "GSSAPI"}],
    ids=["no-brokers", "no-topic", "sasl-without-credentials", "unsupported-mechanism"],
)
def test_misconfiguration_fails_before_any_data_moves(kwargs):
    defaults = {"bootstrap_servers": "b:9092", "topic": "orders"}
    with pytest.raises(ValueError):
        ki.kafka_options(**{**defaults, **kwargs})


def test_unknown_reader_and_trigger_are_rejected(lake):
    with pytest.raises(ValueError):
        ki.build_orders_kafka_stream(lake.spark, {}, reader="rabbitmq")
    with pytest.raises(ValueError):
        ki.run_orders_kafka_stream(lake.spark, lambda *a: None, options={}, checkpoint_path="/tmp/x", trigger="whenever", reader="parquet",
                                   files_path="/tmp")


# ----------------------------------------------------------------------------- streaming wiring

NOTEBOOK = "3_fact_data_processing/5_kafka_orders.py"


@pytest.fixture(scope="module")
def stream(pipeline, tmp_path_factory):
    root = tmp_path_factory.mktemp("kafka_root")
    files = tmp_path_factory.mktemp("kafka_shaped")
    pipeline.k_root, pipeline.k_files = root, files
    pipeline.k_run = lambda **extra: pipeline.run(NOTEBOOK, source_root=str(root), reader="parquet", files_path=str(files), **extra)
    return pipeline


def _drop(stream, values, first_offset, start=BASE):
    """Land Kafka-shaped rows as a new parquet file. A streaming file source does not recurse into subfolders."""
    _records(stream, values, first_offset=first_offset, start=start).coalesce(1).write.mode("append").parquet(str(stream.k_files))


def _count(stream, prefix):
    return stream.spark.table(f"{stream.catalog}.silver.orders").filter(f"order_id LIKE '{prefix}-%'").count()


def test_a_run_with_nothing_new_succeeds(stream):
    assert "'batches': 0" in stream.k_run().stdout


def test_new_messages_are_processed_once(stream):
    _drop(stream, [_msg(stream, order_id=f"KS1-{i}") for i in range(3)], first_offset=0)
    assert "'batches': 1" in stream.k_run().stdout
    assert _count(stream, "KS1") == 3
    assert "'batches': 0" in stream.k_run().stdout
    assert _count(stream, "KS1") == 3


def test_a_reset_checkpoint_cannot_double_ingest(stream):
    bronze = f"{stream.catalog}.bronze.orders_kafka"
    before = stream.spark.table(bronze).count()
    shutil.rmtree(stream.k_root / "orders" / "_kafka" / "checkpoint")
    stream.k_run()
    assert stream.spark.table(bronze).count() == before
    assert _count(stream, "KS1") == 3


def test_later_messages_are_picked_up_and_malformed_ones_are_quarantined(stream):
    _drop(stream, [_msg(stream, order_id="KS2-0"), b"%%%", _msg(stream, order_id="KS2-1")], first_offset=3,
          start=BASE + timedelta(hours=2))
    stream.k_run()
    assert _count(stream, "KS2") == 2
    malformed = stream.spark.table(f"{stream.catalog}.silver.quarantine_orders").filter(
        "array_contains(quarantine_reasons, 'malformed_message')").count()
    assert malformed >= 1


def test_the_health_gate_recognises_kafka_runs(stream):
    """A Kafka run must count as a daily run for the valid-rate gate and its Bronze must feed the recompute."""
    stream.run("5_quality/1_run_quality_checks.py")
    results = {r["check_name"]: r["status"] for r in stream.spark.table(f"{stream.catalog}.gold.vw_data_health").collect()}
    assert results["valid_rate"] == "pass"
    assert results["bronze_gold_row_count"] == "pass" and results["bronze_gold_quantity"] == "pass"

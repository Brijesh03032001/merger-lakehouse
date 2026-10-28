#!/usr/bin/env python3
"""Replay a synthetic merger scenario's orders into a Kafka topic as JSON messages (demo and test only).

    make kafka-up
    python scripts/produce_orders_to_kafka.py --bootstrap localhost:9092 --topic orders

Each message is `{"order_id", "order_placement_date", "customer_id", "product_id", "order_qty"}` keyed by
order id. About 1% are unparseable and about 5% carry an extra `promo_code`, like a real feed would. The
rows keep the landing-zone mess (five date formats, bad ids and quantities, duplicates).

It writes through Spark's Kafka sink, so it needs the connector, which is downloaded on first run.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local_lakehouse import build_spark
from local_lakehouse.synthetic import build_scenario, order_messages

KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="localhost:9092", help="bootstrap servers, host:port[,host:port]")
    parser.add_argument("--topic", default="orders")
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--rows-per-day", type=int, default=300)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--malformed-rate", type=float, default=0.01)
    parser.add_argument("--extra-field-rate", type=float, default=0.05)
    parser.add_argument("--dry-run", action="store_true", help="print a sample and exit without touching Kafka")
    args = parser.parse_args()

    scenario = build_scenario(seed=args.seed, days=args.days, rows_per_day=args.rows_per_day)
    messages = order_messages(scenario, seed=args.seed, malformed_rate=args.malformed_rate, extra_field_rate=args.extra_field_rate)
    if args.dry_run:
        for key, value in messages[:5]:
            print(key, value)
        print(f"... {len(messages):,} messages would be sent to {args.bootstrap} / {args.topic}")
        return 0

    spark = build_spark(extra_packages=[KAFKA_CONNECTOR], app_name="produce-orders")
    (
        spark.createDataFrame(messages, "key string, value string")
        .write.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("topic", args.topic)
        .save()
    )
    print(f"sent {len(messages):,} messages to {args.bootstrap} / {args.topic}")
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

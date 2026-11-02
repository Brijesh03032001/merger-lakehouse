#!/usr/bin/env python3
"""Local scale benchmark of the real pipeline notebooks on open-source Spark + Delta Lake.

What it measures (all wall-clock, one machine, local mode - not Databricks):
  1. full historical load of N synthetic order rows (the legacy batch notebook)
  2. one daily increment through the batch notebook and through the Auto Loader notebook
  3. the same micro-batch MERGE with and without the date bound on the MERGE condition
  4. file counts, a selective query and the micro-batch before/after OPTIMIZE ... ZORDER BY

Usage:
    python benchmarks/run_benchmark.py --rows 5000000 --output benchmarks/results/local-5m.json

The orders are synthetic and calibrated to the error rates of the project's documented landing
zone (about 7% duplicates, 10% bad customer ids, 5% bad quantities). Nothing here is production data.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pyspark.sql import functions as F  # noqa: E402

from local_lakehouse import LOCAL_CATALOG, build_spark, init_local_catalog, run_notebook  # noqa: E402
from local_lakehouse.synthetic import build_scenario, write_landing_zone  # noqa: E402

PIPELINE = ROOT / "consolidated_pipeline"
HISTORY_START, HISTORY_DAYS = date(2025, 7, 1), 151
DATE_FORMATS = ["EEEE, MMMM dd, yyyy", "yyyy/MM/dd", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy-MM-dd"]


def generate_orders(spark, *, rows, out_dir, files, customers, products, start, days, prefix, seed):
    """Spark-generated order CSVs with the messy shapes the quality contract exists for."""
    base_rows = int(rows / 1.07)
    h = lambda salt: F.abs(F.hash(F.col("id"), F.lit(seed), F.lit(salt)))
    frame = spark.range(base_rows)
    day = F.date_add(F.lit(start), (h(1) % days).cast("int"))
    formats = F.when(h(2) % 5 == 0, F.date_format(day, DATE_FORMATS[0]))
    for index, pattern in enumerate(DATE_FORMATS[1:], start=1):
        formats = formats.when(h(2) % 5 == index, F.date_format(day, pattern))
    customer_ids = F.array(*[F.lit(c) for c in customers])
    product_ids = F.array(*[F.lit(p) for p in products])
    # Numeric-only bad quantities: text such as "N/A" makes the legacy batch notebooks' inferSchema
    # Bronze incompatible with their own incremental cast, which is a separate, tested defect.
    bad_quantities = F.array(F.lit(""), F.lit("0"), F.lit("-5"))
    orders = frame.select(
        F.concat(F.lit(prefix), F.date_format(day, "yyMMdd"), F.lpad(F.col("id").cast("string"), 9, "0")).alias("order_id"),
        formats.alias("order_placement_date"),
        F.when(h(3) % 100 < 10, F.lit("INVALID")).otherwise(F.element_at(customer_ids, (h(4) % len(customers) + 1).cast("int"))).alias("customer_id"),
        F.element_at(product_ids, (h(5) % len(products) + 1).cast("int")).alias("product_id"),
        F.when(h(6) % 100 < 5, F.element_at(bad_quantities, (h(7) % 3 + 1).cast("int")))
        .otherwise((h(7) % 120 + 1).cast("double").cast("string")).alias("order_qty"),
    )
    duplicates = orders.sample(0.07, seed=seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (
        orders.unionByName(duplicates).repartition(files)
        .write.mode("append").option("header", True).csv(str(out))
    )
    return base_rows


def notebook(spark, relative, root, **parameters):
    merged = {"catalog": LOCAL_CATALOG, "source_root": str(root), **parameters}
    return run_notebook(PIPELINE / relative, spark=spark, parameters=merged)


def table_stats(spark, table):
    detail = spark.sql(f"DESCRIBE DETAIL {LOCAL_CATALOG}.{table}").first()
    return {"files": detail["numFiles"], "size_mb": round(detail["sizeInBytes"] / 1_048_576, 1),
            "rows": spark.table(f"{LOCAL_CATALOG}.{table}").count()}


def median_seconds(action, runs):
    samples = []
    for _ in range(runs):
        started = time.perf_counter()
        action()
        samples.append(time.perf_counter() - started)
    return {"median_s": round(statistics.median(samples), 3), "min_s": round(min(samples), 3),
            "runs": [round(x, 3) for x in samples]}


def environment(args):
    memory = None
    if platform.system() == "Darwin":
        memory = int(subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True).stdout or 0)
    import delta
    import pyspark

    return {
        "machine": platform.machine(), "system": platform.platform(), "cpu_count": os.cpu_count(),
        "memory_gb": round(memory / 1024**3) if memory else None,
        "pyspark": pyspark.__version__, "delta_spark": getattr(delta, "__version__", "4.0.1"),
        "spark_master": "local[*]", "driver_memory": args.driver_memory,
        "shuffle_partitions": args.shuffle_partitions,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=1_000_000, help="historical order rows (before the 7%% duplicates)")
    parser.add_argument("--daily-rows", type=int, default=50_000, help="rows in the daily-increment comparison")
    parser.add_argument("--micro-rows", type=int, default=5_000, help="rows per micro-batch in the MERGE experiments")
    parser.add_argument("--workdir", type=Path, default=ROOT / "benchmarks" / "data")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    parser.add_argument("--optimize-target-mb", type=int, default=8,
                        help="OPTIMIZE output file size; the 1 GB default would fold these tables into one file")
    parser.add_argument("--keep", action="store_true", help="keep the generated lakehouse afterwards")
    args = parser.parse_args()

    shutil.rmtree(args.workdir, ignore_errors=True)
    args.workdir.mkdir(parents=True)
    spark = build_spark(args.workdir / "warehouse", driver_memory=args.driver_memory,
                        shuffle_partitions=args.shuffle_partitions, app_name="fmcg-benchmark")
    init_local_catalog(spark)
    landing = args.workdir / "landing"
    results: dict = {"environment": environment(args), "history_rows_requested": args.rows}

    scenario = build_scenario(seed=11, days=0)
    write_landing_zone(scenario, landing)
    customers = [c["customer_id"] for c in scenario.child_customers]
    products = [p["product_id"] for p in scenario.child_products]
    for relative, extra in (
        ("1_setup/seed_parent_demo_masters.py", {}),
        ("1_setup/dim_date_table_creation.py", {}),
        ("2_dimension_data_processing/1_customer_data_processing.py", {"data_source": "customers"}),
        ("2_dimension_data_processing/2_products_data_processing.py", {"data_source": "products"}),
        ("2_dimension_data_processing/3_pricing_data_processing.py", {"data_source": "gross_price"}),
    ):
        notebook(spark, relative, landing, **extra)

    gen = dict(customers=customers, products=products)
    started = time.perf_counter()
    base = generate_orders(spark, rows=args.rows, out_dir=landing / "orders" / "landing", files=HISTORY_DAYS,
                           start=HISTORY_START, days=HISTORY_DAYS, prefix="HIS", seed=1, **gen)
    results["generate_seconds"] = round(time.perf_counter() - started, 1)
    print(f"generated ~{base:,} base orders (+7% duplicates) in {results['generate_seconds']}s", flush=True)

    # 1. full historical load -------------------------------------------------------------
    full = notebook(spark, "3_fact_data_processing/1_full_load_fact.py", landing, data_source="orders")
    slowest = sorted(full.cell_seconds, key=lambda cell: -cell[1])[:5]
    results["full_load"] = {
        "seconds": round(full.seconds, 1),
        "slowest_cells": [{"seconds": round(s, 1), "starts_with": text} for _, s, text in slowest],
        "tables": {t: table_stats(spark, t) for t in ("bronze.orders", "silver.orders", "silver.quarantine_orders", "gold.sb_fact_orders", "gold.fact_orders")},
    }
    raw_rows = results["full_load"]["tables"]["bronze.orders"]["rows"]
    results["full_load"]["rows_per_second"] = round(raw_rows / full.seconds)
    print(f"full load: {raw_rows:,} raw rows in {full.seconds:.1f}s ({results['full_load']['rows_per_second']:,} rows/s)", flush=True)

    # 2. one daily increment, two ingestion paths ----------------------------------------
    next_day = HISTORY_START + timedelta(days=HISTORY_DAYS + 1)
    daily = {}
    generate_orders(spark, rows=args.daily_rows, out_dir=landing / "orders" / "landing", files=1,
                    start=next_day, days=1, prefix="BAT", seed=2, **gen)
    batch = notebook(spark, "3_fact_data_processing/2_incremental_load_fact.py", landing, data_source="orders")
    daily["batch_notebook_seconds"] = round(batch.seconds, 1)

    warm = landing / "orders" / "landing"
    generate_orders(spark, rows=1_000, out_dir=warm, files=1, start=next_day, days=1, prefix="WRM", seed=3, **gen)
    notebook(spark, "3_fact_data_processing/3_autoloader_orders.py", landing, reader="csv", data_source="orders")  # warm-up
    generate_orders(spark, rows=args.daily_rows, out_dir=warm, files=1, start=next_day + timedelta(days=1), days=1,
                    prefix="ALD", seed=4, **gen)
    stream = notebook(spark, "3_fact_data_processing/3_autoloader_orders.py", landing, reader="csv", data_source="orders")
    daily["autoloader_notebook_seconds"] = round(stream.seconds, 1)
    daily["full_load_seconds"] = results["full_load"]["seconds"]
    daily["rows_in_daily_batch"] = args.daily_rows
    daily["incremental_vs_full_reload"] = round(results["full_load"]["seconds"] / stream.seconds, 1)
    results["daily_increment"] = daily
    print(f"daily increment ({args.daily_rows:,} rows): batch notebook {batch.seconds:.1f}s, "
          f"Auto Loader notebook {stream.seconds:.1f}s, full reload {full.seconds:.1f}s", flush=True)

    # 3 + 4. micro-batch MERGE experiments ---------------------------------------------------
    import consolidated_pipeline.common.order_ingestion as oi

    counter = {"n": 0}

    def micro_batch(use_bounds):
        counter["n"] += 1
        directory = args.workdir / "micro" / f"m{counter['n']}"
        generate_orders(spark, rows=args.micro_rows, out_dir=directory, files=1,
                        start=HISTORY_START + timedelta(days=HISTORY_DAYS + 10 + counter["n"]), days=1,
                        prefix=f"MB{counter['n']:03d}", seed=100 + counter["n"], **gen)
        frame = oi.read_raw_batch(spark, str(directory / "*.csv"))
        started = time.perf_counter()
        oi.process_order_batch(spark, frame, catalog=LOCAL_CATALOG, use_merge_bounds=use_bounds)
        return time.perf_counter() - started

    def merge_samples(use_bounds, runs=3):
        samples = [micro_batch(use_bounds) for _ in range(runs)]
        return {"median_s": round(statistics.median(samples), 2), "runs": [round(x, 2) for x in samples]}

    mid_customer, window = customers[len(customers) // 2], (HISTORY_START + timedelta(days=60), HISTORY_START + timedelta(days=74))
    query = (f"SELECT SUM(sold_quantity) FROM {LOCAL_CATALOG}.gold.sb_fact_orders "
             f"WHERE customer_code = '{mid_customer}' AND date BETWEEN '{window[0]}' AND '{window[1]}'")

    def tables():
        return {t: table_stats(spark, t) for t in ("silver.orders", "gold.sb_fact_orders")}

    def files_read():
        """Files Delta still has to open after data skipping for the selective query."""
        return len(spark.sql(query).inputFiles())

    before = {"tables": tables(), "selective_query": median_seconds(lambda: spark.sql(query).collect(), 5),
              "files_read_by_selective_query": files_read()}
    before["micro_batch_merge_without_bounds"] = merge_samples(False)
    before["micro_batch_merge_with_bounds"] = merge_samples(True)
    results["before_optimize"] = before
    print("before optimize:", json.dumps({k: v for k, v in before.items() if k != "tables"}), flush=True)
    print("before tables:", json.dumps(before["tables"]), flush=True)

    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", str(args.optimize_target_mb * 1024 * 1024))
    optimize = {}
    for table, columns in (("silver.orders", "order_placement_date, customer_id"), ("gold.sb_fact_orders", "date, customer_code")):
        started = time.perf_counter()
        spark.sql(f"OPTIMIZE {LOCAL_CATALOG}.{table} ZORDER BY ({columns})")
        optimize[table] = {"zorder_by": columns, "seconds": round(time.perf_counter() - started, 1)}
    after = {"optimize": optimize, "optimize_target_mb": args.optimize_target_mb, "tables": tables(),
             "selective_query": median_seconds(lambda: spark.sql(query).collect(), 5),
             "files_read_by_selective_query": files_read()}
    after["micro_batch_merge_without_bounds"] = merge_samples(False)
    after["micro_batch_merge_with_bounds"] = merge_samples(True)
    results["after_optimize"] = after
    print("after optimize:", json.dumps({k: v for k, v in after.items() if k != "tables"}), flush=True)
    print("after tables:", json.dumps(after["tables"]), flush=True)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    print(f"wrote {args.output}")
    spark.stop()
    if not args.keep:
        shutil.rmtree(args.workdir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

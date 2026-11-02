"""Fixtures that run the real notebooks on local Spark + Delta Lake against synthetic data."""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

try:
    import delta  # noqa: F401
except ImportError:  # the light CI job installs only requirements-dev.txt
    collect_ignore_glob = ["test_*.py"]

KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"  # matches pyspark 4.0.1 / Scala 2.13
ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "consolidated_pipeline"

DIMENSION_NOTEBOOKS = (
    ("2_dimension_data_processing/1_customer_data_processing.py", "customers"),
    ("2_dimension_data_processing/2_products_data_processing.py", "products"),
    ("2_dimension_data_processing/3_pricing_data_processing.py", "gross_price"),
)


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("delta", reason="pip install -r requirements-spark.txt")
    from local_lakehouse import build_spark, init_local_catalog

    # The Kafka connector is only downloaded when a real broker is configured for tests/spark/test_kafka_broker.py.
    packages = [KAFKA_CONNECTOR] if os.environ.get("KAFKA_BOOTSTRAP_SERVERS") else []
    session = build_spark(tmp_path_factory.mktemp("warehouse"), extra_packages=packages)
    init_local_catalog(session)
    yield session
    session.stop()


def make_runner(spark, catalog: str, source_root: Path):
    from local_lakehouse import run_notebook

    def run(notebook: str, **parameters):
        merged = {"catalog": catalog, "source_root": str(source_root), **parameters}
        return run_notebook(PIPELINE / notebook, spark=spark, parameters=merged)

    return run


@pytest.fixture(scope="session")
def pipeline(spark, tmp_path_factory):
    """A small merger scenario: parent seeded, then the child's dimensions and full fact load."""
    from local_lakehouse import LOCAL_CATALOG
    from local_lakehouse.synthetic import build_scenario, write_landing_zone

    scenario = build_scenario(seed=7, days=20, rows_per_day=200)
    root = write_landing_zone(scenario, tmp_path_factory.mktemp("landing"))
    run = make_runner(spark, LOCAL_CATALOG, root)

    # The parent company's masters exist before the acquired company is loaded.
    run("1_setup/seed_parent_demo_masters.py")
    run("1_setup/dim_date_table_creation.py")
    for notebook, data_source in DIMENSION_NOTEBOOKS:
        run(notebook, data_source=data_source)
    run("3_fact_data_processing/1_full_load_fact.py", data_source="orders")

    return SimpleNamespace(spark=spark, catalog=LOCAL_CATALOG, root=root, scenario=scenario, run=run)

"""Local Spark + Delta Lake session builder."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Sequence

_JAVA_CANDIDATES = (
    "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
    "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
    "/usr/lib/jvm/java-21-openjdk-amd64",
    "/usr/lib/jvm/java-17-openjdk-amd64",
)


def _ensure_java() -> None:
    """Spark 4 needs Java 17+; a stale default ``java`` on PATH is common on macOS."""
    if os.environ.get("JAVA_HOME"):
        return
    for candidate in _JAVA_CANDIDATES:
        if (Path(candidate) / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = candidate
            return


def build_spark(
    warehouse: str | Path | None = None,
    *,
    app_name: str = "fmcg-local",
    master: str = "local[*]",
    driver_memory: str = "2g",
    shuffle_partitions: int = 8,
    extra_packages: Sequence[str] = (),
):
    """Create (or reuse) a Delta-enabled local session.

    ``driver_memory`` only takes effect for the first session in a process, as do ``extra_packages``
    (Maven coordinates such as ``org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1``).
    """
    _ensure_java()
    # Workers must run the driver's interpreter or PySpark raises PYTHON_VERSION_MISMATCH.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        # Databricks JVMs run in UTC. A different JVM default skews Delta time travel against the session zone.
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    if warehouse is not None:
        builder = builder.config("spark.sql.warehouse.dir", str(Path(warehouse).resolve()))
    spark = configure_spark_with_delta_pip(builder, extra_packages=list(extra_packages) or None).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

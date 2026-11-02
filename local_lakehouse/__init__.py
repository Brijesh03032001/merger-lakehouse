"""Run the Databricks notebooks locally on open-source Spark and Delta Lake.

This package exists so the pipeline code can be tested and benchmarked without a
Databricks workspace. It stubs ``dbutils``, ``%run`` and notebook widgets; it
does not emulate Unity Catalog, Auto Loader or Lakeflow Jobs.
"""

from .notebook_runner import LOCAL_CATALOG, init_local_catalog, run_notebook
from .session import build_spark

__all__ = ["LOCAL_CATALOG", "build_spark", "init_local_catalog", "run_notebook"]

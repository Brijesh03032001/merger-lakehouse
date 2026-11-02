import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FULL_LOAD = ROOT / "consolidated_pipeline/3_fact_data_processing/1_full_load_fact.py"
INCREMENTAL = ROOT / "consolidated_pipeline/3_fact_data_processing/2_incremental_load_fact.py"
QUALITY_MODULE = ROOT / "consolidated_pipeline/common/order_quality.py"
SCD2_MODULE = ROOT / "consolidated_pipeline/common/scd2.py"
CUSTOMERS = ROOT / "consolidated_pipeline/2_dimension_data_processing/1_customer_data_processing.py"
PRODUCTS = ROOT / "consolidated_pipeline/2_dimension_data_processing/2_products_data_processing.py"
PRICING = ROOT / "consolidated_pipeline/2_dimension_data_processing/3_pricing_data_processing.py"


def test_databricks_sources_are_valid_python():
    for path in (FULL_LOAD, INCREMENTAL, QUALITY_MODULE, SCD2_MODULE, CUSTOMERS, PRODUCTS, PRICING):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")


def test_both_order_paths_persist_quarantine_and_metrics():
    for path in (FULL_LOAD, INCREMENTAL):
        source = path.read_text(encoding="utf-8")
        assert "%run ../common/order_quality" in source
        assert "split_valid_and_quarantined" in source
        assert "persist_quality_results" in source
        assert "quarantine_orders" in source
        assert "order_quality_metrics" in source
        assert '.otherwise("999999")' not in source


def test_quality_module_defines_stable_reason_codes():
    source = QUALITY_MODULE.read_text(encoding="utf-8")
    expected_codes = {
        "missing_order_id",
        "invalid_order_date",
        "invalid_customer_id",
        "invalid_product_id",
        "invalid_order_qty",
        "duplicate_record",
        "unknown_product_id",
    }
    assert all(code in source for code in expected_codes)


def test_customer_and_product_notebooks_write_scd2_history():
    expectations = {
        CUSTOMERS: ("customer_id", "sb_dim_customers_history"),
        PRODUCTS: ("product_id", "sb_dim_products_history"),
    }
    for path, (business_key, history_table) in expectations.items():
        source = path.read_text(encoding="utf-8")
        assert "%run ../common/scd2" in source
        assert "apply_scd2(" in source
        assert f'business_key="{business_key}"' in source
        assert history_table in source


def test_scd2_runtime_checks_single_current_version_invariant():
    source = SCD2_MODULE.read_text(encoding="utf-8")
    assert "_assert_single_current_version" in source
    assert "multiple current versions" in source


def test_notebooks_do_not_depend_on_personal_workspace_paths():
    for path in ROOT.glob("consolidated_pipeline/**/*.py"):
        source = path.read_text(encoding="utf-8")
        assert "/Workspace/Users/" not in source


def test_ingestion_notebooks_accept_portable_source_root():
    ingestion_paths = (FULL_LOAD, INCREMENTAL, CUSTOMERS, PRODUCTS, PRICING)
    for path in ingestion_paths:
        source = path.read_text(encoding="utf-8")
        assert 'dbutils.widgets.text("source_root"' in source
        assert "source_bucket" not in source


def test_shared_run_targets_are_databricks_notebooks():
    for path in (QUALITY_MODULE, SCD2_MODULE):
        assert path.read_text(encoding="utf-8").startswith("# Databricks notebook source\n")


def test_magic_cells_begin_at_a_command_boundary():
    for path in ROOT.glob("consolidated_pipeline/**/*.py"):
        source = path.read_text(encoding="utf-8")
        for command in source.split("# COMMAND ----------"):
            meaningful = [line for line in command.splitlines() if line.strip()]
            magic_lines = [line for line in meaningful if line.startswith("# MAGIC")]
            if magic_lines:
                assert meaningful[0].startswith("# MAGIC"), path



def test_every_notebook_and_shared_module_is_valid_python():
    for path in ROOT.glob("consolidated_pipeline/**/*.py"):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")


def test_every_run_target_exists():
    for path in ROOT.glob("consolidated_pipeline/**/*.py"):
        for target in re.findall(r"^# MAGIC %run (\S+)", path.read_text(encoding="utf-8"), re.M):
            resolved = (path.parent / target).resolve()
            assert (resolved if resolved.suffix else resolved.with_suffix(".py")).is_file(), f"{path.name} -> {target}"


def test_shared_modules_do_not_import_each_other_as_packages_inside_notebooks():
    """%run shares one namespace; a module may only fall back to a package import behind a NameError guard."""
    for path in (ROOT / "consolidated_pipeline/common").glob("*.py"):
        source = path.read_text(encoding="utf-8")
        for match in re.finditer(r"^\s*from consolidated_pipeline", source, re.M):
            before = source[: match.start()]
            assert "except NameError" in before[-200:], f"{path.name} imports a sibling outside a NameError guard"

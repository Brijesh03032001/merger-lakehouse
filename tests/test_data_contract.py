"""The orders data contract, the Spark code and the deployed defaults must agree."""

import re
from pathlib import Path

import yaml

from data_quality.orders import DATE_FORMATS, REQUIRED_COLUMNS

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = yaml.safe_load((ROOT / "contracts/orders.yml").read_text(encoding="utf-8"))
BUNDLE = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
QUALITY_SOURCE = (ROOT / "consolidated_pipeline/common/order_quality.py").read_text(encoding="utf-8")
INGESTION_SOURCE = (ROOT / "consolidated_pipeline/common/order_ingestion.py").read_text(encoding="utf-8")
GATE_NOTEBOOK = (ROOT / "consolidated_pipeline/5_quality/1_run_quality_checks.py").read_text(encoding="utf-8")


def _widget_default(source, name):
    return re.search(rf'dbutils\.widgets\.text\("{name}", "([^"]*)"', source).group(1)


def test_required_columns_match_the_preflight_audit_and_the_ingestion_code():
    assert set(CONTRACT["required_columns"]) == REQUIRED_COLUMNS
    raw = re.search(r"RAW_COLUMNS = \[(.*?)\]", INGESTION_SOURCE, re.S).group(1)
    assert re.findall(r'"(\w+)"', raw) == CONTRACT["required_columns"]


def test_reason_codes_match_the_spark_contract():
    in_code = set(re.findall(r'F\.lit\("([a-z_]+)"\)', QUALITY_SOURCE))
    assert in_code == set(CONTRACT["reason_codes"]), "add new reason codes to contracts/orders.yml"


def test_date_formats_match_the_python_and_spark_parsers():
    spark_formats = re.findall(r'_try_date\(text, "([^"]+)"\)', QUALITY_SOURCE)
    assert spark_formats == CONTRACT["rules"]["date_formats"]
    to_strptime = lambda pattern: (
        pattern.replace("yyyy", "%Y").replace("MMMM", "%B").replace("MM", "%m").replace("dd", "%d")
    )
    assert [to_strptime(p) for p in spark_formats] == [f for f in DATE_FORMATS]


def test_deployed_thresholds_equal_the_contract():
    assert float(BUNDLE["variables"]["min_valid_rate"]["default"]) == CONTRACT["quality_gate"]["min_valid_rate"]
    assert float(_widget_default(GATE_NOTEBOOK, "min_valid_rate")) == CONTRACT["quality_gate"]["min_valid_rate"]
    assert float(BUNDLE["variables"]["max_data_age_hours"]["default"]) == CONTRACT["freshness"]["max_age_hours"]
    assert float(_widget_default(GATE_NOTEBOOK, "max_age_hours")) == CONTRACT["freshness"]["max_age_hours"]


def test_quantity_ceiling_is_off_everywhere_unless_the_contract_sets_it():
    assert CONTRACT["rules"]["max_order_qty"] is None
    assert BUNDLE["variables"]["max_order_qty"]["default"] == ""
    assert _widget_default(GATE_NOTEBOOK, "max_order_qty") == ""

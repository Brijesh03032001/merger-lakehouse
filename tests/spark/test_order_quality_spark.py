"""The PySpark order contract, exercised on a real local Spark session."""

from datetime import date

import pytest

pytestmark = pytest.mark.spark

SCHEMA = (
    "order_id string, order_placement_date string, customer_id string, product_id string, "
    "order_qty string, read_timestamp timestamp, file_name string"
)


@pytest.fixture(scope="module")
def contract():
    from consolidated_pipeline.common.order_quality import split_valid_and_quarantined

    return split_valid_and_quarantined


def _orders(spark, rows):
    from datetime import datetime

    stamped = [(*row, datetime(2026, 1, 1, 0, 0, index), f"f{index}.csv") for index, row in enumerate(rows)]
    return spark.createDataFrame(stamped, SCHEMA)


def _products(spark):
    return spark.createDataFrame([("1001", "code-1001"), ("1002", "code-1002")], "product_id string, product_code string")


def _reasons(frame):
    return {row["order_id"]: sorted(row["quarantine_reasons"]) for row in frame.collect()}


def test_weekday_prefixed_and_alternate_date_formats_normalise(spark, contract):
    rows = [
        ("A1", "Saturday, December 27, 2025", "501", "1001", "2"),
        ("A2", "27-12-2025", "501", "1001", "3"),
        ("A3", "2025/12/27", "501", "1001", "4"),
        ("A4", "27/12/2025", "501", "1001", "5"),
        ("A5", "2025-12-27", "501", "1001", "6"),
    ]
    valid, quarantined = contract(_orders(spark, rows), _products(spark))
    assert quarantined.count() == 0
    assert {row["order_placement_date"] for row in valid.collect()} == {date(2025, 12, 27)}


def test_each_violation_gets_its_stable_reason_code(spark, contract):
    rows = [
        ("", "2025/12/27", "501", "1001", "2"),
        ("B2", "not a date", "501", "1001", "2"),
        ("B3", "2025/12/27", "ABC", "1001", "2"),
        ("B4", "2025/12/27", "501", "xyz", "2"),
        ("B5", "2025/12/27", "501", "1001", "-3"),
        ("B6", "2025/12/27", "501", "1001", "0"),
        ("B7", "2025/12/27", "501", "9999", "2"),
    ]
    _, quarantined = contract(_orders(spark, rows), _products(spark))
    reasons = _reasons(quarantined)
    assert reasons[""] == ["missing_order_id"]
    assert reasons["B2"] == ["invalid_order_date"]
    assert reasons["B3"] == ["invalid_customer_id"]
    assert reasons["B4"] == ["invalid_product_id", "unknown_product_id"]
    assert reasons["B5"] == ["invalid_order_qty"]
    assert reasons["B6"] == ["invalid_order_qty"]
    assert reasons["B7"] == ["unknown_product_id"]


def test_non_numeric_quantity_is_quarantined_not_a_crash(spark, contract):
    """Regression: cast('double') on 'N/A' raised CAST_INVALID_INPUT under ANSI mode."""
    rows = [("C1", "2025/12/27", "501", "1001", "N/A"), ("C2", "2025/12/27", "501", "1001", "")]
    valid, quarantined = contract(_orders(spark, rows), _products(spark))
    assert valid.count() == 0
    assert _reasons(quarantined) == {"C1": ["invalid_order_qty"], "C2": ["invalid_order_qty"]}
    assert {row["raw_order_qty"] for row in quarantined.collect()} == {"N/A", ""}


def test_only_repeat_copies_are_quarantined_as_duplicates(spark, contract):
    row = ("D1", "2025/12/27", "501", "1001", "2")
    valid, quarantined = contract(_orders(spark, [row, row, row]), _products(spark))
    assert valid.count() == 1
    assert quarantined.count() == 2
    assert {tuple(r["quarantine_reasons"]) for r in quarantined.collect()} == {("duplicate_record",)}


def test_valid_rows_carry_no_internal_columns(spark, contract):
    valid, _ = contract(_orders(spark, [("E1", "2025/12/27", "501", "1001", "2")]), _products(spark))
    assert not [name for name in valid.columns if name.startswith("_") or name.startswith("raw_")]
    assert {"order_id", "order_placement_date", "customer_id", "product_id", "order_qty", "product_code"} <= set(
        valid.columns
    )

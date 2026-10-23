import csv

from data_quality.orders import audit_order_files, parse_order_date, validate_order


def _write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(
            target,
            fieldnames=[
                "order_id",
                "order_placement_date",
                "customer_id",
                "product_id",
                "order_qty",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_parse_order_date_supports_landing_zone_formats():
    expected = "2025-12-27"
    assert parse_order_date("2025/12/27") == expected
    assert parse_order_date("27-12-2025") == expected
    assert parse_order_date("27/12/2025") == expected
    assert parse_order_date("Saturday, December 27, 2025") == expected


def test_validate_order_returns_actionable_issue_codes():
    issues = validate_order(
        {
            "order_id": "",
            "order_placement_date": "not-a-date",
            "customer_id": "ABC987",
            "product_id": "",
            "order_qty": "-2",
        }
    )
    assert issues == [
        "missing_order_id",
        "invalid_order_date",
        "invalid_customer_id",
        "invalid_product_id",
        "invalid_order_qty",
    ]


def test_audit_reports_invalid_and_duplicate_rows(tmp_path):
    source = tmp_path / "orders.csv"
    valid = {
        "order_id": "FDEC829603603",
        "order_placement_date": "Saturday, December 27, 2025",
        "customer_id": "789603",
        "product_id": "25891603",
        "order_qty": "64.0",
    }
    invalid = {**valid, "order_id": "FDEC829603604", "customer_id": "INVALID"}
    _write_csv(source, [valid, valid, invalid])

    report = audit_order_files([source])

    assert report["rows_scanned"] == 3
    assert report["valid_rows"] == 1
    assert report["invalid_rows"] == 2
    assert report["duplicate_rows"] == 1
    assert report["issue_counts"] == {
        "duplicate_record": 1,
        "invalid_customer_id": 1,
    }


def test_audit_fails_closed_on_schema_drift(tmp_path):
    source = tmp_path / "orders.csv"
    source.write_text("order_id,customer_id\n1,2\n", encoding="utf-8")

    report = audit_order_files([source])

    assert report["rows_scanned"] == 0
    assert report["valid_rate"] == 0.0
    assert report["schema_failures"][0]["missing_columns"] == [
        "order_placement_date",
        "order_qty",
        "product_id",
    ]


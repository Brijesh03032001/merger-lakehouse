"""Order data-contract checks that run without a Databricks cluster.

The Databricks notebooks perform the distributed transformations.  This module
provides an inexpensive pre-flight check for landing-zone CSVs and a CI-testable
definition of the order contract.
"""

from __future__ import annotations

import csv
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping


REQUIRED_COLUMNS = {
    "order_id",
    "order_placement_date",
    "customer_id",
    "product_id",
    "order_qty",
}

DATE_FORMATS = (
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%B %d, %Y",
    "%Y-%m-%d",
)


def parse_order_date(value: object) -> str | None:
    """Normalize supported order dates to ISO-8601, or return ``None``."""
    text = str(value or "").strip()
    if not text:
        return None
    if "," in text:
        prefix, remainder = text.split(",", 1)
        if prefix.isalpha():
            text = remainder.strip()
    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(text, date_format).date().isoformat()
        except ValueError:
            continue
    return None


def _positive_number(value: object) -> bool:
    try:
        return float(str(value).strip()) > 0
    except (TypeError, ValueError):
        return False


def validate_order(row: Mapping[str, object]) -> list[str]:
    """Return stable issue codes for one raw order record."""
    issues: list[str] = []
    if not str(row.get("order_id") or "").strip():
        issues.append("missing_order_id")
    if parse_order_date(row.get("order_placement_date")) is None:
        issues.append("invalid_order_date")
    if not str(row.get("customer_id") or "").strip().isdigit():
        issues.append("invalid_customer_id")
    if not str(row.get("product_id") or "").strip().isdigit():
        issues.append("invalid_product_id")
    if not _positive_number(row.get("order_qty")):
        issues.append("invalid_order_qty")
    return issues


def _business_key(row: Mapping[str, object]) -> tuple[str, ...]:
    return (
        str(row.get("order_id") or "").strip(),
        parse_order_date(row.get("order_placement_date")) or "",
        str(row.get("customer_id") or "").strip(),
        str(row.get("product_id") or "").strip(),
        str(row.get("order_qty") or "").strip(),
    )


def audit_order_files(paths: Iterable[str | Path]) -> dict[str, object]:
    """Audit CSV files and return aggregate, machine-readable quality metrics."""
    files = [Path(path) for path in paths]
    issue_counts: Counter[str] = Counter()
    seen: set[tuple[str, ...]] = set()
    rows = valid_rows = duplicate_rows = 0
    schema_failures: list[dict[str, object]] = []

    for path in files:
        with path.open(newline="", encoding="utf-8-sig") as source:
            reader = csv.DictReader(source)
            actual_columns = set(reader.fieldnames or [])
            missing = sorted(REQUIRED_COLUMNS - actual_columns)
            if missing:
                schema_failures.append({"file": str(path), "missing_columns": missing})
                continue

            for row in reader:
                rows += 1
                issues = validate_order(row)
                key = _business_key(row)
                if key in seen:
                    duplicate_rows += 1
                    issues.append("duplicate_record")
                else:
                    seen.add(key)
                issue_counts.update(issues)
                if not issues:
                    valid_rows += 1

    invalid_rows = rows - valid_rows
    valid_rate = round(valid_rows / rows, 6) if rows else 0.0
    return {
        "files_scanned": len(files),
        "rows_scanned": rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "valid_rate": valid_rate,
        "duplicate_rows": duplicate_rows,
        "issue_counts": dict(sorted(issue_counts.items())),
        "schema_failures": schema_failures,
    }


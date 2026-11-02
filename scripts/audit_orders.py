#!/usr/bin/env python3
"""Audit raw order CSVs before a Databricks ingestion run."""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_quality import audit_order_files


def _paths(patterns: list[str]) -> list[Path]:
    matches: set[Path] = set()
    for pattern in patterns:
        matches.update(Path(item) for item in glob.glob(pattern, recursive=True))
    return sorted(path for path in matches if path.is_file() and path.suffix.lower() == ".csv")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("patterns", nargs="+", help="CSV paths or glob patterns")
    parser.add_argument("--output", type=Path, help="Optional JSON report path")
    parser.add_argument("--fail-under", type=float, default=0.0, help="Minimum valid-row rate")
    args = parser.parse_args()

    paths = _paths(args.patterns)
    if not paths:
        parser.error("no CSV files matched")
    report = audit_order_files(paths)
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return 0 if report["valid_rate"] >= args.fail_under else 2


if __name__ == "__main__":
    raise SystemExit(main())


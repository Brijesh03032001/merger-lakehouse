#!/usr/bin/env python3
"""Write a synthetic merger scenario in the landing-zone layout the notebooks read.

    python scripts/generate_demo_data.py demo_data
    ./scripts/upload_demo_data.sh fmcg-free fmcg_dev demo_data

Everything is generated from a seed. The overlap between the parent and acquired company, the
name noise and the data errors are injected on purpose; none of it is real company data.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from local_lakehouse.synthetic import build_scenario, write_ground_truth, write_landing_zone


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("output", type=Path, help="directory to create the landing zone in")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--days", type=int, default=151, help="one orders file per day")
    parser.add_argument("--rows-per-day", type=int, default=320)
    args = parser.parse_args()

    scenario = build_scenario(seed=args.seed, days=args.days, rows_per_day=args.rows_per_day)
    write_landing_zone(scenario, args.output)
    write_ground_truth(scenario, args.output / "ground_truth")
    orders = sum(len(batch["rows"]) for batch in scenario.orders)
    print(
        f"wrote {len(scenario.parent_customers)} parent and {len(scenario.child_customers)} acquired customers, "
        f"{len(scenario.parent_products)} parent and {len(scenario.child_products)} acquired products, "
        f"{orders:,} order rows in {len(scenario.orders)} files -> {args.output}"
    )
    print(f"ground truth (for evaluating entity matching) -> {args.output / 'ground_truth'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

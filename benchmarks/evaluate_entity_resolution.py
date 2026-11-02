#!/usr/bin/env python3
"""Entity-matching precision/recall on synthetic merger scenarios with known ground truth.

Three seed groups, in the order they were used:
  dev       weights and thresholds in MatchConfig were tuned on these
  inspected looked at after tuning; the failures here motivated two blocking keys (token phonetics, letter
            anagram). Optimistic, kept for transparency.
  fresh     never looked at before the last code change. This is the number to quote.
The labels come from the generator's own noise model, so these numbers describe how well the matcher
undoes *that* noise, not how it will do on real company data. Replace the labels with a hand-labelled
set (silver.entity_match_labels) to measure real data.
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from consolidated_pipeline.common import entity_resolution as er  # noqa: E402
from local_lakehouse.synthetic import build_scenario  # noqa: E402

DEV_SEEDS = [1, 2, 3]
INSPECTED_SEEDS = list(range(101, 111)) + list(range(301, 311))
FRESH_SEEDS = list(range(401, 421))
FIELDS = ("precision", "recall", "auto_resolved_share", "blocking_recall", "blocking_reduction_ratio")


def customers(scenario):
    parents = [er.Record(p["customer_code"], *er.split_name_city(p["customer"])) for p in scenario.parent_customers]
    children = [er.Record(c["customer_id"], c["customer_name"], c["city"]) for c in scenario.child_customers]
    return parents, children, {t["child_id"]: t["parent_code"] for t in scenario.customer_truth}, er.CUSTOMER_PROFILE


def products(scenario):
    parents = [er.Record(p["product_code"], p["product"], p["category"]) for p in scenario.parent_products]
    children = [er.Record(c["product_id"], c["product_name"], c["category"]) for c in scenario.child_products]
    return parents, children, {t["child_id"]: t["parent_code"] for t in scenario.product_truth}, er.PRODUCT_PROFILE


def summarise(entity, loader, seeds):
    runs = []
    for seed in seeds:
        parents, children, truth, profile = loader(build_scenario(seed=seed, days=1, rows_per_day=1))
        resolution = er.resolve(parents, children, profile, track_pool=True)
        metrics = er.evaluate(resolution, truth)
        metrics["missed_matches_in_review"] = (
            f"{metrics['true_matches_in_review']}/{metrics['true_matches'] - metrics['correct_matches']}"
        )
        runs.append(metrics)
    summary = {name: {"min": min(r[name] for r in runs), "mean": round(statistics.mean(r[name] for r in runs), 4),
                      "max": max(r[name] for r in runs)} for name in FIELDS}
    summary["seeds"] = seeds
    summary["true_matches"] = sum(r["true_matches"] for r in runs)
    summary["missed_matches_not_in_review"] = sum(
        (r["true_matches"] - r["correct_matches"]) - r["true_matches_in_review"] for r in runs
    )
    summary["wrong_auto_matches"] = sum(r["predicted_matches"] - r["correct_matches"] for r in runs)
    summary["children_per_run"] = runs[0]["children"]
    return summary


def main() -> int:
    report = {
        entity: {split: summarise(entity, loader, seeds) for split, seeds in (("dev", DEV_SEEDS), ("inspected", INSPECTED_SEEDS), ("fresh", FRESH_SEEDS))}
        for entity, loader in (("customers", customers), ("products", products))
    }
    out = ROOT / "benchmarks" / "results" / "entity-resolution.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(f"{'entity':10} {'split':8} {'precision (min/mean)':>22} {'recall (min/mean)':>20} {'auto-resolved':>14} {'blocking recall':>16}")
    for entity, splits in report.items():
        for split, s in splits.items():
            print(f"{entity:10} {split:8} {s['precision']['min']:>10.3f}/{s['precision']['mean']:<10.3f} "
                  f"{s['recall']['min']:>9.3f}/{s['recall']['mean']:<9.3f} {s['auto_resolved_share']['mean']:>14.3f} "
                  f"{s['blocking_recall']['min']:>16.3f}")
    for entity, splits in report.items():
        fresh = splits["fresh"]
        print(f"{entity} (fresh): {fresh['wrong_auto_matches']} wrong auto-matches and "
              f"{fresh['missed_matches_not_in_review']} of {fresh['true_matches']} true matches never reaching the review queue")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

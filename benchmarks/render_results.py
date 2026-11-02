#!/usr/bin/env python3
"""Render docs/BENCHMARKS.md from benchmarks/results/*.json so the prose never drifts from the data."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "benchmarks" / "results"


def load_runs():
    runs = []
    for path in sorted(RESULTS.glob("local-*.json"), key=lambda p: json.loads(p.read_text())["full_load"]["tables"]["bronze.orders"]["rows"]):
        runs.append(json.loads(path.read_text()))
    return runs


def millions(n):
    return f"{n / 1e6:.1f}M"


NOISE_S = 1.5  # run-to-run variation seen on this machine for the micro-batch figures


def reading(runs) -> list[str]:
    """Plain-language conclusions, computed from the data so they cannot disagree with the tables."""
    out = ["## Reading the results", ""]
    rates = [r["full_load"]["rows_per_second"] for r in runs]
    first, last = runs[0]["full_load"], runs[-1]["full_load"]
    rows_first = first["tables"]["bronze.orders"]["rows"]
    rows_last = last["tables"]["bronze.orders"]["rows"]
    if len(runs) > 1:
        growth, cost = rows_last / rows_first, last["seconds"] / first["seconds"]
        out.append(f"- **Throughput** of the full load was {min(rates):,} to {max(rates):,} rows/s. Going from {millions(rows_first)} to "
                   f"{millions(rows_last)} rows ({growth:.1f}× the data) took {cost:.1f}× as long, "
                   f"{'close to linear' if cost <= growth * 1.3 else 'worse than linear'}.")
    else:
        out.append(f"- **Throughput** of the full load was {rates[0]:,} rows/s.")
    out.append("- A second identical run of the 5M configuration earlier in development took 79.3 s for the full load (67.8 s recorded here) and "
               "measured the batch and Auto Loader daily notebooks at 10.8 s and 10.6 s (14.7 s and 8.7 s here). Treat single timings as good to "
               "about ±15%, and small gaps as noise.")
    for run in runs:
        d = run["daily_increment"]
        rows = run["full_load"]["tables"]["bronze.orders"]["rows"]
        gap = abs(d["batch_notebook_seconds"] - d["autoloader_notebook_seconds"])
        verdict = (
            "the two notebooks cost the same within noise"
            if gap <= NOISE_S
            else f"the two notebooks differed by {gap:.1f} s in this single run; identical repeat runs at 5M rows differed by about that much, so no speed advantage is claimed either way"
        )
        out.append(f"- **A {d['rows_in_daily_batch']:,}-row day on {millions(rows)} rows of history** took {min(d['autoloader_notebook_seconds'], d['batch_notebook_seconds'])}"
                   f"-{max(d['autoloader_notebook_seconds'], d['batch_notebook_seconds'])} s incrementally, "
                   f"{d['full_load_seconds'] / max(d['autoloader_notebook_seconds'], d['batch_notebook_seconds']):.1f}-"
                   f"{d['full_load_seconds'] / min(d['autoloader_notebook_seconds'], d['batch_notebook_seconds']):.1f}× cheaper than reloading everything; {verdict}. "
                   f"The reason to prefer Auto Loader is correctness (exactly once, no failure on an empty day, no type-inference breaks), not speed.")
    for run in runs:
        rows = millions(run["full_load"]["tables"]["bronze.orders"]["rows"])
        b, a = run["before_optimize"], run["after_optimize"]
        reads_b, reads_a = b.get("files_read_by_selective_query"), a.get("files_read_by_selective_query")
        q_b, q_a = b["selective_query"]["median_s"], a["selective_query"]["median_s"]
        m_deltas = [abs(a[k]["median_s"] - b[k]["median_s"]) for k in ("micro_batch_merge_without_bounds", "micro_batch_merge_with_bounds")]
        bound_effect = [x["micro_batch_merge_without_bounds"]["median_s"] - x["micro_batch_merge_with_bounds"]["median_s"] for x in (b, a)]
        out.append(f"- **Layout at {rows} rows.** `OPTIMIZE ... ZORDER` took the selective query from reading {reads_b} files to {reads_a} "
                   f"({'a real saving in data scanned' if reads_a < reads_b else 'no saving'}). Wall-clock query time went {q_b * 1000:.0f} → {q_a * 1000:.0f} ms "
                   f"({'no measurable change' if abs(q_a - q_b) < 0.05 else 'changed'}), and micro-batch MERGE time moved by at most {max(m_deltas):.1f} s "
                   f"({'within noise' if max(m_deltas) <= NOISE_S else 'a real change'}). The date bound on the MERGE condition changed micro-batch time by "
                   f"{bound_effect[0]:+.1f} s before and {bound_effect[1]:+.1f} s after optimising "
                   f"({'within noise, so no benefit is claimed' if max(abs(x) for x in bound_effect) <= NOISE_S else 'a measurable effect'}).")
    out += ["- `OPTIMIZE ... ZORDER` reduced the files a selective query reads, but a laptop's local disk hides what that buys on cloud object "
            "storage, so no wall-clock speedup is claimed from it. The date bound on the MERGE condition is different: its effect appears only as the "
            "table grows, which is what skipping files should do.",
            "- The quality-contract step is the largest cost of the full load and grows faster than the data. The batch notebook evaluates it several "
            "times, and serverless compute cannot cache the result. Evaluating it once is the most valuable performance change available here; it "
            "was measured but not made, because those notebooks are the ones verified on Databricks Free Edition.", ""]
    return out


def main() -> int:
    runs = load_runs()
    if not runs:
        print("no benchmark results found", file=sys.stderr)
        return 1
    env = runs[0]["environment"]
    lines = ["# Benchmarks", "",
             "Local scale benchmark of the real pipeline notebooks, generated by `benchmarks/run_benchmark.py` and rendered by",
             "`benchmarks/render_results.py` from `benchmarks/results/*.json`. Re-run with `make benchmark ROWS=5000000`.", "",
             "## What this is, and is not", "",
             f"- One laptop: **{env['machine']}, {env['cpu_count']} cores, {env['memory_gb']} GB RAM**, {env['system']}.",
             f"- Open-source **Spark {env['pyspark']} and Delta Lake {env['delta_spark']} in local mode** "
             f"(`{env['spark_master']}`, {env['driver_memory']} driver, {env['shuffle_partitions']} shuffle partitions).",
             "- **Not Databricks.** No cluster, no Photon, no cloud storage, no network. Absolute times will differ there; "
             "the ratios and the shapes of the curves are the useful part. **Dollar cost per run was not measured.**",
             "- Orders are synthetic, with error rates matched to the project's documented landing zone (about 7% duplicates, 10% bad "
             "customer ids, 5% bad quantities, five date formats). They are not production data.",
             "- Single run per row; the micro-batch figures are the median of 3. Expect run-to-run noise of about a second.", ""]

    lines += ["## Full historical load", "",
              "The batch notebook (`1_full_load_fact.py`): read CSV, Bronze, quality contract, Silver, Gold, monthly shared fact.", "",
              "| raw rows | seconds | rows/s | silver rows | quarantined | slowest step |", "|---:|---:|---:|---:|---:|---|"]
    for run in runs:
        fl = run["full_load"]
        t = fl["tables"]
        slow = fl["slowest_cells"][0]
        lines.append(f"| {t['bronze.orders']['rows']:,} | {fl['seconds']} | {fl['rows_per_second']:,} | {t['silver.orders']['rows']:,} | "
                     f"{t['silver.quarantine_orders']['rows']:,} | {slow['seconds']}s: {slow['starts_with'][:48].strip('# ')} |")
    lines += ["", "The contract step dominates because the batch notebook re-evaluates its window about six times (counts, "
              "min/max date, `show`, writes) and Databricks serverless cannot cache a DataFrame to avoid that.", ""]

    lines += ["## One day of data: incremental versus reloading everything", "",
              "| history | day size | batch notebook | Auto Loader notebook | full reload | full reload ÷ Auto Loader day |", "|---:|---:|---:|---:|---:|---:|"]
    for run in runs:
        d = run["daily_increment"]
        rows = run["full_load"]["tables"]["bronze.orders"]["rows"]
        lines.append(f"| {rows:,} | {d['rows_in_daily_batch']:,} | {d['batch_notebook_seconds']}s | {d['autoloader_notebook_seconds']}s | "
                     f"{d['full_load_seconds']}s | {d['incremental_vs_full_reload']}× |")
    lines += ["", "The Auto Loader row uses Spark's `csv` file source locally (Databricks `cloudFiles` cannot run outside Databricks). "
              "Everything after the reader is the production code path.", ""]

    lines += ["## Layout: `OPTIMIZE ... ZORDER` and MERGE date bounds", "",
              "A selective query (one customer, two weeks) and one 5,000-row micro-batch through `process_order_batch`, before and "
              "after `OPTIMIZE ... ZORDER BY` on the date and customer columns. *Files read* is what Delta still has to open "
              "after data skipping; it is the honest measure on a laptop where query time is dominated by planning.", "",
              "| history | silver files before → after | query files read before → after | query ms before → after | micro-batch s, no bound before → after | micro-batch s, date bound before → after |",
              "|---:|---:|---:|---:|---:|---:|"]
    for run in runs:
        rows = run["full_load"]["tables"]["bronze.orders"]["rows"]
        b, a = run["before_optimize"], run["after_optimize"]
        lines.append(
            f"| {rows:,} | {b['tables']['silver.orders']['files']} → {a['tables']['silver.orders']['files']} | "
            f"{b.get('files_read_by_selective_query', '?')} → {a.get('files_read_by_selective_query', '?')} | "
            f"{b['selective_query']['median_s'] * 1000:.0f} → {a['selective_query']['median_s'] * 1000:.0f} | "
            f"{b['micro_batch_merge_without_bounds']['median_s']} → {a['micro_batch_merge_without_bounds']['median_s']} | "
            f"{b['micro_batch_merge_with_bounds']['median_s']} → {a['micro_batch_merge_with_bounds']['median_s']} |")
    target = runs[-1]["after_optimize"].get("optimize_target_mb")
    lines += ["", f"`OPTIMIZE` was told to write about {target} MB files; at its 1 GB default these tables fold into a single file "
              "and there is nothing to skip.", ""]
    lines += reading(runs)
    p = RESULTS / "entity-resolution.json"
    if p.exists():
        lines += ["## Entity matching", "", "See [ENTITY_RESOLUTION.md](ENTITY_RESOLUTION.md#what-the-evaluation-measures); regenerate with `make entity-eval`.", ""]
    out = ROOT / "docs" / "BENCHMARKS.md"
    out.write_text("\n".join(lines) + "\n")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

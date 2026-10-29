# Databricks notebook source
"""Delta Lake I/O for entity resolution: read records, persist decisions, close the review loop."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

try:
    Record  # already defined when this runs via %run after entity_resolution
except NameError:
    from consolidated_pipeline.common.entity_resolution import (
        MATCHED_STATUSES,
        STATUS_REVIEW,
        Record,
        evaluate,
        split_name_city,
    )

XREF_SCHEMA = (
    "source_system string, source_code string, unified_id string, match_status string, "
    "match_method string, match_score double, matched_code string, reason string, "
    "llm_rationale string, pipeline_run_id string"
)
CANDIDATE_SCHEMA = (
    "entity string, child_code string, parent_code string, score double, method string, "
    "core double, descriptor double, attribute double, pipeline_run_id string"
)
QUEUE_SCHEMA = (
    "entity string, child_code string, child_name string, child_attribute string, parent_code string, "
    "parent_name string, parent_attribute string, score double, reason string, "
    "runner_up_code string, runner_up_score double, llm_rationale string, pipeline_run_id string"
)
METRICS_SCHEMA = (
    "entity string, pipeline_run_id string, parent_records long, child_records long, matched long, "
    "needs_review long, no_match long, auto_resolved_share double, pairs_considered long, "
    "pairs_possible long, blocking_reduction_ratio double, llm_model string, llm_calls long, "
    "llm_cache_hits long, llm_input_tokens long, llm_output_tokens long, labelled_children long, "
    "precision double, recall double"
)
CACHE_SCHEMA = "cache_key string, is_match boolean, confidence double, rationale string, model string"

OVERRIDES_DDL = (
    "entity STRING, child_code STRING, parent_code STRING COMMENT 'NULL = analyst says no match', "
    "decided_by STRING, decided_at TIMESTAMP"
)
LABELS_DDL = "entity STRING, child_code STRING, parent_code STRING COMMENT 'NULL = truly unmatched'"


def table_name(catalog: str, schema: str, name: str) -> str:
    return f"{catalog}.{schema}.{name}"


def unified_id(source_system: str, code: str, matched_parent: str | None = None) -> str:
    """Parent records are the system of record: a matched child takes its parent's identity."""
    if source_system == "parent":
        return f"P-{code}"
    return f"P-{matched_parent}" if matched_parent else f"C-{code}"


# --------------------------------------------------------------------------- reading


def load_records(spark: SparkSession, entity: str, catalog: str, silver: str = "silver", gold: str = "gold"):
    """Return (parents, children) for ``customers`` or ``products``.

    The shared Gold dimension holds both companies' rows, so the parent set is the
    shared dimension minus the rows that came from the child's own Silver table.
    """
    if entity == "customers":
        child = spark.table(f"{catalog}.{silver}.customers").select(
            F.col("customer_id").cast("string").alias("code"),
            F.col("customer_name").alias("name"),
            F.col("city").alias("attribute"),
        ).dropDuplicates(["code"])
        shared = spark.table(f"{catalog}.{gold}.dim_customers").select(
            F.col("customer_code").cast("string").alias("code"), F.col("customer").alias("label")
        )
        parent_rows = shared.join(child.select("code"), on="code", how="left_anti").collect()
        parents = [Record(row["code"], *split_name_city(row["label"])) for row in parent_rows]
    elif entity == "products":
        child = spark.table(f"{catalog}.{silver}.products").select(
            F.col("product_code").alias("code"), F.col("product").alias("name"), F.col("category").alias("attribute")
        ).dropDuplicates(["code"])
        shared = spark.table(f"{catalog}.{gold}.dim_products").select(
            F.col("product_code").alias("code"), F.col("product").alias("name"), F.col("category").alias("attribute")
        )
        parent_rows = shared.join(child.select("code"), on="code", how="left_anti").collect()
        parents = [Record(row["code"], row["name"], row["attribute"]) for row in parent_rows]
    else:
        raise ValueError(f"unknown entity: {entity!r}")
    children = [Record(row["code"], row["name"], row["attribute"]) for row in child.collect()]
    return parents, children


def ensure_control_tables(spark: SparkSession, catalog: str, silver: str = "silver") -> None:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name(catalog, silver, 'entity_match_overrides')} ({OVERRIDES_DDL})")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name(catalog, silver, 'entity_match_labels')} ({LABELS_DDL})")


def load_overrides(spark: SparkSession, entity: str, catalog: str, silver: str = "silver") -> dict:
    """Latest analyst decision per child record; ``None`` means 'confirmed no match'."""
    rows = (
        spark.table(table_name(catalog, silver, "entity_match_overrides"))
        .filter(F.col("entity") == entity)
        .orderBy("decided_at")
        .collect()
    )
    return {row["child_code"]: row["parent_code"] for row in rows}


def load_labels(spark: SparkSession, entity: str, catalog: str, silver: str = "silver") -> dict:
    rows = spark.table(table_name(catalog, silver, "entity_match_labels")).filter(F.col("entity") == entity).collect()
    return {row["child_code"]: row["parent_code"] for row in rows}


def load_llm_cache(spark: SparkSession, catalog: str, silver: str = "silver") -> dict:
    name = table_name(catalog, silver, "entity_match_llm_cache")
    if not spark.catalog.tableExists(name):
        return {}
    return {
        row["cache_key"]: {"is_match": row["is_match"], "confidence": row["confidence"], "rationale": row["rationale"]}
        for row in spark.table(name).collect()
    }


# --------------------------------------------------------------------------- writing


def _upsert(spark: SparkSession, frame: DataFrame, target: str, keys: list[str]) -> None:
    if not spark.catalog.tableExists(target):
        frame.write.format("delta").option("delta.enableChangeDataFeed", "true").mode("overwrite").saveAsTable(target)
        return
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    (
        DeltaTable.forName(spark, target).alias("t")
        .merge(frame.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _replace_entity(spark: SparkSession, frame: DataFrame, target: str, entity: str) -> None:
    writer = frame.write.format("delta").option("delta.enableChangeDataFeed", "true")
    if spark.catalog.tableExists(target):
        writer.mode("overwrite").option("replaceWhere", f"entity = '{entity}'").saveAsTable(target)
    else:
        writer.mode("overwrite").saveAsTable(target)


def persist_resolution(
    spark: SparkSession,
    *,
    catalog: str,
    entity: str,
    parents,
    children,
    resolution,
    llm_new_entries: dict | None = None,
    llm_model: str | None = None,
    llm_usage: dict | None = None,
    labels: dict | None = None,
    silver: str = "silver",
    gold: str = "gold",
) -> dict:
    """Write the cross-reference, review queue, candidates, LLM cache and run metrics."""
    run_id = str(uuid4())
    by_child = {c.code: c for c in children}
    by_parent = {p.code: p for p in parents}

    xref_rows = [
        ("parent", p.code, unified_id("parent", p.code), "parent_master", "none", None, None, None, None, run_id)
        for p in parents
    ]
    for d in resolution.decisions:
        matched = d.parent_code if d.status in MATCHED_STATUSES else None
        xref_rows.append((
            "child", d.child_code, unified_id("child", d.child_code, matched), d.status, d.method, float(d.score),
            d.parent_code, d.reason, d.llm_rationale, run_id,
        ))
    xref = spark.createDataFrame(xref_rows, XREF_SCHEMA).withColumn("decided_at", F.current_timestamp())
    _upsert(spark, xref, table_name(catalog, gold, f"xref_{entity}"), ["source_system", "source_code"])

    candidate_rows = [
        (entity, c["child_code"], c["parent_code"], float(c["score"]), c["method"],
         float(c["core"]), float(c["descriptor"]), float(c["attribute"]), run_id)
        for c in resolution.candidates
    ]
    candidates = spark.createDataFrame(candidate_rows, CANDIDATE_SCHEMA).withColumn(
        "scored_at", F.current_timestamp()
    )
    _replace_entity(spark, candidates, table_name(catalog, silver, "entity_match_candidates"), entity)

    queue_rows = []
    for d in resolution.decisions:
        if d.status != STATUS_REVIEW:
            continue
        child, parent = by_child[d.child_code], by_parent.get(d.parent_code)
        queue_rows.append((
            entity, d.child_code, child.name, child.attribute, d.parent_code,
            parent.name if parent else None, parent.attribute if parent else None,
            float(d.score), d.reason, d.runner_up_code, d.runner_up_score, d.llm_rationale, run_id,
        ))
    queue = spark.createDataFrame(queue_rows, QUEUE_SCHEMA).withColumn("queued_at", F.current_timestamp())
    _replace_entity(spark, queue, table_name(catalog, silver, "entity_match_review_queue"), entity)

    if llm_new_entries:
        cache_rows = [(key, e["is_match"], float(e["confidence"]), e["rationale"], llm_model) for key, e in llm_new_entries.items()]
        cache = spark.createDataFrame(cache_rows, CACHE_SCHEMA).withColumn("cached_at", F.current_timestamp())
        _upsert(spark, cache, table_name(catalog, silver, "entity_match_llm_cache"), ["cache_key"])

    metrics = _metrics(entity, run_id, parents, children, resolution, labels, llm_model, llm_usage)
    fields = [column.split()[0] for column in METRICS_SCHEMA.split(", ")]
    metrics_frame = spark.createDataFrame(
        [tuple(metrics[name] for name in fields)], METRICS_SCHEMA
    ).withColumn("measured_at", F.current_timestamp())
    (
        metrics_frame.write.format("delta").option("mergeSchema", "true").mode("append")
        .saveAsTable(table_name(catalog, silver, "entity_match_metrics"))
    )
    return metrics


def _metrics(entity, run_id, parents, children, resolution, labels, llm_model, llm_usage) -> dict:
    counts: dict[str, int] = {}
    for d in resolution.decisions:
        counts[d.status] = counts.get(d.status, 0) + 1
    total = len(resolution.decisions)
    review = counts.get(STATUS_REVIEW, 0)
    metrics = {
        "entity": entity,
        "pipeline_run_id": run_id,
        "parent_records": len(parents),
        "child_records": len(children),
        "matched": sum(counts.get(s, 0) for s in MATCHED_STATUSES),
        "needs_review": review,
        "no_match": total - sum(counts.get(s, 0) for s in MATCHED_STATUSES) - review,
        "auto_resolved_share": round(1 - review / total, 4) if total else 1.0,
        "pairs_considered": resolution.pairs_considered,
        "pairs_possible": resolution.pairs_possible,
        "blocking_reduction_ratio": round(resolution.reduction_ratio, 4),
        "llm_model": llm_model,
        "llm_calls": (llm_usage or {}).get("calls", 0),
        "llm_cache_hits": (llm_usage or {}).get("cache_hits", 0),
        "llm_input_tokens": (llm_usage or {}).get("input_tokens", 0),
        "llm_output_tokens": (llm_usage or {}).get("output_tokens", 0),
        "labelled_children": None,
        "precision": None,
        "recall": None,
    }
    if labels:
        scoped = {code: labels[code] for code in (d.child_code for d in resolution.decisions) if code in labels}
        if scoped:
            scores = evaluate(resolution, scoped)
            metrics.update(labelled_children=len(scoped), precision=scores["precision"], recall=scores["recall"])
    return metrics

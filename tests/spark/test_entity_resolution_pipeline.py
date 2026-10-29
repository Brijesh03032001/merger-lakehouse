"""Entity resolution notebook run end to end on local Spark + Delta, scored against ground truth."""

import pytest
from pyspark.sql import functions as F

import consolidated_pipeline.common.entity_resolution as er

pytestmark = [pytest.mark.spark, pytest.mark.slow]


def _table(pipeline, schema, name):
    return pipeline.spark.table(f"{pipeline.catalog}.{schema}.{name}")


@pytest.fixture(scope="module")
def resolved(pipeline):
    pipeline.run("4_entity_resolution/1_resolve_entities.py", entity="customers")
    pipeline.run("4_entity_resolution/1_resolve_entities.py", entity="products")
    return pipeline


def test_parent_is_derived_as_shared_dimension_minus_child_rows(resolved):
    scenario = resolved.scenario
    xref = _table(resolved, "gold", "xref_customers")
    assert xref.filter("source_system = 'parent'").count() == len(scenario.parent_customers)
    assert xref.filter("source_system = 'child'").count() == len(scenario.child_customers)


def test_every_record_appears_once_in_the_crossref(resolved):
    for entity in ("customers", "products"):
        xref = _table(resolved, "gold", f"xref_{entity}")
        assert xref.count() == xref.select("source_system", "source_code").distinct().count()


def test_matched_children_take_their_parents_identity(resolved):
    xref = _table(resolved, "gold", "xref_customers").filter("source_system = 'child'").collect()
    truth = {t["child_id"]: t["parent_code"] for t in resolved.scenario.customer_truth}
    for row in xref:
        if row["match_status"] == er.STATUS_AUTO:
            assert row["unified_id"] == f"P-{row['matched_code']}"
            assert truth[row["source_code"]] == row["matched_code"], "auto-match must be correct on this data"
        elif row["match_status"] in (er.STATUS_REVIEW, er.STATUS_NO_MATCH):
            assert row["unified_id"] == f"C-{row['source_code']}", "unresolved children stay separate"


def test_customer_matching_against_ground_truth(resolved):
    xref = _table(resolved, "gold", "xref_customers").filter("source_system = 'child'").collect()
    truth = {t["child_id"]: t["parent_code"] for t in resolved.scenario.customer_truth}
    predicted = {r["source_code"]: r["matched_code"] for r in xref if r["match_status"] in er.MATCHED_STATUSES}
    correct = sum(1 for child, parent in predicted.items() if truth[child] == parent)
    positives = sum(1 for parent in truth.values() if parent)
    assert correct == len(predicted), "precision must be 1.0 on this scenario"
    assert correct / positives >= 0.75


def test_review_queue_holds_exactly_the_review_decisions(resolved):
    queue = _table(resolved, "silver", "entity_match_review_queue").filter("entity = 'customers'")
    in_xref = _table(resolved, "gold", "xref_customers").filter("match_status = 'needs_review'").count()
    assert queue.count() == in_xref
    assert queue.filter("parent_code IS NULL OR child_name IS NULL OR reason IS NULL").count() == 0


def test_metrics_row_reports_blocking_and_auto_resolution(resolved):
    row = (
        _table(resolved, "silver", "entity_match_metrics")
        .filter("entity = 'customers'").orderBy(F.desc("measured_at")).first()
    )
    assert row["child_records"] == len(resolved.scenario.child_customers)
    assert row["blocking_reduction_ratio"] > 0.8
    assert row["llm_calls"] == 0, "the LLM is opt-in and off by default"
    assert row["precision"] is None, "no labels supplied, so no precision claimed"


def test_rerun_is_idempotent(resolved):
    before = _table(resolved, "gold", "xref_customers").count()
    resolved.run("4_entity_resolution/1_resolve_entities.py", entity="customers")
    after = _table(resolved, "gold", "xref_customers")
    assert after.count() == before
    assert after.select("source_system", "source_code").distinct().count() == before
    queue = _table(resolved, "silver", "entity_match_review_queue").filter("entity = 'customers'")
    assert queue.count() == queue.select("child_code").distinct().count()


def test_analyst_override_closes_the_review_loop(resolved):
    spark, catalog = resolved.spark, resolved.catalog
    queue = _table(resolved, "silver", "entity_match_review_queue").filter("entity = 'customers'").collect()
    assert queue, "scenario should leave something for a human"
    confirm, reject = queue[0], queue[-1]
    spark.sql(
        f"""INSERT INTO {catalog}.silver.entity_match_overrides VALUES
            ('customers', '{confirm['child_code']}', '{confirm['parent_code']}', 'analyst', current_timestamp()),
            ('customers', '{reject['child_code']}', NULL, 'analyst', current_timestamp())"""
    )
    resolved.run("4_entity_resolution/1_resolve_entities.py", entity="customers")
    xref = {r["source_code"]: r for r in _table(resolved, "gold", "xref_customers").collect() if r["source_system"] == "child"}
    assert xref[confirm["child_code"]]["match_status"] == er.STATUS_MANUAL_MATCH
    assert xref[confirm["child_code"]]["unified_id"] == f"P-{confirm['parent_code']}"
    assert xref[reject["child_code"]]["match_status"] == er.STATUS_MANUAL_NO_MATCH
    open_children = {r["child_code"] for r in _table(resolved, "silver", "entity_match_review_queue").filter("entity = 'customers'").collect()}
    assert confirm["child_code"] not in open_children and reject["child_code"] not in open_children


def test_labelled_set_produces_precision_and_recall(resolved):
    spark, catalog = resolved.spark, resolved.catalog
    values = ", ".join(
        f"('customers', '{t['child_id']}', {('NULL' if t['parent_code'] is None else repr(t['parent_code']))})"
        for t in resolved.scenario.customer_truth
    )
    spark.sql(f"INSERT INTO {catalog}.silver.entity_match_labels VALUES {values}")
    resolved.run("4_entity_resolution/1_resolve_entities.py", entity="customers")
    row = (
        _table(resolved, "silver", "entity_match_metrics")
        .filter("entity = 'customers'").orderBy(F.desc("measured_at")).first()
    )
    assert row["labelled_children"] == len(resolved.scenario.customer_truth)
    assert 0.0 <= row["precision"] <= 1.0 and 0.0 <= row["recall"] <= 1.0
    assert row["precision"] >= 0.97


def test_product_matching_against_ground_truth(resolved):
    scenario = resolved.scenario
    xref = _table(resolved, "gold", "xref_products").filter("source_system = 'child'").collect()
    by_name = {p["product_name"]: p for p in scenario.child_products}
    assert len(xref) <= len(scenario.child_products), "children sharing a product_code collapse to one row"
    matched = [r for r in xref if r["match_status"] in er.MATCHED_STATUSES]
    parents = {p["product_code"] for p in scenario.parent_products}
    assert matched and all(r["matched_code"] in parents for r in matched)

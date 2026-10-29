import statistics

import pytest

from consolidated_pipeline.common import entity_resolution as er
from local_lakehouse.synthetic import build_scenario


def _rec(code, name, attribute=None, external_id=None):
    return er.Record(code, name, attribute, external_id)


# ---------------------------------------------------------------- primitives


def test_jaro_winkler_matches_published_reference_values():
    assert er.jaro_winkler("martha", "marhta") == pytest.approx(0.9611, abs=1e-4)
    assert er.jaro_winkler("dixon", "dicksonx") == pytest.approx(0.8133, abs=1e-4)


def test_soundex_matches_reference_codes():
    assert er.soundex("Robert") == er.soundex("Rupert") == "R163"
    assert er.soundex("Tymczak") == "T522"


def test_edit_similarity_counts_a_transposition_as_one_edit():
    assert er.edit_similarity("protein", "protien") == pytest.approx(1 - 1 / 7)
    assert er.edit_similarity("vitaleaf", "vitafuel") < 0.6


def test_tokenize_drops_legal_form_noise_and_folds_accents():
    assert er.tokenize("Zenathlète Foods Pvt. Ltd.") == ["zenathlete", "foods"]
    assert er.tokenize("Flex & Co") == ["flex"]
    assert er.tokenize(None) == []


def test_canonical_city_repairs_typos_but_does_not_invent_cities():
    for variant in ("Bengaluruu", "Bangalore", "Bengalore"):
        assert er.canonical_city(variant) == "Bengaluru"
    assert er.canonical_city("NewDheli") == "New Delhi"
    assert er.canonical_city("Unknown") is None
    assert er.canonical_city("") is None
    assert er.canonical_city("Atlantis") == "atlantis"


def test_split_name_city_only_splits_on_a_real_city_suffix():
    assert er.split_name_city("Zenathlete Foods-Bengaluru") == ("Zenathlete Foods", "Bengaluru")
    assert er.split_name_city("Recovery Lane-Unknown") == ("Recovery Lane", None)
    assert er.split_name_city("Bolt-Forge Foods LLP") == ("Bolt-Forge Foods LLP", None)


# ---------------------------------------------------------------- decisions


def _resolve(parents, children, **kwargs):
    return er.resolve(parents, children, er.CUSTOMER_PROFILE, **kwargs)


def test_same_name_and_city_is_an_automatic_match():
    parents = [_rec("P1", "Zenathlete Foods", "Bengaluru"), _rec("P2", "Primefuel Nutrition", "Pune")]
    result = _resolve(parents, [_rec("C1", "ZENATHLETE  FOODS PVT LTD", "Bangalore")])
    decision = result.decisions[0]
    assert (decision.status, decision.parent_code, decision.method) == (er.STATUS_AUTO, "P1", "exact_name_attribute")
    assert decision.score == pytest.approx(1.0)


def test_split_join_and_typo_variants_still_match():
    parents = [_rec("P1", "Primefuel Nutrition", "Hyderabad")]
    for variant in ("Prime Fuel Nutrition", "Prime-Fuel Nutrition", "Primefuell Nutrition", "Nutrition Primefuel"):
        decision = _resolve(parents, [_rec("C", variant, "Hyderabad")]).decisions[0]
        assert decision.status == er.STATUS_AUTO, variant
        assert decision.parent_code == "P1"


def test_brand_lookalikes_do_not_auto_match():
    parents = [_rec("P1", "Vitafuel Provisions", "New Delhi")]
    decision = _resolve(parents, [_rec("C", "Vitaleaf Provisions", "New Delhi")]).decisions[0]
    assert decision.status != er.STATUS_AUTO


def test_same_brand_in_a_different_business_goes_to_review_not_auto():
    """The merger's hard case: 'Primefuel Dairy' is not 'Primefuel Nutrition'."""
    parents = [_rec("P1", "Primefuel Nutrition", "Hyderabad")]
    decision = _resolve(parents, [_rec("C", "Primefuel Dairy", "Hyderabad")]).decisions[0]
    assert decision.status == er.STATUS_REVIEW
    assert decision.parent_code == "P1"


def test_same_brand_different_business_and_city_is_not_surfaced_to_reviewers():
    parents = [_rec("P1", "Primefuel Nutrition", "Pune")]
    decision = _resolve(parents, [_rec("C", "Primefuel Dairy", "Hyderabad")]).decisions[0]
    assert decision.status == er.STATUS_NO_MATCH
    assert decision.parent_code is None


def test_unrelated_names_never_match():
    parents = [_rec("P1", "Zenathlete Foods", "Bengaluru")]
    decision = _resolve(parents, [_rec("C", "Quantum Plumbing", "Bengaluru")]).decisions[0]
    assert decision.status == er.STATUS_NO_MATCH


def test_close_runner_up_makes_a_strong_match_ambiguous():
    parents = [_rec("P1", "Aerolabs Dairy", "Mumbai"), _rec("P2", "Aerolabs Dairy", "Mumbai")]
    decision = _resolve(parents, [_rec("C", "Aerolabs Dairy", "Mumbai")]).decisions[0]
    assert decision.status == er.STATUS_REVIEW
    assert "ambiguous" in decision.reason


def test_two_children_claiming_one_parent_keep_only_the_better_claim():
    parents = [_rec("P1", "Zenathlete Foods", "Bengaluru")]
    children = [_rec("C1", "Zenathlete Foods", "Bengaluru"), _rec("C2", "Zenathlette Foods", "Bengaluru")]
    by_child = {d.child_code: d for d in _resolve(parents, children).decisions}
    assert by_child["C1"].status == er.STATUS_AUTO
    assert by_child["C2"].status == er.STATUS_REVIEW
    assert "claimed" in by_child["C2"].reason


def test_matching_external_id_wins_over_a_different_name():
    parents = [_rec("P1", "Acme Trading", "Pune", external_id="GST123")]
    child = _rec("C", "Completely Renamed Holdings", "Chennai", external_id="GST123")
    decision = _resolve(parents, [child]).decisions[0]
    assert (decision.status, decision.method, decision.score) == (er.STATUS_AUTO, "exact_id", 1.0)


def test_manual_overrides_beat_every_automatic_decision():
    parents = [_rec("P1", "Zenathlete Foods", "Bengaluru"), _rec("P2", "Other Foods", "Pune")]
    children = [_rec("C1", "Zenathlete Foods", "Bengaluru"), _rec("C2", "Quantum Plumbing", "Pune")]
    result = _resolve(parents, children, overrides={"C1": None, "C2": "P2"})
    by_child = {d.child_code: d for d in result.decisions}
    assert by_child["C1"].status == er.STATUS_MANUAL_NO_MATCH and by_child["C1"].parent_code is None
    assert by_child["C2"].status == er.STATUS_MANUAL_MATCH and by_child["C2"].parent_code == "P2"


# ---------------------------------------------------------------- LLM hook


class _Scripted:
    def __init__(self, verdict):
        self.verdict, self.calls = verdict, []

    def decide(self, entity, child, parent):
        self.calls.append((entity, child.code, parent.code))
        return self.verdict


def _review_case():
    return [_rec("P1", "Primefuel Nutrition", "Hyderabad")], [_rec("C", "Primefuel Dairy", "Hyderabad")]


def test_confident_llm_match_resolves_a_review_item():
    parents, children = _review_case()
    judge = _Scripted(er.AdjudicationResult(True, 0.95, "same brand, same city"))
    decision = _resolve(parents, children, adjudicator=judge).decisions[0]
    assert (decision.status, decision.method, decision.parent_code) == (er.STATUS_LLM_MATCH, "llm", "P1")
    assert decision.llm_rationale == "same brand, same city"


def test_confident_llm_rejection_clears_the_parent_link():
    parents, children = _review_case()
    judge = _Scripted(er.AdjudicationResult(False, 0.9, "different product line"))
    decision = _resolve(parents, children, adjudicator=judge).decisions[0]
    assert (decision.status, decision.parent_code) == (er.STATUS_LLM_NO_MATCH, None)


def test_unconfident_llm_leaves_the_item_in_the_human_queue():
    parents, children = _review_case()
    judge = _Scripted(er.AdjudicationResult(True, 0.5, "unsure"))
    decision = _resolve(parents, children, adjudicator=judge).decisions[0]
    assert decision.status == er.STATUS_REVIEW
    assert "not confident" in decision.reason


def test_llm_is_only_asked_about_review_items():
    parents = [_rec("P1", "Zenathlete Foods", "Bengaluru")]
    judge = _Scripted(er.AdjudicationResult(True, 0.99, ""))
    _resolve(parents, [_rec("C1", "Zenathlete Foods", "Bengaluru"), _rec("C2", "Quantum Plumbing", "Pune")], adjudicator=judge)
    assert judge.calls == []


# ---------------------------------------------------------------- end to end on synthetic merger data


def _customers(seed):
    scenario = build_scenario(seed=seed, days=1, rows_per_day=1)
    parents = [er.Record(p["customer_code"], *er.split_name_city(p["customer"])) for p in scenario.parent_customers]
    children = [er.Record(c["customer_id"], c["customer_name"], c["city"]) for c in scenario.child_customers]
    truth = {t["child_id"]: t["parent_code"] for t in scenario.customer_truth}
    return parents, children, truth


def test_customer_matching_quality_on_unseen_synthetic_seeds():
    """Regression floor, not a claim about real data: the noise model is the generator's own."""
    metrics = []
    for seed in (201, 202, 203, 204, 205):
        parents, children, truth = _customers(seed)
        resolution = _resolve(parents, children, track_pool=True)
        metrics.append(er.evaluate(resolution, truth))
    assert min(m["precision"] for m in metrics) >= 0.97
    assert statistics.mean(m["recall"] for m in metrics) >= 0.85
    assert statistics.mean(m["auto_resolved_share"] for m in metrics) >= 0.85
    assert min(m["blocking_recall"] for m in metrics) == 1.0
    assert min(m["blocking_reduction_ratio"] for m in metrics) >= 0.85


def test_product_matching_quality_on_unseen_synthetic_seeds():
    for seed in (201, 202, 203):
        scenario = build_scenario(seed=seed, days=1, rows_per_day=1)
        parents = [er.Record(p["product_code"], p["product"], p["category"]) for p in scenario.parent_products]
        children = [er.Record(c["product_id"], c["product_name"], c["category"]) for c in scenario.child_products]
        truth = {t["child_id"]: t["parent_code"] for t in scenario.product_truth}
        metrics = er.evaluate(er.resolve(parents, children, er.PRODUCT_PROFILE, track_pool=True), truth)
        assert metrics["precision"] >= 0.97
        assert metrics["recall"] >= 0.9
        assert metrics["blocking_recall"] == 1.0


def test_no_true_match_is_lost_without_a_human_or_llm_seeing_it():
    """Every missed match must be in the review queue, never silently dropped as no_match."""
    parents, children, truth = _customers(206)
    resolution = _resolve(parents, children, track_pool=True)
    metrics = er.evaluate(resolution, truth)
    missed = metrics["true_matches"] - metrics["correct_matches"]
    assert metrics["true_matches_in_review"] >= missed - 1

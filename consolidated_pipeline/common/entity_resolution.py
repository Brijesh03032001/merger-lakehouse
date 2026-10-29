# Databricks notebook source
"""Entity resolution for the parent / acquired-company merger.

Both companies describe the same real-world customers and products with
different IDs and spellings. This module decides which child records are the
same entity as which parent records:

    normalise -> block -> score -> decide (auto / LLM / human review)

It is standard-library only so it unit-tests without Spark. The Delta I/O lives
in ``entity_resolution_delta``. Weights and thresholds are hand-set defaults in
``MatchConfig``; ``evaluate`` measures them against labelled pairs.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping, Protocol, Sequence

STATUS_AUTO = "auto_match"
STATUS_LLM_MATCH = "llm_match"
STATUS_LLM_NO_MATCH = "llm_no_match"
STATUS_REVIEW = "needs_review"
STATUS_NO_MATCH = "no_match"
STATUS_MANUAL_MATCH = "manual_match"
STATUS_MANUAL_NO_MATCH = "manual_no_match"
MATCHED_STATUSES = frozenset({STATUS_AUTO, STATUS_LLM_MATCH, STATUS_MANUAL_MATCH})

LEGAL_STOPWORDS = frozenset(
    {"pvt", "private", "ltd", "limited", "llp", "llc", "inc", "co", "corp", "company", "the", "and"}
)
KNOWN_CITIES = (
    "Bengaluru", "Hyderabad", "New Delhi", "Mumbai", "Chennai", "Pune", "Kolkata", "Ahmedabad",
)
CITY_ALIASES = {"bangalore": "Bengaluru", "delhi": "New Delhi", "hyd": "Hyderabad"}
SIZE_PATTERN = re.compile(r"^\d+(\.\d+)?(g|kg|ml|l)$")

CUSTOMER_DESCRIPTORS = frozenset(
    {
        "nutrition", "foods", "food", "wellness", "dairy", "snacks", "traders", "stores", "store",
        "mart", "retail", "health", "provisions", "sports", "supermarket", "distributors",
    }
)


# --------------------------------------------------------------------------- strings


def _fold(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()


def tokenize(text: str | None) -> list[str]:
    """Lower-case ASCII word tokens with legal-form noise (Pvt, Ltd, &, The) removed."""
    if not text:
        return []
    tokens = re.findall(r"[a-z0-9]+", _fold(text).replace("&", " and "))
    return [token for token in tokens if token not in LEGAL_STOPWORDS]


def _jaro(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    window = max(max(len(a), len(b)) // 2 - 1, 0)
    a_hit = [False] * len(a)
    b_hit = [False] * len(b)
    matches = 0
    for i, char in enumerate(a):
        for j in range(max(0, i - window), min(len(b), i + window + 1)):
            if not b_hit[j] and b[j] == char:
                a_hit[i] = b_hit[j] = True
                matches += 1
                break
    if not matches:
        return 0.0
    mismatched = k = 0
    for i, hit in enumerate(a_hit):
        if hit:
            while not b_hit[k]:
                k += 1
            mismatched += a[i] != b[k]
            k += 1
    transpositions = mismatched / 2
    return (matches / len(a) + matches / len(b) + (matches - transpositions) / matches) / 3


def jaro_winkler(a: str, b: str) -> float:
    similarity = _jaro(a, b)
    if similarity <= 0.7:
        return similarity
    prefix = 0
    for x, y in zip(a[:4], b[:4]):
        if x != y:
            break
        prefix += 1
    return similarity + prefix * 0.1 * (1 - similarity)


def edit_similarity(a: str, b: str) -> float:
    """1 - normalised Damerau-Levenshtein (optimal string alignment) distance."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    rows = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    for i in range(len(a) + 1):
        rows[i][0] = i
    for j in range(len(b) + 1):
        rows[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            cost = a[i - 1] != b[j - 1]
            rows[i][j] = min(rows[i - 1][j] + 1, rows[i][j - 1] + 1, rows[i - 1][j - 1] + cost)
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                rows[i][j] = min(rows[i][j], rows[i - 2][j - 2] + 1)
    return 1 - rows[len(a)][len(b)] / max(len(a), len(b))


def soundex(word: str) -> str:
    codes = {c: d for d, letters in {"1": "bfpv", "2": "cgjkqsxz", "3": "dt", "4": "l", "5": "mn", "6": "r"}.items()
             for c in letters}
    letters = [c for c in _fold(word) if c.isalpha()]
    if not letters:
        return ""
    encoded, previous = letters[0].upper(), codes.get(letters[0], "")
    for char in letters[1:]:
        code = codes.get(char, "")
        if code and code != previous:
            encoded += code
        if char not in "hw":
            previous = code
    return (encoded + "000")[:4]


def _is_abbreviation(short: str, long: str) -> bool:
    """'hny' -> 'honey', 'pnut' -> 'peanut': same first letter, letters appear in order."""
    if len(short) < 3 or len(short) >= len(long) or short[0] != long[0]:
        return False
    remaining = iter(long)
    return all(char in remaining for char in short)


def token_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    similarity = edit_similarity(a, b)
    if similarity >= 0.75:
        return similarity
    short, long = sorted((a, b), key=len)
    return 0.9 if _is_abbreviation(short, long) else 0.0


def aligned_similarity(a: Sequence[str], b: Sequence[str]) -> float:
    """Greedy one-to-one token alignment; unmatched tokens count against the score."""
    if not a or not b:
        return 0.0
    pairs = sorted(
        ((token_similarity(x, y), i, j) for i, x in enumerate(a) for j, y in enumerate(b)), reverse=True
    )
    used_a: set[int] = set()
    used_b: set[int] = set()
    total = 0.0
    for similarity, i, j in pairs:
        if similarity > 0 and i not in used_a and j not in used_b:
            used_a.add(i)
            used_b.add(j)
            total += similarity
    return total / max(len(a), len(b))


# --------------------------------------------------------------------------- records


@dataclass(frozen=True)
class Record:
    """One source row. ``attribute`` is the tie-breaker (city for customers, category for products)."""

    code: str
    name: str
    attribute: str | None = None
    external_id: str | None = None


@dataclass(frozen=True)
class Profile:
    """Entity-specific normalisation: which tokens are descriptive, how attributes compare."""

    entity: str
    descriptors: frozenset[str]
    compact_compare: bool
    canonical_attribute: Callable[[str | None], str | None]


def _letters(text: str) -> str:
    return "".join(re.findall(r"[a-z]+", _fold(text)))


def canonical_city(text: str | None) -> str | None:
    """Map spelling variants to a known city; unrecognised text is kept normalised, not guessed."""
    key = _letters(text or "")
    if not key or key == "unknown":
        return None
    if key in CITY_ALIASES:
        return CITY_ALIASES[key]
    best_city, best = None, 0.0
    for city in KNOWN_CITIES:
        similarity = jaro_winkler(key, _letters(city))
        if similarity > best:
            best_city, best = city, similarity
    return best_city if best >= 0.9 else key


def canonical_category(text: str | None) -> str | None:
    tokens = tokenize(text)
    return " ".join(sorted(tokens)) or None


def split_name_city(label: str, known: Iterable[str] = KNOWN_CITIES) -> tuple[str, str | None]:
    """Split the pipeline's 'Customer Name-City' label; the suffix must really be a city."""
    if "-" in label:
        name, _, suffix = label.rpartition("-")
        city = canonical_city(suffix)
        if suffix.strip().lower() == "unknown" or city in set(known):
            return name.strip(), city
    return label.strip(), None


CUSTOMER_PROFILE = Profile("customers", CUSTOMER_DESCRIPTORS, True, canonical_city)
PRODUCT_PROFILE = Profile("products", frozenset(), False, canonical_category)


@dataclass(frozen=True)
class Prepared:
    record: Record
    tokens: tuple[str, ...]
    descriptors: frozenset[str]
    compact: str
    attribute: str | None


def prepare(record: Record, profile: Profile) -> Prepared:
    tokens = tokenize(record.name)
    descriptors = frozenset(t for t in tokens if t in profile.descriptors or SIZE_PATTERN.match(t))
    core = tuple(t for t in tokens if t not in descriptors) or tuple(tokens)
    return Prepared(record, core, descriptors, "".join(core), profile.canonical_attribute(record.attribute))


def block_keys(item: Prepared) -> set[str]:
    keys = {f"token:{token}" for token in item.tokens if len(token) >= 4}
    keys |= {f"tokensound:{soundex(token)}" for token in item.tokens if len(token) >= 4}
    if item.record.external_id:
        keys.add(f"id:{item.record.external_id}")  # a shared ID must pair records whatever their names
    if item.compact:
        keys |= {f"prefix:{item.compact[:4]}", f"suffix:{item.compact[-4:]}", f"sound:{soundex(item.compact)}"}
        # Letters in order: any pure transposition ("Iroxn" / "Ironx") shares this key.
        keys.add(f"letters:{''.join(sorted(item.compact))}")
    return keys


# --------------------------------------------------------------------------- scoring


@dataclass(frozen=True)
class MatchConfig:
    auto_threshold: float = 0.90
    review_threshold: float = 0.75
    ambiguity_margin: float = 0.03
    core_weight: float = 0.70
    descriptor_weight: float = 0.15
    attribute_weight: float = 0.15
    neutral: float = 0.6  # credit when a descriptor is present on only one side
    core_floor: float = 0.5  # raw name similarity at or below this earns no credit
    core_ceiling: float = 0.9  # ...and at or above this earns full credit (typos live here)
    candidates_kept: int = 5
    llm_accept_confidence: float = 0.8


def score_pair(child: Prepared, parent: Prepared, profile: Profile, config: MatchConfig) -> dict[str, float | str]:
    if child.record.external_id and child.record.external_id == parent.record.external_id:
        return {"score": 1.0, "method": "exact_id", "core": 1.0, "descriptor": 1.0, "attribute": 1.0}

    core = aligned_similarity(child.tokens, parent.tokens)
    if profile.compact_compare and child.compact and parent.compact:
        core = max(core, edit_similarity(child.compact, parent.compact))
    if child.descriptors and parent.descriptors:
        union = child.descriptors | parent.descriptors
        descriptor = len(child.descriptors & parent.descriptors) / len(union)
    else:
        descriptor = config.neutral
    if child.attribute and parent.attribute:
        agreement = aligned_similarity(child.attribute.split(), parent.attribute.split())
        attribute = 1.0 if agreement >= 0.88 else 0.0
    else:
        attribute = 0.5
    calibrated = min(1.0, max(0.0, (core - config.core_floor) / (config.core_ceiling - config.core_floor)))
    score = config.core_weight * calibrated + config.descriptor_weight * descriptor + config.attribute_weight * attribute
    same_name = child.compact == parent.compact or sorted(child.tokens) == sorted(parent.tokens)
    exact = same_name and child.descriptors == parent.descriptors and attribute == 1.0
    return {
        "score": round(score, 6),
        "method": "exact_name_attribute" if exact else "fuzzy",
        "core": round(core, 4),
        "descriptor": round(descriptor, 4),
        "attribute": attribute,
    }


# --------------------------------------------------------------------------- LLM hook


@dataclass(frozen=True)
class AdjudicationResult:
    is_match: bool
    confidence: float
    rationale: str


class Adjudicator(Protocol):
    def decide(self, entity: str, child: Record, parent: Record) -> AdjudicationResult: ...


# --------------------------------------------------------------------------- decisions


@dataclass
class Decision:
    child_code: str
    parent_code: str | None
    score: float
    method: str
    status: str
    reason: str
    runner_up_code: str | None = None
    runner_up_score: float | None = None
    llm_rationale: str | None = None


@dataclass
class Resolution:
    decisions: list[Decision]
    candidates: list[dict]
    pairs_considered: int
    pairs_possible: int
    pool: set[tuple[str, str]] | None = None  # every blocked pair; only kept when track_pool=True

    @property
    def reduction_ratio(self) -> float:
        return 1 - self.pairs_considered / self.pairs_possible if self.pairs_possible else 0.0


def resolve(
    parents: Sequence[Record],
    children: Sequence[Record],
    profile: Profile = CUSTOMER_PROFILE,
    config: MatchConfig = MatchConfig(),
    *,
    overrides: Mapping[str, str | None] | None = None,
    adjudicator: Adjudicator | None = None,
    track_pool: bool = False,
) -> Resolution:
    """Decide, for every child record, which parent record (if any) is the same entity.

    ``overrides`` maps child code -> parent code (or ``None`` for "no match") and wins
    over every automatic decision, so analysts can resolve the review queue.
    """
    prepared_parents = [prepare(record, profile) for record in parents]
    index: dict[str, list[Prepared]] = {}
    for item in prepared_parents:
        for key in block_keys(item):
            index.setdefault(key, []).append(item)

    decisions: list[Decision] = []
    candidates: list[dict] = []
    pool_pairs: set[tuple[str, str]] = set()
    considered = 0
    for record in children:
        child = prepare(record, profile)
        pool = {p.record.code: p for key in block_keys(child) for p in index.get(key, ())}
        considered += len(pool)
        if track_pool:
            pool_pairs.update((record.code, code) for code in pool)
        scored = sorted(
            ((score_pair(child, p, profile, config), p) for p in pool.values()),
            key=lambda pair: (-pair[0]["score"], pair[1].record.code),
        )
        for features, parent in scored[: config.candidates_kept]:
            candidates.append({"child_code": record.code, "parent_code": parent.record.code, **features})
        decisions.append(_decide(record, scored, config, overrides))

    _demote_shared_parents(decisions)
    if adjudicator is not None:
        _adjudicate(decisions, {c.code: c for c in children}, {p.code: p for p in parents}, profile, config, adjudicator)
    return Resolution(decisions, candidates, considered, len(parents) * len(children),
                      pool_pairs if track_pool else None)


def _decide(record, scored, config, overrides) -> Decision:
    if overrides is not None and record.code in overrides:
        target = overrides[record.code]
        if target is None:
            return Decision(record.code, None, 0.0, "manual", STATUS_MANUAL_NO_MATCH, "analyst decision")
        return Decision(record.code, target, 1.0, "manual", STATUS_MANUAL_MATCH, "analyst decision")
    if not scored:
        return Decision(record.code, None, 0.0, "none", STATUS_NO_MATCH, "no candidates after blocking")

    best_features, best = scored[0]
    runner = scored[1] if len(scored) > 1 else None
    runner_code = runner[1].record.code if runner else None
    runner_score = runner[0]["score"] if runner else None
    score = best_features["score"]
    common = dict(runner_up_code=runner_code, runner_up_score=runner_score)

    if score < config.review_threshold:
        return Decision(record.code, None, score, "fuzzy", STATUS_NO_MATCH, "below review threshold", **common)
    if score >= config.auto_threshold:
        if runner_score is not None and score - runner_score < config.ambiguity_margin:
            return Decision(record.code, best.record.code, score, best_features["method"], STATUS_REVIEW,
                            "ambiguous: runner-up within margin", **common)
        return Decision(record.code, best.record.code, score, best_features["method"], STATUS_AUTO,
                        "score above auto threshold", **common)
    return Decision(record.code, best.record.code, score, best_features["method"], STATUS_REVIEW,
                    "score in review band", **common)


def _demote_shared_parents(decisions: list[Decision]) -> None:
    """Two children claiming one parent is a duplicate or a mistake: keep the best, review the rest."""
    claims: dict[str, list[Decision]] = {}
    for decision in decisions:
        if decision.status == STATUS_AUTO and decision.parent_code is not None:
            claims.setdefault(decision.parent_code, []).append(decision)
    for group in claims.values():
        group.sort(key=lambda d: -d.score)
        for loser in group[1:]:
            loser.status = STATUS_REVIEW
            loser.reason = "parent also claimed by a higher-scoring child record"


def _adjudicate(decisions, children, parents, profile, config, adjudicator) -> None:
    for decision in decisions:
        if decision.status != STATUS_REVIEW or decision.parent_code is None:
            continue
        result = adjudicator.decide(profile.entity, children[decision.child_code], parents[decision.parent_code])
        decision.llm_rationale = result.rationale
        if result.confidence < config.llm_accept_confidence:
            decision.reason += "; LLM not confident"
        elif result.is_match:
            decision.status, decision.method = STATUS_LLM_MATCH, "llm"
        else:
            decision.status, decision.method, decision.parent_code = STATUS_LLM_NO_MATCH, "llm", None


# --------------------------------------------------------------------------- evaluation


def evaluate(resolution: Resolution, truth: Mapping[str, str | None]) -> dict:
    """Pair-level precision/recall of matched decisions against labelled child -> parent truth."""
    decisions = resolution.decisions
    predicted = {d.child_code: d.parent_code for d in decisions if d.status in MATCHED_STATUSES}
    positives = {child: parent for child, parent in truth.items() if parent is not None}
    correct = sum(1 for child, parent in predicted.items() if positives.get(child) == parent)
    queue = [d for d in decisions if d.status == STATUS_REVIEW]
    total = len(decisions)
    return {
        "children": total,
        "true_matches": len(positives),
        "predicted_matches": len(predicted),
        "correct_matches": correct,
        "precision": round(correct / len(predicted), 4) if predicted else 1.0,
        "recall": round(correct / len(positives), 4) if positives else 1.0,
        "review_queue": len(queue),
        "true_matches_in_review": sum(1 for d in queue if positives.get(d.child_code) == d.parent_code),
        "auto_resolved_share": round(1 - len(queue) / total, 4) if total else 1.0,
        "blocking_recall": (
            round(sum(1 for pair in positives.items() if pair in resolution.pool) / len(positives), 4)
            if positives and resolution.pool is not None else None
        ),
        "blocking_reduction_ratio": round(resolution.reduction_ratio, 4),
    }

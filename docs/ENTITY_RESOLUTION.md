# Entity resolution for the merger

A tutorial lakehouse loads the acquired company's customers and products into the parent's tables and
stops. In a real acquisition the two companies already share customers and sell near-identical products
under different IDs and different spellings, so the merged tables double-count them. This stage decides
which acquired-company records are the same real-world entity as a parent record.

```
parent records ─┐                                   ┌─ auto_match   (score ≥ 0.90, clear winner)
                ├─ normalise → block → score ─ decide ─ needs_review (0.75 ≤ score < 0.90, or ambiguous)
child records  ─┘                                   └─ no_match     (score < 0.75)
                                                          │
                                  Claude (optional, off by default) settles needs_review
                                                          │
                                  still unsure ─► silver.entity_match_review_queue ─► analyst override
                                                                                          │
                                         gold.xref_customers / xref_products  ◄───────────┘
```

Code: [entity_resolution.py](../consolidated_pipeline/common/entity_resolution.py) (pure Python, no Spark),
[llm_adjudicator.py](../consolidated_pipeline/common/llm_adjudicator.py),
[entity_resolution_delta.py](../consolidated_pipeline/common/entity_resolution_delta.py),
notebook [1_resolve_entities.py](../consolidated_pipeline/4_entity_resolution/1_resolve_entities.py).

## How a pair is scored

1. **Normalise.** Fold accents, lower-case, treat `&` as `and`, drop legal-form noise (`Pvt`, `Ltd`, `LLP`, `The`, …).
   Split what is left into *core* tokens (the brand) and *descriptor* tokens (`Foods`, `Dairy`, `Retail`, pack
   sizes such as `60g`). Cities are mapped to a canonical city (`Bangalore`, `Bengalore`, `Bengaluruu` → `Bengaluru`).
2. **Block.** Only pairs sharing at least one key are compared: a name token, a per-token phonetic code (Soundex), the
   first or last four letters, the whole-name Soundex, the letters of the name in sorted order (catches transposition
   typos), or a shared external ID. This cut the number of comparisons by about 90% with blocking recall of 1.0 on
   most seeds (see the table for the exceptions).
3. **Score.** `0.70 × core + 0.15 × descriptor + 0.15 × attribute`.
   *core* is token alignment or edit distance on the joined brand, calibrated so that a 1-letter typo scores about
   0.9 and two different brands that merely look alike score near 0. *descriptor* is the overlap of descriptor
   tokens, with partial credit when only one side has one (the other company just omitted it). *attribute* is city
   agreement for customers and category agreement for products; a missing value earns half credit.
4. **Decide.** ≥ 0.90 with a runner-up more than 0.03 behind is an automatic match. Between 0.75 and 0.90, or when two
   candidates are close, the pair goes to review. A parent claimed by two children keeps only the better claim.
   An exact shared external ID is a score of 1.0.

The hard case is `Primefuel Dairy` against `Primefuel Nutrition`: same brand, different business. Their descriptors
conflict, so they score about 0.85 when the city agrees (review) and are dropped when it does not.

The weights are hand-set, not learned. `MatchConfig` holds every number.

## Review loop

`silver.entity_match_review_queue` lists what is still undecided. An analyst resolves a pair with one insert and the
next run honours it over every automatic decision:

```sql
-- confirm a match
INSERT INTO fmcg.silver.entity_match_overrides
VALUES ('customers', '789412', '70045', 'your.name', current_timestamp());
-- confirm it is NOT a match (parent_code NULL)
INSERT INTO fmcg.silver.entity_match_overrides
VALUES ('customers', '789433', NULL, 'your.name', current_timestamp());
```

Add rows to `silver.entity_match_labels (entity, child_code, parent_code)` for children you have verified by hand and each
run records precision and recall against them in `silver.entity_match_metrics`. That, not the synthetic numbers below, is
how you measure this on real data.

## Using Claude for the ambiguous band

Off by default (`llm_enabled=false`). When on, only the review band is sent, one pair at a time, as
`{name, city or category}` for each side. No contact details, amounts or IDs leave the workspace.

- Model `claude-opus-5-5` by default (job parameter `llm_model`), structured JSON output, effort `low`.
- A reply is acted on only when its self-reported confidence is at least 0.8; otherwise the pair stays in the queue.
- A refusal, a truncated or malformed reply, or an API outage never decides a pair: it stays in the queue and is not cached.
- Decisions are cached by pair, model and prompt version in `silver.entity_match_llm_cache`, so reruns cost nothing and a
  deploy cannot flip earlier decisions.
- `llm_max_pairs` caps uncached calls per run (default 200). Token counts are recorded in `silver.entity_match_metrics`.
- Record text is passed as quoted data with an instruction not to follow it, so a customer named
  `Ignore previous instructions` is just a name.

Before enabling it on real data, confirm the data may be sent to an external model under your organisation's data
classification rules. Store the key in a Databricks secret scope (`secret_scope` parameter), never in a notebook.

**The LLM path has not been run against the live API.** It is covered by unit tests with a fake client that check the request
shape, caching, budget, failure handling and wiring into the matcher. The first live run is the real test.

## What the evaluation measures

[`benchmarks/evaluate_entity_resolution.py`](../benchmarks/evaluate_entity_resolution.py) scores the matcher on synthetic
scenarios with known truth: 250 parent customers, 58 acquired customers of which about 60% are noisy copies of a parent
customer, about 10% are same-brand impostors, and the rest are unrelated; 60 parent products and 36 acquired products.
Noise comprises typos, token reordering, split/joined words, dropped or swapped legal suffixes, dropped sector words, casing,
`The` prefixes, stale or misspelled cities, and flavour abbreviations.

Seeds were used in three groups. *dev* seeds were used to set the weights and thresholds. *inspected* seeds were looked at
after tuning, and their failures motivated two blocking keys; treat them as optimistic. *fresh* seeds had never been looked
at before the last code change and are the only numbers worth quoting.

| entity | seeds | precision (min / mean) | recall (min / mean) | resolved without a human | blocking recall (min) |
|---|---|---|---|---|---|
| customers | dev (3) | 1.000 / 1.000 | 0.833 / 0.880 | 0.898 | 1.000 |
| customers | inspected (20) | 1.000 / 1.000 | 0.833 / 0.928 | 0.911 | 1.000 |
| customers | **fresh (20)** | **0.969 / 0.994** | **0.861 / 0.924** | **0.909** | **0.944** |
| products | dev (3) | 1.000 / 1.000 | 0.952 / 0.984 | 0.991 | 1.000 |
| products | inspected (20) | 1.000 / 1.000 | 0.952 / 0.991 | 0.994 | 1.000 |
| products | **fresh (20)** | **1.000 / 1.000** | **0.905 / 0.983** | **0.990** | **1.000** |

On the fresh customer seeds (about 1,200 acquired customers, 720 true matches) there were 4 wrong automatic matches, and 9
true matches (1.25%) were never compared at all because blocking missed them (typos that break every key, for example a
transposition plus a missing letter). Everything else that was missed sits in the review queue, which a person or the LLM
can settle. Raising the review threshold from 0.70 to 0.75 cut the review queue from about 17 to about 5 items per run
without losing any true match, which is why 0.75 is the default.

**What this does not show.** The labels come from the same generator that made the noise, so this measures how well the
matcher undoes *those* noise types, not how it performs on a real customer master. Real data has failure modes the generator
does not (acronyms, trading names versus legal names, parent/subsidiary relationships, merged branches). Treat these
numbers as a regression floor and a description of the method. Real precision and recall need a hand-labelled sample.

Run it yourself: `make entity-eval`.

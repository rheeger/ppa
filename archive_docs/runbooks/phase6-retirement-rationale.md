# Phase 6 retirement rationale

This runbook documents two retirements that happened inside Phase 6:

1. The **original Phase 6** (LLM enrichment + budgets + run-scoped provenance from the v2 vision) was retired before any code was written because a Phase 2.875 / 2.9 audit showed it was already covered.
2. **Phase 6 Tier 3 (`MODULE_SEMANTIC` / semantic kNN seed linker)** was retired after roughly 15 calibration iterations because the approach turned out to be the wrong tool for the problem.

Phase 6 Tiers 1, 2, and 4 **shipped** and are delivering real value. The retirement history below is the honest trail of what was tried, what was learned, and what replaces each retired piece.

---

## Retirement 1 — original Phase 6 (LLM enrichment)

The v2 vision originally scoped Phase 6 as LLM enrichment with:

- `enrich-pending` / `enrichment-status` / `enrichment-revert` / `enrichment-report` commands
- An `enrichment_queue` → `enrichment_runs` pipeline with run_id-scoped provenance
- A $200 budget cap with the `BudgetTracker` class
- Pilot-batch review flow

**Why retired:** Phase 2.875 had already shipped the comprehensive enrichment system:

- `archive_sync/llm_enrichment/` — `EnrichmentOrchestrator`, `LlmEnrichmentRunner`, `CardEnrichmentRunner` with SQLite `InferenceCache`, staged JSONL output, ~461k threads processed through Gemini on 2026-04-13 (`_artifacts/_enrichment-runs/enrich-20260413-31fb894a/`).
- CLI commands already exist: `ppa enrich`, `ppa enrich-emails`, `ppa enrich-cards`, `ppa enrich-email-thread`, `ppa enrich-finance`, `ppa enrich-document`, `ppa enrich-imessage-thread`.
- Budget monitoring was already operational outside the PPA codebase (user infrastructure).
- Run-tracking already happens via `_artifacts/_enrichment-runs/<run_id>/` manifests.

**What replaced it:** A focused 3-tier plan closing genuine retrieval-quality gaps (graph edge trust, confidence-weighted retrieval, semantic kNN linker). The v2 vision narrative at Phase 6 should be treated as **historical context**; implementation truth is the phase plan at `.cursor/plans/phase_6_llm_enrichment_f286b0bd.plan.md` and the shipped code.

---

## Retirement 2 — Phase 6 Tier 3 (`MODULE_SEMANTIC` / semantic kNN seed linker)

### What it was

Tier 3 proposed a new seed-link module (`MODULE_SEMANTIC`) that used pgvector IVFFlat kNN over card embeddings to discover semantically-related card pairs that no deterministic linker could find. Each candidate pair went through an LLM judge (Gemini 2.0 Flash Lite → gpt-4o-mini fallback) to verify. Pairs that passed both the kNN threshold and the LLM gate would become `semantically_related` seed-link edges.

The theory: cosine similarity identifies pairs that "look semantically related," and an LLM verifier catches the false positives.

### What actually shipped + calibration trail

Written, calibrated extensively, then retired. All code preserved for reference:

- `archive_cli/seed_links.py` — `MODULE_SEMANTIC` constant, `_generate_semantic_candidates` (kNN with chunk-level overfetch), `LinkSurfacePolicy(LINK_TYPE_SEMANTICALLY_RELATED)`, formula branch in `evaluate_seed_link_candidate` (dual-tier: strict for same-type, lenient for cross-type).
- `archive_cli/migrations/003_semantic_linker.py` — `link_decisions.embedding_score` column (kept; harmless).
- `archive_scripts/phase6_precompute.py`, `phase6_iter_offline.py`, `phase6_turns.json` — offline calibration framework (precompute kNN + LLM judge cache once, iterate formulas against the cache for $0).
- `archive_scripts/phase6_apply_filter_offline.py`, `phase6_sample_for_review.py`, `phase6_multimodel_spot.py` — analysis scripts.
- `archive_tests/test_semantic_linker.py`, `test_semantic_linker_kNN.py` — updated to assert the retired state.
- `_artifacts/_phase6-iterations/` — cache-1020.json, cache-1pct\*.json, review markdown, 10-turn comparison JSON. **Do not delete**; they are the empirical evidence that informs Phase 6.5.

### Calibration journey (~15 iterations, ~$4 in LLM spend)

1. **First ship (conservative formula).** Original formula `0.45·det + 0.12·lex + 0.13·graph + 0.18·llm + 0.12·emb − risk`. For MODULE_SEMANTIC the det/lex/graph components are all 0 by construction, so the maximum achievable `final_confidence` was `0.30` — well below the 0.85 review floor. **The linker was dormant; 0 candidates surfaced.**
2. **5-turn iteration / 102 source cards (`cache-1020.json` predecessor).** Added a semantic-specific branch `0.60·llm + 0.40·emb`. All 17 surfaced candidates were same-type pairs (vaccinations ↔ vaccinations, etc.). Zero cross-type matches at that sample size.
3. **10-turn iteration / 1020 source cards.** Scaled the sample 10x. Cross-type emerged (965 cross-type pairs in cache, 826 with LLM verdict YES). Calibrated to multiplicative formula + min_llm 0.90 + min_emb 0.85 + require YES + auto_promote_floor 0.85.
4. **Multi-model spot check (300 pairs).** gpt-4o-mini vs gpt-4o: only **84% verdict agreement** — gpt-4o is more cautious (44 UNSURE vs 16). Gemini 2.0 Flash Lite: 88.6% agreement with gpt-4o-mini, 0 UNSURE verdicts (biased toward YES). Gemini 3.1 Flash Lite Preview: 53% agreement with gpt-4o — too erratic. Concluded: Gemini 2.5 Flash Lite was the best cost/signal choice for a production judge; gpt-4o at scale would cost ~$2,500 per sweep on the full vault.
5. **1pct sweep / 1914 source cards / `cache-1pct.json`.** 18,689 LLM-judged pairs, $1.87. At the calibrated gate: 7,129 surfaced, 6,170 auto-promote, 334 cross-type. Proved the approach scaled.
6. **Email classification filter (`cache-1pct-filtered.json`, `v2`, `v3`).** Dropped marketing/automated/noise/personal-classified email pairs. After backfilling the actual production `classify_index.db` (219k thread classifications from the 2026-04-13 enrich run, vs the earlier 10pct-slice 46k), the filter still caught junk correctly — but only ~5% of surfaced pairs were blocked because the rest were non-email or transactional-classified pairs.
7. **Type allowlist + same-type restriction + summary-template noise filter (Tier 4 / Step 24).** Dropped attachments, media_asset, Apple Health aggregates, github review emails, filename-pattern templates. Cache size dropped from 18,689 → 2,041 pairs. Surfaced count: 223 (down from 6,896). **The noise was finally tractable.**
8. **Dual-tier formula (policy v5).** Same-type strict gate (llm≥0.90, emb≥0.85); cross-type lenient gate (llm≥0.70, emb≥0.55). Surfaced 65 cross-type bridges (flight↔email_thread, accommodation↔calendar_event, vaccination↔medical_record, place↔finance, etc.) alongside 158 same-type pairs. Cost per sweep: $0.20.

At this point, the cross-type bucket looked good on inspection:

- `flight: "FW: HEEGER, ROBERT -- Newark, Jun 20"` ↔ `email_thread: "FW: HEEGER, ROBERT -- Newark, Jun 20"` (the booking email)
- `shipment: Zappos #55413859` ↔ `email_thread: "Your Zappos.com Tracking Number for Order #55413859"`
- `accommodation: "Reservation at Handlery Union Square Hotel"` ↔ `email_thread: "Hotel in SF"`
- `vaccination: "polio (oral)"` ↔ `document: "Robbie E Heeger immunization record"`

### Why we retired it anyway

During manual review of the surfaced candidates, two failure modes kept appearing that no further formula tuning could fix:

1. **Cosine can't distinguish "same kind" from "same instance."**  
   Example: `FedEx shipment 792237550300 delivered Nov 10` ↔ `FedEx Shipment 792491657549 Delivered` — different tracking numbers, both shipped FedEx. Cosine and the LLM both say "related" because the template language matches; neither reads the tracking number as a distinguishing fingerprint.

2. **1-to-N broadcast storms.**  
   Example: a single `document: "Endaoment Board of Directors meeting minutes, Sept 23, 2022"` gets linked to ~20 different Endaoment meeting transcripts. The doc is ABOUT Endaoment in general, not about any specific meeting. kNN returns all 20 because they all share the topic cluster. Each edge adds noise, not information.

**The root insight:** the connections we actually want (flight ↔ its confirmation email, purchase ↔ its credit-card charge, accommodation ↔ the trip's flight) are instance-level connections that require **structural fingerprints**: tracking numbers, confirmation codes, amounts, dates, ical_uids. Embedding similarity and LLM judgment work from content resemblance, which isn't the same as identity.

### What replaced it

Phase 6 Tiers 1, 2, and 4 keep their shipped value. For the actual cross-card-linking problem, **Phase 6.5** (drafted at `.cursor/plans/phase_6_5_<id>.plan.md`) extends the Phase 2.875 seed-link architecture with deterministic cross-derived-card linkers:

- `MODULE_FINANCE_RECONCILE` — match finance records to purchase/meal_order/ride/subscription/payroll by `amount` + date + merchant/counterparty.
- `MODULE_TRIP_CLUSTER` — accommodation ↔ flight/car_rental via date-overlap + city match.
- `MODULE_MEETING_ARTIFACT` — meeting_transcript ↔ calendar_event via shared `ical_uid` or title+start_at match.
- Edge-rule materialization fix — the `shipments.linked_purchase` field is already populated for 81% of shipments (by the extractor) but never gets materialized as an edge; trivial to fix.

Precision of these structural matchers: approximately 100% by construction (exact field match). Cost: $0. These build on, rather than compete with, the 22,496 existing `derived_from` edges Phase 2.875 already writes at extraction time.

### Empirical cost/value scorecard

| approach                                           | edges per sweep                      | precision | LLM cost                    | full-vault projected cost | full-vault projected useful edges |
| -------------------------------------------------- | ------------------------------------ | --------- | --------------------------- | ------------------------- | --------------------------------- |
| MODULE_SEMANTIC (Tier 3) at final calibration      | 223 surfaced / ~20-30 genuine new    | ~70-80%   | $0.20 per 1914-source sweep | ~$25                      | ~250                              |
| MODULE_SHIPMENT_TRACKING (probe, Phase 6.5 sketch) | 477 edges from just tracking numbers | ~100%     | $0                          | $0                        | 1500-2000                         |
| projected 4 Phase 6.5 structural modules combined  | —                                    | ~100%     | $0                          | $0                        | 3000-6000                         |

The structural approach produces ~10-20x more real edges at 0% of the LLM cost, and the precision is built into the rule (tracking numbers, amounts, ical_uids are instance-level fingerprints).

---

## Operational state after retirement

### `MODULE_SEMANTIC` code: kept, unwired

- `MODULE_SEMANTIC` constant, `_generate_semantic_candidates`, the semantic branch in `evaluate_seed_link_candidate`, and `LinkSurfacePolicy(LINK_TYPE_SEMANTICALLY_RELATED)` all remain in `archive_cli/seed_links.py`.
- `CARD_TYPE_MODULES` has every card type's tuple scrubbed of `MODULE_SEMANTIC`. **No `semanticLinker` jobs are emitted by `ppa seed-link-enqueue`.**
- `LLM_REVIEW_MODULES` no longer contains `MODULE_SEMANTIC`, so `llm_judge_candidate` short-circuits to the skip sentinel for any hypothetical semantic candidate.
- Migration 003 (`embedding_score` column on `link_decisions`) stays; the column defaults to 0 and is harmless when unused.
- Tests in `archive_tests/test_semantic_linker*.py` updated to assert the retired state, while still exercising the preserved code via `monkeypatch`.

### Phase 6 Tier 4 assets that paid for themselves

All kept, all valuable beyond semantic linking:

- **`slice-bootstrap --copy-from-schema=<src>`** — single command produces a fully-equipped slice schema (cards + chunks + embeddings copied, card_classifications copied, IVFFlat built) in ~2 minutes. First use saved 91 minutes and $0.62 on the 1pct slice vs the naive rebuild flow.
- **`embed-pending --copy-from-schema=<src>`** — the building block underneath slice-bootstrap's embedding step. Idempotent.
- **`card_classifications` projection table** + `archive_scripts/phase6_backfill_classifications.py` — durable triage classification storage, replacing the sidecar `_classify_index_*.db` SQLite pattern. Any future classification-based filtering (not just semantic linking) reads from here.
- **`triage_classification` + `triage_confidence` + `triage_card_types` + `triage_classified_at` + `triage_classify_model` fields on `EmailThreadCard`** — frontmatter-durable classification, so the data survives rebuilds and is visible to agents reading raw cards.
- **IVFFlat vector-index automation** — caught a 140x kNN slowdown bug during the precompute; now auto-built on slice bootstrap.

### How to opt-in-revive `MODULE_SEMANTIC` (if someone wants to)

```python
# In a one-off script or env-gated code path:
from archive_cli.seed_links import CARD_TYPE_MODULES, MODULE_SEMANTIC

# Re-attach to the card types you want to experiment on:
CARD_TYPE_MODULES["document"] = (*CARD_TYPE_MODULES["document"], MODULE_SEMANTIC)
# etc.

# Then enqueue + run a seed-link worker against those sources.
# Restore LLM_REVIEW_MODULES similarly if you want the LLM judge to run.
```

All the calibration tooling under `archive_scripts/phase6_*.py` still works — the code path is intact, just not in the default execution graph.

---

## Lessons learned (for future reference when similar ideas come up)

1. **At small sample sizes, semantic kNN appears to produce only same-type noise. Cross-type signal only emerges at ≥1000 source cards.** The first 5-turn 102-source sweep produced zero cross-type pairs and I almost concluded the approach was structurally impossible. The 1020-source sweep found 965. Always scale up before giving up on semantic approaches.

2. **LLM verdict agreement between "comparable" models is lower than you'd expect.** gpt-4o-mini vs gpt-4o: 84%. Gemini 2.5 Flash Lite vs gpt-4o: 76.5%. Gemini 3.1 Flash Lite Preview vs gpt-4o: 53%. If you're using a cheap judge, account for ~15-25% of its verdicts being "more permissive than gold-standard" — adjust gate thresholds accordingly, or ensemble.

3. **Precompute + offline-iterate is the right calibration pattern** for any LLM-in-the-loop system. We spent $4 on actual LLM calls and ran ~30 turns of formula/filter iterations against the cache for free. If we'd re-run the LLM on every turn, calibration would have cost ~$60+.

4. **The useful cross-type bridges semantic linker finds are almost all structural connections that exist at extraction time anyway.** Specifically, `flight ↔ email_thread` is already in `edges` via `flight.source_email`. `shipment ↔ email_thread` likewise. The bridges that _aren't_ already there (`finance ↔ purchase`, `accommodation ↔ flight`, `meeting_transcript ↔ calendar_event`) have clean structural predicates (amount+date, date-overlap+city, ical_uid) that don't need embeddings or LLM judges.

5. **Build deterministic structural linkers before embedding-based linkers.** Deterministic gives 100% precision for free; embedding gives ~70-80% precision at non-trivial cost. Only reach for embeddings when deterministic options are exhausted AND the pair type has genuinely unpredictable structural patterns. This ordering was inverted in the original Phase 6 plan and cost us several days.

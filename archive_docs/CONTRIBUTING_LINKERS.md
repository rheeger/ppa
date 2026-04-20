# Contributing a Linker

A linker finds relationships between two existing cards in your archive.
Connectors bring data in, extractors turn emails into structured cards,
and linkers wire those cards together — finding that this Amazon charge
corresponds to that Amazon order, that this flight and this hotel are part
of the same trip, that this transcript records that calendar event.

This guide assumes you've read `archive_docs/LINKER_ARCHITECTURE.md`.

## When to write a linker

Before writing code, answer these four questions:

1. **Is this an edge or a new card?** Linkers produce edges between existing
   cards. If your work creates entirely new cards, that's an extractor.
2. **Is there a structural fingerprint?** Linkers succeed when source and
   target share a deterministic identifier (amount, date, IATA code, ical_uid,
   confirmation code). Topical bridges ("documents about X" ↔ "transcripts
   about X") are a knowledge-cache concern, not a linker concern — see the
   Phase 6 Tier 3 retirement rationale in `runbooks/linker-retirement-protocol.md`.
3. **Do existing DeclEdgeRules cover it?** Check `archive_cli/card_registry.py`
   for a `DeclEdgeRule` on the relationship field you want to materialize. If
   yes, fix the materializer, don't write a linker.
4. **Can you express the tier ladder in ≤5 rows?** If not, your predicate is
   too complex. Redesign.

## The contribution workflow

```
1. Scaffold a new module file:
   archive_cli/linker_modules/{your_name}.py

2. Implement:
   - generator(catalog, source) -> list[SeedLinkCandidate]
   - scoring_fn(features) -> (det, lex, graph, emb, risk)
   - CatalogIndexSpec declarations for any module-specific indexes
   - LinkSurfacePolicy for new link types
   - register_linker(LinkerSpec(...)) at module bottom

3. Import from archive_cli/linker_modules/__init__.py

4. Unit tests: archive_tests/test_{your_name}_linker.py
   - One positive + one negative per tier
   - Negative control (unrelated fixtures -> 0 candidates)

5. Calibration:
   ppa linker calibrate --module yourCamelLinker --scope ppa_1pct
   ppa linker replay   --module yourCamelLinker --cache <path>

6. Per-module DoD (X.1-X.5):
   X.1 Implement
   X.2 Preview (--limit 25)
   X.3 Full 1pct dry-run
   X.4 Threshold iteration (offline)
   X.5 Comparison report + human gate (PROCEED / TIGHTEN / NARROW / SKIP-MODULE)
```

The reusable plan template at `.cursor/plans/_templates/linker.plan.md` has
the step-by-step DoD structure pre-baked.

## Tier ladder design

Structure predicates in decreasing precision:

```
TIER_A (1.00):  strongest structural fingerprint (e.g. exact primary-key)
TIER_B (0.90):  amount + date + fuzzy merchant
TIER_C (0.78):  amount + looser date + secondary signal (thread id)
TIER_D (0.55):  amount + date only  (review-only, retirable)
```

Every candidate stores its tier in `features["tier"]`, its score in
`features["deterministic_score"]`, and its risk penalty in
`features["risk_penalty"]`. The scoring_fn returns the 5-tuple from those.

Short-circuit: the first tier that matches a `(source, target)` pair wins;
lower tiers skip that pair for that target.

## Calibration discipline

- Run `--limit 25` first to catch crashes and verify tier distribution.
- Full 1pct dry-run writes `candidates-{date}.jsonl`. Iterate thresholds
  offline with `ppa linker replay` — no regen unless feature extraction
  changed.
- Spot-check 30 per band:
  - HIGH (auto-promote): 30/30 correct. No exceptions.
  - MEDIUM (auto-promote): ≥28/30 correct.
  - LOW (review-only): ≥24/30 correct; retire the tier if below.
- `ppa linker calibrate --module X --report` generates the markdown the
  human reviews.

## Submission

Your PR should include:

- The module file under `archive_cli/linker_modules/`.
- Unit tests under `archive_tests/`.
- A calibration cache JSONL + report under `_artifacts/_linkers/{module}/`.
- A plan file under `.cursor/plans/` following the template at
  `.cursor/plans/_templates/linker.plan.md`.

CI runs (future `linker-ci.yml`):

- All unit tests pass.
- `ppa linker info --module X` returns the registered spec (i.e. the
  `register_linker` call fires at import time without errors).
- `ppa linker list --json` includes your module with the expected fields.
- Retrofit-invariant check: all seven legacy modules' snapshot tests still
  pass (you haven't accidentally changed shared infrastructure).

## Retirement

If calibration shows your linker can't meet the HIGH-band precision bar
after iteration, that's fine — retire it. Flip `lifecycle_state="retired"`
in the LinkerSpec registration, document the retirement in
`archive_docs/runbooks/linker-retirement-protocol.md` appendix, and leave
the code in place for future revival. The `retired` state is visible in
`ppa linker list` so future operators see the history.

See the Phase 6 Tier 3 (`MODULE_SEMANTIC`) entry in the retirement protocol
for a worked example.

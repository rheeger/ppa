# Linker Retirement Protocol

Linkers are experiments. Some work, some don't. This runbook is the template
every retirement follows — each retirement becomes a new entry in Appendix A.

## Why a protocol

Retiring a linker cleanly means:

1. **Preserve the code.** The generator, scoring_fn, tests, and calibration
   cache stay in place — future contributors benefit from seeing what was
   tried.
2. **Unwire it from production.** `register_linker` sees
   `lifecycle_state="retired"` and skips `CARD_TYPE_MODULES` wiring, so the
   worker never enqueues jobs for it. No cost, no risk.
3. **Document the reason.** The calibration trail, the failure mode, and the
   decisive data point go in this runbook's appendix so future operators
   don't re-try the same thing blindly.
4. **Keep a revival path.** Flipping `lifecycle_state` back to `"active"` and
   re-enqueuing is a one-line change. The bar for revival is "new signal
   that wasn't available at retirement time."

## Retirement checklist

For each retirement, add an Appendix A entry (template below) AND:

- [ ] Flip `lifecycle_state="retired"` in the `register_linker` call.
- [ ] (Optional) Write `_artifacts/_linkers/_lifecycle_overrides.json` so
      ops can retire without a deploy. `ppa linker retire --module X` does
      this atomically.
- [ ] Confirm `ppa linker list --lifecycle retired` shows the module.
- [ ] Confirm `CARD_TYPE_MODULES` no longer contains the module for any
      source type (i.e. the worker will not enqueue it).
- [ ] Leave unit tests in place; they continue to cover the retired code
      so future revival starts from a green baseline.
- [ ] Reference the retirement entry from any related phase plan or vision
      doc.

## Appendix A — Retirement log

### A.1 — `MODULE_SEMANTIC` / Phase 6 Tier 3 / retired 2026-04-19

**What it was.** A seed-link module that used pgvector IVFFlat kNN over card
embeddings to discover semantically-related card pairs, with an LLM judge
(Gemini 2.0 Flash Lite → gpt-4o-mini fallback) to verify. Theory:
cosine similarity identifies pairs that "look semantically related," and
an LLM verifier catches the false positives.

**Calibration trail.** 15 iterations over ~$4 of LLM spend. Caches at
`_artifacts/_phase6-iterations/cache-1pct*.json`, review at
`_artifacts/_phase6-iterations/review-1pct-20260419.md`, final calibration
report at `_artifacts/_semantic-linker-calibration/calibration-20260419.md`.
Full narrative in `archive_docs/runbooks/phase6-retirement-rationale.md`
(the Phase-6-specific file that predates this protocol; retained for history).

**Decisive data point.** At final calibration (policy v5, dual-tier gate,
type allowlist + classification filter), 223 candidates surfaced on the 1pct
slice. Manual review showed two irreducible failure modes:

1. Cosine can't distinguish "same kind" from "same instance." Two FedEx
   shipments with different tracking numbers register as related because
   template language matches.
2. 1-to-N broadcast storms: a board-minutes document linked to ~20 different
   Endaoment meetings because they all share the topic cluster.

No further formula tuning could fix these — the predicate is structurally
wrong for instance-level linking.

**What replaced it.** Phase 6.5 structural linkers:
`MODULE_FINANCE_RECONCILE`, `MODULE_TRIP_CLUSTER`, `MODULE_MEETING_ARTIFACT`.
Precision ~100% by construction (exact field match), $0 LLM cost, ~10-20x
more useful edges per sweep.

**Revival criterion.** A credible approach for distinguishing instance-level
from kind-level similarity (e.g. embeddings augmented with structured-field
features, or a model explicitly trained on instance disambiguation). Until
then, cosine-plus-LLM remains the wrong tool for this problem.

**Code disposition.** All code in place. `LinkerSpec` registered with
`lifecycle_state="retired"`. `register_linker` automatically keeps it out
of `CARD_TYPE_MODULES`, so `ppa seed-link-enqueue` never emits jobs for it.
Opt-in revival:

```python
from dataclasses import replace
from archive_cli.linker_framework import ALL_LINKERS, register_linker, unregister_linker

spec = ALL_LINKERS["semanticLinker"]
unregister_linker("semanticLinker")
register_linker(replace(spec, lifecycle_state="active"))
```

Or write an override file:

```bash
ppa linker revive --module semanticLinker
```

### A.2 — `RECONCILE_TIER_PHASE2875_LLM_SIGNAL` / finance reconcile / retired 2026-04-23

**What it was.** A second tier inside `MODULE_FINANCE_RECONCILE` that read Phase
2.875 `enrich_finance/match_candidates.jsonl` and tried to pair finance cards
with derived transaction cards using LLM-signed amount signals.

**Decisive data point.** Rows in `match_candidates.jsonl` use
`target_card_type='email_message'` — the enrichment found the _source email_,
not the merchant-side purchase/meal_order/etc. card. The tier either fired
zero useful edges or would have promoted the wrong hypothesis if wired
loosely.

**What replaced it.** The remaining ladder: `TIER_SOURCE_EMAIL`, `TIER_HIGH`,
`TIER_MEDIUM`, `TIER_LOW` only. A correct 2-hop (finance → email → derived)
would be redundant with `TIER_SOURCE_EMAIL` when email linkage exists.

**Code disposition.** Loader and matcher removed from
`archive_cli/linker_modules/finance_reconcile.py`. Module stays active; only
the tier is gone.

## Appendix B — Retirement entry template

When retiring a new linker, copy this skeleton into Appendix A:

```markdown
### A.N — `MODULE_X` / Phase Y / retired YYYY-MM-DD

**What it was.** One-paragraph description.

**Calibration trail.** Artifact paths under `_artifacts/_linkers/{module}/`.

**Decisive data point.** The one measurement that made retirement
unavoidable.

**What replaced it.** Pointer to the new approach, or "nothing — the
problem is deferred to Phase Z."

**Revival criterion.** What signal would justify trying again.

**Code disposition.** `lifecycle_state` flipped; tests retained.
```

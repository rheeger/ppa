# Linker quality gates — the precision-first standard

This runbook is the authoritative standard for how we judge a linker's
output. It exists because Phase 6.5 Step 1a was almost shipped against an
arbitrary "≥50% finance.source_email coverage" gate that produced
categorically wrong links. Coverage is **not** a quality metric. Precision
is. This runbook codifies that.

Applies to:

- `archive_cli/linker_modules/{X}.py` linkers (Phase 6.5)
- legacy linkers in `archive_cli/linker_modules/{identity,communication,calendar,media,orphan,graph}.py`
- the `LinkSurfacePolicy` floors in `archive_cli/seed_links.py`
- upstream enrichment writers that _feed_ a linker (e.g. the
  `match_resolver.py` writer that populates `finance.source_email`)

If a piece of code creates an edge that ends up in `ppa.edges` or writes a
field on a card that another linker consumes as ground truth, this gate
applies.

---

## The standard

A link is allowed to **auto-promote** to a real edge in
`PPA_INDEX_SCHEMA=ppa` only when one of these is true:

### 1. Deterministic identity match

The link is justified by a single field whose semantics are _identity_:

- exact `ical_uid` match
- exact `gmail_thread_id` / `gmail_message_id` match
- exact `imessage_chat_id` match
- exact `external_id` (provider-issued) match — e.g. `purchase.order_number`,
  flight confirmation code
- exact slug / UID equality

These are reserved for tiers with `deterministic_score >= 0.95`. They are
also the only situations where a _single_ signal can carry an
auto-promote.

### 2. Multi-variable corroboration in tight bounds

The link is justified by **at least three** independent signals, each of
which is in a tight bound. "Tight bound" means:

| signal class                     | tight bound                                                                |
| -------------------------------- | -------------------------------------------------------------------------- |
| money amount                     | exact cents (`abs(a-b) <= 0.01`)                                           |
| timestamp delta — transactions   | ≤ 2 days                                                                   |
| timestamp delta — meetings       | ≤ 15 minutes                                                               |
| timestamp delta — travel         | ≤ 24 hours from a stay-window endpoint                                     |
| merchant / counterparty / vendor | normalized merchant token agreement (per `merchant_normalizer`)            |
| city / location                  | exact normalized city match (or fixed-list synonym), not lenient substring |
| participant overlap              | ≥ 2 distinct participants, both also normalized                            |
| same currency                    | identical ISO currency code                                                |

Three of those, in agreement, with no flag from the upstream provenance
review, qualifies for auto-promote.

### 3. Anything else is review-only or rejected

- Two-signal agreement → **review band**, never auto-promote.
- One-signal agreement (amount alone, date alone, merchant alone, city
  alone, participants alone, embedding alone) → **rejected**.
- Tiers that read fields written by upstream LLM disambiguators (e.g.
  `finance.source_email` written by `match_resolver.py`) **must
  independently corroborate** the link with at least one tight-bound
  signal. The wikilink alone is never enough.

This is the rule violated in Step 1a. The wikilink was written by an LLM
disambiguator with a low threshold, then `financeReconcileLinker.TIER_SOURCE_EMAIL`
trusted it as if it were ground truth and emitted 0.98 confidence.

---

## Per-tier checklist (every new or modified tier must answer all four)

Before adding or changing a linker tier, the change description in code
review must answer:

1. **Identity or corroboration?** Which path of "the standard" justifies
   the tier? If corroboration, list the three independent signals.
2. **What are the bounds?** Quote exact cent / minute / day numbers.
   Anything outside the table above must be defended.
3. **What's the failure mode?** Describe one concrete way this tier could
   produce a false positive on the seed vault, and the predicate that
   prevents it.
4. **What's the calibration sample?** Cite the artifact path under
   `_artifacts/_linkers/{module}/calibration/` showing a stratified
   sample of ≥30 candidates, with verdicts, achieving ≥95% precision.

A tier without all four cannot move above the `auto_review_floor`.

---

## `LinkSurfacePolicy` floors

`auto_promote_floor` is the threshold above which a candidate becomes an
edge automatically. `auto_review_floor` is the threshold above which the
candidate goes to `link_review_queue` for human inspection.

The floors must be set so that:

- Every tier whose `deterministic_score` ≥ `auto_promote_floor` passes
  the per-tier checklist above.
- Every tier with `deterministic_score` between `auto_review_floor` and
  `auto_promote_floor` is intentionally review-only.
- A tier whose precision review came in below 95% must either tighten
  predicates until it passes again, or have its `deterministic_score`
  dropped below `auto_promote_floor`, or be retired.

---

## Calibration protocol

Before any tier auto-promotes against `PPA_INDEX_SCHEMA=ppa`:

1. **Generate** at least 30 candidates per tier on the 1pct slice (or seed
   if 1pct is insufficient).
2. **Stratify** the sample so it includes:
   - candidates near the lower bound of each predicate (e.g. exactly at
     the date-delta limit)
   - duplicate-pressure cases (one target referenced by many sources)
   - generic / template cases (e.g. recurring "Working Block" calendar
     events, marketing receipts)
3. **Manually verify** each pair, recording verdicts as `TP / FP / unclear`.
4. **Compute precision** as `TP / (TP + FP)`. Target: **≥95%** for any
   tier that auto-promotes.
5. **Reject or downgrade** tiers below 95%. Either tighten predicates,
   move below the auto-promote floor, or retire (with `linker-retirement-protocol.md`).
6. **Commit** the calibration report at
   `_artifacts/_linkers/{module}/calibration/report-{date}.md` with the
   verdicts inline. The report is the audit trail; the link decision is
   not committed without it.

---

## Upstream-feeder gate (the Step 1a lesson)

When a linker reads a field that was written by an LLM-driven workflow
(prompt + disambiguator), the _workflow_ is also a linker for the purpose
of this standard.

- The workflow must produce its own calibration report at the same
  precision target before its writes can feed an auto-promoting tier.
- A linker that consumes such a field but **also** corroborates with
  tight-bound signals (path 2 in the standard) does not need the
  workflow itself to hit 95%, because the corroboration insulates the
  edge.
- A linker that relies _solely_ on the workflow's output (e.g. the
  current `RECONCILE_TIER_SOURCE_EMAIL` reading `finance.source_email`)
  must either be downgraded to review-only, or rewritten to corroborate.

Practical implication for the current `finance.source_email` data: the
broad-pass writes from the 2026-04-26 Step 1a run failed precision
review and were pruned. The remaining `source_email` set has not yet
been certified, so any tier that consumes it must corroborate
independently until the upstream writer is calibrated.

---

## What "verifiable accuracy" means in practice

The `link_evidence` table already records every feature that contributed
to a candidate's score. We use that:

- `archive_query` and `archive_graph` consumers can read the evidence
  payload and surface the predicate that justified the edge.
- Promotion artifacts under `_artifacts/_linkers/{module}/promotion/`
  must include the per-tier candidate counts and the calibration report
  hash so an auditor can trace any edge back to its evidence.
- Reverting a bad calibration is then surgical: drop edges by
  `(module_name, tier)` from `link_decisions` + `edges` tables.

A link in production must satisfy:

- it is in `ppa.edges`
- the corresponding `ppa.link_decisions` row has a `final_confidence`
  that meets the active `LinkSurfacePolicy` floor
- the corresponding `ppa.link_evidence` row(s) cite predicates from this
  runbook's tight-bound table
- the linker's calibration report covering its tier is committed and
  ≥95% precision

If any of those is missing, the edge should not be in production.

---

## Coverage is context only

We report coverage (`% finance cards with source_email`, `% transcripts
with linked calendar`, etc.) for situational awareness:

- It tells us how much _opportunity_ a linker has.
- It tells us which fields enrichment workflows under-populated.

It does **not** tell us anything about correctness. Coverage MUST NOT
appear as a pass/fail gate in any plan, runbook, or commit.

The 2026-04-26 Step 1a run took finance.source_email from 2.67% to
58.36%. Coverage went up; the review packet showed roughly 4,730 of
those 5,004 newly-added links were `high` or `medium` risk and largely
wrong. Coverage rose, quality fell. We pruned back to 5.72%. That's the
canonical example.

---

## Owner / review

- This runbook is owned by the linker framework maintainer.
- Updates require a Phase 6.5 plan amendment and a calibration report
  showing the new gate is met.
- Linked from `archive_docs/LINKER_ARCHITECTURE.md` and the Phase 6.5
  plan as the precision-first contract.

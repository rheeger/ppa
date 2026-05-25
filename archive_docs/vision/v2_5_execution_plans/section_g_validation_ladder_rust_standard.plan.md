# Section G Execution Plan - Validation Ladder and Rust Execution Standard

## Objective

Define the required path from v2.5 implementation work to safe production completion on Arnold.

This plan exists to prevent v2.5 from moving directly from design to production mutation. Every v2.5 change must graduate through a validation ladder:

```text
synthetic fixtures -> small slice -> larger slice -> local seed dry-run -> local seed staging apply -> Arnold dry-run -> Arnold reviewed apply -> Arnold soak/readiness
```

It also defines the Rust execution standard. v2.5 should use the Rust engine for vault scanning, cache reads, materialization, chunking, and slice/seed validation wherever the existing Rust engine supports the path.

## Non-Goals

- Do not implement the validation harness in this planning pass.
- Do not run Arnold cleanup from this plan.
- Do not require physical vault pruning.
- Do not require a new Rust rewrite for v2.5.
- Do not bypass Python code where Rust does not already own the path.
- Do not treat wall-clock speed as a reason to skip correctness gates.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_docs/phase_2_9_audit.md`
- `ppa/archive_docs/reports/archive_crate-benchmark-tier2-baseline.json`
- `ppa/archive_crate/`
- `ppa/archive_cli/ppa_engine.py` or equivalent engine selection helpers
- `ppa/archive_cli/vault_cache.py`
- `ppa/archive_cli/materializer.py`
- `ppa/archive_cli/loader.py`
- `ppa/archive_tests/`
- `ppa/archive_scripts/`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_b_current_arnold_cleanup.plan.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_f_arnold_observability_v3_gate.plan.md`

## Agent Handoff Checklist

Before implementation:

- Read `README.md`, `v2.5vision.md`, and this plan before any other v2.5 implementation work.
- Implement the report/gate vocabulary before Section B or Arnold apply code exists.
- Make command refusal rules testable.
- Make engine-mode reporting mandatory for long corpus jobs.

Likely implementation files:

- shared report/gate module.
- CLI guard utilities.
- test helpers for fixture/slice/seed/Arnold gate metadata.
- status/readiness module consumed by Section F.

Required first tests:

- Arnold apply is refused without reviewed Arnold dry-run decision run.
- report includes ladder gate and engine mode.
- readiness fails if required prior gate evidence is missing.
- full reclassification/embedding/linker flags are opt-in.

Stop conditions:

- an implementation path can mutate Arnold without Section G evidence.
- reports cannot distinguish fixture/slice/seed/Arnold runs.
- long-running jobs do not emit JSON reports.
- Python fallback hides Rust/Python divergence.

## Core Rule

No v2.5 apply path may run against Arnold until the same operation has passed:

1. synthetic tests.
2. real slice dry-run.
3. real slice apply/rollback.
4. larger slice apply/rollback.
5. local seed dry-run.
6. local seed staging apply/rollback.
7. Arnold dry-run with reviewed report.

Arnold is the final production target, not the first place the workflow proves itself.

## Validation Ladder

### Gate 0: Documentation Readiness

Purpose:

- Confirm the implementation plan is specific before code changes begin.

Required evidence:

- Section A-G execution plans are present.
- Each plan names objective, non-goals, data model choices, CLI behavior, rollout, tests, rollback, reporting, and definition of done.
- Open design questions are resolved or explicitly marked as blockers.

Exit criteria:

- v2.5 implementation work can begin.

### Gate 1: Synthetic Fixtures

Purpose:

- Validate policy and processor behavior without production data volume.

Required coverage:

- `EmailPromotionPolicy` examples from Section A.
- classification precedence from Section B.
- Gmail promotion/suppression/quarantine outcomes from Section C.
- source updater batch accounting from Section D.
- processor staleness/version/corpus-state rules from Section E.
- status/readiness calculations from Section F.

Exit criteria:

- Unit tests pass.
- No real vault mutation.
- No LLM calls unless an explicit test fixture stubs them.

### Gate 2: Small Real Slice

Purpose:

- Prove behavior against real vault data with bounded blast radius.

Requirements:

- Use a real slice that includes marketing, transactional, personal, attachment, derived-card, and unknown-classification examples.
- Run corpus-hygiene dry-run.
- Review sample buckets.
- Apply only to the slice or slice schema.
- Roll back.
- Rebuild slice and verify suppressed records do not reappear as active.

Exit criteria:

- dry-run deterministic.
- apply/rollback works.
- rebuild safety passes.
- suppressed email absent from default retrieval, vector retrieval, enrichment queue, and link candidates.

### Gate 3: Larger Real Slice

Purpose:

- Surface runtime and scale issues before seed.

Requirements:

- Use a larger slice than Gate 2.
- Run with progress logging and JSON reports.
- Capture wall-time and throughput for scan, decision generation, apply, rebuild validation, embedding filtering, and linker filtering.
- Confirm new LLM calls are limited to missing/stale classification.

Exit criteria:

- Runtime is bounded and explainable.
- Report sizes are manageable.
- No full reclassification.
- No full embedding/link reruns except explicitly requested validation jobs.

### Gate 4: Local Seed Dry-Run

Purpose:

- Evaluate the complete current seed corpus without mutation.

Requirements:

- Read the seed locally.
- Generate dry-run decisions and reports.
- Use existing classification stores first.
- Produce classification coverage, suppression counts, quarantine counts, derived-card impact, and estimated index impact.
- Do not apply.

Exit criteria:

- Operator reviews dry-run report.
- Classification reuse rate meets expectation.
- New LLM call count is understood and acceptable.
- No unexpected high-risk buckets.

### Gate 5: Local Seed Staging Apply

Purpose:

- Prove apply and rollback at seed scale without touching the canonical seed or Arnold.

Requirements:

- Use a copied vault, staging schema, or staging corpus state store.
- Apply a reviewed decision run.
- Validate search/hybrid/vector/linker suppression behavior.
- Validate derived-card preservation.
- Run incremental rebuild validation.
- Run full rebuild validation if the implementation touches materialization or corpus-state projection.
- Roll back and verify prior active state.

Exit criteria:

- staging apply passes.
- rollback passes.
- rebuild safety passes.
- no canonical seed mutation.

### Gate 6: Arnold Dry-Run

Purpose:

- Evaluate production state without mutation.

Requirements:

- Run dry-run only.
- Produce human and JSON reports.
- Record Arnold source cursors, schema, classification stores, and decision run ID.
- Review samples for high-confidence suppressions, quarantines, active overrides, derived-card impacts, and unknown classifications.

Exit criteria:

- dry-run reviewed.
- apply candidate approved.
- rollback point identified.
- no production mutation has occurred.

### Gate 7: Arnold Reviewed Apply

Purpose:

- Apply the reviewed corpus-state changes to Arnold safely.

Requirements:

- Apply only a dry-run decision run generated on Arnold.
- Revalidate that source cursors and major counts did not drift unexpectedly.
- Persist decision records before any active-state changes.
- Apply DB-first / status-store-first.
- Do not delete vault markdown.
- Write apply report.
- Run post-apply retrieval, embedding, enrichment, linker, and rebuild-safety checks.

Exit criteria:

- apply succeeds.
- suppressed email absent from default surfaces.
- derived cards preserved.
- rollback remains available.

### Gate 8: Arnold Soak and v3 Readiness

Purpose:

- Prove Arnold stays healthy through normal maintenance.

Requirements:

- Run normal maintenance cycles.
- Verify future Gmail sync uses classify-before-promotion.
- Verify source freshness status.
- Verify processor backlog and failures.
- Verify suppressed email does not return after rebuild or maintenance.
- Verify v3 readiness gate from Section F.

Exit criteria:

- Arnold status reports ready for v3.
- failures, if any, are documented and resolved.

## Rust Execution Standard

v2.5 should default to the Rust engine for hot paths already validated in Phase 2.9.

Required standards:

- Use `PPA_ENGINE=rust` for slice, seed, and Arnold validation unless explicitly testing Python parity.
- Use Rust vault cache for census and type-filtered scans where possible.
- Use Rust materialization/chunking paths for rebuild-safety validation.
- Avoid Python full-vault walks for corpus hygiene unless debugging parity or unsupported edge cases.
- Treat Rust/Python divergence as a blocking validation issue before Arnold apply.
- Capture engine mode in every dry-run/apply/report.

Rust-owned or Rust-accelerated paths to prefer:

- vault walking and cache build.
- frontmatter parsing / content hashing where already wired.
- type-filtered note scans from vault cache.
- materializer row batch path.
- chunk building and chunk hashing.
- person index / resolution paths where applicable.

Python remains appropriate for:

- adapter orchestration.
- Gmail/Calendar/iMessage/Photos provider logic.
- policy glue until a Rust rewrite is justified.
- LLM classification/extraction calls.
- CLI/report formatting.

## Long Wall-Time Guardrails

Every v2.5 execution plan should respect these guardrails:

- Never classify all email by default.
- Never embed all chunks by default.
- Never run all linkers by default.
- Never apply to Arnold without reviewed dry-run.
- Prefer metadata, classification stores, and indexed projections over full vault walks.
- Use count/sample modes before full reports.
- Checkpoint long-running dry-runs and applies.
- Emit progress for scan, classify reuse, decision generation, apply, rebuild validation, embedding filtering, and linker filtering.
- Write JSON reports for every long operation.
- Include throughput and elapsed runtime in reports.

## Seed and Arnold Safety Rules

Seed safety:

- The canonical seed is never the first apply target.
- Local seed apply must use a copied vault, staging schema, or staging corpus-state store.
- Rollback must be validated on staging before any production apply.

Arnold safety:

- Arnold dry-run precedes Arnold apply.
- Arnold apply uses a decision run generated on Arnold.
- Arnold apply is DB-first and non-destructive.
- Arnold apply never prunes markdown in v2.5.
- Arnold rollback remains available after apply.
- Arnold readiness requires post-apply soak.

## Required Report Fields

Every ladder run should include:

- ladder gate name.
- archive instance: fixture, slice, larger slice, seed staging, Arnold dry-run, Arnold apply.
- engine mode: `rust`, `python`, or parity.
- vault path and schema.
- decision run ID.
- policy version.
- classification source counts.
- new LLM call count.
- active/suppressed/quarantine counts.
- dirty processor count.
- embedding/linker affected counts.
- elapsed runtime and throughput by phase.
- warnings/errors.
- next gate recommendation.

## Tests and Validation

Future implementation should add tests or scripts that prove:

- Gate 1 synthetic fixtures pass without real vault access.
- Gate 2 slice apply/rollback passes.
- Gate 3 larger slice produces bounded reports and no full reclassification.
- Gate 5 seed staging apply does not mutate canonical seed.
- Rust engine mode is recorded in reports.
- Unsupported Python fallbacks are visible.
- Arnold readiness cannot pass unless prior gates are recorded.

## Definition of Done

Section G implementation is ready when:

- The validation ladder is encoded in docs, scripts, or status checks.
- v2.5 apply commands refuse Arnold apply without a reviewed Arnold dry-run decision run.
- Reports include ladder gate and engine mode.
- Slice and seed staging apply/rollback are proven before Arnold apply.
- Rust engine is the default for supported scan/cache/materialization/chunking validation paths.
- Wall-time guardrails prevent accidental full reclassification, embedding, or linker reruns.
- Arnold readiness depends on passing the ladder and the Section F gate.
- Standard exit codes distinguish success, runtime failure, validation failure, unsafe-operation refusal, and external dependency blockage.
- Required artifact paths are stable enough for `ppa status` and future agents to locate reports.

## Completion Artifacts

The implementation agent must leave:

- validation ladder state/report schema.
- command refusal tests.
- report artifact directory convention.
- engine-mode reporting test.
- Arnold apply refusal test without reviewed Arnold dry-run.
- full reclassification/full embedding/all-linker opt-in tests.
- readiness fixture showing missing gate evidence fails closed.

## Commit Instructions

Commit this section by itself.

- Start only from a clean tree.
- Stage only Section G implementation, tests, docs, and artifacts.
- Commit subject: `v2.5 section G: validation ladder rust standard`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before Section A work starts.

# Section B Execution Plan - Current Arnold Corpus Cleanup

**Status (Aug 2026, HEAD `b57136f`):** First-pass CCS apply/rollback landed earlier (DB-first). **This campaign applied vault-remove of suppressed marketing on the canonical seed** `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` schema `ppa` (~502,622 files deleted; index + Gmail ledger purged). No `rollback.json`; rollback cannot restore those deletes.

**Product fork (locked):** suppressed marketing is **deleted**. Quarantine **stays** as labeled cards (`retrieval_weight=0.35`). Older B text that said delete quarantine too is superseded.

**Do not re-run vault-remove.** Remaining B work: quadratic UID collect + persist `rollback.json` on **future** applies. Arnold prune is out of scope.

## Objective

Specify how v2.5 cleans the email corpus already present in the living archive.

The cleanup must:

- Reuse existing classification data before making any new LLM calls.
- Convert raw marketing/bulk/noise email from active archive artifacts into suppressed ledger records.
- Preserve useful derived structured cards that still have an independent reason to exist.
- Keep the process deterministic, reviewable, rebuild-safe, and rollback-safe.
- **Match inbound Gmail (locked fork):** suppressed notes are not in the vault (deleted + purged + ledger). Quarantine notes **stay** as labeled cards. Do not delete quarantine.

## Non-Goals

- Do not implement cleanup in this planning pass (first-pass CCS tooling already exists).
- Do not re-run vault-remove on this seed (already applied). Do not physically prune Arnold from this section. Do not delete quarantine cards.
- Do not rerun classification over all email.
- Do not remove useful derived cards just because their source raw email is suppressed.
- Do not change Gmail as the source of record for suppressed bulk email.
- Do not hide or silently discard suppression decisions.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_a_email_corpus_semantics.plan.md`
- `ppa/archive_sync/llm_enrichment/classify_index.py`
- `ppa/archive_sync/llm_enrichment/classify.py`
- `ppa/archive_sync/llm_enrichment/enrich_runner.py`
- `ppa/archive_sync/llm_enrichment/workflows/email_thread.py`
- `ppa/archive_vault/schema.py`
- `ppa/archive_cli/schema_ddl.py`
- `ppa/archive_cli/materializer.py`
- `ppa/archive_cli/embedder.py`
- `ppa/archive_cli/seed_links.py`
- `ppa/archive_cli/index_query.py`
- `ppa/archive_cli/server.py`

## Agent Handoff Checklist

Before implementation:

- Read `README.md`, `v2.5vision.md`, Section A, Section G, and this plan.
- Verify `EmailPromotionPolicy` fixture tests pass before building cleanup.
- Implement dry-run census before any apply code.
- Implement slice apply/rollback before local seed staging.
- This machine’s canonical seed **already received** hygiene apply. Do not copy the seed first. Do not apply to Arnold from this section.

Likely implementation files:

- new `archive_cli/commands/corpus_hygiene.py` or equivalent.
- new corpus decision store module.
- materializer/index filtering only after dry-run and staging apply are proven.
- tests under `archive_tests/` for classification precedence, dry-run determinism, apply, rollback, and rebuild safety.

Required first tests:

- no LLM call when `card_classifications` or `ClassifyIndex` has reusable classification.
- dry-run is deterministic.
- suppressed email disappears from default retrieval in a slice/staging schema.
- rollback restores prior corpus state.

Stop conditions:

- dry-run wants broad new LLM classification.
- report counts are nondeterministic.
- apply path cannot roll back.
- suppressed markdown reappears as active after rebuild.
- any path would mutate Arnold before Arnold dry-run review.

## Required Future Command Shape

Implementation should introduce a documentation-backed command family like:

```bash
ppa corpus-hygiene email census --dry-run --format text
ppa corpus-hygiene email census --dry-run --format json --output /path/report.json
ppa corpus-hygiene email sample --bucket high_confidence_marketing --limit 50
ppa corpus-hygiene email apply --decision-run RUN_ID
ppa corpus-hygiene email rollback --decision-run RUN_ID
```

Exact names can change during implementation, but the behavior cannot:

- `census --dry-run` computes decisions and writes no active-corpus changes.
- `sample` produces reviewable examples from deterministic buckets.
- `apply` applies a previously generated decision run.
- `rollback` restores previous active/suppressed/quarantine state without LLM calls.

## B1. Freeze Policy and Snapshot Requirements

Before cleanup, implementation should create a decision run:

| Field | Meaning |
| ----- | ------- |
| `decision_run_id` | Stable ID for this dry-run/apply cycle |
| `policy_version` | `EMAIL_PROMOTION_POLICY_VERSION` used |
| `created_at` | Run creation time |
| `archive_instance` | Arnold production archive identity |
| `index_schema` | Postgres schema being evaluated |
| `vault_path` | Vault path |
| `classification_sources` | Inputs used for classification reuse |
| `dry_run_report_path` | Path to JSON report |

Snapshot requirements:

- Snapshot source cursors from `_meta/sync-state.json`.
- Snapshot or reference current `_artifacts/_classify_index*.db` files.
- Snapshot the current `card_classifications` row count and hashable export.
- Snapshot current active counts for `email_thread`, `email_message`, `email_attachment`.
- Snapshot current chunk and embedding counts for email cards.
- Snapshot ingestion log min/max IDs or timestamps relevant to the run.
- Record git commit SHA and PPA schema version if available.

The snapshot does not need to copy the entire vault for the first cleanup pass, but it must be enough to verify and roll back active/suppressed state.

## B2. Canonical Classification Reuse

The cleanup implementation should build a canonical decision input for every Gmail thread currently represented in Arnold.

Classification precedence:

1. `card_classifications` rows in Postgres.
2. `_artifacts/_classify_index*.db` / `ClassifyIndex` rows from prior enrichment runs.
3. `email_thread.triage_classification` frontmatter.
4. Deterministic Stage 0 gates from `known_senders.py` / enrichment runner.
5. New LLM classification only when no reusable classification exists or content hash changed.

New LLM calls should be counted and reported separately. A normal cleanup run should aim for near-zero model calls if existing classification coverage is high.

### Decision Store

Implementation should introduce a durable decision store. It may be Postgres or SQLite, but it must be:

- durable across rebuilds.
- queryable by Gmail thread ID.
- exportable for review.
- versioned by policy.
- able to retain decision history for rollback.

Recommended logical table: `email_corpus_decisions`.

Required fields:

| Field | Meaning |
| ----- | ------- |
| `decision_run_id` | Links decisions to one dry-run/apply cycle |
| `source_key` | Source/account identity |
| `account_email` | Gmail account |
| `gmail_thread_id` | External thread ID |
| `gmail_history_id` | Gmail history marker |
| `thread_body_sha` | Content hash used for staleness |
| `thread_uid` | Existing `email_thread` UID, if present |
| `message_uids` | Existing message UIDs |
| `attachment_uids` | Existing attachment UIDs |
| `derived_uids` | Derived cards with provenance to this email |
| `classification` | Raw classification |
| `canonical_classification` | Normalized classification |
| `confidence` | Classifier confidence |
| `card_types` | Extractable card types |
| `classification_source` | Source used by precedence rule |
| `classify_prompt_version` | Prompt version, if known |
| `classify_model` | Model, if known |
| `policy_version` | Promotion policy version |
| `previous_corpus_state` | Existing state before apply |
| `corpus_decision` | `active`, `suppressed`, or `quarantine` |
| `processor_decision` | Downstream processor decision |
| `decision_reason` | Primary reason key |
| `decision_signals` | JSON of labels/overrides/signals |
| `applied_at` | Empty until apply |

## B3. Dry-Run Census Spec

`ppa corpus-hygiene email census --dry-run` should produce both a human-readable summary and JSON report.

Required summary sections:

1. **Run metadata**
   - decision run ID.
   - policy version.
   - vault path and schema.
   - classification source counts.
   - new LLM calls required.

2. **Current corpus**
   - existing active `email_thread`, `email_message`, `email_attachment` counts.
   - existing email chunks.
   - existing email embeddings.

3. **Classification coverage**
   - counts by canonical classification.
   - unknown/unclassified count.
   - low-confidence count.
   - stale-content count.

4. **Proposed corpus decisions**
   - active.
   - suppressed.
   - quarantine.
   - unchanged.
   - newly demoted from active to suppressed/quarantine.

5. **Decision reasons**
   - `marketing_classification`
   - `promotions_label`
   - `automated_notification`
   - `noise_classification`
   - `owner_participation_override`
   - `starred_important_override`
   - `transactional_extractable`
   - `low_confidence_quarantine`
   - `attachment_quarantine`
   - `derived_card_quarantine`

6. **Derived-card impact**
   - derived cards whose source raw email will be suppressed.
   - derived cards preserved as active.
   - derived cards requiring review because source classification conflicts with extracted type.

7. **Index impact**
   - chunks removed or hidden from default retrieval.
   - embeddings excluded from vector search.
   - semantic-link source/target candidates reduced.

8. **Review buckets**
   - sample IDs and short summaries for required buckets.

Dry-run determinism requirement:

- Same inputs, same policy version, same classification data -> same decisions and same JSON report except timestamps/run IDs.

## B4. Review Buckets

The implementation should produce reviewable samples for:

| Bucket | Purpose |
| ------ | ------- |
| `high_confidence_marketing` | Validate obvious suppressions |
| `promotions_label_suppression` | Validate Gmail label behavior |
| `automated_noise` | Validate automated/noise suppression |
| `suppressed_with_attachments` | Avoid hiding meaningful attachments |
| `suppressed_with_derived_cards` | Confirm derived cards are preserved or reviewed |
| `quarantine_conflicts` | Resolve ambiguous policy cases |
| `active_overrides` | Validate owner-action override behavior |
| `unknown_classification` | Estimate any required model reruns |

Each sample should include:

- thread ID.
- subject.
- sender/domain.
- labels.
- message count.
- classification/confidence/source.
- proposed decision/reason.
- linked derived card UIDs, if any.
- minimal preview/snippet.

## B5. Manual Overrides

Overrides should be stored as policy inputs. Recommended logical shape:

| Field | Meaning |
| ----- | ------- |
| `override_id` | Stable ID |
| `scope` | `thread`, `sender`, `domain`, `label`, or `global` |
| `value` | Thread ID, sender, domain, label, etc. |
| `action` | `force_active`, `force_suppressed`, `force_quarantine`, `force_review` |
| `reason` | Human-entered reason |
| `created_at` | Timestamp |
| `created_by` | Operator identity if available |

Overrides must be included in dry-run reports and decision records.

## B6. Safe Apply Flow

Apply should be DB-first and reversible.

Recommended sequence:

1. Validate that the decision run was generated against the current policy version.
2. Validate that source cursors and major corpus counts have not drifted unexpectedly since dry-run.
3. Persist decision records and suppression ledger rows.
4. Mark affected existing email cards as `active`, `suppressed`, or `quarantine` in a rebuild-safe projection or status store.
5. Exclude suppressed/quarantine cards from default retrieval.
6. Exclude suppressed/quarantine chunks from embedding search.
7. Exclude suppressed/quarantine cards from semantic-link source and target candidate sets.
8. Remove suppressed/quarantine cards from future enrichment queues.
9. Preserve active derived cards with provenance to suppressed source email.
10. Write an apply report with before/after counts.

First-pass CCS apply hid rows in Postgres. **This campaign deleted suppressed marketing** (~502,622 files) and purged those UIDs + wrote the Gmail ledger. Quarantine cards were **not** deleted. Inbound Gmail: suppressed does not emit; quarantine writes labeled cards (`emit_cards=True`). Do not re-run vault-remove.

## B7. Rebuild Safety

Suppression must survive:

- incremental rebuild.
- full rebuild.
- embedding refresh.
- linker rerun.
- MCP server restart.

The materializer or index layer must consult the corpus decision/status store so existing markdown files do not reappear as active cards after rebuild.

Implementation should choose one of two strategies and document it before coding:

1. **Filter at materialization:** suppressed cards are not projected into active tables/chunks.
2. **Project with active state:** cards remain indexed with `corpus_state`, and all default retrieval/search/embed/link paths filter to `active`.

The execution preference is strategy 2 if it preserves auditability and rollback with less destructive behavior.

## B8. Derived Card Preservation

Derived cards that exist **only** because of a **suppressed** thread are in the vault-remove set. Quarantine-derived cards stay with the labeled quarantine cards. (`derived_uids` was recorded and ignored on earlier passes — leftover, not a reason to re-run vault-remove.)

Keep derived cards that still have an independent reason to exist (a proven action with structured fields: `meal_order`, `ride`, `flight`, `purchase`, etc.). Preserve `source_email` provenance on those keepers even if the source email is gone.

## B9. Rollback

Rollback should not require LLM calls or Gmail refetch.

Rollback should:

- Load the previous decision state for the `decision_run_id`.
- Restore prior `corpus_state` for all affected cards.
- Restore default retrieval/embedding/link eligibility.
- Preserve the decision history for audit.
- Emit a rollback report with counts.

This campaign wrote **no** `rollback.json`. Rollback cannot restore the ~502,622 deleted files. Future applies must persist `rollback.json`. Do not restore the full marketing pile. Do not treat missing rollback as a reason to re-apply.

## B10. Tests and Validation

Future implementation should include:

- Unit tests for classification precedence.
- Unit tests for decision record creation.
- Unit tests for dry-run determinism.
- Unit tests proving no LLM call is made when classification is reusable.
- Integration tests against a slice with marketing, transactional, personal, attachment, derived-card, and unknown examples.
- Search tests proving suppressed email is absent from default query/hybrid/vector surfaces.
- Linker tests proving suppressed email is neither source nor target.
- Rebuild tests proving suppression survives incremental and full rebuild.
- Rollback tests proving prior active state is restored.

## B11. Validation Ladder and Rust Standard

Cleanup must graduate through the Section G ladder before Arnold apply.

Required gates:

1. **Synthetic fixtures:** classification precedence, decision records, dry-run determinism, and no-LLM-reuse behavior pass.
2. **Small slice:** dry-run, apply, rollback, and rebuild-safety pass on a slice containing marketing, transactional, personal, attachment, derived-card, and unknown examples.
3. **Larger slice:** runtime, report size, classification reuse, suppressed retrieval filtering, and linker filtering are measured.
4. **Local seed dry-run:** full seed is evaluated without mutation; report is reviewed.
5. **Local seed apply:** this campaign applied on the canonical seed (schema `ppa`), not a copy. Do not copy-and-reapply. Future Arnold still uses staging.
6. **Arnold dry-run:** production state is evaluated without mutation and samples are reviewed.
7. **Arnold reviewed apply:** only a reviewed Arnold decision run can be applied.
8. **Arnold soak:** normal maintenance proves suppressed email does not return.

Rust standard:

- Use `PPA_ENGINE=rust` for slice and seed/staging scan, cache, materialization, and rebuild-safety validation unless explicitly testing Python parity.
- Prefer Rust vault cache and type-filtered cache reads for census generation.
- Avoid Python full-vault walks in corpus hygiene unless debugging parity.
- Every dry-run/apply/rollback report must include engine mode, elapsed runtime, and throughput by phase.
- Any Rust/Python divergence in materialized active/suppressed behavior blocks Arnold apply.

## Operational Reporting

Apply and rollback reports should be written as JSON and summarized in text.

Required metrics:

- evaluated threads.
- active/suppressed/quarantine counts.
- classification source counts.
- new LLM calls.
- affected cards/chunks/embeddings.
- derived cards preserved.
- review bucket counts.
- errors.
- rollback token or decision run ID.

## Definition of Done

Section B implementation is ready when:

- Existing Arnold email can be evaluated without rerunning classification for already-classified threads.
- Dry-run census is deterministic and reviewable.
- First-pass CCS apply is landed (hide in Postgres).
- **This seed:** suppressed marketing deleted (~502,622 files), UIDs purged, Gmail ledger written. Quarantine stays as labeled cards. Do not re-run vault-remove.
- Suppressed email is gone from the vault and from default retrieval — not only filtered at query time.
- Derived-only marketing cards are removed; independently useful derived cards remain.
- A later Gmail continue does not recreate removed **suppressed** thread UIDs. Quarantine inbound writes cards.
- This apply has no `rollback.json`; those deletes are not restorable. Future applies must persist rollback. Full-pile restore is out of scope.
- Remaining: quadratic UID collect + persist `rollback.json` on future applies. Arnold prune is out of scope.

## Completion Artifacts

The implementation agent must leave:

- dry-run JSON and human summary for fixture/slice gates.
- local seed dry-run report.
- local seed staging apply report.
- rollback report proving restored prior state.
- rebuild-safety report proving suppressed records do not reappear.
- Arnold dry-run report if production apply is being proposed.
- this-seed vault-remove evidence (~502,622 suppressed files deleted; no `rollback.json`). Do not re-apply. Arnold file prune is out of scope.

## Commit Instructions

Section B may require two commits because dry-run and apply/rollback are separate gates.

Dry-run commit:

- Start only from a clean tree.
- Stage only Section B dry-run/census/classification-reuse work, tests, and dry-run artifacts.
- Commit subject: `v2.5 section B: current arnold cleanup dry run`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before apply/rollback work starts.

Apply/rollback commit:

- Start only after the dry-run commit exists and the tree is clean.
- Stage only Section B staging apply/rollback/rebuild-safety work, tests, and artifacts.
- Commit subject: `v2.5 section B: current arnold cleanup apply rollback`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before Section C work starts.

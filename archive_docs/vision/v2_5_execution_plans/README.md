# PPA v2.5 Execution Plans - Agent Handoff

This directory is the implementation entrypoint for v2.5. A zero-context agent should read this file first, then `../v2.5vision.md`, then the section plans in the order below.

v2.5 implementation is production-sensitive. Do not start on Arnold. Do not start by writing broad migrations. Do not run expensive corpus jobs before the relevant dry-run/report gate exists.

## Required Implementation Sequence

Implement in this order:

1. **Section G - Validation Ladder + Rust Standard**
   - Establish the ladder, report shape, engine-mode reporting, and command refusal rules.
   - Nothing else should be able to apply to Arnold before this exists.

2. **Section A - Email Corpus Semantics**
   - Implement `EmailPromotionPolicy` semantics and fixture tests.
   - This is pure decision logic before cleanup or sync mutation.

3. **Section B - Current Arnold Corpus Cleanup, dry-run only**
   - Build classification reuse and dry-run census first.
   - No apply path until dry-run reports and sample buckets are reviewable.

4. **Section B - staging apply/rollback**
   - Apply only to slices and local seed staging.
   - Prove rebuild safety and rollback before any production write.

5. **Section C - Future Gmail Sync Promotion, dry-run/report mode**
   - Add classify-before-promotion behavior behind dry-run/reporting.
   - Do not enable production mutation until Section G gates pass.

6. **Section D - Source Updater Contract**
   - Standardize source status, batch reports, cursor safety, and dirty UIDs.

7. **Section E - Processor DAG**
   - Wire dirty inputs to processors incrementally.
   - Avoid full embeddings, full linkers, and broad LLM reruns by default.

8. **Section F - Arnold Observability + v3 Gate**
   - Make readiness fail closed until Sections A-G are proven through Arnold soak.

9. **Arnold reviewed apply and soak**
   - Only after slices, local seed/staging, Arnold dry-run, and rollback proof pass.

## Global Invariants

- `EmailPromotionPolicy` is the only policy for historical cleanup and future Gmail sync.
- Existing classification is reused before new LLM calls.
- Gmail remains the source of record for suppressed bulk email.
- Suppression is auditable and reversible.
- The first v2.5 implementation does not physically delete vault markdown.
- Existing active cards are not silently demoted by routine sync.
- Seed and Arnold are never first apply targets.
- Arnold apply requires a reviewed Arnold dry-run `decision_run_id`.
- Full reclassification, full embeddings, and all-linker runs are explicit opt-in exceptions.
- `PPA_ENGINE=rust` is the default for supported scan/cache/materialization/chunking validation paths.
- Rust/Python divergence blocks Arnold apply.

## Commands to Avoid by Default

Do not run these by default during v2.5 implementation:

- full-vault Python walks for corpus census when cache/index paths exist.
- full email reclassification.
- full embedding regeneration.
- all-linker reruns.
- production apply without `decision_run_id`.
- Arnold apply without an Arnold dry-run generated from current Arnold state.
- physical vault pruning.

If any of these become necessary, the implementation must add an explicit flag, report the expected blast radius, and require a reviewed confirmation path.

## CLI Defaults

All new v2.5 commands should default to safe behavior:

- `--dry-run` by default for corpus hygiene and future-sync evaluation.
- `--apply` requires a `decision_run_id`.
- Arnold apply requires an explicit Arnold confirmation flag or equivalent guard.
- full reclassification requires an explicit flag.
- full embedding/linker reruns require explicit flags.
- every long operation writes JSON and human summaries.
- every report includes engine mode, counts, elapsed runtime, throughput, and next recommended gate.

## Standard Exit Codes

New v2.5 CLI commands should use predictable exit codes:

| Code | Meaning |
| ---- | ------- |
| `0` | Success. Command completed and report was written. |
| `1` | Runtime failure. See report/errors. |
| `2` | Validation failed. Inputs were readable, but gate/check did not pass. |
| `3` | Refused unsafe operation. Missing dry-run, decision run, confirmation, gate evidence, or explicit expensive-work flag. |
| `4` | Blocked by external dependency. Auth, provider, source, database, or model unavailable. |

Commands that refuse unsafe work should return `3`, not `1`, so automation can distinguish a guardrail from a broken command.

## Required Artifact Paths

Implementation can refine exact names, but every long v2.5 operation must write artifacts under stable run directories:

```text
ppa/logs/v2_5/
  gate-<gate_name>/
    <run_id>/
      report.json
      summary.md
      samples.jsonl          # if sample output exists
      errors.jsonl           # if errors exist
      rollback.json          # if apply created rollback state
```

Every report must include enough paths to find related artifacts from `ppa status`.

## Per-Section Deliverables

| Section | Minimum implementation deliverables |
| ------- | ----------------------------------- |
| G | Gate/report framework, refusal rules, engine-mode reporting, validation matrix tracking |
| A | `EmailPromotionPolicy`, category normalization, fixture tests, policy decision report shape |
| B dry-run | classification reuse loader, `email_corpus_decisions` dry-run records, census report, sample buckets |
| B apply | staging apply, rollback, rebuild-safety validation, suppression-aware retrieval checks |
| C | Gmail classify-before-promotion dry-run, ledger/quarantine behavior, cursor-safety tests |
| D | source updater declarations, committed batch reports, source status store, cursor-safety tests |
| E | processor declarations, staleness detection, active-only skips, dirty-input scheduling tests |
| F | JSON status, human status summary, health thresholds, v3 readiness gate, operator runbook pointers |

No section is complete with code alone. Each section must produce reports/tests proving the relevant gate behavior.

## Commit Protocol

v2.5 implementation uses one commit per section. A future agent must not mix sections in a single commit.

Clean-tree requirements:

- Before starting a section, run `git status --short` and confirm the tree is clean.
- If the tree is not clean, stop and ask for review unless the dirty files are the intentional uncommitted work for the current section.
- After finishing a section, run the required tests/reports, stage only that section's files, and create exactly one commit.
- After the commit, run `git status --short` again and confirm the tree is clean before starting the next section.
- If commit hooks modify files, inspect those changes, rerun relevant tests if needed, include the hook changes in the same section commit, and confirm the tree is clean after commit.

Commit subject convention:

```text
v2.5 section <LETTER>: <section slug>
```

Required subjects:

| Section | Commit subject |
| ------- | -------------- |
| G | `v2.5 section G: validation ladder rust standard` |
| A | `v2.5 section A: email corpus semantics` |
| B dry-run | `v2.5 section B: current arnold cleanup dry run` |
| B apply/rollback | `v2.5 section B: current arnold cleanup apply rollback` |
| C | `v2.5 section C: future gmail sync promotion` |
| D | `v2.5 section D: source updater contract` |
| E | `v2.5 section E: processor dag` |
| F | `v2.5 section F: arnold observability v3 gate` |

Commit body pattern:

```text
Implements Section <LETTER> by <one sentence summary of implementation>.

Validation:
- <test/report command or artifact>
- <test/report command or artifact>

Artifacts:
- <report path>
- <summary path>

Safety:
- tree clean before start: yes
- tree clean after commit: yes
- production mutation: no/yes with reviewed decision_run_id <id>
```

Do not start the next section until the previous section commit exists and the tree is clean.

## Shared Logical Contracts

These are logical contracts. Implementation may use Postgres, SQLite, or staged files, but the fields and semantics should remain stable.

### `email_corpus_decisions`

| Field | Meaning |
| ----- | ------- |
| `decision_run_id` | Dry-run/apply cycle |
| `source_key` | Source/account identity |
| `account_email` | Gmail account |
| `gmail_thread_id` | External thread ID |
| `gmail_history_id` | Gmail history marker |
| `thread_body_sha` | Content hash for staleness |
| `thread_uid` | Existing or future thread card UID |
| `message_uids` | Existing or future message UIDs |
| `attachment_uids` | Existing or future attachment UIDs |
| `derived_uids` | Derived cards linked to this email |
| `classification` | Raw classification |
| `canonical_classification` | Normalized classification |
| `confidence` | Classifier confidence |
| `card_types` | Extractable card types |
| `classification_source` | `card_classifications`, `classify_index`, `frontmatter`, `stage0`, `new_llm`, etc. |
| `policy_version` | `EMAIL_PROMOTION_POLICY_VERSION` |
| `previous_corpus_state` | Existing state before apply |
| `corpus_decision` | `active`, `suppressed`, or `quarantine` |
| `processor_decision` | typed extraction, enrichment, no processing, suppressed, review |
| `decision_reason` | Primary reason key |
| `decision_signals` | Supporting labels/overrides/signals |
| `applied_at` | Empty until apply |

### `email_corpus_overrides`

| Field | Meaning |
| ----- | ------- |
| `override_id` | Stable ID |
| `scope` | `thread`, `sender`, `domain`, `label`, or `global` |
| `value` | Scope value |
| `action` | `force_active`, `force_suppressed`, `force_quarantine`, `force_review` |
| `reason` | Human-entered reason |
| `created_at` | Timestamp |
| `created_by` | Operator if available |

### `source_updater_runs`

| Field | Meaning |
| ----- | ------- |
| `run_id` | Source run ID |
| `source_key` | Source/account identity |
| `source_type` | Gmail, Calendar, iMessage, Photos, etc. |
| `cursor_before`, `cursor_after` | Cursor state |
| `observed`, `promoted`, `suppressed`, `quarantined`, `updated`, `deleted_or_tombstoned` | Counts |
| `dirty_card_uids_count` | Downstream dirty count |
| `status` | `success`, `partial`, `failed`, `blocked` |
| `errors`, `warnings` | Issues |
| `engine_mode` | `rust`, `python`, or `parity` where relevant |

### `processor_runs`

| Field | Meaning |
| ----- | ------- |
| `run_id` | Processor run ID |
| `processor_key` | Processor name |
| `processor_version` | Logic/prompt/schema version |
| `input_uid` | Input card/decision |
| `input_hash` | Staleness hash |
| `input_corpus_state` | Active/suppressed/quarantine |
| `status` | `pending`, `running`, `complete`, `skipped`, `failed`, `stale` |
| `skip_reason` | Why skipped |
| `output_uids` | Produced cards/rows |
| `error` | Failure summary |

### Maintenance Report

Every long v2.5 operation should produce a report with:

- run ID and ladder gate.
- archive instance.
- vault path and schema.
- engine mode.
- policy and processor versions.
- source summaries.
- corpus decision summaries.
- processor summaries.
- embedding/linker summaries.
- elapsed runtime and throughput by phase.
- errors and warnings.
- rollback token or decision run ID.
- next recommended gate.

## Validation Matrix

| Gate | Purpose | Required before moving on |
| ---- | ------- | ------------------------- |
| Synthetic fixtures | Prove rules in isolation | Unit tests pass, no real vault mutation |
| Small slice | Prove behavior on real examples | dry-run/apply/rollback/rebuild safety pass |
| Larger slice | Prove runtime/report scale | bounded runtime, no broad LLM work |
| Local seed dry-run | Evaluate full seed without mutation | report reviewed, classification reuse acceptable |
| Local seed staging apply | Prove seed-scale apply/rollback safely | copied vault/staging schema only, rollback passes |
| Arnold dry-run | Evaluate production without mutation | report and samples reviewed |
| Arnold reviewed apply | Apply reviewed production decision run | DB-first, non-destructive, rollback available |
| Arnold soak/readiness | Prove ongoing health | normal maintain cycles pass, v3 readiness passes |

## Stop Conditions

Stop and ask for review if:

- classification reuse is materially lower than expected.
- new LLM calls are unexpectedly high.
- dry-run suppresses surprising personal/important/starred threads.
- derived card preservation is ambiguous.
- report counts differ between identical dry-runs.
- Rust/Python validation diverges.
- rebuild re-promotes suppressed records.
- rollback fails on any slice or staging run.
- any command would mutate canonical seed or Arnold before its gate.

## Final Readiness

v2.5 is implementation-ready for v3 only when:

- Sections A-G are implemented.
- the validation matrix has passed through Arnold soak.
- `ppa status` reports v3 readiness as ready.
- suppressed marketing email is excluded from default retrieval, embeddings, linkers, and enrichment queues.
- future Gmail sync uses classify-before-promotion.
- rollback remains available for the Arnold apply run.

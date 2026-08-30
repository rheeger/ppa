# PPA v2.5 Execution Plans - Agent Handoff

This directory is the implementation entrypoint for v2.5. A zero-context agent should read this file first, then `../v2.5vision.md`, then the section plans in the order below.

v2.5 implementation is production-sensitive. Do not start on Arnold. Do not start by writing broad migrations. Do not run expensive corpus jobs before the relevant dry-run/report gate exists.

## Current Status (read this first)

HEAD `b57136f` on branch `v2.5` (Aug 2026). Goal of the current wave: **close out this local seed**. Arnold deploy/apply is deferred / out of current scope.

**Landed:** Sections A–C/G; D Phase 1 + Phase 2 (runner + Track B catch-up + Track C calendar mint); E Phase 1 + Phase 2 (Track A wired dirty-UID executors + rematerialize allowlist `1c02e10`); F surfaces; H local gates 0–5 / 5b (earlier CCS staging + Gmail 5b). Merge commits `685d07b` `5ed5b07` `0fb51b4` + join proof `5737db3`. **Scale hot paths** `0c53d7e`…`3a90bc0` (1pct + 10pct). **2026-08-26–28 local-seed campaign** applied hygiene + most live updaters **on the canonical seed** `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` schema `ppa`. That seed **was** the apply target. The written sequence (slice vault-remove → 1pct/10pct updaters → Gate 5b on a staging copy → catch-up) was **skipped**. Do not copy the seed first. “Never mutate the canonical seed” is a historical constraint for **future Arnold**, not this machine.

**Product fork (locked this campaign):** suppressed marketing is **deleted** from vault + purged from index + Gmail ledger. Quarantine **stays** as labeled cards (`retrieval_weight=0.35`). New inbound uncertain Gmail writes those cards (`emit_cards=True`). Suppressed inbound does not emit. This forks written C3 (“quarantine = compact review, no cards”) and older B text that said delete quarantine too.

**Demonstrated on this seed:**

- Hygiene apply ~502,622 files deleted. No `rollback.json`; rollback cannot restore those deletes.
- Live updaters SUCCESS: calendar, contacts, otter, file-libraries, beeper, imessage, gmail-messages, gmail-correspondents (vault-first + `after:last_sync` + HTTP/batch). Do not full-mailbox-walk correspondents.
- GitHub FAILED (`--stage-dir` required). Photos not run.
- Dirty rematerialize: allowlist incremental; host UID after merge (`b57136f`). Index ~1,392,108 cards, 3,962,176 embeddings, pending 0.
- No full rebuild, no IVFFlat, no `--catch-up`.

**What remains (Arnold excluded):**

1. Hygiene leftovers: quadratic UID collect + persist `rollback.json` on **future** applies. Do not re-run vault-remove.
2. GitHub `--stage-dir`.
3. One `maintain --run-processors` on this seed.
4. Soak + F readiness with real-run evidence (surfaces can go green on snapshots today).

Parked: Photos, catch-up, full rebuild, Arnold 6+.

The five whole-corpus I/O engines are done. Do not re-implement hygiene COPY, census cache dump, Gmail/Calendar cache indexes, or dirty-only extract. Dirty-UID rematerialize allowlist landed (`1c02e10`). Still-open (not those engines): `embed_pending` is limit-N.

Snapshots from `--record-source-status` / `--record-processor-status` are **not** live updating. D/E Phase 2 are **not** missing.

## Required Implementation Sequence

### Phase 1 (done — do not re-implement)

1. **Section G** — validation ladder, report shape, engine-mode reporting, refusal rules.
2. **Section A** — `EmailPromotionPolicy` + fixtures.
3. **Section B dry-run** — classification reuse + census.
4. **Section B apply/rollback** — staging apply/rollback tooling.
5. **Section C** — Gmail classify-before-promotion gate.
6. **Section D Phase 1** — declarations, batch shapes, status snapshots.
7. **Section E Phase 1** — processor declarations, plans, status snapshots.
8. **Section F Phase 1** — status/readiness surfaces.

### Phase 2 + promotion (landed through A/B/C merge + 2026-08-26–28 seed campaign)

9. **Section D Phase 2** — landed (`source updater execution` + Track B `20401ea` + Track C `66e1300` + Gmail/Calendar cache indexes `0bdbd48` `9139c58`). Runner is real; maintain still needs explicit source keys; cursors still list/page-token.
10. **Section E Phase 2** — landed (`processor dag execution` + Track A `fa5f5a2` + dirty-only extract `35901b0` + rematerialize allowlist `1c02e10`). Executors are wired. Remaining: one `maintain --run-processors` on this seed; `embed_pending` is still limit-N.
11. **Section H local 0–5b + scale + seed campaign** — 0–5b done on fixtures / staging / capped Gmail; scale ladder `3a90bc0` on 1pct + 10pct; 2026-08-26–28 campaign applied hygiene + most live updaters **on this seed**. **Next:** hygiene leftovers + GitHub `--stage-dir` + one `maintain --run-processors` → local soak + F real-run evidence. Do not copy the seed. Do not re-run vault-remove. Parked: Photos, catch-up, full rebuild, Arnold.

Do not claim freshness from snapshots. Do not treat Arnold as the next step.

## Global Invariants

- `EmailPromotionPolicy` is the only policy for historical cleanup and future Gmail sync.
- Existing classification is reused before new LLM calls.
- Gmail remains the source of record for suppressed bulk email.
- Suppression is auditable and reversible.
- First-pass hygiene (CCS-only) hid rows in Postgres. **This campaign deleted suppressed marketing** from the vault + purged index + Gmail ledger. Quarantine **stays** as labeled cards (`retrieval_weight=0.35`). Do not re-run vault-remove. Do not delete quarantine.
- Existing active cards are not silently demoted by routine sync.
- This machine’s canonical seed **was** the hygiene/updater apply target. “Never mutate the canonical seed” is a historical constraint for **future Arnold**, not this seed. Do not copy the seed first.
- Arnold corpus apply requires a reviewed Arnold dry-run `decision_run_id`.
- Full reclassification, full embeddings, and all-linker runs are explicit opt-in exceptions.
- `PPA_ENGINE=rust` is the default for supported scan/cache/materialization/chunking validation paths.
- Rust/Python divergence blocks Arnold apply.
- **Source freshness requires real updater runs; processor health requires real processor runs.**

## Commands to Avoid by Default

Do not run these by default during v2.5 implementation:

- full-vault Python walks for corpus census when cache/index paths exist.
- full email reclassification.
- full embedding regeneration.
- all-linker reruns.
- production apply without `decision_run_id`.
- Arnold apply without an Arnold dry-run generated from current Arnold state.
- another vault-remove of suppressed mail on this seed (already applied; no `rollback.json`). Do not delete quarantine cards. Physical prune of Arnold remains out of scope.
- full Phase 9 `ppa-deploy-v2` / rebuild for routine code promotion (prefer `ppa-sync` + `ppa-install` + `migrate`).
- claiming Gate H soak success from `--record-*-status` alone.

If any of these become necessary, the implementation must add an explicit flag, report the expected blast radius, and require a reviewed confirmation path.

## CLI Defaults

All new v2.5 commands should default to safe behavior:

- `--dry-run` by default for corpus hygiene and future-sync evaluation.
- `--apply` requires a `decision_run_id` (corpus) or explicit `--apply` (updaters/processors).
- production apply requires an explicit confirmation flag (`--confirm-production`) and `PPA_ARCHIVE_INSTANCE_ROLE=production`.
- full reclassification requires an explicit flag.
- full embedding/linker reruns require explicit flags.
- every long operation writes JSON and human summaries.
- every report includes engine mode, counts, elapsed runtime, throughput, and next recommended gate.

Section G implements the control plane as `archive_cli/validation_gates/` with CLI entrypoint `ppa gates` (`status`, `readiness`, `record`, `guard-production-apply`, `guard-expensive`). Production apply guards key off `PPA_ARCHIVE_INSTANCE_ROLE=production` (or a `production:` instance label prefix).

## Standard Exit Codes

New v2.5 CLI commands should use predictable exit codes:

| Code | Meaning                                                                                                                |
| ---- | ---------------------------------------------------------------------------------------------------------------------- |
| `0`  | Success. Command completed and report was written.                                                                     |
| `1`  | Runtime failure. See report/errors.                                                                                    |
| `2`  | Validation failed. Inputs were readable, but gate/check did not pass.                                                  |
| `3`  | Refused unsafe operation. Missing dry-run, decision run, confirmation, gate evidence, or explicit expensive-work flag. |
| `4`  | Blocked by external dependency. Auth, provider, source, database, or model unavailable.                                |

Commands that refuse unsafe work should return `3`, not `1`, so automation can distinguish a guardrail from a broken command.
Section G should expose reusable refusal guards that return this code when prior gate evidence, a reviewed decision run, production instance confirmation (`PPA_ARCHIVE_INSTANCE_ROLE=production`), or explicit expensive-work opt-in is missing.

## Required Artifact Paths

Implementation can refine exact names, but every long v2.5 operation must write artifacts under stable run directories:

```text
ppa/logs/validation-gates/
  gate-<gate_name>/
    <run_id>/
      report.json
      summary.md
      samples.jsonl          # if sample output exists
      errors.jsonl           # if errors exist
      rollback.json          # if apply created rollback state
      dirty_uids.jsonl       # if source updater produced dirty UIDs
```

Every report must include enough paths to find related artifacts from `ppa status`.

## Per-Section Deliverables

| Section   | Minimum implementation deliverables                           | Status                                                                                  |
| --------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| G         | Gate/report framework, refusal rules, engine-mode reporting   | Done                                                                                    |
| A         | `EmailPromotionPolicy`, fixtures                              | Done                                                                                    |
| B dry-run | classification reuse, census, samples                         | Done                                                                                    |
| B apply   | staging apply, rollback, rebuild-safety                       | Done (Arnold apply in H)                                                                |
| C         | Gmail classify-before-promotion gate                          | Done (enable in H)                                                                      |
| D Phase 1 | declarations, batch shapes, snapshots                         | Done                                                                                    |
| D Phase 2 | **run adapters**, commit cursors, dirty UIDs, maintain flag   | Most live streams SUCCESS on this seed; GitHub `--stage-dir` open; Photos parked        |
| E Phase 1 | declarations, plan/staleness, snapshots                       | Done                                                                                    |
| E Phase 2 | **run processors** on dirty UIDs, maintain flag               | **Landed** (Track A + dirty extract)                                                    |
| F         | JSON/human status, readiness fail-closed on real-run evidence | Surfaces landed; **evidence hole remains**                                              |
| H         | Seed updater proof, corpus apply, soak, readiness             | Seed campaign landed on this machine. Remaining: GitHub `--stage-dir`, processors maintain, soak + F. Arnold deferred |

No section is complete with code alone. Each section must produce reports/tests proving the relevant gate behavior.

## Commit Protocol

v2.5 implementation uses one commit per section (or per phase for D/E). A future agent must not mix sections in a single commit.

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

| Section             | Commit subject                                          |
| ------------------- | ------------------------------------------------------- |
| G                   | `v2.5 section G: validation ladder rust standard`       |
| A                   | `v2.5 section A: email corpus semantics`                |
| B dry-run           | `v2.5 section B: current arnold cleanup dry run`        |
| B apply/rollback    | `v2.5 section B: current arnold cleanup apply rollback` |
| C                   | `v2.5 section C: future gmail sync promotion`           |
| D Phase 1           | `v2.5 section D: source updater contract`               |
| D Phase 2           | `v2.5 section D: source updater execution`              |
| E Phase 1           | `v2.5 section E: processor dag`                         |
| E Phase 2           | `v2.5 section E: processor dag execution`               |
| F                   | `v2.5 section F: arnold observability v3 gate`          |
| F harden (optional) | `v2.5 section F: readiness real-run evidence`           |
| H docs (optional)   | `v2.5 section H: validation promotion runbook`          |

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

### `gate_runs`

Section G owns the parent run registry for v2.5 gate evidence. This contract is modeled on the existing `schema_migrations` applied-at ledger, the `link_jobs` status/version/input-hash pattern, and the `meta` watermark store.

| Field                                      | Meaning                                                                                                                                         |
| ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `run_id`                                   | Stable parent run ID issued by the Section G gate registry                                                                                      |
| `gate`                                     | Validation ladder gate, e.g. `synthetic_fixtures`, `small_slice`, `local_seed_staging_apply`, `production_dry_run`, `production_reviewed_apply` |
| `archive_instance`                         | Canonical instance label for fixture, slice, seed staging, production dry-run, or production apply                                              |
| `vault_path`                               | Vault path evaluated by the run                                                                                                                 |
| `index_schema`                             | Postgres schema or staged schema evaluated by the run                                                                                           |
| `engine_mode`                              | `rust`, `python`, or `parity`                                                                                                                   |
| `policy_version`                           | Policy version active for the run, when relevant                                                                                                |
| `input_hash`                               | Hash/fingerprint of important inputs used to prove determinism or detect drift                                                                  |
| `status`                                   | `pending`, `running`, `passed`, `failed`, `blocked`, or `refused`                                                                               |
| `reviewed`                                 | Whether a human/operator review was recorded for gates that require review                                                                      |
| `approved`                                 | Whether the reviewed run is approved for the next gate or apply action                                                                          |
| `report_path`                              | Path to JSON report artifact                                                                                                                    |
| `summary_path`                             | Path to human-readable summary artifact                                                                                                         |
| `created_at`, `started_at`, `completed_at` | Run timing metadata                                                                                                                             |
| `applied_at`                               | Empty unless this run performed an apply                                                                                                        |
| `error`                                    | Failure/refusal summary                                                                                                                         |

Section-specific run and decision tables should reference or mirror `gate_runs.run_id`; they should not invent unrelated run IDs.

### Archive Instance Identity

Section G should define one canonical archive instance label derived from existing `ArchiveConfig` inputs: `index_schema`, a safe `index_dsn` descriptor or fingerprint, and `vault_path`. Optional `PPA_ARCHIVE_INSTANCE_ROLE` prefixes labels (`fixture:`, `slice:`, `production:`, etc.) and production apply guards require role `production`.

### `email_corpus_decisions`

`decision_run_id` is issued by the Section G gate registry and should reference or mirror `gate_runs.run_id`. Section B owns the email decision rows; Section G owns the parent run/gate state.

| Field                      | Meaning                                                                            |
| -------------------------- | ---------------------------------------------------------------------------------- |
| `decision_run_id`          | Dry-run/apply cycle                                                                |
| `source_key`               | Source/account identity                                                            |
| `account_email`            | Gmail account                                                                      |
| `gmail_thread_id`          | External thread ID                                                                 |
| `gmail_history_id`         | Gmail history marker                                                               |
| `thread_body_sha`          | Content hash for staleness                                                         |
| `thread_uid`               | Existing or future thread card UID                                                 |
| `message_uids`             | Existing or future message UIDs                                                    |
| `attachment_uids`          | Existing or future attachment UIDs                                                 |
| `derived_uids`             | Derived cards linked to this email                                                 |
| `classification`           | Raw classification                                                                 |
| `canonical_classification` | Normalized classification                                                          |
| `confidence`               | Classifier confidence                                                              |
| `card_types`               | Extractable card types                                                             |
| `classification_source`    | `card_classifications`, `classify_index`, `frontmatter`, `stage0`, `new_llm`, etc. |
| `policy_version`           | `EMAIL_PROMOTION_POLICY_VERSION`                                                   |
| `previous_corpus_state`    | Existing state before apply                                                        |
| `corpus_decision`          | `active`, `suppressed`, or `quarantine`                                            |
| `processor_decision`       | typed extraction, enrichment, no processing, suppressed, review                    |
| `decision_reason`          | Primary reason key                                                                 |
| `decision_signals`         | Supporting labels/overrides/signals                                                |
| `applied_at`               | Empty until apply                                                                  |

### `email_corpus_overrides`

| Field         | Meaning                                                                |
| ------------- | ---------------------------------------------------------------------- |
| `override_id` | Stable ID                                                              |
| `scope`       | `thread`, `sender`, `domain`, `label`, or `global`                     |
| `value`       | Scope value                                                            |
| `action`      | `force_active`, `force_suppressed`, `force_quarantine`, `force_review` |
| `reason`      | Human-entered reason                                                   |
| `created_at`  | Timestamp                                                              |
| `created_by`  | Operator if available                                                  |

### `source_updater_runs`

`run_id` should reference or mirror `gate_runs.run_id`. Section D owns source-updater accounting; Section G owns the parent run/gate state and archive-instance evidence.

| Field                                                                                   | Meaning                                      |
| --------------------------------------------------------------------------------------- | -------------------------------------------- |
| `run_id`                                                                                | Source run ID                                |
| `source_key`                                                                            | Source/account identity                      |
| `source_type`                                                                           | Gmail, Calendar, iMessage, Photos, etc.      |
| `cursor_before`, `cursor_after`                                                         | Cursor state                                 |
| `observed`, `promoted`, `suppressed`, `quarantined`, `updated`, `deleted_or_tombstoned` | Counts                                       |
| `dirty_card_uids_count`                                                                 | Downstream dirty count                       |
| `status`                                                                                | `success`, `partial`, `failed`, `blocked`    |
| `errors`, `warnings`                                                                    | Issues                                       |
| `engine_mode`                                                                           | `rust`, `python`, or `parity` where relevant |

### `processor_runs`

`run_id` should reference or mirror `gate_runs.run_id`. Section E owns processor state; Section G owns the parent run/gate state and readiness evidence.

| Field                | Meaning                                                        |
| -------------------- | -------------------------------------------------------------- |
| `run_id`             | Processor run ID                                               |
| `processor_key`      | Processor name                                                 |
| `processor_version`  | Logic/prompt/schema version                                    |
| `input_uid`          | Input card/decision                                            |
| `input_hash`         | Staleness hash                                                 |
| `input_corpus_state` | Active/suppressed/quarantine                                   |
| `status`             | `pending`, `running`, `complete`, `skipped`, `failed`, `stale` |
| `skip_reason`        | Why skipped                                                    |
| `output_uids`        | Produced cards/rows                                            |
| `error`              | Failure summary                                                |

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

Report shape implementation binding:

- Use a dataclass-backed schema with `to_dict()` JSON serialization, following the existing `DeployStep` / `DeployResult` pattern.
- Include status literals, elapsed timing, warnings/errors, details dictionaries, and artifact paths.
- Extend the shared report shape with section-specific summaries rather than creating separate report formats per section.
- `archive_instance` comes from the Section G instance-identity helper.
- `run ID` / `decision run ID` comes from the Section G gate registry.

## Validation Matrix

| Gate                                            | Purpose                                  | Required before moving on                                   |
| ----------------------------------------------- | ---------------------------------------- | ----------------------------------------------------------- |
| Synthetic fixtures                              | Prove rules in isolation                 | Unit tests pass, no real vault mutation                     |
| Small slice                                     | Prove corpus hygiene on real examples    | dry-run/apply/rollback/rebuild safety pass                  |
| Larger slice                                    | Prove runtime/report scale               | bounded runtime, no broad LLM work                          |
| Local seed dry-run                              | Evaluate full seed without mutation      | report reviewed, classification reuse acceptable            |
| Local seed staging apply                        | Prove seed-scale apply/rollback safely   | **This machine:** already applied on the canonical seed (2026-08-26–28). Do not copy-and-reapply. Future Arnold still uses a staging copy. |
| **Local seed source updater + processor proof** | Prove live update on seed                | real updater runs + dirty UID processor runs (Gate 5b)      |
| Arnold code deploy                              | Put Phase 2 code on Arnold               | `ppa-sync` + `ppa-install` (+ migrate); MCP restarted       |
| **Arnold source updater + processor proof**     | Prove live update on production          | real runs with cursors (Gate 6b)                            |
| Production dry-run                              | Evaluate corpus cleanup without mutation | report and samples reviewed                                 |
| Production reviewed apply                       | Apply reviewed production decision run   | DB-first, non-destructive, rollback available               |
| Production soak/readiness                       | Prove ongoing health                     | maintain with **real** updaters/processors; readiness ready |

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
- any command would mutate **Arnold** before its gate, or re-run vault-remove / full-mailbox correspondents on this already-applied seed.
- Gate 5b or 6b (updater/processor proof) would be skipped.
- readiness would pass on snapshot-only source/processor status.

## Final Readiness

v2.5 unblocks v3 only when:

- D Phase 2 and E Phase 2 are implemented.
- the validation matrix has passed through Arnold soak (including Gates 5b and 6b).
- `ppa readiness --format json` reports ready with **real** updater and processor run evidence.
- suppressed marketing email is excluded from default retrieval, embeddings, linkers, and enrichment queues.
- future Gmail sync uses classify-before-promotion.
- rollback remains available for the Arnold apply run.

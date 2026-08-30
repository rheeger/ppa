# Section F Execution Plan - Arnold Observability and v3 Readiness Gate

**Status (Aug 2026, HEAD `b57136f`):** Surfaces landed. **Evidence hole remains** — readiness can go green on snapshots. Remaining close-out: soak + real-run evidence on this seed after one `maintain --run-processors`. Arnold soak/6+ deferred.

## Objective

Define how Arnold reports production health after v2.5 and how PPA decides it is ready to resume v3 packaging work.

Section F turns the concepts from Sections A-E into operator-visible status:

- source freshness.
- corpus health.
- classification coverage.
- cleanup state.
- processor backlog and failures.
- embedding/link health.
- maintenance reports.
- v3 readiness.

## Non-Goals

- Do not build the v3 polished `ppa status` UI in this planning pass.
- Do not implement status commands in this planning pass.
- Do not require a web dashboard.
- Do not hide failures behind a single green/red indicator.
- Do not declare v3 ready based only on docs existing; v3 readiness requires later implementation proof on Arnold.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_b_current_arnold_cleanup.plan.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_d_source_updater_contract.plan.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_e_processor_dag.plan.md`
- `ppa/archive_cli/commands/maintain.py`
- `ppa/archive_cli/__main__.py`
- `ppa/archive_cli/index_query.py`
- `ppa/archive_cli/server.py`
- `ppa/archive_cli/embedder.py`
- `ppa/archive_cli/seed_links.py`
- `ppa/archive_cli/schema_ddl.py`

## Agent Handoff Checklist

Before implementation:

- Read `README.md`, `v2.5vision.md`, Sections B, D, E, G, and this plan.
- Confirm Section A, Section B dry-run/apply, Section C, Section D, Section E, and Section G commits are present.
- Confirm Section D and E provide machine-readable source/processor state without requiring live sync/processor execution.
- Run `git status --short --branch` and stop if the tree is not clean.
- Build machine-readable status before polished text UI.
- Make readiness fail closed until all required reports/gates exist.
- Do not declare v3 ready based only on docs or partial implementation.
- Prefer aggregating existing status stores over inventing new Section F-only state.

Likely implementation files:

- `archive_cli/commands/status.py` or equivalent.
- `archive_cli/status/` or equivalent aggregation module.
- `archive_cli/commands/maintain.py`
- report/status modules introduced by Sections B, D, E, G.
- `archive_cli/server.py` only if MCP should expose health later.

Required first tests:

- JSON status shape.
- readiness pass/fail logic.
- source failure appears in status.
- suppressed email visibility failure blocks readiness.
- missing vault/config/database returns structured blocked/failed status, not traceback.
- readiness can be evaluated without running source sync or processors.

Stop conditions:

- status cannot identify the failing source/processor/gate.
- readiness can pass without Section G gate evidence.
- reports do not include engine mode or decision run IDs.
- Arnold status hides partial failures behind a green summary.
- status implementation starts mutating source cursors, corpus state, embeddings, or processors.
- Section F builds a separate source/processor state model instead of consuming Sections B/D/E/G.
- normal operator misuse produces tracebacks instead of structured status/errors.

## Production Status Surfaces

v2.5 should define three status surfaces:

1. **Human status:** `ppa status` or equivalent text output.
2. **Machine status:** JSON report for automation and future v3 CLI rendering.
3. **Historical reports:** append-only maintenance reports for trend/debugging.

The first implementation can be plain text and JSON. v3 can later make it pretty with `rich`.

Section F first implementation should:

1. Add machine-readable status aggregation.
2. Add human-readable summary from the same payload.
3. Add readiness evaluation that fails closed.
4. Add tests with fixture state from Sections B, D, E, and G.
5. Avoid changing maintenance execution semantics except to emit or reference reports.

## `ppa status` Required Sections

### 1. Archive Summary

Fields:

- archive name / instance identity.
- vault path.
- index schema.
- current git commit / build version if available.
- last successful maintenance run.
- current readiness state.

### 2. Source Freshness

For each source:

| Field                  | Meaning                                               |
| ---------------------- | ----------------------------------------------------- |
| `source_key`           | Source/account identity                               |
| `source_type`          | Gmail, Calendar, iMessage, Photos, etc.               |
| `enabled`              | Whether source participates in maintenance            |
| `state`                | `fresh`, `stale`, `failed`, `blocked`, `never_synced` |
| `last_success_at`      | Last successful sync                                  |
| `last_attempt_at`      | Last attempted sync                                   |
| `cursor_summary`       | Human-readable cursor                                 |
| `observed_last_run`    | Last run observed count                               |
| `promoted_last_run`    | Last run promoted count                               |
| `suppressed_last_run`  | Last run suppressed count                             |
| `quarantined_last_run` | Last run quarantine count                             |
| `deleted_last_run`     | Last run deleted/tombstoned count                     |
| `last_error`           | Last error summary                                    |

### 3. Corpus Health

Counts:

- active cards by source.
- active cards by card type.
- suppressed records by source.
- quarantine records by source.
- email active/suppressed/quarantine totals.
- suppression counts by reason.
- quarantine counts by reason.

### 4. Email Hygiene

Fields:

- `EMAIL_PROMOTION_POLICY_VERSION`.
- total Gmail threads evaluated.
- classification coverage percent.
- unclassified thread count.
- classification source counts:
  - `card_classifications`
  - `classify_index`
  - `frontmatter`
  - `stage0`
  - `new_llm`
- active override count.
- manual override count.
- pending review bucket counts.
- last corpus-hygiene dry-run/apply run ID.

### 5. Processor Health

For each processor:

- pending.
- running.
- complete in last maintenance run.
- failed.
- skipped.
- stale due to input hash.
- stale due to processor version.
- stale due to corpus state.
- last error.
- LLM-provider dependency status if relevant.

### 6. Embedding and Link Health

Fields:

- active chunks.
- active chunks embedded.
- suppressed/quarantine chunks excluded.
- embedding model ID.
- pending embeddings.
- linker backlog.
- linker failures.
- suppressed cards excluded from link candidates.

### 7. Maintenance Health

Fields:

- last run ID.
- last run status: `success`, `partial`, `failed`.
- started/completed timestamps.
- source updater errors.
- processor errors.
- rollback points.
- next recommended action.

## Machine-Readable Status Shape

Future implementation should expose JSON roughly like:

```json
{
  "archive": {
    "instance": "arnold",
    "vault_path": "/srv/ppa/secure/vault",
    "schema": "ppa",
    "status": "healthy"
  },
  "sources": [],
  "corpus": {
    "active": {},
    "suppressed": {},
    "quarantine": {}
  },
  "email_hygiene": {
    "policy_version": "email-promotion-v1",
    "classification_coverage": 0.99
  },
  "processors": [],
  "embeddings": {},
  "linkers": {},
  "maintenance": {},
  "v3_readiness": {
    "ready": false,
    "failed_checks": []
  }
}
```

Exact fields may change, but the categories above are required.

Required top-level status fields:

| Field                | Meaning                                             |
| -------------------- | --------------------------------------------------- |
| `archive`            | Instance, vault, schema, build/git info             |
| `sources`            | Section D source state summaries                    |
| `corpus`             | Section B active/suppressed/quarantine summaries    |
| `email_hygiene`      | Policy/classification/decision-run coverage         |
| `processors`         | Section E processor state summaries                 |
| `embeddings`         | Active chunk embedding status                       |
| `linkers`            | Linker backlog/failure/suppression filtering status |
| `maintenance`        | Last maintain/report state                          |
| `validation_gates`   | Section G gate evidence                             |
| `v3_readiness`       | Fail-closed readiness result                        |
| `errors`, `warnings` | Structured issues                                   |

Status command behavior:

- `--format json` emits only JSON on stdout.
- text output is concise but includes failing gate/source/processor names.
- missing config/vault/database should produce structured output and documented exit code.
- status must not run live source sync, processor execution, embeddings, or linkers.

## Maintenance Report Shape

Every `ppa maintain` run should write an append-only report.

Recommended path:

- `ppa/logs/maintenance/maintain-YYYYMMDD-HHMMSS-RUNID.json`

Required sections:

- run metadata.
- source updater summaries.
- corpus decision summaries.
- processor summaries.
- embedding summaries.
- linker summaries.
- errors and warnings.
- readiness checks.
- next action.

Reports should be both:

- machine-readable JSON.
- summarized to stderr/stdout for operators.

Section F should not require fully rewriting `ppa maintain` in its first implementation. It can start by aggregating existing source/corpus/processor/gate reports and adding a report writer hook for future maintain cycles.

## Health Thresholds

Initial status thresholds:

| Check                         | Healthy                | Warning               | Failed                                  |
| ----------------------------- | ---------------------- | --------------------- | --------------------------------------- |
| Source updater                | Last run succeeded     | Stale or partial      | Failed/blocked                          |
| Gmail classification coverage | >= 98%                 | 95-98%                | < 95%                                   |
| Quarantine backlog            | Small and reviewed     | Growing               | Unreviewed large backlog                |
| Processor failures            | 0 blocking failures    | Non-blocking failures | Blocking failures                       |
| Embedding coverage            | Active chunks embedded | Pending backlog       | Embedding failures                      |
| Suppression filter            | Suppressed excluded    | Unknown               | Suppressed visible in default retrieval |
| Rebuild safety                | Verified               | Not recently verified | Failed verification                     |

The implementation can tune exact numeric thresholds, but failures should be explicit.

## v3 Readiness Gate

v3 should not resume packaging until Arnold passes this gate after v2.5 implementation.

### Required Checks

1. **Current corpus cleaned**
   - Dry-run and apply completed.
   - Active/suppressed/quarantine counts recorded.
   - Review buckets resolved or intentionally deferred.

2. **Classification reuse proven**
   - Existing classifications reused.
   - New LLM calls limited to missing/stale inputs.
   - Coverage meets threshold.

3. **Suppression visible everywhere it matters**
   - Suppressed email absent from default query.
   - Suppressed email absent from hybrid/vector retrieval.
   - Suppressed email excluded from semantic/linker candidate generation.
   - Suppressed email excluded from future enrichment queues.

4. **Future Gmail sync promotion working**
   - New marketing thread suppresses before card creation.
   - New transactional thread promotes and queues processors.
   - Previously suppressed thread can be re-promoted by owner action.
   - Routine sync does not silently demote active cards.

5. **Source freshness working**
   - Gmail, Calendar, iMessage, and Photos source states are visible.
   - **At least one successful real updater run** (not status snapshot alone) exists per required source within freshness policy, with cursor before/after and dirty UID counts in the run report.
   - Cursor safety verified (cursor advances only after persisted side effects).
   - Source failure isolation verified.

6. **Processor DAG working**
   - Dirty inputs queue only affected processors.
   - **At least one successful real processor run** on dirty UIDs from an updater batch (plan → apply), not declaration/status seed alone.
   - Processor version bump creates expected stale outputs.
   - Suppressed cards skip active-only processors.

7. **Maintenance stable**
   - `ppa maintain --run-source-updaters --run-processors` (or equivalent) can run through normal source/update/process/status flow.
   - Reports are written.
   - Partial failure behavior is visible and recoverable.
   - `--record-source-status` / `--record-processor-status` alone **do not** satisfy this check.

8. **Rebuild safety verified**
   - Full or incremental rebuild preserves corpus decisions.
   - Suppressed email does not reappear as active due to markdown still existing.

### Readiness Output

`ppa status` should show:

```text
v3 readiness: NOT READY
  failed:
    - gmail future sync promotion not verified
    - processor DAG stale-output tests failing
```

or:

```text
v3 readiness: READY
  corpus hygiene: pass
  source freshness: pass
  processor DAG: pass
  observability: pass
```

## Operator Documentation Requirements

v2.5 implementation should produce or update docs explaining:

- How to run corpus hygiene dry-run.
- How to review suppression/quarantine buckets.
- How to apply cleanup.
- How to roll back cleanup.
- How to interpret `ppa status`.
- How to recover source updater failures.
- How to read maintenance reports.
- What must pass before v3 work resumes.

This can live in a future runbook, but Section F must require it.

## Tests and Validation

Future implementation should include:

- Unit tests for JSON status shape.
- Unit tests for health threshold evaluation.
- Unit tests for v3 readiness pass/fail logic.
- Integration tests for status after source updater success/failure.
- Integration tests for status after corpus cleanup apply.
- Integration tests proving suppressed email is absent from retrieval and reported as suppressed.
- Integration tests for maintenance report creation.
- Golden-output tests for human-readable status.
- CLI tests proving status does not mutate source/corpus/processor state.
- CLI tests proving missing vault/config/database returns structured output and exit `4`, not traceback.
- Tests proving readiness fails when Section B/D/E/G evidence is missing, failed, unreviewed, or from the wrong archive instance.

## Validation Ladder and Rust Standard

Observability is the final proof layer for the Section G ladder.

Required gates:

1. **Synthetic fixtures:** status JSON, threshold evaluation, and v3 readiness pass/fail logic work with fixture reports.
2. **Small slice:** status shows corpus decisions, source state, and processor state after dry-run/apply/rollback.
3. **Larger slice:** status remains readable and report size is manageable.
4. **Local seed dry-run:** status summarizes full-seed dry-run without mutation.
5. **Local seed staging apply:** status shows apply, rollback, rebuild-safety, and processor outcomes.
6. **Arnold dry-run:** status shows not-ready until apply/soak checks pass.
7. **Arnold reviewed apply and soak:** status is the authority for v3 readiness.

Rust standard:

- Status and maintenance reports must record engine mode for scan/cache/materialization/chunking paths.
- Readiness fails if required Rust-backed validation was skipped without an explicit waiver.
- Readiness fails on unresolved Rust/Python divergence in active/suppressed materialization or retrieval behavior.
- Reports must include wall-time and throughput for long validation phases so v2.5 can avoid blind long-running jobs.

## Rollback / Recovery

Observability must support recovery:

- Every apply/maintenance run has a run ID.
- Status references the latest rollback-capable decision run.
- Reports include enough source cursor and processor metadata to diagnose partial runs.
- Failed readiness checks include next action.

## Definition of Done

Section F implementation is ready when:

- `ppa status` exposes source freshness, corpus health, email hygiene, processor health, embedding/link health, and maintenance health.
- Machine-readable status exists.
- Append-only maintenance reports exist.
- Health thresholds are explicit.
- v3 readiness gate exists and fails closed.
- Arnold can show why it is or is not ready for v3.
- Operator docs explain dry-run, apply, rollback, status, and maintenance report interpretation.
- v3 readiness cannot pass unless Section G gate evidence exists through Arnold soak.
- **v3 readiness cannot pass on snapshot-only source/processor status;** required checks 5–7 need real run reports from D/E Phase 2.
- status includes report paths, decision run IDs, engine mode, rollback status, and failed gate details.
- status/readiness can run without executing live sync, processors, embeddings, or linkers.
- missing config/vault/database cases return structured output and documented exit code, not tracebacks.
- Section F consumes Sections B, D, E, and G state; it does not duplicate their state models.

## Completion Artifacts

The implementation agent must leave:

- JSON status sample.
- human-readable status sample.
- maintenance report sample.
- readiness pass/fail fixture reports.
- operator runbook references.
- proof that readiness fails when any required Section G gate evidence is missing.
- blocked/failure status examples for missing vault/config/database.
- status sample proving partial failures are visible and not hidden by a green summary.
- status sample proving no live source sync or processor execution occurred.

## Commit Instructions

Commit this section by itself.

- Start only from a clean tree.
- Stage only Section F implementation, tests, docs, and artifacts.
- Commit subject: `v2.5 section F: arnold observability v3 gate`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before D/E Phase 2 or Section H work starts.
- If readiness hardening is needed after D/E Phase 2, use a separate commit: `v2.5 section F: readiness real-run evidence`.

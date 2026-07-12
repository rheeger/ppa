# Section D Execution Plan - Source Updater Contract

## Objective

Define the product-level source freshness contract for v2.5, then **execute** it so sources actually stay current.

Existing adapters already fetch data and maintain cursor-like state. v2.5 names and standardizes the contract above those adapters so every source can answer:

- What external data did we observe?
- What became active cards?
- What was suppressed or quarantined?
- What was deleted or tombstoned?
- What cursor/watermark is safe to commit?
- Which cards are dirty for downstream processors?
- Is the source fresh, stale, failed, or blocked?

## Implementation Phases

### Phase 1 — Contract (LANDED)

Commit: `v2.5 section D: source updater contract` (`ff62a04f` on branch `v2.5`).

Delivered:

- Source updater declarations for Gmail, Calendar, iMessage, Photos, Health/structured templates.
- Batch summary / run report shapes.
- Cursor commit helpers and staleness helpers.
- Status snapshot into `_meta/source-updaters.json` / DB state store.
- CLI: `ppa source-updaters` (declarations, status, report helpers).
- `ppa maintain --record-source-status` snapshots only — **does not run adapters**.

Phase 1 is **not** live updating. Do not treat it as Section D complete for v3 readiness.

### Phase 2 — Execution (NEXT IMPLEMENTATION WORK)

Objective: run existing adapters under the SourceUpdater contract and feed dirty UIDs to Section E.

Non-goals for Phase 2:

- Do not replace `BaseAdapter` / invent a new sync framework.
- Do not require webhooks.
- Do not enable Arnold production updater runs before Section H seed proof.
- Do not silently demote existing active cards during routine sync (Section C rules still apply).

#### Phase 2 Agent Handoff Checklist

Before implementation:

- Confirm tree clean on `v2.5`; Phase 1 D commit present.
- Read this plan, Section C, Section E Phase 2, Section H, and `archive_cli/commands/maintain.py`.
- Prefer wrapping `fetch_batches` / existing sync handler over rewriting adapters.
- Start with Gmail + Calendar; then iMessage + Photos.

Likely files:

- `archive_sync/source_updaters/runner.py` (new) — orchestrate one source update run.
- `archive_sync/source_updaters/batch.py` — fill batch summaries from adapter outcomes.
- `archive_cli/source_updaters/cli.py` — add `run` / `dry-run` commands.
- `archive_cli/commands/maintain.py` — invoke enabled updaters before processor steps.
- Adapter files only where batch metadata must be emitted.

Required CLI shape (names may refine; semantics must hold):

```bash
# Dry-run: plan what would sync; do not advance cursors
ppa source-updaters run --source gmail-messages:<account> --dry-run --format json

# Apply: run adapter batch, persist side effects, then commit cursor
ppa source-updaters run --source gmail-messages:<account> --apply --format json

# Maintain integration
ppa maintain --run-source-updaters [--source KEY ...]
```

Expected exit codes: `0` success, `1` runtime, `2` validation, `3` refused, `4` blocked (auth/provider).

#### Phase 2 Required Behavior

1. Resolve declaration → adapter instance → current cursor from `sync-state.json` / state store.
2. Run adapter `fetch_batches` (or existing sync entrypoint) with promotion gate for Gmail (Section C).
3. Persist cards / ledger / tombstones **before** cursor commit.
4. Write `SourceUpdaterRunReport` with observed/promoted/suppressed/quarantined/updated/deleted, `dirty_card_uids`, cursor before/after, status.
5. Persist dirty UIDs where Section E can consume them (file under run artifacts and/or DB).
6. Isolate failures per source; one failed source must not block others.
7. Auth/permission failures → `blocked` (exit `4`), not endless retry.

#### Phase 2 Definition of Done

- Gmail and Calendar: `--apply` advances cursor only after persisted side effects; reports dirty UIDs.
- iMessage and Photos: same contract (may land in same commit or immediate follow-up commit `v2.5 section D: source updater execution sources`).
- `ppa maintain --run-source-updaters` runs enabled sources and writes reports under `ppa/logs/v2_5/` or `ppa/logs/validation-gates/`.
- Tests: fixture adapter runs, cursor safety, failure isolation, Gmail promotion-gated batch counts.
- Commit subject: `v2.5 section D: source updater execution`

#### Phase 2 Completion Artifacts

- Runner module + CLI.
- Example run reports for Gmail and Calendar (fixture or seed dry-run).
- Dirty UID artifact path documented for E Phase 2.
- Maintain flag wiring.
- Focused tests passing; tree clean after one commit.

## Non-Goals (whole section)

- Do not replace the existing adapter framework.
- Do not force every source to use Gmail-style promotion policy.
- Do not require webhooks for sources that only support polling.
- Do not define v3 setup UX here; v3 consumes this contract later.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_a_email_corpus_semantics.plan.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_c_future_gmail_sync_promotion.plan.md`
- `ppa/archive_sync/source_updaters/` (Phase 1)
- `ppa/archive_sync/adapters/base.py`
- `ppa/archive_sync/adapter_contracts.py`
- `ppa/archive_sync/handler.py`
- `ppa/archive_vault/sync_state.py`
- `ppa/archive_sync/adapters/gmail_messages.py`
- `ppa/archive_sync/adapters/calendar_events.py`
- `ppa/archive_sync/adapters/imessage.py`
- `ppa/archive_sync/adapters/photos.py`
- `ppa/archive_cli/commands/maintain.py`

## Core Concept (Phase 1 + Phase 2)

`SourceUpdater` is a contract layered over an adapter. It does not replace adapters.

Adapter responsibility:

- Fetch source records.
- Convert active records to cards.
- Write through the existing vault merge/write path.
- Maintain low-level cursor state.

SourceUpdater responsibility:

- Normalize source identity and status.
- Decide or delegate promotion/suppression/quarantine.
- Emit committed batch summaries.
- Emit dirty cards for downstream processors.
- Persist source health and cursor metadata.
- Provide status data to `ppa status`.

## SourceUpdater Declaration

Each source updater should declare:

| Field                      | Meaning                                                                         |
| -------------------------- | ------------------------------------------------------------------------------- |
| `source_key`               | Stable source/account/scope identity, e.g. `gmail-messages:account@example.com` |
| `source_type`              | `gmail`, `calendar`, `imessage`, `photos`, `health`, etc.                       |
| `adapter_name`             | Existing adapter implementation                                                 |
| `adapter_version`          | Version string for fetch/transform logic                                        |
| `promotion_policy_version` | Policy version if the source has a promotion gate                               |
| `cursor_kind`              | `history_id`, `sync_token`, `page_token`, `rowid`, `modified_at`, `hash`, etc.  |
| `supports_incremental`     | Whether incremental sync is supported                                           |
| `supports_deletes`         | Whether source deletion/tombstone can be detected                               |
| `supports_webhook`         | Whether external triggers are supported                                         |
| `requires_polling`         | Whether scheduled polling is required                                           |
| `default_active_policy`    | `all_active`, `promotion_gated`, `metadata_gated`, etc.                         |
| `last_success_at`          | Last successful committed sync                                                  |
| `last_attempt_at`          | Last attempted sync                                                             |
| `last_error`               | Last failure summary                                                            |
| `last_cursor`              | Last committed cursor summary                                                   |

## Committed Batch Contract

Every updater run should return one or more committed batches.

| Field                        | Meaning                                          |
| ---------------------------- | ------------------------------------------------ |
| `batch_id`                   | Stable run/batch ID                              |
| `source_key`                 | Source identity                                  |
| `started_at`, `completed_at` | Batch timing                                     |
| `cursor_before`              | Cursor at batch start                            |
| `cursor_after`               | Cursor committed after persistence               |
| `observed`                   | External records observed                        |
| `unchanged`                  | Records skipped as unchanged                     |
| `promoted`                   | Records that produced active cards               |
| `suppressed`                 | Records intentionally not promoted               |
| `quarantined`                | Records held for review                          |
| `updated`                    | Existing active cards updated                    |
| `deleted_or_tombstoned`      | External records removed/unavailable             |
| `dirty_card_uids`            | Cards that downstream processors should evaluate |
| `decision_run_id`            | Corpus decision run if applicable                |
| `errors`                     | Batch errors                                     |
| `warnings`                   | Non-fatal issues                                 |

Cursor commit rule:

- `cursor_after` is committed only after active card writes, suppression/quarantine records, tombstones, and dirty-set metadata are persisted.

This preserves the existing adapter safety rule that cursor patches should not skip unpersisted work.

## Source State Store

Implementation should persist source status in a durable store. It may begin as `_meta/source-updaters.json` or a Postgres table, but it must become visible to `ppa status`.

Required logical fields:

| Field                | Meaning                                               |
| -------------------- | ----------------------------------------------------- |
| `source_key`         | Primary identity                                      |
| `source_type`        | Source type                                           |
| `enabled`            | Whether source participates in maintenance            |
| `last_success_at`    | Last successful sync                                  |
| `last_attempt_at`    | Last attempted sync                                   |
| `last_error_at`      | Last error timestamp                                  |
| `last_error`         | Error class/message                                   |
| `cursor_summary`     | Human-readable cursor                                 |
| `cursor_payload`     | Machine-readable cursor                               |
| `adapter_version`    | Adapter version at last success                       |
| `policy_version`     | Promotion policy at last success                      |
| `last_batch_summary` | Counts from last committed batch                      |
| `staleness_state`    | `fresh`, `stale`, `failed`, `blocked`, `never_synced` |

## Source-Specific Contracts

### Gmail

Default policy: `promotion_gated`.

Cursor:

- Prefer Gmail History API `historyId`.
- Keep page-token scan fallback.
- Keep quick-update hashes for active card rewrite avoidance.

Batch behavior:

- Observed threads become active, suppressed, or quarantine.
- Suppressed/quarantine records must be persisted before cursor commit.
- New or changed active cards emit dirty UIDs for processors.
- Routine sync does not silently demote existing active cards; it emits recommendations.

Deletes:

- If Gmail reports deletion, mark corresponding active email cards as tombstoned or unavailable.
- Suppressed ledger records may update status to `source_deleted`.

### Google Calendar

Default policy: `all_active`.

Cursor:

- Prefer Google Calendar sync token.
- Use event `etag` for quick-update.

Batch behavior:

- Events are active by default because they represent intentional time blocks.
- Updated events emit dirty UIDs for embeddings/linkers.
- Cancelled/deleted events become inactive/tombstoned, not silently removed.

Deletes:

- Calendar deletions should create tombstone/inactive state and dirty affected linkers.

### iMessage

Default policy: `all_active`.

Cursor:

- Continue rowid tailing with `last_completed_message_rowid`.

Batch behavior:

- Messages and threads are active by default because they are personal communications.
- Updated/deleted semantics are best-effort because local Messages data may not expose provider-complete history.

Deletes:

- If local deletion is detectable, mark tombstone/inactive. If not, report delete support as limited.

### Photos

Default policy: `metadata_gated`.

Cursor:

- Use modified-at and metadata hash.

Batch behavior:

- Metadata cards are active when they provide temporal/location context.
- Binary/photo assets are not automatically semantic artifacts.
- Large binary processing is out of scope for v2.5.

Deletes:

- Missing assets can become tombstoned if the source scan can distinguish deletion from permission/path errors.

### Health and Structured Sources

Default policy: `all_active`.

Rationale:

- These sources are already curated or structured.
- Records generally represent facts/actions, not marketing.

Behavior:

- Use source-specific cursors/hashes.
- Emit dirty UIDs for downstream processors when structured fields change.
- Tombstone unavailable records when the source can prove deletion.

## Integration With `ppa maintain`

Target order after D Phase 2 + E Phase 2:

1. Run enabled source updaters (`--run-source-updaters`).
2. Persist source batch reports and dirty UIDs.
3. Materialize active card changes.
4. Evaluate processor DAG against dirty UIDs and run pending processors (E Phase 2).
5. Embed active changed chunks (dirty only; opt-in for full).
6. Run affected linkers (dirty only; opt-in for all).
7. Write maintenance report / optional status snapshots.
8. Update maintenance watermark/status.

Phase 1 only did step 7 snapshots. Phase 2 must add steps 1–2. E Phase 2 adds 4–6.

## Error Handling

Source updater failures should be isolated by source.

States:

| State          | Meaning                                                          |
| -------------- | ---------------------------------------------------------------- |
| `fresh`        | Last sync succeeded and is within freshness policy               |
| `stale`        | Last success is older than freshness policy                      |
| `failed`       | Last attempt failed                                              |
| `blocked`      | Requires manual action, auth, permission, or source availability |
| `never_synced` | Configured but not yet successful                                |

Failure rules:

- A failed source should not block other source updaters.
- A failed batch must not commit cursor past unpersisted records.
- Errors should include retryability and next action.
- Auth/permission failures should be marked `blocked`, not retried endlessly.

## Tests and Validation

Future implementation should include:

- Unit tests for source updater declaration validation.
- Unit tests for committed batch summaries.
- Unit tests for cursor commit safety.
- Unit tests for source state transitions.
- Gmail fixture test for promoted/suppressed/quarantine batch counts.
- Calendar fixture test for sync token/etag update.
- iMessage fixture test for rowid cursor.
- Photos fixture test for metadata hash skip.
- Maintain integration test proving one source failure does not block other sources.
- Status test proving source state appears in production status.

## Validation Ladder and Rust Standard

Source updaters must prove cursor and dirty-set behavior before Arnold enablement.

Required gates:

1. **Synthetic fixtures:** source declarations, batch summaries, cursor commit safety, and state transitions pass.
2. **Small slice/source fixture:** Gmail, Calendar, iMessage, and Photos representative batches produce expected promoted/suppressed/quarantine/dirty counts.
3. **Larger slice:** run source updater reporting at realistic volume and capture wall-time.
4. **Local seed dry-run:** source updater state is computed without mutating canonical seed.
5. **Local seed staging apply:** cursor and dirty-set persistence are tested against staging state.
6. **Arnold dry-run:** source freshness and proposed batch behavior are reported without changing production.
7. **Arnold enablement:** source updater runs only after cursor rollback/recovery is documented.

Rust standard:

- Use Rust cache/type-filtered scans to reconcile active card state during source updater validation.
- Source reports must include engine mode when cache/materialization paths are used.
- Avoid provider-triggered broad rescans when history/sync-token/rowid/modified-at cursors are available.
- Rust/Python divergence in dirty-card discovery blocks Arnold enablement.

## Operational Reporting

Every source updater run should produce a machine-readable report:

| Field                                                                                   | Meaning                                   |
| --------------------------------------------------------------------------------------- | ----------------------------------------- |
| `source_key`                                                                            | Source identity                           |
| `status`                                                                                | `success`, `partial`, `failed`, `blocked` |
| `cursor_before`, `cursor_after`                                                         | Cursor summary                            |
| `observed`, `promoted`, `suppressed`, `quarantined`, `updated`, `deleted_or_tombstoned` | Counts                                    |
| `dirty_card_uids_count`                                                                 | Dirty count                               |
| `errors`, `warnings`                                                                    | Issues                                    |
| `next_action`                                                                           | Human-readable recovery/action            |

Section F consumes these reports.

## Rollback / Recovery

Recovery rules:

- Cursor rollback should use the prior source state snapshot if a bad batch is detected before subsequent successful sync.
- Active card rollback should use existing vault/projection history and decision records.
- Suppressed Gmail records can be rehydrated from Gmail because Gmail remains source of record.
- Tombstones should be reversible if the source record reappears or deletion was misdetected.

## Definition of Done

**Phase 1 (landed):** declarations, batch shapes, cursor helpers, status snapshots, CLI read paths, focused contract tests.

**Phase 2 (required for H / v3):**

- Source updater **execution** exists for Gmail and Calendar (then iMessage/Photos).
- Committed batch summaries are persisted from real runs.
- Cursor commit safety preserved under apply.
- Gmail uses promotion-gated reporting.
- Dirty UIDs are consumable by E Phase 2.
- `ppa maintain --run-source-updaters` invokes updaters (not only snapshots).
- Section H seed and Arnold updater gates can pass.
- Reports include gate name, engine mode when relevant, cursor before/after, dirty count, errors, next action.

## Completion Artifacts

**Phase 1:** declaration registry, fixture batch reports, cursor-safety tests, status sample.

**Phase 2:** runner + CLI `run`, maintain flag, Gmail/Calendar example run reports, dirty UID artifact, focused execution tests, commit `v2.5 section D: source updater execution`.

## Commit Instructions

Phase 1 already committed as `v2.5 section D: source updater contract`.

Phase 2:

- Start only from a clean tree.
- Stage only Phase 2 execution files, tests, and artifacts.
- Commit subject: `v2.5 section D: source updater execution`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before Section E Phase 2 starts.

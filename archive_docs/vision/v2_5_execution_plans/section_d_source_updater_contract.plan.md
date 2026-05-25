# Section D Execution Plan - Source Updater Contract

## Objective

Define the product-level source freshness contract for v2.5.

Existing adapters already fetch data and maintain cursor-like state. v2.5 should name and standardize the contract above those adapters so every source can answer:

- What external data did we observe?
- What became active cards?
- What was suppressed or quarantined?
- What was deleted or tombstoned?
- What cursor/watermark is safe to commit?
- Which cards are dirty for downstream processors?
- Is the source fresh, stale, failed, or blocked?

## Non-Goals

- Do not replace the existing adapter framework in the first implementation.
- Do not force every source to use Gmail-style promotion policy.
- Do not require webhooks for sources that only support polling.
- Do not implement source updaters in this planning pass.
- Do not define v3 setup UX here; v3 consumes this contract later.

## Existing Code and Docs to Inspect Before Implementation

- `ppa/archive_docs/vision/v2_5_execution_plans/README.md`
- `ppa/archive_docs/vision/v2.5vision.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_a_email_corpus_semantics.plan.md`
- `ppa/archive_docs/vision/v2_5_execution_plans/section_c_future_gmail_sync_promotion.plan.md`
- `ppa/archive_sync/adapters/base.py`
- `ppa/archive_sync/adapter_contracts.py`
- `ppa/archive_sync/handler.py`
- `ppa/archive_vault/sync_state.py`
- `ppa/archive_sync/adapters/gmail_messages.py`
- `ppa/archive_sync/adapters/calendar_events.py`
- `ppa/archive_sync/adapters/imessage.py`
- `ppa/archive_sync/adapters/photos.py`
- `ppa/archive_cli/commands/maintain.py`

## Agent Handoff Checklist

Before implementation:

- Read `README.md`, `v2.5vision.md`, Sections C, E, F, G, and this plan.
- Treat `SourceUpdater` as a layer over existing adapters, not a replacement framework.
- Preserve existing `BaseAdapter` cursor commit safety.
- Start by recording source declarations and batch reports before changing maintain behavior.

Likely implementation files:

- `archive_sync/adapter_contracts.py`
- `archive_sync/adapters/base.py`
- source status/report module.
- `archive_cli/commands/maintain.py`
- adapter-specific files only where needed for source status/batch metadata.

Required first tests:

- source declaration validation.
- batch report counts.
- cursor patch commits only after side effects persist.
- one source failure does not block unrelated sources.

Stop conditions:

- design requires replacing all adapters at once.
- source failure can advance cursor past unpersisted work.
- dirty UID discovery requires slow full-vault Python scans where cache/index paths exist.
- status cannot explain blocked/auth-failed sources.

## Core Concept

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

| Field | Meaning |
| ----- | ------- |
| `source_key` | Stable source/account/scope identity, e.g. `gmail-messages:account@example.com` |
| `source_type` | `gmail`, `calendar`, `imessage`, `photos`, `health`, etc. |
| `adapter_name` | Existing adapter implementation |
| `adapter_version` | Version string for fetch/transform logic |
| `promotion_policy_version` | Policy version if the source has a promotion gate |
| `cursor_kind` | `history_id`, `sync_token`, `page_token`, `rowid`, `modified_at`, `hash`, etc. |
| `supports_incremental` | Whether incremental sync is supported |
| `supports_deletes` | Whether source deletion/tombstone can be detected |
| `supports_webhook` | Whether external triggers are supported |
| `requires_polling` | Whether scheduled polling is required |
| `default_active_policy` | `all_active`, `promotion_gated`, `metadata_gated`, etc. |
| `last_success_at` | Last successful committed sync |
| `last_attempt_at` | Last attempted sync |
| `last_error` | Last failure summary |
| `last_cursor` | Last committed cursor summary |

## Committed Batch Contract

Every updater run should return one or more committed batches.

| Field | Meaning |
| ----- | ------- |
| `batch_id` | Stable run/batch ID |
| `source_key` | Source identity |
| `started_at`, `completed_at` | Batch timing |
| `cursor_before` | Cursor at batch start |
| `cursor_after` | Cursor committed after persistence |
| `observed` | External records observed |
| `unchanged` | Records skipped as unchanged |
| `promoted` | Records that produced active cards |
| `suppressed` | Records intentionally not promoted |
| `quarantined` | Records held for review |
| `updated` | Existing active cards updated |
| `deleted_or_tombstoned` | External records removed/unavailable |
| `dirty_card_uids` | Cards that downstream processors should evaluate |
| `decision_run_id` | Corpus decision run if applicable |
| `errors` | Batch errors |
| `warnings` | Non-fatal issues |

Cursor commit rule:

- `cursor_after` is committed only after active card writes, suppression/quarantine records, tombstones, and dirty-set metadata are persisted.

This preserves the existing adapter safety rule that cursor patches should not skip unpersisted work.

## Source State Store

Implementation should persist source status in a durable store. It may begin as `_meta/source-updaters.json` or a Postgres table, but it must become visible to `ppa status`.

Required logical fields:

| Field | Meaning |
| ----- | ------- |
| `source_key` | Primary identity |
| `source_type` | Source type |
| `enabled` | Whether source participates in maintenance |
| `last_success_at` | Last successful sync |
| `last_attempt_at` | Last attempted sync |
| `last_error_at` | Last error timestamp |
| `last_error` | Error class/message |
| `cursor_summary` | Human-readable cursor |
| `cursor_payload` | Machine-readable cursor |
| `adapter_version` | Adapter version at last success |
| `policy_version` | Promotion policy at last success |
| `last_batch_summary` | Counts from last committed batch |
| `staleness_state` | `fresh`, `stale`, `failed`, `blocked`, `never_synced` |

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

Future `ppa maintain` should move toward this order:

1. Run enabled source updaters.
2. Persist source batch reports.
3. Materialize active card changes.
4. Evaluate processor DAG against dirty UIDs.
5. Embed active changed chunks.
6. Run affected linkers.
7. Write maintenance report.
8. Update maintenance watermark/status.

The first implementation can stage this incrementally, but Section F status should treat source updater results as first-class production health.

## Error Handling

Source updater failures should be isolated by source.

States:

| State | Meaning |
| ----- | ------- |
| `fresh` | Last sync succeeded and is within freshness policy |
| `stale` | Last success is older than freshness policy |
| `failed` | Last attempt failed |
| `blocked` | Requires manual action, auth, permission, or source availability |
| `never_synced` | Configured but not yet successful |

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

| Field | Meaning |
| ----- | ------- |
| `source_key` | Source identity |
| `status` | `success`, `partial`, `failed`, `blocked` |
| `cursor_before`, `cursor_after` | Cursor summary |
| `observed`, `promoted`, `suppressed`, `quarantined`, `updated`, `deleted_or_tombstoned` | Counts |
| `dirty_card_uids_count` | Dirty count |
| `errors`, `warnings` | Issues |
| `next_action` | Human-readable recovery/action |

Section F consumes these reports.

## Rollback / Recovery

Recovery rules:

- Cursor rollback should use the prior source state snapshot if a bad batch is detected before subsequent successful sync.
- Active card rollback should use existing vault/projection history and decision records.
- Suppressed Gmail records can be rehydrated from Gmail because Gmail remains source of record.
- Tombstones should be reversible if the source record reappears or deletion was misdetected.

## Definition of Done

Section D implementation is ready when:

- Source updater declarations exist for Gmail, Calendar, iMessage, Photos, and at least one structured source.
- Committed batch summaries are persisted.
- Cursor commit safety is preserved.
- Gmail uses promotion-gated reporting.
- Calendar/iMessage/structured sources default active appropriately.
- Source updater results feed `ppa maintain`.
- Source freshness appears in production status.
- Tests cover cursor safety, failure isolation, and source state transitions.
- Section G gates pass through source updater slice/staging validation before Arnold source enablement.
- Source updater reports include gate name, engine mode when relevant, cursor before/after, dirty count, errors, and next action.

## Completion Artifacts

The implementation agent must leave:

- source declaration registry or equivalent.
- committed batch report examples for Gmail, Calendar, iMessage, Photos, and one structured source.
- cursor-safety test report.
- failure-isolation test report.
- source status sample consumed by Section F.
- no source cursor mutation on Arnold before Arnold dry-run review.

## Commit Instructions

Commit this section by itself.

- Start only from a clean tree.
- Stage only Section D implementation, tests, and artifacts.
- Commit subject: `v2.5 section D: source updater contract`
- Commit body must follow the shared pattern in `README.md`.
- After commit, `git status --short` must be clean before Section E work starts.

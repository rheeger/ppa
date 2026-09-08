# Change publication contract (P02-A)

This is the canonical mutation journal. Search dirty-UIDs are a compatibility
intake, not the event spine. `ChangeRecord` and `ChangeBatch` live in
`archive_engine.contracts` and are not redefined here.

Query quality here means a machine that reads after an edit cites the live
revision. The journal is how that revision stays discoverable after a crash,
and how warehouse / publication / later subscribers keep independent cursors.

## Journal

- Path: `_meta/change-journal.sqlite3` (SQLite WAL, `synchronous=FULL`)
- Schema version: `1` (`JOURNAL_SCHEMA_VERSION`)
- Lock: `_meta/change-journal.lock` plus `BEGIN`-serialized local mutations
- Staged bytes: `_meta/change-journal/staged/<mutation_id>`
- Identity: persisted `archive_id` (explicit `PPA_ARCHIVE_ID`, else a hash of
  the resolved vault root)

Protocol for a file mutation:

1. Reserve `mutation_id` and persist **prepared** (uid, path, before/after
   revision, operation, staged content reference).
2. Validate the path with `resolve_contained_path` / `normalize_vault_rel`.
3. Fsync staged bytes, atomically replace or unlink through the contained
   writer, fsync the parent directory.
4. Persist **committed**. Deletion keeps tombstone metadata (`tombstone_json`).

On open / `mark_vault_written` / explicit `reconcile()`:

- file matches `after_revision` → commit
- file matches `before_revision` and staged bytes exist → resume replace
- file matches `before_revision` and no staged bytes → leave prepared
- anything else → **conflict** (never marked successful)

Idempotency key: `sha256(uid, operation, before_revision, after_revision)`.
A retry of the same key returns the existing record. Two writers cannot apply
against a stale `before_revision`; the journal lock serializes them.

## Consumers

`ChangeBatch.consumer_name` is required. Each consumer has its own high
watermark, per-record acks, and explicit gaps. Acknowledging publication never
advances warehouse (or any other cursor).

| Name          | Slot                                         |
| ------------- | -------------------------------------------- |
| `publication` | First consumer; serving generations (P02-B+) |
| `warehouse`   | Materialization / snapshot                   |
| `vectors`     | Chunks + embeddings                          |
| `graph`       | Edge projection                              |
| `seed-link`   | Seed-link jobs                               |
| `enrichment`  | Enrichment workers                           |
| `claims`      | Future assertion/claim layer                 |

Watermark is the contiguous acked prefix. Out-of-order completions stay in
`consumer_gaps` until that sequence is acked.

## P07 checkpoint hook

`archive_engine.changes.recovery_checkpoint_binding(vault)` returns the same
`{status, reason, value}` envelope P07-A reserved as unavailable:

```python
{
  "archive_id": {"status": "available", "reason": "p02_change_journal", "value": "<id>"},
  "checkpoint": {
    "status": "available",
    "reason": "p02_change_journal",
    "value": {
      "archive_id": "...",
      "schema_version": 1,
      "high_watermark": 12,
      "prepared_count": 0,
      "journal_rel_path": "_meta/change-journal.sqlite3",
      "consumers": {"publication": {"high_watermark": 12, "gaps": []}, "...": {}}
    }
  }
}
```

P07 adopts this hook; this slice does not rewrite `recovery_manifest.py`.

## Warehouse table (migration 009)

Fresh bootstrap and `009_change_consumers` both create `change_consumers`
(`consumer_name`, `high_watermark`, `snapshot_ref`, `gaps_json`). The
canonical cursor remains the local journal; Postgres stores warehouse
snapshot references.

## Legacy DIRTY

`mark_serving_index_dirty` still appends `DIRTY`. Those UIDs are imported as
`legacy_dirty` records when the UID is not already in the journal. DIRTY is
not truncated here. `publish_serving_index` must not call
`serving_index_truncate_dirty` after a successful publish.

Streamed embedding export finalizes an `ExportReceipt` only after both staged
files exist, pair, and checksum. Missing staged files fail closed and must
not fall back to empty in-memory vectors. The existing `PublisherLease`
covers ACTIVE selection through promotion/ack.

## Writer inventory (P02-D)

Covered:

- `archive_vault.vault.write_card` / `update_frontmatter_fields` / `delete_card`
- `archive_sync.adapters.base` write seam (`_write_canonical_card`)
- `mark_vault_written` reconcile hook
- `mark_serving_index_dirty` DIRTY intake (`legacy_dirty`)
- `archive_cli/loader.py` warehouse materialization → `acknowledge_materialized`
- `archive_cli/embedder.py` + `store.embed_pending` → `OPERATION_EMBED`
- `archive_cli/batch_embedder.py` ingest → bounded `request_reconciliation`
- `archive_cli/store.py` rebuild → warehouse ack + DIRTY
- `archive_cli/corpus_hygiene/apply.py` vault-remove → `delete_card`
- Enrichment vault writes (`card_enrichment_runner`) via `mutation_context`
- Manual file edits via `request_reconciliation(uid_to_rel=...)` (no vault walk)

Adapter subclasses and extractors that call `write_card` still journal
create/update. Direct `path.write_text` callers must call
`request_reconciliation` with a cache-built UID map.

P03 publisher port: `archive_engine.publication.publish(eligible_checkpoint, context)
-> PublicationReceipt`.

## Rollback

The journal is additive under `_meta`. Removing `009` drops only the
warehouse cursor table. Legacy DIRTY remains readable. Do not delete
committed mutation rows to “undo” a card; write a new delete/update.

## Generation layout (P02-B)

Each generation is an immutable segment. Incremental publish writes only the
dirty UID set, replacement chunks/edges, new vectors, and explicit tombstones.
The reader walks `layout.json` oldest→newest, newest wins, tombstones hide.

```json
{
  "layout_version": 1,
  "mode": "full | delta | compact",
  "parent_generation": "",
  "base_generation": "",
  "snapshot_id": "",
  "source_watermark": 0,
  "tombstone_uids": [],
  "tombstone_chunk_keys": [],
  "replaced_uids": [],
  "embedding_spec": {}
}
```

Rules:

- Deleted UID = dirty UID with no warehouse row. Absence from a limited query
  is not a tombstone.
- Replacement-by-UID retires all prior chunk keys and incident edges for that
  UID, then writes the new set.
- New vectors carry a full `EmbeddingSpec`. Mixed model/dim/metric/normalization
  vs the parent fails closed. Mixed `chunk_schema` on different live UIDs is
  allowed.
- Vector search unions IVF hits across segments, filters to live keys, then
  ranks. Per-segment candidate budget is `max(k * chain_depth, candidate_budget)`.
- Compaction is an explicit full rebuild (`mode=compact`) when chain depth or
  delta/live-vector ratio exceeds `PPA_PUBLICATION_MAX_CHAIN_DEPTH` /
  `PPA_PUBLICATION_DELTA_RATIO`. Small mutations must not secretly full-export.
- GC keeps ACTIVE, the parent chain, and pinned generations (in-process and
  live interprocess pin files). Dead-PID pin files are ignored and removed.
- One publisher per archive via `PUBLISHER.lock` + `PUBLISHER.lease`. A dead
  owner is stealable; a live owner raises `PublisherBusyError`.
- Pre-promotion validation is fail-closed. `COMPLETE` is fsynced before the
  `ACTIVE` rename. Publication acks only the captured `ChangeBatch`.

`archive_engine.publication.publish(eligible_checkpoint, context)` is the
publisher port P03-D calls. `publish_snapshot` remains the generation writer.

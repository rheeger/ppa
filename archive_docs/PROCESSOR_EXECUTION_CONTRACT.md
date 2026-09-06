# Processor execution contract (P03-A)

This is the scheduler and revision-receipt contract. It does not claim
embedding allowlists (P03-B), dynamic derived outputs (P03-C), or a unified
`maintain` path (P03-D).

Query quality here means a later reader can tell whether a derived card was
actually produced for a specific input revision — or that it stayed blocked —
instead of trusting a loop that marked every requested UID complete.

## Import the frozen receipt

P03 fills `OutputReceipt`. Do not redefine it.

```python
from archive_engine.contracts import OutputReceipt, OutputRevision
from archive_sync.processors.scheduler import ProcessorScheduler
```

`OutputReceipt.outputs` is the compiler artifact later invalidation (P07, a
future assertion layer) uses: created/changed/deleted UID+revision pairs.

## Unit of completion

A receipt is identified by

`(processor_key, processor_version, input_uid, input_revision, dependency_receipt_digest)`.

`input_revision` is the current input hash for that processor. The digest is a
SHA-256 over the required dependency receipts for the same input. Completion
refers to exactly those inputs.

## State machine

Persisted scheduler statuses:

| Scheduler status | `OutputReceipt.status` | Legacy `processor_input_state.status` |
| --- | --- | --- |
| `pending` | `pending` | `pending` |
| `running` | `pending` | `running` |
| `complete` | `completed` | `complete` |
| `valid_no_output` | `completed` (empty `outputs`) | `complete` |
| `blocked_dependency` | `dependency_unmet` | `skipped` |
| `blocked_provider` | `blocked` | `skipped` |
| `retryable_failure` | `failed` | `failed` |
| `permanent_failure` | `failed` | `failed` |
| `superseded` | `skipped` | `skipped` |

Legacy `processor_input_state` rows without a matching receipt are
`legacy_unknown`. They are not evidence that the current revision completed.

## Dependencies

Declarations keep `depends_on` as the required-edge shorthand. Explicit edges
use `ProcessorDependency(processor_key, kind, when)`:

- `required` — failure or block of that prerequisite for this input revision
  prevents the descendant from calling its executor
- `optional` — failure does not block; the digest omits the failed optional
- `conditional` — applies only when `when` (`field=value`) matches the snapshot

A failed prerequisite blocks **only that input's** descendants. Unrelated
inputs continue. A processor that is not in the current run does not invent a
failure; missing receipts are unknown lineage, not a silent complete.

Static cycles and unknown keys in the provided declaration set are rejected
with visiting/visited DFS (`ProcessorGraphError`).

## Leases and restart

`running` rows carry `lease_owner` and `lease_expires_at`. An expired lease
may be retried. `commit_receipt` compare-and-swaps the per-`(processor, uid)`
head pointer: an old worker cannot complete an older revision after a newer
input revision has taken the head. That older write is stored as `superseded`.

Restart retries `pending`, expired `running`, and `retryable_failure`.
`complete` / `valid_no_output` for the current revision+digest are
`already_current`.

## What P03-A does not change

Thin adapters may still call `_complete_items` after aggregate work. The
scheduler will not persist `complete` without an `OutputReceipt`. Embedding
still collects UIDs and calls `embed_pending(limit=...)` with no allowlist —
that is P03-B. Dynamic enqueue of created/changed UIDs is P03-C.

## Schema

Migration `010_processor_receipts` creates `processor_receipts` and
`processor_receipt_heads`. Fresh bootstrap creates the same tables in
`schema_ddl` so marking 010 applied does not leave an empty schema.

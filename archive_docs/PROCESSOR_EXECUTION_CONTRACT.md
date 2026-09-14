# Processor execution contract

An archive can contain a newly imported email before its extracted purchase or updated search entry is ready. Processor receipts identify which outputs were produced for which input revision. Publication uses that evidence so a failed step cannot make unfinished records look current.

This page defines scheduler states, dependencies, revision receipts, and embedding selection. The [processor validation report](reports/processor-execution-validation.md) records the integrated maintenance checks.

## Shared receipt

Processors populate the shared `OutputReceipt` type. Import it rather than redefining it.

```python
from archive_engine.contracts import OutputReceipt, OutputRevision
from archive_sync.processors.scheduler import ProcessorScheduler
```

`OutputReceipt.outputs` lists the identifiers and revisions of records created, changed, or deleted by a processor. Later processing uses those pairs to identify outputs affected by an input change.

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

- `required`: failure or block of that prerequisite for this input revision
  prevents the descendant from calling its executor
- `optional`: failure does not block; the digest omits the failed optional dependency
- `conditional`: applies only when `when` (`field=value`) matches the snapshot

A failed prerequisite blocks **only that input's** descendants. Unrelated
inputs continue. A processor that is not in the current run does not invent a
failure; missing receipts leave the dependency history unknown.

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

## Embedding selection

Dirty embed must pass `uid_allowlist` and/or `chunk_key_allowlist` into
`store.embed_pending`. The predicate is applied in SQL before `limit`, which
is only a budget on the already-selected pending set. Compatible
`EmbeddingSpec` cache hits reuse existing rows and do not call the model.
Provider failure or leftover pending chunk keys leave that card
`failed` or `pending`; neither state marks it complete.

Unscoped backlog drain is reserved for the existing admin route
(`ppa embed-pending`, MCP `archive_embed_pending`, maintain's explicit
`unscoped=True` call).

## Integration with maintenance

Maintenance can enqueue newly created or changed records for further processing. Receipts carry their revisions through to publication, so a source update can make its derived records searchable in the same run. The [processor validation report](reports/processor-execution-validation.md) records the integration checks.

## Schema

Migration `010_processor_receipts` creates `processor_receipts` and
`processor_receipt_heads`. Fresh bootstrap creates the same tables in
`schema_ddl` so marking 010 applied does not leave an empty schema.

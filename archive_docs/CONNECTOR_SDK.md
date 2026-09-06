# Connector SDK (P08-A / P08-B)

A contributor adds a connector by registering a factory. The runtime looks the
factory up by `connector_id`. Do not add a source-specific `if` / `elif` in
core dispatch.

Query quality here means a machine can tell which account a source object
belongs to. The same provider ID in two accounts is two cards.

## Status

P08-A is the SDK + sample tracer. P08-B runs the existing Gmail and calendar
adapters through that runtime with synthetic provider responses.

Gmail/calendar parsing stays in the adapters (`fetch_batches` / `to_card`).
The SDK validates the manifest, wraps batches, persists through the adapter
write/merge seam (so P07 overrides survive), then emits P02 `ChangeRecord`s
and P03 `OutputReceipt`s. The cursor candidate becomes `committed_cursor`
only after that persist.

The sample connector still uses the skip-rewrite contained writer and labels
warehouse publication as pending. Do not claim a sample card is
warehouse-searchable from this slice.

Locally installed Python connector code is trusted executable code. Manifest
validation is a compatibility gate, not a sandbox. Remote untrusted packages
are out of scope until the signed-update design.

## North star

Identity is `(archive_id, source, account_scope, provider_object_id)`.

- `source_id` on the card is `account_scope:provider_object_id`
- Sample UID material includes `archive_id`; existing Gmail/calendar UIDs keep
  their prior recipe (`source + account_scope + provider_object_id`) so cards
  do not move
- Two accounts with the same provider object ID stay distinct

Connectors emit sourced facts and provenance. They do not write inferred
"same person" or "authorized that charge" onto source fields. A provider
tombstone is not an archive-forget.

## Add a connector

1. Create a module that implements `fetch` and `normalize`.
2. Return a `ConnectorManifest` with owned fields, identity recipe, cursor
   schema, delete/retention policy, freshness capability, and fixture metadata.
3. Call `register_connector(connector_id, factory)`.
4. Exercise the connector through `execute_connector` with an injected
   `CanonicalWriter`.

Existing Gmail/calendar adapters set `uses_connector_sdk = True` and resolve
through `archive_sync.connectors.legacy.adapter_for_source`. Handler and
source-updater dispatch for those two IDs go through that helper.

```python
from archive_engine.contracts import AccessContext, ArchiveIdentity
from archive_sync.connectors import (
    execute_connector,
    register_connector,
)
from archive_sync.connectors.runtime import ContainedVaultWriter

writer = ContainedVaultWriter(vault)
result = execute_connector(
    "your.connector",
    identity=identity,
    access=access,
    writer=writer,
    cursor={},
)
```

`AccessContext` is required. A deny or archive-id mismatch fails before write.

## Manifest

Validated **before** a write. Rejected payloads never reach the writer.

Required fields include `connector_id`, `connector_version`, `sdk_version`,
compatible engine/card-contract range, supported sources and account scopes,
emitted card types, deterministic field ownership, `identity_recipe` (must
include `account_scope`), cursor schema/version, delete and retention policy,
freshness capability/interval, rate/batch limits, auth/egress capabilities,
and fixture metadata. Optional `event_handler` is recorded, not executed.

`sdk_version` must be `"1"`. Secret keys (`token`, `password`, `api_key`, …)
are forbidden on the manifest. `freshness_capability` is one of `polling`,
`event-capable`, `import-only`. Do not mark an import-only fixture as live.

## Fetch / normalize / batch

`fetch` returns an ordered `FetchedBatch`: batch id, source/account, cursor
before, cursor-after **candidate**, raw record refs, and event identities.

`normalize` returns typed canonical proposals with provenance and supporting
source IDs. The runtime checks field ownership and the identity recipe, then
calls the injected writer.

The cursor candidate becomes `committed_cursor` only after every proposal in
the batch has been persisted (create or idempotent replay). A connector cannot
self-report a committed cursor.

Replaying the same page yields the same UID and does not create a second file.
Manual corrections applied through P07 survive that replay.

## Engine contracts

Import these; do not copy them:

- `ArchiveIdentity`
- `AccessContext`
- `ChangeRecord` (emitted after a durable create or non-duplicate update)
- `OutputReceipt` (emitted for the same persist)

The writer is a protocol. No new `isinstance` checks against
`DefaultArchiveStore` or other store types. Connectors do not run warehouse
SQL.

## Sample

`sample.fixture` is fixture-backed. It emits `email_message` cards for two
accounts that share `sample-msg-001`. There is no live Gmail (or any live
provider) path.

## Existing connectors (P08-B)

`gmail-messages` and `calendar-events` register in `connectors/legacy.py`.
Tests and acceptance use synthetic `to_card` items only — no live Google
credentials, no seed vault.

## Lifecycle (P08-C)

`archive_sync.connectors.replay` owns cursor migration, expired-cursor state,
duplicate/out-of-order events, scoped thread dirty-UIDs, provider tombstone
versus archive-forget, and pending context scopes.

- Cursor v1 → v2 keeps `history_id` / `page_token`. Expired tokens are
  `expired`, not a silent mailbox reset. Bounded replay is opt-in and capped.
- A reply dirties the thread UID plus changed message UIDs. Unrelated threads
  stay clean.
- Burst keys use P01-B1 identity (`p01b1-burst-1`) when a resolver is
  attached. Without one, burst freshness is `unknown` and pending scopes wait.
- Incompatible connector versions restore the last safe cursor and never
  delete canonical cards.

## Later slices

- **P08-D** — contributor template and quality gate; P09 registers the CLI

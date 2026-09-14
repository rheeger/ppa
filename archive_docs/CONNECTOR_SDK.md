# Connector SDK reference

A connector brings a service or export into PPA's shared record format. Once imported, those records can be searched alongside the rest of the archive. This reference defines how a connector preserves account identity, source fields, and progress across repeated imports. The [specification](SPECIFICATION.md#sources) lists supported sources.

## Identity and source ownership

Connector identity is `(archive_id, source, account_scope, provider_object_id)`. Two accounts can contain the same provider object ID without describing the same record.

The card's `source_id` is `account_scope:provider_object_id`. Sample and contributor UIDs include `archive_id`; the Gmail and Calendar bridge preserves existing UID recipes so imported cards do not move.

Emit the source's fields with provenance. Do not write an inferred identity or an assertion such as “authorized that charge” into a source-owned field. A provider deletion and a user's decision to forget an archived record are separate lifecycle events.

## Add a connector

1. Copy the [connector template](examples/connector-template/README.md) into a package for your source.
2. Fill `manifest.json`, implement `fetch` and `normalize` in `connector.py`, and supply `fixtures.json` with valid records and a negative case that must not emit.
3. Register the factory with `register_connector(connector_id, factory)`.
4. Run the replay and compatibility check in a disposable vault:

   ```bash
   unset PPA_TEST_PG_DSN
   ppa connector check \
     --package archive_docs/examples/connector-template \
     --vault /tmp/ppa-connector-check \
     --output /tmp/ppa-connector-check/verdict.json
   ```

The check validates the manifest before importing the connector, replays the fixture, checks account-scoped identity and owned fields, and writes a JSON verdict. Replaying a page must not create another copy of the same record.

The module command `python -m archive_sync.connectors.cli check` is also available. Core dispatch uses the registry; do not add source-specific branches to `archive_sync/handler.py`.

## Manifest contract

The manifest declares compatibility before a write. Required fields cover connector and SDK versions, engine and card-contract ranges, supported sources and accounts, emitted types, deterministic field ownership, and an identity recipe that includes `account_scope`.

It also declares cursor schema and version, delete and retention policy, freshness capability and interval, rate and batch limits, authentication and egress capabilities, and fixture metadata. An optional `event_handler` is recorded, not executed.

`sdk_version` must be `"1"`. Secrets such as tokens, passwords, and API keys are forbidden in the manifest. `freshness_capability` is `polling`, `event-capable`, or `import-only`. An export should not claim live freshness.

Installed connector code is trusted executable Python. Manifest validation checks compatibility; it is not a sandbox for untrusted packages.

## Fetch, normalize, and persist

`fetch` returns an ordered `FetchedBatch`. `normalize` returns typed canonical proposals. A cursor becomes committed only after persistence succeeds.

Import the shared `ArchiveIdentity`, `AccessContext`, `ChangeRecord`, and `OutputReceipt` contracts. Write through the contained writer protocol rather than warehouse SQL or direct file writes:

```python
from archive_sync.connectors import execute_connector
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

This sketch assumes the factory is registered and the caller has supplied the instance, access context, and vault. A deny or archive-ID mismatch must fail before writing.

## Replay and adapter compatibility

`archive_sync.connectors.replay` handles cursor migration, expired cursors, duplicate and out-of-order events, thread changes, provider tombstones, and pending context. Missing context resolution leaves freshness unknown.

`sample.fixture` uses the SDK directly. Gmail messages and Calendar events run through the adapter bridge. Other adapters still use the earlier ingest path; run `ppa connector legacy-list` to inspect them. The [source specification](SPECIFICATION.md#sources) distinguishes incremental updates from export imports and inactive refresh paths.

The next adapter migration should use `register_connector` and `archive_sync.connectors.legacy.adapter_for_source`. Preserve account identity and replay behavior, then show that the new records are readable through the shared query engine.

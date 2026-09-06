# Connector SDK (P08-A through P08-D)

A contributor adds a connector by registering a factory. The runtime looks the
factory up by `connector_id`. Do not add a source-specific `if` / `elif` in
core dispatch. Do not edit `archive_sync/handler.py`.

Query quality here means a machine can tell which account a source object
belongs to. The same provider ID in two accounts is two cards.

## Status

P08-A is the SDK + sample tracer. P08-B runs Gmail and calendar through that
runtime with synthetic provider responses. P08-C owns cursor lifecycle and
thread freshness. P08-D is the contributor template and quality/replay command.

`python -m archive_sync.connectors.cli` is the SDK test command. P09 will
register it on the product CLI. It is not wired into handler dispatch here.

Locally installed Python connector code is trusted executable code. Manifest
validation is a compatibility gate, not a sandbox. Remote untrusted packages
are out of scope until the signed-update design.

## North star

Identity is `(archive_id, source, account_scope, provider_object_id)`.

- `source_id` on the card is `account_scope:provider_object_id`
- Sample and contributor UIDs include `archive_id`; Gmail/calendar keep their
  prior recipe so existing cards do not move
- Two accounts with the same provider object ID stay distinct

Connectors emit sourced facts and provenance. They do not write inferred
"same person" or "authorized that charge" onto source fields. A provider
tombstone is not an archive-forget.

## Add a connector

1. Copy `archive_docs/examples/connector-template/`.
2. Fill `manifest.json`, `connector.py` (`fetch` + `normalize`), and
   `fixtures.json` (include a negative that must not emit).
3. Call `register_connector(connector_id, factory)` in the package.
4. Run the check command:

```bash
unset PPA_TEST_PG_DSN
.venv/bin/python -m archive_sync.connectors.cli check \
  --package archive_docs/examples/connector-template \
  --vault /tmp/ppa-connector-check \
  --output /tmp/ppa-connector-check/verdict.json
```

The command validates the manifest **before** importing `connector.py`,
replays the fixture page, checks account-scoped identity, owned-field
coverage, and false-promotion negatives, then writes a JSON verdict.

Incompatible `sdk_version` or engine/card range fails before any write.

```python
from archive_sync.connectors import execute_connector, register_connector
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

`fetch` returns an ordered `FetchedBatch`. `normalize` returns typed
canonical proposals. The cursor candidate becomes `committed_cursor` only
after persist. Replay of the same page yields the same UID and does not
create a second file.

## Engine contracts

Import these; do not copy them: `ArchiveIdentity`, `AccessContext`,
`ChangeRecord`, `OutputReceipt`. The writer is a protocol. No warehouse SQL.

## Sample and template

- `sample.fixture` — SDK-native tracer in `archive_sync/connectors/sample.py`
- `example.contributor` — copyable package under
  `archive_docs/examples/connector-template/`

Neither talks to live Gmail.

## Migrated vs remaining adapters

Through the adapter bridge today:

- `gmail-messages`
- `calendar-events`

SDK-native (not a vault adapter): `sample.fixture`

**Still legacy** (pre-SDK ingest). Do not claim these migrated.

Executable: `imessage`, `otter-transcripts`, `file-libraries`, `photos`,
`beeper`, `contacts`, `github-history`, `gmail-correspondents`.

Export-only: `copilot-finance`, `linkedin`, `notion-people`, `notion-staff`,
`apple-health`, `medical-records`, `seed-people`.

Stable seam for the next migration: `register_connector` plus
`archive_sync.connectors.legacy.adapter_for_source`. Print the live list with
`python -m archive_sync.connectors.cli legacy-list`.

## Lifecycle (P08-C)

`archive_sync.connectors.replay` owns cursor migration, expired-cursor state,
duplicate/out-of-order events, scoped thread dirty-UIDs, provider tombstone
versus archive-forget, and pending context scopes. Burst freshness is
`unknown` until a P01 resolver is attached.

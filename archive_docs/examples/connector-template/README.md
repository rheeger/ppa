# Connector template

Copy this directory. Do not edit `archive_sync/handler.py` or add a
source-specific `if` in core dispatch.

## Files

- `manifest.json` — compatibility contract. Validated before `connector.py` loads.
- `connector.py` — `fetch` + `normalize` + `register_connector`.
- `fixtures.json` — synthetic provider page. Include a negative that must not emit.

No live credentials. No seed vault. No warehouse SQL.

## Check

```bash
unset PPA_TEST_PG_DSN
.venv/bin/python -m archive_sync.connectors.cli check \
  --package archive_docs/examples/connector-template \
  --vault /tmp/ppa-connector-check \
  --output /tmp/ppa-connector-check/verdict.json
```

Compatible means: manifest parses, two accounts with the same provider ID stay
distinct, replay creates zero extra cards, owned fields are populated, and
fixture negatives are not promoted onto source fields.

Incompatible `sdk_version` / engine range fails before any write.

## Register

`register_connector("your.id", factory)` in your package. That is the only
registration step. P09 will hang the check command on the product CLI.

## Remaining adapters

`python -m archive_sync.connectors.cli legacy-list` prints the adapters that
still use the pre-SDK ingest path. Only `gmail-messages` and `calendar-events`
are migrated through the adapter bridge. Do not claim the rest have moved.

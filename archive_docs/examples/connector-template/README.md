# Connector template

Use this package to bring a new service or export into an independent PPA archive. Its synthetic records let another contributor check identity, field ownership, and replay without live credentials.

## Files

| File | Purpose |
| --- | --- |
| `manifest.json` | Declares compatibility, source ownership, accounts, and lifecycle behavior |
| `connector.py` | Implements fetch, normalize, and factory registration |
| `fixtures.json` | Supplies a synthetic provider page, including a case that must not emit |

## Try the template

From a configured developer checkout:

```bash
unset PPA_TEST_PG_DSN
ppa connector check \
  --package archive_docs/examples/connector-template \
  --vault /tmp/ppa-connector-check \
  --output /tmp/ppa-connector-check/verdict.json
```

The check verifies that the manifest parses, two accounts with the same provider ID stay distinct, replay creates no extra cards, and owned fields are populated. Incompatible SDK or engine versions fail before writing.

Copy the directory for your connector, replace the fixture and implementation, then register your factory with `register_connector("your.id", factory)`. Keep source-specific dispatch out of `archive_sync/handler.py` and write through the contained writer.

The [connector SDK guide](../../CONNECTOR_SDK.md) defines the full contract. Use `ppa connector legacy-list` to inspect adapters still waiting for migration.

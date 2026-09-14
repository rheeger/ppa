# Engineering playbook

A new source or record type should become useful throughout the archive: searchable, connected to related records, and readable by existing clients. This playbook describes the code and checks that keep those behaviors aligned. The [specification](SPECIFICATION.md) defines current capabilities.

Use an isolated archive for development. Long maintenance jobs follow the [detached-job instructions](../.cursor/skills/long-running-jobs/SKILL.md).

## Adding a new card type

1. Define a Pydantic model in `archive_vault/schema.py` extending `BaseCard`.
2. Give every new field a default value.
3. Decide which fields are deterministic-only and which are LLM-eligible.
4. Register the model in `CARD_TYPES`.
5. Register the path, chunk, and edge profiles in `archive_vault/card_contracts.py`, then update `archive_cli/card_registry.py`, the projection registry, and [card type contracts](CARD_TYPE_CONTRACTS.md) together.
6. Define the vault path convention in the writer that emits the card.
7. Add tests for strict validation, permissive reads, and frontmatter export.

Rules:

- Prefer additive schema changes.
- Do not rename or remove existing fields.
- Use lists for multi-value fields.
- A card should represent something that happened, a booking, a person, or a durable entity. Do not add types that exist only so an agent can summarize.

## Adding a new adapter

Use the [connector SDK](CONNECTOR_SDK.md) for a new service integration. The steps below describe the earlier adapter interface, which still supports sources awaiting SDK migration.

1. Create the adapter in `archive_sync/adapters/`.
2. Implement `fetch()` for raw extraction only.
3. Implement `to_card()` returning `(card, provenance, body)`.
4. Use deterministic provenance for adapter-owned fields unless you have a reason to override field sources.
5. Register the source id in `archive_sync/source_updaters/constants.py` (`EXECUTABLE_ADAPTER_SOURCE_IDS` or `EXPORT_ADAPTER_SOURCE_IDS`).
6. Add tests for `fetch()`, `to_card()`, and shared ingest behavior.
7. Verify with a dry run before touching a real vault.

Rules:

- Do not write files directly from adapters.
- Keep source parsing resilient with column fallbacks.
- Apple Photos and Apple Health currently use imports without active ongoing refresh. A change to refresh behavior needs source-specific validation and an update to the specification.
- One account's mail must not become another archive's evidence.

## Adding a new field to an existing card

1. Add the field with a default in `archive_vault/schema.py`.
2. Update any adapters or enrichment steps that should populate it.
3. Add provenance for the new field.
4. Update relevant tests.
5. Run `ppa validate` against a fixture vault.

Rules:

- Never change an existing field type in place.
- Define defaults explicitly so imports and later reads interpret a missing field consistently.

## Adding a new extractor

Follow the [extractor development instructions](../.cursor/skills/extractor-dev/SKILL.md). Start with a sample of the provider's message formats, map their fields, implement the extractor, and verify both valid transactions and misleading lookalikes. Extractors live in `archive_sync/extractors/`. They make receipts usable as meals, flights, and purchases, so clients can filter and count those records across providers.

## Adding a new enrichment step

1. Create a class in `archive_sync/llm_enrichment/` extending the existing step pattern.
2. Set `name`, `version`, `target_fields`, and `method`.
3. Return `{field_name: (value, provenance_entry)}` from `run()`.
4. Include `input_hash` and enrichment version in provenance.
5. Add tests for `should_run()` and output writes.

Rules:

- Never write deterministic-only fields with `method="llm"`.
- Bump `version` when prompts or logic materially change, so the inference cache does not serve a stale answer.
- Keep model-written fields distinguishable from source-reported fields, so a client can tell what the source actually established.

## Adding a new linker

Follow [CONTRIBUTING_LINKERS.md](CONTRIBUTING_LINKERS.md). Modules live in `archive_cli/linker_modules/`. Linkers exist so an agent can hop from a charge to a purchase, or a flight to a hotel, without guessing from similar titles.

## Switching LLM providers

1. Implement a provider under `archive_cli/providers/` (or the enrichment provider registry already in use).
2. Register it.
3. Keep choice in config and `PPA_*` env, not hardcoded in feature code.
4. Re-run provider tests and any enrichment tests.

Rules:

- Do not hardcode provider choice outside config.
- Keep provider failures non-destructive and cache-aware.

## Running imports safely

1. Bind the instance (`ppa.json` / `PPA_PATH`, `PPA_INDEX_DSN`, `PPA_INDEX_SCHEMA`).
2. Run source work through `ppa maintain --apply` (or a single source updater) so new cards, extractors, and publish stay on one path.
3. Launch that job detached with `--log-file` before the subcommand.
4. Run `ppa validate` and `ppa health` before trusting lookup.
5. Query through the running MCP server so repeated checks reuse the open archive index.

Rules:

- Prefer repeated, smaller imports over giant one-off runs.
- Use dry runs for new sources and schema changes.
- One writer per vault.

## Local Postgres smoke test

Bind a disposable fixture vault and a separate warehouse schema before running these commands. Make targets use the current environment and may otherwise select a maintainer-specific default.

1. Copy `.env.pgvector.example` to `.env.pgvector`.
2. Run `make pg-up`.
3. Run `make bootstrap-postgres`.
4. Run `make rebuild-indexes`.
5. Run `make embed-pending` if you need meaning-based search.
6. Run `ppa status` and a few `ppa search` / `ppa query` / `ppa read` calls.

Rules:

- Keep local Postgres bound to `127.0.0.1`.
- Live MCP and CLI query use the Rust serving index. Postgres is the warehouse.
- Use synthetic fixtures first. For larger samples, set the source explicitly and follow [slice testing](SLICE_TESTING.md).

## Rebuilding the derived index

1. Confirm the vault is the one you intend to index.
2. Set `PPA_PATH`, `PPA_INDEX_DSN`, and `PPA_INDEX_SCHEMA`.
3. Run `python -m archive_cli bootstrap-postgres` the first time against a fresh database.
4. Run `python -m archive_cli --log-file logs/rebuild.log rebuild-indexes` detached for a long rebuild.
5. Run `ppa status` to confirm the published generation.
6. If meaning-based search is in scope, run `embed-pending` for the target model.

Rules:

- Use incremental maintain and publication for ordinary imports. Rebuild when recovery, a source purge, or a schema/index change requires it.
- Rebuild after chunking or typed-edge changes, because those affect lookup even if canonical cards are unchanged.
- Treat the derived index as disposable. If it looks wrong, rebuild it rather than patching it.
- Agent answers should still read canonical cards before final output.

## Checking derived index health

1. Run `ppa status` and `ppa health`.
2. Compare card counts and the published serving generation against expected import results.
3. Run representative `archive_search`, `archive_vector_search`, `archive_hybrid_search`, and `archive_graph` calls through the already-running MCP.
4. Use `archive_retrieval_explain` if ranking looks off.
5. If results look stale, publish or rebuild before investigating deeper.

Rules:

- Do not assume search results are current immediately after an import unless maintain or rebuild has published.
- Use the vault and `ppa validate` when you need source-of-truth confirmation.

## Checking embedding backlog

1. Confirm that maintain or an intentional rebuild has materialized current chunk rows.
2. Run `archive_embedding_status` through MCP for the target model.
3. Run `archive_embedding_backlog` to inspect which chunks remain pending.
4. Run `archive_embed_pending` or detached `ppa embed-pending` to fill pending chunks.

Rules:

- Treat backlog reporting as operational telemetry, not as canonical evidence.
- The built-in `hash` provider is for local plumbing, not production semantic quality.
- Keep provider model and vector dimension aligned with the index configuration.

## Recovery when the index is lost

1. Leave the canonical vault untouched.
2. Restore or recreate the Postgres warehouse if needed.
3. Rebuild indexes and republish the serving generation.
4. Smoke-test exact read, search, and graph queries before relying on the server.

Rules:

- Losing the derived index is recoverable.
- Missing vault records require a backup or another source copy. Protect the vault and required decision state first.
- Verify the restored source content before relying on answers from the rebuilt index.

## Operational commands

```bash
ppa search "banh mi"
ppa query --type meal_order
ppa read <uid>
ppa person "Sarah"
ppa analytics subscriptions
ppa validate
ppa status
ppa health
ppa maintain
ppa serve
ppa mcp-config
```

Admin rebuild, embed, and linker operations: [PPA_RUNTIME_CONTRACT.md](PPA_RUNTIME_CONTRACT.md). Dated operational results: [validation reports](reports/README.md).

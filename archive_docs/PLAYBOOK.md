# PPA contributor playbook

This page is the current how-to for changing the product. Historical HFA / `hey-arnold` / `HFA_LIB_PATH` steps are retired. `ppa` is this repository. Living product state is [STATUS.md](STATUS.md). The human product story is [README.md](../README.md).

Every change should leave an agent more able to cite evidence the user owns. Cards stay canonical. Search, warehouse, and MCP stay derived.

Long jobs (`maintain`, `rebuild-indexes`, `embed-pending`, `slice-seed`, extract, enrich, Gmail catch-up) launch detached. Read `.cursor/skills/long-running-jobs/SKILL.md`. Never a Cursor-managed terminal.

## Adding a new card type

1. Define a Pydantic model in `archive_vault/schema.py` extending `BaseCard`.
2. Give every new field a default value.
3. Decide which fields are deterministic-only and which are LLM-eligible.
4. Register the model in `CARD_TYPES`.
5. Add the path family, chunk profile, edge profile, and projection row to [CARD_TYPE_CONTRACTS.md](CARD_TYPE_CONTRACTS.md) and `archive_cli/card_registry.py` in the same change.
6. Define the vault path convention in the writer that emits the card.
7. Add tests for strict validation, permissive reads, and frontmatter export.

Rules:

- Prefer additive schema changes.
- Do not rename or remove existing fields.
- Use lists for multi-value fields.
- A card should represent something that happened, a booking, a person, or a durable entity. Do not add types that exist only so an agent can summarize.

## Adding a new adapter

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
- Live Photos and Apple Health refresh stay parked unless you are explicitly unparking them. Import-only paths are fine.
- One account's mail must not become another archive's evidence.

## Adding a new field to an existing card

1. Add the field with a default in `archive_vault/schema.py`.
2. Update any adapters or enrichment steps that should populate it.
3. Add provenance for the new field.
4. Update relevant tests.
5. Run `ppa validate` against a fixture vault.

Rules:

- Never change an existing field type in place.
- Never rely on unnamed magic defaults.

## Adding a new extractor

Follow `.cursor/skills/extractor-dev/SKILL.md` (Census, Template Eras, Field Mapping, Implementation, Verification). Extractors live in `archive_sync/extractors/`. They write typed cards (meals, flights, purchases) so an agent can filter by type instead of grepping receipt email.

## Adding a new enrichment step

1. Create a class in `archive_sync/llm_enrichment/` extending the existing step pattern.
2. Set `name`, `version`, `target_fields`, and `method`.
3. Return `{field_name: (value, provenance_entry)}` from `run()`.
4. Include `input_hash` and enrichment version in provenance.
5. Add tests for `should_run()` and output writes.

Rules:

- Never write deterministic-only fields with `method="llm"`.
- Bump `version` when prompts or logic materially change, so the inference cache does not serve a stale answer.
- Language models enrich. They do not become the record.

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
2. Run source work through `ppa maintain` (or a single source updater) so new cards, extractors, and publish stay on one path.
3. Launch that job detached with `--log-file` before the subcommand.
4. Run `ppa validate` and `ppa health` before trusting lookup.
5. Ask through the already-running MCP. Do not cold-open `archive_cli` once per question against a living vault.

Rules:

- Prefer repeated, smaller imports over giant one-off runs.
- Use dry runs for new sources and schema changes.
- One writer per vault.

## Local Postgres smoke test

1. Copy `.env.pgvector.example` to `.env.pgvector`.
2. Run `make pg-up`.
3. Run `make bootstrap-postgres`.
4. Run `make rebuild-indexes`.
5. Run `make embed-pending` if you need meaning-based search.
6. Run `ppa status` and a few `ppa search` / `ppa query` / `ppa read` calls.

Rules:

- Keep local Postgres bound to `127.0.0.1`.
- Live MCP and CLI query use the Rust serving index. Postgres is the warehouse.
- Use a slice (`make test-slice-smoke`, then `make slice-local-1pct`) before touching a large seed.

## Rebuilding the derived index

1. Confirm the vault is the one you intend to index.
2. Set `PPA_PATH`, `PPA_INDEX_DSN`, and `PPA_INDEX_SCHEMA`.
3. Run `python -m archive_cli bootstrap-postgres` the first time against a fresh database.
4. Run `python -m archive_cli --log-file logs/rebuild.log rebuild-indexes` detached for a long rebuild.
5. Run `ppa status` to confirm the published generation.
6. If meaning-based search is in scope, run `embed-pending` for the target model.

Rules:

- Rebuild after imports, source purges, or schema and index field changes.
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

1. Rebuild or rematerialize first so chunk rows are current.
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
- Losing the vault is not. Protect the vault first.
- A rebuilt index is not a license to invent missing cards.

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

Admin rebuild, embed, and linker operations: [PPA_RUNTIME_CONTRACT.md](PPA_RUNTIME_CONTRACT.md). Living-seed ops notes: [STATUS.md](STATUS.md).

# HFA Playbook

## Adding a New Card Type

1. Define a new Pydantic model in `skills/hfa/schema.py` extending `BaseCard`.
2. Give every new field a default value.
3. Decide which fields are deterministic-only and which are LLM-eligible.
4. Register the model in `CARD_TYPES`.
5. Define its vault path convention in the consumer that writes it.
6. Add tests for strict validation, permissive reads, and frontmatter export.

Rules:

- Prefer additive schema changes.
- Do not rename or remove existing fields.
- Use lists for multi-value fields.

## Adding a New Adapter

1. Create the adapter in `skills/archive-sync/adapters/`.
2. Implement `fetch()` for raw extraction only.
3. Implement `to_card()` returning `(card, provenance, body)`.
4. Use `deterministic_provenance()` for adapter-owned fields unless you have a reason to override field sources.
5. Register the CLI command in `skills/archive-sync/handler.py`.
6. Add tests for `fetch()`, `to_card()`, and shared ingest behavior.
7. Verify with `--dry-run` before touching a real vault.

Rules:

- Do not override `ingest()` unless the shared contract truly cannot express the source.
- Do not write files directly from adapters.
- Keep source parsing resilient with column fallbacks.

## Adding a New Field to an Existing Card

1. Add the field with a default in `skills/hfa/schema.py`.
2. Update any adapters or enrichment steps that should populate it.
3. Add provenance for the new field.
4. Update relevant tests.
5. Run doctor validation against a fixture vault.

Rules:

- Never change an existing field type in place.
- Never rely on unnamed magic defaults.

## Adding a New Enrichment Step

1. Create a class extending `EnrichmentStep`.
2. Set `name`, `version`, `target_fields`, and `method`.
3. Return `{field_name: (value, provenance_entry)}` from `run()`.
4. Include `input_hash` and `enrichment_version` in provenance.
5. Add tests for `should_run()` and output writes.

Rules:

- Never write deterministic-only fields with `method="llm"`.
- Bump `version` when prompts or logic materially change.
- Use `GROUNDING_INSTRUCTION` through the provider layer.

## Switching LLM Providers

1. Implement a provider in `skills/hfa/llm_provider.py`.
2. Register it in `PROVIDER_REGISTRY`.
3. Update `_meta/llm-config.json`.
4. Re-run provider tests and any enrichment tests.

Rules:

- Do not hardcode provider choice outside config.
- Keep provider failures non-destructive and cache-aware.

## Running Imports Safely

1. Import in canonical order:
   Apple/VCF -> Google -> LinkedIn -> Notion -> Gmail correspondents
2. Run `bash scripts/hfa-post-import.sh`.
3. Review `_meta/dedup-candidates.json`.
4. Check `archive-doctor validate` output.

Rules:

- Prefer repeated, smaller imports over giant one-off runs.
- Use dry runs for new sources and schema changes.

## Canonical People Seed Import

1. Start from an empty vault.
2. Run `python skills/archive-sync/handler.py seed-people --source-dir <path-to-canonical-People-dir>`.
3. Run `python skills/archive-doctor/handler.py validate`.
4. Run `python skills/archive-doctor/handler.py stats`.

Rules:

- Use this only for the canonical seed set that predates the HFA ingest pipeline.
- Prefer canonical HFA source labels during normalization, e.g. `contacts.apple` instead of `vcf`.
- After the initial seed, use normal adapters for ongoing imports.

## Purging a Source

1. Run `python skills/archive-doctor/handler.py purge-source --source <source-name>`.
2. Confirm identity-map cleanup.
3. Confirm sync-state cleanup.
4. Re-run `stats` and `validate`.

Rules:

- Purge by source only when you intend to re-import or permanently remove that dataset.

## Preview Deploy on the VM

1. Run `make hfa-bootstrap` from the HFA worktree.
2. Run `make hfa-status`.
3. If the backup mount exists, run `make hfa-backup-enable`.
4. Verify with `make hfa-backup-status`.

Rules:

- Keep preview deploys on `hfa` until PR `#6` is merged.
- `make hfa-bootstrap` is the safe path because it syncs a dedicated VM worktree at `/home/arnold/.openclaw/worktrees/hfa`, installs HFA Python deps, initializes the vault, deploys systemd units, and prints remote status.
- `make hfa-backup-enable` intentionally fails if the backup parent path is not mounted on the VM.

## Live Smoke Test

1. Start from an empty vault.
2. Import a small real dataset in canonical order: Apple/VCF -> Google -> LinkedIn -> Notion -> Gmail correspondents.
3. Run `make hfa-post-import`.
4. Review `_meta/dedup-candidates.json`, then run `make hfa-validate` and `make hfa-stats`.
5. Snapshot card hashes, wipe the vault, re-import the same data, and compare hashes to confirm idempotency.

Rules:

- Do the live smoke test only after the HFA pytest suite is green.
- Keep the first smoke dataset intentionally small so failures are easy to inspect and retry.

## External `ppa` Consumer

1. Keep `ppa` in its separate local workspace for now.
2. Point `HFA_LIB_PATH` at `.../hey-arnold/skills`.
3. Point `HFA_VAULT_PATH` at the target vault.
4. Point `ARCHIVE_INDEX_DSN` at the Postgres instance that backs the derived index.
5. Run `python -m pytest archive_tests/test_server.py -q` in the `ppa` workspace after MCP changes.
6. Run `python -m pytest archive_tests/test_retrieval_integration.py -q` when retrieval, chunking, ranking, or embedding behavior changes.

Rules:

- Treat `ppa` as an external consumer of `hfa`, not as code that belongs in the `hey-arnold` PR.
- If more external consumers appear, package `hfa` explicitly rather than copying logic across repos.
- Treat Postgres as the required `ppa` index backend.
- Prefer live `pgvector` retrieval tests over mocked assertions for ranking work.

## Local Postgres Smoke Test

1. `cd ppa`
2. Copy `.env.pgvector.example` to `.env.pgvector`.
3. Run `make pg-up`.
4. Run `make bootstrap-postgres`.
5. Run `make rebuild-indexes`.
6. Run `make index-status`.
7. Run `make embed-pending`.

Rules:

- Keep local Postgres bound to `127.0.0.1`.
- The local Docker workflow auto-discovers the mapped port through the `ppa` Makefile; do not hardcode a port in the smoke path.
- Use the local smoke test before moving changes to Arnold.

## Arnold VM Archive Postgres

1. Run `make hfa-archive-env`.
2. Run `make hfa-archive-pg-enable`.
3. Run `make hfa-archive-pg-status`.
4. Run `make hfa-ppa-sync`.
5. Run `make hfa-ppa-install`.
6. Run `make hfa-ppa-configure`.
7. Run `make hfa-archive-bootstrap-postgres` (DB prerequisites only; use after `pg_restore` the same way).
8. Run `make hfa-archive-index-rebuild`.
9. Run `make hfa-archive-index-status`.

Rules:

- The VM Postgres service is Docker-backed and managed by `systemd`.
- Keep the Postgres listener bound to localhost on Arnold.
- Treat the VM vault as canonical and the index as disposable.
- Use `make hfa-archive-bootstrap` when you want the default end-to-end bootstrap flow.
- `make hfa-archive-index-bootstrap` is a convenience bundle: env, secure env, mount, sync, install, configure, then `hfa-archive-bootstrap-postgres`. Prefer the granular steps above (or `hfa-archive-bootstrap-postgres` alone after code and `.env` are already correct) to avoid redundant unlock/rsync during cutovers.
- For **manual** SSH checks against Postgres or `ppa`, **source `/home/arnold/openclaw/.env`** and use **`$ARCHIVE_INDEX_DSN`** (and related vars) — do not assume the literal password `archive` in examples. See `docs/runbooks/hfa-archive-rollout.md`.

## Rebuilding The Derived Archive Index

1. Confirm the canonical vault is the one you intend to index.
2. Point `HFA_VAULT_PATH` at that vault.
3. Point `HFA_LIB_PATH` at `.../hey-arnold/skills`.
4. Point `ARCHIVE_INDEX_DSN` at the Postgres database used by `ppa`.
5. Run `python -m archive_cli bootstrap-postgres` the first time against a fresh database.
6. Run `python -m archive_cli rebuild-indexes` from the `ppa` workspace.
7. Run `python -m archive_cli index-status` to confirm counts, schema version, `chunk_schema_version`, and `chunk_count`.
8. If semantic retrieval is in scope, run `python -m archive_cli embed-pending` for the target model/version.

Rules:

- Rebuild after imports, source purges, or schema/index field changes.
- Rebuild after chunking or typed-edge changes, because those affect retrieval semantics even if canonical cards are unchanged.
- Bootstrap Postgres first when the database or schema is new.
- Treat the derived index as disposable. If it looks wrong, rebuild it rather than patching it manually.
- Agent answers should still read canonical cards before final output.

## Checking Derived Index Health

1. Run `python -m archive_cli index-status`.
2. Compare card counts, `chunk_count`, and `chunk_schema_version` against expected import results.
3. Run representative `archive_search`, `archive_vector_search`, `archive_hybrid_search`, and `archive_graph` calls through MCP after rebuild.
4. Check retrieval explanation metadata such as `matched_by`, `chunk`, `graph_hops`, and `provenance_bias` if ranking looks off.
5. If results look stale, rebuild indexes before investigating deeper.

Rules:

- Do not assume search/index results are current immediately after an import unless the rebuild step has run.
- Use the canonical vault and `archive-doctor validate` when you need source-of-truth confirmation.

## Checking Embedding Backlog

1. Rebuild the derived index first so chunk rows are current.
2. Run `archive_embedding_status` through MCP for the target model/version.
3. Run `archive_embedding_backlog` through MCP to inspect which chunks remain pending.
4. Run `archive_embed_pending` through MCP or `python -m archive_cli embed-pending` to fill pending chunks for the configured provider.

Rules:

- Treat backlog reporting as operational telemetry, not as canonical evidence.
- The built-in `hash` provider is for local/dev/test plumbing, not production semantic quality.
- For production-quality semantic retrieval, configure `ARCHIVE_EMBEDDING_PROVIDER=openai` and supply an OpenAI key (literal `OPENAI_API_KEY`, or `op://` resolution — see below).
- Keep provider model and vector dimension aligned with the index configuration. `ppa` now rejects mismatched provider/index dimensions.
- Prefer embedding in batches and read failure reporting instead of assuming the entire run succeeded.
- On Arnold, `make hfa-archive-env` writes `ARCHIVE_USE_ARNOLD_OPENAI_KEY`, `ARCHIVE_OPENAI_API_KEY_OP_REF` (default `op://Arnold/OPENAI_API_KEY/credential`), and `ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE` (default general SA file `op-service-account-token`, same vault family as `arnoldlib` — not the gate-only `op-tokens-service-account-token`). `ppa` uses those to `op read` the key at runtime; ensure user `archive` can read the token file (mode/ACL) if systemd loads `.env` but the subprocess still opens the file.

## Evaluating Retrieval Quality

1. Rebuild the index after any chunking, edge, or ranking change.
2. Embed pending chunks for the model/version you want to evaluate.
3. Run the `ppa` pytest suite.
4. If Docker is available, run the live retrieval integration tests so ranking is checked against real Postgres + `pgvector`.
5. Smoke-test a small set of exact, lexical, vector, and hybrid queries against canonical cards.

Rules:

- Ranking work should not rely only on fake-index tests.
- Keep golden retrieval queries grounded in canonical cards and relationships.
- Read canonical cards before deciding that a ranking change is “better.”

## Recovery When The Index Is Lost

1. Leave the canonical vault untouched.
2. Restore or recreate the Postgres index database if needed.
3. Re-run `python -m archive_cli rebuild-indexes`.
4. Re-run `python -m archive_cli index-status`.
5. Smoke-test exact read, search, and graph queries before relying on the server.

Rules:

- Losing the derived index is recoverable.
- Losing the vault is not. Protect the vault first.
- If Postgres is lost but the vault remains intact, rebuild from the vault instead of trying to reconstruct truth from search state.

## Operational Commands

- Import contacts: `python skills/archive-sync/handler.py contacts`
- Import LinkedIn: `python skills/archive-sync/handler.py linkedin --csv-path <path>`
- Import Notion people: `python skills/archive-sync/handler.py notion-people --csv-path <path>`
- Import Copilot: `python skills/archive-sync/handler.py copilot-finance --csv-path <path>`
- Import Gmail correspondents: `python skills/archive-sync/handler.py gmail-correspondents --account-email <email>`
- Import Photos library: `python skills/archive-sync/handler.py photos --library-path <path-to-photoslibrary> --source-label apple-photos --quick-update`
- Build iMessage snapshot bundle: `python scripts/hfa-imessage-snapshot.py --output-dir <snapshot-dir>`
- Import iMessage snapshot: `python skills/archive-sync/handler.py imessage --snapshot-dir <snapshot-dir>`
- Doctor dedup: `python skills/archive-doctor/handler.py dedup-sweep`
- Doctor validate: `python skills/archive-doctor/handler.py validate`
- Doctor stats: `python skills/archive-doctor/handler.py stats`
- Post-import automation: `bash scripts/hfa-post-import.sh`
- Backup automation: `bash scripts/hfa-backup.sh`
- Rebuild ppa index: `python -m archive_cli rebuild-indexes`
- Bootstrap ppa Postgres schema: `python -m archive_cli bootstrap-postgres`
- ppa index status: `python -m archive_cli index-status`

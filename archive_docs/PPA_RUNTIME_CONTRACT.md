# PPA runtime contract

PPA exposes the same archive through a command line and MCP, so a person can change clients without changing the stored history. This contract defines entrypoints, configuration, retrieval behavior, and compatibility requirements for contributors who change those interfaces.

Live retrieval uses the Rust serving index; Postgres is the derived warehouse. Instance configuration resolves through explicit overrides, environment, instance file, and defaults. Breaking changes require an explicit migration path.

## 1. Command-line entrypoints

The canonical entrypoints are:

```
python -m archive_cli serve
```

and, after installing PPA and its native extension, the console script:

```
ppa serve
```

Both invoke `archive_cli.__main__:main`. This starts the MCP server using stdio transport when `serve` is used. When no subcommand is given, `serve` is the default.

### CLI subcommands

| Subcommand | Purpose |
| --- | --- |
| `serve` | Start MCP server (stdio); optional `--tunnel USER@HOST` spawns SSH to `PPA_TUNNEL_PORT` |
| `mcp-config` | Print MCP JSON from current `PPA_*` env; see [config handling](MCP_SETUP.md#configuration) |
| `search <query>` | Full-text search (JSON on stdout) |
| `read <path_or_uid>` | Read one note (JSON) |
| `read-many <uid> …` | Read multiple notes (JSON) |
| `query` | Structured query with `--type` / `--source` / etc. (JSON) |
| `graph <note_path>` | Wikilink graph from a note (JSON) |
| `person <name>` | Person lookup by name, slug, email, or phone (JSON) |
| `timeline` | Notes in date range (JSON) |
| `stats` | Vault/index stats (JSON) |
| `validate` | Validate all vault cards (JSON) |
| `duplicates` | Dedup candidates from `_meta` (JSON) |
| `vector-search <query>` | Semantic search (JSON) |
| `hybrid-search <query>` | Hybrid lexical + vector (JSON) |
| `explain <query>` | Retrieval explain payload (JSON) |
| `embedding-status` | Embedding coverage (JSON) |
| `embedding-backlog` | Pending embedding chunks (JSON) |
| `status` | Report source and publication state for the current instance |
| `instance-status` | Report native, warehouse, authentication, and backup capabilities; configuration alone does not prove freshness |
| `readiness` | Check readiness for the current instance; another deployment's results do not transfer |
| `setup` | Fixture-only independent archive (`sample.fixture`). Does not overwrite an existing root. |
| `maintain` | Incremental maintain + serving publish |
| `rebuild-indexes` | Replace derived index data with a rebuild from the vault |
| `index-status` | Report index health (human-readable text, MCP parity) |
| `bootstrap-postgres` | Create extensions and base schema layout |
| `embed-pending` | Process embedding backlog |
| `migrate` | Apply pending SQL schema migrations |
| `migration-status` | Report migration history and pending count |
| `health` | Check vault, DB, embeddings, migrations |
| `projection-inventory` | List registered typed projections |
| `projection-status` | Show projection coverage |
| `projection-explain <uid>` | Explain projection for a card |
| `duplicate-uids` | Find duplicate UIDs |
| `build-benchmark-sample` | Build a benchmark vault sample |
| `benchmark-rebuild` | Benchmark rebuild performance |
| `evidence` | Retrieve a compact chronological set of source references |
| `temporal-neighbors` | Find records near a timestamp |
| `analytics` | Run typed queries, context expansion, subscription history, trip-cost, and change queries |
| `backup` / `verify-backup` | Create encrypted backup artifacts or verify an existing bundle |
| `restore` | Write a restored archive into a new root |
| `activate-restore` | Rebuild the restored archive's warehouse and serving index |

Seed-link subcommands (`seed-link-*`, `link-*`, `review-link-candidate`, `benchmark-seed-links`) are gated by `PPA_SEED_LINKS_ENABLED` and exit with a message when disabled.

MCP tools whose names end in `_json` (for example `archive_search_json`, `archive_hybrid_search_json`, `archive_retrieval_explain_json`) are unchanged on the wire; they share the same underlying command functions as their non-JSON siblings, with formatting handled in `server.py`.

---

## 2. Environment Contract

### 2.1 Resolution rule

Instance resolution uses the precedence below.
`instance_dir` / an explicit vault path is CLI-equivalent for `storage.vault_path`.
Bound instances skip working-directory `ppa.yml` discovery, so unrelated configuration cannot redirect the archive root.

`_ppa_env()` is the getter for `PPA_*` in feature code. Historical `ARCHIVE_*` aliases
still exist in some launchers; do not treat them as the instance contract.

1. CLI overrides / bound instance directory
2. **`PPA_*`** env var
3. Instance config file (`ppa.json` / `ppa.yml` / `ppa.yaml`)
4. Code default

### 2.2 Core environment variables

| Variable                 | Purpose                    | Default                   |
| ------------------------ | -------------------------- | ------------------------- |
| `PPA_INDEX_DSN`          | Postgres connection string | _(required)_              |
| `PPA_INDEX_SCHEMA`       | Postgres schema name       | `ppa`                     |
| `PPA_PATH`               | Vault root directory       | `~/Archive/vault`         |
| `PPA_EMBEDDING_PROVIDER` | Embedding provider         | `hash`                    |
| `PPA_EMBEDDING_MODEL`    | Embedding model            | `default-embedding-model` |
| `PPA_EMBEDDING_VERSION`  | Embedding schema version   | `1`                       |
| `PPA_MCP_TOOL_PROFILE`   | Tool profile gate          | `full`                    |
| `PPA_CONFIG_PATH`        | Explicit config file path  | _(auto-discover)_         |
| `PPA_RUNTIME_MODE`       | Runtime mode               | `stdio`                   |
| `PPA_SEED_LINKS_ENABLED` | Enable seed-link subsystem | `0` (disabled)            |

### 2.3 Tuning environment variables

These control rebuild, embedding, and flush behavior.

| Variable                           | Default                              |
| ---------------------------------- | ------------------------------------ |
| `PPA_VECTOR_DIMENSION`             | `1536`                               |
| `PPA_CHUNK_CHAR_LIMIT`             | `1200`                               |
| `PPA_EMBED_BATCH_SIZE`             | `32`                                 |
| `PPA_EMBED_MAX_RETRIES`            | `3`                                  |
| `PPA_EMBED_CONCURRENCY`            | `4`                                  |
| `PPA_EMBED_WRITE_BATCH_SIZE`       | _(= batch)_                          |
| `PPA_EMBED_PROGRESS_EVERY`         | `0`                                  |
| `PPA_EMBED_DEFER_VECTOR_INDEX`     | `0`                                  |
| `PPA_REBUILD_WORKERS`              | _(cpu count)_                        |
| `PPA_REBUILD_BATCH_SIZE`           | `1000`                               |
| `PPA_REBUILD_COMMIT_INTERVAL`      | `5000`                               |
| `PPA_REBUILD_PROGRESS_EVERY`       | `10000`                              |
| `PPA_REBUILD_EXECUTOR`             | `thread`                             |
| `PPA_REBUILD_STAGING_MODE`         | `direct`                             |
| `PPA_FORCE_FULL_REBUILD`           | `0`                                  |
| `PPA_DISABLE_MANIFEST_CACHE`       | `0`                                  |
| `PPA_ANYDOC_EXTRACT_CACHE`         | `~/.ppa/anydoc-extract-cache.sqlite` |
| `PPA_SEED_FROZEN`                  | `0`                                  |
| `PPA_REBUILD_RESUME`               | `0`                                  |
| `PPA_REBUILD_FLUSH_MAX_TOTAL_ROWS` | _(adaptive)_                         |
| `PPA_REBUILD_FLUSH_ROW_MULT`       | `120`                                |
| `PPA_REBUILD_FLUSH_MAX_EDGES`      | `100000`                             |
| `PPA_REBUILD_FLUSH_MAX_CHUNKS`     | `50000`                              |
| `PPA_REBUILD_FLUSH_MAX_BYTES`      | `268435456`                          |
| `PPA_OPENAI_TIMEOUT_SECONDS`       | `60`                                 |
| `PPA_OPENAI_MAX_RETRIES`           | `3`                                  |
| `PPA_OPENAI_BASE_URL`              | OpenAI default                       |
| `PPA_STATEMENT_TIMEOUT_MS`         | `30000`                              |
| `PPA_CONNECT_TIMEOUT`              | `5`                                  |

### 2.4 Legacy launcher variables

These variables support older deployment launchers. Their names remain relevant when maintaining those launchers; independent instances use the current configuration contract.

| Variable                              | Purpose                                  |
| ------------------------------------- | ---------------------------------------- |
| `PPA_USE_ARNOLD_OPENAI_KEY`           | Historical 1Password-resolved OpenAI key |
| `PPA_OPENAI_API_KEY_OP_REF`           | Historical 1Password reference           |
| `PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE`   | Historical service-account token file    |
| `PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF` | Historical service-account OP ref        |

Some launchers still accept `ARCHIVE_*` spellings of the same names. Use current instance configuration for new deployments.

### 2.5 Removed environment variables

| Variable       | Status      | Notes                                        |
| -------------- | ----------- | -------------------------------------------- |
| `HFA_LIB_PATH` | **Removed** | Current code imports `archive_vault`; no external HFA checkout is required |

---

## 3. Config File Discovery

1. Bound instance directory (`--instance-dir` / `ppa setup` root / explicit vault)
2. Explicit: `PPA_CONFIG_PATH` (or historical `ARCHIVE_CONFIG_PATH`)
3. Auto-discover in CWD **only when no instance dir or vault is bound** (first match wins):
   - `ppa.yml` / `ppa.yaml` / `ppa.json`
   - legacy `archive-mcp.yml` / `archive-mcp.yaml` / `archive-mcp.json`

Config files are optional. CLI and env beat the file. Secrets are never printed by `config explain`.

---

## 4. Tool Profiles

Tool profiles gate which MCP tools are exposed. Set `PPA_MCP_TOOL_PROFILE`. The [privacy contract](PRIVACY_CONTRACT.md) defines accepted values and handling of invalid profiles. Membership is defined in `archive_engine/access.py`; record restrictions apply separately from tool selection.

### `full` (default)

All tools are exposed when this profile is selected or the variable is unset. Set `read-only` explicitly for a client that should only retrieve records.

### `read-only`

Read and search tools only. No admin, write, or index-lifecycle tools.

Tools: `archive_analytics`, `archive_evidence`, `archive_graph`, `archive_hybrid_search`, `archive_hybrid_search_json`, `archive_knowledge`, `archive_person`, `archive_query`, `archive_read`, `archive_read_many`, `archive_retrieval_explain_json`, `archive_search`, `archive_search_json`, `archive_stats`, `archive_status_json`, `archive_temporal_neighbors`, `archive_timeline`, `archive_vector_search`.

### `remote-read`

Minimal retrieval subset that excludes raw card reads and admin operations. Authentication is configured separately by the deployment.

Tools: `archive_analytics`, `archive_evidence`, `archive_query`, `archive_search`, `archive_search_json`, `archive_stats`, `archive_timeline`.

### `admin-only`

Maintenance and index-lifecycle tools. Not for general retrieval.

Tools: `archive_bootstrap_postgres`, `archive_duplicate_uids`, `archive_duplicates`, `archive_embed_estimate`, `archive_embed_pending`, `archive_embedding_backlog`, `archive_embedding_status`, `archive_index_status`, `archive_link_candidate`, `archive_link_candidates`, `archive_link_quality_gate`, `archive_projection_explain`, `archive_projection_inventory`, `archive_projection_status`, `archive_rebuild_indexes`, `archive_retrieval_explain`, `archive_review_link_candidate`, `archive_seed_link_backfill`, `archive_seed_link_enqueue`, `archive_seed_link_promote`, `archive_seed_link_refresh`, `archive_seed_link_report`, `archive_seed_link_surface`, `archive_seed_link_worker`, `archive_status_json`, `archive_validate`.

---

## 5. Retrieval Semantics

Agent-facing wording (safety, job router, type-filter recipes) is generated from
`archive_cli/mcp_instructions.py` and served as MCP `instructions` plus tool
descriptions. Job recipes live in `.cursor/skills/archive-query/`. Update both
when teaching agents how to query. Those descriptions must preserve the retrieval and evidence rules below.

### Choose and expand retrieval

Start with the most specific information the question provides, then expand as needed:

1. **Exact lookup** for UID, path, email, phone, handle, and provider IDs
2. **Structured query** for type, source, person, org, and date filters
3. **Lexical search** for keyword and phrase recall
4. **Vector retrieval** for vague or cross-cutting natural-language recall
5. **Hybrid retrieval** for lexical anchors plus semantic and graph expansion
6. **Graph expansion** to collect neighboring evidence
7. **Canonical card reads** before final answers

### Source evidence

- Read canonical cards before making factual claims
- Do not treat search hits or embeddings as canonical truth
- Prefer deterministic fields over inferred summaries when they disagree
- Report conflicting evidence and identify the records that disagree.
- If retrieval confidence is low, ask a follow-up or narrow the scope
- If exact reads and search disagree, prefer canonical and treat the index as stale

### Retrieval modes

The [retrieval contract](RETRIEVAL_CONTRACT.md) defines the modes and their evidence rules. The [analytical query contract](EVIDENCE_QUERY_CONTRACT.md) adds completeness, coverage, and freshness requirements for totals and dated evidence.

---

## 6. Remote-Read Boundary

HTTP MCP can serve an archive from the host that owns the vault. Configure authentication and network transport for that deployment. The files stay on the host, while returned records travel to the client.

The `remote-read` profile provides a smaller retrieval set without raw card reads. It does not configure a passkey gate or encryption. The earlier [Arnold security design](runbooks/historical-arnold-security.md) describes those host-specific controls.

[MCP setup](MCP_SETUP.md) covers connection options. [Data boundaries](DATA_BOUNDARIES.md) explains the separate provider and client paths.

## 7. Python Package Surface

### Current packages

| Package | Responsibility |
| --- | --- |
| `archive_cli` | CLI and MCP adapters, store integration, warehouse operations, and maintenance |
| `archive_engine` | Shared runtime contracts, configuration, identity, access, publication, and analytics |
| `archive_vault` | Card models, provenance, identity helpers, and contained file I/O |
| `archive_sync` | Source connectors, adapters, extractors, and processing |
| `archive_crate` | Native scanning, materialization, chunking, validation, and retrieval |
| `archive_doctor` | Validation, duplicate detection, and archive quality |
| `archive_auth` | Source authentication helpers |

### Shared interfaces

CLI and MCP route through `ArchiveStore` in `archive_cli.store` and the shared engine contracts. `PostgresArchiveIndex` in `archive_cli.index_store` supports warehouse operations. It is not the live query engine.

Core engine modules must not import `archive_cli.commands` or `archive_cli.server`. See the [engine contract](ENGINE_CONTRACT.md) for shared types and the [architecture](ARCHITECTURE.md) for runtime composition.

### Schema evolution

Numbered SQL migrations live in `archive_cli/migrations/`. Fresh bootstrap and upgrades must produce compatible schemas; `schema_migrations` tracks applied versions.

A card type needs its model in `archive_vault/schema.py`, metadata in `archive_vault/card_contracts.py`, and derived registration in `archive_cli/card_registry.py` and the projection registry. Update [card type contracts](CARD_TYPE_CONTRACTS.md) and focused tests in the same change.

### Seed links

Seed-link operations remain opt-in through `PPA_SEED_LINKS_ENABLED`. Keep inferred relationships distinguishable from source fields and preserve the [linker quality gates](runbooks/linker-quality-gates.md).

## 8. Archive identity and storage

Each independent instance owns its vault root, archive identity, warehouse schema, serving generation, and journal checkpoint. Two instances may contain the same external provider IDs without sharing records or access.

`archive_id` comes from explicit configuration or a hash of the canonical root and schema binding. Keep that identity consistent when opening the archive through different clients. The [recovery contract](RECOVERY_CONTRACT.md) defines how backups retain the archive identity and checkpoint.

Readable records belong to the vault. The warehouse and serving index support queries and can be rebuilt with the required records and saved decisions. A source account need not remain accessible for clients to read what has already been imported.

## 9. Deployment bindings

The living seed is served by one HTTP MCP process on the archive host. Agents connect with a bearer token. They do not each launch `ppa serve`. Local stdio remains valid for fixtures, slices, and a machine that is not the HTTP owner. Authentication and network transport belong to the deployment. See [http-mcp-singleton.md](runbooks/http-mcp-singleton.md).

`ppa serve --tunnel USER@HOST` can start an SSH forward as a child of the MCP process. The forward stops with the server. `PPA_TUNNEL_PORT` defaults to `5433`, and `PPA_TUNNEL_REMOTE_PORT` defaults to `5432`. This forwards Postgres access; the client still needs the vault and native search index locally unless it queries the archive host through HTTP MCP.

The [historical security design](runbooks/historical-arnold-security.md) and [backup runbook](runbooks/historical-arnold-backup.md) preserve earlier host-specific paths and services. The [MCP reference](MCP_SETUP.md) defines current connection choices.

## 10. Compatibility Policy

A change to commands, environment resolution, tool names, fields, or configuration can break a working archive or client. Preserve existing behavior unless the change includes an explicit migration step and relevant compatibility tests.

New code uses the current `archive_*` packages and `PPA_*` configuration. Older `ARCHIVE_*` and `HFA_*` names may exist at historical launcher boundaries; inspect the affected launcher rather than assuming every alias is accepted in current feature code.

Deprecation notices belong in logs, not on MCP stdout. Keep `ppa serve` and `python -m archive_cli serve` consistent, preserve source record IDs, and retain the distinction between canonical records and derived indexes during migrations.

## 11. Contributor checks

1. Verify equivalent CLI and MCP behavior where the interface is shared.
2. Preserve instance identity, configuration precedence, and access limits.
3. Keep stdout valid for the command's output format and send diagnostics to stderr or configured logs.
4. Update this contract and the relevant setup or agent guide when user-visible behavior changes.
5. Provide an upgrade or recovery path for incompatible schema, index, or configuration changes.

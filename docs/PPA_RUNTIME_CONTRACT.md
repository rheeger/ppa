# PPA Runtime Contract

> **Status**: Frozen as of Phase 2.9 (2026-03-23).
> This document is the single authoritative reference for all PPA runtime surfaces.
> Any breaking change requires an explicit migration step.

---

## 1. CLI Entrypoint

The canonical entrypoints are:

```
python -m archive_mcp serve
```

and, after `pip install -e .`, the console script:

```
ppa serve
```

Both invoke `archive_mcp.__main__:main`. This starts the MCP server using stdio transport when `serve` is used. When no subcommand is given, `serve` is the default.

### CLI subcommands (frozen)

| Subcommand                 | Purpose                                             | Safety on production |
| -------------------------- | --------------------------------------------------- | -------------------- |
| `serve`                    | Start MCP server (stdio)                            | Safe                 |
| `search <query>`           | Full-text search (JSON on stdout)                   | Safe                 |
| `read <path_or_uid>`       | Read one note (JSON)                                | Safe                 |
| `read-many <uid> …`        | Read multiple notes (JSON)                           | Safe                 |
| `query`                    | Structured query with `--type` / `--source` / etc. (JSON) | Safe            |
| `graph <note_path>`        | Wikilink graph from a note (JSON)                   | Safe                 |
| `person <name>`            | Person profile by slug (JSON)                       | Safe                 |
| `timeline`                 | Notes in date range (JSON)                          | Safe                 |
| `stats`                    | Vault/index stats (JSON)                            | Safe                 |
| `validate`                 | Validate all vault cards (JSON)                     | Safe                 |
| `duplicates`               | Dedup candidates from `_meta` (JSON)                | Safe                 |
| `vector-search <query>`    | Semantic search (JSON)                              | Safe                 |
| `hybrid-search <query>`    | Hybrid lexical + vector (JSON)                      | Safe                 |
| `explain <query>`        | Retrieval explain payload (JSON)                    | Safe                 |
| `embedding-status`         | Embedding coverage (JSON)                           | Safe                 |
| `embedding-backlog`        | Pending embedding chunks (JSON)                     | Safe                 |
| `status`                   | Index + runtime status JSON (same as MCP `archive_status_json`) | Safe      |
| `rebuild-indexes`          | Truncate and rebuild all index tables from vault    | **DESTRUCTIVE**      |
| `index-status`             | Report index health (human-readable text, MCP parity) | Slow, may OOM    |
| `bootstrap-postgres`       | Create extensions and base schema layout            | Safe on fresh DB     |
| `embed-pending`            | Process embedding backlog                           | Safe                 |
| `migrate`                  | Apply pending SQL schema migrations                 | Safe                 |
| `migration-status`         | Report migration history and pending count          | Safe                 |
| `health`                   | Check vault, DB, embeddings, migrations             | Safe                 |
| `projection-inventory`     | List registered typed projections                   | Safe                 |
| `projection-status`        | Show projection coverage                            | Safe                 |
| `projection-explain <uid>` | Explain projection for a card                       | Safe                 |
| `duplicate-uids`           | Find duplicate UIDs                                 | Safe                 |
| `build-benchmark-sample`   | Build a benchmark vault sample                      | Safe                 |
| `benchmark-rebuild`        | Benchmark rebuild performance                       | Safe                 |

Seed-link subcommands (`seed-link-*`, `link-*`, `review-link-candidate`, `benchmark-seed-links`) are gated by `PPA_SEED_LINKS_ENABLED` and exit with a message when disabled.

MCP tools whose names end in `_json` (for example `archive_search_json`, `archive_hybrid_search_json`, `archive_retrieval_explain_json`) are unchanged on the wire; they share the same underlying command functions as their non-JSON siblings, with formatting handled in `server.py`.

---

## 2. Environment Contract

### 2.1 Resolution rule

PPA code reads `PPA_*` env vars exclusively via `_ppa_env()`. No alias fallback exists.
Integration layers (e.g. the hey-arnold Makefile) are responsible for translating
their own variable names to `PPA_*` when invoking PPA subprocesses.

1. **`PPA_*`** env var (only source checked)
2. Config file value (if applicable)
3. Code default

### 2.2 Core environment variables

| Variable                 | Purpose                    | Default                            |
| ------------------------ | -------------------------- | ---------------------------------- |
| `PPA_INDEX_DSN`          | Postgres connection string | _(required)_                       |
| `PPA_INDEX_SCHEMA`       | Postgres schema name       | `archive_mcp`                      |
| `PPA_PATH`               | Vault root directory       | `~/Archive/production/hf-archives` |
| `PPA_EMBEDDING_PROVIDER` | Embedding provider         | `hash`                             |
| `PPA_EMBEDDING_MODEL`    | Embedding model            | `default-embedding-model`          |
| `PPA_EMBEDDING_VERSION`  | Embedding schema version   | `1`                                |
| `PPA_MCP_TOOL_PROFILE`   | Tool profile gate          | `full`                             |
| `PPA_CONFIG_PATH`        | Explicit config file path  | _(auto-discover)_                  |
| `PPA_RUNTIME_MODE`       | Runtime mode               | `stdio`                            |
| `PPA_SEED_LINKS_ENABLED` | Enable seed-link subsystem | `0` (disabled)                     |

### 2.3 Tuning environment variables

These control rebuild, embedding, and flush behavior.

| Variable                           | Default        |
| ---------------------------------- | -------------- |
| `PPA_VECTOR_DIMENSION`             | `1536`         |
| `PPA_CHUNK_CHAR_LIMIT`             | `1200`         |
| `PPA_EMBED_BATCH_SIZE`             | `32`           |
| `PPA_EMBED_MAX_RETRIES`            | `3`            |
| `PPA_EMBED_CONCURRENCY`            | `4`            |
| `PPA_EMBED_WRITE_BATCH_SIZE`       | _(= batch)_    |
| `PPA_EMBED_PROGRESS_EVERY`         | `0`            |
| `PPA_EMBED_DEFER_VECTOR_INDEX`     | `0`            |
| `PPA_REBUILD_WORKERS`              | _(cpu count)_  |
| `PPA_REBUILD_BATCH_SIZE`           | `1000`         |
| `PPA_REBUILD_COMMIT_INTERVAL`      | `5000`         |
| `PPA_REBUILD_PROGRESS_EVERY`       | `10000`        |
| `PPA_REBUILD_EXECUTOR`             | `thread`       |
| `PPA_REBUILD_STAGING_MODE`         | `direct`       |
| `PPA_FORCE_FULL_REBUILD`           | `0`            |
| `PPA_DISABLE_MANIFEST_CACHE`       | `0`            |
| `PPA_SEED_FROZEN`                  | `0`            |
| `PPA_REBUILD_RESUME`               | `0`            |
| `PPA_REBUILD_FLUSH_MAX_TOTAL_ROWS` | _(adaptive)_   |
| `PPA_REBUILD_FLUSH_ROW_MULT`       | `120`          |
| `PPA_REBUILD_FLUSH_MAX_EDGES`      | `100000`       |
| `PPA_REBUILD_FLUSH_MAX_CHUNKS`     | `50000`        |
| `PPA_REBUILD_FLUSH_MAX_BYTES`      | `268435456`    |
| `PPA_OPENAI_TIMEOUT_SECONDS`       | `60`           |
| `PPA_OPENAI_MAX_RETRIES`           | `3`            |
| `PPA_OPENAI_BASE_URL`              | OpenAI default |
| `PPA_STATEMENT_TIMEOUT_MS`         | `30000`        |
| `PPA_CONNECT_TIMEOUT`              | `5`            |

### 2.4 Arnold integration environment variables

These are instance-specific to the Arnold deployment and do not use the `PPA_*` canonical prefix. They are part of the Arnold integration seam (§9) and are expected to be provided by the thin Arnold wrapper.

| Variable                              | Purpose                                    |
| ------------------------------------- | ------------------------------------------ |
| `PPA_USE_ARNOLD_OPENAI_KEY`           | Use Arnold's 1Password-resolved OpenAI key |
| `PPA_OPENAI_API_KEY_OP_REF`           | 1Password reference for OpenAI key         |
| `PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE`   | 1Password service account token file       |
| `PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF` | 1Password service account token OP ref     |

All four also accept `ARCHIVE_*` aliases (`ARCHIVE_USE_ARNOLD_OPENAI_KEY`, etc.).

### 2.5 Removed environment variables

| Variable       | Status      | Notes                                        |
| -------------- | ----------- | -------------------------------------------- |
| `HFA_LIB_PATH` | **Removed** | `hfa` is now an installed package dependency |

---

## 3. Config File Discovery

1. Explicit: `PPA_CONFIG_PATH` (or `ARCHIVE_CONFIG_PATH`)
2. Auto-discover in CWD (first match wins):
   - `archive-mcp.yml` / `archive-mcp.yaml` / `archive-mcp.json`
   - `ppa.yml` / `ppa.yaml` / `ppa.json`

Config files are optional. Environment variables always win.

### Config precedence

1. Environment variables
2. Config file
3. Code defaults

---

## 4. Tool Profiles

Tool profiles gate which MCP tools are exposed. Set via `PPA_MCP_TOOL_PROFILE` (or `ARCHIVE_MCP_TOOL_PROFILE`).

### `full` (default)

All tools exposed. Used for local development and direct Arnold access.

### `read-only`

Read and search tools only. No admin, write, or index-lifecycle tools.

Tools: `archive_search`, `archive_read`, `archive_query`, `archive_graph`, `archive_person`, `archive_timeline`, `archive_stats`, `archive_vector_search`, `archive_hybrid_search`, `archive_search_json`, `archive_hybrid_search_json`, `archive_read_many`, `archive_status_json`, `archive_retrieval_explain_json`

### `remote-read`

Minimal subset for passkey-gated remote access. Excludes raw note reads and admin operations.

Tools: `archive_search`, `archive_query`, `archive_timeline`, `archive_stats`, `archive_search_json`

### `admin-only`

Maintenance and index-lifecycle tools. Not for general retrieval.

Tools: `archive_validate`, `archive_duplicates`, `archive_duplicate_uids`, `archive_rebuild_indexes`, `archive_bootstrap_postgres`, `archive_index_status`, `archive_projection_inventory`, `archive_projection_status`, `archive_projection_explain`, `archive_retrieval_explain`, `archive_embedding_status`, `archive_embedding_backlog`, `archive_embed_pending`, seed-link tools

---

## 5. Retrieval Semantics

### Retrieval order (frozen)

Agents should use retrieval methods in this order:

1. **Exact lookup** for UID, path, email, phone, handle, and provider IDs
2. **Structured query** for type, source, person, org, and date filters
3. **Lexical search** for keyword and phrase recall
4. **Vector retrieval** for vague or cross-cutting natural-language recall
5. **Hybrid retrieval** for lexical anchors plus semantic and graph expansion
6. **Graph expansion** to collect neighboring evidence
7. **Canonical card reads** before final answers

### Grounding rules (frozen)

- Read canonical cards before making factual claims
- Do not treat search hits or embeddings as canonical truth
- Prefer deterministic fields over inferred summaries when they disagree
- If evidence conflicts, surface the conflict instead of collapsing it silently
- If retrieval confidence is low, ask a follow-up or narrow the scope
- If exact reads and search disagree, prefer canonical and treat the index as stale

### Retrieval modes (frozen)

| Mode             | Purpose                                                | Truth rule                                                         |
| ---------------- | ------------------------------------------------------ | ------------------------------------------------------------------ |
| Exact read       | Fetch one canonical card by path or UID                | Canonical markdown is the answer source                            |
| Structured query | Filter by deterministic fields                         | Results from derived substrate; claims require canonical grounding |
| Lexical search   | Term/phrase recall over `cards.search_text`            | Retrieval aid, not canonical truth                                 |
| Semantic search  | Vector retrieval over derived chunks                   | Embeddings are lossy; must ground back to canonical cards          |
| Hybrid search    | Combined lexical + vector + graph + provenance ranking | Optimized retrieval, not canonical truth                           |
| Graph expansion  | Expand neighboring evidence                            | Edges derived from canonical references + approved link surfaces   |

---

## 6. Remote-Read Boundary

The remote-read path is passkey-gated and token-authenticated. It provides a read-only subset of the MCP surface.

### Transport model

```
Client -> HTTPS/TLS -> passkey-gate -> local-only transport -> archive-mcp -> vault + Postgres
```

TLS terminates at the gate. The gate authenticates, enforces tool policy, issues scoped tickets, and audits requests.

### Access classes

| Class                     | Scope                                                                          | Tier   |
| ------------------------- | ------------------------------------------------------------------------------ | ------ |
| `mcp.archive.read`        | Low-risk structured retrieval (search, query, timeline, stats, vector, hybrid) | 0      |
| `mcp.archive.sensitive`   | Dense personal content (read, person, graph)                                   | 1      |
| `mcp.archive.admin`       | Maintenance and index lifecycle                                                | 2      |
| `mcp.archive.remote.read` | Public-client scope (search, query, timeline, stats)                           | Remote |

### Security invariants

- `archive-mcp` and Postgres stay off the public network
- Raw vault files are not directly accessible to the `arnold` runtime identity
- Cloud backups contain only encrypted artifacts
- Plaintext archive content exists only inside the mounted unlocked runtime path

---

## 7. Python Package Surface

### Module structure (post-2.1 refactoring)

| Module              | Lines | Role                                                         |
| ------------------- | ----- | ------------------------------------------------------------ |
| `index_config.py`   | ~330  | Constants, env getters (`_ppa_env`), utility functions       |
| `chunk_builders.py` | ~720  | Type-aware chunk building for each card type                 |
| `scanner.py`        | ~330  | Vault scanning, canonical row building, manifest diffing     |
| `materializer.py`   | ~470  | Row materialization, edge building, person lookup            |
| `loader.py`         | ~1200 | Rebuild orchestration, data loading, manifest management     |
| `schema_ddl.py`     | ~460  | DDL, table creation, index management, migration integration |
| `index_query.py`    | ~690  | Search, query, graph traversal                               |
| `embedder.py`       | ~520  | Embedding pipeline                                           |
| `index_store.py`    | ~255  | Thin coordinator, `BaseArchiveIndex`, `PostgresArchiveIndex` |
| `card_registry.py`  | —     | Unified `CardTypeRegistration` entries for all 22 card types |
| `migrate.py`        | —     | `MigrationRunner`: applies numbered SQL migrations           |
| `config.py`         | —     | Config loading with `PPA_*` / `ARCHIVE_*` dual-lookup        |
| `contracts.py`      | —     | Shared dataclasses and protocols                             |
| `errors.py`         | —     | `PpaError` hierarchy for commands vs. string errors          |
| `commands/`         | —     | Shared command layer: search, read, query, graph, status, admin, explain, seed_links; used by MCP and CLI |
| `server.py`         | —     | MCP tool wrappers, tool-profile gating, string formatting    |
| `store.py`          | —     | `ArchiveStore` service boundary                              |

### Import paths (frozen during transition)

| Package          | Role                                 | Future name  |
| ---------------- | ------------------------------------ | ------------ |
| `archive_mcp`    | MCP server, index, retrieval         | `ppa`        |
| `hfa`            | Shared schema, vault I/O, provenance | `ppa_core`   |
| `archive_sync`   | Source adapters                      | `ppa_sync`   |
| `archive_doctor` | Vault validation and repair          | `ppa_doctor` |

These import names are preserved through the Phase 2 split. The `ppa` Python namespace is adopted later with `archive_mcp` as a backward-compatible alias.

### Public API

The public API is `PostgresArchiveIndex` from `archive_mcp.index_store`, consumed by `ArchiveStore` from `archive_mcp.store`. This is the MCP's service boundary.

`PostgresArchiveIndex` MRO: `SchemaDDLMixin` → `EmbedderMixin` → `QueryMixin` → `LoaderMixin` → `BaseArchiveIndex`

### Schema evolution

Schema changes are handled by numbered SQL migrations in `archive_mcp/migrations/`. `_create_schema()` remains the fresh-install path; migrations handle the delta path. The `schema_migrations` table tracks applied versions.

### Card-type registry

Adding a card type is a two-file change:

1. `hfa/schema.py` — add Pydantic model + `CARD_TYPES` entry
2. `archive_mcp/card_registry.py` — add `CardTypeRegistration` entry

Typed projection tables, edge rules, chunk dispatch, and person-edge labelling are derived automatically from the registry.

### Seed-links subsystem

The seed-links subsystem (8 tables, 11 MCP tools, 3,373 lines) is opt-in, gated by `PPA_SEED_LINKS_ENABLED`. When disabled, tables are not created and tools return a clear message.

---

## 8. Runtime Identities and Storage Layout

### Identities

| Identity   | Owns                                    | Runs                              |
| ---------- | --------------------------------------- | --------------------------------- |
| `archive`  | Mounted vault tree, archive index paths | `archive-mcp`                     |
| `arnold`   | OpenClaw, passkey gate                  | General agent, does not own vault |
| `postgres` | Postgres data paths (inside Docker)     | Postgres container                |

### Storage layout (Arnold)

| Path                              | Contents                   |
| --------------------------------- | -------------------------- |
| `/srv/hfa-secure/`                | Encrypted mount root       |
| `/srv/hfa-secure/vault`           | Canonical markdown vault   |
| `/srv/hfa-secure/postgres`        | Postgres data directory    |
| `/mnt/user/backups/hfa-encrypted` | Encrypted backup artifacts |

---

## 9. Arnold Integration Seam

Arnold is a thin consumer of the PPA engine. The integration seam consists of:

### What Arnold provides

- **Secrets**: 1Password-resolved OpenAI API key, service account tokens
- **Env setup**: `PPA_INDEX_DSN`, `PPA_PATH`, `PPA_INDEX_SCHEMA`, `PPA_EMBEDDING_*` via launcher script
- **Encrypted storage**: LUKS-encrypted volume at `/srv/hfa-secure`
- **Docker Postgres**: `hfa-archive-postgres` container with data on the encrypted volume
- **Systemd units**: `hfa-archive-mcp.service`, `hfa-archive-postgres.service` (transitional names; renamed to `ppa-*` in Phase 2.8)
- **Passkey gate**: Authentication and tool-profile enforcement for remote access
- **Backup scheduling**: Orthanc cron job running vault + Postgres backups daily at 03:00 UTC

### What Arnold expects from the PPA

- `python -m archive_mcp serve` starts the MCP via stdio
- All `PPA_*` env vars are accepted (with `ARCHIVE_*` / `HFA_*` aliases)
- Config file discovery works from CWD
- `ArchiveStore` remains the service boundary
- Tool profiles gate the same tool sets
- Retrieval semantics produce consistent results

### What must not change without migration

- CLI entrypoint and subcommand names
- Env var resolution (canonical + alias)
- Config file discovery paths
- Tool profile names and membership
- `ArchiveStore` and `PostgresArchiveIndex` as public classes
- Schema migration runner interface
- MCP tool names (all `archive_*` prefixed)

---

## 10. Compatibility Policy

### Alias support timeline

`ARCHIVE_*` and `HFA_*` env var aliases are **supported indefinitely** during the `archive_mcp` Python package era. They will be deprecated (with warnings) only when the Python package is renamed to `ppa`, which is a separate phase after the boundary split is confirmed stable.

### Breaking change definition

A **breaking change** is any modification that would cause an existing deployment using only `ARCHIVE_*` / `HFA_*` env vars and `archive-mcp.yml` config files to stop working or produce different behavior.

### Migration policy

1. No breaking changes without at least one release where both old and new names work
2. Deprecation warnings are added before removal (logged at startup, not printed to stdout)
3. `PPA_*` canonical names always take precedence when both are set
4. Config file auto-discovery checks both `archive-mcp.*` and `ppa.*` filenames
5. Make targets retain `ARCHIVE_*` variable names during transition (they pass through to Python where aliases are resolved)

### What is frozen

| Surface                | Frozen? | Notes                                                  |
| ---------------------- | ------- | ------------------------------------------------------ |
| CLI subcommand names   | Yes     | No renames during transition                           |
| Env var aliases        | Yes     | Supported until `ppa` package rename                   |
| Config file discovery  | Yes     | Both `archive-mcp.*` and `ppa.*` checked               |
| Tool profile names     | Yes     | `full`, `read-only`, `remote-read`, `admin-only`       |
| MCP tool names         | Yes     | All `archive_*` prefixed                               |
| Retrieval semantics    | Yes     | Order, grounding rules, modes                          |
| Python import paths    | Yes     | `archive_mcp`, `hfa`, `archive_sync`, `archive_doctor` |
| Public API classes     | Yes     | `PostgresArchiveIndex`, `ArchiveStore`                 |
| Schema migration table | Yes     | `schema_migrations` in the configured schema           |

---

## 11. What Must Not Break During The Split

1. `python -m archive_mcp serve` starts the MCP
2. The env contract resolves correctly (with `PPA_*` canonical and `ARCHIVE_*`/`HFA_*` aliases)
3. Config file discovery works the same way
4. Tool profiles gate the same tool sets
5. Retrieval order and grounding rules produce the same behavior
6. The remote-read boundary enforces the same policy
7. `ArchiveStore` remains the MCP's service boundary
8. Schema migrations apply correctly on existing databases
9. Card-type registry produces the same materialized output
10. Seed-link gating behavior is preserved
11. `hfa` is consumed as an installed package, not via `sys.path` hacks
12. Existing systemd units continue to work until explicitly renamed to `ppa-*` in Phase 2.8

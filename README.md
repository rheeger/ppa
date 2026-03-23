# PPA — Personal Private Archives

A semantic memory engine that indexes a personal digital life and turns data exhaust (emails, texts, calendar, photos, medical records, documents, code history) into a searchable, interconnected private knowledge base.

PPA is the **engine**. Each deployment is an **instance** — one person's or family's private data running on the PPA engine.

## Architecture

```
Vault (markdown)  →  Derived Index (Postgres + pgvector)  →  MCP Server (query interface)
     ↑ canonical          ↑ disposable, rebuildable              ↑ tools for agents
```

- **Vault**: canonical truth as markdown files with YAML frontmatter and provenance metadata.
- **Index**: Postgres with pgvector — cards, typed edges, chunks, embeddings, typed projection tables, external IDs.
- **MCP**: stdio-transport MCP server exposing search, read, query, graph, and admin tools.

The vault outlasts any database or query layer. If Postgres dies, re-derive it. If the MCP protocol evolves, swap the server.

## Packages

| Package          | Description                                                    |
| ---------------- | -------------------------------------------------------------- |
| `archive_mcp`    | MCP server, index pipeline, retrieval, embeddings (31 modules) |
| `hfa`            | Schema library, vault I/O, provenance, identity resolution     |
| `archive_sync`   | Source adapters for 18 data sources                            |
| `archive_doctor` | Vault validation, dedup sweep, stats, purge                    |

All four ship as one installable package (`pip install -e .`). Python import names are transitional — the repo is `ppa/` but imports remain `archive_mcp`, `hfa`, `archive_sync`, `archive_doctor` until the canonical rename in a future phase.

## Supported Sources

PPA ships adapters for 18 data sources across six categories:

**Communication**

- Gmail messages (full thread import with incremental sync)
- Gmail correspondents (contact graph from message history)
- iMessage (local `chat.db` snapshot)
- Beeper (unified messaging bridge)
- Otter.ai transcripts (meeting recordings)

**Calendar & Scheduling**

- Google Calendar events

**People & Identity**

- Google Contacts
- LinkedIn (export-driven)
- Notion people directories
- Seed people (manual/curated entries)

**Files & Media**

- File libraries (local directory scan)
- Google Photos (metadata + private annotations)
- GitHub history (commits, PRs, issues)

**Health & Medical**

- Apple Health (export-driven)
- Medical records (structured clinical data)
- Epic EHI (electronic health information export)

**Finance**

- Copilot finance (transaction and account data)

Each adapter produces vault-format markdown cards with typed frontmatter and provenance metadata. Adapters are incremental where the source supports it.

## Quickstart

```bash
cd ppa
python3 -m venv .venv
.venv/bin/pip install -e .
```

### Local Postgres

```bash
cp .env.pgvector.example .env.pgvector
make pg-up
make bootstrap-postgres
make rebuild-indexes
make embed-pending
```

### Run the MCP server

```bash
# Local development (Docker Postgres)
./run-local-seed-mcp.sh

# Remote instance (via SSH tunnel)
./scripts/ppa-tunnel.sh &   # forward port 5433
./run-arnold-mcp.sh
```

### Cursor integration

`~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "archive-local": {
      "command": "/path/to/ppa/run-local-seed-mcp.sh"
    },
    "archive-remote": {
      "command": "/path/to/ppa/run-arnold-mcp.sh"
    }
  }
}
```

## Environment Variables

All env vars use the `PPA_` prefix.

### Core

| Variable               | Description                                                    | Default                     |
| ---------------------- | -------------------------------------------------------------- | --------------------------- |
| `PPA_PATH`             | Vault directory path                                           | `~/Archive/vault`           |
| `PPA_INDEX_DSN`        | Postgres connection string                                     | (required)                  |
| `PPA_INDEX_SCHEMA`     | Postgres schema name                                           | `archive_mcp`               |
| `PPA_INSTANCE_NAME`    | Human-readable instance name for the MCP server                | `Personal Private Archives` |
| `PPA_MCP_TOOL_PROFILE` | Tool profile: `full`, `read-only`, `remote-read`, `admin-only` | `full`                      |
| `PPA_CONFIG_PATH`      | Explicit config file path                                      | (auto-discover `ppa.yml`)   |

### Embeddings

| Variable                            | Description                                   | Default                       |
| ----------------------------------- | --------------------------------------------- | ----------------------------- |
| `PPA_EMBEDDING_PROVIDER`            | `hash` (dev/test) or `openai` (production)    | `hash`                        |
| `PPA_EMBEDDING_MODEL`               | Model label for lifecycle tracking            | `default-embedding-model`     |
| `PPA_EMBEDDING_VERSION`             | Version number for lifecycle tracking         | `1`                           |
| `PPA_EMBED_BATCH_SIZE`              | Chunks per embedding batch                    | `32`                          |
| `PPA_EMBED_CONCURRENCY`             | Concurrent embedding workers                  | `4`                           |
| `PPA_EMBED_PROGRESS_EVERY`          | Print progress every N chunks                 | `0`                           |
| `PPA_EMBED_DEFER_VECTOR_INDEX`      | Drop/rebuild ANN index around large backfills | unset                         |
| `PPA_USE_ARNOLD_OPENAI_KEY`         | Resolve OpenAI key from 1Password             | unset                         |
| `PPA_OPENAI_API_KEY_OP_REF`         | 1Password reference for OpenAI key            | (required if using 1Password) |
| `PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE` | Path to 1Password SA token file               | (required if using 1Password) |

### Rebuild tuning

| Variable                      | Description                                        | Default   |
| ----------------------------- | -------------------------------------------------- | --------- |
| `PPA_REBUILD_WORKERS`         | Parallel scan workers                              | CPU count |
| `PPA_REBUILD_BATCH_SIZE`      | Rows per materialization batch                     | `1000`    |
| `PPA_REBUILD_COMMIT_INTERVAL` | Cards per load commit                              | `5000`    |
| `PPA_REBUILD_EXECUTOR`        | `serial`, `thread`, or `process`                   | `thread`  |
| `PPA_FORCE_FULL_REBUILD`      | Force full rebuild even if incremental is possible | unset     |
| `PPA_FORBID_REBUILD`          | Block all rebuilds (production safety)             | unset     |

### Feature flags

| Variable                 | Description                                                     | Default  |
| ------------------------ | --------------------------------------------------------------- | -------- |
| `PPA_SEED_LINKS_ENABLED` | Enable the seed-links enrichment subsystem (8 tables, 11 tools) | disabled |
| `PPA_SEED_FROZEN`        | Skip vault scan when manifest hash matches                      | unset    |

## MCP Tools

### Read & Search

| Tool                         | Description                                   | Profiles        |
| ---------------------------- | --------------------------------------------- | --------------- |
| `archive_search`             | Full-text lexical search                      | all             |
| `archive_read`               | Read note by path or UID                      | all             |
| `archive_read_many`          | Batch read by paths/UIDs                      | full, read-only |
| `archive_query`              | Structured query by type, source, people, org | all             |
| `archive_vector_search`      | Semantic search over embedded chunks          | full, read-only |
| `archive_hybrid_search`      | Combined lexical + semantic + graph retrieval | full, read-only |
| `archive_search_json`        | Lexical search as JSON                        | all             |
| `archive_hybrid_search_json` | Hybrid search as JSON                         | full, read-only |

### Graph & People

| Tool               | Description                               |
| ------------------ | ----------------------------------------- |
| `archive_graph`    | Linked notes from materialized edge graph |
| `archive_person`   | Person profile + linked notes             |
| `archive_timeline` | Notes in date range                       |

### Status & Admin

| Tool                         | Description                            |
| ---------------------------- | -------------------------------------- |
| `archive_stats`              | Vault health metrics                   |
| `archive_validate`           | Schema + provenance validation         |
| `archive_duplicates`         | Pending dedup candidates               |
| `archive_index_status`       | Derived index metadata                 |
| `archive_embedding_status`   | Chunk coverage and pending embeddings  |
| `archive_embed_pending`      | Generate embeddings for pending chunks |
| `archive_bootstrap_postgres` | Bootstrap Postgres schema              |
| `archive_rebuild_indexes`    | Rebuild derived index from vault       |

## Agent Query Guidance

Use tools in this order:

1. `archive_read` for exact UID/path reads.
2. `archive_query` for type/source/people filters.
3. `archive_search` for keyword and phrase recall.
4. `archive_vector_search` for semantic recall when the question is vague.
5. `archive_hybrid_search` for exact anchors + semantic + graph expansion.
6. `archive_graph` to expand neighboring evidence.
7. Read the canonical card before giving a final answer.

Search hits are retrieval aids, not canonical truth. The vault is truth.

## Testing

```bash
.venv/bin/python -m pytest tests/
```

275 tests covering:

- Schema library and vault I/O (64 tests)
- Source adapters for all 18 data sources (121 tests)
- Vault doctor operations (7 tests)
- MCP server behavior and command dispatch (28 tests)
- Live Postgres integration: rebuild, search, vector search, hybrid ranking, graph expansion
- Script behavior for backup, init-vault, post-import, spool imports

Live retrieval tests start a disposable pgvector Docker container automatically. If Docker is unavailable, they skip cleanly.

## Production Safety

Production MCP launchers should set `PPA_FORBID_REBUILD=1` to block rebuild operations and prevent accidental data loss on the production database.

**Never run** `rebuild-indexes` or `bootstrap-postgres` against a production instance. The index should be built locally and transferred via dump-restore. If a rebuild is ever needed, build locally, dump, and restore.

**Verify via psql, not Python.** For production verification, use direct SSH + psql commands to avoid triggering vault scans.

## Scripts

Shell scripts (`scripts/ppa-*.sh`) for vault operations:

- `ppa-init-vault.sh` — scaffold a new vault
- `ppa-backup.sh` / `ppa-backup-encrypt.sh` / `ppa-backup-restore.sh` — encrypted vault backup and restore
- `ppa-backup-upload.sh` / `ppa-backup-upload-icloud.sh` / `ppa-backup-upload-gdrive.sh` — backup upload to cloud storage
- `ppa-provision-storage.sh` / `ppa-unlock.sh` / `ppa-mount.sh` / `ppa-lock.sh` — encrypted volume management
- `ppa-post-import.sh` — run sync + doctor after vault imports
- `ppa-pg-backup.sh` — stream Postgres dump to backup location
- `ppa-tunnel.sh` — SSH tunnel to remote Postgres instance

Python scripts (`scripts/ppa-*.py`) for source extraction and import:

- Gmail, Calendar, iMessage, Photos, GitHub extraction and parallel import
- Google workspace backfill and account scope migration

## Documentation

See `docs/` for architecture, playbook, card type contracts, retrieval contracts, security model, and operational runbooks.

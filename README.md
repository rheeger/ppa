# PPA — Personal Private Archives

**A local archive and search stack for the data you already have in other products** — mail, chat, calendar, photos, health and finance exports, code history, and more. Import into a canonical markdown **vault**, index in **Postgres + pgvector** (lexical + semantic + hybrid retrieval, plus a typed graph), and query from the CLI or over **MCP** for editor and agent workflows.

Your life is split across apps with different UIs and exports. There is no vendor that gives you one definitive copy of “everything.” PPA is the **one place you control**: files on disk you can back up, a database you can rebuild from those files, and tools that search across sources without uploading your corpus to someone else’s AI product.

| Without                        | With                                |
| ------------------------------ | ----------------------------------- |
| Search each product separately | One index over what you’ve imported |
| Model only sees what you paste | MCP tools over your own material    |
| No single portable archive     | Vault + regenerable index           |

Typical uses: recall and research across sources, agents in-editor against your corpus (**[archive_docs/AGENT_USAGE.md](archive_docs/AGENT_USAGE.md)**), long-term retention with provenance on every card.

### Similar projects

If you only need to **index folders of markdown you already have** (notes, docs, meeting files) with local hybrid search and MCP, **[qmd](https://github.com/tobi/qmd)** is built for that: SQLite + BM25 + vectors + reranking, all local. **PPA** adds **multi-service ingest** (Gmail, iMessage, GitHub, health exports, …), a **vault-first** contract, Postgres, and graph/people/timeline tooling. Use whichever matches where your data actually lives.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Sources (APIs, exports, local DBs)  →  vault (markdown)     │
└───────────────────────────────┬─────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────┐
│  Postgres + pgvector: cards, chunks, edges, embeddings       │
└───────────────────────────────┬─────────────────────────────┘
                                ▼
┌─────────────────────────────────────────────────────────────┐
│  MCP server (stdio): search, read, query, graph, admin       │
└─────────────────────────────────────────────────────────────┘
```

| Layer     | Role                                                       |
| --------- | ---------------------------------------------------------- |
| **Vault** | Markdown, YAML frontmatter, provenance — what you back up  |
| **Index** | Derived; rebuild from the vault anytime                    |
| **MCP**   | Tools for humans and agents; profiles gate destructive ops |

---

## Requirements

- Python **3.10+**
- Postgres with **pgvector** (repo ships `Makefile` + Docker for a sane local loop)
- Embeddings: **`hash`** when you’re iterating; **`openai`** (or API-compatible) when you want vectors that mean something

---

## Quick start

**Install**

```bash
git clone https://github.com/rheeger/ppa.git
cd ppa
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

**Postgres + index + embeddings** (local Docker via `Makefile`)

```bash
cp .env.pgvector.example .env.pgvector
make pg-up
make bootstrap-postgres
make rebuild-indexes
make embed-pending
```

**Run MCP** (adjust paths)

```bash
./archive_scripts/run-local-seed-mcp.sh
# Remote index: ./archive_scripts/ppa-tunnel.sh & ./archive_scripts/run-arnold-mcp.sh
# Or portable: ppa serve --tunnel user@host  (tunnel is a child of the MCP process)
```

**Quick setup for any MCP client**

1. `pip install -e .`
2. Export `PPA_INDEX_DSN`, `PPA_PATH`, `PPA_INDEX_SCHEMA` (and embedding vars as needed).
3. `ppa mcp-config` → paste into `~/.cursor/mcp.json` (or Claude Desktop / Codex). Add API keys only in the client `env` block — they are never printed by `mcp-config`.

Template: **[archive_docs/examples/ppa.mcp-example.json](archive_docs/examples/ppa.mcp-example.json)** · details: **[archive_docs/MCP_SETUP.md](archive_docs/MCP_SETUP.md)**.

**CLI entrypoint** (what the scripts wrap)

```bash
python -m archive_cli serve
```

Other subcommands (`rebuild-indexes`, `embed-pending`, migrations, …): **[archive_docs/PPA_RUNTIME_CONTRACT.md](archive_docs/PPA_RUNTIME_CONTRACT.md)**.

---

## MCP server

PPA exposes a **stdio** MCP server (same pattern as [qmd’s MCP](https://github.com/tobi/qmd#mcp-server): subprocess per client unless you tunnel to a remote index).

**Tools exposed** (names vary slightly by `PPA_MCP_TOOL_PROFILE`; seed-link tools are optional — see runtime contract):

| Area               | Tools                                                                                                                                                        |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Search & read**  | `archive_search`, `archive_search_json`, `archive_vector_search`, `archive_hybrid_search`, `archive_hybrid_search_json`, `archive_read`, `archive_read_many` |
| **Structured**     | `archive_query`                                                                                                                                              |
| **Graph & people** | `archive_graph`, `archive_person`, `archive_timeline`                                                                                                        |
| **Explain**        | `archive_retrieval_explain`, `archive_retrieval_explain_json`                                                                                                |
| **Status**         | `archive_stats`, `archive_validate`, `archive_duplicates`, `archive_index_status`, `archive_embedding_status`, `archive_embed_pending`, …                    |
| **Admin**          | `archive_rebuild_indexes`, `archive_bootstrap_postgres`, projections — **restrict in production**                                                            |

Agent prompts and call order: **[archive_docs/AGENT_USAGE.md](archive_docs/AGENT_USAGE.md)**.

**Portable config** (after `pip install -e .`, `ppa` on `PATH`):

```json
{
  "mcpServers": {
    "ppa": {
      "command": "ppa",
      "args": ["serve"],
      "env": {
        "PPA_INDEX_DSN": "postgresql://archive:archive@127.0.0.1:5432/archive",
        "PPA_INDEX_SCHEMA": "archive_seed",
        "PPA_PATH": "/path/to/vault",
        "PPA_EMBEDDING_PROVIDER": "openai",
        "PPA_EMBEDDING_MODEL": "text-embedding-3-small"
      }
    }
  }
}
```

Remote Postgres via SSH: `"args": ["serve", "--tunnel", "user@host"]` and point `PPA_INDEX_DSN` at `127.0.0.1:5433` (or `PPA_TUNNEL_PORT`). **`archive_scripts/run-local-seed-mcp.sh`** / **`archive_scripts/run-arnold-mcp.sh`** remain supported convenience wrappers.

**Claude Desktop** — same `command` / `args` shape under your `claude_desktop_config.json` MCP block.

---

## Packages

Single editable install; import names are legacy until someone finishes the rename:

| Module           | Role                                     |
| ---------------- | ---------------------------------------- |
| `archive_cli`    | MCP server, index, retrieval, embeddings |
| `archive_vault`  | Schema, vault I/O, provenance            |
| `archive_sync`   | Sources → vault cards                    |
| `archive_doctor` | Validate, dedupe, stats                  |

---

## Connections (17 adapters)

If it’s listed here, there’s code that already knows how to turn it into cards. Canonical IDs: **`archive_sync/adapter_contracts.py`**. Messy details: **`archive_sync/adapters/`**.

### Communication & meetings

| Service                  | Ingests                   | Connection                                           |
| ------------------------ | ------------------------- | ---------------------------------------------------- |
| **Gmail**                | Threads, messages         | Google API; incremental                              |
| **Gmail correspondents** | People graph from mail    | From Gmail                                           |
| **iMessage**             | Chats, messages           | Local `chat.db`                                      |
| **Beeper**               | Threads, DMs, attachments | Local BeeperTexts SQLite (`index.db`, macOS default) |
| **Otter.ai**             | Transcripts               | Otter HTTP API                                       |

### Calendar & contacts

| Service             | Ingests | Connection |
| ------------------- | ------- | ---------- |
| **Google Calendar** | Events  | API        |
| **Google Contacts** | People  | People API |

### People & directories

| Service         | Ingests             | Connection                            |
| --------------- | ------------------- | ------------------------------------- |
| **LinkedIn**    | Network CSV         | Export file                           |
| **Notion**      | People / staff rows | CSV (`notion-people`, `notion-staff`) |
| **Seed people** | Hand-curated        | Local seeds                           |

### Files, photos, code

| Service            | Ingests                              | Connection                                                           |
| ------------------ | ------------------------------------ | -------------------------------------------------------------------- |
| **File libraries** | Trees of files                       | Directory scan                                                       |
| **Apple Photos**   | Assets, albums, faces/labels (macOS) | [osxphotos](https://github.com/RhetTbull/osxphotos) on local library |
| **GitHub**         | Commits, PRs, issues                 | GitHub API                                                           |

### Health & medical

| Service            | Ingests                                     | Connection                                                                                                      |
| ------------------ | ------------------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **Apple Health**   | Vitals, activity rollups                    | Health export XML                                                                                               |
| **Clinical / EHR** | Encounters, labs, meds, immunizations, docs | **`medical-records`**: FHIR JSON, CCD/XML, PDFs, Epic EHI TSV, etc. (whatever your provider actually gives you) |

### Finance

| Service     | Ingests      | Connection                                           |
| ----------- | ------------ | ---------------------------------------------------- |
| **Copilot** | Transactions | CSV (default `~/Downloads/copilot-transactions.csv`) |

---

## Config (minimum)

Prefix everything **`PPA_*`**. The exhaustive list is **[archive_docs/PPA_RUNTIME_CONTRACT.md](archive_docs/PPA_RUNTIME_CONTRACT.md)** §2 — this is the “just let me run it” subset:

| Var                      | Purpose                                                       |
| ------------------------ | ------------------------------------------------------------- |
| `PPA_PATH`               | Vault root (default `~/Archive/vault`; override in `ppa.yml`) |
| `PPA_INDEX_DSN`          | Postgres DSN (required)                                       |
| `PPA_INDEX_SCHEMA`       | Schema (default `ppa`)                                        |
| `PPA_MCP_TOOL_PROFILE`   | `full` / `read-only` / `remote-read` / `admin-only`           |
| `PPA_EMBEDDING_PROVIDER` | `hash` or `openai`                                            |

Optional: **`PPA_CONFIG_PATH`** forces a specific `ppa.yml`.

---

## Production

- **`PPA_FORBID_REBUILD=1`** on anything facing a real database. Cheap insurance.
- Treat `rebuild-indexes` / `bootstrap-postgres` on prod like `rm -rf` — default stance is local build → dump → restore (or your written playbook).
- Sanity checks: **`psql` over SSH**. Not “run a random Python command and accidentally walk the vault.”

More: **[archive_docs/SECURITY_MODEL.md](archive_docs/SECURITY_MODEL.md)**, **[archive_docs/PPA_BACKUP_AND_RESTORE.md](archive_docs/PPA_BACKUP_AND_RESTORE.md)**, **`archive_docs/runbooks/`**.

---

## Scripts

- **`scripts/ppa-*.sh`** — vault init, encrypted backup/restore, volumes, post-import, pg dump, tunnels
- **`scripts/ppa-*.py`** — Gmail / Calendar / iMessage / Photos / GitHub extract + import glue

---

## Tests

```bash
.venv/bin/python -m pytest archive_tests/
```

~275 tests. Integration kicks up Dockerized pgvector when it can; otherwise it skips without drama.

---

## Docs

| Doc                                                                          | About                     |
| ---------------------------------------------------------------------------- | ------------------------- |
| [archive_docs/ARCHITECTURE.md](archive_docs/ARCHITECTURE.md)                 | System layout             |
| [archive_docs/INDEXING.md](archive_docs/INDEXING.md)                         | Index pipeline            |
| [archive_docs/AGENT_USAGE.md](archive_docs/AGENT_USAGE.md)                   | Agent / tool usage        |
| [archive_docs/MCP_SETUP.md](archive_docs/MCP_SETUP.md)                       | MCP client config, tunnel |
| [archive_docs/PPA_RUNTIME_CONTRACT.md](archive_docs/PPA_RUNTIME_CONTRACT.md) | CLI, env (frozen)         |
| [archive_docs/PLAYBOOK.md](archive_docs/PLAYBOOK.md)                         | Ops                       |
| [archive_docs/CARD_TYPE_CONTRACTS.md](archive_docs/CARD_TYPE_CONTRACTS.md)   | Card types                |
| [archive_docs/RETRIEVAL_CONTRACT.md](archive_docs/RETRIEVAL_CONTRACT.md)     | Retrieval                 |
| [archive_docs/vision/](archive_docs/vision/)                                 | Long-form roadmap (v2–v4) |

---

## Contributing

PRs genuinely welcome. Run **`pytest`** when you touch index / adapters / MCP surfaces. If CLI, env, or MCP semantics move, **update** **[archive_docs/PPA_RUNTIME_CONTRACT.md](archive_docs/PPA_RUNTIME_CONTRACT.md)** — that file is the handshake with anyone automating this.

# PPA — Personal Private Archives

**A local archive and search stack for the data you already have in other products** — mail, chat, calendar, photos, health and finance exports, code history, and more. Import into a canonical markdown **vault**, index in **Postgres + pgvector** (lexical + semantic + hybrid retrieval, plus a typed graph), and query from the CLI or over **MCP** for editor and agent workflows.

Your life is split across apps with different UIs and exports. There is no vendor that gives you one definitive copy of “everything.” PPA is the **one place you control**: files on disk you can back up, a database you can rebuild from those files, and tools that search across sources without uploading your corpus to someone else’s AI product.

| Without | With |
| ------- | ---- |
| Search each product separately | One index over what you’ve imported |
| Model only sees what you paste | MCP tools over your own material |
| No single portable archive | Vault + regenerable index |

Typical uses: recall and research across sources, agents in-editor against your corpus (**[docs/AGENT_USAGE.md](docs/AGENT_USAGE.md)**), long-term retention with provenance on every card.

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

| Layer | Role |
| ----- | ---- |
| **Vault** | Markdown, YAML frontmatter, provenance — what you back up |
| **Index** | Derived; rebuild from the vault anytime |
| **MCP** | Tools for humans and agents; profiles gate destructive ops |

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
./run-local-seed-mcp.sh
# Remote index: ./scripts/ppa-tunnel.sh & ./run-arnold-mcp.sh
```

**CLI entrypoint** (what the scripts wrap)

```bash
python -m archive_mcp serve
```

Other subcommands (`rebuild-indexes`, `embed-pending`, migrations, …): **[docs/PPA_RUNTIME_CONTRACT.md](docs/PPA_RUNTIME_CONTRACT.md)**.

---

## MCP server

PPA exposes a **stdio** MCP server (same pattern as [qmd’s MCP](https://github.com/tobi/qmd#mcp-server): subprocess per client unless you tunnel to a remote index).

**Tools exposed** (names vary slightly by `PPA_MCP_TOOL_PROFILE`; seed-link tools are optional — see runtime contract):

| Area | Tools |
| ---- | ----- |
| **Search & read** | `archive_search`, `archive_search_json`, `archive_vector_search`, `archive_hybrid_search`, `archive_hybrid_search_json`, `archive_read`, `archive_read_many` |
| **Structured** | `archive_query` |
| **Graph & people** | `archive_graph`, `archive_person`, `archive_timeline` |
| **Explain** | `archive_retrieval_explain`, `archive_retrieval_explain_json` |
| **Status** | `archive_stats`, `archive_validate`, `archive_duplicates`, `archive_index_status`, `archive_embedding_status`, `archive_embed_pending`, … |
| **Admin** | `archive_rebuild_indexes`, `archive_bootstrap_postgres`, projections — **restrict in production** |

Agent prompts and call order: **[docs/AGENT_USAGE.md](docs/AGENT_USAGE.md)**.

**Cursor** — `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "archive-local": {
      "command": "/absolute/path/to/ppa/run-local-seed-mcp.sh"
    },
    "archive-remote": {
      "command": "/absolute/path/to/ppa/run-arnold-mcp.sh"
    }
  }
}
```

**Claude Desktop** — same `command` / `args` shape under your `claude_desktop_config.json` MCP block.

---

## Packages

Single editable install; import names are legacy until someone finishes the rename:

| Module | Role |
| ------ | ---- |
| `archive_mcp` | MCP server, index, retrieval, embeddings |
| `hfa` | Schema, vault I/O, provenance |
| `archive_sync` | Sources → vault cards |
| `archive_doctor` | Validate, dedupe, stats |

---

## Connections (17 adapters)

If it’s listed here, there’s code that already knows how to turn it into cards. Canonical IDs: **`archive_sync/adapter_contracts.py`**. Messy details: **`archive_sync/adapters/`**.

### Communication & meetings

| Service | Ingests | Connection |
| ------- | ------- | ---------- |
| **Gmail** | Threads, messages | Google API; incremental |
| **Gmail correspondents** | People graph from mail | From Gmail |
| **iMessage** | Chats, messages | Local `chat.db` |
| **Beeper** | Threads, DMs, attachments | Local BeeperTexts SQLite (`index.db`, macOS default) |
| **Otter.ai** | Transcripts | Otter HTTP API |

### Calendar & contacts

| Service | Ingests | Connection |
| ------- | ------- | ---------- |
| **Google Calendar** | Events | API |
| **Google Contacts** | People | People API |

### People & directories

| Service | Ingests | Connection |
| ------- | ------- | ---------- |
| **LinkedIn** | Network CSV | Export file |
| **Notion** | People / staff rows | CSV (`notion-people`, `notion-staff`) |
| **Seed people** | Hand-curated | Local seeds |

### Files, photos, code

| Service | Ingests | Connection |
| ------- | ------- | ---------- |
| **File libraries** | Trees of files | Directory scan |
| **Apple Photos** | Assets, albums, faces/labels (macOS) | [osxphotos](https://github.com/RhetTbull/osxphotos) on local library |
| **GitHub** | Commits, PRs, issues | GitHub API |

### Health & medical

| Service | Ingests | Connection |
| ------- | ------- | ---------- |
| **Apple Health** | Vitals, activity rollups | Health export XML |
| **Clinical / EHR** | Encounters, labs, meds, immunizations, docs | **`medical-records`**: FHIR JSON, CCD/XML, PDFs, Epic EHI TSV, etc. (whatever your provider actually gives you) |

### Finance

| Service | Ingests | Connection |
| ------- | ------- | ---------- |
| **Copilot** | Transactions | CSV (default `~/Downloads/copilot-transactions.csv`) |

---

## Config (minimum)

Prefix everything **`PPA_*`**. The exhaustive list is **[docs/PPA_RUNTIME_CONTRACT.md](docs/PPA_RUNTIME_CONTRACT.md)** §2 — this is the “just let me run it” subset:

| Var | Purpose |
| --- | ------- |
| `PPA_PATH` | Vault root (default `~/Archive/vault`; override in `ppa.yml`) |
| `PPA_INDEX_DSN` | Postgres DSN (required) |
| `PPA_INDEX_SCHEMA` | Schema (default `archive_mcp`) |
| `PPA_MCP_TOOL_PROFILE` | `full` / `read-only` / `remote-read` / `admin-only` |
| `PPA_EMBEDDING_PROVIDER` | `hash` or `openai` |

Optional: **`PPA_CONFIG_PATH`** forces a specific `ppa.yml`.

---

## Production

- **`PPA_FORBID_REBUILD=1`** on anything facing a real database. Cheap insurance.
- Treat `rebuild-indexes` / `bootstrap-postgres` on prod like `rm -rf` — default stance is local build → dump → restore (or your written playbook).
- Sanity checks: **`psql` over SSH**. Not “run a random Python command and accidentally walk the vault.”

More: **[docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md)**, **[docs/PPA_BACKUP_AND_RESTORE.md](docs/PPA_BACKUP_AND_RESTORE.md)**, **`docs/runbooks/`**.

---

## Scripts

- **`scripts/ppa-*.sh`** — vault init, encrypted backup/restore, volumes, post-import, pg dump, tunnels  
- **`scripts/ppa-*.py`** — Gmail / Calendar / iMessage / Photos / GitHub extract + import glue

---

## Tests

```bash
.venv/bin/python -m pytest tests/
```

~275 tests. Integration kicks up Dockerized pgvector when it can; otherwise it skips without drama.

---

## Docs

| Doc | About |
| --- | ----- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System layout |
| [docs/INDEXING.md](docs/INDEXING.md) | Index pipeline |
| [docs/AGENT_USAGE.md](docs/AGENT_USAGE.md) | Agent / tool usage |
| [docs/PPA_RUNTIME_CONTRACT.md](docs/PPA_RUNTIME_CONTRACT.md) | CLI, env (frozen) |
| [docs/PLAYBOOK.md](docs/PLAYBOOK.md) | Ops |
| [docs/CARD_TYPE_CONTRACTS.md](docs/CARD_TYPE_CONTRACTS.md) | Card types |
| [docs/RETRIEVAL_CONTRACT.md](docs/RETRIEVAL_CONTRACT.md) | Retrieval |

---

## Contributing

PRs genuinely welcome. Run **`pytest`** when you touch index / adapters / MCP surfaces. If CLI, env, or MCP semantics move, **update** **[docs/PPA_RUNTIME_CONTRACT.md](docs/PPA_RUNTIME_CONTRACT.md)** — that file is the handshake with anyone automating this.

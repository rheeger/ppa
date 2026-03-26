# PPA — Personal Private Archives

Mail, messages, calendar, photos, health portals, bank exports, git history, meeting transcripts — each one sits in a different product with its own UI, export format, and retention rules. There is **no single system** that is “your archive.” If you need something that might be in any of those places, you open several apps or dig through old downloads. Nothing gives you one definitive view.

PPA pulls from the services and files you choose into **one vault of markdown** (with metadata so you know the source), builds a **search index** in Postgres with pgvector, and exposes an **MCP server** so editors and agents can query that index. **The vault is what you keep.** The database is derived and can be rebuilt from the vault. A running deployment is one **instance** (for example, one person or one household).

| Without | With |
| ------- | ---- |
| Search each product separately | One search stack over everything you imported |
| Model only sees what you paste | MCP tools over your own indexed material |
| No portable copy of “all of it” | Files you own + DB you can regenerate |

Typical uses: piecing together who said what and when, using agents in-editor against your corpus (**[docs/AGENT_USAGE.md](docs/AGENT_USAGE.md)**), or keeping a long-term record where each item still traces back to its origin.

---

## How it works

```
Markdown vault  →  Postgres + pgvector  →  MCP server (stdio)
```

| Layer | Role |
| ----- | ---- |
| **Vault** | Markdown, YAML frontmatter, provenance — the thing you back up |
| **Index** | Cards, edges, chunks, embeddings. Derived. Rebuildable. |
| **MCP** | Search, read, query, graph, admin — gated by tool profile |

MCP clients come and go. The vault is forever.

---

## Requirements

- Python **3.10+**
- Postgres with **pgvector** (repo ships `Makefile` + Docker for a sane local loop)
- Embeddings: **`hash`** when you’re iterating; **`openai`** (or API-compatible) when you want vectors that mean something

---

## Quick start

```bash
git clone https://github.com/rheeger/ppa.git
cd ppa
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e .
```

DB up, schema, index from vault, then embeddings:

```bash
cp .env.pgvector.example .env.pgvector
make pg-up
make bootstrap-postgres
make rebuild-indexes
make embed-pending
```

MCP (adjust paths):

```bash
./run-local-seed-mcp.sh
# Remote: ./scripts/ppa-tunnel.sh & ./run-arnold-mcp.sh
```

Underlying entrypoint:

```bash
python -m archive_mcp serve
```

Everything else the binary can do lives in **[docs/PPA_RUNTIME_CONTRACT.md](docs/PPA_RUNTIME_CONTRACT.md)**.

---

## MCP host config

`~/.cursor/mcp.json` (tack on more servers the same way):

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

## MCP tools (shape)

Lexical + structured query + vector + hybrid, graph/peek at neighbors, people, timelines, then the admin/rebuild knobs (behind profiles). **Exact names + “don’t shoot yourself in prod”:** runtime contract + **[docs/AGENT_USAGE.md](docs/AGENT_USAGE.md)**.

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

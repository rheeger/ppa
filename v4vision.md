# PPA v4 Vision — Native App, Hosted Service, Rust Engine

---

## The Core Thesis

After v3, PPA is a **proven multi-user personal knowledge system**. Anyone comfortable with Docker and a terminal can set up their own private archive, connect their data sources, and make their AI tools smarter about their life. The engine works, the extractors work, the knowledge cache works, the CLI is polished.

v4 makes PPA **accessible to everyone**. Three pillars:

1. **Native Mac app** — No Docker, no terminal, no API project setup. Install, connect Gmail with one click, and your AI tools know you. The archive runs invisibly on your Mac.

2. **PPA Service** — A hosted service layer that removes the operational burdens self-hosters solve manually: OAuth proxy (no Google Cloud project needed), signed connector updates (extractors stay current automatically), and compiled app distribution (notarized `.dmg` with auto-updates).

3. **Rust engine rewrite** — The Python engine is proven but not built for scale. A Rust rewrite of the core engine — vault scanning, materialization, FTS, embedding pipeline, MCP server — delivers the performance needed for archives with millions of cards and positions PPA as infrastructure that can power multiple archives on a single machine.

The v4 user is anyone who uses AI tools and has a decade of email, messages, and digital life. They don't know what Docker or MCP is. They install an app, connect their accounts, and their AI assistant suddenly knows their restaurant preferences, travel history, and who their VIP contacts are.

**Still fully open source.** The engine, all connectors, the Mac app source — everything remains MIT-licensed. The paid service removes operational friction. No feature gates. No usage limits. The subscription pays for convenience, not for the software.

---

## Principles

v2 principles 1–9 and v3 principles 10–14 remain in force. v4 adds:

15. **Rust at the core, Python at the edges.** The engine — vault scanning, materialization, indexing, FTS, embedding pipeline, MCP server — is rewritten in Rust for performance and reliability. Extractors, knowledge domain definitions, and adapter logic remain Python — they change frequently, benefit from rapid iteration, and aren't performance-critical. The Rust engine exposes a Python FFI layer (via PyO3) so existing extractors and adapters work without modification.

16. **The app is invisible when working.** PPA is infrastructure, not a destination. The UI exists for setup, status, and troubleshooting. The product experience happens inside whatever AI tool the user already uses — Cursor, Claude Desktop, ChatGPT, or voice assistants.

17. **No kill switch.** An expired subscription never disables the software. The user's archive continues to work — vault, index, extractors, knowledge, MCP. The subscription provides ongoing service (OAuth proxy, connector updates, app distribution). If the subscription lapses, the service stops but the archive doesn't.

18. **Connectors are the recurring value.** Email template formats change, new services emerge, OAuth scopes evolve. The signed connector update feed is what keeps archives current. This is the subscription value — not access to features, but maintenance of the living system.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                      PPA Core Engine (Rust)                          │
│                                                                      │
│  vault scanner  ·  materializer  ·  FTS engine   ·  MCP server      │
│  chunk builder  ·  embedding pipeline             ·  maintain cycle  │
│  edge materializer  ·  quality scorer             ·  index store     │
│                                                                      │
│                     DatabaseAdapter (trait)                           │
│                     ┌──────────────────────┐                        │
│                     │   PostgresAdapter    │                        │
│                     │   (embedded or remote)│                        │
│                     └──────────────────────┘                        │
│                                                                      │
│                     PyO3 FFI Layer                                   │
│                     ┌──────────────────────────────────┐            │
│                     │  Python extractors + adapters    │            │
│                     │  Python knowledge definitions    │            │
│                     │  Python entity resolution        │            │
│                     └──────────────────────────────────┘            │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
             ┌─────────────────┼──────────────────┐
             ▼                                     ▼
  ┌──────────────────────┐           ┌──────────────────────┐
  │   Mac App Shell      │           │   VM/Linux Shell     │
  │   (Tauri / Rust)     │           │   (Docker + CLI)     │
  │                      │           │                      │
  │ Embedded Postgres    │           │ Docker Postgres      │
  │ Native UI (web view) │           │ LUKS encrypted vol   │
  │ Keychain + Touch ID  │           │ systemd services     │
  │ Menu bar agent       │           │ SSH/HTTPS MCP        │
  │ Auto-updates         │           │ ppa setup wizard     │
  │ MCP auto-registration│           │ cron maintain        │
  └──────────────────────┘           └──────────────────────┘
             │                                     │
             └──────────┬──── optional ────────────┘
                        ▼
           ┌────────────────────────┐
           │   PPA Service Layer   │
           │   (ppa.dev / GCP)     │
           │                       │
           │  OAuth proxy          │
           │  Connector update feed│
           │  App distribution     │
           │  Billing (Stripe)     │
           │  Telemetry (opt-in)   │
           └────────────────────────┘
```

---

## Business Model

### Fully open source (MIT)

| Component                             | Repository                  |
| ------------------------------------- | --------------------------- |
| PPA engine (Rust core + Python edges) | `ppa-dev/ppa`               |
| All connectors and extractors         | `ppa-dev/ppa` (extractors/) |
| Mac app source code (Tauri)           | `ppa-dev/ppa` (app/)        |
| VM/Linux Docker setup                 | `ppa-dev/ppa` (deploy/)     |
| Knowledge domain definitions          | `ppa-dev/ppa` (knowledge/)  |

### Paid service — one price, one decision

|                  | Free (self-hosted)                                                     | PPA Service ($12/month)                             |
| ---------------- | ---------------------------------------------------------------------- | --------------------------------------------------- |
| **Software**     | Full engine, all connectors, all knowledge domains — build from source | Same software, pre-built                            |
| **Database**     | You configure Postgres (Docker or existing)                            | Invisible. Embedded Postgres managed by the app.    |
| **OAuth**        | Register your own Google Cloud project                                 | Click "Connect Gmail." Done.                        |
| **Updates**      | `git pull && cargo build` (or pull Docker image)                       | Auto-updates to app and connectors via signed feed  |
| **Distribution** | Build from source                                                      | Signed, notarized `.dmg` with auto-updater          |
| **Telemetry**    | None                                                                   | Opt-in anonymous analytics (consented during setup) |
| **Support**      | GitHub Issues                                                          | Email                                               |

**No feature gates. No usage limits. No artificial restrictions.**

### What the service provides

**1. OAuth Proxy** — A verified Google OAuth application. Users click "Connect Gmail" → Google consent screen → done. The service exchanges the auth code for tokens, encrypts them with the device's public key (X25519 + XChaCha20-Poly1305), and returns the encrypted blob. The service never uses the tokens and doesn't store them after delivery. Stateless within 5 minutes of each flow.

**2. Signed Connector Update Feed** — When email template formats change (DoorDash redesigns receipts, Amazon restructures confirmations), the updated extractor is signed with an Ed25519 key and published to `ppa.dev/connectors/manifest.json`. Paid users' instances pull and apply updates automatically. Free users pull from the git repo.

**3. Compiled, Notarized Mac App** — A signed, notarized `.dmg` with embedded Postgres, Rust engine, bundled Python runtime, and auto-updates.

### What the service does NOT provide

| Not Provided                    | Why                                                           |
| ------------------------------- | ------------------------------------------------------------- |
| Hosted embeddings / LLM compute | BYOK. Users bring their own OpenAI key or run Ollama locally. |
| Data storage or sync            | The user's data stays on their hardware.                      |
| Feature-gated tiers             | One price. Every feature is available to everyone.            |
| Usage limits                    | No card caps, no query throttling, no source limits.          |

---

## Phase 16: Rust Engine — Core Rewrite

**What it is:** A ground-up rewrite of the PPA engine's performance-critical paths in Rust, with Python extractors and adapters running via PyO3 FFI. The Rust engine replaces the Python engine for vault scanning, materialization, indexing, FTS, embedding orchestration, and MCP serving.

**Why it exists:** The Python engine is correct but not fast. At 1.85M vault files, a cold vault scan takes ~42 minutes. A full rebuild takes hours. Incremental operations are fast enough for one user but won't scale if PPA needs to support larger archives, faster first-run experiences, or multiple archives on one machine. Rust eliminates the Python overhead for I/O-heavy, CPU-bound operations while keeping Python for the frequently-changing, non-performance-critical logic (extractors, adapters, knowledge definitions).

**Why now (v4, not v3):** v3 proves multi-user with the Python engine. v4 optimizes. Rewriting before validating with real users risks building the wrong thing fast. After v3, the architecture is stable, the interfaces are proven, the test suite is comprehensive (500+ tests), and the team has Rust experience from the Tauri shell.

### What moves to Rust

| Component                  | Current (Python)                                                                | Rust Rewrite                                          | Why                                                                                                                                                                   |
| -------------------------- | ------------------------------------------------------------------------------- | ----------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Vault scanner**          | `scanner.py` — walks vault, reads frontmatter, computes hashes, builds manifest | `archive_crate/src/scanner.rs`                        | I/O-bound. Python's GIL limits parallel file reads. Rust with `rayon` can saturate SSD bandwidth. Expected 10-20x speedup on cold scan.                               |
| **Materializer**           | `materializer.py` — transforms parsed cards into database rows                  | `archive_crate/src/materializer.rs`                   | CPU-bound string processing, quality scoring, search_text construction. Rust eliminates interpreter overhead.                                                         |
| **Loader**                 | `loader.py` — batch inserts into Postgres, manages transactions, checkpointing  | `archive_crate/src/loader.rs`                         | Orchestration + bulk I/O. Rust's `tokio-postgres` with pipelining can saturate Postgres ingestion bandwidth.                                                          |
| **Index query**            | `index_query.py` — all search, temporal, graph queries                          | `archive_crate/src/query.rs`                          | Query construction and result processing. Moderate speedup but more importantly enables async query handling for concurrent MCP requests.                             |
| **FTS engine**             | Postgres GIN + `ts_vector` (unchanged)                                          | Same (Postgres FTS stays)                             | FTS is already in Postgres. Rust handles query construction and result processing, not the FTS computation itself.                                                    |
| **Chunk builder**          | `chunk_builders.py`, `chunking.py` — splits card text into embedding chunks     | `archive_crate/src/chunker.rs`                        | CPU-bound text processing. Parallelizable with `rayon`.                                                                                                               |
| **Embedding orchestrator** | `embedder.py` — manages embedding API calls, batching, rate limiting            | `archive_crate/src/embedder.rs`                       | Async I/O with `reqwest` + `tokio`. Better connection pooling, retry logic, and concurrency than Python's `asyncio`.                                                  |
| **MCP server**             | `server.py` — stdio MCP protocol handler                                        | `archive_crate/src/mcp.rs`                            | The MCP server is the hot path for user experience. Rust eliminates Python startup time (currently ~2-3s for interpreter + imports) and reduces per-request overhead. |
| **Database adapter**       | `db/postgres.py` — psycopg wrapper                                              | `archive_crate/src/db/postgres.rs` — `tokio-postgres` | Async connection pooling, pipelining, prepared statements.                                                                                                            |

### What stays Python

| Component                                       | Why Python                                                                                                                                                                     |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Extractors** (`archive_sync/extractors/*.py`) | Change frequently (template versioning), use regex heavily (Python regex is fine for this), community-contributed. Rewriting 20+ extractors in Rust is high cost, low benefit. |
| **Adapters** (`archive_sync/adapters/*.py`)     | API client code (Google, GitHub, Otter, etc.). Python has better library support for OAuth flows, API clients, and data format parsing.                                        |
| **Entity resolution** (`entity_resolution.py`)  | Complex string matching, fuzzy logic. Python's ecosystem (rapidfuzz, etc.) is mature. Not on the hot path.                                                                     |
| **Knowledge domain definitions**                | SQL query templates + computation logic. Changes frequently, not performance-critical.                                                                                         |
| **Enrichment pipeline**                         | LLM API calls. Performance is API-rate-limited, not CPU-limited.                                                                                                               |

### PyO3 FFI bridge

The Rust engine calls Python extractors and adapters via PyO3:

```rust
use pyo3::prelude::*;

fn run_extractor(
    py: Python<'_>,
    extractor_module: &str,
    email_frontmatter: &HashMap<String, String>,
    email_body: &str,
) -> PyResult<Vec<CardDict>> {
    let module = py.import(extractor_module)?;
    let extractor_class = module.getattr("Extractor")?;
    let instance = extractor_class.call0()?;
    let result = instance.call_method1(
        "extract",
        (frontmatter_to_py(py, email_frontmatter)?, email_body),
    )?;
    parse_card_dicts(result)
}
```

**Interface contract:** Python extractors implement the same `EmailExtractor` base class from Phase 2. The Rust engine calls `extract(frontmatter, body) -> list[dict]` and receives card dictionaries. The Rust side handles vault writes, UID generation, provenance, and materialization. Python only handles parsing.

**Python runtime embedding:** The Rust binary embeds a Python interpreter via PyO3. At build time, the Python version and site-packages location are configured. The embedded Python shares the same package environment as the CLI — `pip install -e .` works for both.

### Performance targets

| Operation                              | Python (current) | Rust (target)                    | Speedup |
| -------------------------------------- | ---------------- | -------------------------------- | ------- |
| Cold vault scan (1.85M files)          | ~42 min          | ~3-5 min                         | 8-15x   |
| Full rebuild (1.85M cards)             | ~2 hours         | ~15-25 min                       | 5-8x    |
| Incremental rebuild (1K changed cards) | ~30-60s          | ~3-5s                            | 10-15x  |
| Noop rebuild (no changes)              | ~8-10s           | <1s                              | 10x     |
| MCP server startup                     | ~2-3s            | <200ms                           | 10-15x  |
| Embedding orchestration (100K chunks)  | API-limited      | API-limited (better concurrency) | 1-2x    |

These targets are based on typical Rust-vs-Python speedups for I/O and CPU workloads. Actual numbers will be benchmarked against the Phase 0 test infrastructure.

### Crate structure

```
archive_crate/                        # Rust workspace
├── Cargo.toml
├── ppa-engine/                  # Main engine library
│   └── src/
│       ├── lib.rs
│       ├── scanner.rs           # Vault scanning + manifest
│       ├── materializer.rs      # Card → DB row transformation
│       ├── loader.rs            # Batch Postgres ingestion
│       ├── query.rs             # Search, temporal, graph queries
│       ├── chunker.rs           # Text → embedding chunks
│       ├── embedder.rs          # Embedding API orchestration
│       ├── mcp.rs               # MCP stdio server
│       ├── quality.rs           # Quality scoring
│       ├── features.rs          # activity_at extraction, etc.
│       ├── db/
│       │   ├── mod.rs           # DatabaseAdapter trait
│       │   └── postgres.rs      # tokio-postgres implementation
│       ├── vault/
│       │   ├── mod.rs           # Vault I/O
│       │   ├── frontmatter.rs   # YAML frontmatter parsing
│       │   └── card.rs          # Card data structures
│       └── config.rs            # Configuration (ppa.yml, env vars)
├── ppa-cli/                     # CLI binary
│   └── src/
│       └── main.rs              # ppa setup, serve, maintain, etc.
├── ppa-python/                  # PyO3 bridge
│   └── src/
│       └── lib.rs               # Python ↔ Rust FFI for extractors
└── ppa-app/                     # Tauri Mac app (from v4 Phase 18)
    └── src-tauri/
        └── src/
            ├── main.rs
            ├── postgres.rs      # Embedded Postgres lifecycle
            └── keychain.rs      # macOS Keychain integration
```

### Migration strategy

The rewrite is **incremental, not big-bang**. Each module is rewritten and tested independently:

1. **Phase 16a — Scaffold + vault scanner.** Set up the Rust workspace, PyO3 bridge, and build pipeline. Rewrite the vault scanner first — it's the most I/O-bound component and the easiest to test (input: directory of files, output: manifest of cards). Benchmark against the Python scanner on the 5% slice.

2. **Phase 16b — Materializer + loader.** Rewrite the card-to-DB-row pipeline. This is CPU-bound and touches the most code paths (quality scoring, search_text construction, edge materialization). The full Phase 0 test suite must pass against the Rust materializer producing identical output to the Python materializer.

3. **Phase 16c — Query layer + MCP server.** Rewrite the query engine and MCP server. This is the user-facing hot path. After this phase, `ppa serve` starts the Rust MCP server (not the Python one). All MCP tool tests must pass.

4. **Phase 16d — Embedding orchestrator + chunker.** Rewrite the embedding pipeline with proper async I/O. This is the least urgent (API-rate-limited) but benefits from better concurrency.

5. **Phase 16e — CLI.** Rewrite `ppa` CLI entrypoint in Rust. All subcommands (`setup`, `serve`, `maintain`, `status`, `lock`, `unlock`, etc.) are Rust, calling into `ppa-engine` and `ppa-python` as needed.

**At every stage:** The Phase 0 test suite runs against both the Python and Rust implementations. The Rust implementation must produce **identical output** (same database rows, same query results, same MCP responses) as the Python implementation. The test suite is the migration safety net.

**Coexistence period:** During the migration, the Python engine continues to work. Users can run either engine via a config flag (`PPA_ENGINE=rust` or `PPA_ENGINE=python`). The Python engine is deprecated once the Rust engine passes all tests and benchmarks confirm the performance targets.

### Testing strategy

- **Phase 0 test suite as the contract.** Every behavioral test in `slice_manifest.json` must pass against the Rust engine. Every structural invariant (zero orphans, edge counts, quality scores) must hold. This is the definition of "correct rewrite."
- **Row-level diff.** After a full rebuild with the Rust engine, compare every row in every table against the Python engine's output. Excluded columns: timestamps, sequence IDs (same exclusion list as Phase 0's incremental-vs-full test).
- **Benchmark suite.** Run the Phase 0 benchmark suite (1% and 5% slices) against both engines. The Rust engine must meet or exceed the performance targets above.
- **MCP protocol tests.** All MCP tool calls produce identical responses from both engines. Automated comparison of JSON responses for a suite of representative queries.

**Files touched:** New: entire `archive_crate/` Rust workspace. Modified: `pyproject.toml` (build system integration for Rust binary), `Makefile` (Rust build targets), CI pipeline (Rust build + test).

### Definition of Done

_Phase 16a (scanner):_

- Rust vault scanner produces identical manifest to Python scanner on the 5% slice
- Cold scan time meets performance target (8-15x faster than Python)
- PyO3 bridge works: Rust can call Python extractors and receive card dicts

_Phase 16b (materializer + loader):_

- Full rebuild with Rust materializer produces row-identical output to Python materializer
- Phase 0 test suite passes against Rust-built index
- Rebuild time meets performance target

_Phase 16c (query + MCP):_

- All MCP tool tests pass against Rust MCP server
- All `slice_manifest.json` query/answer pairs pass
- MCP server startup meets performance target (<200ms)
- Concurrent MCP requests handled without blocking

_Phase 16d (embedder + chunker):_

- Embedding pipeline produces identical chunks and manages API calls correctly
- Concurrent embedding requests improve throughput over Python implementation

_Phase 16e (CLI):_

- `ppa` CLI works end-to-end as a Rust binary
- All subcommands functional
- Python engine deprecated behind `PPA_ENGINE=python` flag

_Overall:_

- Row-level diff between Python and Rust engines shows zero differences (excluding timestamps/sequences)
- Benchmark suite confirms all performance targets met
- No Python dependencies in the hot path (startup, query, MCP serving)

---

## Phase 17: OAuth Proxy Service

**What it is:** A hosted service on GCP that handles OAuth flows for data source connections. The PPA Service is a verified Google OAuth application that facilitates the OAuth dance for all PPA users, without ever seeing user data.

**Why it exists:** v3 users create their own Google Cloud project for OAuth — workable for technical users, but a wall for everyone else. The OAuth proxy solves this once for all users.

### OAuth flow

```
User's Device                    PPA OAuth Service (ppa.dev on GCP)
─────────────                    ─────────────────────────────────
1. User clicks "Connect Gmail"
   App generates ephemeral
   X25519 keypair

2. App opens browser to          3. Service redirects to Google
   ppa.dev/oauth/google?            OAuth with PPA's verified
   device_pk=<public_key>&          client_id + scopes
   state=<random>

                                 4. User approves on Google's
                                    consent screen

                                 5. Google redirects to
                                    ppa.dev/oauth/callback
                                    with authorization code

                                 6. Service exchanges code for
                                    refresh_token + access_token

                                 7. Service encrypts tokens with
                                    device_pk (X25519 + XChaCha20-Poly1305)

                                 8. Service stores encrypted blob
                                    ephemerally (5 min TTL)

9. App polls ppa.dev/oauth/poll  ← 10. Returns encrypted token blob

11. App decrypts with private key
12. Stores refresh_token in Keychain
13. Uses refresh_token directly
    with Google — service never
    sees email content
```

### Supported OAuth providers

| Provider   | Scopes                                                     | Adapters                  |
| ---------- | ---------------------------------------------------------- | ------------------------- |
| **Google** | `gmail.readonly`, `calendar.readonly`, `contacts.readonly` | Gmail, Calendar, Contacts |
| **GitHub** | `repo` (read), `read:user`                                 | GitHub history            |

### Google OAuth verification

Requirements for `gmail.readonly`:

1. Verified domain ownership (`ppa.dev`)
2. Published privacy policy and terms of service
3. CASA Tier 2 security assessment
4. OAuth consent screen with app name, logo, support email

**Timeline:** 2-6 weeks. Start the verification process early — it can run in parallel with all other v4 development.

**Fallback:** Developer mode (bring your own OAuth project) continues to work for self-hosters and during the verification period.

### Service infrastructure

GCP Cloud Run (serverless, auto-scaling, pay-per-request):

```
ppa-service/
├── oauth/
│   ├── google.rs          # Google OAuth flow
│   ├── github.rs          # GitHub OAuth flow
│   ├── crypto.rs          # X25519 token encryption
│   └── routes.rs          # HTTP endpoints
├── connectors/
│   ├── feed.rs            # Signed connector manifest
│   └── signing.rs         # Ed25519 signing
├── billing/
│   ├── stripe.rs          # Subscription management
│   └── license.rs         # License key validation
├── telemetry/
│   └── ingest.rs          # Anonymous usage events
└── deploy/
    ├── Dockerfile
    └── cloudbuild.yaml
```

**Files touched:** New: `ppa-service/` (Rust service). New: `archive_crate/ppa-engine/src/connector_updater.rs` (feed client). Modified: extractor registry (load from both built-in and downloaded directories).

### Definition of Done

- Google OAuth verification approved
- OAuth flow works end-to-end: app → service → Google → encrypted token → app → Keychain
- Service stateless within 5 minutes of each flow
- Connector feed publishes signed packages from CI
- Deployed to GCP Cloud Run

---

## Phase 18: Native Mac App

**What it is:** A native Mac application built with Tauri (Rust shell + web frontend) that packages the Rust PPA engine with embedded Postgres, setup wizard, menu bar presence, dashboard, and MCP auto-registration.

### Technology stack

**Tauri (Rust + web UI):** The app shell is Rust — the same language as the v4 engine. The frontend is a web UI (Svelte or React) rendered in a native WebView.

- Small binary (~15MB Tauri + Rust engine + ~50MB Postgres binaries)
- Native macOS integration (menu bar, Keychain, notifications, LaunchAgent)
- No Python runtime bundled (extractors run via embedded Python, ~40MB, or pre-compiled to Rust via `inline-python` in future)

**The Rust engine IS the app.** Unlike the v3 concept where Tauri managed a Python subprocess, in v4 the Tauri app links directly to `ppa-engine` as a Rust library. No subprocess management, no IPC, no health checks. The engine runs in-process. Python extractors are called via PyO3 from within the same process.

### Embedded Postgres

Same approach as originally planned for v3, but now managed by Rust directly:

```rust
// Postgres lifecycle in the Tauri app
fn start_postgres(data_dir: &Path, socket_dir: &Path) -> Result<Child> {
    // initdb if needed
    if !data_dir.join("PG_VERSION").exists() {
        Command::new(bundled_bin("initdb"))
            .args(["-D", data_dir.to_str().unwrap()])
            .status()?;
    }
    // start with PPA-specific config
    Command::new(bundled_bin("pg_ctl"))
        .args(["start", "-D", data_dir.to_str().unwrap(), "-l", log_path])
        .status()?;
    // wait for ready
    wait_for_pg_ready(socket_dir)?;
    Ok(())
}
```

Unix socket only (no TCP listener). Postgres config tuned for single-user desktop:

```
shared_buffers = '128MB'
work_mem = '32MB'
maintenance_work_mem = '128MB'
effective_cache_size = '256MB'
max_connections = 5
listen_addresses = ''
```

### App structure

```
PPA.app/
└── Contents/
    ├── MacOS/
    │   ├── ppa                  # Tauri + ppa-engine (single Rust binary)
    │   ├── python/              # Bundled Python runtime (for extractors)
    │   │   ├── python3.12
    │   │   └── site-packages/   # Extractor + adapter code
    │   └── postgres/            # Embedded Postgres binaries
    │       ├── bin/
    │       │   ├── postgres
    │       │   ├── pg_ctl
    │       │   ├── initdb
    │       │   └── pg_isready
    │       └── lib/
    ├── Resources/
    │   ├── ui/                  # Web UI assets
    │   ├── signing-key.pub      # Ed25519 public key for connector feed
    │   └── default-config.yml
    └── Info.plist
```

**Total app size:** ~115-130MB (Rust engine + Tauri ~15MB + Python ~40MB + Postgres ~50MB + UI ~5MB).

### Setup wizard

Same flow as the v3 CLI wizard, but rendered in the web UI:

1. **Welcome** — "PPA is a personal archive that makes your AI tools smarter."
2. **Vault passphrase** — Set passphrase, Touch ID enrollment
3. **API keys** — OpenAI key or Ollama configuration
4. **Connect data sources** — Gmail via OAuth proxy (one click), plus optional sources
5. **AI tool registration** — Auto-detect Cursor, Claude Desktop; toggle to register
6. **Initial sync** — Rich progress view with per-source status, per-phase breakdown
7. **Telemetry consent** — Opt-in, unchecked by default
8. **Done** — "Ask your AI assistant anything about your life."

### Menu bar agent

Menu bar utility (no Dock icon by default):

```
┌──────────────────────────────────────┐
│  PPA                                  │
│                                      │
│  ● Archive healthy                    │
│  37,482 cards · 46 knowledge facets  │
│  Last sync: 2 hours ago              │
│                                      │
│  Sources:                            │
│    ✓ Gmail ─────────── 461K emails   │
│    ✓ Calendar ──────── 1,200 events  │
│    ✓ Contacts ──────── 892 people    │
│                                      │
│  MCP:                                │
│    ✓ Cursor connected                │
│    ✓ Claude Desktop connected        │
│                                      │
│  ─────────────────────────────────── │
│  Add Source...                        │
│  Open Dashboard                      │
│  Lock Archive             ⌘L        │
│  Settings                 ⌘,        │
│  Quit PPA                 ⌘Q        │
└──────────────────────────────────────┘
```

### Dashboard

Full window with tabs: Overview, Sources, Knowledge, Activity, Settings. Same layout as described in the original v3 vision — card counts, knowledge freshness, source status, maintenance log, configuration.

### MCP auto-registration

Writes config entries to detected MCP clients (Cursor, Claude Desktop). No env vars in the config — the engine reads everything from `ppa.yml` and Keychain.

### Background maintenance

LaunchAgent runs `ppa maintain` every 6 hours (configurable). Connector updates checked and applied on each cycle. Brief notification on completion.

### Auto-updates + auto-launch

- App checks for updates on launch and daily (Tauri built-in updater)
- Connector updates applied without restart via signed feed
- Auto-launch on login via LaunchAgent
- Touch ID unlock on launch

### Notarization

Distributed as a notarized `.dmg` outside the App Store.

**Files touched:** New: `archive_crate/ppa-app/` (Tauri project integrated into the Rust workspace).

### Definition of Done

- App installs from notarized `.dmg`, launches, completes setup wizard
- Encrypted volume + Touch ID unlock works
- Embedded Postgres starts/stops cleanly, crash recovery works
- OAuth flow connects Gmail via PPA Service
- Initial sync completes with rich progress UI
- MCP auto-registers with Cursor and Claude Desktop
- Menu bar + dashboard functional with real data
- Background maintenance on schedule
- Auto-updates work
- App size under 150MB
- App passes Apple notarization

---

## Phase 19: Billing + Distribution

**What it is:** Stripe subscription management, license key validation, `.dmg` distribution, and `ppa.dev`.

### One subscription: $12/month

Via Stripe Checkout:

1. User visits `ppa.dev` → "Download" → Stripe Checkout
2. Subscribes → receives license key via email
3. Downloads `.dmg` from `ppa.dev/download`
4. Pastes license key during app setup
5. App validates locally (Ed25519 signature) + weekly online check

**Graceful degradation on expired license:**

- All PPA functionality continues (vault, index, extractors, knowledge, MCP)
- OAuth proxy stops working for new connections (existing tokens keep working)
- Connector feed stops delivering updates (existing connectors keep working)
- Auto-updates stop (existing version keeps working)
- Dashboard shows: "Subscription expired. Archive continues to work. Renew for OAuth and updates."

**No kill switch.** Ever.

### ppa.dev

```
ppa.dev/
├── /              # Landing page
├── /download      # .dmg for Mac, Docker for Linux
├── /docs          # Documentation
├── /privacy       # Privacy policy
├── /terms         # Terms of service
├── /oauth/...     # OAuth proxy endpoints
├── /connectors/   # Connector feed
└── /api/          # License validation, telemetry
```

**Files touched:** New: `ppa-web/` (site source). New: `ppa-service/billing/` (Stripe integration).

### Definition of Done

- Stripe flow works end-to-end
- License key validation works (local + online)
- Graceful degradation on expired license
- Notarized `.dmg` on `ppa.dev/download` and GitHub Releases
- `ppa.dev` live with landing page, docs, pricing

---

## Dependencies Between Phases

```
v3 Complete (Phase 15)
│
├── Phase 16: Rust engine rewrite
│   ├── 16a: Scaffold + scanner (2-3 weeks)
│   ├── 16b: Materializer + loader (4-6 weeks) ← after 16a
│   ├── 16c: Query + MCP server (3-4 weeks) ← after 16b
│   ├── 16d: Embedder + chunker (2-3 weeks) ← parallel with 16c
│   └── 16e: CLI (2-3 weeks) ← after 16c
│
├── Phase 17: OAuth proxy service (parallel with 16)
│   └── Google OAuth verification (start ASAP, 2-6 week lead time)
│
├── Phase 18: Mac app (Tauri) ← needs 16 (Rust engine), 17 (OAuth)
│
└── Phase 19: Billing + distribution ← needs 18
```

**Critical path:** Phase 16a → 16b → 16c → 16e → Phase 18 → Phase 19

**Parallel work:**

- Phase 17 (OAuth service) can run entirely in parallel with Phase 16 (Rust engine)
- Phase 16d (embedder) can run in parallel with Phase 16c (query/MCP)
- Google OAuth verification should start as early as possible (long lead time)

---

## Risks

| Risk                                                                                                                                                                                                              | Impact                                                   | Mitigation                                                                                                                                                                                                                                                | Decision Point                                                                                                                                                           |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Rust rewrite takes longer than estimated** — Translating Python's dynamic typing and library ecosystem to Rust is harder than expected. Edge cases in YAML parsing, date handling, and Unicode text processing. | v4 timeline extends significantly.                       | Incremental migration (module by module). Python engine stays functional throughout. If a module takes >2x estimated time, evaluate whether to keep it in Python behind PyO3.                                                                             | Phase 16b: if materializer rewrite exceeds 8 weeks, keep entity resolution, knowledge, and enrichment in Python permanently.                                             |
| **PyO3 bridge overhead** — Calling Python extractors from Rust via PyO3 adds overhead per extraction call. GIL contention if extractors run concurrently.                                                         | Extraction performance doesn't improve (or regresses).   | Extractors are not on the hot path for queries — they run during `ppa maintain`, not during MCP requests. Single-threaded extraction via PyO3 is acceptable. If parallel extraction is needed, use multiple Python sub-interpreters (PyO3 supports this). | Phase 16a: benchmark PyO3 call overhead. If >1ms per call, evaluate alternatives (subprocess with batch protocol).                                                       |
| **Postgres embedded reliability** — Same risk as original v3 Phase 10b.                                                                                                                                           | App data corruption.                                     | Extensive startup/shutdown/crash testing. WAL recovery verification. Unix socket only.                                                                                                                                                                    | Phase 18: if crash recovery fails in testing, ship with Postgres.app as a dependency.                                                                                    |
| **Google OAuth verification** — Rejection or extended delay.                                                                                                                                                      | Users can't connect Gmail via the service.               | Start early. Developer mode fallback works for self-hosters. If rejected, ship Mac app without OAuth proxy (users create their own GCP project).                                                                                                          | Phase 17: if >6 weeks without approval, ship without OAuth proxy and add it post-launch.                                                                                 |
| **Tauri + Rust engine integration complexity** — The Tauri app links directly to `ppa-engine`. Build system complexity (Cargo workspace + Tauri + PyO3 + Postgres binaries).                                      | Build failures, long compile times, difficult debugging. | Keep the build system simple: single Cargo workspace, clear crate boundaries. CI builds on every push. Cross-compilation for arm64/x86_64 via GitHub Actions.                                                                                             | Phase 18: if build system takes >2 weeks to stabilize, separate ppa-engine as a pre-built binary and have Tauri manage it as a subprocess (like the original v3 design). |
| **App size** — Rust engine + Python runtime + Postgres binaries + Tauri + UI may exceed 150MB.                                                                                                                    | User perception of bloat.                                | Profile each component's contribution. Strip debug symbols. Use `upx` for binary compression. Consider dropping Python runtime if all extractors can be compiled to Rust (post-v4).                                                                       | Phase 18: if >200MB, evaluate dropping bundled Python (require system Python) or compressing Postgres binaries.                                                          |

---

## Post-v4 Roadmap (explicitly deferred)

| Feature                                 | Why Deferred                                                                         | When It Might Return                                                                               |
| --------------------------------------- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------- |
| **Hosted embedding/enrichment service** | BYOK works. Hosted compute adds trust boundary.                                      | When user base expands beyond technical users.                                                     |
| **iOS companion app**                   | Mac + VM covers the core audience. Mobile archive access is niche.                   | When Mac app is stable and there's demand.                                                         |
| **Full Rust extractors**                | Python extractors work via PyO3. Rewriting 20+ extractors is high cost, low benefit. | When community contributions in Rust outnumber Python, or when PyO3 overhead becomes a bottleneck. |
| **Team/shared archives**                | v4 is single-user. Multi-user adds auth, permissions, conflict resolution.           | If enterprise demand emerges.                                                                      |
| **Linux desktop app**                   | Tauri supports Linux desktop. Low priority vs. Mac and Docker.                       | When Linux desktop users request it (Tauri makes this relatively easy).                            |
| **Web dashboard for VM deployments**    | `ppa status` CLI is sufficient for self-hosters.                                     | If VM deployments grow and users request a GUI.                                                    |
| **Alternative database backends**       | Postgres works everywhere PPA runs. SQLite would enable iOS.                         | If iOS becomes a priority.                                                                         |

---

## Summary Table

| Phase  | What                          | Effort         | Depends On             | Ships to Users                       |
| ------ | ----------------------------- | -------------- | ---------------------- | ------------------------------------ |
| 16a    | Rust scaffold + vault scanner | 2-3 weeks      | v3 complete            | No (internal)                        |
| 16b    | Rust materializer + loader    | 4-6 weeks      | 16a                    | No (internal)                        |
| 16c    | Rust query layer + MCP server | 3-4 weeks      | 16b                    | No (internal)                        |
| 16d    | Rust embedder + chunker       | 2-3 weeks      | 16a (parallel w/ 16c)  | No (internal)                        |
| 16e    | Rust CLI                      | 2-3 weeks      | 16c                    | Yes (self-hosters get faster engine) |
| 17     | OAuth proxy service           | 4-6 weeks      | v3 complete (parallel) | No (service infra)                   |
| **18** | **Native Mac app (Tauri)**    | **8-12 weeks** | **16e, 17**            | **Yes — v4 launch**                  |
| 19     | Billing + distribution        | 3-4 weeks      | 18                     | Yes                                  |

**Total effort:** ~30-43 weeks of work across all phases.

**Calendar time with parallelization (2-3 workstreams):** ~24-32 weeks (~6-8 months) from v3 completion.

**Critical path:** 16a (3 wk) → 16b (6 wk) → 16c (4 wk) → 16e (3 wk) → 18 (12 wk) → 19 (4 wk) = **~32 weeks serial.**

---

## The Full Product Ladder

```
v2  — Knowledge system for one user (you)
v3  — Self-hosted for N users (friends, HN, self-hosters)
v4  — Mac app + service for everyone (install and forget)
```

Each version validates the next. Each version's users are the beta testers for the next version's assumptions. The engine gets rewritten in Rust not because Python is broken, but because the architecture is proven and the performance ceiling matters for the product PPA is becoming.

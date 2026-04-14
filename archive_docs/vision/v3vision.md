# PPA v3 Vision — Self-Hosted Multi-User

---

## The Core Thesis

After v2, PPA is a **personal knowledge system** — 37 card types, 17 adapters, 46 knowledge facets, full MCP surface, automated maintenance. It understands your life well enough to answer questions instantly, build context automatically, and get smarter over time. But it runs on one machine, for one person, configured by hand.

v3 takes PPA from **one user to many**. The goal: anyone with a Linux server (or a Mac with Docker) can run their own private archive. The CLI setup wizard replaces manual configuration. Vault encryption replaces trust-the-OS. Docker Compose replaces "read the Makefile and figure it out." The engine stays Python, the database stays Postgres, the interface stays MCP — but the operational experience goes from "built by and for its creator" to "built for anyone comfortable with a terminal."

The v3 user is someone who already lives inside Claude, Cursor, and chatbots — and wants those tools to actually know them. They're comfortable with Docker, API keys, and SSH. They found PPA on Hacker News or GitHub. They don't need a GUI; they need a clean `ppa setup` and good docs.

**Fully open source.** The engine, all connectors, all knowledge domains — everything is MIT-licensed. Anyone can clone, build, and run PPA with zero limitations. Zero phone-home. Zero license keys. Zero artificial restrictions.

**v4 builds on v3** with a native Mac app, OAuth proxy service, signed connector feed, billing, and a full Rust engine rewrite. See `v4vision.md`. v3 is the foundation that proves PPA works for people who aren't its creator.

---

## Principles

v2 principles 1–9 remain in force. v3 adds:

10. **The user's data never leaves their hardware.** There is no PPA service in v3. No cloud component. No telemetry endpoint. The user connects their data sources directly from their machine to the service provider. The archive, the index, and the MCP server all run on the user's hardware.

11. **BYOK (Bring Your Own Keys) for all AI services.** Users provide their own OpenAI API key (or run local models via Ollama) for embeddings and enrichment. PPA does not proxy, meter, or resell AI compute.

12. **Every long-running operation communicates its progress clearly.** Phase, count, percentage, throughput, ETA in `M:SS` — displayed in the terminal with visual progress bars. The user should never wonder "is this stuck?" or "how long will this take?" If an operation will take 3 hours, say so upfront and show continuous progress. Honesty about duration beats false urgency about speed.

13. **Connectors are open source.** The community extends connector coverage. Someone has Grubhub emails you don't? They write the extractor, everyone benefits. The Extractor Development Lifecycle (Phase 2.5) and contribution framework make this frictionless.

14. **The setup wizard is the product.** In v3, the terminal IS the interface. `ppa setup` must be polished, intuitive, and well-crafted — not a script that dumps instructions, but a guided flow that configures everything. First impressions happen in the terminal.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    PPA Core Engine (Python)                    │
│                                                                │
│  vault (markdown)  ·  index (Postgres)     ·  extractors      │
│  knowledge cache   ·  MCP server (stdio)   ·  maintain cycle  │
│  entity resolution ·  embedding (BYOK)     ·  enrichment      │
└───────────────────────────┬────────────────────────────────────┘
                            │
              ┌─────────────┼─────────────────┐
              ▼                                ▼
   ┌──────────────────────┐      ┌──────────────────────┐
   │  Docker Compose      │      │  Bare Metal           │
   │                      │      │                      │
   │  Docker Postgres     │      │  System Postgres     │
   │  ppa-engine container│      │  pip install -e .    │
   │  maintainer container│      │  systemd / cron      │
   │  encrypted volume    │      │  LUKS volume         │
   └──────────────────────┘      └──────────────────────┘
              │                                │
              └──────────┬─────────────────────┘
                         ▼
              ┌──────────────────────┐
              │  MCP Access          │
              │                      │
              │  Local (same machine)│
              │  SSH tunnel          │
              │  HTTPS + mTLS       │
              └──────────────────────┘
```

No service layer. No cloud. No phone-home. Everything runs on the user's hardware.

---

## Card Type Inventory

**Unchanged from v2.** All 37 types (22 existing + 11 derived + 2 entity + 2 system) carry forward. v3 does not add new card types — it changes who can run the engine and how they set it up.

Future card types driven by community connectors (post-v3): `bank_transaction`, `crypto_transaction`, `reading_highlight`, `workout`, `sleep_record`, `recipe`. These arrive via the connector contribution framework, not as core types.

---

## Phase 10: Vault Encryption + Passkey Auth

**What it is:** Volume-level encryption for the vault and Postgres data, passkey/biometric unlock, and secure credential storage. The archive locks independently of OS-level disk encryption.

**Why it exists:** For an archive containing email, health records, financial data, and personal communications, users need cryptographic confidence. OS-level encryption (FileVault, dm-crypt) decrypts transparently when the user is logged in — any process with disk access can read the vault. The vault gets its own encrypted volume that locks independently.

### Encrypted volume

The vault and Postgres data directory live inside an encrypted container that mounts as a regular directory when unlocked.

**Linux implementation (primary for v3):**

```bash
# Create LUKS container (one-time, during ppa setup)
dd if=/dev/zero of=/srv/ppa/vault.img bs=1M count=0 seek=51200  # 50GB sparse
cryptsetup luksFormat /srv/ppa/vault.img
cryptsetup open /srv/ppa/vault.img ppa-vault
mkfs.ext4 /dev/mapper/ppa-vault
mount /dev/mapper/ppa-vault /srv/ppa/secure
```

**macOS implementation (for Mac users running Docker):**

```bash
# Create encrypted sparse bundle (one-time, during ppa setup)
hdiutil create -size 50g -type SPARSEBUNDLE -encryption AES-256 \
  -stdinpass -fs APFS -volname "PPA Archive" ~/PPA.sparsebundle

# Mount (on unlock)
hdiutil attach ~/PPA.sparsebundle -stdinpass -mountpoint /path/to/ppa/secure

# Unmount (on lock)
hdiutil detach /path/to/ppa/secure
```

When the volume is mounted, the vault and Postgres data directory are accessible at their expected paths. When locked, they're ciphertext on disk.

### Passkey / biometric unlock

The encryption key is derived from a passphrase set during setup (Argon2id: passphrase + random salt → 256-bit key).

**On macOS:** The derived key is stored in the macOS Keychain, protected by biometric access control (`kSecAccessControlBiometryCurrentSet | kSecAccessControlDevicePasscode`). Subsequent unlocks use Touch ID with passphrase fallback.

**On Linux:** The derived key is stored via `libsecret` / `secret-service` D-Bus API if a system keyring is available (GNOME Keyring, KDE Wallet). If no keyring is available (headless servers), the passphrase is prompted on every unlock.

```
First setup (ppa setup):
  1. User sets vault passphrase
  2. PPA derives encryption key via Argon2id
  3. Key stored in OS keychain (if available) or prompts each time
  4. Encrypted volume created and mounted
  5. Vault initialized inside the volume

Subsequent unlocks (ppa unlock):
  Linux with keyring: key retrieved from keyring, volume mounts
  Linux headless:     passphrase prompted, key derived, volume mounts
  macOS:              Touch ID → Keychain → volume mounts

Lock (ppa lock):
  1. Stop MCP server + maintenance
  2. Stop Postgres (if managed by PPA)
  3. Unmount encrypted volume
```

**Passphrase change:** Re-encrypts the volume header with a new key derived from the new passphrase. Does not re-encrypt all data (LUKS and sparsebundle both support header-only rekey). Instant operation.

### Credential storage

All sensitive credentials stored securely, never in plaintext `.env` files:

| Credential                 | macOS                | Linux (with keyring) | Linux (headless)        |
| -------------------------- | -------------------- | -------------------- | ----------------------- |
| Vault encryption key       | Keychain (biometric) | libsecret            | Prompt each time        |
| Google OAuth refresh token | Keychain             | libsecret            | Encrypted file in vault |
| GitHub token               | Keychain             | libsecret            | Encrypted file in vault |
| OpenAI API key             | Keychain             | libsecret            | Encrypted file in vault |
| Postgres password          | Keychain             | libsecret            | Encrypted file in vault |

**Migration from env vars:** Users who currently use `PPA_INDEX_DSN`, `OPENAI_API_KEY`, etc. in `.env` files continue to work. The engine reads env vars as a fallback if no secure credential store entry exists. `ppa setup` migrates credentials into the secure store.

### CLI commands

- `ppa lock` — unmount encrypted volume, stop engine
- `ppa unlock` — prompt for passphrase/biometric, mount volume, start engine
- `ppa change-passphrase` — re-derive encryption key, rekey volume header
- `ppa export-vault --output /path/to/backup.tar.gz.enc` — encrypted vault backup

**Files touched:** New: `archive_cli/encryption.py` (volume create/mount/unmount, key derivation via Argon2id), `archive_cli/credentials.py` (credential storage abstraction — macOS Keychain, Linux libsecret, encrypted-file fallback). Modified: `archive_cli/config.py` (credential loading from secure store with env var fallback), `archive_cli/__main__.py` (lock/unlock/change-passphrase/export-vault commands).

### Definition of Done

- Encrypted volume creates, mounts, and unmounts cleanly on Linux (LUKS) and macOS (sparsebundle)
- Touch ID unlock works on macOS (with passphrase fallback)
- Passphrase-only unlock works on headless Linux
- PPA engine operates normally against a mounted encrypted vault (full Phase 0 test suite passes)
- No plaintext credentials on disk when secure store is available
- `ppa lock` / `ppa unlock` CLI commands work
- Passphrase change completes instantly (header rekey, not full re-encryption)
- Crash during lock does not corrupt the volume (Postgres WAL + filesystem journaling)
- Env var fallback works for users who prefer `.env` configuration

---

## Phase 11: CLI Setup Wizard

**What it is:** An interactive, polished CLI wizard (`ppa setup`) that takes a user from zero to a running PPA instance — vault, encryption, database, API keys, data sources, and MCP configuration — in one guided flow.

**Why it exists:** Today, setting up PPA requires reading the README, editing `.env` files, running `make` targets, manually configuring OAuth credentials, and knowing which CLI commands to run in what order. This is fine for the person who wrote the code. It's a wall for anyone else. The wizard is the product in v3.

### The wizard

```
$ ppa setup

  ╔══════════════════════════════════════╗
  ║  PPA — Personal Private Archives     ║
  ║  Setup                               ║
  ╚══════════════════════════════════════╝

  Step 1/7: Vault location

  Where should PPA store your archive?
  This directory will contain your vault (markdown files)
  and the Postgres database. It will be encrypted at rest.

  Vault path [/srv/ppa]: █

  Step 2/7: Encryption

  Set a passphrase to encrypt your vault.
  You'll need this passphrase to start PPA after a reboot.

  Passphrase: ████████████
  Confirm:    ████████████

  ✓ Created encrypted volume at /srv/ppa/vault.img (50 GB sparse)
  ✓ Mounted at /srv/ppa/secure

  Step 3/7: Database

  PPA uses PostgreSQL for indexing. Choose a configuration:

  > [1] Docker (recommended) — PPA manages Postgres in a container
    [2] Existing Postgres — connect to your own instance

  Choice [1]: █

  ✓ Generated Postgres credentials
  ✓ Started PostgreSQL container (pgvector/pgvector:pg17)
  ✓ Database initialized

  Step 4/7: API keys

  PPA uses AI models for semantic search and enrichment.
  Provide your API key, or skip to use keyword search only.

  [1] OpenAI API key
  [2] Ollama (local models — no API key needed)
  [3] Skip (keyword search only, add later with 'ppa config')

  Choice [1]: █
  OpenAI API key: sk-████████████████

  ✓ API key verified (text-embedding-3-small accessible)
  ✓ Stored securely

  Step 5/7: Data sources

  Connect your accounts. Data flows directly from the provider
  to this machine — no intermediary.

  Connect Gmail?

  To connect Gmail, you need a Google Cloud OAuth project.
  Follow these steps (takes ~5 minutes):

    1. Go to console.cloud.google.com/apis/credentials
    2. Create a new project (or select existing)
    3. Enable the Gmail API
    4. Create OAuth 2.0 credentials (Desktop application)
    5. Download the client secret JSON

  Path to client_secret.json (or 'skip'): ~/Downloads/client_secret_123.json

  ✓ Opening browser for Google OAuth consent...
  ✓ Gmail connected

  Also enable?
    [x] Google Calendar (same project, no extra setup)
    [x] Google Contacts (same project, no extra setup)
    [ ] Apple Photos (macOS only)
    [ ] iMessage (macOS only, requires Full Disk Access)
    [ ] GitHub (paste personal access token)
    [ ] Skip remaining

  Step 6/7: MCP access

  How will AI tools connect to this PPA instance?

  > [1] Local only (same machine)
    [2] SSH tunnel (remote access from your laptop)
    [3] HTTPS with client certificates

  Choice [2]: █

  ✓ Generated MCP config. Paste into ~/.cursor/mcp.json on your laptop:

  {
    "mcpServers": {
      "ppa": {
        "command": "ppa",
        "args": ["serve", "--tunnel", "user@this-server"],
        "env": {
          "PPA_INDEX_DSN": "postgresql://ppa:****@127.0.0.1:5433/ppa",
          "PPA_PATH": "/srv/ppa/secure/vault"
        }
      }
    }
  }

  Step 7/7: Initial sync

  Starting initial data import. This may take a while for large inboxes.
  Progress will be shown below. You can also check 'ppa status' from
  another terminal.

  Gmail     ████████████████████████████░░░░ 87%
            381,000 / 438,000 emails · 2,340 emails/sec · ETA 0:24

  Calendar  ████████████████████████████████ Complete
            1,247 events imported

  Contacts  ████████████████████████████████ Complete
            892 contacts imported

  ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄

  After sync completes, PPA will automatically:
    → Extract structured data from emails (meal orders, flights, rides...)
    → Build the search index
    → Compute embeddings for semantic search
    → Generate knowledge cache (what you eat, where you travel, who you talk to)

  Each step shows its own progress. Total time depends on inbox size.
  Run 'ppa status' anytime to check.

  ✓ Setup complete.
```

### `ppa status` — the ongoing dashboard

```
$ ppa status

  PPA — Archive Status
  ─────────────────────────────────────────────────────

  Vault:     /srv/ppa/secure/vault
  Database:  postgresql://ppa@localhost:5432/ppa (Docker)
  Encrypted: ✓ Volume mounted

  Cards:          438,291 total
  ├── email       412,847
  ├── calendar      1,247
  ├── person          892
  ├── meal_order    1,203
  ├── ride            847
  ├── purchase      1,456
  └── (12 more types...)

  Knowledge:      46 facets across 9 domains
  ├── Fresh:      41
  ├── Stale:       5 (refreshing on next maintain cycle)
  └── Empty:       0

  Embeddings:     100% (438,291 / 438,291 chunks)
  Last maintain:  2 hours ago (next: in 4 hours)

  MCP:            Serving on stdio
  Connections:    Last query 14 minutes ago

  Sources:
    ✓ Gmail         438,000 emails    last sync: 2h ago
    ✓ Calendar        1,247 events    last sync: 2h ago
    ✓ Contacts          892 people    last sync: 2h ago
```

### `ppa connect` — add sources after setup

```
$ ppa connect github
  GitHub personal access token: ghp_████████████████
  ✓ Token verified (user: rheeger)
  ✓ Syncing repositories...

$ ppa connect imessage
  ✓ Scanning ~/Library/Messages/chat.db...
  ✓ Found 23,412 messages across 847 conversations
  ✓ Import started (run 'ppa status' to monitor)
```

### `ppa config` — manage configuration after setup

```
$ ppa config set embedding-provider openai
$ ppa config set openai-api-key
  OpenAI API key: sk-████████████████
  ✓ Stored securely

$ ppa config show
  vault_path:          /srv/ppa/secure/vault
  database:            postgresql://ppa@localhost:5432/ppa
  embedding_provider:  openai
  embedding_model:     text-embedding-3-small
  enrichment_model:    openai:gpt-4o-mini
  maintain_schedule:   every 6 hours
  mcp_transport:       ssh (user@server)
```

### Terminal UI library

Use `rich` (Python) for all terminal output:

- Progress bars with ETA, throughput, and percentage
- Styled prompts with input validation
- Tables for status display
- Spinners for short operations
- Color-coded status indicators (✓ green, ✗ red, ● yellow)

The wizard should feel as polished as `npm init`, `gh auth login`, or `railway init`.

**Files touched:** New: `archive_cli/commands/setup.py` (interactive wizard), `archive_cli/commands/connect.py` (add data sources), `archive_cli/commands/config_cmd.py` (manage configuration). Modified: `archive_cli/__main__.py` (new subcommands), `archive_cli/commands/status.py` (enhanced status display with `rich`). New dependency: `rich` in `pyproject.toml`.

### Definition of Done

- `ppa setup` completes end-to-end: vault, encryption, database, API key, Gmail OAuth, MCP config
- Wizard handles errors gracefully (invalid API key, OAuth failure, Docker not installed) with clear messages and recovery options
- `ppa status` shows comprehensive archive health with formatted output
- `ppa connect` adds new data sources after initial setup
- `ppa config` manages all configuration without editing files
- Progress bars on all long operations (sync, extract, rebuild, embed) with ETA and throughput
- Wizard works on both Linux and macOS
- Wizard detects an existing vault and offers to import/reconnect
- OAuth guide (Google Cloud project creation) is clear enough for a first-timer to follow in 5 minutes
- All credentials stored in secure store (Keychain / libsecret / encrypted file)

---

## Phase 12: Docker Compose Packaging

**What it is:** A Docker Compose bundle and Dockerfile that packages PPA for self-hosted deployment. One `docker compose up` from zero to running.

**Why it exists:** The current setup requires a local Python environment, manual Postgres configuration, and familiarity with the Makefile. Docker Compose is the standard for self-hosted services — it handles Postgres, networking, volumes, and restart policy. Users who run Plex, Home Assistant, or Nextcloud already know this pattern.

### Docker Compose bundle

```yaml
services:
  ppa:
    image: ghcr.io/ppa-dev/ppa-engine:latest
    volumes:
      - ${PPA_VAULT_PATH:-./vault}:/data/vault
      - config:/data/config
    environment:
      - PPA_INDEX_DSN=postgresql://ppa:${PPA_DB_PASSWORD}@postgres:5432/ppa
      - PPA_PATH=/data/vault
      - PPA_INDEX_SCHEMA=archive_cli
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "127.0.0.1:${PPA_MCP_PORT:-8432}:8432" # MCP over HTTPS (optional)
    command: ["serve"]
    restart: unless-stopped

  postgres:
    image: pgvector/pgvector:pg17
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./postgres.conf:/etc/postgresql/postgresql.conf:ro
    environment:
      - POSTGRES_USER=ppa
      - POSTGRES_DB=ppa
      - POSTGRES_PASSWORD=${PPA_DB_PASSWORD}
    command: ["-c", "config_file=/etc/postgresql/postgresql.conf"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ppa"]
      interval: 5s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  maintainer:
    image: ghcr.io/ppa-dev/ppa-engine:latest
    volumes:
      - ${PPA_VAULT_PATH:-./vault}:/data/vault
      - config:/data/config
    environment:
      - PPA_INDEX_DSN=postgresql://ppa:${PPA_DB_PASSWORD}@postgres:5432/ppa
      - PPA_PATH=/data/vault
      - PPA_INDEX_SCHEMA=archive_cli
    command: ["maintain", "--schedule", "${PPA_MAINTAIN_SCHEDULE:-0 */6 * * *}"]
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

volumes:
  pgdata:
  config:
```

### Dockerfile

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml .
COPY archive_cli/ archive_cli/
COPY archive_sync/ archive_sync/
COPY archive_vault/ archive_vault/
COPY archive_doctor/ archive_doctor/
COPY archive_scripts/ archive_scripts/

RUN pip install --no-cache-dir -e .

ENTRYPOINT ["ppa"]
```

### Postgres configuration

```
# postgres.conf — tuned for single-user archive workloads
shared_buffers = '256MB'
work_mem = '64MB'
maintenance_work_mem = '256MB'
effective_cache_size = '512MB'
max_connections = 20
```

Matches the Phase 0 test infrastructure configuration and Arnold's production tuning.

### Integration with the CLI wizard

`ppa setup` detects whether Docker is available and offers it as the recommended database option:

- **Docker available:** Wizard generates `.env` and `docker-compose.yml`, runs `docker compose up -d`, waits for Postgres health check, continues with data source setup.
- **Docker not available:** Wizard falls back to "Existing Postgres" path — user provides a DSN.

### HTTPS MCP transport

For users connecting MCP clients from other machines (laptop → home server):

- **SSH tunnel (existing, recommended):** `ppa serve --tunnel user@host`. Works today.
- **HTTPS with mTLS (new):** `ppa serve --https --cert-dir /path/to/certs`. The wizard optionally generates a self-signed CA, server cert, and client cert.

```bash
ppa certs generate --output /srv/ppa/certs

# Generates:
# ca.pem, server.pem, server-key.pem, client.pem, client-key.pem
#
# Copy client.pem and client-key.pem to your laptop.
# MCP clients connect to https://your-server:8432 with the client cert.
```

### Arnold migration

Arnold can either:

- Continue with its current setup (systemd + manual config) — nothing breaks
- Migrate to the Docker Compose bundle: run `ppa setup` on Arnold, point at the existing vault

The wizard detects an existing vault and offers to reconnect rather than initialize from scratch.

**Files touched:** New: `ppa-deploy/` directory (docker-compose.yml, Dockerfile, postgres.conf, .env.example). New: `archive_cli/tls.py` (certificate generation for HTTPS transport). Modified: `archive_cli/server.py` (HTTPS transport option), `archive_cli/commands/setup.py` (Docker Compose integration).

### Definition of Done

- `docker compose up` starts PPA + Postgres from fresh state
- `ppa setup` with Docker path generates configs, starts containers, and completes full setup
- Docker image published to `ghcr.io/ppa-dev/ppa-engine`
- Docker image size under 500MB
- `ppa maintain` runs on schedule via the maintainer container
- MCP server accessible via SSH tunnel and HTTPS with mTLS
- Wizard detects existing vault and offers import
- Arnold can migrate to Docker Compose or continue as-is
- Container restart policy handles crashes and reboots

---

## Phase 13: Progress UX Overhaul

**What it is:** A focused pass on every long-running PPA operation to add rich terminal progress bars, ETAs, throughput metrics, and clear phase labeling.

**Why it exists:** v2 principle 9 established operational logging. v3 elevates it from a logging concern to a product-experience concern. When the terminal IS the product, progress visibility IS the UX. A user watching a 2-hour initial sync needs to feel confident and informed at every moment.

### Operations that get progress treatment

| Operation                | Current UX                 | v3 UX                                                    |
| ------------------------ | -------------------------- | -------------------------------------------------------- |
| Gmail sync               | Log lines on stderr        | Progress bar: emails/sec, ETA, count                     |
| Email extraction         | Log lines with yield rates | Per-extractor progress bar, item counts, quality metrics |
| Vault rebuild            | 6-step log output          | Multi-step progress with per-step bar, overall ETA       |
| Embedding pass           | Chunk count on stderr      | Progress bar: chunks/sec, API cost estimate, ETA         |
| Entity resolution        | Minimal logging            | Progress bar: entities processed, merges found           |
| Knowledge refresh        | Per-facet log lines        | Domain-by-domain progress, stale/fresh counts            |
| `ppa maintain`           | Step-by-step logs          | Multi-phase progress with overall cycle ETA              |
| Vault scan (cache build) | Minimal                    | File count progress bar with ETA                         |

### Implementation

Use `rich` for all progress output:

```python
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeRemainingColumn

with Progress(
    SpinnerColumn(),
    "[progress.description]{task.description}",
    BarColumn(),
    "[progress.percentage]{task.percentage:>3.0f}%",
    "·",
    "{task.completed}/{task.total}",
    "·",
    TimeRemainingColumn(),
) as progress:
    task = progress.add_task("Gmail sync", total=461000)
    for batch in sync_batches():
        process(batch)
        progress.update(task, advance=len(batch))
```

### Multi-phase operations

For operations like `ppa setup` initial sync or `ppa maintain`, show a multi-phase display:

```
  Phase 1/5: Gmail sync
  ████████████████████████████████ Complete · 438,000 emails · 3:42

  Phase 2/5: Email extraction
  ████████████████░░░░░░░░░░░░░░░░ 52%
  meal_order: 623 · ride: 412 · purchase: 891 · flight: 47 · ETA 1:15

  Phase 3/5: Index rebuild        Waiting...
  Phase 4/5: Embedding pass       Waiting...
  Phase 5/5: Knowledge cache      Waiting...

  Overall: ██████████░░░░░░░░░░░░░░░░░░░░░░ 34% · ETA 4:12
```

### Log file integration

All progress is also written to the log file (when `--log-file` is specified) in a structured, parseable format. The terminal UI and the log file are independent output streams — the terminal gets `rich` formatting, the log file gets structured text per v2's operational logging convention.

**Files touched:** Modified: `archive_cli/log.py` (rich progress integration), `archive_sync/adapters/gmail_messages.py` (progress callbacks), `archive_sync/extractors/runner.py` (per-extractor progress), `archive_cli/loader.py` (rebuild progress), `archive_cli/embedder.py` (embedding progress), `archive_cli/commands/maintain.py` (multi-phase progress), `archive_cli/vault_cache.py` (scan progress). New dependency: `rich` in `pyproject.toml` (if not already added in Phase 11).

### Vault scan cache improvements

Phase 13 also addresses vault scan cache performance for enrichment and other multi-step pipelines. The current cache (`vault-scan-cache.sqlite3`) indexes all 1.8M+ vault notes with a full-vault fingerprint. Any file write invalidates the fingerprint and triggers a full rebuild (~46 minutes on the seed vault). This is the right behavior for the MCP server (which needs a consistent global view), but catastrophic for pipelines like `ppa enrich` that write vault cards between steps.

**Phase 2.875 workaround (in place):** The enrichment orchestrator refreshes the stored fingerprint after each step (~55s stat walk) so the next step sees a cache hit and skips the full rebuild. This saves ~4 hours on a 6-step full-seed enrichment run but still walks all 1.8M files to recompute the fingerprint.

**Phase 13 improvements:**

1. **Type-filtered cache loading.** `VaultScanCache.build_or_load` gains an optional `card_types: list[str]` parameter. When set, the fingerprint and cache only cover the specified subdirectories (e.g. `Finance/`, `Documents/`). Enrichment steps that only need one card type avoid walking the entire vault. The MCP server continues to use the full cache (no `card_types` filter).

2. **Per-directory fingerprints.** Instead of one hash over all 1.8M paths, store per-top-level-directory fingerprints (e.g. `Email/`, `Finance/`, `Documents/`, `Calendar/`). A write to `Email/` only invalidates the `Email/` portion. Consumers that only need `Finance/` see a cache hit without any I/O. The combined fingerprint is the hash of all per-directory fingerprints (backward compatible for the MCP server).

3. **Shared in-memory cache for orchestrated pipelines.** The orchestrator builds the cache once and passes the `VaultScanCache` instance to each runner, eliminating redundant `build_or_load` calls entirely. Runners accept an optional `scan_cache` parameter; if provided, they skip `build_or_load`. This requires adding `scan_cache: VaultScanCache | None = None` to `CardEnrichmentRunner`, `LlmEnrichmentRunner`, and `run_document_text_extraction`.

4. **Progress bar on cache build.** The full-vault scan (currently silent for minutes) gets a `rich` progress bar showing files scanned, throughput, and ETA. This is the "Vault scan (cache build)" row in the operations table above.

**Files touched (in addition to those listed above):** `archive_cli/vault_cache.py` (type-filtered loading, per-directory fingerprints, progress callbacks), `archive_sync/llm_enrichment/enrichment_orchestrator.py` (shared cache instance), `archive_sync/llm_enrichment/card_enrichment_runner.py` (optional `scan_cache` parameter), `archive_sync/llm_enrichment/enrich_runner.py` (optional `scan_cache` parameter), `archive_sync/llm_enrichment/document_text_extractor.py` (optional `scan_cache` parameter).

### Definition of Done

- Every operation listed above shows a `rich` progress bar with percentage, count, throughput, and ETA
- Multi-phase operations show overall progress plus per-phase breakdown
- Progress display works correctly when output is a terminal (rich formatting) and when piped/redirected (plain text fallback)
- `--log-file` output is structured text, independent of terminal formatting
- No operation runs for more than 30 seconds without visible progress output
- ETA estimates are within 2x of actual time after the first 10% of work completes
- Progress bars degrade gracefully when total is unknown (spinner + count + throughput, no percentage)
- `ppa enrich` on the full seed does not rebuild the vault scan cache between steps (fingerprint refresh or shared instance)
- Type-filtered cache loads complete in under 10 seconds for single card types on the full seed
- Per-directory fingerprints correctly detect changes scoped to their directory without invalidating unrelated directories

---

## Phase 14: Self-Hosting Documentation

**What it is:** Comprehensive documentation for self-hosters — from "what is PPA?" to "I have it running and my AI tools are querying my archive."

**Why it exists:** Documentation is the other half of the product in v3. The CLI wizard handles the happy path, but users need docs for: understanding what PPA does before they install it, troubleshooting when something goes wrong, adding data sources after initial setup, understanding the architecture, and contributing connectors.

### Documentation structure

```
archive_docs/
├── README.md                        # What PPA is, quick start, screenshot
├── SELF_HOSTING.md                  # Complete self-hosting guide
│   ├── Prerequisites                # Docker, Python, Postgres options
│   ├── Quick start (Docker)         # 5-minute path
│   ├── Quick start (bare metal)     # For users without Docker
│   ├── Google OAuth setup           # Step-by-step with screenshots
│   ├── Data source configuration    # Per-source guides
│   ├── MCP client configuration     # Cursor, Claude Desktop, others
│   ├── Encryption and security      # Vault encryption, credential storage
│   ├── Maintenance                  # ppa maintain, cron, monitoring
│   ├── Backup and restore           # Vault backup, Postgres dump
│   ├── Upgrading                    # git pull, pip install, migrations
│   └── Troubleshooting              # Common issues and solutions
├── ARCHITECTURE.md                  # Updated for v3 (exists, needs refresh)
├── CONTRIBUTING_CONNECTORS.md       # Extractor contribution guide (Phase 15)
├── FAQ.md                           # Common questions
└── MCP_SETUP.md                     # Updated for v3 (exists, needs refresh)
```

### Google OAuth setup guide

This is the highest-friction part of self-hosted setup. The documentation must include:

1. Step-by-step instructions with screenshots for creating a Google Cloud project
2. Enabling Gmail, Calendar, and Contacts APIs
3. Creating OAuth 2.0 credentials (Desktop application type)
4. Downloading the client secret JSON
5. What scopes are requested and why
6. Handling the "unverified app" warning during OAuth consent
7. How to add test users (for personal projects under 100 users)

### Updated README

The repo README gets a rewrite for the v3 audience:

- Lead with what PPA does (not how it's built)
- Quick start in 5 commands (Docker Compose path)
- Screenshot or terminal recording of `ppa setup` and `ppa status`
- Link to full self-hosting guide
- Link to architecture docs for contributors
- Link to connector contribution guide

**Files touched:** New: `archive_docs/SELF_HOSTING.md`, `archive_docs/FAQ.md`. Modified: `README.md` (rewrite for v3 audience), `archive_docs/ARCHITECTURE.md` (refresh), `archive_docs/MCP_SETUP.md` (refresh).

### Definition of Done

- `archive_docs/SELF_HOSTING.md` covers every section listed above
- Google OAuth setup guide is followable by someone who has never used Google Cloud Console (tested with at least one person)
- README leads with value proposition, not implementation details
- FAQ covers at least 10 common questions (gathered from setup testing with real users)
- All existing docs updated to reflect v3 CLI commands and configuration
- Docs are cross-linked (README → Self-hosting → OAuth guide → MCP setup)

---

## Phase 15: Connector Contribution Framework

**What it is:** Documentation, CI pipelines, and tooling that make it easy for the community to contribute new extractors and adapters.

**Why it's a phase:** PPA's long-term value scales with connector coverage. Making it frictionless for contributors to add new extractors — with a clear spec format, test harness, and promotion path — turns the community into a connector development team.

### Contribution workflow

```
1. Contributor reads archive_docs/CONTRIBUTING_CONNECTORS.md
2. Copies the extractor template: archive_sync/extractors/specs/_template.md
3. Runs `ppa sender-census --domain <sender>` against their vault
4. Follows the Extractor Development Lifecycle (EDL) from Phase 2.5
5. Submits PR with:
   - Extractor module (archive_sync/extractors/<name>.py)
   - Spec document (archive_sync/extractors/specs/<name>.md)
   - Test fixtures (archive_tests/fixtures/emails/<name>/)
   - Registry entry (archive_sync/extractors/registry.py)
6. CI runs:
   - Unit tests against fixtures
   - Field validation checks
   - Idempotency test
   - Lint + type check
7. Maintainer reviews, merges
```

### Template repository

A GitHub template repo (`ppa-dev/ppa-connector-template`) with:

```
ppa-connector-template/
├── extractor.py              # Skeleton extractor with TODOs
├── spec.md                   # Spec template from Phase 2.5
├── fixtures/
│   ├── receipt_2024.md       # Example fixture format
│   └── receipt_2026.md
├── archive_tests/
│   └── test_extractor.py     # Test skeleton
└── README.md                 # Step-by-step contribution guide
```

### CI for connector PRs

GitHub Actions workflow that runs automatically on PRs touching `archive_sync/extractors/`:

- Parse test fixtures → run extractor → verify output card fields
- Run field validation (`validate_field` from Phase 2.5) on all output
- Verify idempotency (run twice, assert identical output)
- Verify sender patterns don't overlap with existing extractors
- Generate a quality report (yield rate, field population rates)

### Documentation

`archive_docs/CONTRIBUTING_CONNECTORS.md`:

1. How to identify extractable emails in your vault
2. How to use `ppa sender-census` and `ppa template-sampler`
3. The EDL phases (census → template discovery → anchor mapping → implementation → verification)
4. Field validation rules and how to add new ones
5. Test fixture format and conventions
6. How to submit a PR
7. How to test against the seed slice

**Files touched:** New: `archive_docs/CONTRIBUTING_CONNECTORS.md`. New: `.github/workflows/connector-ci.yml`. New: template repo `ppa-dev/ppa-connector-template`. Modified: `archive_sync/extractors/specs/README.md` (links to contribution guide).

### Definition of Done

- `archive_docs/CONTRIBUTING_CONNECTORS.md` written with clear, step-by-step instructions
- Template repo published at `ppa-dev/ppa-connector-template`
- CI workflow runs on extractor PRs and produces quality reports
- At least 1 community-contributed connector submitted and merged via the framework (proof of process)
- Connector CI catches a deliberate bug in a test PR (regression gate works)

---

## Dependencies Between Phases

```
v2 Complete (Phase 9)
│
├── Phase 10: Vault encryption + passkey auth
│
├── Phase 11: CLI setup wizard ← uses 10 for encryption steps
│   └── Phase 12: Docker Compose packaging ← wizard generates Docker configs
│       └── Phase 14: Documentation ← documents everything from 10-13
│
├── Phase 13: Progress UX overhaul (parallel with 10-12)
│
└── Phase 15: Connector contribution framework (anytime, no hard dependencies)
```

**Critical path:** Phase 10 → Phase 11 → Phase 12 → Phase 14

**Parallel work:**

- Phase 13 (progress UX) has no dependencies — can start immediately and land incrementally
- Phase 15 (connector framework) is documentation + CI — can start anytime
- Phases 10 and 13 can run in parallel from day one

**Calendar time with 2 workstreams:** ~15-20 weeks (~4-5 months) from v2 completion.

---

## Risks

| Risk                                                                                                                                                                                                          | Impact                                                                  | Mitigation                                                                                                                                                                                                                                               | Decision Point                                                                                                                                        |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Google OAuth setup friction** — Even with step-by-step docs, creating a Google Cloud project and OAuth credentials is confusing for users who've never done it. The "unverified app" warning scares people. | Users abandon setup at the Gmail step.                                  | Write the best possible guide with screenshots. Record a 3-minute video walkthrough. Consider providing a shared "PPA Community" OAuth project for beta testers (limited to 100 test users). v4 solves this with the OAuth proxy service.                | Phase 11: test the OAuth guide with 3+ people who haven't used Google Cloud Console. If >50% get stuck, add the video and/or community OAuth project. |
| **Vault encryption complexity on Linux** — LUKS requires root access for `cryptsetup`. Docker users may not have root. Headless servers may not have a keyring.                                               | Encryption setup fails or is skipped.                                   | Detect capabilities during `ppa setup`. If no root: skip LUKS, use an encrypted file-based vault (GPG or age). If no keyring: prompt for passphrase each time. Offer "no encryption" as an explicit opt-out with a warning.                              | Phase 10: if LUKS setup fails on >30% of test systems, default to file-level encryption.                                                              |
| **Docker Compose version fragmentation** — Different Linux distros ship different Docker Compose versions. Docker Compose v1 vs v2 syntax differences.                                                        | `docker compose up` fails on some systems.                              | Target Docker Compose v2 only (`docker compose`, not `docker-compose`). Document minimum Docker version. Detect and warn during `ppa setup`.                                                                                                             | Phase 12: test on Ubuntu 22.04, 24.04, Debian 12, and Fedora 40.                                                                                      |
| **Progress bar accuracy** — ETA estimates require knowing the total work upfront. For Gmail sync, the total email count isn't known until the sync starts. For extraction, yield rates vary per extractor.    | ETAs are wildly inaccurate, eroding trust.                              | Use adaptive ETA: start with throughput-only (no percentage), switch to percentage+ETA once total is known. For extraction, estimate totals from `ppa extract-emails --dry-run` counts. Accept that first-run ETAs will be rougher than subsequent runs. | Phase 13: if ETAs are consistently >2x off, remove them and show only throughput + elapsed time.                                                      |
| **Self-hosted wizard scope creep** — Making the wizard work for every possible server configuration (bare metal, Docker, existing Postgres, remote Postgres, various Linux distros, macOS) is unbounded.      | Phase 11 takes longer than estimated.                                   | Scope the wizard to two paths: (1) Docker Compose (recommended) and (2) existing Postgres (user provides DSN). Bare metal without Docker is documented but not wizarded.                                                                                 | Phase 11: if the wizard exceeds 5 weeks, ship Docker-only path and defer bare metal wizard.                                                           |
| **Documentation debt** — Writing comprehensive docs is time-consuming and unglamorous. Easy to defer.                                                                                                         | Users can't self-host without good docs; the wizard alone isn't enough. | Phase 14 is explicitly a phase, not an afterthought. Documentation is a deliverable with a Definition of Done, not a "nice to have."                                                                                                                     | Phase 14: ship docs before announcing on HN.                                                                                                          |

---

## v4 Preview

v4 takes PPA from self-hosted to consumer product. See `v4vision.md` for the full vision. Key additions:

- **Native Mac app** (Tauri) — menu bar, setup wizard, embedded Postgres, auto-updates
- **OAuth proxy service** — verified Google OAuth app, so users never create their own GCP project
- **Signed connector update feed** — automatic extractor updates pushed to all users
- **Billing** — $12/month subscription for the service layer
- **Full Rust engine rewrite** — vault scanning, materialization, FTS, and MCP server in Rust for maximum performance
- **ppa.dev** — marketing site, download page, documentation portal

v3 proves PPA works for multiple users. v4 packages it for everyone.

---

## Summary Table

| Phase  | What                             | Effort    | Depends On             | Ships to Users          |
| ------ | -------------------------------- | --------- | ---------------------- | ----------------------- |
| **10** | Vault encryption + passkey auth  | 3-4 weeks | v2 complete            | No (internal)           |
| **11** | **CLI setup wizard**             | 4-6 weeks | 10                     | **Yes — the v3 launch** |
| **12** | Docker Compose packaging         | 2-3 weeks | 11                     | Yes                     |
| **13** | Progress UX overhaul             | 2-3 weeks | v2 complete (parallel) | Yes                     |
| **14** | Self-hosting documentation       | 2-3 weeks | 11, 12                 | Yes                     |
| **15** | Connector contribution framework | 2-3 weeks | Anytime                | Yes (community)         |

**Total effort:** ~15-22 weeks of work across all phases.

**Calendar time with parallelization (2 workstreams):** ~12-16 weeks (~3-4 months) from v2 completion.

**Critical path:** 10 (4 wk) → 11 (6 wk) → 12 (3 wk) → 14 (3 wk) = **~16 weeks serial.**

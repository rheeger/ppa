# Archive MCP Server

MCP server for querying the Heeger-Friedman Family Archives vault.

The current implementation is a vault-canonical, derived-index architecture:

- Canonical data stays in the HFA markdown vault.
- `archive-mcp` builds and reads a derived relational index for fast lookup, search, and graph traversal.
- Postgres is now the primary backend for the derived index and operational plane.
- `pgvector` remains the next retrieval layer to add on top of this Postgres substrate.

## Repository

This tree is its **own git repository** (branch `main`): [github.com/rheeger/archive-mcp](https://github.com/rheeger/archive-mcp) (private). It stays a **sibling** of `hey-arnold-hfa` on disk; Arnold deploy uses `make hfa-archive-mcp-sync` from the HFA Makefile, which rsyncs this checkout. For rollout notes, record **`git rev-parse HEAD`** (and `git status` if dirty).

## Setup

```bash
cd archive-mcp
"/Users/rheeger/Code/rheeger/hey-arnold-hfa/.venv/bin/python" -m pip install -e .
 HFA_VAULT_PATH=/Users/rheeger/Archive/production/hf-archives \
HFA_LIB_PATH=/Users/rheeger/Code/rheeger/hey-arnold-hfa/skills \
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:<mapped-port>/archive \
python -m archive_mcp
```

## Local Postgres Quickstart

1. Copy `.env.pgvector.example` to `.env.pgvector`.
2. Start the local pgvector stack.
3. Bootstrap the schema.
4. Rebuild the archive index from the local vault.
5. Embed pending chunks.

The Docker local flow uses a random localhost port mapping. Use the provided `Makefile` targets instead of hardcoding a DSN; they auto-discover the mapped port.

### PostgreSQL 17 for dump and restore

The compose service uses **`pgvector/pgvector:pg17`**. Use **PG 17 client tools** for logical dumps and restores so the major version matches the server.

- **Local:** do not rely on an older host `pg_dump` (for example Homebrew `postgresql@14`). Use:

  ```bash
  make dump-seed-schema
  # or: make dump-schema DUMP_SCHEMA=your_schema ARCHIVE_DUMP_OUT=out.dump
  ```

  That runs `pg_dump` **inside** the running `archive-postgres` container (PG 17).

- **Large seed → Arnold (100+ GiB):** `pg_dump` does not print row progress; use the streaming helper so the pipe shows **bytes, rate, and optional ETA**:

  ```bash
  brew install pv   # once, for progress meter
  cd archive-mcp
  # Optional: approximate total dump size in bytes for pv ETA (example: ~135 GiB)
  export ARCHIVE_STREAM_BYTES=$((135 * 1024 * 1024 * 1024))
  export ARNOLD_HOST=arnold@192.168.50.27   # default if unset
  make dump-seed-schema-stream-arnold
  ```

  Logs use fixed vocabulary: `step 1/4` … `step 4/4`, `complete`, then remote `ls` of the dump file. Without `ARCHIVE_STREAM_BYTES`, `pv` still shows elapsed, throughput, and cumulative bytes.

- **Large seed (~100+ GiB), enough space on `/srv/hfa-secure` for dump + PGDATA:** **`make dump-seed-schema`** then **`make scp-restore-seed-arnold`** (`scripts/scp-restore-seed-arnold.sh`). Copies with **`rsync`** (resumable, no extra compression) then **`pg_restore --jobs`** (default **4**, override **`PG_RESTORE_JOBS`**) from a file on the volume — usually **much faster** than the one-shot pipe. Or one target: **`make dump-and-scp-restore-seed-arnold`**.

- **No room for a `.dump` on Arnold** (encrypted LV too small, or root `/` full): use **`make pipe-restore-seed-arnold`** (`scripts/stream-seed-pipe-restore-arnold.sh`) — local **`pg_dump` → `pv` → SSH → `docker exec -i … pg_restore`**. The script runs **`CREATE EXTENSION IF NOT EXISTS vector`** on the remote DB first (otherwise **`public.vector`** errors). You still need enough **`/srv/hfa-secure`** for **vault + final live PGDATA**; grow the LUKS volume if the restored cluster will not fit. See **`hey-arnold-hfa` runbook — Disk preflight**.

- **Arnold:** run `pg_restore` with the **same image major** as the systemd unit — invoke it via `docker exec` on `hfa-archive-postgres` (see `hey-arnold-hfa` archive rollout runbook). Skip this if you already used **pipe restore** (restore ran during the stream).

Using the local `Makefile`:

```bash
cd archive-mcp
cp .env.pgvector.example .env.pgvector
make pg-up
make bootstrap-postgres
make rebuild-indexes
make index-status
make embed-pending
```

Useful local commands:

- `make pg-up`
- `make pg-down`
- `make pg-logs`
- `make pg-psql`
- `make dump-seed-schema` / `make dump-schema` (PG 17 `pg_dump` — **`scripts/dump-schema-local.sh`**: `step 1/4`…`4/4`, **`pv`** when `brew install pv`; optional **`ARCHIVE_STREAM_BYTES`**). **Full transcript** defaults to **`logs/archive-dump.log`** (repo root under `archive-mcp/`). Tail with **`tail -f logs/archive-dump.log`**. Override: **`ARCHIVE_DUMP_LOG_FILE=/tmp/dump.log`**, or disable: **`ARCHIVE_DUMP_LOG=0`**.
- **`make dump-seed-schema-fast`** — **`PG_DUMP_FAST=1`** (`-Z0`, less CPU, bigger artifact) + **`PG_DUMP_JOBS=8`** parallel **`-Fd`** dump inside the container, then **`docker cp`** to **`archive_seed.dump.d`**. Usually much faster on a capable Mac. Override jobs: **`make dump-seed-schema-fast PG_DUMP_JOBS=12`**. Restore: **`make scp-restore-seed-arnold`** (auto-uses **`.dump.d`** if there is no **`.dump`** file).
- **`PG_DUMP_FAST=1`** alone (no parallel): **`make dump-seed-schema PG_DUMP_FAST=1`** — faster single **`.dump`** at the cost of size.
- `make dump-seed-schema-stream-arnold` (stream `archive_seed` to Arnold with `pv` + step logs; see PostgreSQL 17 section above)
- `make scp-restore-seed-arnold` / `make dump-and-scp-restore-seed-arnold` (local `.dump` → rsync → remote `pg_restore -j`). Transcript: **`logs/archive-scp-restore.log`** — run **`tail -f logs/archive-scp-restore.log`**. Disable: **`ARCHIVE_SCP_RESTORE_LOG=0`**.
- `make pipe-restore-seed-arnold` (stream straight into remote `pg_restore`; no `archive_seed.dump` on the VM)
- `make build-benchmark-sample`
- `make benchmark-rebuild`
- `make smoke`

By default the server loads the shared HFA library from `../hey-arnold-hfa/skills`.
Override with `HFA_LIB_PATH` if needed.

Optional environment variables:

- `HFA_VAULT_PATH`: canonical vault path
- `HFA_LIB_PATH`: shared HFA library path
- `ARCHIVE_INDEX_DSN`: Postgres DSN for the primary derived index backend
- `ARCHIVE_INDEX_SCHEMA`: Postgres schema name. Defaults to `archive_mcp`
- `ARCHIVE_VECTOR_DIMENSION`: vector width for future embedding rows. Defaults to `1536`
- `ARCHIVE_CHUNK_CHAR_LIMIT`: max characters per derived chunk. Defaults to `1200`
- `ARCHIVE_EMBEDDING_PROVIDER`: embedding provider name. Supported options: `hash`, `openai`
- `ARCHIVE_EMBEDDING_MODEL`: default embedding model label used for lifecycle tracking
- `ARCHIVE_EMBEDDING_VERSION`: default embedding version used for lifecycle tracking
- `ARCHIVE_EMBED_BATCH_SIZE`: chunk batch size for embedding runs. Defaults to `32`
- `ARCHIVE_EMBED_MAX_RETRIES`: retries for failed embed batches. Defaults to `3`
- `ARCHIVE_EMBED_CONCURRENCY`: concurrent embedding workers. Defaults to `4`
- `ARCHIVE_EMBED_WRITE_BATCH_SIZE`: batched upsert size for embedding rows. Defaults to `ARCHIVE_EMBED_BATCH_SIZE`
- `ARCHIVE_EMBED_PROGRESS_EVERY`: print progress every N embedded chunks. Defaults to `0`
- `ARCHIVE_EMBED_DEFER_VECTOR_INDEX`: when truthy, drop/rebuild the ANN vector index around a large backfill
- `OPENAI_API_KEY`: required when `ARCHIVE_EMBEDDING_PROVIDER=openai`
- `ARCHIVE_OPENAI_BASE_URL`: optional OpenAI-compatible base URL override. Defaults to `https://api.openai.com/v1`
- `ARCHIVE_OPENAI_TIMEOUT_SECONDS`: request timeout for OpenAI-compatible embedding calls. Defaults to `60`
- `ARCHIVE_OPENAI_MAX_RETRIES`: retries for OpenAI-compatible embedding calls. Defaults to `3`
- `ARCHIVE_USE_ARNOLD_OPENAI_KEY`: when set to `1`, `archive-mcp` can resolve `OPENAI_API_KEY` from the Arnold 1Password vault for local testing
- `ARCHIVE_OPENAI_API_KEY_OP_REF`: 1Password reference used when `ARCHIVE_USE_ARNOLD_OPENAI_KEY=1`. Defaults to `op://Arnold/OPENAI_API_KEY/credential`
- `ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_OP_REF`: 1Password reference for the service-account token used during local secret resolution. Defaults to `op://Arnold-Passkey-Gate/Service Account Auth Token: Arnold-Passkey-Gate/credential`
- `ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE`: optional path to the 1Password service-account token file used for local secret resolution
- `ARCHIVE_REBUILD_WORKERS`: worker count for rebuild scans
- `ARCHIVE_REBUILD_BATCH_SIZE`: rows per materialization batch
- `ARCHIVE_REBUILD_COMMIT_INTERVAL`: cards per load commit
- `ARCHIVE_REBUILD_PROGRESS_EVERY`: progress print interval
- `ARCHIVE_REBUILD_EXECUTOR`: `serial`, `thread`, or `process`
- `ARCHIVE_BENCHMARK_SOURCE_VAULT`: seed vault to slice for benchmark samples. Defaults to `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127`

## Benchmarking And Profiles

Use the definitive seed vault at `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` as the benchmark source.

Create a representative sample:

```bash
cd archive-mcp
make build-benchmark-sample \
  ARCHIVE_BENCHMARK_SOURCE_VAULT=/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127 \
  ARCHIVE_BENCHMARK_OUTPUT_VAULT=/Users/rheeger/Archive/tests/hf-archives-benchmark-sample \
  ARCHIVE_BENCHMARK_SAMPLE_PERCENT=1
```

Use `ARCHIVE_BENCHMARK_SAMPLE_PERCENT` for fail-fast seed slices:

- `1` for the first correctness and runtime pass
- `5` after the obvious hotspots are fixed
- `10` as the largest allowed pre-full-run benchmark slice

The sample builder now enforces a hard `10%` ceiling when `sample_percent` is set.

Run a profile benchmark against the sample or full seed:

```bash
cd archive-mcp
make benchmark-rebuild \
  HFA_VAULT_PATH=/Users/rheeger/Archive/tests/hf-archives-benchmark-sample \
  ARCHIVE_INDEX_SCHEMA=archive_bench_local \
  ARCHIVE_BENCHMARK_PROFILE=local-laptop
```

Built-in profiles:

- `local-laptop`: local multi-process scan tuned for a developer machine
- `vm-large`: more aggressive worker and batch sizing for a larger host

### Seed Link Benchmarking

The seed link enrichment flow can be benchmarked separately from embedding and retrieval work.

Recommended fail-fast workflow:

1. Build a 1% sample from the definitive seed.
2. Run `benchmark-seed-links` against that sample.
3. Inspect `elapsed_seconds`, `jobs_per_second`, `candidates_per_second`, review backlog, and quality-gate output.
4. Optimize worker count, candidate pruning, and prompt usage.
5. Repeat on `5%`.
6. Repeat on `10%`.
7. Only then run against the full seed.

Example:

```bash
cd archive-mcp
make build-benchmark-sample \
  ARCHIVE_BENCHMARK_SOURCE_VAULT=/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127 \
  ARCHIVE_BENCHMARK_OUTPUT_VAULT=/Users/rheeger/Archive/tests/hf-archives-benchmark-sample-1pct \
  ARCHIVE_BENCHMARK_SAMPLE_PERCENT=1

make benchmark-seed-links \
  HFA_VAULT_PATH=/Users/rheeger/Archive/tests/hf-archives-benchmark-sample-1pct \
  ARCHIVE_INDEX_SCHEMA=archive_seed_links_bench_1pct \
  ARCHIVE_BENCHMARK_PROFILE=local-laptop
```

Useful options:

- `ARCHIVE_SEED_LINK_INCLUDE_LLM=1` to include cheap-model adjudication in the benchmark
- `ARCHIVE_SEED_LINK_APPLY_PROMOTIONS=1` to benchmark the full promotion path instead of candidate generation and review only
- `ARCHIVE_SEED_LINK_MODULES="communicationLinker,calendarLinker"` to benchmark only a subset of modules

Direct CLI equivalents:

```bash
python -m archive_mcp build-benchmark-sample \
  --source-vault /Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127 \
  --output-vault /Users/rheeger/Archive/tests/hf-archives-benchmark-sample-5pct \
  --sample-percent 5

ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:<mapped-port>/archive \
python -m archive_mcp benchmark-seed-links \
  --vault /Users/rheeger/Archive/tests/hf-archives-benchmark-sample-5pct \
  --schema archive_seed_links_bench_5pct \
  --profile local-laptop
```

## Index Maintenance

Bootstrap the Postgres schema and pgvector-ready tables:

```bash
HFA_VAULT_PATH=/Users/rheeger/Archive/production/hf-archives \
HFA_LIB_PATH=/Users/rheeger/Code/rheeger/hey-arnold-hfa/skills \
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:<mapped-port>/archive \
python -m archive_mcp bootstrap-postgres
```

Rebuild the derived index:

```bash
HFA_VAULT_PATH=/Users/rheeger/Archive/production/hf-archives \
HFA_LIB_PATH=/Users/rheeger/Code/rheeger/hey-arnold-hfa/skills \
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:<mapped-port>/archive \
python -m archive_mcp rebuild-indexes
```

Check index status:

```bash
HFA_VAULT_PATH=/Users/rheeger/Archive/production/hf-archives \
HFA_LIB_PATH=/Users/rheeger/Code/rheeger/hey-arnold-hfa/skills \
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:<mapped-port>/archive \
python -m archive_mcp index-status
```

Embed pending chunks:

```bash
HFA_VAULT_PATH=/Users/rheeger/Archive/production/hf-archives \
HFA_LIB_PATH=/Users/rheeger/Code/rheeger/hey-arnold-hfa/skills \
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:<mapped-port>/archive \
ARCHIVE_EMBEDDING_PROVIDER=hash \
ARCHIVE_EMBEDDING_MODEL=archive-hash-dev \
ARCHIVE_EMBEDDING_VERSION=1 \
python -m archive_mcp embed-pending --limit 1000
```

Embed pending chunks with a real OpenAI-compatible provider:

```bash
HFA_VAULT_PATH=/Users/rheeger/Archive/production/hf-archives \
HFA_LIB_PATH=/Users/rheeger/Code/rheeger/hey-arnold-hfa/skills \
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:<mapped-port>/archive \
ARCHIVE_EMBEDDING_PROVIDER=openai \
ARCHIVE_EMBEDDING_MODEL=text-embedding-3-small \
ARCHIVE_EMBEDDING_VERSION=1 \
ARCHIVE_EMBED_BATCH_SIZE=512 \
ARCHIVE_EMBED_CONCURRENCY=4 \
ARCHIVE_EMBED_WRITE_BATCH_SIZE=512 \
ARCHIVE_EMBED_PROGRESS_EVERY=5000 \
OPENAI_API_KEY=your-key-here \
python -m archive_mcp embed-pending --limit 100000
```

For a full local backfill over a large archive, prefer the `make` target so the Docker DSN is resolved automatically:

```bash
cd archive-mcp
make embed-pending \
  HFA_VAULT_PATH=/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127 \
  ARCHIVE_INDEX_SCHEMA=archive_seed \
  ARCHIVE_EMBEDDING_PROVIDER=openai \
  ARCHIVE_EMBEDDING_MODEL=text-embedding-3-small \
  ARCHIVE_EMBED_LIMIT=0 \
  ARCHIVE_EMBED_BATCH_SIZE=512 \
  ARCHIVE_EMBED_CONCURRENCY=4 \
  ARCHIVE_EMBED_WRITE_BATCH_SIZE=512 \
  ARCHIVE_EMBED_PROGRESS_EVERY=5000 \
  ARCHIVE_EMBED_DEFER_VECTOR_INDEX=1
```

Notes on the upgraded embedding pipeline:

- pending chunks are now claimed with `FOR UPDATE SKIP LOCKED`, so multiple local runners can cooperate without duplicating work
- provider requests can run concurrently with bounded worker count
- embedding rows are upserted in batches instead of one row per `INSERT`
- progress can print while the backfill is running
- large backfills can temporarily defer ANN index maintenance until the end

Test local embedding generation with the Arnold-vault OpenAI key instead of copying a plaintext key into `archive-mcp`:

```bash
cd archive-mcp
make embed-pending \
  ARCHIVE_EMBEDDING_PROVIDER=openai \
  ARCHIVE_EMBEDDING_MODEL=text-embedding-3-small \
  ARCHIVE_USE_ARNOLD_OPENAI_KEY=1 \
  ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_OP_REF='op://Arnold-Passkey-Gate/Service Account Auth Token: Arnold-Passkey-Gate/credential'
```

Notes:

- this resolves the existing Arnold-vault key at `op://Arnold/OPENAI_API_KEY/credential`
- it now prefers the `Arnold-Passkey-Gate` op path for the service-account token: `op://Arnold-Passkey-Gate/Service Account Auth Token: Arnold-Passkey-Gate/credential`
- if `OPENAI_API_KEY` is already present as a real env var, `archive-mcp` uses it directly
- if `OPENAI_API_KEY` is an `op://...` reference, set `ARCHIVE_USE_ARNOLD_OPENAI_KEY=1` so `archive-mcp` knows it should resolve it via `op read`
- if the token op path is unavailable, `archive-mcp` can still fall back to `ARCHIVE_OP_SERVICE_ACCOUNT_TOKEN_FILE`
- no secret value is stored in this repo

Operational rule for this slice:

- Rebuild indexes after imports, source purges, or schema changes that affect indexed fields.
- Bootstrap Postgres before the first rebuild against a fresh database.
- Treat Postgres as the primary metadata/index plane for `archive-mcp`.
- Rebuilds now emit built-in progress updates for scan, materialize, and load stages so long runs are observable even without extra tooling.
- Rebuilds now materialize chunk rows as part of the derived index lifecycle.
- Rebuilds now stamp `chunk_schema_version` into the index metadata and chunk rows so retrieval/embedding changes are explicit.
- Use embedding status and backlog reporting to understand how many chunks are still waiting for vector generation.
- `hash` is the built-in deterministic provider for local/dev/test. It is useful for plumbing and tests, not for production semantic quality.
- `openai` is the first real embedding provider path and should be preferred for production semantic quality.
- Embedding runs now validate provider model/dimension consistency, batch work, and report failures instead of silently treating the entire run as one opaque step.
- Agent answers should still read canonical cards before responding.
- The derived index is disposable and rebuildable. The vault is the source of truth.

## Arnold VM Quickstart

The HFA repo now includes:

- a Docker-backed systemd unit: `config/systemd/hfa-archive-postgres.service`
- VM Make targets in `hey-arnold-hfa/Makefile` to sync/install `archive-mcp`, enable Postgres, bootstrap the schema, rebuild indexes, and embed chunks

Primary Arnold flow from `hey-arnold-hfa`:

```bash
make hfa-archive-bootstrap
make hfa-archive-embed-pending
```

Useful Arnold targets:

- `make hfa-archive-env`
- `make hfa-archive-pg-enable`
- `make hfa-archive-pg-status`
- `make hfa-archive-mcp-sync`
- `make hfa-archive-mcp-install`
- `make hfa-archive-bootstrap-postgres` (DB prerequisites only; use when code and `.env` are already in place, e.g. before or after a schema restore)
- `make hfa-archive-index-bootstrap` (convenience: env, mount, sync, install, configure, then bootstrap-postgres)
- `make hfa-archive-index-rebuild`
- `make hfa-archive-index-status`
- `make hfa-archive-embed-pending`

## Cursor config

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "archive": {
      "command": "python",
      "args": ["-m", "archive_mcp"],
      "env": {
        "HFA_VAULT_PATH": "/Users/rheeger/Archive/production/hf-archives",
        "HFA_LIB_PATH": "/Users/rheeger/Code/rheeger/hey-arnold-hfa/skills",
        "ARCHIVE_INDEX_DSN": "postgresql://archive:archive@localhost:<mapped-port>/archive",
        "ARCHIVE_INDEX_SCHEMA": "archive_mcp"
      }
    }
  }
}
```

## Tools

- `archive_search` — Full-text search over the derived index
- `archive_read` — Read note by path or UID
- `archive_query` — Structured query by indexed frontmatter fields
- `archive_graph` — Get linked notes from the materialized edge graph
- `archive_person` — Get person profile + linked notes
- `archive_timeline` — Notes in date range
- `archive_stats` — Indexed vault health summary
- `archive_validate` — Schema + provenance validation summary
- `archive_duplicates` — Pending dedup candidates
- `archive_rebuild_indexes` — Rebuild the derived index from canonical cards
- `archive_bootstrap_postgres` — Bootstrap Postgres schema and pgvector-ready tables
- `archive_index_status` — Report derived index metadata
- `archive_embedding_status` — Report chunk coverage and pending embeddings for a model/version
- `archive_embedding_backlog` — List chunks still waiting on embeddings for a model/version
- `archive_embed_pending` — Generate embeddings for pending chunks using the configured provider
- `archive_vector_search` — Semantic search over embedded chunks with card-level grouping, filters, and explanation metadata
- `archive_hybrid_search` — Combined lexical, semantic, graph-aware retrieval with card-level ranking and explanation metadata
- `archive_search_json` — Same lexical search as `archive_search`, JSON payload (read-only / full profiles; also `remote-read`)
- `archive_hybrid_search_json` — Hybrid rows + embedding metadata as JSON (read-only / full)
- `archive_read_many` — Batch read by JSON array of paths or UIDs (read-only / full; not in `remote-read`)
- `archive_status_json` — Index + runtime + resolved `retrieval` config as JSON (read-only / full; admin includes it too)
- `archive_retrieval_explain_json` — `archive_retrieval_explain`-style payload as JSON using the v2 explain schema (read-only / full)

## Retrieval Quality

The current retrieval layer is now more opinionated than the original plumbing slice:

- chunking is type-aware for `person`, `email_thread`, `email_message`, `imessage_thread`, and `calendar_event`
- semantic results are grouped back to the card level instead of returning raw chunk order directly
- typed edges are materialized from canonical fields in addition to wikilinks
- hybrid ranking combines exact lexical anchors, lexical score, vector similarity, graph proximity, card-type priors, recency, and deterministic/provenance bias
- vector and hybrid search support `type_filter`, `source_filter`, `people_filter`, `start_date`, and `end_date`
- vector and hybrid responses expose why a result matched with fields like `matched_by`, `score`, `chunk`, `graph_hops`, and `provenance_bias`

### Type-Aware Chunking

Current derived chunks are intentionally shaped around card semantics:

- `person`: profile, role, context, and body chunks
- `email_thread`: subject, context, thread summary, rolling conversation windows, and recency window chunks
- `email_message`: subject, snippet, context, invite context, and body chunks
- `imessage_thread`: conversation context, summary, rolling windows, and recency window chunks
- `calendar_event`: title/time, participants, description, source linkage, and body chunks

This keeps embeddings aligned with the canonical card model instead of treating every card as just a blob of text.

### Typed Edges

The derived graph now includes typed relationships such as:

- `thread_has_message`
- `message_in_thread`
- `message_has_attachment`
- `thread_has_person`
- `message_mentions_person`
- `thread_has_calendar_event`
- `message_has_calendar_event`
- `event_has_message`
- `event_has_thread`
- `event_has_person`
- `entity_has_external_id`

`archive_graph` still expands only canonical card-to-card edges. Synthetic external-ID nodes exist for ranking/navigation, not as canonical notes.

## Agent Query Guidance

Use tools in this order:

1. `archive_read` for exact UID/path reads.
2. `archive_query` for type/source/people filters.
3. `archive_search` for keyword and phrase recall.
4. `archive_vector_search` for semantic recall when the question is vague but scope is known.
5. `archive_hybrid_search` when you want exact anchors plus semantic and graph expansion (or `archive_hybrid_search_json` / `archive_retrieval_explain_json` for structured agent parsing).
6. `archive_graph` to expand neighboring evidence.
7. Read the canonical card before giving a final answer.

Do not treat derived search hits as canonical truth. They are retrieval aids.

## Testing

Run the full suite:

```bash
.venv/bin/python -m pytest -q
```

Retrieval quality coverage now includes:

- fast fake-index server tests for MCP behavior and command dispatch
- direct chunking tests for type-aware chunk materialization
- live Postgres + `pgvector` integration tests for rebuild, lexical search, vector search, hybrid ranking, filters, and typed-edge graph expansion

The live retrieval tests start a disposable `pgvector/pgvector` Docker container automatically when Docker is available. If Docker is unavailable, those tests skip cleanly instead of pretending the mocked path is enough.

## Current Slice vs Target End State

Implemented now:

- Postgres-first derived relational index for exact lookup, structured query, search, timeline, stats, and graph traversal
- Postgres bootstrap path that creates pgvector-ready `chunks` and `embeddings` tables
- rebuild-time type-aware chunk materialization for cards, threads, messages, and events
- embedding lifecycle scaffolding for per-model/version pending chunk reporting, batching, retry, and provider consistency checks
- deterministic embedding generation path for local/dev/test via the built-in `hash` provider
- card-level vector retrieval with explanation metadata
- hybrid retrieval with lexical, semantic, graph, recency, and provenance-aware ranking
- CLI and MCP rebuild/status operations
- live Postgres retrieval evaluation coverage
- vault-canonical retrieval contract

Still planned:

- richer org/date/entity filters beyond the current retrieval surface
- deeper graph-distance and relationship-strength ranking
- broader provider benchmarking such as `text-embedding-3-small` vs `text-embedding-3-large`
- richer operator tooling and agent retrieval rules

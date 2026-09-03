# Query bottleneck — Phase 1

Cutover evidence and seed benches: [serving-index-cutover.md](serving-index-cutover.md).

Live MCP, 2026-09-03. Warehouse card count from `archive_stats`: **1,383,312**.

This report is the hard stop that locks the serving-index `VectorAnn` crate and the publish RAM cap. It does **not** claim hybrid is fixed. `QueryEmbedCache` reuses a query vector; it does not skip kNN.

## What returned vs what timed out

| Tool | Result |
| --- | --- |
| `search`, `query`, `timeline`, `temporal_neighbors`, `person` | Returned |
| `hybrid_search`, `vector_search` | Postgres `statement timeout` (default 30s via `PPA_STATEMENT_TIMEOUT_MS`) |

Production serving cannot stay on pgvector. A “Postgres fallback” is a fallback to timeout.

This shell had no `PPA_INDEX_DSN` at the time of the live tools, so `EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)` was not captured against the warehouse. The capture helper is `archive_cli/query_explain.py` (`explain_sql`). Use it on a DSN-backed session to dump FTS, listing, and vector kNN plans, including canceled plans.

## Phase clocks (store / index method, not MCP transport)

Added in `archive_cli/query_timing.py` and wired through `QueryMixin.fetch_hybrid_lexical_vector` / `DefaultArchiveStore`:

- `connect_ms`, `embed_ms`, `lexical_sql_ms`, `vector_sql_ms`, `graph_sql_ms`, `fusion_ms`, `total_ms`
- Lexical and vector jobs time themselves from job start so a sibling future wait is not billed to the other branch

Listing tools returned, but we do **not** yet have a store-method p95 against the 1.38M warehouse. SLO after cutover is 250 ms p95 for listing/search/graph/person; hybrid/vector 250 ms on query-embed hit and 1000 ms on miss. Exceeding is a bug.

## Scale facts that lock VectorAnn

Cards ≠ chunks. HNSW memory is **chunk-scaled**.

At 1.38M cards, even a 1:1 chunk/embedding ratio at 1536-d float32 is:

`1_383_312 × 1536 × 4 ≈ 8.5 GiB` of raw vectors.

A 2:1 chunk ratio is ~17 GiB. An in-RAM HNSW graph on top of that does not fit a publish gate meant for a laptop/workstation serving process.

**Choice: `ivf_mmap_v1`** (local IVF-style lists + `memmap2` over `embeddings.bin`). Not `usearch`, not `hnsw_rs`. `VectorAnn` is a trait so the impl can change without leaking into `hybrid`.

- Document vectors enter only as a bulk copy from Postgres `embeddings` during maintain
- Query vectors are embedded on miss and stored in `QueryEmbedCache`
- kNN probes `sqrt(n)` lists (clamped 1–4096), `nprobe ≤ 32`

## RAM publish cap

`PPA_SERVING_INDEX_MAX_RSS_MB` default: **8192**.

Verify estimates `embedding_count × dim × 4` before `serving_index_publish`. If the estimate exceeds the cap, **do not publish**; leave `ACTIVE` on the last good generation and log `serving_index_refresh_failed`.

Metadata + Tantivy + IVF assignment lists are additional RSS. The 8 GiB gate is the first reject; raise the env only when the host can hold the mmap working set.

## Hash vs OpenAI embed; warm vs cold connect

Not measured on the live warehouse in this session (no DSN). Hash embed (`archive-hash-dev`) is local and belongs on the hybrid miss budget only as `embed_ms`. OpenAI embed is provider-bound and is why query-embed miss SLO is 1000 ms, not 250 ms. Cold `PostgresArchiveIndex._connect()` opens a new psycopg connection per op (hybrid: two threads × new connections). That connect tax is one of the reasons listing can return while vector kNN still dies at 30s — connect is not the timeout; IVFFlat kNN is.

## What this does not unlock

Caching the query vector does not skip the timed-out warehouse kNN. Do not ship `QueryEmbedCache` and call hybrid fixed. After cutover, hybrid/vector run inside `archive_crate::serving_index` against mmap IVF + Tantivy.

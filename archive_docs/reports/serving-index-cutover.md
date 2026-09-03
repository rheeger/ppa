# Serving-index cutover

Live seed, 2026-09-03. Warehouse: **1,383,312** cards, **4,336,374** chunks, **4,348,078** embeddings (`text-embedding-3-small` v1). ACTIVE generation **`1788472066779`**.

Phase 1 locked the crate and RAM cap: [query-bottleneck-phase1.md](query-bottleneck-phase1.md). This report is the cutover evidence.

## Contract

| Layer | Role |
| --- | --- |
| Vault `*.md` | Canonical card bodies. `read` opens files. |
| Postgres | Derived warehouse. COPY + GIN/IVFFlat stay for admin / embed-GC only. |
| `archive_crate::serving_index` | Only MCP/CLI query engine. |
| `QueryEmbedCache` | Reuses the query vector. Does not skip kNN. |
| `run_maintenance` | Sole publisher of `ACTIVE`. Writers append `DIRTY`. |

`PPA_ENGINE=rust` is scan / cache / materialize. It never selects the query engine.

On disk:

```text
<vault>/_meta/rust-search-index/{DIRTY,ACTIVE,generations/<id>/}
<vault>/_meta/query-embed-cache.sqlite
```

Env getters only (`archive_cli/index_config.py`): `PPA_SERVING_INDEX_PATH`, `PPA_SERVING_INDEX_MAX_RSS_MB` (default 8192; seed publish used 32768), `PPA_QUERY_EMBED_CACHE_*`.

Fail-closed: no `ACTIVE` → `ServingIndexUnavailableError` → MCP `{"error":"serving_index_unavailable"}`. Rollback is the last good `ACTIVE` generation, not Postgres FTS/pgvector. That path statement-timeouts at 30s.

`QueryMixin` (`archive_cli/index_query.py`) is the test oracle only.

## Engine

- Lexical: Tantivy (`tantivy-en-v1`)
- Vector: `ivf_mmap_v1` over `embeddings.bin` (not usearch / hnsw_rs)
- Metadata + graph CSR in process
- `temporal_neighbors` / `timeline`: activity-sorted keyset at open (`(activity_at, uid)`). Date-only queries use the day window; `activity_end_at` is interval overlap when present (exported on the next publish)
- Rank pipeline version: `2026.03.19.hfa1`

CLI: `ppa serving-index-status`, `ppa serving-index-verify`.

Publish: last step of `ppa maintain` / `rebuild-indexes`. Scripts: `archive_scripts/publish-serving-index.py`, `archive_scripts/benchmark-serving-index.py`.

## Before (Postgres warehouse, Phase 1)

| Tool | Result |
| --- | --- |
| search / query / timeline / person | Returned |
| hybrid / vector | `statement_timeout` at **30,000 ms**. Empty. |

A production “Postgres fallback” is a fallback to timeout.

## After (store-method clock, n=8)

From `logs/serving-index-bench.json` on generation `1788472066779`, plus the activity-index rebench after the neighbors keyset.

| Op | First serving-index p95 | After activity index | SLO |
| --- | --- | --- | --- |
| search | 51 ms | — | 250 pass |
| query | 17 ms | — | 250 pass |
| timeline | 95 ms | **0.05 ms** | 250 pass |
| temporal_neighbors | **413 ms fail** | **0.18 ms** | 250 pass |
| evidence | 3 ms | — | 250 pass |
| hybrid hit | 21 ms | — | 250 pass |
| vector hit | 17 ms | — | 250 pass |
| hybrid miss | 1872 ms | — | 1000 fail (OpenAI embed + cold mmap) |
| vector miss | 1239 ms | — | 1000 fail |

Hit-path hybrid is ~**1,400×** the old 30s timeout floor. Neighbors dropped ~**2,300×** after the activity-sorted scan (413 ms → 0.18 ms).

## Multi-method reconstruction (2026-09-03)

Question: who was invited to the Endaoment board dinner, when was it, and what else happened that evening?

18 store calls, engine sum **3.5 s** (first hybrid/vector still paying embed). On a warm query-embed cache the three semantic legs are ~20 ms each vs **90 s of timeouts** on Postgres.

Found: iMessages `hfa-imessage-message-45a02429fe33` / `12bb81002e78` on **2020-01-31** (Carter Wilkinson, Michael Anderson); Messinger family RSVP email `hfa-email-message-a309aa62ccbe` on **2019-11-05**. `temporal_neighbors` at `2020-01-31T18:00Z` returned in 0.25 ms.

Do not `graph` hops=2 from `People/robbie-heeger.md` — mega-hub, MCP 30s. Start from the dinner card (hops=1 was 0.3 ms).

## Ops

```bash
ppa serving-index-status
ppa serving-index-verify
python -m archive_cli --log-file logs/serving-index-publish.log
# maintain / rebuild-indexes publishes ACTIVE when DIRTY
python archive_scripts/benchmark-serving-index.py
```

Restart MCP after publish or crate rebuild so the process opens the new generation.

## Still open

- Publish is a full rebuild, not incremental-from-`DIRTY` UIDs
- `query` with sparse filters can still walk a large in-memory set
- Graph hops≥2 on high-degree person cards can exceed MCP timeout
- Hybrid/vector **miss** still exceeds 1000 ms (provider-bound embed + cold 25 GiB mmap)
- IVF lists are round-robin (`i % nlist`)
- No 1/5/10pct slice-vs-oracle parity suite yet
- `activity_end_at` is in the schema; existing seed generation needs a republish to populate interval rows

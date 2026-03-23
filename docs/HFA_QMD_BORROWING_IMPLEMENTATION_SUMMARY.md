# HFA “borrow from QMD” — what we implemented

This is a plain-language record of the work done to bring QMD-style retrieval patterns into **ppa** while keeping HFA’s canonical cards, typed projections, and security boundaries. It matches the plan’s phases in substance; a few items are intentionally shallow (called out below).

---

## Big picture

- **Canonical markdown cards stay the source of truth.** Search and explain output are derived hints only.
- **Retrieval is staged:** fetch candidates from Postgres → merge/fuse → score → optional rerank → return results.
- **The store (`DefaultArchiveStore`) owns orchestration** (query planning, multi-query fetch, rerank). The Postgres index focuses on **SQL fetch** (lexical rows, vector rows, graph neighbors).
- **Config** gained a structured `retrieval` block (planner, reranker, explain, context, candidate multiplier) plus `runtime.local_model_runtime` defaults for future local OpenAI-compatible hosts.

---

## Phase-by-phase

### 1 — Staged hybrid pipeline

- New module: `archive_mcp/retrieval_pipeline.py` (merge, fuse, rank, score breakdowns, `PIPELINE_VERSION`).
- `PostgresArchiveIndex`: `fetch_hybrid_lexical_vector`, `fetch_graph_neighbors_for_uids`; `hybrid_search` still works and delegates to the pipeline for backward compatibility.

### 2 — Query planning (deterministic first)

- New module: `archive_mcp/query_planner.py`.
- Extracts quoted phrases, emails, year/ISO dates, type/source **hints**, optional extra subqueries.
- Inferred filters merge with caller filters only when explicit filters are empty (explicit wins).
- **Guardrail:** the bare word “calendar” in a sentence (e.g. “calendar dinner”) is **not** mapped to `source_filter=calendar`, so natural language does not accidentally filter out Gmail-backed cards.

### 3 — Reranking

- New module: `archive_mcp/reranker.py`: `none`, `heuristic` (token overlap), `model` (currently same as heuristic until a real API is wired).
- Position-aware `blend_rerank_scores` with an **exact-match floor** so strong lexical anchors are not buried.

### 4 — Explainability (v2)

- `docs/RETRIEVAL_EXPLAIN_SCHEMA.md` documents the JSON shape.
- `retrieval_explain_payload_v2` in `explain.py`; `retrieval_explain` uses it when `retrieval.explain.enabled` is true.
- Includes query plan, candidate counts, fusion strategy, per-hit score components, rerank score, context, winning chunk, short “why ranked” string.

### 5 — Type-aware chunking (completed in this pass)

- **`CHUNK_SCHEMA_VERSION` is now `4`.** Rebuild + re-embed if you rely on embeddings (existing vectors still work for old chunks until you refresh).
- **Documents:** if the body has **two or more ATX markdown headings** (`#` … `######`), chunks are emitted as `document_section` (one per section) instead of a single monolithic `document_body`.
- **Meeting transcripts:** with the same heading structure, chunks include `meeting_transcript_section` per section, plus `meeting_transcript_turn` for:
  - **Otter-style** lines (`Speaker | text`), and
  - **Colon speaker blocks** (`Name: …`) when pipe lines are absent.
- **Without** that structure, behavior matches the previous path (summary, rolling windows, `meeting_transcript_body`).
- **`chunking.render_chunks_for_card`** remains the public entry; it still delegates to index materialization for a single code path.

### 6 — Context as a retrieval primitive

- `features.py`: `build_context_json`, `build_context_text`, `build_context_prefix_for_embed_row`.
- **Embeddings:** optional prefix of structured context before chunk text when `retrieval.context.include_in_embeddings` is true (JOIN in embedding batch claim).
- **Rerank / explain / results:** context fields wired through store and explain v2 where configured.

### 7 — Long-lived / local model runtime

- **No separate daemon** was added (per plan: defer until needed).
- Config surface: `runtime.local_model_runtime` (enabled, base_url, provider_kind, healthcheck_path).
- `status` / `archive_status_json` expose resolved `retrieval` and `local_model_runtime`.
- `embedding_provider.py` docstring notes alignment with OpenAI-compatible local URLs for future planner/rerank clients.

### 8 — JSON-first agent tools

- New MCP tools: `archive_search_json`, `archive_hybrid_search_json`, `archive_read_many`, `archive_status_json`, `archive_retrieval_explain_json`.
- **Tool profiles:** full + read-only get the JSON set; **remote-read** only adds `archive_search_json` (still no broad read/hybrid JSON there).
- **`archive_hybrid_search`** (string output) now goes through **the store**, so it uses the same pipeline as JSON hybrid.

---

## Files and docs touched (high level)

| Area                        | Main locations                                                                             |
| --------------------------- | ------------------------------------------------------------------------------------------ |
| Pipeline / planner / rerank | `retrieval_pipeline.py`, `query_planner.py`, `reranker.py`                                 |
| Store orchestration         | `store.py`                                                                                 |
| Index fetch + chunking      | `index_store.py`                                                                           |
| Config                      | `config.py`, `contracts.py` (`ArchiveConfig.retrieval`)                                    |
| Explain                     | `explain.py`, `RETRIEVAL_EXPLAIN_SCHEMA.md`                                                |
| Context / embed prefix      | `features.py`, embedding batch SQL in `index_store.py`                                     |
| MCP surface                 | `server.py`, `README.md`                                                                   |
| Runtime docs                | `ARCHIVE_RUNTIME_AND_CONFIG.md`                                                            |
| Tests                       | `test_retrieval_pipeline.py`, updates to `test_server.py`, `test_retrieval_integration.py` |

---

## What is _not_ fully done (honest follow-ups)

- **Model-backed query expansion** and **real cross-encoder / API rerank** — stubs only; enable `reranker.provider: model` does not yet call a remote scorer by default.
- **Dedicated long-lived HFA process** for embeddings/rerank — not built; use external OpenAI-compatible servers via config/env when you add it.
- **Git thread / email thread** chunk profiles were not redesigned in this pass (plan listed them as longer-term refinements beyond heading/speaker work on document + transcript).

---

## What you should do operationally

1. After pulling these changes, **rebuild the derived index** if you want chunk v4 materialized everywhere: `archive_rebuild_indexes` (or your usual rebuild path).
2. **Re-run embed** for your embedding model/version so new chunk boundaries and optional context prefixes are reflected in vectors.
3. Tune **`ppa.yml`** `retrieval` section if you want planner variants, reranker, or explain verbosity different from defaults.

---

## Tests

- Full `ppa` pytest suite (including live Postgres when Docker is available) should pass with `.venv/bin/python -m pytest`.
- `test_retrieval_pipeline.py` covers merges, fusion meta, planner hints, rerank blend, and heuristic ordering.

This document is the human-readable “done + caveats” summary for the QMD borrowing work in **ppa**.

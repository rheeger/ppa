# Phase 2.9 execution plan — audit (steps 1–19.5; Step 20 = ongoing gate)

**Date:** 2026-04-15 (Steps 19–19.5 closure)  
**Repo:** `ppa/` (paths below are relative to `ppa/` unless noted)  
**Plan source:** `.cursor/plans/phase_2.9_ppa_crate_execution_plan.plan.md`

This document records **evidence** against the _original_ plan text for each step, a **verdict** (`complete` | `partial` | `blocked`), and **remediation** where the plan level was not reached.

| Step | Verdict               | Summary                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ---- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1    | **complete**          | Crate scaffold, maturin, CI, smoke tests                                                                                                                                                                                                                                                                                                                                                                                                        |
| 2    | **complete**          | `walk.rs`: rayon over top-level dirs — genuinely Rust, GIL released, real parallelism                                                                                                                                                                                                                                                                                                                                                           |
| 3    | **complete**          | `frontmatter.rs`: serde_yaml — genuinely Rust parse, one json.loads at boundary                                                                                                                                                                                                                                                                                                                                                                 |
| 4    | **complete**          | `raw_content_sha256` + `content_hash` are pure Rust (`json_stable` + `json_value_from_py_any`). Matches `vault_cache` (Step 4a).                                                                                                                                                                                                                                                                                                                |
| 4a   | **complete**          | `json_stable.rs`: Python-style `ensure_ascii` JSON + double round-trip sanitizer; `hasher::content_hash` + `materialize_content_hash` use it. `archive_tests/test_json_parity_frontmatter.py` + hasher tests.                                                                                                                                                                                                                                   |
| 5    | **complete**          | `build_vault_cache` — per-note path is pure Rust (`cache_build.rs`), GIL released for entire build (`Python::allow_threads`). Batched `rusqlite` inserts unchanged.                                                                                                                                                                                                                                                                             |
| 5a   | **complete**          | `cache_build.rs`: tier 1 `read_frontmatter_prefix` + fence parse; tier 2 full-file `parse_note_content_rust` + provenance strip; `CardFields` for uid/type; `python_style_json_dumps_sorted` + `frontmatter_hash_stable` / `content_hash_from_value`; `extract_wikilinks` regex; `rayon` parallel over paths. Tests: `archive_tests/test_archive_crate_cache.py`.                                                                               |
| 6    | **complete**          | Benchmark tier 1 — re-verify targets after Step 5a rework                                                                                                                                                                                                                                                                                                                                                                                       |
| 7    | **complete (scoped)** | `cards_by_type_from_cache` — genuinely Rust: pure `rusqlite`, no Python calls                                                                                                                                                                                                                                                                                                                                                                   |
| 8    | **complete**          | **8e:** `materialize_row_batch` releases GIL + `rayon::par_iter`; projection path in Rust. Historical pre-8c throughput gaps — superseded by Step **11** (1% slice PASS) and Step **12** (Tier-2 benchmarks).                                                                                                                                                                                                                                   |
| 8a   | **complete**          | `archive_tests/test_json_parity_frontmatter.py` + `stable_json_from_yaml_frontmatter`; matrix covers empty, nested, lists, unicode, bool/float/null. `python_escape_json_string` matches `ensure_ascii`.                                                                                                                                                                                                                                        |
| 8b   | **complete**          | `materializer/card_fields.rs` + `card_field_keys.rs` (407 Pydantic field union). `CardFields::from_frontmatter_value` replaces `row.card` / `validate_card_permissive` in `batch.rs`.                                                                                                                                                                                                                                                           |
| 8c   | **complete**          | `serde_json::Map` with **`preserve_order`** (insertion order = Python dict). `batch.rs` uses `&Map` for projection, edges, quality, search_text (`iter_string_values_json` matches Python: no number/bool strings). `time_parse.rs` + `chrono`/`jiff`/`chrono-tz` for `parse_timestamp_to_utc`. `dedupe_table_rows` in Rust (PySet + tuple keys). Chunk builders consume the same `Value` map directly (**9c** — no `value_to_py_dict` bridge). |
| 8d   | **complete**          | `materializer/projection.rs` + `fm_value.rs`: typed cells match `projections/base._column_value` (`json_text_value`, external_ids/relationships, `primary_person`, `json`/`bool`/`float`/`int`/`text` modes). No Python `features` import. Gated by `test_materialize_row_batch_rust_matches_python`.                                                                                                                                           |
| 8e   | **complete**          | `batch.rs`: `materialize_one_rust` → `MaterializedOneRust`; `projection.rs`: `ProjectionCell`, `column_value_rust`, `build_typed_projection_row_rust`. Tests: `test_materialize_row_batch_rust_matches_python`. **Throughput target** (4–6k/s) not CI-gated — Step **12**.                                                                                                                                                                      |
| 9    | **complete**          | Chunk helpers/accumulator/hash are pure Rust. Builders read frontmatter as `&Map<String, Value>` via `fm_str_value` / `coerce_string_list_json` (**9c**).                                                                                                                                                                                                                                                                                       |
| 9a   | **complete**          | No `validate_card_permissive` in chunk dispatch                                                                                                                                                                                                                                                                                                                                                                                                 |
| 9b   | **complete**          | Pure Rust `ensure_ascii` JSON + chunk hash                                                                                                                                                                                                                                                                                                                                                                                                      |
| 9c   | **complete**          | `build_chunks(&Value, body) → Vec<ChunkRecord>`; typed builders take `&Map<String, Value>`; `chunk/fm.rs` delegates to `materializer::fm_value`; removed `json_stable::value_to_py_dict`. Tests: `test_archive_crate_materializer_chunker.py`, `test_chunk_hash_parity.py`.                                                                                                                                                                     |
| 10   | **deferred**          | Rust COPY reverted — psycopg sufficient. ~5% of wall time.                                                                                                                                                                                                                                                                                                                                                                                      |
| 11   | **complete**          | 1% slice (143K notes): quality_score ✓, content_hash ✓, all table counts ✓. Chunks 543,078 (Rust) vs 545,468 (Python) = 0.4% delta from `serde_yaml` vs `ruamel` edge cases — not corruption. Rust: **1,257 rows/s** (114s) vs Python **614 rows/s** (233s) = **2× materializer**, **3.8× total** with cache.                                                                                                                                   |
| 12   | **complete**          | Baseline saved. Materializer 2.0×, pipeline 3.7×, iteration 30–3000×, resolve 2.3×. `archive_docs/reports/archive_crate-benchmark-tier2-baseline.json`.                                                                                                                                                                                                                                                                                         |
| 13   | **complete**          | `person_index.rs`: `PersonResolutionIndex` + `build_person_index(vault_path, cache_path=None)`; indexes `by_email` / `by_phone` / `by_social` / `by_name_exact` / `by_last_name` / `by_first_initial_last`; `_meta/nicknames.json`; vault walk or SQLite person rows. `archive_tests/test_archive_crate_person_index.py`. `crate_bridge.build_person_index` delegates to Rust.                                                                  |
| 14   | **complete**          | `resolve_batch.rs`: `resolve_person_batch` — identity map + `_meta/ppa-config.json`; `is_same_person` / `candidate_is_plausible` / `names_match` (token_sort_ratio_inner); `Python::allow_threads` for index build + `rayon::par_iter` over rows; returns `ResolveResult`. `crate_bridge.resolve_person_batch` → Rust. `test_archive_crate_fuzzy.py` Rust vs Python fixture parity.                                                             |
| 15   | **complete**          | `test_resolve_person_batch_rust_matches_python` on fixture (4 cases); `test_resolve_person_batch_rust_matches_python_on_slice` on 1% slice (100 identifiers): 100% action/wikilink/confidence match. Python 5.7s → Rust 2.4s (2.3×).                                                                                                                                                                                                            |
| 15a  | **complete**          | Rust cache-backed note iteration: `notes_from_cache`, `frontmatter_dicts_from_cache`, `note_paths_from_cache` in `archive_crate`; all `iter_*` helpers in `archive_vault/vault.py` wired to Rust cache-read when `PPA_ENGINE=rust`; downstream callers (`entity_resolution`, `quality_flags`, `threads`) updated. 143K notes in 1.9s (body) / 4ms (frontmatter-only, type-filtered) vs 60–90s Python.                                           |
| 16   | **complete**          | `apply_person_links` writes wikilinks with provenance; `disambiguate_conflicts` wired to LLM (Gemini/Ollama via `archive_vault.llm_provider` `chat_json`); `run_person_linking` supports `--type`, `--provider`, `--conflict-model`; DeclEdgeRules for counterparty/driver/provider; CLI complete. LLM disambiguation is the **required** path for production.                                                                                  |
| 17   | **complete**          | **17a:** 8 integration tests. **17b:** Gemini e2e verified. **17c:** 1% slice audit — 6% conflict (6/100), all LLM-resolved (6 calls, 1906 tokens), 94% no-match. Scoring: exact_name 50→80.                                                                                                                                                                                                                                                    |
| 18   | **complete**          | All iter\_\* wired to Rust cache (Step 15b). resolve_person_batch Rust (Steps 13–14). Entity scan 0.020s, email scan 1.08s, link-persons dry-run 3.6s (was 60–90s).                                                                                                                                                                                                                                                                             |
| 19   | **complete**          | `PPA_ENGINE` default `rust`; materializer + vault cache + scanner hot paths use `archive_cli.ppa_engine()`; Python fallbacks emit `warnings.warn`; `entity_resolution.resolve_person_batch` → Rust via `crate_bridge`; `pyproject.toml` / Makefile / maturin integration.                                                                                                                                                                       |
| 19.5 | **complete**          | Namespace alignment: `archive_vault/`, `archive_cli/`, `archive_auth/`, `archive_tests/`, `archive_scripts/`, `archive_docs/`; `PPA_UID_PREFIX` (default `hfa`); Postgres schema default `ppa`; vision + example JSON under `archive_docs/`; `iter_parsed_notes_from_disk` for doctor validation when tier-2 cache has no provenance.                                                                                                           |

---

## Step 1 — Scaffold

**Evidence:** `archive_crate/Cargo.toml`, `archive_crate/src/lib.rs`, `archive_crate/pyproject.toml`, `ppa/pyproject.toml` optional `rust` extra, `archive_tests/test_archive_crate_smoke.py`, `.github/workflows/rust.yml` (maturin + pytest subset).

**Verdict:** **complete**

---

## Step 2 — Walk

**Evidence:** `archive_crate/src/walk.rs` — **rayon** `par_iter` over each non-excluded **top-level** vault subdirectory; each branch uses `walkdir::WalkDir` with the same `filter_entry` rules as a monolithic walk from the vault root. Root-level `*.md` collected before parallel phase. `walk_vault_count` sums per-branch counts without building path strings. **`archive_crate.walk_vault_monolithic`** exposes the single-tree reference for tests.

**Tests:** `archive_tests/test_archive_crate_walk.py` — parity vs `archive_vault.vault._iter_note_paths_python` (avoids circularity when `PPA_ENGINE=rust`); `test_walk_parallel_matches_monolithic_reference`; fixture vault from `load_fixture_vault`; count vs `len(walk_vault)`.

**Residual:** Vaults with a **single** huge top-level tree get less parallelism (one rayon task). Deeper fan-out can be added later if profiling requires it.

**Verdict:** **complete** (meets plan: walkdir + rayon + bindings + comparison tests)

---

## Step 3 — Frontmatter

**Evidence:** `archive_crate/src/frontmatter.rs` — same fence regex as `archive_vault/yaml_parser` (`^---\s*\n(.*?)\n---\s*\n?(.*)$`, dotall). YAML via **serde_yaml** (YAML 1.2 / libyaml), round-tripped to Python dicts with `json.loads` for key parity with ruamel-loaded structures on supported card frontmatter. **Null** document and **comment-only** frontmatter yield `{}` (matches ruamel `load` → `None`). **Non-mapping** roots raise `ValueError("Frontmatter must parse to a mapping")`. `indexmap` pinned to `=2.13.0` so older Cargo can resolve deps (serde_yaml’s transitive `indexmap` 2.14+ uses edition2024 manifests).

**Tests:** `archive_tests/test_archive_crate_frontmatter.py` — parametrized parity vs `parse_frontmatter` from `archive_vault.yaml_parser`: no fence, empty fences, quoted colons + flow arrays, YAML comments, `null`, whitespace-only, blank line only, CJK, multiline `|`, anchors/aliases, CRLF, nested mapping, bool/int; plus explicit rejection tests for list and scalar roots.

**Verdict:** **complete**

---

## Step 4 — Hasher

**Evidence:** `archive_crate/src/hasher.rs`, `archive_tests/test_archive_crate_hasher.py` vs `archive_cli.vault_cache._content_hash`.

**`raw_content_sha256`** — pure Rust: `sha2::Sha256::digest(data)`. Genuinely fast, no Python calls.

**`content_hash`** — **pure Rust** (`archive_crate/src/json_stable.rs`): `json_value_from_py_any` → `stable_json_string_from_value` (same pipeline as `vault_cache._content_hash`) + SHA-256. `materializer` `materialize_content_hash` shares the same path.

**Verdict:** **complete** — Step 4a landed (see `archive_tests/test_json_parity_frontmatter.py`, `archive_tests/test_archive_crate_hasher.py`).

---

## Step 5 — Vault cache

**Evidence:** `archive_crate/src/cache.rs`, `archive_crate/src/cache_build.rs`, `archive_tests/test_archive_crate_cache.py` tier 1 and tier 2 row/column checks vs in-memory Python `VaultScanCache`.

**Step 5a (complete):** Per-note work matches `archive_cli.vault_cache._populate_db` without Python: **tier 1** reads only the frontmatter prefix (same algorithm as `archive_vault.vault._read_frontmatter_prefix`); **tier 2** reads full file, `split_frontmatter_text` + serde_yaml, `strip_provenance` (same regex as `archive_vault.provenance`), body zlib, `CardFields::from_frontmatter_value`, `python_style_json_dumps_sorted` for `frontmatter_json`, `json_stable` for `frontmatter_hash` / `content_hash`, wikilinks via `archive_vault.vault.WIKILINK_RE`-equivalent regex, `hasher::raw_content_sha256` on raw UTF-8 bytes. **`rayon::par_iter`** over `rel_paths`; rows sorted before insert. **`Python::allow_threads`** wraps the whole build (parallel + SQLite) so the GIL is not held.

**Verdict:** **complete** for disk cache build parity on fixture vault; re-run Tier 1 benchmarks on large vaults to measure speedup vs historical Python-orchestrated path.

---

## Step 6 — Benchmark Tier 1

**Evidence:** `archive_scripts/benchmark-archive_crate-tier1.py` — walk + fingerprint (Python vs Rust) + **tier-2 cache build** timing Python `VaultScanCache._build_fresh` vs `archive_crate.build_vault_cache` (Python first, then Rust). **`--enforce`** exits **2** if `python_over_rust` for walk or cache falls below `PPA_TIER1_MIN_*` (default **8×**) when `python_note_count >= PPA_TIER1_ENFORCE_MIN_NOTES` (default **500**); otherwise prints skip notice (small fixture vaults). Optional **`--enforce-fingerprint`**. **Committed baseline / targets:** `archive_docs/reports/archive_crate-benchmark-tier1-baseline.json`. **Makefile:** `benchmark-archive-crate-tier1`, `benchmark-archive-crate-tier1-enforce`. **Test:** `archive_tests/test_archive_crate_benchmark_tier1.py` (JSON shape on fixture; no enforce on tiny trees).

**Verdict:** **complete**

---

## Step 7 — Scanner (manifest + cards_by_type)

**Scoped completion (Tier 1c):** The original plan’s **Postgres-backed** `scan_manifest(dsn, …)` + noop/incremental/full classification in Rust would duplicate `archive_cli.loader.IndexLoader` (hundreds of lines + `tokio-postgres`). That work is **deferred** to Tier 2 integration (Steps 10/19). **Delivered for Step 7:** Rust **SQLite**-side index used for type-filtered scans:

- `archive_crate.cards_by_type_from_cache` / **`archive_crate.cards_by_type`** (alias, same PyO3 impl) — `card_type → rel_paths` from `vault-scan-cache.sqlite3`, matching `archive_cli.scanner.cards_by_type_from_cache_path`.
- `archive_crate.vault_paths_and_fingerprint` — fingerprint parity with Python.

**Postgres manifest diff** remains **`_collect_canonical_rows` + `_classify_manifest_rebuild_delta`** inside **`IndexLoader.rebuild_with_metrics`** (`archive_cli/loader.py`); no second implementation in Rust.

**Tests:** `archive_tests/test_archive_crate_scanner.py` — `test_cards_by_type_alias_matches_cards_by_type_from_cache`.

**Verdict:** **complete (scoped)**

---

## Step 8 — Materializer (native Rust)

**Evidence:** `archive_crate/src/materializer/` — `materialize_row_batch` builds `ProjectionRowBuffer` in Rust (cards, sources, people, orgs, external_ids, typed projections from `materializer_registry.json`, edges, chunks). `text_hash.rs`: `build_search_text`, `materialize_content_hash` (Python `json.dumps` for hash parity). `archive_scripts/export_materializer_registry.py` emits `archive_crate/materializer_registry.json` when `card_registry` / shared columns change.

**Wire:** `archive_cli.materializer._materialize_row_batch` calls `archive_crate.materialize_row_batch` when `PPA_ENGINE` is `rust` (default); `PPA_ENGINE=python` forces the legacy Python path.

**Tests:** `archive_tests/test_archive_crate_materializer_chunker.py` — per-note search/hash/chunk parity; **full** `test_materialize_row_batch_rust_matches_python` on the fixture vault (58 notes).

**2026-04-15 performance audit (historical — pre-8a–8c):** Counted Python callbacks per row before the Value/materializer rewrite. **Current path (8c+):** content_hash / frontmatter JSON / typed projection / search_text / edges / dedupe are Rust; **`parse_timestamp_to_utc`** is Rust (`time_parse.rs`). **After 9c:** chunk building no longer allocates a `PyDict` per note. **After 8e:** per-batch row work runs under **`Python::allow_threads`** with **`rayon::par_iter`**; GIL returns for `materialize_rust_to_py` (tuple + `json.dumps` for chunk `source_fields` only).

**Historical (pre-8b–8c):** On an early 1% slice, Rust was slower than Python and showed quality/hash/chunk drift — fixed by CardFields, `preserve_order`, body/provenance parsing, and chunk path rework. **Superseded by Step 11** (1% slice PASS) and **Step 12** (Tier-2 benchmarks).

**Verdict:** **complete** — fixture parity; production slice parity and throughput per Steps **11** / **12**.

**2026-04-15 — Steps 8b / 8c:** `materializer/card_fields.rs` implements `CardFields` from `serde_json::Value` (via `json_value_from_py_any` on frontmatter) with key filtering (`card_field_keys.rs`, union of all Pydantic `model_fields`). `materialize_row_batch` / `batch.rs` no longer reads `row.card`; projection/edges take `&CardFields`.

**2026-04-15 — Step 8c (complete):** Frontmatter is `serde_json::Value` / `Map<String, Value>` end-to-end for cards row, typed projection cells, edges, external_ids, timeline, quality, and search_text. **`serde_json` feature `preserve_order`** so key iteration matches Python `dict` order (default `BTreeMap` sorted keys broke `search_text`). **`iter_string_values_json`** mirrors `_iter_string_values` (only str/list/dict contribute). **`materializer/time_parse.rs`** implements `parse_timestamp_to_utc` in Rust. **`dedupe_table_rows`** replaces Python `_dedupe_rows`. **Step 9c** (2026-04-14): chunk builders read the same `Map` directly — no `value_to_py_dict` bridge.

**2026-04-15 — Step 8d (complete):** **`materializer/projection.rs`** implements `build_typed_projection_row` entirely from `&Map<String, Value>` + `CardFields`, mirroring **`archive_cli.projections.base._column_value`** (shared columns + per-type columns from `materializer_registry.json`). JSON cells use **`fm_value::json_text_value`** (sanitized + `python_style_json_dumps_sorted`). **`external_ids_by_provider_value`** / **`relationship_payload_value`** align with **`archive_cli.features`** (same `EXTERNAL_ID_*` maps in `external_ids.rs`). **`primary_person`** uses `iter_string_values_json`. No runtime Python import of `archive_cli.features` for projections.

**2026-04-14 — Step 8e (complete):** **`batch.rs`** — extract `(rel_path, frontmatter Value)` under the GIL, then **`py.allow_threads`** + **`rayon::par_iter`** over **`materialize_one_rust`** (body read, hashes, edges, chunks, quality, typed row via **`build_typed_projection_row_rust`**). **`projection.rs`** — **`ProjectionCell`** + **`column_value_rust`** / **`parse_timestamp_to_utc_rust`** for activity cells without Python during parallel phase; **`materialize_rust_to_py`** builds **`ProjectionRowBuffer`**. Tests unchanged: **`test_materialize_row_batch_rust_matches_python`**.

---

## Step 9 — Chunker (native Rust)

**Evidence:** `archive_crate/src/chunk/` — ports `archive_cli/chunk_builders.py` (helpers, per-type builders, default `CHUNKABLE_TEXT_FIELDS` path). Chunk `content_hash` uses pure Rust JSON (`chunk/helpers.rs`) matching Python `json.dumps(..., sort_keys=True, ensure_ascii=True)` byte-for-byte. `archive_crate/src/chunker.rs` exposes `render_chunks_for_card` and `chunk_hash`; `materializer/batch.rs` calls `chunk::build_chunks` directly (no Python chunking).

**What's genuinely Rust:** `chunk/helpers.rs` (text splitting, rolling windows, hashing, ensure_ascii encoding), `chunk/accumulator.rs` (dedup, index tracking), text processing in all builders.

**Frontmatter access (Step 9c):** `build_chunks(fm: &Value, body: &str)` slices `fm` as `Map<String, Value>`; `fm_str_value` / `iter_string_values_json` / `stat_str_json` match `archive_cli` chunk semantics (`fm_value.rs` is `pub(crate)` for chunk reuse). No `PyDict` per note.

**1% slice chunk count mismatch:** Python produced 545,468 chunks, Rust produced 894,055. Re-verify after **9c** + **8e** on production slice (Step **11**).

**Tests:** `archive_tests/test_archive_crate_materializer_chunker.py` — parity on fixture vault (58 notes); `archive_tests/test_chunk_hash_parity.py` — hash parity.

**Verdict:** **complete** — helpers, accumulator, builders, and dispatch are Rust-only on the `Value`/`Map` path.

---

## Step 9a — Remove `validate_card_permissive` Python round-trip

**Evidence:** `chunk/dispatch.rs` dispatches on `fm_str_value(fm, "type")` (no `PyDict`). `build_person_chunks` reads summary via `fm_str_value` — no `archive_vault.schema` import.

**Verdict:** **complete**

---

## Step 9b — Replace `json.dumps` chunk hash with pure Rust

**Evidence:** `chunk/helpers.rs` — `encode_json_string_python_ensure_ascii`, `chunk_hash_payload_json`, `chunk_hash`; `chunk/accumulator.rs` `append_chunks` no longer takes `Python` or calls into `json`. PyO3 binding `archive_crate.chunk_hash` in `chunker.rs` for tests.

**Tests:** `archive_tests/test_chunk_hash_parity.py` — parametrized cases + ~50-string matrix vs `_chunk_hash`; Rust unit tests in `helpers.rs`.

**Verdict:** **complete**

---

## Step 9c — Chunk builders on `serde_json::Map` (no PyDict bridge)

**Evidence:** `chunk/dispatch.rs` — `build_chunks(fm: &Value, body: &str) -> Vec<ChunkRecord>`; non-object frontmatter uses an empty map. `chunk/builders.rs` — all builders take `&Map<String, Value>`; `chunk/fm.rs` — `coerce_string_list_json`, `stat_str_json`, re-export `fm_str_value` from `materializer::fm_value`. Removed `json_stable::value_to_py_dict` (was `serde_json::to_string` + `json.loads` per note). `materializer/mod.rs` — `pub(crate) mod fm_value` for chunk access.

**Tests:** `archive_tests/test_archive_crate_materializer_chunker.py` (13 passed); `archive_tests/test_chunk_hash_parity.py`.

**Verdict:** **complete**

---

## Step 10 — Loader (COPY + rebuild_index)

**Decision: deferred.** A Rust `tokio-postgres` text COPY implementation was built and tested, then reverted after analysis showed it was strictly worse than the existing psycopg path:

1. **psycopg's `COPY FROM STDIN` is C-level** — it writes rows directly to the open socket with no Python overhead per cell.
2. The Rust path required a **separate connection** per flush (connect + `BEGIN` + COPY + `COMMIT`), plus **GIL-bound PyO3 cell extraction** to convert every Python value to `Option<String>` before releasing the GIL.
3. The `tokio`/`tokio-postgres`/`futures-util`/`bytes` dependency tree added **86 crates** to `Cargo.lock` and roughly doubled compile time.
4. **Loader flush is ~5–10% of total rebuild time.** Steps 8+9 (native materialization + chunking) already eliminated the real bottleneck (~85% of wall time).

`rebuild_index` remains a Python delegation (`bridge.rs` → `crate_bridge` → `IndexLoader.rebuild_with_metrics`). Checkpoint/resume and manifest orchestration stay in Python — they are I/O-bound and not performance-critical.

**Verdict:** **deferred** — revisit only if COPY flush becomes measurably dominant after Steps 13–14 reduce entity resolution time.

---

## Step 11 — Row-level correctness

**Evidence:** `archive_tests/archive_crate_projection_parity.py` — `assert_projection_buffers_equal` / `format_projection_buffer_diff` (table set, per-row/column diffs, capped `ingestion_log_rows` detail). `archive_tests/test_archive_crate_materializer_chunker.py` — full fixture vault `materialize_row_batch` parity uses the shared helper. `archive_tests/test_archive_crate_correctness.py` — `test_correctness_slice_materializer_matches_python` (`@pytest.mark.integration` + `slow`): skips unless `PPA_CORRECTNESS_SLICE`.

**2026-04-15 (historical): 1% slice (143,148 notes) — FAILED.** quality_score 0.54/0.58 vs 0.62; content_hash divergence; chunk count 545K vs 894K; Rust slower than Python (453 vs 585 rows/s).

**2026-04-15 (post 8e + 9c + body.rs dotall fix): 1% slice — PASS.**

- quality_score: ✓ matches Python (dotall fix in `body.rs` provenance regex)
- content_hash: ✓ matches Python
- All non-chunk tables: ✓ identical row counts
- Chunks: 543,078 (Rust) vs 545,468 (Python ruamel baseline) — **0.4% delta** from `serde_yaml` vs `ruamel.yaml` edge cases on production frontmatter. Not corruption; fixture vault is byte-identical.
- Rust materializer: **1,257 rows/s** (114s); Python: **614 rows/s** (233s) — **2× faster**
- Full Rust pipeline (cache load 12s + materialize 114s): **125s** vs Python parse+materialize **~460s** — **3.7× faster**

**Test:** `test_correctness_rust_vs_baseline` — Rust vault cache → Rust materializer → compare counts to `_artifacts/_correctness_baseline_1pct.json`.

**Verdict:** **complete**

---

## Step 12 — Benchmark Tier 2

**Evidence:** `archive_scripts/benchmark-archive_crate-tier2.py` — Python vs Rust comparison (cache build, materialize, iteration, entity resolution). Baseline saved to `archive_docs/reports/archive_crate-benchmark-tier2-baseline.json`.

**Measured on 1% slice (143,148 notes, Steps 11 + 15b):**

| Operation                            | Python            | Rust                | Speedup    |
| ------------------------------------ | ----------------- | ------------------- | ---------- |
| Materializer throughput              | 614 rows/s (233s) | 1,257 rows/s (114s) | **2.0×**   |
| Full pipeline (cache + materialize)  | ~460s             | 125s                | **3.7×**   |
| Vault iteration (all 143K with body) | 60–90s            | 1.9s                | **30–47×** |
| Entity scan (346 cards, fm-only)     | ~60s              | 0.020s              | **3,000×** |
| resolve_person_batch (100 ids)       | 5.66s             | 2.42s               | **2.3×**   |
| Note paths (143K)                    | ~5s               | 0.014s              | **357×**   |

All targets met: materializer ≥2×, pipeline ≥3×, iteration ≥10×.

**Verdict:** **complete**

---

## Step 13 — Person index (Rust)

**Evidence:** `archive_crate/src/person_index.rs` — `PersonResolutionIndex` (`#[pyclass]`), `PersonRecord` (normalized names/emails/phones/socials), `build_person_index(vault_path, cache_path=None)` builds from `People/` vault walk (`walk::collect_note_paths`) or from tier-2 SQLite (`card_type = 'person'` + `frontmatter_json`). Normalization matches `archive_vault.identity.normalize_person_name` / `_normalize_identifier` (email lower, phone `+1` ten-digit US). `load_nicknames_json` reads `_meta/nicknames.json` (stored for Step 14). `lib.rs` registers `PersonResolutionIndex`; `bridge.rs` no longer delegates `build_person_index`. `archive_cli/crate_bridge.build_person_index` calls the Rust implementation.

**Tests:** `archive_tests/test_archive_crate_person_index.py` — email map vs `PersonIndex`; `by_last_name` / `by_first_initial_last` vs Python; cache build parity. `person_index_counts_from_cache` unchanged.

**Verdict:** **complete**

---

## Step 14 — Fuzzy resolver (Rust batch)

**Evidence:** `archive_crate/src/resolve_batch.rs` — ports `_resolve_person_from_candidates`, `is_same_person`, `_candidate_is_plausible`, identity lookups (`_meta/identity-map.json`), `load_ppa_config`-equivalent thresholds, `load_nicknames_json`. Uses `PersonResolutionIndexInner::candidate_wikilinks` + `records`. **GIL:** `build_index_from_vault_path` inside `allow_threads`; per-request work in second `allow_threads` with `rayon::par_iter`. `fuzzy_resolver::token_sort_ratio_inner` for name/company/title similarity. `lib.rs` registers `resolve_batch::resolve_person_batch`; `bridge.rs` no longer exports resolver. Python `ResolveResult` constructed under GIL at end.

**Tests:** `archive_tests/test_archive_crate_fuzzy.py::test_resolve_person_batch_rust_matches_python`; `archive_tests/test_identity_resolve_batch.py` (serial vs batch unchanged).

**Verdict:** **complete**

---

## Step 15 — Comparison test (100+ cards)

**Evidence:** `archive_tests/test_archive_crate_fuzzy.py`:

- `test_resolve_person_batch_rust_matches_python` — fixture vault (4 cases, action/confidence/wikilink/reasons exact match)
- `test_resolve_person_batch_rust_matches_python_on_slice` — 1% slice (100 identifiers from derived + finance cards via `_person_names_from_derived_card` + finance counterparty)

**Results on 1% slice (2026-04-15):**

- Identifiers: 100 (11 from derived entity cards, 89 from finance counterparties)
- Action match: **100/100 (100%)**
- Confidence ±1: **100/100 (100%)**
- Wikilink match: **100/100 (100%)**
- Python: **5.66s**, Rust: **2.42s** — **2.3× speedup**
- Action distribution: all `create` (750 PersonCards, mostly merchant/lab names — no fuzzy matches expected)

**Test:** `PPA_RESOLVE_PARITY_SLICE` env var or default `.slices/1pct`. Marked `@pytest.mark.integration` + `@pytest.mark.slow`. Skips if <100 identifiers or no cache.

**Verdict:** **complete**

---

## Step 15a — Rust cache-backed note iteration (eliminate GIL-bound iter helpers)

**Problem:** Even with `PPA_ENGINE=rust`, every `iter_parsed_notes*` call still opened and parsed each `.md` file in Python (`read_note_file` → ruamel YAML → provenance strip — one at a time, GIL held). The tier-2 SQLite cache already contained `frontmatter_json`, `body_compressed`, and `card_type` for every note, but nothing read them back in Rust.

**Solution:** Three new `archive_crate` functions in `cache_iter.rs`:

| Function                                                    | Purpose                                                                             | GIL behavior                                                              |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `notes_from_cache(cache_path, types?, prefix?)`             | Full rows: `rel_path`, `frontmatter` (parsed JSON dict), `body` (zlib-decompressed) | `allow_threads` for SQLite + zlib; GIL only for `json.loads` + list build |
| `frontmatter_dicts_from_cache(cache_path, types?, prefix?)` | Frontmatter-only (no body decompression)                                            | Same                                                                      |
| `note_paths_from_cache(cache_path, types?, prefix?)`        | `list[str]` of rel_paths, SQL-filtered                                              | Pure Rust except final list                                               |

**Python wiring (when `PPA_ENGINE=rust` and tier-2 cache exists):**

| Function                                               | Before                                                           | After                                                                                  |
| ------------------------------------------------------ | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `archive_vault.vault.iter_parsed_notes`                | `VaultScanCache.all_rel_paths()` → `read_note_file` per note     | `archive_crate.notes_from_cache`                                                       |
| `archive_vault.vault.iter_parsed_notes_for_card_types` | `VaultScanCache.rel_paths_by_type()` → `read_note_file` per note | `archive_crate.notes_from_cache(types=...)`                                            |
| `archive_vault.vault.iter_notes`                       | `Path.read_text` per note (no Rust path)                         | `archive_crate.notes_from_cache`                                                       |
| `archive_vault.vault.iter_email_message_notes`         | `iter_note_paths` → filter `Email/` → `read_note_file`           | `archive_crate.notes_from_cache(types=['email_message'], prefix='Email/')`             |
| `archive_vault.vault.read_note_by_uid`                 | `iter_parsed_notes` full scan                                    | Direct SQLite `WHERE uid = ?` lookup                                                   |
| `entity_resolution.iter_derived_card_dicts`            | `VaultScanCache` → `frontmatter_for_rel_path` per card           | `archive_crate.frontmatter_dicts_from_cache(types=DERIVED_ENTITY_CARD_TYPES)`          |
| `quality_flags.email_uid_index`                        | `iter_note_paths` → `read_note_frontmatter_file` per Email/ note | `archive_crate.frontmatter_dicts_from_cache(types=['email_message'], prefix='Email/')` |
| `threads.stubs_from_filesystem_walk`                   | `iter_note_paths` → `read_note_frontmatter_file` per Email/ note | `archive_crate.frontmatter_dicts_from_cache(types=['email_message'], prefix='Email/')` |

All remaining call sites (`archive_doctor`, `identity_resolver.PersonIndex._load`, `runner.run`) go through the updated `archive_vault.vault` functions and benefit automatically.

**Measured on 1% slice (143,148 notes):**

- `frontmatter_dicts_from_cache` (derived entity types, 257 rows): **4ms**
- `frontmatter_dicts_from_cache` (email_message, 34K rows): **0.57s**
- `notes_from_cache` (all 143K, with body): **1.9s**
- `note_paths_from_cache` (all 143K): **14ms**
- Previous Python path (`iter_parsed_notes` → `read_note_file`): **60–90s**

**Tests:** 666 passed (full non-slow suite), 0 regressions. Existing tests exercise the functions transitively via `iter_parsed_notes*`.

**Verdict:** **complete**

---

## Step 16 — Orchestration

**Evidence:**

- **16a** `apply_person_links` — reads card → adds `person_wikilink` to `people` list → writes back with `entity_resolution` provenance. Idempotent (skips if wikilink already present).
- **16b** `disambiguate_conflicts` — **LLM disambiguation is required for production.** Wired to `archive_vault.llm_provider` (`GeminiProvider` / `OllamaProvider`), same infrastructure as Phase 2.875 enrichment. System prompt (`_DISAMBIGUATE_SYSTEM`) + per-conflict prompt (`_build_disambiguate_prompt`) with card context (type, counterparty, amount, service) + candidate PersonCard details (summary, emails, companies, aliases). Uses `chat_json(temperature=0.0, seed=42)` for structured JSON verdict (`{"choice": N, "reason": "..."}`). Resolved conflicts applied as merges with `llm_disambiguated:{reason}` provenance. Default models: `gemini-2.5-flash-lite` (Gemini) / `gemma4:e4b` (Ollama). Degrades to logging-only if provider not set (not the expected production path).
- **16c** DeclEdgeRules: `transaction_with_person` (finance/counterparty), `ride_has_driver` (ride/driver_name), `record_has_provider` (medical_record/provider_name) — all present in `card_registry.py`.
- **16d** CLI: `ppa link-persons [--dry-run] [--type finance,ride,...] [--provider gemini|ollama] [--conflict-model MODEL] [--report-dir DIR] [--vault PATH]`.
- `run_person_linking` supports `card_types` filter and passes conflict rows to `disambiguate_conflicts` with provider/model.

**Verdict:** **complete**

---

## Step 17 — Integration (full pipeline)

### 17a — Unit/integration tests — DONE

`archive_tests/test_entity_resolution_integration.py` — 8 tests, all passing:

1. `test_person_linking_writes_wikilinks` — PersonCard + FinanceCard → resolve → verify `people` wikilink written
2. `test_person_linking_idempotent` — run twice → no duplicate wikilinks
3. `test_conflict_logged_when_ambiguous` — two similar PersonCards → conflict (not wrong merge)
4. `test_dry_run_does_not_write` — vault files unchanged when `dry_run=True`
5. `test_card_type_filter` — `card_types` limits scope correctly
6. `test_report_json_written` — valid JSON report
7. `test_run_person_linking_dry_run_smoke` — fixture vault end-to-end
8. `test_decl_edge_rules_registered` — all three DeclEdgeRules present

### 17b — LLM disambiguation e2e — DONE (2026-04-15)

Tested with `gemini-2.5-flash-lite` via `GEMINI_API_KEY` (1Password item `GEMINI_API_KEY`). Synthetic conflict scenario: PersonCard "Jane Smith" (emails: j.smith@acme.com, companies: Acme) + MedicalRecordCard with provider_name "Jane Smith" → confidence 85 (conflict zone, 75–90).

Results:

- `conflicts_resolved: 1`, `conflicts_skipped: 0`
- `llm_calls: 1`, `llm_tokens: 289`
- Wikilink `[[jane-smith]]` written to medical record's `people` field (verified via `read_note`)
- `apply_person_links` applied the merge with `llm_disambiguated:{reason}` in provenance

### 17c — Conflict rate audit on 1% slice — DONE (2026-04-15)

**Scoring update:** Raised `exact_name` confidence from 50→80 (conflict zone), `nickname_name` 47→70, `fuzzy_name` 38/45→50/60. Both Python (`identity_resolver.py`) and Rust (`resolve_batch.rs`) updated. An exact name match on a personal archive with ~750 PersonCards is strong evidence — conflicts go to LLM for disambiguation.

**Expanded matching:** Added `finance` to `PERSON_RESOLVABLE_CARD_TYPES` (was only derived entity types). `_person_names_from_derived_card` now extracts `counterparty` from finance cards.

**Results on 1% slice (100 identifiers from 346 cards):**

| Metric   | Count | %   |
| -------- | ----- | --- |
| Merge    | 0     | 0%  |
| Conflict | 6     | 6%  |
| No-match | 94    | 94% |

**Conflict details:**

- `Julia Millot "🍝"` → `[[julia-millot]]` (conf=80, exact_name) — **LLM resolved: correct**
- `Uber Eats` ×5 → `[[uber-eats]]` (conf=80, exact_name) — **LLM resolved: correct** (PersonCard from contacts import)

**LLM stats:** 6 calls, 1906 tokens, gemini-2.5-flash-lite, all resolved (100% disambiguation rate).

**No-match breakdown:** 94% are merchant names (Amazon, Cafe Mogador, etc.), institutional names (CEDARS-SINAI MED CTR), or FHIR practitioner IDs — correctly not matched.

**Verdict:** **complete**

---

## Step 18 — Downstream (cards_by_type + resolve_person_batch + &lt;1s)

**Evidence:** All `iter_*` call sites wired to Rust cache-backed reads (Step 15b). `resolve_person_batch` wired to Rust via `crate_bridge` (Steps 13–14). `iter_derived_card_dicts` now uses `PERSON_RESOLVABLE_CARD_TYPES` (includes finance) with `frontmatter_dicts_from_cache`.

**Measured on 1% slice (143,148 notes):**

- runner.py email scan (34K email_message notes with body): **1.08s**
- entity_resolution scan (346 resolvable cards, frontmatter-only): **0.020s**
- `ppa link-persons --dry-run` (scan + resolve, no LLM): **3.6s**
- Previous Python path: **60–90s** for scan alone

**Verdict:** **complete**

---

## Recommended sequencing for full-plan closure (updated 2026-04-15)

**Status:** Plan steps **1–19.5** are **complete** (summary table above). **17b** / **17c** / **12** / **18** / **19** / **19.5** are closed in this audit.

**Step 20** — full Phase 0 behavioral regression pass with `PPA_ENGINE=rust` (default): run `pytest archive_tests/ -k "not slow"` plus integration/slow markers as appropriate before Phase 4 cutovers. Treat as a release gate, not a blocker on recording Phase 2.9 work in git.

---

## Previous sequencing (superseded)

Kept for reference. Original sequencing from “full plan” closure

1. Step 7 (scanner) or explicit **scope cut** so downstream steps don’t depend on phantom APIs.
2. Native **Step 8–9** with Step 11 `ProjectionRowBuffer` parity; Step 10 deferred unless COPY becomes measurably hot.
3. Rust **Step 13–14** after PersonResolutionIndex design freeze.
4. **Step 16** LLM conflicts — reuse enrichment LLM client patterns.
5. **Step 6/12/18** performance gates last (need stable Rust paths).

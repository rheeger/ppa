# Retrieval fidelity validation (P01-D)

Search-product checkpoint for P01-A through P01-C, including B1/B2. This is a
P04-C / P10 input. **It does not block P02.** P02 remains unblocked at the
P01-B format/ANN freeze (`a014f8f068f8ce552a21bc8688cb55f6bb501f18`).

**production_proven = false.** No production rollout. Scale at 1e6 × 1536 is
**blocked**, not waived.

Branch: `codex/ppa-p01-retrieval-fidelity`. Worktree: `ppa-p01`.
`PPA_TEST_PG_DSN` was unset. No seed. Embedding provider: `hash` /
`archive-hash-dev`.

## Format and engine identity (measured this run)

| Item | Value |
| --- | --- |
| `SERVING_INDEX_FORMAT_VERSION` | **2** |
| `VECTOR_IMPL` | `ivf_centroids_v2` |
| Freeze fixture SHA-256 | `bc00019ed3f0db856d9f3093ff0f1b3c6fa18b230b006307092e6d2277a1dca0` |
| `schema.rs` SHA-256 | `27be40d47e6ec0f5a416b714bfeda8b2e246f6d4e6c5c6b6178eb58aeb90c5f8` |
| Imported extension SHA-256 | `a40a920595ccef4d402d39e856a63a74f35f5dde9d37dac138a0d15575b6b0a8` |
| Wheel SHA-256 | `f1d3da57a7b45b6488bbab138b8edd80aa640a3b2b0ca2ec491b2c9f71f61328` |
| Wheel | `.plan-wheels/archive_crate-0.1.0-cp314-cp314-macosx_11_0_arm64.whl` |
| Ranking / pipeline on ANN publish | `p01b2-rrf-1` / `2026.09.06.p01b2` |

`vector.rs`, `vector_train.rs`, and `p01b_format_freeze.json` were not edited
in this slice.

## Slice SHAs (already landed)

| Slice | SHA |
| --- | --- |
| P01-A | `02da6d14dd3189677f026cc6b4f60519258be95a` |
| P01-B | `a014f8f068f8ce552a21bc8688cb55f6bb501f18` |
| P01-B1 | `0acb8badac1f1ca980e453636fb14c0a1fdad5ad` |
| P01-B2 | `8e32318351180773f00975b42a0de48f8af7631d` |
| P01-C | `2707b6d11c17da25c8b6cd2f8a01a441768e344e` |

## Acceptance suite (re-run this slice)

Command: `.venv/bin/python -m archive_tests.acceptance.run --suite p01 --output logs/plans/p01/P01-D --require-integration`

All **6** scenarios passed. Seed argument was `0` (synthetic fixtures only).

| Scenario | Status | Measured |
| --- | --- | --- |
| `p01.ann.modulo_adversary` | passed | Recall@5 trained IVF **1.0**, modulo-IVF **0.0**. n=1089, nlist=33, nprobe=32, candidates_scored=1088, `scanned_all` implied false (1088 < 1089). Scenario elapsed **0.582 s** including publish. |
| `p01.bursts.short_answer` | passed | Short-answer Recall@1 **0.0 → 1.0** on email/iMessage/Beeper. `CHUNK_SCHEMA_VERSION` 6. |
| `p01.fusion.rrf_rerank_freshness` | passed | RRF nDCG@2 **1.0**, raw blend **0.6309**. Recall@1 **1.0**. HTTP rerank called. Rare-token weight **0.0**. |
| `p01.clients.evidence_envelope` | passed | Zero-hit/eleven-irrelevant **low**; exact **high** and `complete=false`. CLI=MCP. Denial `status=denied`. |
| `p01.fidelity.export_native_mcp` | passed | Exact UID hit; quarantine listed; suppressed excluded; inferred edge `method=inferred`, confidence **0.58**. MCP JSON uid `hfa-person-p01aact001`, confidence **high**. |
| `p01.checkpoint.search_product` | passed | Format v2 identity; million-vector **blocked**; `production_proven=false`. |

Fidelity publish generation `1788670780095`: 4 cards, 16 chunks, 0 embeddings, 1 edge. During `archive_rebuild_indexes` an in-run `serving_index_refresh_failed` (`InFailedSqlTransaction`) was logged; the subsequent explicit `publish_serving_index` succeeded. Recorded, not ignored.

## Other verification (this slice)

| Command | Result |
| --- | --- |
| `cargo test --locked --manifest-path archive_crate/Cargo.toml` | **13 passed**, 0 failed |
| `pytest archive_tests/ -m 'not integration and not slow and not openai'` | **1486 passed**, **3 failed**, **87 deselected**, 0 skipped in JUnit |
| `ruff check` on D files | clean |

The three unit failures are outside retrieval A–C:

- `archive_sync/extractors/test_confidence.py::test_confidence_written_to_frontmatter` — `write_card` path/staging error
- `archive_sync/extractors/test_integration.py::test_full_extraction_pipeline` — `extracted_cards` 9 vs 10
- `test_ppa_maintain_nightly.py::test_dry_run_exits_zero` — nightly helper still binds a seed vault path

They are not treated as P01 retrieval regressions and were not “fixed” here to green the suite.

## Scale (blocked, not waived)

`python archive_scripts/benchmark-serving-index.py --scale-profile million_vector`

| Quantity | Measured |
| --- | --- |
| Profile | `million_vector`, n=1,000,000, dimension=**1536** |
| nlist (sqrt cap) | 1000 |
| embeddings_bytes | 6,144,000,000 |
| train_rss_bytes | 8,912,027,648 |
| query_rss_bytes | 6,154,144,000 |
| disk_bytes (envelope) | 8,301,627,648 |
| `PPA_SERVING_TRAIN_MEMORY_MB` | 8192 |
| physical_memory_bytes | 137,438,953,472 |
| disk_free_bytes | 42,385,313,792 |
| executed | **false** |
| status | **blocked** — `train_rss_bytes` exceeds train memory cap |
| production_proven | **false** |

Recall@10 / Recall@20 at production dimension were **not measured**. The plan’s 0.95 / 0.90 figures are not merge gates and are not reported as results.

P01-B also recorded warm knn **0.722 ms** on the 1089 × 8 adversary. That number was not re-clocked as an isolated knn in D; D’s ANN scenario elapsed includes publish.

Store-method p95 on the live seed (cutover report, 2026-09-03) is a different corpus and is **not** reused as this checkpoint’s latency proof.

## Quality handoff (defaults, not a release verdict)

Proposed defaults after A–C, still subject to P04-D labeled evaluation:

- Format 2 / `ivf_centroids_v2`, `nprobe=32`, `nlist=sqrt(N)` capped at 4096
- Fusion `rrf_k60`; extra rare-token weight **off** (0.0)
- Model rerank **off**; unconfigured model is unavailable, not heuristic-as-model
- Confidence is evidence quality; volume does not produce `high`

P04 labeled / held-out corpus: **unavailable**. Model-quality proof: **unavailable**.
Fixing the modulo adversary is not a P04-D release verdict.

## Plan acceptance criteria

| AC | Verdict | Artifact |
| --- | --- | --- |
| Trust, aliases, emails, IDs, edge method/confidence survive export → native → MCP; missing stays unknown | **pass** | `p01.fidelity.export_native_mcp` in `logs/plans/p01/P01-D/results.json` |
| Hits explain channel and cite card + span/revision; stale offsets fail closed | **pass** | fidelity exact `match_channel`; bursts span/prefix; chunk evidence tests from B |
| Suppressed excluded; quarantine weight 0.35 | **pass** | fidelity listed_uids / quarantine row |
| Deterministic vs inferred not conflated | **pass** | inferred edge `method=inferred`, confidence 0.58 |
| ANN beats modulo at selective probe; no full-scan pass | **pass** | Recall@5 1.0 vs 0.0; 1088/1089 candidates |
| Exact UID/identifier coverage on supported IDs | **pass** | handle.search + MCP JSON uid |
| Empty / invalid vector / filter edge cases have tests | **pass** | cargo `invalid_vectors_are_not_assigned` + serving index tests from B |
| CLI/MCP confidence reasons + envelope | **pass** | `p01.clients.evidence_envelope` |
| Performance report (recall, latency, RSS, disk, train/publish) | **pass on 1089×8; scale blocked** | this report + scale probe; million@1536 not executed |
| Conversation bursts | **pass** | Recall@1 0.0 → 1.0 |
| RRF / rerank / diversity / freshness ablations | **pass** | fusion scenario; model quality unavailable |
| Model rerank never silently heuristic | **pass** | fusion `http_rerank_called`; unconfigured model unavailable |

## Proof states

| Layer | State |
| --- | --- |
| Implementation (A–C, B1/B2) | proven on synthetic / isolated fixtures |
| Integration | proven (`--require-integration` p01 suite) |
| Scale (1e6 × 1536) | **blocked** |
| Production | **not proven** |

P02 may continue from the P01-B freeze. This checkpoint does not reopen that format.

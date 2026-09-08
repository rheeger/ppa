# PR31 correctness closeout

**Status:** isolated implementation complete. R0 runbook written. Owner approved seed apply + 4.35M train. R1 in progress on the local seed.

| Field               | Value                                                                           |
| ------------------- | ------------------------------------------------------------------------------- |
| Branch              | `codex/ppa-pr31-correctness-closeout`                                           |
| Plan                | `archive_docs/plans/pr31-pr29-pr30-correctness-closeout.md`                     |
| Adoption            | `archive_docs/runbooks/pr31-adoption.md`                                        |
| Implementation base | `0ae7cbdb8a52b8e0625d5c84a8e6dc5d3de351d1`                                      |
| Suite               | `logs/plans/pr31/suite-proof/results.json` (`ok: true`, `--scale-profile pr31`) |
| Wheels              | `logs/plans/pr31/release/release-manifest.json`                                 |
| Living archive      | R1 authorized; wait for in-flight nightly rematerialize before publish          |

## Finding closure

| ID  | Closeout                                                                                             | Owner | Proof                                                                  |
| --- | ---------------------------------------------------------------------------------------------------- | ----- | ---------------------------------------------------------------------- |
| F01 | Alice/Bob Smith household phone never auto-merges; journaled proposals                               | F     | `test_identity_repair_decisions.py`, installed `identity-repair merge` |
| F02 | `resolve_person_card` is unique-only; `serving_index_person` returns `unique\|ambiguous\|unresolved` | E     | Rust metadata + installed `ppa person` / MCP `archive_person`          |
| F03 | Repair apply calls `merge_identities()` only                                                         | F     | `identity_repair.py` + apply-path test                                 |
| F04 | Search/query/vector/hybrid/timeline/temporal compile `PreparedPeopleFilter` once                     | E     | `metadata.rs` + `mod.rs`                                               |
| F05 | Parent increment retired; pending `OutputReceipt` drained by maintain                                | G     | `test_thread_projection_recovery.py`, maintain drain                   |
| F06 | Opaque handles stay empty canonical; Python/Rust typed parse                                         | D     | `test_identifier_contract.py`                                          |
| F07 | `ScanRejection` journaled; default publish blocked                                                   | H     | `test_rebuild_rejection_coverage.py`                                   |
| F08 | Preview writes nothing; apply journals `proposed_link`                                               | I     | conversation lifecycle + linker                                        |
| F09 | Incomplete export fails closed; no empty fallback                                                    | A     | `test_export_failure_atomicity.py`                                     |
| F10 | Captured journal receipt persisted; ack only captured sequences                                      | B     | publication-capture journal                                            |
| F11 | `PublisherLease` acquired before ACTIVE/export                                                       | B     | `test_publication_snapshot_boundary.py`                                |
| F12 | Native open + exact/query/graph canaries before ACTIVE                                               | C     | `test_candidate_activation.py`                                         |
| F13 | Installed wheel outside checkout; real stdio MCP JSON-RPC                                            | J     | `p31.end_to_end` (`mcp_helper_used: false`)                            |
| F14 | Shared services; architecture guards                                                                 | K     | `test_pr31_architecture.py`                                            |

## Demonstrations exercised

- Installed `ppa` from hashed wheels in a venv **outside** the checkout (`archive_crate` path under `/var/folders/.../ppa-p31-install-*/venv`, not the repo).
- Isolated vault → `bootstrap-postgres` → `rebuild-indexes` → `maintain` → `identity-repair merge` (preview then apply).
- Household phone stayed ambiguous on installed `ppa person +15551230000` and on MCP `archive_person` via the installed SDK stdio client.
- Stub email merged; Alice/Bob were not in `applied_redirects`.
- `--scale-profile pr31`: one-shot resolution over **1,000,000** UIDs; selective IVF at **10,000×1536** and **100,000×1536** with candidate fraction **0.05** and planted-neighbor Recall@5.

## Scale (K)

Required 10k/100k×1536 selective profiles passed. Disk preflight for a 4.35M×1536 corpus was recorded (`resource_preflight_ok: true`); that full train was **not** executed (RAM/training envelope). It is not waived as proven.

## Live adoption

R0: `archive_docs/runbooks/pr31-adoption.md`. Owner approved seed apply and the 4.35M train. R1 waits for the in-flight 2026-09-07 nightly rematerialize, then merge-apply (stub-eligible only), thread rollup, conversation proposals, and a full openai serving-index publish. Historical named merges are not undone. Warm MCP canaries go in this section after publish, redacted.

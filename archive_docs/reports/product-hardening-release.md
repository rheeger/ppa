# Product hardening release report (P04-D)

Implementation-complete gate for the ten-plan program. **This is not R0/R1.**
`production_proven = false`. No seed. No live Google. No production Postgres.

## Candidate

| Field | Value |
| --- | --- |
| Worktree | `/Users/rheeger/Code/rheeger/ppa-p04d` |
| Branch | `codex/ppa-p04d-release-gate` |
| Integrated base | P06-D `9e9edd072e5397a8ccab8038b60198715b8f2593` |
| P04-C merge | `70031d06ed35b7800414bb24b55166453c34ac48` (`ad0a967`) |
| Current-run command | `python -m archive_tests.acceptance.run --suite release --output logs/plans/program --require-integration` |
| Current-run evidence | `logs/plans/program/release-evidence.json` |

The SHA of the P04-D gate commit is the integrated release SHA. Sibling `--suite pNN` logs are **inherited** and are not a substitute for this run.

## Inherited child finals

| Plan | Final SHA | Origin |
| --- | --- | --- |
| P01 | `7bf33a3fec79481a81c75ae91415a60b1b1fc1cb` | inherited sibling |
| P02 | `0d221a726308c52bf3e5f543d7002804b22e8c54` | inherited sibling |
| P03 | `39299aef6f5585401cd35abd06573ff43e50ade7` | inherited sibling |
| P04-C | `70031d06ed35b7800414bb24b55166453c34ac48` | merged into this candidate |
| P05 | `e51eb5ab4ca8065efbbec4f0b1787be4d8325ae5` | inherited sibling |
| P06-D | `9e9edd072e5397a8ccab8038b60198715b8f2593` | this candidate's base |
| P07 | `6c20aec5b8157d0dcc3094634ea5607315412ebf` | inherited sibling |
| P08 | `34377be98638499c8c86c47ccf9fd1244fc18731` | inherited sibling |
| P09 | `663cb2e5bd9606afd706457f19c8fa6c7f4f117c` | inherited sibling |
| P10 | `4885ac7e37e5a7b0745167a609f122ce9e3a828d` | inherited sibling |

## Current-run suite

`--suite release` registered **1** scenario (`release.integrated_gate`) and **passed**.
It executed destination proofs for all ten plans on isolated per-destination vaults.

Registered child scenario counts (must be >0 or release fails): p01=6, p02=4, p03=4, p04=3, p05=2, p06=3, p07=3, p08=4, p09=3, p10=4.

`archive_tests/test_release_manifest.py --require-integration`: **9 passed**.

## Quality verdict

**`accept_defaults`**

Reasons:

- All six held-out oracle cases matched (no missing required UIDs, no forbidden leaks).
- Trained IVF Recall@5 **1.0** vs modulo-IVF **0.0** on the 1089-vector adversary; `nprobe=32`, `nlist=33`, **1088/1089** candidates scored (not a full scan).
- P01 defaults unchanged: format 2, `ivf_centroids_v2`, `rrf_k60`, rare-token weight 0.

Optional feature: **model rerank** = `retain_baseline_for_optional_feature` (no model-quality proof; stays disabled).

Recall@10/20 0.95/0.90 is **measure-then-propose**, not a merge gate. Those numbers were not measured at 1536-d and are not reported as results.

No material regressions. Authorization tests used evidence discovery/context only — no `authorized=true`.

## Labeled relations (current run)

| Case | Owner | Status |
| --- | --- | --- |
| same trip | P10-C | passed (lookalike excluded; unmatched ride reported separately) |
| same charge | P10-C | passed (482.00 USD, lookalike excluded) |
| not-the-same-person | P07-C | passed |
| reply-is-authorization | P01-B1 | passed (request + reply; no authorized flag) |
| renewal-is-not-current-subscription | P10-C | passed (last-observed cancel) |

## Hard gates

Zero unauthorized access, zero lost committed changes, zero stale deleted objects, zero false completion, exact aggregate arithmetic, full/delta equivalence, relation-label correctness: **all true** on this SHA.

P08-C `freshness_unknown` is not treated as final proof. P06-D burst attach/replay ran on this candidate (`p06.convergence.installed_engine` passed). P09-D analytics CLI/MCP labeled pending; P10-D `archive_analytics` passed here — capability is **supported**.

## Native engine identity (imported this process)

| Field | Value |
| --- | --- |
| Path | `.../archive_crate/archive_crate.cpython-313-darwin.so` |
| SHA-256 | `ac5fa810612c3f7713c53f103f18ebbcf13f9dc29dad2bbe83ee2aec935b3e3b` |
| Crate source | unchanged from P06-D; P04-D did not edit `archive_crate/` |

CI `product-release.yml` builds the wheel from the candidate checkout. Do not import an older unrelated binary.

## Platform and scale

| Target | Status |
| --- | --- |
| macOS arm64 + CPython 3.12 | **proven** (P09 inherited packaged install) |
| This current run | macOS arm64 + CPython 3.13.15 (not a new matrix claim) |
| Linux x86_64 | **unproven** — not invented |
| Million-vector @1536 | **blocked** — `train_rss_bytes=8912027648` exceeds `PPA_SERVING_TRAIN_MEMORY_MB=8192`. Not waived, not executed, not fabricated. |

## Other verification

| Check | Result |
| --- | --- |
| `git diff --check` | clean |
| `ruff check` on D files | clean after import sort |
| Rust/crate tests | not re-run; crate sources unchanged |
| Full `archive_tests/ -m 'not integration'` | not re-run this slice (impractical); release-manifest unit+integration ran |

## Recommendation

**Implementation-complete for R0 review.** Not a production rollout. Not R0. Not R1.

`production_proven=false`.

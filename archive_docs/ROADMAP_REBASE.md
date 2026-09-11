# PPA roadmap rebase

**Superseded as the living map on 2026-09-09.** Read [STATUS.md](STATUS.md) and [PRODUCT_CAPABILITY_MATRIX.md](PRODUCT_CAPABILITY_MATRIX.md) first. This file is a P09-A inventory (`d36663b`, 2026-09-06). Analytics, AccessContext, bursts/RRF, saved scopes, and the ten-plan hardening (PR 29) landed after it.

**Slice:** P09-A. **Inventory date:** 2026-09-06. **Baseline SHA:** `d36663b`.  
**Inputs:** this repository’s code, tests, runtime/MCP docs, and v2 / v2.5 / v3 / v4 vision files. No external attachments.

This document mapped **historical phase claims** to **then-current code**. Keep it for archaeology. Do not use the “Deferred to P10” analytics row or the “person lookup is name/slug only” implication as current truth.

## How to use this rebase

1. Treat source code as authoritative.
2. Treat vision docs as intent plus dated assumptions.
3. When a vision sentence conflicts with this file or `PRODUCT_CAPABILITY_MATRIX.md`, the matrix wins until P09-D rewrites the public docs.
4. Do not revive Arnold-as-home, a populated knowledge cache, or a “still need a broad Rust rewrite” story.

## Product destination (unchanged)

A second archive can still assemble answers from typed cards and labeled relationships. Saved scopes (when they exist) are reusable **filters**, not tenancy or project ACLs. Independent instances beat shared multi-user databases for this increment.

## Phase map

| Historical claim | Where it was written | Current truth | Disposition |
| --- | --- | --- | --- |
| v2 closed; retrieval surface is the product; Phase 7 knowledge cache skipped | `archive_docs/vision/v2vision.md` (Phase 7 skipped 2026-04-24; `archive_knowledge` always lexical fallback) | Still true. `knowledge_for_domain` falls back to search. No `archive_cli/knowledge/`, no `refresh-knowledge`. | **Keep.** Do not claim a populated cache. |
| ~46 facets / 9 domains / living profile | `v2vision.md` original Phase 7; `v3vision.md` opening thesis and status mock | Schema types `knowledge` and `observation` exist. No generator. Tool fallback is search. Program explicitly defers living profiles. | **Deferred.** Later increment. Not a v3 prerequisite. |
| 37 card types | `README.md`, `v3vision.md` | `CARD_TYPES` has **36** keys in `archive_vault/schema.py`. | **Correct the count** in P09-D. |
| Postgres + pgvector is the query engine / “target serving architecture” | `ARCHITECTURE.md`, `INDEXING.md`, older v3/v4 FTS language | MCP/CLI query the Rust serving index. Postgres is the derived warehouse. `QueryMixin` is the test oracle. Live pgvector search timed out at 30s and was retired. | **Rebase.** Serving shipped. |
| Broad Rust rewrite of scanner, materializer, FTS, MCP still ahead | `v4vision.md` Phase 16 crate sketch (`ppa-engine/`, `ppa-cli/`, Rust MCP) | `archive_crate` already exports walk, cache, materialize, chunk, person-batch, validator, and serving_index search/query/vector/hybrid/graph/timeline/neighbors. Python keeps adapters, enrichment, MCP, CLI. | **Do not repeat as TODO.** Incremental Rust remains (ANN, publication); the greenfield rewrite sketch is outdated. |
| v2.5-done = Arnold promotion / validation ladder Gates 6+ | older H text; `v3vision.md` “Prerequisite: v2.5 readiness on Arnold” | v2.5-done is the **local seed living archive**. Arnold is down and is **not** the long-term home. Formal leftover `ready: false` is `local_seed_living_corpus`. Do not copy the seed or restage a fake ladder. | **No Arnold gate** for local v2.5 or for starting independent-instance work. |
| Photos, Apple Health, `--catch-up` still open v2.5 closers | superseded H checklists | Parked in `PARKED_ADAPTER_SOURCE_IDS` and v2.5 current-status tables. Adapters exist; freshness does not require them. | **Parked.** Not a packaging blocker. |
| Source updaters / processor DAG / status surfaces | v2.5 Sections D/E/F | Declarations + runners + `ppa status` exist. Receipts, transactional publish, and honest per-instance freshness remain later slices. | **Partial / landed contracts.** Execution hardening is P02/P03. |
| “After v3, the knowledge cache works” / “the engine stays Python” | `v3vision.md` thesis; `v4vision.md` opening | Knowledge cache does not work (empty). Engine is Python orchestration + **shipped Rust serving**. | **Rebase v3/v4 headers.** |
| `ppa setup` / Docker Compose / vault encryption UX as v3 product | `v3vision.md` Phase 10–11 | No `setup` / `connect` / `config explain` parsers. Config is split env + cwd file + `_meta/ppa-config.json`. | **Deferred to P09-B/C**, without Arnold or encryption-UX as prerequisites. |
| Independent second archive | implied by v3 “many users”; this program’s P09 | One-machine, env-bound instance. Derived `archive_instance` label only. Two-instance isolation **unproven**. | **This workstream.** A then B/C/D. |
| Shared multi-user / household ACL | v3 “one user to many” | Out of scope. Two independent instances, not one tenancy model. | **Do not build.** |
| Native app, OAuth proxy, billing, signed connector feed | `v4vision.md` | Out of this program. | **Later product.** |
| Narrative workflows (subscriptions, trip costs, changes-since) | v2/v3 “ask what subscriptions…” product copy; P10 plan | Copy in `README.md` describes the *intent*. No workflow API. Compose query/evidence/read. | **Deferred to P10.** Do not imply it is shipped. |
| Arnold as canonical MCP remote | `MCP_SETUP.md`, `PPA_RUNTIME_CONTRACT.md` §2.4/§8/§9, `PPA_BACKUP_AND_RESTORE.md`, `SECURITY_MODEL.md` | Historical host ops. Local v2.5 superseded Arnold-as-home. HTTP MCP on the creator machine is an instance choice, not the product architecture. | **Rewrite in P09-D**; keep historical notes labeled historical. |

## v2.5 local vs Arnold (explicit)

| Statement | Status |
| --- | --- |
| Local seed on the creator machine is the living high-signal corpus | Historical ops fact in vision README; **not** a second-instance default path |
| Arnold deploy / soak / Gates 6+ required before local v2.5 or v3 packaging | **False.** Struck by `v2.5vision.md` and `vision/README.md` |
| New archives inherit `local_seed_living_corpus` | **Forbidden.** P09-D must bind that exception to the original instance only |
| Photos / Health / `--catch-up` block independent instances | **False.** They are parked sources, not install prerequisites |

## Serving migration (explicit)

| Old sentence | Replacement truth |
| --- | --- |
| “Postgres is now the primary backend for this derived index layer” + pgvector as semantic retrieval (`INDEXING.md`) | Postgres is the **warehouse**. Live query is `<vault>/_meta/rust-search-index` (`ACTIVE` generation). |
| “Target serving architecture: Postgres + pgvector” (`ARCHITECTURE.md`) | Serving architecture is Rust mmap index published by `maintain` / `rebuild-indexes`. |
| “v4 Rust engine rewrite delivers FTS / MCP / scanner” | Scanner, materializer, and serving query already ship in `archive_crate`. MCP remains Python. |
| “Query indexes are future MCP/query layers” (`ARCHITECTURE.md` overview) | MCP query tools are shipped (`archive_cli/server.py`). |

## Knowledge cache (explicit)

| Claim | Truth |
| --- | --- |
| Knowledge cache is populated / 41 of 46 facets fresh | **False.** No generator; `archive_knowledge` fallback is search (`fallback: true`). |
| `archive_knowledge` is a synthesized brief | **False.** MCP instructions already say treat fallback as search. |
| Knowledge types should be removed | **No.** Keep `knowledge` / `observation` schema so a later increment can write sourced cards. Do not fill them with inferential living profiles in this program. |

## Second instance vs shared tenancy (explicit)

| Model | Status |
| --- | --- |
| Separate archive IDs, roots, warehouse bindings, credentials | **Intended** (P09-B/C/D). Not implemented beyond env/DSN/vault path. |
| One shared DB with household ACLs | **Out of scope.** |
| Saved scopes as permission grants | **Forbidden.** Scopes are filters intersected with policy (P09-B + P05). |
| CWD config discovery as the scheduled-job contract | **Legacy.** Document as adapter; P09-B binds instance directory or explicit path. |

## Future prerequisites (evidence-bearing only)

Each item names the next slice that must produce evidence. None of these are “done because a vision doc said so.”

| Prerequisite | Slice | Evidence that will count |
| --- | --- | --- |
| Typed instance config + saved scopes | P09-B | Precedence tests; empty-scope ≠ search-everything |
| Clean install + first fixture query | P09-C | Installed wheel path + first grounded read |
| Two-instance restart without cross-talk | P09-D | Two IDs/roots/checkpoints |
| Shared typed records | P06-A | Frozen dataclasses + exact-read fixture |
| Format/ANN freeze | P01-B | Serving record + ANN format tests |
| Durable journal | P02 | Consumer cursors + generation validation |
| Output receipts | P03 | Revision-specific outputs |
| AccessContext | P05 | Deny on hop / mixed-source |
| Restore same story | P07 | Isolated restore receipt |
| Connector replay | P08 | Account-isolated replay |
| Narrative workflows | P10 | Coverage/freshness on three workflows |
| Release gate | P04-D | Integrated SHA, no fabricated rows |

## Outdated public-doc paragraphs for P09-D

P09-D rewrites these. This list is the handoff; do not edit those files in P09-A.

### `README.md`

- “PPA currently models 37 card types” — replace with the 36 `CARD_TYPES` keys (or a generated count).
- Quick Start `pip install -e .` without building `archive_crate` — native serving is required for supported retrieval; editable checkout is not the clean-install story.
- Product questions such as “what subscriptions am I paying for?” read as if a workflow exists — label as compose-from-cards until P10 lands.
- “The graph is evidence-aware. Edges carry type and confidence.” — warehouse `edges` has no confidence/method/evidence columns; seed-link confidence is a separate gated path.

### `archive_docs/ARCHITECTURE.md`

- Overview still describes `skills/hfa/`, `skills/archive-sync/`, `ppa/` as an external workspace, and “future MCP/query layers.”
- Type system lists ~10 card classes, not the current 36.
- Vault layout (`hf-archives/`, `Email/`, `IMessage/`) is the old HFA sketch.
- “Target serving architecture: Postgres + pgvector.”
- “Current Migration State” (PR #6, `hey-arnold`, `HFA_LIB_PATH`) is pre-monorepo.

### `archive_docs/INDEXING.md`

- “Postgres is now the primary backend for this derived index layer.”
- Target end state that names pgvector as semantic retrieval without the Rust serving generation.
- “Docker-backed pgvector integration tests” as the live retrieval contract.

### `archive_docs/PPA_RUNTIME_CONTRACT.md`

- Status line “Frozen as of Phase 2.9” — serving cutover and nightly maintain changed the contract.
- Config discovery listing `archive-mcp.yml` (code discovers `ppa.yml` / `ppa.yaml` / `ppa.json`).
- §2.4 Arnold integration env vars as if Arnold were the product home.
- “`full` … Used for local development and **direct Arnold access**.”
- §8 storage layout (Arnold) and §9 “Arnold is a thin consumer” as current architecture.
- Alias claim “No alias fallback exists” vs later “All four also accept `ARCHIVE_*` aliases.”

### `archive_docs/MCP_SETUP.md`

- “Local vs remote (Arnold)” presenting Arnold as the canonical remote client and Ginger/Tailscale as the product topology.

### `archive_docs/AGENT_USAGE.md`

- “`ppa health` is the shell equivalent of `archive_stats` / `archive_status_json`” — those are distinct commands; keep routing, fix the equivalence.

### `archive_docs/PPA_BACKUP_AND_RESTORE.md`

- Entire runbook scoped to “HFA instance on Arnold (192.168.50.27).” Relabel historical; point at P07 restore once it exists.

### `archive_docs/SECURITY_MODEL.md`

- Scope and threat model centered on Hey Arnold VM / passkey gate / OpenClaw on Arnold.

### `archive_docs/vision/README.md`

- Keep the **v2.5-done = local seed, Arnold is not the home** paragraph. Do not revert it to an Arnold gate.
- Leave the machine-specific seed path as a historical instance note, not a default for new installs.

### `archive_docs/vision/v3vision.md`

- Opening: “37 card types, 17 adapters, 46 knowledge facets.”
- “The engine stays Python.”
- “Prerequisite: v3 packaging assumes v2.5 readiness on Arnold.”
- Status mock showing “Knowledge: 46 facets… Fresh: 41.”
- Architecture diagram with `knowledge cache` as a running box.

### `archive_docs/vision/v4vision.md`

- Opening: “the knowledge cache works.”
- Pillar 3 / Principle 15 “Rust engine rewrite” of scanner, materializer, FTS, MCP as future work.
- Crate tree (`ppa-engine/`, `ppa-cli/`, Rust MCP) that does not match `archive_crate/src/`.

### `archive_docs/runbooks/ppa-rollout.md` and related Arnold runbooks

- Treat as historical host ops. Do not present Arnold copy/restore as the independent-instance path.

## P09-D rewrite rules

When those paragraphs are replaced:

- Lead with the matrix statuses, not phase numbers.
- Say Rust serving **exists**; say what is still unproven (ANN gates, two-instance soak, clean install).
- Say knowledge is **empty/deferred**, not “coming in v3.”
- Say Arnold is **not** a prerequisite.
- Say saved scopes are filters.
- Record `local_seed_living_corpus` as a bound historical exception on the original instance only.

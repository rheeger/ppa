# PPA product capability matrix

**Slice:** P09-A. **Inventory date:** 2026-09-06. **Baseline SHA:** `d36663b` (`main`).  
**Scope:** current capability as implemented in this repository. Not a rewrite of `README.md` or `ARCHITECTURE.md` (that is P09-D).  
**How to read:** every row is grounded in a path that exists in this checkout. Status is not inferred from vision docs or historical host reports.

## Status vocabulary

| Status | Meaning |
| --- | --- |
| **shipped** | Callable code path exists, with at least one focused test or cutover report in-repo. Not a production-proven claim. |
| **partial** | Code exists but is incomplete, warehouse-only, fallback, gated off, or missing a required sibling contract. |
| **deferred** | Intentionally not built. Named owner and remaining evidence gap are recorded. |
| **unproven** | Behavior is specified or partially coded, but this repo does not contain isolated/integration/scale proof that it works as a product guarantee. |
| **empty** | Schema or tool exists; the capability returns no precomputed product (fallback or vacant table). |

Production-proven is **not** a status in this matrix. R0/R1 remain separate.

## North star

PPA succeeds when a machine can find typed cards, hop labeled relationships, and assemble a sourced answer. Cards stay canonical. Search, warehouse, links, and MCP stay derived.

This inventory answers: **which query and relationship capabilities are shipped, empty, partial, or deferred?**

### Query and relationship surface

| Capability | Status | What a machine can do today | Code | Tests / evidence | Remaining gap |
| --- | --- | --- | --- | --- | --- |
| Exact read of a card | **shipped** | `archive_read` / `ppa read` open vault Markdown. Serving supplies UID→path when the warehouse index is live; file bytes are canonical. | `archive_cli/store.py` (`read`), `archive_cli/commands/read.py`, `archive_cli/server.py` | `archive_tests/test_tool_confidence.py`, `archive_tests/test_server.py` | P05 access policy; P06 single typed port |
| Batch read | **shipped** | `archive_read_many` / `ppa read-many` | `archive_cli/server.py`, `archive_cli/commands/read.py` | `archive_tests/test_server.py` | Same as exact read |
| Structured filter query | **shipped** | `archive_query` / `ppa query` by type, source, person, org, date against the Rust serving index | `archive_cli/store.py` (`query`), `archive_crate/src/serving_index/mod.rs` (`serving_index_query`) | `archive_tests/test_serving_index.py` | Saved scopes (P09-B); typed predicate AST (P10) |
| Lexical search | **shipped** | `archive_search` / `ppa search` via Tantivy in the serving generation | `archive_cli/serving_index.py` (`search`), `archive_crate/src/serving_index/lexical.rs` | `archive_tests/test_serving_index.py` | Citation/span fidelity (P01) |
| Semantic / vector search | **shipped** | `archive_vector_search` over `ivf_mmap_v1` in the serving generation. Postgres pgvector is **not** the live query engine. | `archive_cli/serving_index.py` (`vector`), `archive_crate/src/serving_index/vector.rs` | `archive_tests/test_serving_index.py`, `archive_docs/reports/serving-index-cutover.md` | Selective-probe ANN recall **unproven** as a gate (P01/P04) |
| Hybrid search | **shipped** | `archive_hybrid_search` lexical + vector + graph ranking | `archive_cli/store.py` (`hybrid_search`), `archive_cli/retrieval_pipeline.py`, `archive_crate/src/serving_index/mod.rs` (`serving_index_hybrid`) | `archive_tests/test_retrieval_pipeline.py`, `archive_tests/test_serving_index.py` | RRF/rerank/freshness product (P01-B2); bursts (P01-B1) |
| Compact evidence stack | **shipped** | `archive_evidence` / `ppa evidence`: dated hits with parent/attachment/duplicate pointers; optional `narrative` outline | `archive_cli/store.py` (`evidence`), `archive_cli/commands/evidence.py`, `archive_cli/card_traversal.py` | `archive_tests/test_card_traversal.py` | Coverage/freshness envelope (P10); span citations (P01) |
| Timeline | **shipped** | `archive_timeline` activity-sorted keyset | `archive_cli/serving_index.py` (`timeline`), `archive_crate/src/serving_index/mod.rs` (`serving_index_timeline`) | `archive_tests/test_serving_index.py`, `archive_docs/reports/serving-index-cutover.md` | Pagination cursors (P10) |
| Temporal neighbors | **shipped** | `archive_temporal_neighbors` around a timestamp | `archive_cli/serving_index.py` (`temporal_neighbors`), `archive_crate/src/serving_index/mod.rs` (`serving_index_temporal_neighbors`) | `archive_tests/test_serving_index.py`, `archive_tests/test_tool_confidence.py` | Burst/neighbor context (P01-B1, P10-B) |
| Person lookup | **shipped** | `archive_person` / `ppa person` by name or slug | `archive_cli/commands/graph.py`, `archive_cli/serving_index.py` (`person`) | `archive_tests/test_tool_confidence.py` | Persistent `ArchiveIdentity` / instance isolation (P06/P09) |
| Graph hops | **partial** | `archive_graph` expands neighbors. Deterministic edges render as `[edge_type]`. Seed-link edges can render `[seed:edge_type, conf=…]` when seed-links are enabled. Warehouse `edges` rows have **no** `method` / `confidence` / `evidence_uids` columns. Serving graph stores `trust` (default 1.0). | `archive_cli/commands/formatters.py` (`format_graph`), `archive_crate/src/serving_index/graph.rs`, `archive_cli/schema_ddl.py` (`edges`) | `archive_tests/test_graph_edge_trust.py`, `archive_tests/test_weighted_graph_boost.py` | Export real method/confidence/evidence (P01); deny inheritance (P05) |
| Retrieval explain | **shipped** | `archive_retrieval_explain` / `ppa explain` | `archive_cli/explain.py`, `archive_cli/query_explain.py` | `archive_tests/test_retrieval_pipeline.py` | Envelope/completeness fields (P01-C, P10) |
| Stats / status | **partial** | `archive_stats`, `archive_status_json`, `ppa status`, `ppa readiness`. Surfaces exist. Formal `ready: true` is **not** a product claim; local-seed leftover `ready: false` is an accepted historical exception (`local_seed_living_corpus`), not a second-instance waiver. | `archive_cli/status/aggregate.py`, `archive_cli/status/readiness.py` | `archive_tests/status/`, `archive_tests/test_health_check.py` | Per-instance freshness without inherited exceptions (P09-D) |
| `archive_knowledge` | **empty** | Tool exists. It reads `knowledge_cards` and, if none are fresh, returns **lexical search fallback** (`fallback: true`). There is no `archive_cli/knowledge/` package and no `refresh-knowledge` command. Do not treat fallback rows as a synthesized brief. | `archive_cli/index_query.py` (`knowledge_for_domain`), `archive_cli/mcp_instructions.py`, `archive_cli/server.py` | `archive_tests/test_tool_confidence.py` (`test_knowledge_domain_fallback_includes_confidence`) | Living-profile / 46-facet cache stays **deferred**. Owner: future increment, not this program. |
| Narrative analytics workflows | **deferred** | No subscription-lifecycle, reconciled-trip-cost, or changes-since workflow API. Agents can compose `query` + `evidence` + `read` themselves. | *(not present; planned P10)* | — | P10 after P06-B / P01-D |
| Saved scopes | **deferred** | No named type/source/person/time presets. Every call repeats filters. Empty filter means unscoped search, not an empty-scope result. | *(not present; planned `archive_engine/scopes.py`)* | — | P09-B |
| Access-bounded query | **partial** | `PPA_MCP_TOOL_PROFILE` gates which **tools** exist (`full` / `read-only` / `remote-read` / `admin-only`). That is not an `AccessContext` over people/sources/domains. Graph hops do not inherit deny. | `archive_cli/server.py` (`_TOOL_PROFILES`) | `archive_tests/test_server.py` | P05 |

## Serving and engine

| Capability | Status | Code | Notes |
| --- | --- | --- | --- |
| Rust serving index as the MCP/CLI query engine | **shipped** | `archive_crate/src/serving_index/mod.rs`, `archive_cli/serving_index.py`, `archive_cli/store.py` (`_try_serving_query`) | Default query path. Fail-closed: no `ACTIVE` → `ServingIndexUnavailableError`. Rollback is the last good generation, not Postgres FTS/pgvector. |
| Rust vault walk / cache / materialize / chunk | **shipped** | `archive_crate/src/{walk,cache,materializer,chunk}.rs`, `archive_cli/ppa_engine.py` | `PPA_ENGINE=rust` (default) selects these loops. `PPA_ENGINE=python` is legacy for scan/cache/materialize only — it does **not** select a query engine. |
| Postgres warehouse | **shipped** | `archive_cli/index_store.py`, `archive_cli/schema_ddl.py`, `archive_cli/loader.py` | Derived COPY/GIN/IVFFlat warehouse and admin/embed path. Not the live semantic-search engine. |
| Postgres FTS / pgvector as live query | **retired** | `archive_cli/index_query.py` (`QueryMixin`) | Test oracle only. Cutover report: `archive_docs/reports/serving-index-cutover.md`. |
| Broad remaining “Rust engine rewrite” (scanner, materializer, FTS, MCP in Rust) | **do not restate as TODO** | `archive_crate/src/lib.rs` | Serving, walk, cache, materialize, chunk, person-batch, and validator already exist. Remaining Rust work is incremental (ANN fidelity, publication, typed ports), not a greenfield rewrite. |
| Typed engine ports (`ArchiveIdentity`, `AccessContext`, `EvidenceEnvelope`, …) | **deferred** | *(not present; planned `archive_engine/`)* | P06-A. `archive_cli/contracts.py` has `ArchiveConfig` / `ArchiveStore` only. |
| Transactional change journal | **deferred** | *(not present)* | P02. Today: DIRTY UID append + `ACTIVE` publish (`archive_cli/serving_index.py`, `archive_crate/src/serving_index/{dirty,generation}.rs`). |
| Processor output receipts | **partial** | `archive_sync/processors/{declarations,runner,plan}.py` | DAG + dirty-UID executors exist. Revision-specific `OutputReceipt` is P03. |

## Cards, ingest, and relationships

| Capability | Status | Code | Notes |
| --- | --- | --- | --- |
| Typed card schema | **shipped** | `archive_vault/schema.py` (`CARD_TYPES`, 36 keys) | README/v3 still say “37 types”. Grounded count is **36**: person, finance, medical_record, vaccination, email_thread, email_message, email_attachment, imessage_thread, imessage_message, imessage_attachment, beeper_thread, beeper_message, beeper_attachment, calendar_event, media_asset, document, meeting_transcript, git_repository, git_commit, git_thread, git_message, meal_order, grocery_order, ride, flight, accommodation, car_rental, purchase, shipment, subscription, event_ticket, payroll, place, organization, knowledge, observation. |
| Typed projections | **shipped** | `archive_cli/card_registry.py`, `archive_cli/projections/` | One registration per type, including `knowledge` / `observation` tables that stay empty unless cards exist. |
| Live source adapters | **partial** | `archive_sync/adapters/*.py`, `archive_sync/source_updaters/constants.py` | Executable live IDs: `gmail-messages`, `calendar-events`, `imessage`, `otter-transcripts`, `file-libraries`, `beeper`, `contacts`, `github-history`, `gmail-correspondents`. |
| Parked live sources | **deferred** (parked) | `PARKED_ADAPTER_SOURCE_IDS` in `archive_sync/source_updaters/constants.py` | `photos`, `apple-health` / `health`. Adapters exist (`archive_sync/adapters/photos.py`, `apple_health.py`) but are not required for freshness and are refused as campaign closers. |
| Export-only adapters | **partial** | `EXPORT_ADAPTER_SOURCE_IDS` | `copilot-finance`, `linkedin`, `notion-people`, `notion-staff`, `medical-records`, `seed-people`, plus health. Import-only; not live updaters. |
| Email extractors | **shipped** | `archive_sync/extractors/registry.py` (`build_default_registry`) | Uber Eats, Uber rides, DoorDash, Amazon, Instacart, shipping, Lyft, United, Airbnb, rental cars. |
| Deterministic linkers | **shipped** | `archive_cli/linker_modules/__init__.py` | calendar, communication, finance_reconcile, graph, identity, media, meeting_artifact, orphan, semantic, trip_cluster. |
| Seed / inferred links | **partial** | `archive_cli/seed_links.py` | Gated by `PPA_SEED_LINKS_ENABLED` (default off). Proposed links are not source facts. |
| Knowledge / observation cards as a living profile | **empty** + **deferred** | `archive_vault/schema.py` (`KnowledgeCard`, `ObservationCard`) | Types and warehouse tables exist so a later increment can write them. No facet generator, no 46-facet tree, no populated cache. `archive_knowledge` fallback is search. |
| `--catch-up` full mailbox walk | **deferred** (parked) | `archive_cli/source_updaters/cli.py`, `archive_cli/commands/maintain.py` | Flag exists. v2.5 leaves catch-up parked. |

## Instance, config, and packaging

| Capability | Status | Code | Notes |
| --- | --- | --- | --- |
| Env + file config | **partial** | `archive_cli/config.py`, `archive_cli/index_config.py`, `archive_vault/config.py` | Split across `ArchiveConfig`, `_meta/ppa-config.json` (`PPAConfig`), and `PPA_*` getters. CWD discovery of `ppa.yml` / `ppa.yaml` / `ppa.json`. No single versioned root model. |
| Persistent archive ID | **partial** | `archive_cli/validation_gates/instance_identity.py` | Derived label from vault stem + schema + DSN descriptor, or `PPA_ARCHIVE_INSTANCE`. Not a provisioned `ArchiveIdentity`. Role can be inferred from env; directory name must not become a production role (P09 invariant). |
| Two independent instances in one process | **unproven** | same + `archive_cli/commands/_resolve.py` | Store is built per call from env/config. No tested same-process isolation. |
| Saved scopes | **deferred** | — | P09-B |
| `ppa setup` / clean install | **deferred** | `archive_cli/__main__.py` (no `setup` parser) | P09-C. Current install is editable checkout + maturin native wheel. |
| `ppa connect` / `config explain` / authenticated backup CLI | **deferred** | — | P09-C adapters over P07/P08 services. |
| `ppa deploy` | **partial** | `archive_cli/commands/deploy.py` | Phase-9 host deploy helper (migrate/rebuild/verify). Not independent-instance setup. Historical Arnold targeting is not a local v2.5 gate. |
| Native serving required for supported retrieval | **shipped** (runtime) / **unproven** (packaged install) | `pyproject.toml` (notes maturin; does not ship a wheel), `archive_crate/` | Query path imports `archive_crate`. Clean-install proof is P09-C. |

## Privacy, durability, analytics

| Capability | Status | Owner of remaining work | Gap |
| --- | --- | --- | --- |
| Tool-profile gating | **partial** | P05 | Tools can be disabled. Denied people/sources can still appear via graph/read if the tool is allowed. |
| Contained I/O / egress policy | **deferred** | P05 | — |
| Canonical correction journal + restore of the same story | **partial** | P07 | Embedding-cache restore scripts/tests exist (`archive_tests/test_restore_path.py`, `archive_tests/test_ppa_backup.py`). Isolated restore of corrected narrative is not proven. Arnold-scoped runbook `archive_docs/PPA_BACKUP_AND_RESTORE.md` is historical host ops, not the product restore contract. |
| Connector SDK / account isolation | **partial** | P08 | Adapters exist; versioned SDK + replay gates do not. |
| Bounded analytical workflows | **deferred** | P10 | — |
| Release / acceptance harness | **partial** | P04 | Large pytest tree exists; isolated product gate + ANN oracle are not this slice. |

## What this matrix refuses to claim

- Arnold readiness, Arnold deploy, or copying the seed as a prerequisite to local v2.5. v2.5-done is a **local seed living archive**. See `archive_docs/vision/README.md` and `archive_docs/vision/v2.5vision.md`.
- A populated knowledge cache, 46 fresh facets, or “the model is the answer.”
- That a broad Rust rewrite of serving/scan/materialize is still ahead of the product. Those loops are already in `archive_crate`.
- Production-proven health from historical slice or host reports.
- Shared multi-user tenancy. The intended next product step is **independent instances**, not one ACL hierarchy.

## Owner index for deferred / empty rows

| Item | Owner | Evidence still required |
| --- | --- | --- |
| Knowledge cache / 46 facets / living profiles | Later increment (explicitly out of this program) | None in this program. Keep schema + `archive_knowledge` fallback honest. |
| Saved scopes + typed instance config | P09-B | Precedence matrix, redacted explain, same-process isolation |
| Setup / packaged native install | P09-C | Wheel/lock hashes, installed CLI outside checkout |
| Independent-instance soak + public doc rewrite | P09-D | Two archive IDs/roots through restart |
| Typed engine records + one query/read path | P06 | Contract fixtures; CLI/MCP parity |
| ANN recall + citation spans | P01 | Selective-probe oracle; span round-trip |
| Change journal + complete generations | P02 | Full vs incremental agreement |
| Output receipts | P03 | Revision-specific outputs |
| AccessContext / egress | P05 | Deny on hop and mixed-source derived |
| Restore of corrected story | P07 | Isolated restore receipt |
| Connector SDK | P08 | Replay + account isolation |
| Narrative workflows | P10 | Coverage/freshness on three workflows |
| Product release gate | P04 | Composed suite on integrated SHA |

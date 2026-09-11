# PPA product capability matrix

**Inventory date:** 2026-09-09. **Baseline SHA:** `a815dc1` (`main`).  
**Scope:** current capability as implemented in this checkout. Living narrative: [STATUS.md](STATUS.md). When this file conflicts with a vision doc or [ROADMAP_REBASE.md](ROADMAP_REBASE.md), this file and STATUS win.  
**How to read:** every row is grounded in a path that exists in this checkout. Status is not inferred from vision docs or historical host reports. Isolated acceptance is not `production_proven`.

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
| Structured filter query | **shipped** | `archive_query` / `ppa query` / `archive_analytics workflow=query` by type, source, person, org, date, and optional predicate AST against the Rust serving index. Saved scopes resolve. Empty intersection is empty-scope. | `archive_cli/store.py` (`query`), `archive_engine/query.py`, `archive_crate/src/serving_index/mod.rs` (`serving_index_query`) | `archive_tests/test_serving_index.py`, `archive_tests/test_evidence_clients.py` | Span citations remain incremental |
| Lexical search | **shipped** | `archive_search` / `ppa search` via Tantivy in the serving generation | `archive_cli/serving_index.py` (`search`), `archive_crate/src/serving_index/lexical.rs` | `archive_tests/test_serving_index.py` | Span fidelity remains incremental |
| Semantic / vector search | **shipped** | `archive_vector_search` over trained `ivf_centroids_v2` in the serving generation. Postgres pgvector is **not** the live query engine. | `archive_cli/serving_index.py` (`vector`), `archive_crate/src/serving_index/vector.rs` | `archive_tests/test_serving_index.py`, `archive_docs/reports/retrieval-fidelity-validation.md` | Million-vector @1536 train blocked on an 8 GB cap; living seed uses a higher host cap |
| Hybrid search | **shipped** | `archive_hybrid_search` lexical + vector + graph ranking with RRF fusion, conversation bursts, and optional freshness. Model rerank stays optional and off. | `archive_cli/store.py` (`hybrid_search`), `archive_cli/retrieval_pipeline.py`, `archive_crate/src/serving_index/mod.rs` (`serving_index_hybrid`) | `archive_tests/test_retrieval_pipeline.py`, `archive_docs/reports/retrieval-fidelity-validation.md` | Optional model-quality proof |
| Compact evidence stack | **shipped** | `archive_evidence` / `ppa evidence`: dated hits with parent/attachment/duplicate pointers; optional `narrative` outline; coverage and freshness fields | `archive_cli/store.py` (`evidence`), `archive_engine/query.py` | `archive_tests/test_card_traversal.py`, `archive_docs/EVIDENCE_QUERY_CONTRACT.md` | — |
| Timeline | **shipped** | `archive_timeline` activity-sorted keyset | `archive_cli/serving_index.py` (`timeline`), `archive_crate/src/serving_index/mod.rs` (`serving_index_timeline`) | `archive_tests/test_serving_index.py` | — |
| Temporal neighbors | **shipped** | `archive_temporal_neighbors` around a timestamp. Neighbor context can add one preceding and one following unit on the same thread or burst lane. | `archive_cli/serving_index.py` (`temporal_neighbors`), `archive_engine/context.py` | `archive_tests/test_serving_index.py`, `archive_docs/EVIDENCE_QUERY_CONTRACT.md` | — |
| Person lookup | **shipped** | `archive_person` / `ppa person` by name, slug, UID, email, or phone. Returns `unique` / `ambiguous` / `unresolved`. Does not pick a household winner. | `archive_cli/commands/graph.py`, `archive_crate/src/serving_index/metadata.rs` | `archive_tests/test_tool_confidence.py` | Invitation From-lines can still steal aliases; see archive-query skill |
| Graph hops | **shipped** | `archive_graph` expands neighbors under `AccessContext`. Bounded BFS has depth, node/edge, and elapsed budgets. Denied neighbors are never entered. Warehouse `edges` rows have **no** `method` / `confidence` / `evidence_uids` columns. Serving graph stores `trust` (default 1.0). | `archive_crate/src/serving_index/graph.rs`, `archive_engine/access.py` | `archive_tests/test_graph_edge_trust.py`, `archive_docs/reports/engine-convergence-validation.md` | Seed-link confidence remains a gated path |
| Retrieval explain | **shipped** | `archive_retrieval_explain` / `ppa explain` | `archive_cli/explain.py`, `archive_cli/query_explain.py` | `archive_tests/test_retrieval_pipeline.py` | — |
| Stats / status | **partial** | `archive_stats`, `archive_status_json`, `ppa status`, `ppa readiness`. Surfaces exist. Formal `ready: true` is **not** a product claim. `local_seed_living_corpus` is bound to the original local seed only (`archive_cli/status/instance_policy.py`). | `archive_cli/status/aggregate.py`, `archive_cli/status/readiness.py` | `archive_tests/status/`, `archive_tests/test_instance_health.py` | Formal ready leftover accepted |
| `archive_knowledge` | **empty** | Tool exists. It reads `knowledge_cards` and, if none are fresh, returns **lexical search fallback** (`fallback: true`). There is no `archive_cli/knowledge/` package and no `refresh-knowledge` command. Do not treat fallback rows as a synthesized brief. | `archive_cli/index_query.py` (`knowledge_for_domain`), `archive_cli/mcp_instructions.py`, `archive_cli/server.py` | `archive_tests/test_tool_confidence.py` (`test_knowledge_domain_fallback_includes_confidence`) | Living-profile / 46-facet cache stays **deferred**. Owner: future increment, not this program. |
| Narrative analytics workflows | **shipped** | `ppa analytics` / `archive_analytics`: subscriptions (last-observed, not invented current), trip costs (identity join, no FX), changes-since (journal, not derived refreshes as life events). Completeness fields are required. | `archive_engine/analytics/`, `archive_cli/commands/analytics.py`, `archive_cli/server.py` (`archive_analytics`) | `archive_tests/test_evidence_clients.py`, `archive_docs/EVIDENCE_QUERY_CONTRACT.md`, `archive_docs/reports/p10-runtime-capability-delta.md` | Isolated proof. Not production_proven. |
| Saved scopes | **shipped** | Named type/source/person/time presets. Filters intersect `AccessContext`. Unknown names fail closed. Empty intersection is empty-scope, never unscoped search. | `archive_engine/scopes.py` | `archive_tests/acceptance/scenarios/p10_clients.py` | — |
| Access-bounded query | **shipped** | `AccessContext` is applied before ranking, neighbor expand, and graph BFS. Denied sources do not leak through hops. `PPA_MCP_TOOL_PROFILE` still gates which **tools** exist. | `archive_engine/access.py`, `archive_engine/contracts.py` (`AccessContext`) | `archive_docs/reports/engine-convergence-validation.md` | Contained egress remains later |

## Serving and engine

| Capability | Status | Code | Notes |
| --- | --- | --- | --- |
| Rust serving index as the MCP/CLI query engine | **shipped** | `archive_crate/src/serving_index/mod.rs`, `archive_cli/serving_index.py`, `archive_cli/store.py` (`_try_serving_query`) | Default query path. Fail-closed: no `ACTIVE` → `ServingIndexUnavailableError`. Rollback is the last good generation, not Postgres FTS/pgvector. |
| Rust vault walk / cache / materialize / chunk | **shipped** | `archive_crate/src/{walk,cache,materializer,chunk}.rs`, `archive_cli/ppa_engine.py` | `PPA_ENGINE=rust` (default) selects these loops. `PPA_ENGINE=python` is legacy for scan/cache/materialize only — it does **not** select a query engine. |
| Postgres warehouse | **shipped** | `archive_cli/index_store.py`, `archive_cli/schema_ddl.py`, `archive_cli/loader.py` | Derived COPY/GIN/IVFFlat warehouse and admin/embed path. Not the live semantic-search engine. |
| Postgres FTS / pgvector as live query | **retired** | `archive_cli/index_query.py` (`QueryMixin`) | Test oracle only. Cutover report: `archive_docs/reports/serving-index-cutover.md`. |
| Broad remaining “Rust engine rewrite” (scanner, materializer, FTS, MCP in Rust) | **do not restate as TODO** | `archive_crate/src/lib.rs` | Serving, walk, cache, materialize, chunk, person-batch, and validator already exist. Remaining Rust work is incremental (ANN fidelity, publication, typed ports), not a greenfield rewrite. |
| Typed engine ports (`ArchiveIdentity`, `AccessContext`, `EvidenceEnvelope`, …) | **shipped** | `archive_engine/contracts.py`, `archive_engine/service.py` | Isolated CLI/MCP share `ArchiveRuntime`. Not a production soak. |
| Transactional change journal | **shipped** | `archive_engine/changes/`, `_meta/change-journal.sqlite3` | Writers emit `ChangeRecord` or `request_reconciliation`. See `archive_docs/reports/publication-validation.md`. |
| Processor output receipts | **shipped** | `archive_engine/publication.py`, `archive_sync/processors/` | `publish(eligible_checkpoint)` is the served watermark. See `archive_docs/reports/processor-execution-validation.md`. |
| Content-keyed embeddings | **shipped** | `archive_cli/embedder.py` (`reuse_embeddings_by_content`) | Chunk keys are content hashes. Rematerialize reuses leftover vectors. PR 33. |

## Cards, ingest, and relationships

| Capability | Status | Code | Notes |
| --- | --- | --- | --- |
| Typed card schema | **shipped** | `archive_vault/schema.py` (`CARD_TYPES`, 36 keys) | Grounded count is **36**: person, finance, medical_record, vaccination, email_thread, email_message, email_attachment, imessage_thread, imessage_message, imessage_attachment, beeper_thread, beeper_message, beeper_attachment, calendar_event, media_asset, document, meeting_transcript, git_repository, git_commit, git_thread, git_message, meal_order, grocery_order, ride, flight, accommodation, car_rental, purchase, shipment, subscription, event_ticket, payroll, place, organization, knowledge, observation. |
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
| Two independent instances in one process | **partial** | `archive_engine/config.py`, `archive_tests/test_independent_instances.py` | Person + organization fixtures stay isolated through restart. `production_proven=false`. |
| Saved scopes | **shipped** | `archive_engine/scopes.py` | Filters intersected with `AccessContext`. Empty intersection is empty-scope. |
| `ppa setup` / clean install | **partial** | `archive_cli/commands/setup.py` | Fixture-only. Packaged install proven on macOS arm64 + CPython 3.12 only. |
| `ppa connect` / `config explain` / authenticated backup CLI | **deferred** | — | P09-C adapters over P07/P08 services. |
| `ppa deploy` | **partial** | `archive_cli/commands/deploy.py` | Phase-9 host deploy helper (migrate/rebuild/verify). Not independent-instance setup. Historical Arnold targeting is not a local v2.5 gate. |
| Native serving required for supported retrieval | **shipped** (runtime) / **unproven** (packaged install) | `pyproject.toml` (notes maturin; does not ship a wheel), `archive_crate/` | Query path imports `archive_crate`. Clean-install proof is P09-C. |

## Privacy, durability, analytics

| Capability | Status | Owner of remaining work | Gap |
| --- | --- | --- | --- |
| Tool-profile gating | **shipped** | P05 | `PPA_MCP_TOOL_PROFILE` disables tools. `AccessContext` also denies people/sources on hops. |
| Contained I/O / egress policy | **deferred** | later increment | — |
| Canonical correction journal + restore of the same story | **partial** | P07 | Versioned decisions exist. Isolated restore of a corrected narrative is proven on fixtures, not as a long soak. Arnold-scoped runbook `archive_docs/PPA_BACKUP_AND_RESTORE.md` is historical host ops. |
| Connector SDK / account isolation | **partial** | P08 | Adapters plus account-isolated replay on fixtures. Not a public SDK product. |
| Bounded analytical workflows | **shipped** | P10-D | `archive_analytics` / `ppa analytics`. Isolated. |
| Release / acceptance harness | **shipped** | P04-D | `--suite release` on the integrated SHA. Not R0. |

## What this matrix refuses to claim

- Arnold as the home of the corpus, or copying the seed there. v2.5-done is a **local seed living archive**. Arnold may be a remote HTTP MCP client. See [STATUS.md](STATUS.md).
- A populated knowledge cache, 46 fresh facets, or “the model is the answer.”
- That a broad Rust rewrite of serving/scan/materialize is still ahead of the product. Those loops are already in `archive_crate`.
- Production-proven health from historical slice or host reports.
- Shared multi-user tenancy. The intended next product step is **independent instances**, not one ACL hierarchy.

## Owner index for deferred / empty rows

| Item | Owner | Evidence still required |
| --- | --- | --- |
| Knowledge cache / 46 facets / living profiles | Later increment | Keep schema + `archive_knowledge` fallback honest. |
| Saved scopes + typed instance config | **landed** (P09) | Isolated two-instance restart. Not a stranger-ready v3 install. |
| Setup / packaged native install | **partial** (P09-C) | Fixture setup + macOS arm64 / CPython 3.12 wheel path. Linux unproven. |
| Independent-instance soak + public docs | **partial** | Fixture isolation landed. Living STATUS/README refresh is this inventory. |
| Typed engine records + one query/read path | **landed** (P06) | Isolated. |
| ANN recall + conversation bursts + RRF | **landed** (P01) | Isolated Recall@5 1.0 on the trained-IVF adversary. Span citations remain incremental. |
| Change journal + complete generations | **landed** (P02) | Isolated. Living-seed incomplete-publish block is PR 32. |
| Output receipts | **landed** (P03) | Isolated. |
| AccessContext on hops | **landed** (P05) | Isolated. Contained egress remains later. |
| Restore of corrected story | **partial** (P07) | Fixture path. Not a long soak. |
| Connector account isolation | **partial** (P08) | Fixture replay. Not a public SDK. |
| Narrative workflows | **landed** (P10-D) | Isolated `archive_analytics`. |
| Product release gate | **landed** (P04-D) | Not R0. `production_proven=false`. |
| Person lookup by phone/email + honest ambiguity | **landed** (PRs 30–32) | Living seed. |
| Content-keyed embedding reuse | **landed** (PR 33) | Living seed. |

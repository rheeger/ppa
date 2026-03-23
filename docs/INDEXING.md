# HFA Indexing

## Purpose

HFA is vault-canonical. The markdown vault remains the source of truth. Indexes exist to make retrieval and operations fast, not to redefine what is true.

## Current Implementation Slice

The first indexing slice lives in `ppa` and provides:

- exact lookup by UID and path
- structured query over indexed card metadata
- lexical search over indexed card text
- rebuild-time type-aware chunk materialization from canonical card fields and bodies
- timeline and stats from derived metadata
- graph traversal from typed materialized edges and wikilinks
- semantic retrieval with card-level vector aggregation
- hybrid retrieval with lexical, semantic, graph, recency, and provenance-aware ranking

Postgres is now the primary backend for this derived index layer.

## Target End State

The long-term target remains:

- Postgres for metadata and operational state
- `pgvector` for semantic retrieval
- normalized external ID and graph tables for multi-provider history and agent navigation

The current Postgres slice is the contract-shaping step for the larger `pgvector` and hybrid retrieval rollout.

## Index Contract

The derived index must follow these rules:

1. It is rebuildable from canonical cards and deterministic operational state.
2. It never becomes the source of truth.
3. It may mirror provenance-derived metadata, but canonical provenance stays on the card.
4. It must support additive schema evolution without forcing premature canonical hardening.
5. It must be safe to delete and rebuild.

## Indexed Primitives

Current and planned primitives:

- `cards`: canonical card metadata such as `uid`, `rel_path`, `slug`, `type`, summary, and key timeline fields
- `external_ids`: provider and account identifiers that resolve to canonical entities
- `edges`: normalized relationships derived from card fields and wikilinks, including typed thread/message/event/person links
- `chunks`: pgvector-ready chunk rows shaped to the semantics of the source card type
- `embeddings`: pgvector-ready embedding rows keyed by chunk, model, and version
- search rows: lexical search text and later chunk-level semantic retrieval rows
- index metadata: schema version, chunk schema version, counts, and rebuild information

## Current Chunking Policy

Current rebuilds materialize chunks from card-aware layouts:

- `person`: profile, role, context, and body chunks
- `email_thread`: subject, context, thread summary, rolling conversation windows, and recency window chunks
- `email_message`: subject, snippet, context, invite context, and body chunks
- `imessage_thread`: context, summary, rolling conversation windows, and recency window chunks
- `calendar_event`: title/time, participants, description, source linkage, and body chunks
- fallback cards: summary/body and selected text fields

This policy is still additive and rebuildable, but it is no longer intentionally naive. The goal is stable retrieval units that preserve card semantics before embeddings are generated.

## Current Edge Policy

The derived graph now materializes more than plain wikilinks.

Current typed edges include:

- `thread_has_message`
- `message_in_thread`
- `message_has_attachment`
- `thread_has_person`
- `message_mentions_person`
- `thread_has_calendar_event`
- `message_has_calendar_event`
- `event_has_message`
- `event_has_thread`
- `event_has_person`
- `entity_has_external_id`

Important rule:

- synthetic external-ID nodes help ranking and navigation, but canonical card-to-card traversal remains the default operator path

## Embedding Lifecycle Status

The current slice generates embeddings and tracks lifecycle state per model/version.

Use this to answer:

- how many chunks exist for a given embedding model/version
- how many chunks have embeddings already
- how many chunks are still pending
- which chunks should be embedded next

The current development path includes:

- a built-in deterministic `hash` embedding provider for plumbing, local testing, and lifecycle validation
- an OpenAI-compatible provider path for production-quality semantic retrieval
- batch-size controls for embedding runs
- retry/backoff on failed embed batches and failed OpenAI-compatible requests
- provider model/dimension consistency checks before writing vectors
- chunk schema version tracking so retrieval changes are explicit in operational metadata

## Provenance Contract

Provenance remains a canonical-card concern first.

Indexed rows should mirror enough metadata to let retrieval behave safely:

- `card_uid`
- `rel_path`
- `card_type`
- `chunk_type`
- `chunk_schema_version`
- `source_fields`
- `content_hash`
- `schema_version`
- embedding model and version once vectors exist

Important rule:

- embeddings are lossy retrieval artifacts, never truth

## Operational Workflow

After imports or source changes:

1. run the import
2. run doctor validation
3. rebuild the derived index
4. confirm index status
5. fill pending embeddings for the target model/version if semantic retrieval is in scope
6. smoke-test representative lexical, vector, and hybrid queries

Commands:

```bash
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:5432/archive \
python -m archive_mcp bootstrap-postgres
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:5432/archive \
python -m archive_mcp rebuild-indexes
ARCHIVE_INDEX_DSN=postgresql://archive:archive@localhost:5432/archive \
python -m archive_mcp index-status
```

Bootstrap first on a fresh Postgres database, then rebuild from the canonical vault.

When validating rebuilds, confirm that `chunk_count` is present in index status.
When validating chunking/ranking changes, also confirm `chunk_schema_version`.

Use MCP embedding tools to inspect backlog before wiring a real provider:

- `archive_embedding_status`
- `archive_embedding_backlog`
- `archive_embed_pending`

Current semantic retrieval tools:

- `archive_vector_search`
- `archive_hybrid_search`

Current retrieval behavior:

- vector search groups chunk hits back to the card level before ranking
- hybrid search can use exact lexical anchors, vector similarity, graph proximity, recency, card-type priors, and provenance bias
- vector and hybrid search both support `type_filter`, `source_filter`, `people_filter`, `start_date`, and `end_date`
- vector and hybrid responses expose match explanations such as `matched_by`, `score`, `chunk`, `graph_hops`, and `provenance_bias`

Important note:

- with the built-in `hash` provider, semantic behavior is useful for plumbing and deterministic tests, not for production-quality semantic relevance
- for production, prefer the OpenAI-compatible embedding provider path and track model/version explicitly

## TDD Requirement

Indexing changes should follow a test-first workflow:

1. add or update failing tests for new backend behavior, query behavior, or index contracts
2. implement the smallest change that makes those tests pass
3. verify parity against canonical-card behavior where applicable

Retrieval changes should include live backend coverage when possible. In `ppa`, that now means Docker-backed `pgvector` integration tests in addition to fake-index MCP tests.

## Pgvector Next

Adding `pgvector` on top of the current Postgres layer should not change the canonical model.

That migration should preserve:

- the vault-canonical rule
- rebuildability
- provenance boundaries
- additive schema evolution
- agent grounding on canonical cards

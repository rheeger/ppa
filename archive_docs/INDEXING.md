# Indexing a personal archive

PPA processes imported records ahead of a question so a client can search one catalog across services. The index brings keywords, semantic matches, dates, record types, and relationships into the same query interface. Keeping that work outside an individual chat lets another client use it too.

The Markdown vault remains authoritative for record contents. Indexes contain derived data and pointers back to those records; an agent reads the cards before citing them.

## Storage roles

The Rust serving generation at `<vault>/_meta/rust-search-index` handles live CLI and MCP retrieval. `ACTIVE` selects the complete generation available to readers. Postgres holds the derived warehouse, embedding state, and analytical data. Postgres FTS and pgvector remain test and warehouse paths, not a fallback when serving is unavailable.

`ppa maintain` publishes ordinary changes. `rebuild-indexes` rebuilds the derived representation when recovery or a schema change requires it. Publication watermarks describe what readers can see; a configured source does not establish that its latest records have been published.

## What gets indexed

| Record | Purpose |
| --- | --- |
| `cards` | UID, path, type, summary, dates, and other searchable metadata |
| `external_ids` | Provider and account identifiers used to resolve source objects |
| `edges` | Relationships derived from card fields, references, and approved links |
| `chunks` | Search units shaped for each card type |
| `embeddings` | Vectors identified by content, model, and version |
| Index metadata | Schema versions, counts, checkpoints, and publication information |

A new field needs a deliberate path into search or a typed projection. Adding text to an index is not a substitute for defining what that field means on a card. See [card type contracts](CARD_TYPE_CONTRACTS.md) and [typed projections](TYPED_PROJECTION_ARCHITECTURE.md).

## Preserve context when chunking

A short answer inside a long email thread can be difficult to find if the entire thread is one search unit. Card-aware chunking keeps subjects, participants, and conversation windows available alongside the message body.

Current layouts include person profiles, email subjects and invitation context, rolling thread windows, calendar participants, documents, and meeting transcripts. Conversation bursts give short exchanges their own context. Neighbor expansion can return one preceding and one following unit with separate citations.

Chunk boundaries and source revisions matter. If stored offsets no longer match the card, a client must not quote the wrong passage. The [retrieval fidelity](RETRIEVAL_FIDELITY_CONTRACT.md) and [evidence query](EVIDENCE_QUERY_CONTRACT.md) contracts define those checks.

## Materialize relationships

Typed edges include message-to-thread, message-to-attachment, event-to-person, and event-to-message relationships. These let a client follow a search hit to the record that explains it.

Synthetic external-ID nodes help ranking and navigation. Canonical card-to-card traversal remains the usual path for reading evidence. Link proposals have separate confidence and promotion rules; indexing a proposal must not turn it into a source-reported fact.

## Generate and reuse embeddings

Embeddings support meaning-based retrieval. They are approximate representations used for search, and their identity includes provider, model, revision, dimension, metric, and chunk schema. Cached vectors can only be reused when that identity is compatible.

Content-keyed embedding reuse avoids paying to embed unchanged text again after rematerialization. Pending work remains visible through `archive_embedding_status` and `archive_embedding_backlog`; `archive_embed_pending` is an administrative operation.

The built-in `hash` provider supports deterministic fixture tests and local plumbing. It does not provide useful semantic similarity. A semantic provider needs its model and dimensions configured explicitly. Remote providers receive the text sent for embedding, subject to the supported [egress controls](DATA_BOUNDARIES.md#provider-egress).

## Keep the catalog current

For normal source updates:

1. Bind the intended instance and run `ppa maintain` through the [maintenance job workflow](PLAYBOOK.md#running-imports-safely).
2. Check source results, processor receipts, and the published generation.
3. Inspect any embedding backlog for the configured model.
4. Query representative records through the running MCP and read their cards.

For recovery or an index-contract change, follow [rebuilding the derived index](PLAYBOOK.md#rebuilding-the-derived-index). Bootstrap a fresh warehouse before a rebuild. Keep long jobs detached and logged, with one writer per vault.

Check `chunk_count` and `chunk_schema_version` when changing chunking. A successful source import alone does not establish that search can see the new records.

## Contribute an indexing change

Preserve rebuildability, source provenance, and additive schema evolution. Add focused tests for the retrieval behavior that changes, including a nearby negative case. Verify native serving behavior and use Postgres integration where warehouse parity matters.

When ranking changes, inspect `matched_by`, score components, graph contribution, and provenance bias with the [explain payload](RETRIEVAL_EXPLAIN_SCHEMA.md). Measure relevance against known evidence, not just whether a query returns something.

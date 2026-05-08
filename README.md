# PPA - Personal Private Archives

PPA is a private knowledge system for the data your life or organization already creates: email, messages, calendar events, files, photos, health records, financial exports, code history, receipts, travel, meetings, and more.

Most knowledge bases start with notes you write. PPA starts with evidence from the systems where things actually happened. It converts that evidence into typed Markdown cards, preserves provenance for every meaningful field, builds a Postgres + pgvector retrieval index, and exposes the whole archive to humans and agents through a CLI and MCP.

The result is not another chatbot and not another notes app. PPA is the canonical archive and retrieval engine underneath them.

## Why PPA Exists

Your important context is scattered across products that were never designed to work together. Gmail knows the receipt. Calendar knows the time. Copilot knows the charge. Apple Photos knows the place. GitHub knows the work. iMessage knows the relationship. No single vendor gives you a durable, portable, searchable view across all of it.

PPA gives you that view without making a third-party app the source of truth:

- Ask "where did I get that delivery banh mi?" across food orders, receipts, finance records, and messages.
- Ask "what was I doing around Dec 27?" across calendar, travel, photos, purchases, rides, and conversations.
- Ask "which flight, hotel, and rental car were part of this trip?" from structural links, not vague similarity.
- Ask "which purchase matches this credit-card charge?" through deterministic finance reconciliation.
- Ask "tell me about my relationship with Sarah" by combining the PersonCard, message threads, calendar events, photos, and graph neighbors.
- Ask "what subscriptions am I paying for?" from subscription lifecycle cards, not a keyword search over old emails.

A folder indexer can search files. A note app can search notes. PPA is built for the harder problem: turning messy personal or organizational exhaust into a durable, queryable, evidence-backed knowledge system.

## What Makes PPA Different

**The vault is canonical.** PPA stores the archive as Markdown files with YAML frontmatter and provenance blocks. The database, embeddings, graph tables, and projections are derived artifacts. If the index disappears, rebuild it from the vault.

**It models actions, not just documents.** PPA understands meals, groceries, rides, flights, accommodations, purchases, shipments, subscriptions, event tickets, payroll, medical records, messages, calendar events, commits, issues, people, places, and organizations as first-class card types.

**Provenance is a trust boundary.** Deterministic imports, LLM enrichment, manual edits, and derived fields are distinguishable. Agents can prefer canonical fields over summaries, exact fields over inferred ones, and body-backed evidence over embeddings.

**Retrieval is multi-modal.** The same corpus supports exact reads, structured queries, lexical search, vector search, hybrid search, graph traversal, temporal neighbors, people lookup, timeline lookup, retrieval explanations, and index health checks.

**The graph is evidence-aware.** Edges carry type and confidence. Deterministic links, wikilinks, derived-from relationships, and promoted inferred links do not get treated as equal. Hybrid retrieval uses confidence-weighted graph boosts instead of blind neighbor expansion.

**Agents get tools, not vibes.** PPA exposes MCP tools with clear routing guidance and confidence signaling. The consuming agent does the reasoning; PPA retrieves, ranks, cites, and shows when the archive may not have enough data.

**It is private by design.** The vault lives on disk. Postgres can run locally or behind SSH. Embeddings and enrichment can use local or cloud providers depending on your configuration. API keys stay in client env blocks and are not printed by generated MCP config.

**It works for a person or an organization.** The archive's central entity is configurable. PPA can represent an individual, a household, a company, an admin account, or another organizational identity without forking the codebase.

## Core Capabilities

### Multi-Source Ingest

PPA ships adapters that turn real services, exports, and local databases into canonical cards:

- Communication and meetings: Gmail, Gmail correspondents, iMessage, Beeper, Otter.ai
- Calendar and contacts: Google Calendar, Google Contacts
- People directories: LinkedIn exports, Notion people/staff CSVs, seed people
- Files, photos, and code: file libraries, Apple Photos, GitHub
- Health and medical: Apple Health exports, clinical/EHR records, FHIR JSON, CCD/XML, PDFs, Epic EHI TSVs
- Finance: Copilot transaction CSVs

Adapters are idempotent. Re-running an import should produce the same canonical cards instead of duplicating history.

### Typed Cards

PPA currently models 37 card types:

- Core entities: `person`, `place`, `organization`
- Communication: `email_thread`, `email_message`, `email_attachment`, `imessage_thread`, `imessage_message`, `imessage_attachment`, `beeper_thread`, `beeper_message`, `beeper_attachment`
- Time and media: `calendar_event`, `media_asset`, `document`, `meeting_transcript`
- Finance and health: `finance`, `medical_record`, `vaccination`
- Code: `git_repository`, `git_commit`, `git_thread`, `git_message`
- Derived transactions: `meal_order`, `grocery_order`, `ride`, `flight`, `accommodation`, `car_rental`, `purchase`, `shipment`, `subscription`, `event_ticket`, `payroll`
- System knowledge: `knowledge`, `observation`

The guiding rule is simple: a card should represent a proven action, booking, request, transaction, communication, entity, or durable observation. Marketing emails and passive notifications do not become first-class cards unless they contain structured evidence of something that actually happened.

### Enrichment

PPA can enrich sparse source cards before indexing them:

- Email transaction extraction classifies threads, skips noise, and emits typed cards for receipts, travel, purchases, subscriptions, rides, and payroll.
- Thread enrichment adds summaries and entity mentions to email, iMessage, and Beeper conversations.
- Finance enrichment classifies counterparties and links transactions to people, organizations, purchases, meals, and subscriptions.
- Document enrichment extracts text from supported file formats and adds summaries, dates, and entity mentions.

LLMs are used as enrichment tools, not as the archive of record. Their outputs are schema-validated, provenance-tagged, cached, and resumable.

### Linkers

PPA links related cards through deterministic and confidence-scored modules:

- Finance reconciliation links charges to purchases, meal orders, and subscriptions.
- Trip clustering links flights, accommodations, and car rentals using airports, cities, dates, and booking structure.
- Meeting artifact linking connects calendar events, meeting transcripts, and communication threads.
- Shipment linking connects tracking notices to purchase cards.
- Identity, communication, calendar, media, graph, and orphan-repair linkers fill out the broader relationship graph.

This is where PPA deliberately differs from naive semantic search. Similarity is useful for recall, but instance-level relationships need structural fingerprints: confirmation codes, source emails, amounts, dates, tracking numbers, IATA routes, calendar IDs, and shared participants.

### Retrieval

PPA supports several retrieval paths over the same archive:

- `archive_read` and `archive_read_many` for canonical evidence
- `archive_query` for structured filters by type, source, person, org, and date
- `archive_search` for lexical recall
- `archive_vector_search` for semantic recall
- `archive_hybrid_search` for lexical + vector + graph ranking
- `archive_temporal_neighbors` for "what happened around this time?"
- `archive_person`, `archive_graph`, and `archive_timeline` for relationship and chronology work
- `archive_retrieval_explain` for understanding why results ranked the way they did
- `archive_status_json`, `archive_embedding_status`, and related tools for operational health

Retrieval responses include confidence signaling. Sparse or surprising results are logged as retrieval gaps so maintenance can surface where the archive needs more data, better extraction, or better linking.

### Maintenance

PPA is designed for regular incremental operation:

- New imports write vault cards.
- Extractors produce derived cards from new source material.
- Entity resolution creates or updates people, places, and organizations.
- Incremental rebuilds update the index without reprocessing the whole vault.
- `ppa maintain` sequences the routine maintenance path and reports new cards, extracted cards, resolved entities, rebuild work, enrichment queue depth, retrieval gaps, skipped steps, and errors.

Full rebuilds remain available as a reset button, but the normal operating model is incremental.

### Performance And Correctness

PPA includes a Rust performance layer, `archive_crate`, for high-volume vault and index work. It accelerates vault walking, scan-cache building, manifest scanning, validation, row materialization, chunk construction, and batch operations while keeping adapters, enrichment, orchestration, and MCP in Python.

Correctness is treated as a product feature. Health checks validate vault structure, migrations, embeddings, graph behavior, and known query/answer pairs. Rebuild and linker workflows are covered by regression tests, and the Python engine remains available as a fallback via `PPA_ENGINE=python`.

## Architecture

```text
Sources and exports
  -> canonical Markdown vault
  -> Postgres + pgvector index
  -> CLI and MCP tools
  -> humans, editors, agents, assistants
```

The architecture is intentionally layered:

- **Vault:** Human-readable Markdown cards with YAML frontmatter and provenance. This is what you own and back up.
- **Index:** Derived Postgres tables for cards, chunks, embeddings, projections, edges, classifications, retrieval gaps, and operational state.
- **Graph:** Typed relationships with confidence, including wikilinks, materialized schema edges, derived-from links, and promoted link candidates.
- **Retrieval:** Exact, structured, lexical, vector, hybrid, temporal, graph, and explain tools over the same index.
- **MCP:** A stable tool surface for Cursor, Claude Desktop, Codex, and other MCP clients.

This separation is the core safety property. The archive can evolve, the index can be rebuilt, embeddings can be regenerated, and agents can change without surrendering the canonical record.

## Quick Start

### Requirements

- Python 3.10+
- Postgres with pgvector
- Optional embeddings provider: `hash` for local plumbing, `openai` or API-compatible providers for semantic quality

### Install

```bash
git clone https://github.com/rheeger/ppa.git
cd ppa
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

### Start Local Postgres And Build The Index

```bash
cp .env.pgvector.example .env.pgvector
make pg-up
make bootstrap-postgres
make rebuild-indexes
make embed-pending
```

### Run The MCP Server

```bash
ppa serve
```

For a paste-ready MCP client config:

```bash
ppa mcp-config
```

Minimum environment:

```bash
export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:5432/archive"
export PPA_INDEX_SCHEMA="archive_seed"
export PPA_PATH="/path/to/vault"
export PPA_EMBEDDING_PROVIDER="openai"
export PPA_EMBEDDING_MODEL="text-embedding-3-small"
```

Remote Postgres over SSH is supported:

```bash
ppa serve --tunnel user@host
```

Details: [MCP setup](archive_docs/MCP_SETUP.md), [runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md), and [example MCP config](archive_docs/examples/ppa.mcp-example.json).

## CLI Surface

The `ppa` command and MCP tools share the same retrieval semantics. Common commands:

```bash
ppa search "banh mi"
ppa hybrid-search "that flight to NYC"
ppa query --type meal_order
ppa temporal-neighbors "2025-12-27T18:00:00Z"
ppa person "Sarah"
ppa graph "People/sarah.md"
ppa read "hfa-email-message-..."
ppa status
ppa health
ppa maintain
```

Admin commands such as `rebuild-indexes`, `bootstrap-postgres`, migrations, embedding backfills, and linker operations are documented in the [runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md). Restrict admin tools in production with `PPA_MCP_TOOL_PROFILE`.

## Packages

Single editable install:

- `archive_cli`: CLI, MCP server, index, retrieval, embeddings, maintenance, linkers
- `archive_vault`: card schema, vault I/O, provenance, validation
- `archive_sync`: source adapters, extractors, enrichment, entity resolution
- `archive_doctor`: validation, dedupe, stats, vault quality tools
- `archive_crate`: Rust extension for high-volume vault scanning, validation, and index materialization

## Production Notes

- Set `PPA_FORBID_REBUILD=1` around real production databases unless a rebuild is intentional.
- Prefer local build, dump, restore, or a written deployment playbook for production index changes.
- Use read-only or remote-read MCP tool profiles for clients that should never mutate the index.
- Keep the vault backed up separately from Postgres. The vault is canonical; the index is recoverable.
- Use `ppa health`, `archive_status_json`, and embedding status checks before trusting retrieval after imports or maintenance.

Security, backup, and operations: [security model](archive_docs/SECURITY_MODEL.md), [backup and restore](archive_docs/PPA_BACKUP_AND_RESTORE.md), and [runbooks](archive_docs/runbooks/).

## Tests

```bash
.venv/bin/python -m pytest archive_tests/
```

The test suite covers schema validation, adapters, index behavior, MCP/CLI surfaces, graph/linker behavior, destructive-operation safeguards, migrations, and live pgvector integration when available.

## Documentation

- [Architecture](archive_docs/ARCHITECTURE.md)
- [Indexing](archive_docs/INDEXING.md)
- [Agent usage](archive_docs/AGENT_USAGE.md)
- [MCP setup](archive_docs/MCP_SETUP.md)
- [Runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md)
- [Card type contracts](archive_docs/CARD_TYPE_CONTRACTS.md)
- [Retrieval contract](archive_docs/RETRIEVAL_CONTRACT.md)
- [Linker architecture](archive_docs/LINKER_ARCHITECTURE.md)
- [Contributing linkers](archive_docs/CONTRIBUTING_LINKERS.md)

## Contributing

PRs are welcome. Run the focused tests for the area you touch, and run `pytest` for changes to card schemas, adapters, index materialization, retrieval, MCP, migrations, linkers, or operational safety. If CLI, environment, or MCP semantics change, update [the runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md); that file is the automation handshake.

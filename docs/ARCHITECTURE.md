# HFA Architecture

## Overview

HFA is a markdown-first archive system built around typed cards with YAML frontmatter, an optional markdown body, and field-level provenance. The vault is the source of truth. Adapters import raw data into cards, doctor commands maintain vault quality, and future MCP/query layers read the same shared structures.

## Core Design Principles

1. Markdown is the storage format, not a derived export.
2. Every meaningful field on disk is schema-validated.
3. Every non-empty field written by the system has provenance.
4. Deterministic data stays deterministic. LLMs can enrich, not invent protected fields.
5. Imports are idempotent and safe to re-run.
6. Vault behavior is configurable via `_meta/*.json`, not hidden constants in code.

## Main Components

### `skills/hfa/`

Shared library for:

- YAML parsing and rendering
- schema validation
- provenance parsing and enforcement
- vault iteration and atomic writes
- slug generation
- identity map IO and batch caching
- person resolution and merge rules
- LLM provider abstraction
- enrichment step interfaces
- config, sync state, UID, and hashing helpers

### `skills/archive-sync/`

Import adapters built on top of `hfa`.

- `fetch()` extracts raw source rows
- `to_card()` converts one source row into `(card, provenance, body)`
- `ingest()` in `adapters/base.py` owns identity resolution, merge/create/conflict flow, cursor updates, and atomic writes

### `skills/archive-doctor/`

Vault maintenance commands built on the same library.

- `dedup-sweep`
- `validate`
- `stats`
- `purge-source`

### `archive-mcp/`

Query interface for the vault. The current rewrite lives in the separate local `archive-mcp` workspace and consumes `hfa` directly via `HFA_LIB_PATH` instead of re-implementing note traversal or frontmatter parsing.

## Derived Index Layer

HFA remains vault-canonical. Query indexes are derived artifacts, not the source of truth.

Current implementation slice:

- `archive-mcp` builds a derived relational index from canonical cards.
- Exact lookup, structured query, lexical search, timeline, stats, and graph traversal should prefer the derived index instead of scanning the vault.
- Final answer grounding should still read canonical markdown cards.

Target serving architecture:

- Postgres as the metadata and operational plane
- `pgvector` as one derived retrieval index for semantic search
- normalized edge and external ID tables for multi-hop retrieval and provider history

Read these docs together:

- `docs/hfa/INDEXING.md`
- `docs/hfa/AGENT_USAGE.md`

## Card Anatomy

Every card has three layers:

1. YAML frontmatter
   Structured fields such as `uid`, `type`, `source`, `summary`, `emails`, `amount`.
2. Markdown body
   Human-readable notes or auxiliary text that does not belong in schema fields.
3. Provenance block
   Hidden HTML comment appended to the body. Tracks which pipeline step wrote each field and whether the value was deterministic or LLM-generated.

## Type System

Current concrete card types:

- `PersonCard`
- `FinanceCard`
- `EmailThreadCard`
- `EmailMessageCard`
- `EmailAttachmentCard`
- `CalendarEventCard`
- `IMessageThreadCard`
- `IMessageMessageCard`
- `IMessageAttachmentCard`
- `MediaAssetCard`

All card types extend `BaseCard`. New types should be additive: new model with defaults, registration in `CARD_TYPES`, and a clear vault path convention.

## Vault Layout

Canonical vault shape:

```text
hf-archives/
  _meta/
  People/
  Finance/YYYY-MM/
  Photos/YYYY-MM/
  Email/YYYY-MM/
  EmailThreads/YYYY-MM/
  EmailAttachments/YYYY-MM/
  Calendar/YYYY-MM/
  IMessage/YYYY-MM/
  IMessageThreads/YYYY-MM/
  IMessageAttachments/YYYY-MM/
  Attachments/
  _templates/
  .obsidian/
```

`hfa.vault.iter_notes()` skips `_meta`, `_templates`, `Attachments`, and `.obsidian`.

## Identity Resolution

Identity works in two layers:

1. Exact aliases in `_meta/identity-map.json`
   Email, phone, normalized name, and social handles map to a canonical person wikilink.
2. Fuzzy resolution over `People/`
   `resolve_person()` uses nickname expansion plus weighted scoring across name, email domain, and company to decide `merge`, `conflict`, or `create`.

Batch imports use `IdentityCache` so alias lookups and writes happen in memory until flush.

## Provenance And Indexing Contract

Introducing a database or vector index must not weaken provenance or schema flexibility.

Rules:

1. Canonical field provenance lives on the markdown card.
2. Derived indexes may mirror provenance-derived metadata, but they do not replace canonical provenance.
3. Embeddings are lossy search artifacts, never truth.
4. Additive schema evolution still happens in card models first; indexes materialize only what they need.
5. Agent answers must ground themselves in canonical cards before making claims.

## Anti-Hallucination Architecture

Protection is structural, not prompt-only:

1. Strict schema on write via `validate_card_strict()`
2. Permissive read for forward compatibility via `validate_card_permissive()`
3. Provenance coverage enforced before writes
4. `DETERMINISTIC_ONLY` fields blocked from `method="llm"`
5. LLM provider prompt grounding plus `input_hash` and `enrichment_version` for re-run safety

## Data Flows

### Import

`source -> fetch() -> to_card() -> resolve/merge/create -> write_card()`

For Apple Photos imports, the current adapter shape is:

- `osxphotos` reads the local Photos library metadata in read-only mode
- adapter-level quick-update compares `metadata_sha` against existing `Photos/` cards to skip unchanged assets
- private people labels and ML labels remain optional and flow through distinct provenance sources

### Derived Index Build

`vault -> archive-mcp index builder -> cards/external_ids/edges/search index`

### Agent Retrieval

`query -> exact/structured search -> lexical/vector recall -> graph expansion -> canonical read -> answer`

### Maintenance

`vault -> doctor command -> validate/dedup/stats -> report or rewrite`

### Enrichment

`card -> should_run() -> run() -> write_card()`

## Operational Invariants

- `uid` always starts with `hfa-`
- `source` is always a list
- person emails and phones are always lists
- writes are atomic
- non-empty fields require provenance
- deleting cards must clean identity-map and relevant sync-state entries
- the vault remains canonical even when a derived index exists
- derived indexes must be rebuildable from vault contents plus deterministic operational state
- search hits are not authoritative until grounded against canonical cards

## Current Migration State

As of this refactor checkpoint:

- `hfa` is implemented and tested
- `archive-sync` is migrated to `hfa`
- `archive-doctor` is migrated to `hfa`
- post-import automation and backup assets exist in `hey-arnold`
- `archive-mcp` has been rewritten locally as an external consumer of `hfa`, but that workspace is outside this repo and is not part of PR `#6`
- live VM rollout is still pending because the default deploy flow targets `main`; use `make deploy-workspace DEPLOY_BRANCH=hfa` for branch preview deploys before merge

# PPA Architecture

## Overview

PPA is a markdown-first archive. Typed cards with YAML frontmatter, an optional body, and field-level provenance are canonical. Adapters and connectors write cards. Doctor and maintain commands keep the vault and derived indexes honest. CLI and MCP query the **Rust serving index** published under `<vault>/_meta/rust-search-index` (`ACTIVE` generation). Postgres is the derived warehouse and test oracle, not the live query engine.

The current packages are `archive_vault`, `archive_sync`, `archive_cli`, `archive_doctor`, `archive_engine`, and `archive_crate`. There is no `skills/hfa/` or `skills/archive-sync/` tree. `ppa/` is this repository, not an external consumer via `HFA_LIB_PATH`.

## Core Design Principles

1. Markdown is the storage format, not a derived export.
2. Every meaningful field on disk is schema-validated.
3. Every non-empty field written by the system has provenance.
4. Deterministic data stays deterministic. LLMs can enrich, not invent protected fields.
5. Imports are idempotent and safe to re-run.
6. Vault behavior is configurable via a versioned instance file (`ppa.json`) plus `PPA_*` env. CWD discovery is a legacy adapter, not the scheduled-job contract.
7. Each archive instance has its own root, archive ID, warehouse schema, serving generation, and checkpoints. Saved scopes are reusable **filters**, not tenancy.

## Main Components

### `archive_vault`

Card schema (`CARD_TYPES`, 36 keys), vault I/O, provenance, UID, and path containment.

### `archive_sync`

Source adapters, extractors, enrichment, connectors (`sample.fixture`, SDK-migrated `gmail-messages` / `calendar-events`, remaining legacy adapters). Live Google is not implied by a fixture setup.

### `archive_cli`

CLI, MCP server, warehouse load, maintain/publish, status/readiness, setup/config/connect/backup adapters.

### `archive_engine`

Instance config, `ArchiveIdentity`, `AccessContext`, saved scopes, runtime ports.

### `archive_crate`

Rust walk, cache, materialize, chunk, person-batch, validator, and serving-index search/query/vector/hybrid/graph/timeline/neighbors. Incremental Rust remains (ANN fidelity, publication). A greenfield rewrite of scanner / FTS / MCP is not a current TODO.

### `archive_doctor`

Validate, dedupe, stats, vault quality.

## Serving vs warehouse

| Layer | Role |
| --- | --- |
| Vault Markdown | Canonical. `read` grounds here. |
| Rust serving index | Live MCP/CLI query. Published by `maintain` / `rebuild-indexes`. |
| Postgres | Derived warehouse, embeddings admin path, gate/corpus evidence, test oracle. |
| pgvector / Postgres FTS | Retired as live retrieval. |

A config manifest is **not** freshness. Per-source staleness and per-stage watermarks (journal / materialized / published) are the honesty surface.

## Card Anatomy

Every card has three layers:

1. YAML frontmatter — `uid`, `type`, `source`, `summary`, and type-specific fields.
2. Markdown body — notes that do not belong in schema fields.
3. Provenance block — hidden HTML comment tracking which step wrote each field.

## Type System

`CARD_TYPES` has **36** keys: `person`, `place`, `organization`, communication (`email_*`, `imessage_*`, `beeper_*`), `calendar_event`, `media_asset`, `document`, `meeting_transcript`, finance/health (`finance`, `medical_record`, `vaccination`), git (`git_repository`, `git_commit`, `git_thread`, `git_message`), derived transactions (`meal_order`, `grocery_order`, `ride`, `flight`, `accommodation`, `car_rental`, `purchase`, `shipment`, `subscription`, `event_ticket`, `payroll`), and system types `knowledge` / `observation`.

`knowledge` and `observation` stay in schema so a later increment can write sourced cards. There is no populated knowledge cache and no 46-facet living profile. `archive_knowledge` falls back to search.

## Vault Layout

An independent instance is a directory that owns `ppa.json` (or `_meta/ppa-instance.json`) plus card trees. Typical families:

```text
<instance-root>/
  ppa.json
  People/
  Entities/Organizations/
  Email/YYYY-MM/
  Calendar/YYYY-MM/
  _meta/rust-search-index/
```

The historical HFA sketch (`hf-archives/`, `IMessage/`, seed path) is one instance, not the default for new installs. New instances do not inherit `local_seed_living_corpus`.

## Graph

Warehouse `edges` have no `method` / `confidence` / `evidence_uids`. Serving graph stores `trust` (default 1.0). Seed-link confidence is gated.

## Identity Resolution

1. Exact aliases in `_meta/identity-map.json`.
2. Fuzzy resolution over `People/`.

## Provenance And Indexing Contract

1. Canonical field provenance lives on the markdown card.
2. Derived indexes may mirror provenance-derived metadata; they do not replace it.
3. Embeddings are lossy search artifacts, never truth.
4. Additive schema evolution happens in card models first.
5. Agent answers must ground themselves in canonical cards.

## Anti-Hallucination Architecture

1. Strict schema on write via `validate_card_strict()`
2. Permissive read for forward compatibility
3. Provenance coverage enforced before writes
4. `DETERMINISTIC_ONLY` fields blocked from `method="llm"`
5. LLM outputs schema-validated, provenance-tagged, cached

## Data Flows

### Import

`source -> connector/adapter -> write_card()` inside the instance root.

### Maintain / publish

`vault -> processors (optional) -> warehouse materialize -> serving publish ACTIVE`

### Agent Retrieval

`query -> Rust serving (exact/structured/lexical/vector/hybrid/graph/timeline) -> canonical read`

### Instance health

`ppa status` / `ppa instance-status` / `ppa readiness` evaluate the **current instance**. Missing source or provider capability is `pending` / `unavailable`. Stale or down sources stay visible. Formal `ready: true` is not a product claim. `ppa analytics` / `archive_analytics` are shipped. `production_proven=false` until a long soak. See [STATUS.md](STATUS.md).

## Operational Invariants

- `uid` always starts with `hfa-`
- `source` is always a list
- writes are atomic
- the vault remains canonical
- derived indexes must be rebuildable
- two instances may share external IDs and still keep distinct archive IDs, roots, schemas, serving paths, and checkpoints
- Arnold HTTP MCP is a historical host choice, not the product architecture

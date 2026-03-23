# Typed Projection Architecture

## Purpose

`archive-mcp` now treats typed projections as a first-class derived layer instead of a collection of special cases.

The architecture has three derived responsibilities:

- universal generic substrate for every canonical card
- one typed relational projection for every current canonical card type
- one introspectable registry that drives schema, materialization, status, and audit output

## Layers

### Generic Substrate

These tables are universal:

- `cards`
- `card_sources`
- `card_people`
- `card_orgs`
- `external_ids`
- `duplicate_uid_rows`
- `edges`
- `chunks`

### Typed Projections

Each canonical semantic type gets one typed table:

- `people`
- `finance_records`
- `medical_records`
- `vaccinations`
- `email_threads`
- `email_messages`
- `email_attachments`
- `imessage_threads`
- `imessage_messages`
- `imessage_attachments`
- `beeper_threads`
- `beeper_messages`
- `beeper_attachments`
- `calendar_events`
- `media_assets`
- `documents`
- `meeting_transcripts`
- `git_repositories`
- `git_commits`
- `git_threads`
- `git_messages`

## Registry Ownership

The projection registry now owns:

- table name
- applicable canonical card type
- column definitions
- load and clear ordering
- projection builder name
- projection explain name

The registry is the source of truth for:

- projection inventory
- projection status
- typed table creation
- typed row explainability
- audit docs

## Shared Typed Table Shape

Every typed table includes shared structural columns:

- `card_uid`
- `rel_path`
- `card_type`
- `summary`
- `created`
- `updated`
- `primary_source`
- `source_id`
- `activity_at`
- `external_ids_json`
- `relationships_json`
- `typed_projection_version`
- `canonical_ready`
- `migration_notes`

Then each domain table adds its own stable semantic fields.

## Data Rules

- canonical scalar deterministic fields become direct typed columns
- stable list fields become JSONB arrays unless there is a strong reason to normalize further
- unstable or nested source payloads remain JSONB
- typed rows do not create truth; they materialize canonical truth plus readiness metadata
- `canonical_ready` and `migration_notes` exist so migration gaps are visible in the data plane itself

## Service Surface

Projection inspection is part of the public archive service surface:

- projection inventory
- projection status
- projection explain by `card_uid`

That means typed projections are not hidden internal implementation detail anymore.

# Typed projections

Typed projections make structured questions possible across imported services. A purchase amount or calendar date becomes a predictable field, so a client can filter and aggregate records without parsing each provider's original format again.

A projection is the queryable representation of fields in a stored card. The card remains authoritative, so the projection can be rebuilt without reconnecting the original service.

## Shared and type-specific tables

The generic tables describe every card: `cards`, `card_sources`, `card_people`, `card_orgs`, `external_ids`, `duplicate_uid_rows`, `edges`, and `chunks`.

Each registered card type also has a typed projection for its own fields. Examples include `people`, `finance_records`, `email_messages`, `calendar_events`, `documents`, and the transaction tables. The complete registrations live in `archive_cli/card_registry.py` and `archive_cli/projections/`; see [card type contracts](CARD_TYPE_CONTRACTS.md).

## Registry ownership

The registry declares the table name, applicable card type, columns, load and clear order, builder, and explain function. Schema creation, inventory, status, and audit output use that same registration.

When adding a type, update the card contract and projection registry together. A contributor should not need to discover a second list of special cases before a new record becomes queryable.

## Shared table fields

Each typed table includes `card_uid`, `rel_path`, `card_type`, `summary`, `created`, `updated`, `primary_source`, `source_id`, `activity_at`, `external_ids_json`, `relationships_json`, `typed_projection_version`, `canonical_ready`, and `migration_notes`. Domain tables add their stable type-specific fields.

Deterministic scalar fields become typed columns. Stable lists normally become JSONB arrays, while nested or unstable source payloads remain JSONB. `canonical_ready` and `migration_notes` expose migration gaps to the reader of the projection.

## Inspect a projection

`ppa projection-inventory`, `ppa projection-status`, and `ppa projection-explain <uid>` expose registration, coverage, and a card's derived row. Use them when testing a new field or diagnosing why a structured query cannot use it.

The [contributor playbook](PLAYBOOK.md#adding-a-new-card-type) covers the implementation steps.

# Evidence query contract (P10-A)

**Version:** `p10a.1`  
**Owner:** P10 evidence-query. Neighbor context (P10-B), workflows (P10-C), and CLI/MCP registration (P10-D) extend this document; they do not replace it.

This is the first product surface that can read a **full eligible set** (or say that it did not). Clients synthesize answers from the returned rows. The archive does not answer for you.

## Execution

Typed queries go through `archive_engine.query.execute_typed_query` and an explicit `AccessContext`. Retrieval uses `runtime.retrieval` / `runtime.query`. Native serving implements filter + keyset pagination over registered metadata fields. Postgres is a **read-only warehouse analytical adapter** for allowlisted aggregates, not a semantic-search fallback.

Saved scopes are **not resolved here**. A `saved_scope_name` is rejected until P09. Explicit request filters always work. Scope never grants authority.

## Typed request

`StructuredQueryRequest` carries:

- `archive_id` and `AccessContext` (required; deny is explicit)
- optional predicate AST and/or legacy `type_filter` / `source_filter` / `people_filter` / `org_filter` / date filters
- selected safe fields, order field/direction, page size, cursor
- optional `count` / `sum` aggregate
- optional as-of checkpoint / serving snapshot

Predicate operators: `eq`, `in`, numeric/date `lt` / `lte` / `gt` / `gte`, `exists`, bounded `and` / `or`.

Limits: AST depth 4, 16 terms, in-list length 32, page size 200, 32 returned fields. Unknown fields, type-incompatible operators, unsupported cross-type field access, and any `sql` / `raw_sql` key are rejected **before** execution.

Registered serving fields: `uid`, `type`, `source`, `people`, `org`, `activity_at`, `corpus_state`, `summary`, `slug`, `emails`, `domains`. Type-specific projection fields (`amount`, `currency`, `event_type`) require a compatible `type` constraint.

## Pagination and completeness

Cursors are opaque, versioned, HMAC-integrity-protected, and bound to:

- archive ID
- serving/warehouse snapshot
- access-policy fingerprint
- predicate fingerprint
- order field and direction
- last `(order_value, uid)` keyset (nulls last; UID breaks ties)

Changing filter, policy, or generation invalidates the cursor. Tampered or expired snapshots fail closed with `CursorInvalidError` (`cursor snapshot is stale; restart the query`).

`QueryPage` always distinguishes:

- `matched_total` + `total_status` (`exact` or `unknown`)
- `complete` / `truncated`
- `snapshot` vs `warehouse_checkpoint` (do not combine them as one consistent result without reconciliation)
- `coverage` = `eligible_stored` — never “every real-world event”
- `freshness` independent of coverage

Counts and sums are computed over the **full eligible set after AccessContext**, or marked `unknown`. They are never derived from the current page alone. Restricted scopes are applied **before** totals.

## Warehouse analytics

`archive_engine.analytics.warehouse` compiles `count` / `sum` to parameterized, allowlisted SQL. Clients cannot submit SQL. Statement/row/group limits apply. Mixed serving generation and warehouse checkpoint without reconciliation is rejected.

## Compatibility

`archive_cli/commands/query.py` maps existing type/source/people/org filters onto this contract. Central parser / MCP registration waits for P09-C (P10-D). Simple query row membership is preserved.

## Later slices

- **P10-B** (needs P01-D): neighbor context and bounded graph. Do not implement here.
- **P10-C**: subscription lifecycle, trip costs, changes-since.
- **P10-D**: saved scopes and installed CLI/MCP evidence bundles.

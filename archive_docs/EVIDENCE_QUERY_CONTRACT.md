# Evidence and analytical query contract

A person asking for a total needs to know whether the answer includes every eligible stored record. PPA's typed queries and analytical workflows return completeness, coverage, freshness, and source references so a client can distinguish a supported result from a partial view.

The contract version is `p10d.1`. The client interprets and explains the returned facts; PPA performs the bounded retrieval and arithmetic.

## Execution

Typed queries go through `archive_engine.query.execute_typed_query` and an explicit `AccessContext`. Retrieval uses `runtime.retrieval` / `runtime.query`. The native index filters and paginates registered metadata fields. Postgres supplies a read-only analytical adapter for allowed aggregates. Live semantic search uses the native index.

Saved scopes resolve through `archive_engine.scopes.resolve_effective_scope`. A named preset is looked up in the instance/fixture catalog. Unknown names fail closed (`QueryValidationError`). Request filters replace the same preset dimension. AccessContext is an upper bound and is never widened. An empty intersection is `empty_scope` with zero rows and does not widen into unscoped search.

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
- `coverage` is `eligible_stored`, which covers stored records rather than every real-world event.
- `freshness` independent of coverage

Counts and sums are computed over the **full eligible set after AccessContext**, or marked `unknown`. They are never derived from the current page alone. Restricted scopes are applied **before** totals.

## Warehouse analytics

`archive_engine.analytics.warehouse` compiles `count` / `sum` to parameterized, allowlisted SQL. Clients cannot submit SQL. Statement/row/group limits apply. Mixed serving generation and warehouse checkpoint without reconciliation is rejected.

## Compatibility

`archive_cli/commands/query.py` maps existing type/source/people/org filters onto this contract. Simple query row membership is preserved. `ppa analytics` and `archive_analytics` share `archive_cli/commands/analytics.py`.

## Neighbor context

After ranking, `archive_engine.context.expand_neighbors` adds at most one preceding and one following unit on the same thread / section / burst lane. Matched units and expansion units are separate lists with reasons (`ranked_hit`, `preceding_message`, `following_message`, `adjacent_chunk`).

Every unit cites UID, chunk/message IDs, revision hash, and a half-open UTF-8 `SourceSpan`. Token estimates use the CLI whitespace-split bound (defaults: 2k per hit, 8k total). Overlapping expansions are dropped. When the budget is exhausted, the engine skips extra context units and preserves citations on the units it returns.

If generation offsets no longer match the canonical file revision, the result is `stale-context/refresh-required` and no mismatched text is quoted. `span_unavailable` fails a span-required request even when the UID was retrieved. Denied and mixed-source neighbors are absent, not redacted. No `authorized=true` field is emitted.

## Bounded graph

`serving_index_graph_bounded` enforces depth (default 1, public max 2), max nodes/edges, elapsed budget, and optional relation-type filters **during** native BFS. High-degree hubs return a partial graph with `truncated`, `truncation_reason`, `frontier`, and surviving edge citations (`method`, `evidence_uids`). Denied neighbors are never entered.

## Deterministic workflows

`archive_engine.analytics` exposes three finite workflows over the full eligible set after AccessContext. They return facts, arithmetic, and ambiguity. They do not advise and they do not invent a current subscription.

- **Subscriptions** (`rel-p04b-renewal-is-not-current`): group by service/account/plan. Latest event is last-observed (`last_observed_renewed` / `last_observed_canceled` / …). A later cancel conflicts with reading an old renewal as current. Simultaneous contradictions are `conflict`. Freshness stays `unknown` unless the source proves otherwise.
- **Trip costs** (`rel-p04b-same-trip`, `rel-p04b-same-charge`): membership is confirmation / order / source-email identity. Lookalikes and proximity-only cards are excluded or unmatched. The supported actual charge is counted once; booking and segment estimates are listed separately. Currency groups never convert. Refunds stay negative. `q-p04b-agg-eur-net` is 100.00 + (−40.00) = 60.00 EUR.
- **Changes since checkpoint**: committed journal records after a sequence, plus decision labels. Create/update/delete/correct/merge stay distinct. `embed` / `legacy_dirty` are derived refreshes, not life events. Deletes are bodyless tombstones. Cursors are sequence + snapshot bound.

Evidence kinds remain `source_reported`, `derived`, `proposed_link`, or `unknown`. Proposed observations cannot become source facts. Totals refuse a truncated page.

## CLI and MCP clients

CLI `ppa analytics {query,context,subscriptions,trip-costs,changes-since}` and MCP `archive_analytics` return the same JSON contract (`client_contract_version=p10d.1`):

- rows / hits, citations, totals (`matched_total`, `total_status`)
- effective / saved scope payload
- completeness: `complete`, `truncated`, `coverage=eligible_stored` (or `empty_scope`), `freshness`
- `served_checkpoint` and `materialized_checkpoint` remain distinct; divergence is reported as `stale`.
- evidence kinds: `source_reported` / `derived` / `proposed_link` / `unknown`
- `production_proven=false`

`ppa query` / `archive_query` and `ppa evidence` / `archive_evidence` accept `--saved-scope` / `saved_scope_name`. Context expansion labels matched vs adjacent units. Legacy simple query still works.

These clients do not convert currency, do not emit financial or health advice, and do not invent a current subscription.

Implementation evidence is recorded in [the client validation report](reports/p10-runtime-capability-delta.md). Changes to this contract should update [agent usage](AGENT_USAGE.md) and the [runtime contract](PPA_RUNTIME_CONTRACT.md).

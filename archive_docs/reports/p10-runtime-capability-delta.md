# P10 runtime capability delta

**Slice:** P10-D. **Contract:** `p10d.1`. **`production_proven`:** false.

P09-D owns public README / ARCHITECTURE / MCP_SETUP / runtime-contract rewrites. This file is the P10 capability delta only — do not treat it as a matrix rewrite.

## Newly supported after D

| Surface | Status after P10-D | Notes |
| --- | --- | --- |
| `ppa analytics query` / `archive_analytics workflow=query` | supported (isolated) | Same typed request/result as `execute_typed_query`. Saved scopes resolve. Legacy `ppa query` / `archive_query` still work. |
| `ppa analytics context` / `archive_analytics workflow=context` | supported (isolated) | Matched vs context labels; stale offsets fail closed. |
| `ppa analytics subscriptions` / `workflow=subscription_lifecycle` | supported (isolated) | Last-observed state. Renewal is not current. |
| `ppa analytics trip-costs` / `workflow=trip_costs` | supported (isolated) | Identity joins, no FX, actual charge counted once. |
| `ppa analytics changes-since` / `workflow=changes_since` | supported (isolated) | Journal create/correct/delete; derived refreshes labeled. |
| Saved scopes on query / evidence / analytics | supported (isolated) | Unknown preset fails. Empty intersection is empty-scope, not search-everything. Cannot widen AccessContext. |

P09 setup / config / connect / recovery parsers are unchanged. P04 fixture goldens (`rel-p04b-*`, `q-p04b-*`) are the proof corpus — not a new P04 matrix.

## Completeness fields clients must show

- `coverage` (`eligible_stored` or `empty_scope`) — never “every real-world event”
- `freshness` independent of coverage
- `complete` / `truncated` / `completeness_label`
- `served_checkpoint` vs `materialized_checkpoint` (`checkpoint_agreement=diverged` → visibly stale)
- evidence kinds: `source_reported` / `derived` / `proposed_link` / `unknown`

## Rust retrieval vs warehouse analytics

- **Rust serving index** remains the retrieval engine for filter / pagination / neighbor graph.
- **Postgres warehouse** is a read-only allowlisted adapter for typed `count` / `sum` only. Clients cannot submit SQL.
- Do not combine a serving generation and a warehouse checkpoint as one consistent result when they differ.

## Still false / out of scope

- `production_proven=false`
- No FX conversion, no financial or health advice, no living profiles
- No rewrite of P09 public docs in this slice

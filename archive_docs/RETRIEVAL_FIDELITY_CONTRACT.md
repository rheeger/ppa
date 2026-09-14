# Retrieval fidelity contract

A query across services needs to preserve what each source established and which connections PPA inferred. This contract maps warehouse fields into the native search index. Export must retain those distinctions, including missing evidence.

Serving fields and warehouse columns have different names. For example, the warehouse has no `trust` column; an export must not invent one and present it as a source fact.

Shared record types (`EmbeddingSpec`, `ServingEdge`, `EvidenceEnvelope`,
`ChunkEvidenceRef`, …) are imported from `archive_engine.contracts`. Consumers
must not redefine them.

## Policy invariants

- Missing optional values are empty or `unknown`.
- Missing trust-critical policy metadata **must not** upgrade to `active` or
  `trust=1.0`.
- `suppressed` cards are excluded from search, query, vector, hybrid, timeline,
  person, and graph-neighbor result paths.
- `quarantine` remains retrievable at weight `0.35`
  (`archive_cli.corpus_hygiene.state_store.QUARANTINE_RETRIEVAL_WEIGHT`) unless
  an explicit policy config overrides that constant consistently.
- Deterministic, manual, LLM, and inferred evidence are not conflated.
- An inferred edge cannot acquire deterministic status merely by being
  materialized into serving JSON.
- Provenance labels are not inferred from exact string matching.

## ServingCard field mapping

| Serving field | Warehouse source | Missing / default |
| --- | --- | --- |
| `card_uid` | `cards.uid` | required; skip empty |
| `rel_path` | `cards.rel_path` | required |
| `type` | `cards.type` | required |
| `summary` | `cards.summary` | `""` |
| `slug` | `cards.slug` | `""` |
| `activity_at` | `cards.activity_at` | `""` |
| `activity_end_at` | `cards.activity_end_at` | `""` |
| `search_text` | `cards.search_text` | `""` |
| `source_revision` | `cards.content_hash` | `""` (unknown revision) |
| `people` | `card_people.person` | `[]` |
| `sources` | `card_sources.source` | `[]` |
| `orgs` | `card_orgs.org` | `[]` |
| `aliases` | `people.aliases_json` (person typed projection) | `[]` when table/row absent |
| `emails` | `people.emails_json` (person typed projection) | `[]` when table/row absent |
| `external_ids` | `external_ids.external_id` (normalized unique strings) | `[]` |
| `corpus_state` | `card_corpus_state.corpus_state` | `unknown`; never `COALESCE` to `active` |
| `retrieval_weight` | derived only from a **known** corpus state | `null` when state is unknown |
| `provenance_summary` | no card-level warehouse provenance table | `unknown` |

There is no warehouse `accounts` column. Serving does not invent accounts from
display names. `card_sources` is the source-label surface.

`normalize_corpus_state()` in the warehouse query path still coalesces missing
rows to `active` for Postgres retrieval. Serving export **does not** use that
helper. Known states are only `active`, `quarantine`, and `suppressed`.

| Known `corpus_state` | `retrieval_weight` | Candidate path |
| --- | --- | --- |
| `active` | `1.0` | included |
| `quarantine` | `0.35` | included, demoted |
| `suppressed` | `0.0` | exported for state fidelity; excluded from result paths |
| `unknown` / absent row / absent table | `null` | included, unlabeled, not trusted |

## ServingEdge field mapping

Warehouse `edges` has `source_uid`, `target_uid`, `edge_type`, `field_name`.
It has **no** `confidence`, `method`, `evidence_uids`, or `trust` columns.

Promoted inferred links live on `link_candidates` + `link_decisions` +
`promotion_queue` (`promotion_target='derived_edge'`, `promotion_status='applied'`).

| Serving field | Warehouse `edges` row | Promoted seed-link row | Missing |
| --- | --- | --- | --- |
| `source_uid` / `target_uid` | `edges.source_uid` / `target_uid` | `link_candidates.source_card_uid` / `target_card_uid` | skip empty |
| `edge_type` | `edges.edge_type` | `link_candidates.proposed_link_type` | skip empty |
| `field_name` | `edges.field_name` | `promotion_queue.target_field_name` | `""` |
| `direction` | always `forward` (source → target) | always `forward` | kept even when traversal also discovers the reverse |
| `confidence` | `1.0` via the existing edge-confidence contract (`index_query._graph_neighbor_uids`) | `link_decisions.final_confidence` | omitted or `null`, not `1.0` |
| `method` | `unknown` (no method column) | `inferred` (row origin is the derived-link surface) | `unknown` |
| `evidence_uids` | not in `edges` | `link_evidence` is feature rows, not card UIDs | `[]` |
| `trust` | **derived** from `confidence` for old readers | **derived** from `confidence` only when present | omitted; never default `1.0` |

`ServingEdge` from `archive_engine.contracts` carries `method`, `confidence`,
and `evidence_uids`. Effective neighbor trust for ranking uses
`confidence` if present, else a present `trust` compatibility field, else
unknown (no graph boost).

## Exact identifier retrieval

Exact identifier lookup must cover these fields on exported live (non-suppressed) cards:

- `cards.uid` (exact)
- `cards.slug` (case-insensitive)
- `people.aliases_json` / `people.emails_json`
- `external_ids.external_id`
- `cards.rel_path`

Hits cite `card_uid`, `source_revision` (`content_hash`), `serving_generation`,
and `match_channel=exact`. The shared `ChunkEvidenceRef` contract identifies the revision and source span used for quoted passages.

## Embedding spec

`EmbeddingSpec` identity is required for cache/generation compatibility:

- `provider_namespace` ← `PPA_EMBEDDING_PROVIDER` or `unspecified`
- `model` ← `PPA_EMBEDDING_MODEL`
- `model_revision` ← `PPA_EMBEDDING_VERSION`
- `dimension` ← `PPA_VECTOR_DIMENSION`
- `metric` ← `cosine` (serving cosine space)
- `normalization` ← `l2`
- `chunk_schema` ← `CHUNK_SCHEMA_VERSION`

`QueryEmbedCache` may reuse a vector only under the same spec identity.
The vector artifact layout must preserve the same embedding identity.

## Compatibility

Old `cards.jsonl` rows without the new fields deserialize with empty/unknown
defaults. Old `edges.jsonl` rows that only have `trust` still apply that value
as compatibility trust; `method` stays `unknown` unless present. New writers
emit `method` / `confidence` / `evidence_uids` / `direction` additively.

`matched_by` remains for existing clients. New clients should read
`match_channel` (`exact` / `lexical` / `vector` / `hybrid` / `graph` /
`seed-link`).

## Ranking version

The native serving format is version `2`, with vector implementation `ivf_centroids_v2`. Centroids, assignments, checksums, and `EmbeddingSpec` are required. Older modulo-IVF generations are rejected and need a rebuild.

Native ranking uses `p01b2-rrf-1`; the Python retrieval pipeline identifies itself as `2026.09.06.p01b2`. The quarantine weight is `0.35`. These values are defined in `archive_crate/src/serving_index/schema.rs` and `archive_cli/retrieval_pipeline.py`.

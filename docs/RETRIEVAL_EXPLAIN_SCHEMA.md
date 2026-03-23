# Retrieval explain payload (v2)

Stable JSON shape returned by `DefaultArchiveStore.retrieval_explain()` when `retrieval.explain.enabled` is true (default).

## Top-level fields

| Field                  | Type    | Description                                                                              |
| ---------------------- | ------- | ---------------------------------------------------------------------------------------- |
| `schema`               | string  | Always `archive_retrieval_explain_v2`.                                                   |
| `pipeline_version`     | string  | Retrieval pipeline revision (ties to `archive_mcp.retrieval_pipeline.PIPELINE_VERSION`). |
| `query`                | string  | Original query text.                                                                     |
| `mode`                 | string  | `hybrid` or `vector`.                                                                    |
| `query_plan`           | object  | Planner output: `planner_provider`, `queries[]`, `inferred` filters/hints.               |
| `candidate_generation` | object  | Counts: lexical/vector candidates, graph neighbors, subqueries used.                     |
| `fusion_strategy`      | string  | e.g. `lexical_vector_union_with_graph_boost` or `vector` for vector-only.                |
| `reranker`             | object? | Provider id and `enabled` flag when reranking ran or is configured.                      |
| `results`              | array   | One entry per ranked hit (see below).                                                    |

## Result entry

| Field                  | Type     | Description                                                                                                                                                    |
| ---------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `card_uid`             | string   | Canonical card uid.                                                                                                                                            |
| `rel_path`             | string   | Vault-relative markdown path.                                                                                                                                  |
| `matched_by`           | string[] | Signals that fired (e.g. `hybrid`, `lexical`).                                                                                                                 |
| `score_components`     | object   | Named contributions: `exact_boost`, `lexical_component`, `vector_component`, `graph_boost`, `type_prior`, `recency`, `provenance`, `rerank_contribution`, etc. |
| `rerank_score`         | number   | Reranker score in `[0,1]` when a reranker ran; else `0`.                                                                                                       |
| `final_score`          | number   | Blended score used for ordering.                                                                                                                               |
| `context`              | object   | Structured `build_context_json` fields (card type, summary, projections, …).                                                                                   |
| `context_text`         | string   | Line-oriented context string for agents.                                                                                                                       |
| `winning_chunk`        | object?  | `chunk_type`, `chunk_index`, `preview` when a chunk anchored the hit.                                                                                          |
| `why_this_ranked_here` | string   | Short human/debug summary of the dominant signal.                                                                                                              |

## Contract

Explain output is derived-only: it describes ranking over the index, not canonical truth. Ground answers with `archive_read` / canonical card content.

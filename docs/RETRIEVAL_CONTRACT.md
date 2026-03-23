# Retrieval Contract

This document defines the retrieval modes that `archive-mcp` must keep distinct.

## Retrieval Modes

### Exact Read

Purpose:

- fetch one canonical card by path or UID

Truth rule:

- canonical markdown is the answer source

### Structured Query

Purpose:

- filter cards by deterministic fields such as type, source, people, or org

Truth rule:

- query results come from the generic derived substrate
- final claims still require canonical grounding if used in an answer

### Lexical Search

Purpose:

- fast term/phrase recall over `cards.search_text`

Truth rule:

- lexical hits are retrieval aids, not canonical truth

### Semantic Search

Purpose:

- vector retrieval over derived `chunks`

Truth rule:

- embeddings are lossy artifacts
- vector hits must be grounded back to canonical cards

### Hybrid Search

Purpose:

- combine lexical, vector, graph, type, and provenance-aware ranking

Truth rule:

- hybrid ranking is optimized retrieval, not canonical truth

### Graph Expansion

Purpose:

- expand neighboring evidence around a card

Truth rule:

- graph edges are derived from canonical references plus approved derived link surfaces

## Explainability Requirements

The retrieval system should be able to explain:

- matched mode(s)
- score components
- provenance bias
- graph contribution
- typed projection names associated with the matched card type

Minimum explain payload fields:

- `query`
- `mode`
- `results[].card_uid`
- `results[].rel_path`
- `results[].matched_by`
- `results[].score_components`
- `results[].context`

## Archive Context

Archive context is derived metadata attached to retrieval results so agents can reason better about matches.

Minimum context fields:

- `card_type`
- `source_labels`
- `people`
- `orgs`
- `time_span`
- `provenance_bias`
- `graph_neighbor_types`
- `typed_projection_names`

This context must be derived from canonical or generic substrate data, not manually curated as a second truth source.

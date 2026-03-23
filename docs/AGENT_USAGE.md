# HFA Agent Usage

## Goal

Agents should use HFA and `ppa` to retrieve evidence, not to replace the canonical archive.

## Retrieval Order

Use retrieval methods in this order:

1. exact lookup for UID, path, email, phone, handle, and provider IDs
2. structured query for type, source, person, org, and date filters
3. lexical search for keyword and phrase recall
4. vector retrieval for vague or cross-cutting natural-language recall when you already know the scope
5. hybrid retrieval when you want exact lexical anchors plus semantic and graph expansion
6. graph expansion to collect neighboring evidence
7. canonical card reads before final answers

## Grounding Rules

- Read canonical cards before making factual claims.
- Do not treat search hits or embeddings as canonical truth.
- Prefer deterministic fields over inferred summaries when they disagree.
- If evidence conflicts, surface the conflict instead of collapsing it silently.
- If retrieval confidence is low, ask a follow-up question or narrow the scope.

## Graph Usage

Use graph expansion when the question depends on relationships:

- thread to messages
- message to attachments
- person to thread or message
- message or thread to calendar event
- entity to external IDs or provider accounts

Do not expand arbitrarily. Prefer small neighborhoods around the best anchors first.

## Provenance And Trust

Agents should assume:

- deterministic fields are stronger than LLM summaries
- provenance on canonical cards is the trust boundary
- derived indexes are navigation aids

When ranking or summarizing:

- prefer newer canonical evidence when facts conflict
- keep provenance confidence as a ranking signal
- call out when evidence comes from summaries instead of raw message or event content
- prefer deterministic/body-backed chunks over LLM-derived summary chunks when both match

For embedding lifecycle work:

- use `archive_embedding_status` to understand coverage for a model/version
- use `archive_embedding_backlog` to inspect pending chunks
- use `archive_embed_pending` only as an operational step, not as a reasoning shortcut
- do not confuse “pending embedding” with “missing canonical data”

For semantic retrieval:

- use `archive_vector_search` when you want semantic recall with card-level grouping
- use `archive_hybrid_search` when you want lexical anchors plus semantic and graph expansion
- use filters aggressively on semantic tools: `type_filter`, `source_filter`, `people_filter`, `start_date`, and `end_date`
- still read canonical cards before answering
- treat `hash`-provider semantic results as plumbing-grade, not production-grade relevance
- prefer a real provider such as the OpenAI-compatible path when semantic relevance quality matters
- read explanation metadata in retrieval output: `matched_by`, `score`, `chunk`, `graph_hops`, and `provenance_bias`
- if a result only matches on low-confidence summary text, keep digging before making a factual claim

## Operational Safety

- If imports have run recently, rebuild indexes before relying on derived retrieval.
- If `ppa` is configured against Postgres, treat that Postgres index as the primary retrieval surface.
- If exact reads succeed but search looks stale, treat the index as stale and rebuild it.
- If the index is unavailable, fall back to canonical reads instead of inventing missing results.

## Practical Tool Usage

Current `ppa` tool order:

- `archive_read`
- `archive_query`
- `archive_search`
- `archive_vector_search`
- `archive_hybrid_search`
- `archive_graph`
- `archive_person`
- `archive_timeline`
- `archive_stats`
- `archive_validate`
- `archive_bootstrap_postgres`
- `archive_rebuild_indexes`
- `archive_index_status`
- `archive_embedding_status`
- `archive_embedding_backlog`
- `archive_embed_pending`
  Use `archive_rebuild_indexes` and `archive_index_status` as operational tools, not as substitutes for grounding.

Chunk rows are retrieval substrate for current vector and hybrid search. They improve recall and ranking, but they are not canonical evidence on their own.

# Retrieval contract

People approach their history with different questions. Sometimes they know an exact identifier; sometimes they remember only a topic or a nearby event. PPA offers several retrieval modes over the same catalog so the client can choose the method that fits.

## Retrieval modes

| Mode | Use it to… | Evidence rule |
| --- | --- | --- |
| Exact read | Open a card by path or UID | Canonical Markdown supplies the stored record |
| Structured query | Filter by type, source, person, organization, date, or supported fields | Results come from the derived index; read cards for factual claims |
| Lexical search | Find terms and phrases | Ranked hits locate evidence |
| Semantic search | Find related meaning in derived chunks | Vector similarity is not proof of a fact or relationship |
| Hybrid search | Combine lexical, vector, graph, type, and provenance signals | Read the saved records before citing facts |
| Graph expansion | Follow related records | Edges derive from card references and approved link paths; preserve evidence kind |
| Timeline and neighbors | Recover activity and surrounding context | Nearby records do not establish causation |
| Evidence and analytics | Read a bounded evidence set or compute a supported total | Report completeness, coverage, freshness, and conflicts |

Live retrieval uses the published Rust index, so searching imported history does not require the original providers to be online. Semantic search requires configured embeddings. Allowlisted analytical aggregates can use the Postgres warehouse under the [evidence query contract](EVIDENCE_QUERY_CONTRACT.md). Do not treat a partial search page as a full-set analytical result.

## Explain the match

The retrieval response should identify the matched modes, score components, provenance bias, graph contribution, and typed projection names associated with the card type.

Minimum explain fields are `query`, `mode`, and, for each result, `card_uid`, `rel_path`, `matched_by`, `score_components`, and `context`. See [the explain schema](RETRIEVAL_EXPLAIN_SCHEMA.md) for the payload contract.

## Attach context from the records

Context includes `card_type`, `source_labels`, `people`, `orgs`, `time_span`, `provenance_bias`, `graph_neighbor_types`, and `typed_projection_names`. Derive it from canonical cards or the generic derived index. Do not maintain a separate hand-written version of what a card means.

Access limits apply before ranking, counting, or expanding neighbors. Conflicts, missing fields, and incomplete coverage must remain visible to the client. See [agent usage](AGENT_USAGE.md) for how those distinctions become a sourced answer.

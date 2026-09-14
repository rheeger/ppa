# Using PPA from an agent

PPA lets an agent investigate the user's history across imported services. A question can begin with a person, a partial recollection, or an event. The shared catalog supplies search, relationships, and stored evidence without requiring the user to identify the original account.

Choose retrieval methods for the question. Finding a recommendation may require search and nearby messages. Calculating a total requires the full eligible set of records. A relationship summary requires resolving the person and checking their available channels before writing a narrative.

## Retrieval jobs

The [archive-query instructions](../.cursor/skills/archive-query/SKILL.md) and job files define the retrieval workflow:

1. [Identify a person](../.cursor/skills/archive-query/identify-person.md) by name, email, phone, or other evidence.
2. [Find their available channels](../.cursor/skills/archive-query/census-channels.md) before assuming one thread represents the relationship.
3. [Read a set of records](../.cursor/skills/archive-query/read-a-stack.md) in context.
4. [Answer a fact](../.cursor/skills/archive-query/answer-a-fact.md) with source support.
5. [Reconstruct a story](../.cursor/skills/archive-query/reconstruct-a-story.md) from the relevant records and dates.

A person profile usually needs jobs 1, 2, then 5. Cursor agents must open the matching job file before retrieving. Other clients receive the MCP server instructions and per-tool recipes on initialization and tool discovery.

## Choose the right tool

| The question needs… | MCP | CLI |
| --- | --- | --- |
| Exact words | `archive_search` | `ppa search` |
| Meaning or a partial recollection | `archive_vector_search`, `archive_hybrid_search` | `ppa vector-search`, `ppa hybrid-search` |
| Records by type, source, person, or date | `archive_query` | `ppa query` |
| A person by name, slug, email, or phone | `archive_person` | `ppa person` |
| Related records | `archive_graph` | `ppa graph` |
| Activity in a time window | `archive_timeline`, `archive_temporal_neighbors` | `ppa timeline`, `ppa temporal-neighbors` |
| A compact set of dated evidence | `archive_evidence` | `ppa evidence` |
| Totals, subscriptions, or trip costs | `archive_analytics` | `ppa analytics` |
| The underlying source card | `archive_read`, `archive_read_many` | `ppa read`, `ppa read-many` |
| Current instance status | `archive_status_json` | `ppa status` |

For a large archive, reuse the running MCP server. Starting a new CLI process for each question can repeatedly open a large index. CLI examples describe equivalent operations; they are not a reason to restart retrieval for every lookup.

## Make the answer traceable

Read the stored records, called canonical cards, before citing a factual claim. These remain available even when the original service is inaccessible. Search hits, embeddings, and summaries help locate evidence; cite the saved content and preserve any conflicts or uncertainty.

Use underscore-separated type names such as `email_message`. `people_filter` takes a name or slug, not an email address. `archive_person` accepts names, slugs, emails, and phones, and returns `unique`, `ambiguous`, or `unresolved`. An alias-only match needs confirmation before a person narrative.

For a count or total, check completeness, source coverage, and freshness. An exact count of stored records does not establish that every real-world event was imported. Do not calculate a total from one ranked page or silently combine currencies. The [evidence query contract](EVIDENCE_QUERY_CONTRACT.md) defines these fields.

`archive_knowledge` falls back to ordinary search when no fresh knowledge cards exist. It is not a precomputed personal profile. PPA supplies records and bounded analytical workflows; the agent writes the answer.

## Keep retrieval and maintenance separate

`archive_stats` reports corpus counts. `ppa health` performs structural and behavioral checks. `ppa status` and `archive_status_json` report the current instance's operating state.

Rebuild, embedding, and linker operations change the archive or its derived state. Do not invoke them as a shortcut while answering a question. Long maintenance jobs follow the [detached-job instructions](../.cursor/skills/long-running-jobs/SKILL.md), with one writer per vault.

## Change the agent instructions in one place

The shared instructions live in `archive_cli.mcp_instructions.build_server_instructions()`, the job files above, and `TOOL_DESCRIPTIONS` in the same module. Edit the relevant job recipe and keep the MCP router in sync. Avoid copying a separate version of the workflow into each client's configuration.

See the [MCP reference](MCP_SETUP.md) for connection and access settings.

"""Live agent contract served with the MCP toolset.

FastMCP sends ``build_server_instructions()`` as initialize.instructions.
Per-tool recipes live in ``TOOL_DESCRIPTIONS`` and are attached at registration.

This module is the source of truth. Consuming agents (Arnold, Cursor, others)
receive it on the next tools/list. Update here — do not fork the rules into
skill docs or AGENTS.md.
"""

from __future__ import annotations

import os

DEFAULT_INSTANCE_NAME = "Personal Private Archives"

# Underscore types only. Hyphens match nothing.
TYPE_FILTER_HINT = (
    "type_filter uses underscores: email_message, email_thread, email_attachment, "
    "calendar_event, meeting_transcript, document, person, imessage_message, "
    "beeper_message, flight, ride, meal_order, grocery_order, purchase, "
    "medical_record, media_asset, organization, place, knowledge. "
    "Never hyphens (email-message matches nothing)."
)

SOURCE_FILTER_HINT = (
    "source_filter examples: gmail, google-calendar, otter, notion, imessage, beeper."
)

FILTER_HINT = (
    f"{TYPE_FILTER_HINT} {SOURCE_FILTER_HINT} "
    "people_filter is a person name or slug, never an email address. "
    "Filter aggressively — 20 filtered hits beat 20 unfiltered ones."
)

_CONTRACT = """\
WHAT THIS IS
A retrieval engine over the owner's canonical markdown vault. Cards are the
truth. Search hits, embeddings, and chunks are navigation aids — not quotes.
You reason over returned cards. The archive does not answer for you.

HOW IT WORKS
- Cards are typed markdown notes. Types use underscores, never hyphens
  (email_message, calendar_event, meeting_transcript — not email-message).
- Indexes (Postgres + embeddings) are derived. If reads work but search looks
  stale, the index is stale — do not invent.
- Filters: type_filter, source_filter, people_filter, start_date, end_date.
- Some deployments are read-only. A PPA_MCP_TOOL_PROFILE error means the tool
  is disabled, not that the archive is empty.
- Empty / timeout / unreachable: fail closed. Do not fabricate archive facts.

DO
- Open-ended recall → archive_hybrid_search (or archive_hybrid_search_json).
- Known type/person/source → archive_query.
- Exact phrase → archive_search / archive_search_json.
- Who is X → archive_person, then hybrid with people_filter.
- Date range → archive_timeline. Point-in-time → archive_temporal_neighbors.
- Relationships → archive_graph from a known card.
- Ground facts with archive_read / archive_read_many. Batch reads.
- Retry: reformulate, change filters, switch modes. Never stop after one miss.
- Check confidence. Low = narrow the query or say the archive does not have it.
- Prefer *_json tools when you will parse results.
- Search a person by name (people_filter) and separately by email (query=).

DON'T
- Don't treat snippets, titles, chunks, or embeddings as canonical.
- Don't use hyphenated types (email-message, calendar-event).
- Don't put an email address in people_filter.
- Don't claim a fact from a search summary. Read the card.
- Don't collapse conflicting cards — surface the conflict.
- Don't invent when tools fail or return empty.
- Don't call rebuild / embed / seed-link tools unless you are doing ops.
- Don't give up on a specific fact after one phrasing.

ROUTING
- Known UID or path → archive_read / archive_read_many
- Type / person / source known → archive_query
- Exact keywords → archive_search_json
- Fuzzy / "what do we know" → archive_hybrid_search
- Pure semantic → archive_vector_search
- Who / relationship → archive_person, then archive_graph
- When / timeline → archive_timeline or archive_temporal_neighbors
- Aggregations (orders, rides, shops) → archive_query + type_filter, aggregate yourself
"""


def build_server_instructions(instance_name: str | None = None) -> str:
    """MCP initialize.instructions — high-level system + do/don't + routing."""
    name = (instance_name or os.environ.get("PPA_INSTANCE_NAME") or DEFAULT_INSTANCE_NAME).strip()
    return f"{name}\n\n{_CONTRACT.strip()}\n"


TOOL_DESCRIPTIONS: dict[str, str] = {
    "archive_search": (
        "Full-text keyword/phrase search. Use when you know exact terms. "
        "Hits are retrieval aids — read matching cards before stating facts. "
        f"{FILTER_HINT}"
    ),
    "archive_search_json": (
        "Same as archive_search, structured JSON (paths, summaries, confidence). "
        "Prefer this when you will parse or batch-read results. "
        "Not canonical evidence."
    ),
    "archive_query": (
        "Structured filter by frontmatter. Use when you know the card type, "
        "person, source, or org. "
        f"{FILTER_HINT} "
        "Examples: type_filter=email_message people_filter=Sarah; "
        "type_filter=calendar_event; type_filter=ride. "
        "Aggregate client-side. No date args — use archive_timeline or hybrid "
        "start_date/end_date for windows."
    ),
    "archive_hybrid_search": (
        "Default open-ended retrieval: lexical + semantic + graph, ranked by card. "
        "Start here when you do not know the source type. "
        f"{FILTER_HINT} "
        "Search a sender by people_filter=<name> and a second call query=<email> "
        "type_filter=email_message. Low confidence → reformulate, do not invent."
    ),
    "archive_hybrid_search_json": (
        "Same as archive_hybrid_search as JSON (rows, scores, confidence, "
        "matched_by). Prefer this when parsing. Still read cards before claims."
    ),
    "archive_vector_search": (
        "Semantic-only recall over embeddings. Use for vague conceptual questions "
        "when you already know the scope. Weaker than hybrid for exact names. "
        f"{FILTER_HINT}"
    ),
    "archive_read": (
        "Canonical card read by UID or relative path. This is the trust boundary. "
        "Search hits are not enough for factual claims — read the card. "
        "Dense PII: some clients require passkey approval."
    ),
    "archive_read_many": (
        "Batch canonical reads. paths_json is a JSON array of UIDs or relative "
        "paths. Use after search surfaces 3–5 candidates. One approval for many "
        "cards. Trust boundary is the same as archive_read."
    ),
    "archive_person": (
        "Person profile by name or slug, with linked cards. Use for 'who is X' "
        "then follow with hybrid people_filter and archive_graph. "
        "people_filter elsewhere takes a name, not an email."
    ),
    "archive_graph": (
        "Expand wikilinks and discovered relationships from a known card. "
        "Deterministic [edge_type] edges are authoritative. "
        "[seed:edge_type, conf=X] edges are suggestions — qualify by confidence. "
        "Prefer small neighborhoods (hops=1 or 2) around the best anchors."
    ),
    "archive_timeline": (
        "Cards in a date range (start_date/end_date ISO). No people filter — "
        "pair with hybrid people_filter + start_date/end_date when you need both. "
        "Use archive_temporal_neighbors for a single timestamp."
    ),
    "archive_temporal_neighbors": (
        "Cards before, after, or spanning a timestamp (ISO). "
        "Use for 'what was I doing on Dec 27'. Accepts type/source/people filters. "
        f"{FILTER_HINT}"
    ),
    "archive_knowledge": (
        "Freshest knowledge card for a domain, or lexical fallback. "
        "v2 has no pre-computed knowledge cache — treat fallback rows as search, "
        "not a synthesized brief. Still ground with archive_read."
    ),
    "archive_stats": (
        "Card count and type/source distribution. Use as a preflight: "
        "count>0 means ready; errors or 0 mean unavailable. "
        "Does not replace retrieval."
    ),
    "archive_status_json": (
        "Index + runtime status as JSON. Use before heavy retrieval if search "
        "looks stale. Operational, not a substitute for reading cards."
    ),
}

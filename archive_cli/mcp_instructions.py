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

SOURCE_FILTER_HINT = "source_filter examples: gmail, google-calendar, otter, notion, imessage, beeper."

FILTER_HINT = (
    f"{TYPE_FILTER_HINT} {SOURCE_FILTER_HINT} "
    "people_filter is a person name or slug, never an email address. "
    "Filters help when you know them; skip or loosen them when you want a wide scan. "
    "Defaults are starting points — raise limit or run another type_filter to go wider."
)

CARD_STACK_PLAYBOOK = """\
HOW TO COMPOSE QUERIES
Every retrieval tool is available: archive_search, archive_hybrid_search,
archive_query, archive_person, archive_read, archive_read_many,
archive_evidence, archive_timeline, archive_temporal_neighbors,
archive_graph, archive_vector_search, archive_knowledge, archive_stats.
Start wide or narrow. Then follow parent / attachment / duplicate UIDs.

- Wide scan: archive_search or archive_hybrid_search (raise limit when
  you need more than the default). archive_query when you know type /
  person / source. Multi-type is fine — run another type_filter.
- People: archive_person, then people_filter on search / query / hybrid /
  evidence. people_filter is a name/slug, never an email; put emails in
  query=.
- Dates: archive_timeline or start_date/end_date on evidence / hybrid /
  search. archive_temporal_neighbors for a single timestamp.
- Compact dated stack: archive_evidence (uid, date, type, title, why,
  parent/duplicate/attachment pointers). narrative=true stitches a short
  dated outline citing UIDs.
- Bodies: archive_read / archive_read_many for cards you will use.
  include_attachment_uids / include_duplicate_uids return link lists —
  read those UIDs when you need the text.
- Weak lexical → archive_hybrid_search or archive_vector_search.
- Relationships → archive_graph from a known card.

Defaults (often 8–12) are starting points, not caps.
"""

CARD_STACK_PLAYBOOK_HELP = (
    "Compose queries: start wide (search / hybrid / query) or narrow "
    "(person / type / dates), then follow parent/attachment/duplicate "
    "UIDs. Use evidence for a compact dated stack; read for bodies; "
    "hybrid when lexical is weak. Defaults are starting points — raise "
    "limit or run another type_filter to go wider."
)

_CONTRACT = (
    """\
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
- Email / attachment / document / duplicate / thread stacks can be large.
  Compose: list compactly when you want a dated stack; read bodies for the
  UIDs you will use; follow parent/attachment/duplicate pointers on demand.

"""
    + CARD_STACK_PLAYBOOK
    + """
DO
- Open-ended recall → archive_hybrid_search (or archive_hybrid_search_json).
  Raise limit for a wider scan; follow UIDs with evidence or read.
- Known type/person/source → archive_query (multi-type = more than one call).
- Exact phrase → archive_search / archive_search_json.
- Who is X → archive_person, then search / query / hybrid / evidence with
  people_filter.
- Reconstruct a story → archive_evidence (narrative=true) and/or timeline,
  then read the supporting UIDs.
- Date range listing → archive_timeline or archive_evidence.
- Point-in-time → archive_temporal_neighbors.
- Relationships → archive_graph from a known card.
- Ground facts with archive_read. Batch with archive_read_many when you
  already have the UIDs.
- Retry: reformulate, change filters, switch modes, raise limit. Never stop
  after one miss.
- Check confidence. Low = narrow, widen, or say the archive does not have it.
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
- Known UID or path → archive_read (attachment/duplicate flags = link lists)
- Compact chronological stack → archive_evidence
- Type / person / source known → archive_query
- Exact keywords → archive_search / archive_search_json
- Fuzzy / "what do we know" → archive_hybrid_search
- Pure semantic → archive_vector_search
- Who / relationship → archive_person, then archive_graph
- When / timeline → archive_evidence or archive_timeline or archive_temporal_neighbors
- Aggregations (orders, rides, shops) → archive_query + type_filter, aggregate yourself
"""
)


def build_server_instructions(instance_name: str | None = None) -> str:
    """MCP initialize.instructions — high-level system + do/don't + routing."""
    name = (instance_name or os.environ.get("PPA_INSTANCE_NAME") or DEFAULT_INSTANCE_NAME).strip()
    return f"{name}\n\n{_CONTRACT.strip()}\n"


TOOL_DESCRIPTIONS: dict[str, str] = {
    "archive_search": (
        "Full-text keyword/phrase search. Start here for exact words, or go "
        "wide and raise limit past the default. Hits are retrieval aids — "
        "compose next with archive_evidence (compact dated stack), "
        "archive_query (structured filter), or archive_read (bodies). "
        f"{FILTER_HINT}"
    ),
    "archive_search_json": (
        "Same as archive_search, structured JSON (paths, summaries, confidence). "
        "Prefer this when you will parse. Not canonical evidence — read cards "
        "you will cite."
    ),
    "archive_query": (
        "Structured filter by frontmatter. Use when you know the card type, "
        "person, source, or org. Multi-type is fine — call again with another "
        "type_filter. "
        f"{FILTER_HINT} "
        "Examples: type_filter=email_message people_filter=Sarah; "
        "type_filter=calendar_event; type_filter=ride. "
        "Aggregate client-side. No date args here — use archive_evidence "
        "or archive_timeline / hybrid start_date/end_date."
    ),
    "archive_hybrid_search": (
        "Open-ended discovery: lexical + semantic + graph. Start here when "
        "you do not know the source type, or when lexical search is weak. "
        "Raise limit to go wider. Then follow UIDs with archive_evidence, "
        "archive_query, or archive_read. "
        f"{FILTER_HINT} "
        "Search a sender by people_filter=<name> and a second call query=<email> "
        "type_filter=email_message. Low confidence → reformulate, do not invent."
    ),
    "archive_hybrid_search_json": (
        "Same as archive_hybrid_search as JSON (rows, scores, confidence, "
        "matched_by). Prefer this when parsing. Previews are not extracts — "
        "read cards you will cite."
    ),
    "archive_vector_search": (
        "Semantic-only recall over embeddings. Use for vague conceptual questions "
        "or when hybrid/lexical is weak. Weaker than hybrid for exact names. "
        f"{FILTER_HINT}"
    ),
    "archive_evidence": (
        "Compact dated listing for a card stack. Input: question and/or "
        "people/types/date range. Output: chronological short hits "
        "(uid, date, type, title, support, recency, parent/duplicate/attachment "
        "UIDs as links). Active corpus only. Default limit 12 — raise it to "
        "go wider. Set narrative=true for a short dated outline citing UIDs. "
        "Use search/hybrid/query first when you want a wide scan; use "
        "archive_read when you need bodies. "
        f"{FILTER_HINT}"
    ),
    "archive_read": (
        "Canonical read of one card by UID or path. Trust boundary. "
        "Use after any discovery tool when you need the body. "
        "include_attachment_uids / include_duplicate_uids add link-only UID "
        "lists — follow those UIDs with another read if you need the text. "
        "Dense PII: some clients require passkey approval."
    ),
    "archive_read_many": (
        "Batch canonical reads. paths_json is a JSON array of UIDs or relative "
        "paths. Use for the UIDs you will actually use. Trust boundary is the "
        "same as archive_read."
    ),
    "archive_person": (
        "Person profile by name or slug, with linked cards. Use for 'who is X', "
        "then search / query / hybrid / evidence with people_filter. "
        "people_filter elsewhere takes a name, not an email."
    ),
    "archive_graph": (
        "Expand wikilinks and discovered relationships from a known card. "
        "Deterministic [edge_type] edges are authoritative. "
        "[seed:edge_type, conf=X] edges are suggestions — qualify by confidence. "
        "Prefer small neighborhoods (hops=1 or 2) around the best anchors; "
        "widen hops when the neighborhood is too small."
    ),
    "archive_timeline": (
        "Cards in a date range (start_date/end_date ISO) — dated listing, not "
        "full bodies. Combine with archive_evidence when you also need people "
        "filters and compact support lines. "
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

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
    "Filter aggressively — 20 filtered hits beat 20 unfiltered ones."
)

CARD_STACK_PLAYBOOK = """\
CARD-STACK PLAYBOOK (follow in order — do not dump stacks)
1. Find people: archive_person (name/slug, never an email).
2. Discover with a small limit (8–12): archive_search / archive_hybrid_search.
3. Filter: archive_query (type / people / source) or dates on archive_evidence.
4. List compactly: archive_evidence (uid, date, type, title, one-line why,
   parent/duplicate/attachment pointers). Not full bodies. Default limit 12.
5. Read only what supports the question: archive_read one UID. Use
   include_attachment_uids / include_duplicate_uids for links only.
6. Follow attachments or duplicates on demand — another archive_read per UID.
   Never paste OCR or markdown of multiple PDFs unless you asked to read
   those UIDs.
7. Stitch a short dated outline: archive_evidence narrative=true (or
   `ppa evidence --narrative`). Cite UIDs, not extracts.
"""

CARD_STACK_PLAYBOOK_HELP = (
    "Card-stack playbook: find people (person) → discover with a small limit "
    "(search / hybrid-search) → filter (query) → list compactly (evidence) → "
    "read one card (read) → follow attachment/duplicate UIDs on demand → "
    "optional evidence --narrative for a dated outline. Never dump OCR or "
    "full bodies of multiple attachments unless you asked to read those UIDs."
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
- Email / attachment / document / duplicate / thread stacks overwhelm context.
  Use the card-stack playbook. Compact listings first; read one UID at a time.

"""
    + CARD_STACK_PLAYBOOK
    + """
DO
- Open-ended recall → archive_hybrid_search (or archive_hybrid_search_json),
  small limit, then archive_evidence to list what you will actually read.
- Known type/person/source → archive_query, then archive_evidence.
- Exact phrase → archive_search / archive_search_json (small limit).
- Who is X → archive_person, then evidence/hybrid with people_filter.
- Reconstruct a story → archive_evidence (narrative=true) after compact hits.
- Date range listing without a question → archive_timeline or archive_evidence.
- Point-in-time → archive_temporal_neighbors.
- Relationships → archive_graph from a known card.
- Ground facts with archive_read (one card). Batch only UIDs you will use.
- Retry: reformulate, change filters, switch modes. Never stop after one miss.
- Check confidence. Low = narrow the query or say the archive does not have it.
- Prefer *_json tools when you will parse results.
- Search a person by name (people_filter) and separately by email (query=).

DON'T
- Don't treat snippets, titles, chunks, or embeddings as canonical.
- Don't use hyphenated types (email-message, calendar-event).
- Don't put an email address in people_filter.
- Don't claim a fact from a search summary. Read the card.
- Don't dump an email thread, its attachments, OCR, and duplicates in one turn.
- Don't paste OCR/markdown of multiple PDFs unless you asked to read those UIDs.
- Don't collapse conflicting cards — surface the conflict.
- Don't invent when tools fail or return empty.
- Don't call rebuild / embed / seed-link tools unless you are doing ops.
- Don't give up on a specific fact after one phrasing.

ROUTING
- Known UID or path → archive_read (one card; attachment/duplicate flags = links)
- Compact chronological evidence → archive_evidence
- Type / person / source known → archive_query
- Exact keywords → archive_search_json (small limit)
- Fuzzy / "what do we know" → archive_hybrid_search (small limit)
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
        "Discovery only: full-text keyword/phrase search. Small limit (8–12). "
        "Hits are retrieval aids — do not dump them as the answer. Next: "
        "archive_evidence for a compact dated list, then archive_read one UID. "
        f"{FILTER_HINT}"
    ),
    "archive_search_json": (
        "Same as archive_search, structured JSON (paths, summaries, confidence). "
        "Prefer this when you will parse. Still a discovery step — not a dump. "
        "Not canonical evidence."
    ),
    "archive_query": (
        "Structured filter by frontmatter. Use when you know the card type, "
        "person, source, or org. "
        f"{FILTER_HINT} "
        "Examples: type_filter=email_message people_filter=Sarah; "
        "type_filter=calendar_event; type_filter=ride. "
        "Aggregate client-side. For a compact dated list with people+dates, "
        "prefer archive_evidence. No date args here — use archive_evidence "
        "or archive_timeline / hybrid start_date/end_date."
    ),
    "archive_hybrid_search": (
        "Open-ended discovery: lexical + semantic + graph. Small limit (8–12). "
        "Start here when you do not know the source type, then archive_evidence "
        "to list compactly and archive_read only supporting UIDs. "
        f"{FILTER_HINT} "
        "Search a sender by people_filter=<name> and a second call query=<email> "
        "type_filter=email_message. Low confidence → reformulate, do not invent."
    ),
    "archive_hybrid_search_json": (
        "Same as archive_hybrid_search as JSON (rows, scores, confidence, "
        "matched_by). Prefer this when parsing. Still discovery — then evidence "
        "and one-card reads. Do not treat previews as extracts."
    ),
    "archive_vector_search": (
        "Semantic-only recall over embeddings. Use for vague conceptual questions "
        "when you already know the scope. Weaker than hybrid for exact names. "
        f"{FILTER_HINT}"
    ),
    "archive_evidence": (
        "First-class compact listing for card-stack traversal. Input: question "
        "and/or people/types/date range. Output: chronological short hits "
        "(uid, date, type, title, support, recency, parent/duplicate/attachment "
        "UIDs as links). Active corpus only. Default limit 12. Set narrative=true "
        "for a short dated outline citing UIDs — not full extracts. "
        "Do not use this to dump OCR. Next: archive_read one supporting UID. "
        f"{FILTER_HINT}"
    ),
    "archive_read": (
        "Canonical read of ONE card by UID or path. Trust boundary. "
        "After archive_evidence, read only UIDs that support the question. "
        "include_attachment_uids / include_duplicate_uids add link-only UID "
        "lists — never attachment OCR or duplicate bodies. "
        "Do not paste OCR of multiple PDFs unless you asked to read those UIDs. "
        "Dense PII: some clients require passkey approval."
    ),
    "archive_read_many": (
        "Batch canonical reads. paths_json is a JSON array of UIDs or relative "
        "paths. Use only for the 3–5 UIDs evidence said you should read — not "
        "a whole thread/attachment stack. Trust boundary is the same as "
        "archive_read."
    ),
    "archive_person": (
        "Person profile by name or slug, with linked cards. First playbook "
        "step for 'who is X', then archive_evidence with people_filter. "
        "people_filter elsewhere takes a name, not an email."
    ),
    "archive_graph": (
        "Expand wikilinks and discovered relationships from a known card. "
        "Deterministic [edge_type] edges are authoritative. "
        "[seed:edge_type, conf=X] edges are suggestions — qualify by confidence. "
        "Prefer small neighborhoods (hops=1 or 2) around the best anchors."
    ),
    "archive_timeline": (
        "Cards in a date range (start_date/end_date ISO) — dated listing, not "
        "full bodies. No people filter — use archive_evidence when you need "
        "people + dates + compact support lines. "
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

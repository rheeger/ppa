"""Live agent contract served with the MCP toolset.

FastMCP sends ``build_server_instructions()`` as initialize.instructions.
Per-tool recipes live in ``TOOL_DESCRIPTIONS`` and are attached at registration.

This module is the thin live contract (safety + job router). Job recipes live
in ``.cursor/skills/archive-query/``. MCP-only clients follow the stop tests
here. Cursor agents must open the matching job file before retrieving.
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
HOW TO RECONSTRUCT
A question is almost never one card type. Assemble the event from cards
you open. Rank by how close the card is to what happened:

1. Lived: imessage_message, beeper_message (Discord/Slack/Telegram),
   meeting_transcript, ride, flight. Query messages, not thread cards.
2. Sent: email_message bodies (not email_thread, not attachments).
3. Planned: calendar_event, invites. A calendar block is the plan, not
   attendance. If a message and a calendar disagree, the message wins.
4. Derived: search snippets, embeddings, knowledge, person aliases.
5. Byproduct: git_commit / git_message unless the question is code.

Census matched_total per channel (include Beeper) before you narrate.
Query one type per call. Date-only filters are UTC; for a local day,
query that UTC date and the next, then keep local-time rows. Read bodies
for 1:1s that moved and for any line that would change the story. Follow
parent / attachment / duplicate UIDs. A thin lived query is a thin
index, not an empty life. Do not fill it with calendar or git. Do not
hybrid-search funeral/drive/family without a date.

HOW TO COMPOSE QUERIES
Every retrieval tool is available: archive_search, archive_hybrid_search,
archive_query, archive_analytics, archive_person, archive_read, archive_read_many,
archive_evidence, archive_timeline, archive_temporal_neighbors,
archive_graph, archive_vector_search, archive_knowledge, archive_stats.
Start wide or narrow. Then follow parent / attachment / duplicate UIDs.

- Lived time: archive_query type_filter=imessage_message and
  beeper_message separately. Untyped evidence and archive_timeline rank
  git and calendar over texts.
- Wide scan: archive_search or archive_hybrid_search (raise limit when
  you need more than the default). archive_query when you know type /
  person / source. Multi-type is fine; run another type_filter.
- People: identify-person.md, then people_filter on search / query / hybrid /
  evidence. people_filter is a name/slug, never an email; put emails in
  query=. archive_person is a candidate. Census channels before a profile.
- Dates: start_date/end_date on query / evidence / hybrid. Date-only is
  UTC. archive_temporal_neighbors is for one timestamp you already have.
- Compact dated stack: archive_evidence inside a type_filter or
  people_filter (uid, date, type, title, why, parent/duplicate/attachment
  pointers). narrative=true outlines whatever ranked, still not a recap.
- Bodies: archive_read / archive_read_many for cards you will use.
  include_attachment_uids / include_duplicate_uids return link lists.
  Read those UIDs when you need the text.
- Weak lexical → archive_hybrid_search or archive_vector_search.
- Relationships → archive_graph from a known card.
- Totals, subscriptions, trip costs, changes-since, or typed query +
  saved scope → archive_analytics. Completeness is in the JSON.

Defaults (often 8–12) are starting points, not caps.
"""

CARD_STACK_PLAYBOOK_HELP = (
    "Reconstruct from cards: lived messages over calendar and git, "
    "census matched_total (include Beeper), query one type per call. "
    "Compose: start wide (search / hybrid / query) or narrow "
    "(person / type / dates), then follow parent/attachment/duplicate "
    "UIDs. Evidence is a compact dated stack inside a type; read for "
    "bodies; hybrid when lexical is weak. Defaults are starting points; "
    "raise limit or run another type_filter to go wider."
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
- Some deployments are read-only. A PPA_MCP_TOOL_PROFILE denial
  (ok=false, status=denied) means the tool is disabled, not that the
  archive is empty. Error/denial JSON is never empty success.
- Empty / timeout / unreachable: fail closed. Do not fabricate archive facts.
- high/medium/low is retrieval evidence quality, not the probability a
  proposition is true. One exact identifier match may be high without
  implying source completeness. Eleven weak hits are not high.
- coverage, freshness, and truncated stay unknown when unknown. Read
  confidence_reason; do not infer completeness from result count.
- Email / attachment / document / duplicate / thread stacks can be large.
  Compose: list compactly when you want a dated stack; read bodies for the
  UIDs you will use; follow parent/attachment/duplicate pointers on demand.

"""
    + CARD_STACK_PLAYBOOK
    + """
JOBS
Pick one job. If you can read this repo, open
.cursor/skills/archive-query/<file> and follow it before retrieving.
Every job follows HOW TO RECONSTRUCT. MCP-only clients use the stop tests
here.

1. Identify a person — identify-person.md
   archive_person is a candidate, not identity. Confirm summary equals the
   needle, or an email/phone you already saw on a grounded message. If the
   needle is only in aliases, the name was stolen (Paperless Post / Evite
   From-lines). Search the name and read every type=person hit. Prefer
   email/phone over display name. status=ambiguous: list candidates; do not
   pick. A thin contact stub is not the biography.

2. Census their channels — census-channels.md
   archive_analytics workflow=query once each for imessage_thread,
   imessage_message, beeper_thread, beeper_message, email_message,
   calendar_event, meeting_transcript. Read matched_total. A page of 40
   with truncated is not the corpus. Empty iMessage is not "they did not
   text." Read thread cards (message_count, first_message_at,
   last_message_at) before sampling bodies. type_filter=person plus the
   same people_filter can be empty.

3. Read a stack — read-a-stack.md
   Query one type per call. archive_evidence for a compact dated list
   inside a type_filter. Do not add life/school/work to query= when
   people_filter is set. Recency-sorted archive_query without a type is
   latest chatter (often git). Follow parent/attachment/duplicate UIDs.
   Read bodies only for UIDs you will use.

4. Answer a fact — answer-a-fact.md
   Known UID → archive_read. Ground every cite. Attendance and "did it
   happen" are lived-message facts, not calendar facts. The person tool's
   first hit is not a cite when summary is not the needle.

5. Reconstruct a story — reconstruct-a-story.md
   Identify, then census (include Beeper), then lived message types in
   the window, then bodies. Calendar is the plan. Do not call the last
   email the last update until iMessage and Beeper totals are in hand.

6. Recap a day — recap-a-day.md
   One local day. Messages first, calendar as plan, skip git unless
   asked about code. Date-only is UTC.

DON'T
- Don't treat snippets, titles, chunks, or embeddings as canonical.
- Don't use hyphenated types (email-message, calendar-event).
- Don't put an email address in people_filter.
- Don't claim a fact from a search summary. Read the card.
- Don't collapse conflicting cards — surface the conflict.
- Don't invent when tools fail or return empty.
- Don't call rebuild / embed / seed-link tools unless you are doing ops.
- Don't give up on a specific fact after one phrasing.
- Don't treat an alias-only person hit as identity.
- Don't skip the channel census on a who-is / profile / what-happened question.
- Don't narrate a calendar block as attended when messages say otherwise.
- Don't fill a thin message query with git or calendar.
- Don't hybrid-search biography words (funeral, drive, family) without a date.
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
        "Same as archive_search, structured JSON (paths, summaries, confidence, "
        "confidence_reason, EvidenceEnvelope). Prefer this when you will parse. "
        "Not canonical evidence — read cards you will cite. ok=false is an "
        "error or denial, not a zero-hit search."
    ),
    "archive_query": (
        "Structured filter by frontmatter. Use when you know the card type, "
        "person, source, or org. Multi-type is fine — call again with another "
        "type_filter. Optional saved_scope_name narrows; it never widens policy. "
        "Empty intersection is empty-scope, not search-everything. "
        f"{FILTER_HINT} "
        "Examples: type_filter=email_message people_filter=Sarah; "
        "type_filter=calendar_event; type_filter=ride. "
        "For a day or event, query message types (imessage_message, "
        "beeper_message), not threads. Calendar is the plan. "
        "Totals and completeness are labeled. Aggregate client-side or use "
        "archive_analytics for the three workflows. Dates: start_date/end_date "
        "(date-only is UTC) or archive_evidence."
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
        "confidence_reason, matched_by, EvidenceEnvelope). Prefer this when "
        "parsing. Previews are not extracts — read cards you will cite. "
        "ok=false is an error or denial, not a zero-hit search."
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
        "expand_context labels matched vs adjacent context. saved_scope_name "
        "narrows. Completeness (coverage/freshness/truncated) is visible. "
        "Use search/hybrid/query first when you want a wide scan; use "
        "archive_read when you need bodies. Untyped evidence ranks "
        "git_commit and calendar_event over messages. For lived time, "
        "call per type_filter (imessage_message, beeper_message). "
        f"{FILTER_HINT}"
    ),
    "archive_analytics": (
        "Read-only typed query, neighbor context, or a deterministic workflow "
        "as JSON. workflow=query|context|subscription_lifecycle|trip_costs|"
        "changes_since. Same rows/totals/citations/scopes as `ppa analytics`. "
        "Saved scopes apply; invalid presets fail; empty intersection is empty. "
        "Labels source_reported vs derived vs proposed_link. No FX, no "
        "financial or health advice, production_proven=false. "
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
        "Candidate person card by name, slug, email, or phone. Not identity. "
        "Confirm summary equals the needle before using the card. Alias-only "
        "hits are stolen names (Paperless Post / Evite From-lines). Ambiguous "
        "returns candidate UIDs; do not pick a winner. Then census channels "
        "(identify-person.md). people_filter elsewhere takes a name, not an email."
    ),
    "archive_graph": (
        "Expand wikilinks and discovered relationships from a known card. "
        "Deterministic [edge_type] edges are authoritative. "
        "[seed:edge_type, conf=X] edges are suggestions — qualify by confidence. "
        "Prefer small neighborhoods (hops=1 or 2) around the best anchors; "
        "widen hops when the neighborhood is too small."
    ),
    "archive_timeline": (
        "Cards in a date range (start_date/end_date ISO): dated listing, not "
        "full bodies. Mixed listing; git and calendar dominate. Not a day "
        "recap. For lived time, archive_query per message type instead. "
        "Combine with archive_evidence when you also need people filters "
        "and compact support lines. "
        "Use archive_temporal_neighbors for a single timestamp you already have."
    ),
    "archive_temporal_neighbors": (
        "Cards before, after, or spanning a timestamp (ISO). "
        "Use around one timestamp you already have, not as a day recap. "
        "Accepts type/source/people filters. "
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

"""Workflow A — email thread enrichment (Phase 2.875)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from archive_sync.llm_enrichment.known_senders import classify_thread_prefilter
from archive_sync.llm_enrichment.staging_types import (EntityMention,
                                                       MatchCandidate)
from archive_sync.llm_enrichment.threads import (ThreadDocument, ThreadStub,
                                                 render_thread_for_extraction)

if TYPE_CHECKING:
    from archive_sync.llm_enrichment.classify_index import ClassifyIndex

PROMPT_FILE = Path(__file__).resolve().parent.parent / "prompts" / "enrich_email_thread.txt"
ENRICH_EMAIL_THREAD_PROMPT_VERSION = "v4"

# Strip common LLM boilerplate (Flash models often default to "This thread...").
_THREAD_SUMMARY_BOILERPLATE = re.compile(
    r"^(?:"
    r"in\s+this\s+thread,?\s+"
    r"|(?:this|the)\s+thread\s+(?:"
    r"consists\s+of|documents|discusses|concerns|details|contains|tracks|covers|"
    r"records|describes|relates|follows|outlines|summarizes|has|was|is"
    r")\s+"
    r")",
    re.IGNORECASE | re.VERBOSE,
)


def strip_thread_summary_boilerplate(summary: str) -> str:
    """Remove leading 'This thread…' / 'In this thread…' style openers from LLM summaries."""

    s = summary.strip()
    if not s:
        return s
    m = _THREAD_SUMMARY_BOILERPLATE.match(s)
    if not m:
        return s
    rest = s[m.end() :].lstrip()
    if not rest:
        return s
    if rest[0].islower():
        rest = rest[0].upper() + rest[1:]
    return rest


def load_system_prompt(account_email: str) -> str:
    raw = PROMPT_FILE.read_text(encoding="utf-8")
    return raw.format(account_email=account_email or "(unknown)")


def gate_email_thread_card(card: dict[str, Any]) -> bool:
    """Threads with more than one message (proxy for back-and-forth)."""
    return int(card.get("message_count") or 0) > 1


# Phase 2.75 classify stores categories like noise / marketing / automated / personal / transactional.
# For enrichment we skip threads that were clearly noise or automated; keep personal + transactional.
_CLASSIFY_INDEX_SKIP_FOR_ENRICHMENT = frozenset(
    {"noise", "marketing", "automated", "skip"}
)

# Gmail adapter uses inbound/outbound; older cards may use received/sent.
_OUTBOUND_DIRECTIONS = frozenset({"sent", "outbound"})


def prefilter_email_thread(
    thread_stubs: list[ThreadStub],
    classify_index: ClassifyIndex | None,
    *,
    thread_label_ids: list[str] | None = None,
    user_domains: frozenset[str] | None = None,
) -> tuple[bool, str]:
    """Return (True, reason) if this thread should reach the LLM; else (False, reason_key).

    Tiered: noise first, then outbound participation, then passive Gmail/heuristic signals.
    """

    if not thread_stubs:
        return False, "no_stubs"

    labels = set(thread_label_ids or [])

    # Tier 1: universal noise (before participation check)
    if "CATEGORY_PROMOTIONS" in labels:
        return False, "gmail_promotions"

    tid = str(thread_stubs[0].gmail_thread_id or "").strip()
    if classify_index is not None and tid:
        cached = classify_index.get(tid)
        if cached:
            cat = str(cached.get("category") or "").strip().lower()
            if cat in _CLASSIFY_INDEX_SKIP_FOR_ENRICHMENT:
                return False, "classify_index"

    from_emails = [s.from_email for s in thread_stubs if s.from_email]
    subjects = [s.subject for s in thread_stubs if s.subject]
    decision, _ = classify_thread_prefilter(
        from_emails, subjects, user_domains=user_domains
    )
    if decision == "skip":
        return False, "known_noise"

    # Tier 2: active participation
    if any(s.direction in _OUTBOUND_DIRECTIONS for s in thread_stubs):
        return True, "outbound"

    # Tier 3: passive interest (no outbound)
    if "IMPORTANT" in labels:
        return True, "passive_important"
    if "STARRED" in labels:
        return True, "passive_starred"
    if "CATEGORY_PERSONAL" in labels and "CATEGORY_FORUMS" not in labels:
        return True, "passive_personal"
    if len(thread_stubs) >= 5 and decision == "triage":
        return True, "passive_substantial"

    return False, "no_sent_no_signals"


def render_user_message(doc: ThreadDocument, *, account_email: str) -> str:
    header = "\n".join(
        [
            f"Subject: {doc.subject}",
            f"Thread id: {doc.thread_id}",
            f"Archive owner account_email: {account_email}",
            f"Participants: {', '.join(doc.participants) if doc.participants else '(unknown)'}",
            f"Message count: {doc.message_count}",
            "",
            "--- THREAD ---",
            "",
        ]
    )
    return header + render_thread_for_extraction(doc)


def parse_email_thread_response(
    data: dict[str, Any],
    *,
    source_uid: str,
    run_id: str,
) -> tuple[dict[str, Any], list[EntityMention], list[MatchCandidate]]:
    """Map LLM JSON to field updates + staging rows."""

    summary = strip_thread_summary_boilerplate(str(data.get("thread_summary") or ""))
    field_updates: dict[str, Any] = {}
    if summary:
        field_updates["thread_summary"] = summary

    entities: list[EntityMention] = []
    raw_mentions = data.get("entity_mentions") or []
    if isinstance(raw_mentions, list):
        for m in raw_mentions:
            if not isinstance(m, dict):
                continue
            et = str(m.get("type") or "").strip().lower()
            if et not in ("person", "place", "organization"):
                continue
            name = str(m.get("name") or "").strip()
            if not name:
                continue
            ctx = m.get("context")
            if not isinstance(ctx, dict):
                ctx = {}
            conf = m.get("confidence")
            try:
                c = float(conf) if conf is not None else 0.75
            except (TypeError, ValueError):
                c = 0.75
            entities.append(
                EntityMention(
                    source_card_uid=source_uid,
                    source_card_type="email_thread",
                    workflow="email_thread_enrichment",
                    entity_type=et,
                    raw_text=name,
                    context=ctx,
                    confidence=max(0.0, min(1.0, c)),
                    run_id=run_id,
                )
            )

    matches: list[MatchCandidate] = []
    cal = data.get("calendar_match")
    if isinstance(cal, dict) and cal:
        matches.append(
            MatchCandidate(
                source_card_uid=source_uid,
                source_card_type="email_thread",
                workflow="email_thread_enrichment",
                target_card_type="calendar_event",
                match_signals={
                    "title_keywords": cal.get("title_keywords") or [],
                    "approximate_date": str(cal.get("approximate_date") or ""),
                    "attendee_emails": cal.get("attendee_emails") or [],
                },
                field_to_write="calendar_events",
                confidence=0.75,
                run_id=run_id,
            )
        )

    return field_updates, entities, matches


def response_to_cache_payload(parsed: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize for InferenceCache storage."""

    return parsed if isinstance(parsed, dict) else {"_error": "invalid_json", "raw": str(parsed)}

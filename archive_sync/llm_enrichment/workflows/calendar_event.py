"""Workflow D — calendar_event place extraction (Phase 2.875).

Scope: extract place entities from events with real-world location strings.
Org mentions (deterministic domain split), description (regurgitation of
structured fields), and email_match (cross-linking) are handled elsewhere
(entity resolution Step 10, match resolution Step 9).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from archive_sync.llm_enrichment.staging_types import EntityMention

PROMPT_FILE = Path(__file__).resolve().parent.parent / "prompts" / "enrich_calendar_event.txt"
ENRICH_CALENDAR_EVENT_PROMPT_VERSION = "v2"

_HOLIDAY_RE = re.compile(r"(?i)^holidays?\s+in\s+")
_AUTO_TITLES = frozenset({"focus time", "lunch", "commute", "prep time"})

_VIRTUAL_MARKERS = (
    "zoom.us",
    "meet.google.com",
    "teams.microsoft.com",
    "webex",
    "whereby.com",
    "around.co",
)


def load_system_prompt() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")


def _is_virtual_location(location: str) -> bool:
    loc = location.lower()
    return any(v in loc for v in _VIRTUAL_MARKERS)


def gate_calendar_event(card_data: dict[str, Any]) -> bool:
    """Pass events with a non-empty, non-virtual location string."""

    loc = str(card_data.get("location") or "").strip()
    if not loc:
        return False
    return not _is_virtual_location(loc)


def prefilter_calendar_event(card_data: dict[str, Any]) -> tuple[bool, str]:
    """Skip holidays, cancelled events, and low-signal blocks."""

    status = str(card_data.get("status") or "").strip().lower()
    if status == "cancelled":
        return False, "cancelled"

    title = str(card_data.get("title") or "").strip()
    if _HOLIDAY_RE.match(title):
        return False, "holiday_title"

    attendees = card_data.get("attendee_emails") or []
    att_list = attendees if isinstance(attendees, list) else []
    if title.lower() in _AUTO_TITLES and not att_list:
        return False, "auto_title_no_attendees"

    all_day = bool(card_data.get("all_day"))
    if all_day and not att_list:
        return False, "all_day_no_attendees"

    return True, "ok"


def calendar_content_hash(fm: dict[str, Any]) -> str:
    """Stable hash for inference cache keys (place extraction only)."""

    payload = {
        "title": str(fm.get("title") or ""),
        "location": str(fm.get("location") or ""),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def render_user_message(fm: dict[str, Any]) -> str:
    """Minimal context for place extraction."""

    lines = [
        f"Event: {fm.get('title') or ''}",
        f"Location: {fm.get('location') or ''}",
    ]
    return "\n".join(lines)


def parse_calendar_response(
    data: dict[str, Any],
    *,
    source_uid: str,
    run_id: str,
) -> list[EntityMention]:
    """Extract place EntityMention from LLM JSON."""

    entities: list[EntityMention] = []

    pl = data.get("place_extraction")
    if isinstance(pl, dict) and pl:
        name = str(pl.get("name") or "").strip()
        if name:
            ctx = {k: v for k, v in pl.items() if k != "name"}
            entities.append(
                EntityMention(
                    source_card_uid=source_uid,
                    source_card_type="calendar_event",
                    workflow="calendar_event_enrichment",
                    entity_type="place",
                    raw_text=name,
                    context=ctx,
                    confidence=0.8,
                    run_id=run_id,
                )
            )

    return entities


def response_to_cache_payload(parsed: dict[str, Any] | None) -> dict[str, Any]:
    return parsed if isinstance(parsed, dict) else {"_error": "invalid_json", "raw": str(parsed)}

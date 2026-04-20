"""Normalize timestamp strings to UTC ISO-8601 with ``Z`` suffix (Phase 6.5 Step 16)."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Final

_OFFSET_RE = re.compile(r"[+-]\d{2}:\d{2}$")

# Vault audit / backfill: card_type -> timestamp fields
AUDITED_TIMESTAMP_FIELDS: Final[dict[str, list[str]]] = {
    "calendar_event": ["start_at", "end_at"],
    "meeting_transcript": ["start_at", "otter_updated_at"],
    "email_message": ["sent_at"],
    "imessage_message": ["sent_at"],
    "beeper_message": ["sent_at"],
    "finance": ["activity_at", "created"],
    "purchase": ["activity_at"],
    "meal_order": ["activity_at"],
    "grocery_order": ["activity_at"],
    "ride": ["activity_at", "pickup_at", "dropoff_at"],
    "flight": ["activity_at", "departure_at", "arrival_at"],
    "accommodation": ["activity_at", "check_in", "check_out"],
    "car_rental": ["activity_at", "pickup_at", "dropoff_at"],
    "subscription": ["activity_at", "event_at"],
    "payroll": ["activity_at", "pay_date"],
    "shipment": ["activity_at", "shipped_at", "estimated_delivery", "delivered_at"],
    "event_ticket": ["activity_at", "event_at"],
}


def to_utc_z_iso(value: str | None) -> str:
    """Return canonical ``YYYY-MM-DDTHH:MM:SSZ`` in UTC.

    - Already ends with ``Z`` → returned unchanged.
    - Date-only ``YYYY-MM-DD`` → returned unchanged. The vault schema requires
      date fields (e.g. ``created``, ``updated``, ``pay_date``) to stay in
      ``YYYY-MM-DD`` form; promoting them to a datetime would break ``validate_card_strict``.
    - Offset or naive datetimes (with a time component) → converted to UTC.
    - Unparseable input → returned unchanged (caller may log).
    """
    if not value:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    if s.endswith("Z"):
        return s
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try:
            date.fromisoformat(s)
            return s
        except ValueError:
            return s
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return s
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def classify_timestamp(value: str) -> tuple[str, str | None]:
    """Classify a timestamp string for vault audits.

    Categories: ``utc_z``, ``offset``, ``naive``, ``date_only``, ``empty``, ``unparseable``.
    ``date_only`` is reserved for strict ``YYYY-MM-DD`` strings — these are required
    by the schema for ``created`` / ``updated`` / ``pay_date`` and must not be rewritten.
    """
    if not value:
        return "empty", None
    s = str(value).strip()
    if not s:
        return "empty", None
    if s.endswith("Z"):
        return "utc_z", s
    if _OFFSET_RE.search(s):
        return "offset", s
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        try:
            date.fromisoformat(s)
            return "date_only", s
        except ValueError:
            pass
    try:
        datetime.fromisoformat(s)
        return "naive", s
    except ValueError:
        return "unparseable", s

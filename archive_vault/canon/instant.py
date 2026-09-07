"""ISO-8601 instant and date canon. Naive timestamps become UTC."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def canonical(value: Any) -> str:
    """Normalize a timestamp to UTC ISO-8601 with ``Z``, or ``YYYY-MM-DD`` for date-only."""

    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        parsed = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    cleaned = str(value).strip()
    if not cleaned:
        return ""
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d", "%d %b %Y", "%b %d, %Y", "%B %d, %Y", "%d/%m/%Y"):
            try:
                parsed = datetime.strptime(str(value).strip(), fmt)
            except ValueError:
                continue
            if fmt != "%Y/%m/%d %H:%M:%S":
                return parsed.date().isoformat()
            parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.isoformat().replace("+00:00", "Z")
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    if "T" not in str(value) and parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0:
        return parsed.date().isoformat()
    return parsed.isoformat().replace("+00:00", "Z")


def date_canonical(value: str | None, *formats: str) -> str:
    """Parse ``value`` with ``formats`` (or ISO) into ``YYYY-MM-DD``."""

    raw = str(value or "").strip()
    if not raw:
        return ""
    if _ISO_DATE.fullmatch(raw):
        return raw
    tried = formats or ("%Y-%m-%d", "%d %b %Y", "%b %d, %Y", "%B %d, %Y", "%d/%m/%Y", "%Y%m%d")
    for fmt in tried:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    iso = canonical(raw)
    return iso[:10] if len(iso) >= 10 and iso[4] == "-" else ""

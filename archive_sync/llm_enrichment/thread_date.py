"""Date-scope helpers for vault-side classify and hygiene.

These read card timestamps. They do not walk Gmail.
"""

from __future__ import annotations

from collections.abc import Iterable


def iso_day(value: str | None) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4] == "-":
        return text[:10]
    return ""


def any_activity_since(timestamps: Iterable[str | None], since: str) -> bool:
    """True when any timestamp is on or after ``since`` (YYYY-MM-DD).

    An empty ``since`` means no date filter.
    """

    cutoff = iso_day(since)
    if not cutoff:
        return True
    for raw in timestamps:
        day = iso_day(raw)
        if day and day >= cutoff:
            return True
    return False

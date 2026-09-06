"""Email canon — strip, collapse whitespace, lowercase. No Gmail-dot fold."""

from __future__ import annotations

import re

_WS = re.compile(r"\s+")


def canonical(value: str | None) -> str:
    """Return a stripped, lowercase email. Does not fold Gmail dots or plus-tags."""

    raw = _WS.sub(" ", str(value or "").strip())
    if not raw:
        return ""
    return raw.lower()


def account(value: str | None) -> str:
    """Account-email alias of :func:`canonical`."""

    return canonical(value)


def require_at(value: str | None) -> str:
    """Like :func:`canonical` but empty when the result has no ``@``."""

    text = canonical(value)
    return text if "@" in text else ""

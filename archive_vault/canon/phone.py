"""E.164 phone and contact-handle canon."""

from __future__ import annotations

import re

_NON_DIGIT = re.compile(r"\D")


def canonical(value: str | None) -> str:
    """Normalize a phone to E.164.

    NANP 10-digit and ``1``+10-digit become ``+1XXXXXXXXXX``. An already-``+``
    value keeps its country-code digits. Empty or digit-less input is ``""``.
    """

    raw = str(value or "").strip()
    if not raw:
        return ""
    digits = _NON_DIGIT.sub("", raw)
    if not digits:
        return ""
    if raw.startswith("+"):
        return f"+{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if len(digits) == 10:
        return f"+1{digits}"
    return digits


def contact_handle(value: str | None) -> str:
    """Normalize an iMessage-style handle: email or E.164 phone."""

    raw = re.sub(r"\s+", " ", str(value or "").strip())
    if not raw:
        return ""
    if "@" in raw:
        return raw.lower()
    phone = canonical(raw)
    return phone or raw.lower()


def alias_forms(value: str | None) -> list[str]:
    """E.164 plus national 10-digit and digits-only aliases for indexing."""

    e164 = canonical(value)
    if not e164:
        return []
    digits = _NON_DIGIT.sub("", e164)
    forms = [e164, digits]
    if len(digits) == 11 and digits.startswith("1"):
        forms.append(digits[1:])
    seen: set[str] = set()
    out: list[str] = []
    for item in forms:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out

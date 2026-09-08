"""E.164 phone and contact-handle canon."""

from __future__ import annotations

import re
from os import environ

from archive_vault.canon.types import IdentifierResult

_NON_DIGIT = re.compile(r"\D")
_UNICODE_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")
_EXTENSION = re.compile(r"(?:ext\.?|x|extension)\s*(\d+)\s*$", re.IGNORECASE)


def default_region() -> str:
    """Explicit instance default. Empty means no implicit NANP guess."""

    return (environ.get("PPA_PHONE_REGION") or "US").strip().upper()


def parse(value: str | None, *, region: str | None = None) -> IdentifierResult:
    """Typed phone parse. Opaque handles stay opaque; extensions are metadata."""

    raw = str(value or "").strip().translate(_UNICODE_DIGITS)
    raw = re.sub(r"\s+", " ", raw)
    if not raw:
        return IdentifierResult(kind="phone", original="", canonical="", validity="invalid", reason="empty")
    if re.search(r"[A-Za-z]", raw) and "@" not in raw and not _EXTENSION.search(raw):
        return IdentifierResult(kind="handle", original=raw, canonical="", validity="opaque", reason="opaque_handle")
    extension = ""
    ext_match = _EXTENSION.search(raw)
    if ext_match:
        extension = ext_match.group(1)
        raw = raw[: ext_match.start()].strip()
    plus = raw.startswith("+")
    digits = _NON_DIGIT.sub("", raw)
    if not digits:
        return IdentifierResult(
            kind="handle", original=value or "", canonical="", validity="opaque", reason="no_digits"
        )
    configured = (region if region is not None else default_region()) or ""
    if plus:
        if len(digits) < 8:
            return IdentifierResult(
                kind="phone",
                original=str(value or ""),
                canonical="",
                validity="unknown",
                reason="short_e164",
                extension=extension,
            )
        return IdentifierResult(
            kind="phone",
            original=str(value or ""),
            canonical=f"+{digits}",
            validity="valid",
            region="INTL",
            extension=extension,
        )
    if configured == "US" and len(digits) == 10:
        return IdentifierResult(
            kind="phone",
            original=str(value or ""),
            canonical=f"+1{digits}",
            validity="valid",
            region="US",
            extension=extension,
        )
    if configured == "US" and len(digits) == 11 and digits.startswith("1"):
        return IdentifierResult(
            kind="phone",
            original=str(value or ""),
            canonical=f"+{digits}",
            validity="valid",
            region="US",
            extension=extension,
        )
    return IdentifierResult(
        kind="phone",
        original=str(value or ""),
        canonical="",
        validity="unknown",
        reason="needs_region" if not plus else "unknown",
        region=configured,
        extension=extension,
    )


def canonical(value: str | None) -> str:
    """Compatibility adapter: valid E.164 or empty. Does not emit stripped junk digits."""

    return parse(value).canonical


def contact_handle(value: str | None) -> str:
    """Normalize an iMessage-style handle: email or E.164 phone."""

    raw = re.sub(r"\s+", " ", str(value or "").strip())
    if not raw:
        return ""
    if "@" in raw:
        return raw.lower()
    parsed = parse(raw)
    return parsed.canonical or raw.lower()


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

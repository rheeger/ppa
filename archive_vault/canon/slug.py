"""Filesystem slug canon — punctuation-stripping (slugger algorithm)."""

from __future__ import annotations

import re

_NON_SLUG = re.compile(r"[^a-z0-9\s-]")
_WS = re.compile(r"\s+")
_MULTI_HYPHEN = re.compile(r"-{2,}")


def canonical(value: str | None) -> str:
    """Lowercase, drop punctuation, spaces to hyphens, collapse repeats."""

    slug = str(value or "").lower().strip()
    slug = _NON_SLUG.sub("", slug)
    slug = _WS.sub("-", slug)
    slug = _MULTI_HYPHEN.sub("-", slug)
    return slug.strip("-") or "unknown"

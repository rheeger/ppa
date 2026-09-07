"""Wikilink and person-ref canon."""

from __future__ import annotations

import re

_WRAP = re.compile(r"^\[\[|\]\]$")


def parse(value: str | None) -> str:
    """Strip surrounding ``[[`` ``]]`` and whitespace."""

    return _WRAP.sub("", str(value or "").strip()).strip()


def person_ref(slug: str | None) -> str:
    """``[[slug]]`` person wikilink."""

    cleaned = parse(slug)
    return f"[[{cleaned}]]" if cleaned else ""


def uid_ref(uid: str | None) -> str:
    """``[[uid]]`` card wikilink."""

    cleaned = parse(uid)
    return f"[[{cleaned}]]" if cleaned else ""

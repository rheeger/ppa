"""Single join-key contract for adapters, updaters, seed-links, and serving."""

from __future__ import annotations

from archive_vault.canon import email, handle, instant, phone, place, slug, wikilink
from archive_vault.canon.types import CANON_IDENTIFIER_VERSION, IdentifierResult

CANON_SCHEMA_VERSION = "p31.1"

__all__ = [
    "CANON_IDENTIFIER_VERSION",
    "CANON_SCHEMA_VERSION",
    "IdentifierResult",
    "email",
    "handle",
    "instant",
    "phone",
    "place",
    "slug",
    "wikilink",
]

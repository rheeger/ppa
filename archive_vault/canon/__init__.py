"""Single join-key contract for adapters, updaters, seed-links, and serving."""

from __future__ import annotations

from archive_vault.canon import email, handle, instant, phone, place, slug, wikilink

CANON_SCHEMA_VERSION = "1"

__all__ = [
    "CANON_SCHEMA_VERSION",
    "email",
    "handle",
    "instant",
    "phone",
    "place",
    "slug",
    "wikilink",
]

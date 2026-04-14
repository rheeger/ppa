"""Slug helpers for note paths."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path


def normalize_for_slug(name: str) -> str:
    """Normalize a name into a filesystem-friendly slug."""

    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug)
    slug = re.sub(r"-{2,}", "-", slug)
    return slug.strip("-") or "unknown"


def unique_slug(vault_path: str | Path, base_slug: str, source_id: str) -> str:
    """Return a unique People slug, appending a source-derived suffix if needed."""

    people_dir = Path(vault_path) / "People"
    first_choice = people_dir / f"{base_slug}.md"
    if not first_choice.exists():
        return base_slug

    hash8 = hashlib.sha256(source_id.encode("utf-8")).hexdigest()[:8]
    hashed_slug = f"{base_slug}-{hash8}"
    hashed_path = people_dir / f"{hashed_slug}.md"
    if not hashed_path.exists():
        return hashed_slug

    index = 2
    while True:
        candidate = f"{hashed_slug}-{index}"
        if not (people_dir / f"{candidate}.md").exists():
            return candidate
        index += 1

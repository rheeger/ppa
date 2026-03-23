"""Deterministic HFA UID generation."""

from __future__ import annotations

import hashlib


def generate_uid(prefix: str, source: str, source_id: str) -> str:
    """Return a deterministic UID for a source record."""

    raw = f"{prefix}:{source}:{source_id}".encode("utf-8")
    short = hashlib.sha256(raw).hexdigest()[:12]
    return f"hfa-{prefix}-{short}"

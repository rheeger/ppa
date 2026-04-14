"""Type-aware chunk rendering entrypoint; delegates to chunk builder module."""

from __future__ import annotations

from typing import Any


def render_chunks_for_card(frontmatter: dict[str, Any], body: str) -> list[dict[str, Any]]:
    """Deterministic chunks for a canonical card (same rows as index rebuild)."""
    from .chunk_builders import _build_chunks

    return _build_chunks(frontmatter, body)

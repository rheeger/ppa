"""Read single and multiple notes."""

from __future__ import annotations

import logging
from typing import Any

from ..errors import InvalidInputError
from ..store import DefaultArchiveStore


def read(path_or_uid: str, *, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    """Read one note by path or UID; returns ``store.read`` payload."""
    logger.info("read_start path_or_uid=%r", path_or_uid)
    result = store.read(path_or_uid)
    logger.info("read_done found=%s", bool(result.get("found")))
    return result


def read_many(
    paths_or_uids: list[str],
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Read many notes; returns ``store.read_many`` payload."""
    logger.info("read_many_start count=%s", len(paths_or_uids))
    result = store.read_many(paths_or_uids)
    logger.info("read_many_done count=%s", result.get("count"))
    return result


def parse_paths_json(paths_json: str) -> list[str]:
    """Parse MCP ``paths_json`` parameter into a list of strings."""
    import json

    try:
        paths = json.loads(paths_json)
    except json.JSONDecodeError as exc:
        raise InvalidInputError(f"Invalid JSON: {exc}") from exc
    if not isinstance(paths, list):
        raise InvalidInputError("paths_json must be a JSON array")
    return [str(p) for p in paths]

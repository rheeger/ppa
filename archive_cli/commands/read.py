"""Read single and multiple notes."""

from __future__ import annotations

import logging
from typing import Any

from archive_cli.card_traversal import stack_pointers_from_frontmatter
from archive_vault.yaml_parser import parse_frontmatter

from ..errors import InvalidInputError
from ..store import DefaultArchiveStore
from .confidence import compute_confidence


def read(
    path_or_uid: str,
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    include_attachment_uids: bool = False,
    include_duplicate_uids: bool = False,
) -> dict[str, Any]:
    """Read one note by path or UID; optional stack pointers as links only."""
    logger.info(
        "read_start path_or_uid=%r include_attachment_uids=%s include_duplicate_uids=%s",
        path_or_uid,
        include_attachment_uids,
        include_duplicate_uids,
    )
    result = store.read(path_or_uid)
    logger.info("read_done found=%s", bool(result.get("found")))
    found = bool(result.get("found"))
    result["confidence"] = compute_confidence(
        result_count=1 if found else 0,
        exact_match=found,
        query_text=path_or_uid,
    ).value
    if found and (include_attachment_uids or include_duplicate_uids):
        frontmatter, _body = parse_frontmatter(str(result.get("content") or ""))
        ptrs = stack_pointers_from_frontmatter(frontmatter)
        if include_attachment_uids:
            result["attachment_uids"] = ptrs["attachment_uids"]
        if include_duplicate_uids:
            result["duplicate_uids"] = ptrs["duplicate_uids"]
            if ptrs.get("parent_uid"):
                result["parent_uid"] = ptrs["parent_uid"]
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


def parse_paths_json(paths_json: str | list) -> list[str]:
    """Parse MCP ``paths_json`` into a list of strings.

    mcporter JSON-coerces a stringified array into a real list before the
    tool runs, so accept both a JSON string and an already-decoded list.
    """
    import json

    if isinstance(paths_json, list):
        return [str(p) for p in paths_json]
    try:
        paths = json.loads(paths_json)
    except json.JSONDecodeError as exc:
        raise InvalidInputError(f"Invalid JSON: {exc}") from exc
    if not isinstance(paths, list):
        raise InvalidInputError("paths_json must be a JSON array")
    return [str(p) for p in paths]

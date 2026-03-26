"""Structured query by frontmatter filters."""

from __future__ import annotations

import logging
import time
from typing import Any

from ..store import DefaultArchiveStore


def query(
    *,
    type_filter: str,
    source_filter: str,
    people_filter: str,
    org_filter: str,
    limit: int,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Structured card query; returns rows under ``rows`` key."""
    t0 = time.monotonic()
    logger.info(
        "query_start type=%r source=%r people=%r org=%r limit=%s",
        type_filter,
        source_filter,
        people_filter,
        org_filter,
        limit,
    )
    result = store.query(
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        limit=limit,
    )
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    rows = result.get("rows") or []
    logger.info("query_done elapsed_ms=%s result_count=%s", elapsed_ms, len(rows))
    return result

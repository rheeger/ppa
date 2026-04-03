"""Graph, person profile, and timeline commands."""

from __future__ import annotations

import logging
from typing import Any

from ..store import DefaultArchiveStore


def graph(
    note_path: str,
    *,
    hops: int,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Wikilink graph from a note; wraps ``store.graph``."""
    logger.info("graph_start note_path=%r hops=%s", note_path, hops)
    result = store.graph(note_path, hops=hops)
    logger.info("graph_done has_graph=%s", result.get("graph") is not None)
    return result


def person(name: str, *, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    """Resolve person note content via ``store.person`` (slug fallback included)."""
    logger.info("person_start name=%r", name)
    result = store.person(name)
    logger.info("person_done found=%s", result.get("found"))
    return result


def timeline(
    *,
    start_date: str,
    end_date: str,
    limit: int,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Notes in a date range; wraps ``store.timeline``."""
    logger.info("timeline_start start=%r end=%r limit=%s", start_date, end_date, limit)
    result = store.timeline(start_date=start_date, end_date=end_date, limit=limit)
    rows = result.get("rows") or []
    logger.info("timeline_done result_count=%s", len(rows))
    return result


def temporal_neighbors(
    timestamp: str,
    *,
    direction: str = "both",
    limit: int = 20,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    logger.info("temporal_neighbors_start ts=%r direction=%s", timestamp, direction)
    result = store.temporal_neighbors(
        timestamp,
        direction=direction,
        limit=limit,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
    )
    n = len(result.get("results") or [])
    logger.info("temporal_neighbors_done count=%s", n)
    return result


def knowledge_domain(
    domain: str,
    *,
    fallback_query: str = "",
    limit: int = 5,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    logger.info("knowledge_domain_start domain=%r", domain)
    result = store.knowledge_for_domain(domain, fallback_query=fallback_query, limit=limit)
    logger.info("knowledge_domain_done ok=%s fallback=%s", result.get("ok"), result.get("fallback"))
    return result

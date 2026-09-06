"""Graph, person profile, and timeline commands."""

from __future__ import annotations

import logging
from typing import Any

from archive_cli.retrieval_pipeline import PIPELINE_VERSION

from ..store import DefaultArchiveStore
from .confidence import attach_retrieval_envelope, detect_gaps, log_gaps


def _graph_edge_count(graph: dict | None) -> int:
    if not graph:
        return 0
    n = 0
    for _src, targets in graph.items():
        if isinstance(targets, list):
            n += len(targets)
    return n


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
    g = result.get("graph")
    ec = _graph_edge_count(g if isinstance(g, dict) else None)
    attach_retrieval_envelope(
        result,
        query=note_path,
        result_count=ec,
        method="graph",
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=f"graph:{note_path}", result_count=ec)
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result


def person(name: str, *, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    """Resolve person note content via ``store.person`` (slug fallback included)."""
    logger.info("person_start name=%r", name)
    result = store.person(name)
    logger.info("person_done found=%s", result.get("found"))
    found = bool(result.get("found"))
    attach_retrieval_envelope(
        result,
        query=name,
        result_count=1 if found else 0,
        exact_match=found,
        method="exact" if found else "unknown",
        evidence_kind="source_reported" if found else "unknown",
        pipeline_version=PIPELINE_VERSION,
    )
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
    qtext = f"timeline:{start_date}..{end_date}"
    attach_retrieval_envelope(
        result,
        query=qtext,
        limit=limit,
        method="timeline",
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=qtext, result_count=len(rows))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
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
    qtext = f"temporal_neighbors:{timestamp}"
    attach_retrieval_envelope(
        result,
        query=qtext,
        rows_key="results",
        result_count=n,
        limit=limit,
        method="temporal",
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=qtext, result_count=n)
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
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
    rows = result.get("rows") or []
    if not isinstance(rows, list):
        rows = []
    qtext = f"knowledge:{domain}"
    attach_retrieval_envelope(
        result,
        query=qtext,
        limit=limit,
        method="knowledge",
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=qtext, result_count=len(rows))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result

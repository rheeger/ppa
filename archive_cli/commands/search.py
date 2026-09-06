"""Lexical, vector, and hybrid search commands."""

from __future__ import annotations

import logging
import time
from typing import Any

from archive_cli.retrieval_pipeline import PIPELINE_VERSION

from ..store import DefaultArchiveStore
from .confidence import attach_retrieval_envelope, detect_gaps, log_gaps


def search(
    query: str,
    *,
    limit: int,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Full-text search; returns same dict as ``store.search`` (rows list)."""
    t0 = time.monotonic()
    logger.info("search_start query=%r limit=%s", query, limit)
    result = store.search(query, limit=limit)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    rows = result.get("rows") or []
    logger.info("search_done elapsed_ms=%s result_count=%s", elapsed_ms, len(rows))
    attach_retrieval_envelope(
        result,
        query=query,
        limit=limit,
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=query, result_count=len(rows))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result


def vector_search(
    query: str,
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    **kwargs: Any,
) -> dict[str, Any]:
    """Semantic search over embedded chunks; wraps ``store.vector_search``."""
    t0 = time.monotonic()
    logger.info("vector_search_start query=%r kwargs=%s", query, kwargs)
    result = store.vector_search(query, **kwargs)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    rows = result.get("rows") or []
    logger.info("vector_search_done elapsed_ms=%s result_count=%s", elapsed_ms, len(rows))
    attach_retrieval_envelope(
        result,
        query=query,
        limit=int(kwargs.get("limit", 0) or 0) or None,
        method="vector",
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=query, result_count=len(rows))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result


def hybrid_search(
    query: str,
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    **kwargs: Any,
) -> dict[str, Any]:
    """Hybrid lexical + vector retrieval; wraps ``store.hybrid_search``."""
    t0 = time.monotonic()
    logger.info("hybrid_search_start query=%r kwargs=%s", query, kwargs)
    result = store.hybrid_search(query, **kwargs)
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    rows = result.get("rows") or []
    logger.info("hybrid_search_done elapsed_ms=%s result_count=%s", elapsed_ms, len(rows))
    attach_retrieval_envelope(
        result,
        query=query,
        limit=int(kwargs.get("limit", 0) or 0) or None,
        method="hybrid",
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=query, result_count=len(rows))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result

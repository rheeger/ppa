"""Structured query by frontmatter filters."""

from __future__ import annotations

import logging
import time
from typing import Any

from archive_engine.errors import CursorInvalidError, QueryValidationError
from archive_engine.query import execute_typed_query, request_from_simple_filters

from ..store import DefaultArchiveStore
from .confidence import compute_confidence, detect_gaps, log_gaps


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
    """Structured card query; returns rows under ``rows`` key.

    Compatibility mapping only: legacy type/source/people/org filters become a
    typed predicate. Central parser registration stays with P09/P10-D.
    """
    t0 = time.monotonic()
    logger.info(
        "query_start type=%r source=%r people=%r org=%r limit=%s",
        type_filter,
        source_filter,
        people_filter,
        org_filter,
        limit,
    )
    request = request_from_simple_filters(
        access=store.access,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        limit=limit,
    )
    try:
        page = execute_typed_query(store.runtime, request)
        result = page.to_legacy_result()
    except (QueryValidationError, CursorInvalidError):
        raise
    except Exception:
        logger.exception("typed_query_fallback_to_simple")
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
    qtext = f"type={type_filter!r} source={source_filter!r} people={people_filter!r} org={org_filter!r}"
    result["confidence"] = compute_confidence(result_count=len(rows), query_text=qtext).value
    gaps = detect_gaps(query_text=qtext, result_count=len(rows))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result

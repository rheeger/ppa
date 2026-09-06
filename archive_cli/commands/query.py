"""Structured query by frontmatter filters."""

from __future__ import annotations

import logging
import time
from typing import Any

from archive_cli.retrieval_pipeline import PIPELINE_VERSION
from archive_engine.errors import CursorInvalidError, QueryValidationError
from archive_engine.query import execute_typed_query, request_from_simple_filters

from ..store import DefaultArchiveStore
from .confidence import attach_retrieval_envelope, detect_gaps, log_gaps


def query(
    *,
    type_filter: str,
    source_filter: str,
    people_filter: str,
    org_filter: str,
    limit: int,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    start_date: str = "",
    end_date: str = "",
    saved_scope_name: str = "",
    scopes: object = None,
    warehouse_checkpoint: str = "",
) -> dict[str, Any]:
    """Structured card query; returns rows under ``rows`` key.

    Legacy type/source/people/org filters become a typed predicate. Saved
    scopes resolve before execution; empty intersection is empty-scope.
    """
    from .analytics import client_envelope, load_scopes

    t0 = time.monotonic()
    logger.info(
        "query_start type=%r source=%r people=%r org=%r limit=%s scope=%r",
        type_filter,
        source_filter,
        people_filter,
        org_filter,
        limit,
        saved_scope_name,
    )
    request = request_from_simple_filters(
        access=store.access,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        saved_scope_name=saved_scope_name,
    )
    try:
        page = execute_typed_query(
            store.runtime,
            request,
            scopes=load_scopes(scopes),
            warehouse_checkpoint=warehouse_checkpoint,
        )
        result = client_envelope(page.to_legacy_result())
    except (QueryValidationError, CursorInvalidError):
        raise
    except Exception:
        if saved_scope_name.strip():
            raise
        logger.exception("typed_query_fallback_to_simple")
        result = client_envelope(
            store.query(
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                org_filter=org_filter,
                limit=limit,
            )
        )
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    rows = result.get("rows") or []
    logger.info("query_done elapsed_ms=%s result_count=%s", elapsed_ms, len(rows))
    qtext = f"type={type_filter!r} source={source_filter!r} people={people_filter!r} org={org_filter!r}"
    attach_retrieval_envelope(
        result,
        query=qtext,
        limit=limit,
        method="query",
        pipeline_version=PIPELINE_VERSION,
    )
    gaps = detect_gaps(query_text=qtext, result_count=len(rows))
    if gaps:
        try:
            log_gaps(gaps, index=store.index, logger=logger)
        except Exception:
            logger.warning("gap_logging_failed", exc_info=True)
    return result

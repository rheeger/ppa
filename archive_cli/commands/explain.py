"""Retrieval explain command."""

from __future__ import annotations

import logging
from typing import Any

from ..store import DefaultArchiveStore


def retrieval_explain(
    query: str,
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    **kwargs: Any,
) -> dict[str, Any]:
    """Structured retrieval explanation (v2 schema); wraps ``store.retrieval_explain``."""
    logger.info("retrieval_explain_start query=%r kwargs=%s", query, kwargs)
    result = store.retrieval_explain(query, **kwargs)
    logger.info("retrieval_explain_done")
    return result

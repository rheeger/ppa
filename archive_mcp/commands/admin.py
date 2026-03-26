"""Bootstrap, rebuild, embed, and projection admin commands."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..errors import IndexUnavailableError
from ..index_store import PostgresArchiveIndex, get_index_dsn
from ..store import DefaultArchiveStore


def bootstrap_postgres(*, vault: Path, logger: logging.Logger) -> dict[str, Any]:
    """Create extensions and base schema via ``PostgresArchiveIndex.bootstrap``."""
    logger.info("bootstrap_postgres_start vault=%s", vault)
    dsn = get_index_dsn()
    if not dsn:
        raise IndexUnavailableError("PPA_INDEX_DSN is required")
    result = PostgresArchiveIndex(vault, dsn=dsn).bootstrap()
    logger.info("bootstrap_postgres_done keys=%s", list(result.keys()))
    return result


def rebuild_indexes(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    **kwargs: Any,
) -> dict[str, Any]:
    """Rebuild derived index tables; forwards kwargs to ``store.rebuild``."""
    logger.info("rebuild_indexes_start kwargs=%s", kwargs)
    result = store.rebuild(**kwargs)
    logger.info("rebuild_indexes_done")
    return result


def embed_pending(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    **kwargs: Any,
) -> dict[str, Any]:
    """Embed pending chunks; forwards kwargs to ``store.embed_pending``."""
    logger.info("embed_pending_start kwargs=%s", kwargs)
    result = store.embed_pending(**kwargs)
    logger.info(
        "embed_pending_done embedded=%s failed=%s",
        result.get("embedded"),
        result.get("failed"),
    )
    return result


def projection_inventory(*, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    """List registered projections."""
    logger.info("projection_inventory_start")
    result = store.projection_inventory()
    logger.info("projection_inventory_done")
    return result


def projection_status(*, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    """Projection coverage and readiness."""
    logger.info("projection_status_start")
    result = store.projection_status()
    logger.info("projection_status_done")
    return result


def projection_explain(
    card_uid: str,
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Explain typed projection for one card UID."""
    logger.info("projection_explain_start card_uid=%r", card_uid)
    result = store.projection_explain(card_uid)
    logger.info("projection_explain_done")
    return result

"""Bootstrap, rebuild, embed, and projection admin commands."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..errors import IndexUnavailableError
from ..index_store import PostgresArchiveIndex, get_index_dsn
from ..store import DefaultArchiveStore


def bootstrap_postgres(
    *,
    vault: Path,
    logger: logging.Logger,
    force: bool = False,
) -> dict[str, Any]:
    """Create extensions and base schema via ``PostgresArchiveIndex.bootstrap``.

    Refuses to run on a populated schema unless ``force=True`` (or the
    ``PPA_BOOTSTRAP_FORCE=1`` env var) is set, since ``recreate_typed=True``
    in the underlying ``_create_schema`` ``DROP TABLE … CASCADE``s every
    typed projection.
    """
    logger.info("bootstrap_postgres_start vault=%s force=%s", vault, force)
    dsn = get_index_dsn()
    if not dsn:
        raise IndexUnavailableError("PPA_INDEX_DSN is required")
    result = PostgresArchiveIndex(vault, dsn=dsn).bootstrap(force=force)
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


def embed_gc(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Prune ``embeddings`` rows whose ``chunk_key`` no longer exists in ``chunks``.

    Embeddings are content-addressable (Migration 004 decoupled them from chunk
    row lifecycle). Orphans accumulate when a card's content changes (its old
    chunk_key disappears from ``chunks`` but the matching embedding row stays).
    Run this on demand after rebuilds or large content changes.
    """
    schema = store.index.schema
    with store.index._connect() as conn:  # noqa: SLF001
        total = conn.execute(f"SELECT COUNT(*) FROM {schema}.embeddings").fetchone()
        orphan = conn.execute(
            f"""
            SELECT COUNT(*) FROM {schema}.embeddings e
            WHERE NOT EXISTS (SELECT 1 FROM {schema}.chunks c WHERE c.chunk_key = e.chunk_key)
            """
        ).fetchone()
        total_count = int(total[0] if not isinstance(total, dict) else next(iter(total.values())))
        orphan_count = int(orphan[0] if not isinstance(orphan, dict) else next(iter(orphan.values())))
        logger.info(
            "embed_gc_scan total=%d orphan=%d dry_run=%s",
            total_count,
            orphan_count,
            dry_run,
        )
        deleted = 0
        if not dry_run and orphan_count > 0:
            cur = conn.execute(
                f"""
                DELETE FROM {schema}.embeddings e
                WHERE NOT EXISTS (SELECT 1 FROM {schema}.chunks c WHERE c.chunk_key = e.chunk_key)
                """
            )
            deleted = int(cur.rowcount or 0)
            conn.commit()
            logger.info("embed_gc_deleted rows=%d", deleted)
    return {
        "total_embeddings": total_count,
        "orphan_embeddings": orphan_count,
        "deleted": deleted,
        "dry_run": dry_run,
    }

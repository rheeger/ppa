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
    try:
        from archive_cli.index_store import PostgresArchiveIndex
        from archive_cli.serving_index import publish_serving_index

        if isinstance(getattr(store, "index", None), PostgresArchiveIndex):
            published = publish_serving_index(store, logger=logger)
            result = dict(result)
            result["serving_index"] = published
            if not published.get("ok"):
                logger.error("serving_index_refresh_failed")
    except Exception:
        logger.exception("serving_index_refresh_failed")
    logger.info("rebuild_indexes_done")
    return result


def embed_pending(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    **kwargs: Any,
) -> dict[str, Any]:
    """Embed pending chunks; forwards kwargs to ``store.embed_pending``.

    This is the unscoped admin route. Dirty processor embed must pass an
    allowlist on ``store.embed_pending`` directly and must not come through here.
    """
    if kwargs.get("uid_allowlist") is None and kwargs.get("chunk_key_allowlist") is None:
        kwargs["unscoped"] = True
    logger.info("embed_pending_start kwargs=%s", kwargs)
    result = store.embed_pending(**kwargs)
    logger.info(
        "embed_pending_done embedded=%s failed=%s",
        result.get("embedded"),
        result.get("failed"),
    )
    return result


def serving_index_status(*, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    from archive_cli.serving_index import serving_index_status as _status

    logger.info("serving_index_status_start")
    result = _status(store.vault)
    logger.info("serving_index_status_done generation=%s", result.get("serving_index_generation"))
    return result


def serving_index_verify(*, store: DefaultArchiveStore, logger: logging.Logger) -> dict[str, Any]:
    from archive_cli.serving_index import verify_serving_index

    logger.info("serving_index_verify_start")
    result = verify_serving_index(store.vault)
    logger.info("serving_index_verify_done generation=%s", result.get("generation"))
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
    duplicates: bool = False,
    batch_size: int = 10_000,
) -> dict[str, Any]:
    """Prune leftover embeddings after rematerialize attach.

    Default: delete orphans whose ``content_hash`` is unused by every live chunk.
    ``duplicates=True``: delete leftovers whose identity already has a live list.
    Empty-hash leftovers stay until they can be identified.
    """
    schema = store.index.schema
    def _unused_sql(alias: str) -> str:
        return f"""
        NOT EXISTS (SELECT 1 FROM {schema}.chunks c WHERE c.chunk_key = {alias}.chunk_key)
        AND {alias}.content_hash <> ''
        AND NOT EXISTS (
            SELECT 1 FROM {schema}.chunks live
            WHERE live.content_hash = {alias}.content_hash AND live.content_hash <> ''
        )
        """

    def _duplicate_sql(alias: str) -> str:
        return f"""
        NOT EXISTS (SELECT 1 FROM {schema}.chunks c WHERE c.chunk_key = {alias}.chunk_key)
        AND {alias}.content_hash <> ''
        AND EXISTS (
            SELECT 1
            FROM {schema}.chunks live
            JOIN {schema}.embeddings have
              ON have.chunk_key = live.chunk_key
             AND have.embedding_model = {alias}.embedding_model
             AND have.embedding_version = {alias}.embedding_version
            WHERE live.content_hash = {alias}.content_hash
              AND live.content_hash <> ''
        )
        """

    unused_sql = _unused_sql("e")
    duplicate_sql = _duplicate_sql("e")
    batch = max(int(batch_size or 10_000), 1)
    with store.index._connect() as conn:  # noqa: SLF001
        conn.execute("SET statement_timeout = 0")
        total = conn.execute(f"SELECT COUNT(*) FROM {schema}.embeddings").fetchone()
        orphan = conn.execute(
            f"""
            SELECT COUNT(*) FROM {schema}.embeddings e
            WHERE NOT EXISTS (SELECT 1 FROM {schema}.chunks c WHERE c.chunk_key = e.chunk_key)
            """
        ).fetchone()
        unused = conn.execute(
            f"SELECT COUNT(*) FROM {schema}.embeddings e WHERE {unused_sql}"
        ).fetchone()
        duplicate = conn.execute(
            f"SELECT COUNT(*) FROM {schema}.embeddings e WHERE {duplicate_sql}"
        ).fetchone()
        total_count = int(total[0] if not isinstance(total, dict) else next(iter(total.values())))
        orphan_count = int(orphan[0] if not isinstance(orphan, dict) else next(iter(orphan.values())))
        unused_count = int(unused[0] if not isinstance(unused, dict) else next(iter(unused.values())))
        duplicate_count = int(
            duplicate[0] if not isinstance(duplicate, dict) else next(iter(duplicate.values()))
        )
        logger.info(
            "embed_gc_scan total=%d orphan=%d unused_hash=%d duplicates=%d mode=%s dry_run=%s",
            total_count,
            orphan_count,
            unused_count,
            duplicate_count,
            "duplicates" if duplicates else "unused_hash",
            dry_run,
        )
        deleted = 0
        if not dry_run:
            if duplicates:
                while True:
                    cur = conn.execute(
                        f"""
                        DELETE FROM {schema}.embeddings e
                        WHERE ctid IN (
                            SELECT e2.ctid FROM {schema}.embeddings e2
                            WHERE {_duplicate_sql("e2")}
                            LIMIT %s
                        )
                        """,
                        (batch,),
                    )
                    n = int(cur.rowcount or 0)
                    deleted += n
                    conn.commit()
                    logger.info("embed_gc_duplicate_batch deleted=%d total_deleted=%d", n, deleted)
                    if n < batch:
                        break
            elif unused_count > 0:
                cur = conn.execute(f"DELETE FROM {schema}.embeddings e WHERE {unused_sql}")
                deleted = int(cur.rowcount or 0)
                conn.commit()
                logger.info("embed_gc_deleted rows=%d", deleted)
    return {
        "total_embeddings": total_count,
        "orphan_embeddings": orphan_count,
        "unused_hash_embeddings": unused_count,
        "duplicate_embeddings": duplicate_count,
        "deleted": deleted,
        "dry_run": dry_run,
        "duplicates": duplicates,
    }


def embed_reuse(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> dict[str, Any]:
    """Copy existing vectors onto new chunk keys that share ``content_hash``."""
    from archive_cli.index_config import get_default_embedding_model, get_default_embedding_version

    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    logger.info("embed_reuse_start model=%s version=%s", model, version)
    store.index.backfill_embedding_content_identity()
    result = store.index.reuse_embeddings_by_content(embedding_model=model, embedding_version=version)
    logger.info("embed_reuse_done copied=%s pending_after=%s", result.get("copied"), result.get("pending_after"))
    return {"embedding_model": model, "embedding_version": version, **result}


def embed_remap_slots(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    chunks_jsonl: str,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> dict[str, Any]:
    """One-time remap of orphan vectors onto current keys by card/type/index."""
    from archive_cli.index_config import get_default_embedding_model, get_default_embedding_version

    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    logger.info("embed_remap_slots_start jsonl=%s model=%s version=%s", chunks_jsonl, model, version)
    loaded = store.index.load_slot_map_from_chunks_jsonl(chunks_jsonl)
    remapped = store.index.remap_embeddings_by_slot(embedding_model=model, embedding_version=version)
    reused = store.index.reuse_embeddings_by_content(embedding_model=model, embedding_version=version)
    logger.info(
        "embed_remap_slots_done loaded=%s remapped=%s reused=%s",
        loaded,
        remapped.get("copied"),
        reused.get("copied"),
    )
    return {
        "embedding_model": model,
        "embedding_version": version,
        "slot_map_rows": loaded,
        "remapped": remapped,
        "reused": reused,
    }


def embed_remap_schema(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    schema_versions: tuple[int, ...] | None = None,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> dict[str, Any]:
    """Copy vectors from pre-bump chunk_keys onto current keys."""
    from archive_cli.index_config import get_default_embedding_model, get_default_embedding_version

    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    logger.info("embed_remap_schema_start versions=%s model=%s version=%s", schema_versions, model, version)
    remapped = store.index.remap_embeddings_by_prior_schema(
        embedding_model=model,
        embedding_version=version,
        schema_versions=schema_versions,
    )
    reused = store.index.reuse_embeddings_by_content(embedding_model=model, embedding_version=version)
    logger.info(
        "embed_remap_schema_done remapped=%s reused=%s pending_after=%s",
        remapped.get("copied"),
        reused.get("copied"),
        reused.get("pending_after"),
    )
    return {
        "embedding_model": model,
        "embedding_version": version,
        "remapped": remapped,
        "reused": reused,
    }

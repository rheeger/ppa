"""CLI command handlers for the OpenAI Batch API embedding path."""

from __future__ import annotations

import logging
from typing import Any

from ..batch_embedder import batch_status as _batch_status
from ..batch_embedder import ingest_completed_batches as _ingest
from ..batch_embedder import poll_batches as _poll
from ..batch_embedder import submit_batches as _submit
from ..store import DefaultArchiveStore


def embed_batch_submit(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    embedding_model: str = "",
    embedding_version: int = 0,
    max_batches: int = 0,
    requests_per_batch: int = 0,
    include_context_prefix: bool | None = None,
    artifact_dir: str = "",
) -> dict[str, Any]:
    """Submit up to ``max_batches`` OpenAI batches for pending chunks."""
    ctx = store.config.retrieval.get("context", {}) if include_context_prefix is None else {}
    include_ctx = (
        bool(ctx.get("include_in_embeddings", True)) if include_context_prefix is None else bool(include_context_prefix)
    )
    logger.info(
        "embed_batch_submit_start max_batches=%s requests_per_batch=%s include_context_prefix=%s",
        max_batches,
        requests_per_batch,
        include_ctx,
    )
    result = _submit(
        index=store.index,
        logger_=logger,
        embedding_model=embedding_model,
        embedding_version=embedding_version,
        max_batches=max_batches,
        requests_per_batch=requests_per_batch or 50_000,
        include_context_prefix=include_ctx,
        artifact_dir=artifact_dir or None,
    )
    logger.info(
        "embed_batch_submit_done submitted=%s total_requests=%s",
        result.get("submitted_batches"),
        result.get("total_requests"),
    )
    return result


def embed_batch_poll(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Refresh status counts for every non-ingested batch."""
    logger.info("embed_batch_poll_start")
    result = _poll(index=store.index, logger_=logger)
    logger.info("embed_batch_poll_done polled=%s", result.get("polled"))
    return result


def embed_batch_ingest(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
    artifact_dir: str = "",
    write_batch_size: int = 500,
    workers: int = 1,
) -> dict[str, Any]:
    """Download output files for completed batches and upsert into ``embeddings``."""
    logger.info(
        "embed_batch_ingest_start write_batch_size=%s workers=%s",
        write_batch_size,
        workers,
    )
    result = _ingest(
        index=store.index,
        logger_=logger,
        artifact_dir=artifact_dir or None,
        write_batch_size=max(1, write_batch_size),
        workers=max(1, workers),
    )
    logger.info(
        "embed_batch_ingest_done ingested=%s written=%s failed=%s",
        result.get("ingested_batches"),
        result.get("total_written"),
        result.get("total_failed"),
    )
    return result


def embed_batch_status(
    *,
    store: DefaultArchiveStore,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Return a one-shot status summary across all batches."""
    logger.info("embed_batch_status_start")
    result = _batch_status(index=store.index, logger_=logger)
    logger.info("embed_batch_status_done totals=%s", result.get("totals"))
    return result

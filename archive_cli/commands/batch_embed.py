"""CLI command handlers for the OpenAI Batch API embedding path."""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..batch_embedder import DEFAULT_BATCH_ARTIFACT_DIR
from ..batch_embedder import batch_status as _batch_status
from ..batch_embedder import ingest_completed_batches as _ingest
from ..batch_embedder import poll_batches as _poll
from ..batch_embedder import submit_batches as _submit
from ..store import DefaultArchiveStore

DEFAULT_RECOVERY_CACHE_DIR = "_artifacts/_embedding-recovery-cache"


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


def embed_cache_rotate(
    *,
    logger: logging.Logger,
    artifact_dir: str = "",
    cache_dir: str = "",
    keep: int = 1,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Move ingested ``*-out.jsonl`` files into a long-term recovery cache.

    After a successful ``embed-batch-ingest`` pass, the downloaded output JSONL
    files in ``_artifacts/_embedding-runs/batches/`` are no longer used by the
    embedder pipeline. Moving them into ``_artifacts/_embedding-recovery-cache/run-{ts}/``
    keeps them on disk as a "warm cache" — if a future event ever destroys the
    ``embeddings`` table again, we can re-ingest from local files instead of
    hoping OpenAI still has them within their ~30-day file-retention window.

    Default keeps only the most recent run (older ``run-*/`` directories are
    pruned). Pass ``keep=N`` to retain N runs; ``dry_run=True`` reports what
    would happen without moving or pruning anything.

    Atomicity: each ``Path.rename()`` is atomic on a single filesystem. Files
    move one-by-one; if one fails the rest still get moved (we log the error
    and continue).
    """
    art = Path(artifact_dir or DEFAULT_BATCH_ARTIFACT_DIR)
    cache = Path(cache_dir or DEFAULT_RECOVERY_CACHE_DIR)
    out_files = sorted(art.glob("*-out.jsonl"))
    logger.info(
        "embed_cache_rotate_start artifact_dir=%s cache_dir=%s out_files=%d keep=%d dry_run=%s",
        art,
        cache,
        len(out_files),
        keep,
        dry_run,
    )

    moved: list[str] = []
    move_errors: list[str] = []
    new_run: Path | None = None
    if out_files:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
        new_run = cache / f"run-{ts}"
        if not dry_run:
            new_run.mkdir(parents=True, exist_ok=True)
            manifest = new_run / "MANIFEST.txt"
            manifest.write_text(
                f"# embedding recovery cache run {ts}\n"
                f"# rotated_from: {art.resolve()}\n"
                f"# files: {len(out_files)}\n",
                encoding="utf-8",
            )
            for f in out_files:
                try:
                    f.rename(new_run / f.name)
                    moved.append(f.name)
                except OSError as exc:
                    move_errors.append(f"{f.name}: {exc}")
                    logger.warning("embed_cache_rotate_move_failed file=%s error=%s", f, exc)
        else:
            moved = [f.name for f in out_files]

    runs = sorted(
        [d for d in cache.glob("run-*") if d.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )
    if new_run is not None and new_run not in runs and not dry_run:
        runs = sorted([new_run, *runs], key=lambda p: p.name, reverse=True)
    keep = max(1, keep)
    keep_runs = runs[:keep]
    prune_runs = runs[keep:]
    pruned: list[str] = []
    for d in prune_runs:
        if dry_run:
            pruned.append(str(d))
            continue
        try:
            shutil.rmtree(d)
            pruned.append(str(d))
        except OSError as exc:
            logger.warning("embed_cache_rotate_prune_failed dir=%s error=%s", d, exc)

    logger.info(
        "embed_cache_rotate_done moved=%d errors=%d kept_runs=%d pruned_runs=%d new_run=%s",
        len(moved),
        len(move_errors),
        len(keep_runs),
        len(pruned),
        new_run,
    )
    return {
        "artifact_dir": str(art),
        "cache_dir": str(cache),
        "new_run": str(new_run) if new_run else "",
        "moved": len(moved),
        "move_errors": move_errors,
        "kept_runs": [str(r) for r in keep_runs],
        "pruned_runs": pruned,
        "dry_run": dry_run,
    }

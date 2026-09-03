"""Process-local vault-cache reuse — does not change on-disk cache row shape.

``VaultScanCache.build_or_load`` fingerprints the whole vault (~40s on the seed)
on every call. Nightly maintain does that more than a dozen times. This module
wraps ``build_or_load`` for a single process so later callers skip the walk when
the in-memory cache is still valid.

Installed by ``ppa maintain`` only (``install_process_reuse``). Tests keep the
stock path unless they opt in. Writing adapters must call ``mark_vault_written``
so the next load incrementally refreshes.

During a source-updater batch, call ``begin_defer_vault_written`` / ``flush_deferred_vault_written``
so all updaters reuse one warm cache; fingerprint once after the batch.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any

from archive_cli.vault_cache import VaultScanCache

logger = logging.getLogger("ppa.vault_cache")

_LOCK = threading.RLock()
_CACHES: dict[str, VaultScanCache] = {}
_DIRTY: set[str] = set()
_DEFERRED_WRITTEN: set[str] = set()
_DEFER_DEPTH = 0
_INSTALLED = False
_ORIG_BUILD = VaultScanCache.build_or_load


def _vault_key(vault: Path | str) -> str:
    return str(Path(vault).resolve())


def defer_vault_written_active() -> bool:
    with _LOCK:
        return _DEFER_DEPTH > 0


def begin_defer_vault_written() -> None:
    """Coalesce ``mark_vault_written`` until ``flush_deferred_vault_written``."""

    global _DEFER_DEPTH
    with _LOCK:
        _DEFER_DEPTH += 1
    logger.info("vault-cache defer_vault_written begin depth=%s", _DEFER_DEPTH)


def end_defer_vault_written(*, flush: bool = True) -> None:
    """Leave defer mode; optionally flush pending invalidations."""

    global _DEFER_DEPTH
    with _LOCK:
        _DEFER_DEPTH = max(0, _DEFER_DEPTH - 1)
        depth = _DEFER_DEPTH
    logger.info("vault-cache defer_vault_written end depth=%s flush=%s", depth, flush)
    if depth == 0 and flush:
        flush_deferred_vault_written()


def flush_deferred_vault_written() -> int:
    """Apply all deferred ``mark_vault_written`` calls (does not rebuild)."""

    with _LOCK:
        pending = set(_DEFERRED_WRITTEN)
        _DEFERRED_WRITTEN.clear()
        for key in pending:
            _DIRTY.add(key)
    if pending:
        logger.info("vault-cache flush_deferred count=%s", len(pending))
    return len(pending)


def mark_vault_written(vault: Path | str) -> None:
    """Next ``build_or_load`` for this vault must refresh (fingerprint + incremental)."""

    key = _vault_key(vault)
    with _LOCK:
        if _DEFER_DEPTH > 0:
            _DEFERRED_WRITTEN.add(key)
            logger.info("vault-cache mark_written deferred vault=%s", key)
            return
        _DIRTY.add(key)
    logger.info("vault-cache mark_written vault=%s", key)
    try:
        from archive_cli.serving_index import mark_serving_index_dirty

        mark_serving_index_dirty(vault, "vault_written")
    except Exception:
        logger.debug("serving_index mark_dirty after vault_written failed", exc_info=True)


def rebuild_vault_cache_after_writes(
    vault: Path | str,
    *,
    tier: int = 1,
    progress_every: int = 5000,
) -> VaultScanCache:
    """Flush deferred invalidations and run one incremental cache rebuild."""

    flush_deferred_vault_written()
    started = time.monotonic()
    logger.info("vault-cache maintain rebuild start vault=%s tier=%s", _vault_key(vault), tier)
    cache = VaultScanCache.build_or_load(
        Path(vault),
        tier=tier,
        progress_every=progress_every,
    )
    logger.info(
        "vault-cache maintain rebuild done vault=%s notes=%s elapsed=%.1fs",
        _vault_key(vault),
        cache.note_count(),
        time.monotonic() - started,
    )
    return cache


def clear_process_cache() -> None:
    with _LOCK:
        _CACHES.clear()
        _DIRTY.clear()
        _DEFERRED_WRITTEN.clear()


def process_reuse_installed() -> bool:
    return _INSTALLED


def _wrapped_build_or_load(
    cls: type[VaultScanCache],
    vault: Path,
    *,
    tier: int = 1,
    workers: int = 1,
    progress_every: int = 5000,
    no_cache: bool = False,
) -> VaultScanCache:
    key = _vault_key(vault)
    with _LOCK:
        existing = _CACHES.get(key)
        dirty = key in _DIRTY
        if existing is not None and not no_cache and not dirty and existing.tier() >= tier:
            logger.info(
                "vault-cache process-hit vault=%s tier=%s notes=%s skip_fingerprint=1",
                key,
                existing.tier(),
                existing.note_count(),
            )
            return existing
    cache = _ORIG_BUILD(
        vault,
        tier=tier,
        workers=workers,
        progress_every=progress_every,
        no_cache=no_cache,
    )
    with _LOCK:
        _CACHES[key] = cache
        _DIRTY.discard(key)
    return cache


def install_process_reuse() -> None:
    """Patch ``VaultScanCache.build_or_load`` for this process. Idempotent."""

    global _INSTALLED
    if _INSTALLED:
        return
    VaultScanCache.build_or_load = classmethod(_wrapped_build_or_load)  # type: ignore[method-assign]
    _INSTALLED = True
    logger.info("vault-cache process reuse installed")


def uninstall_process_reuse() -> None:
    """Restore stock ``build_or_load``. Used by unit tests."""

    global _INSTALLED, _DEFER_DEPTH
    if not _INSTALLED:
        return
    VaultScanCache.build_or_load = _ORIG_BUILD  # type: ignore[method-assign]
    _INSTALLED = False
    _DEFER_DEPTH = 0
    clear_process_cache()
    logger.info("vault-cache process reuse uninstalled")


def body_for_rel_path(cache: Any, rel_path: str, vault: Path | str) -> str:
    """Return body from cache, or the note on disk when tier-2 is missing."""

    try:
        return str(cache.body_for_rel_path(rel_path))
    except ValueError:
        pass
    from archive_vault.vault import read_note_file

    note = read_note_file(Path(vault) / rel_path, vault_root=Path(vault))
    return str(note.body or "")


def raw_content_sha256_for_rel_path(cache: Any, rel_path: str, vault: Path | str) -> str:
    try:
        return str(cache.raw_content_sha256_for_rel_path(rel_path))
    except ValueError:
        pass
    import hashlib

    raw = (Path(vault) / rel_path).read_bytes()
    return hashlib.sha256(raw).hexdigest()

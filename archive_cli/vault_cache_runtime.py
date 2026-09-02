"""Process-local vault-cache reuse — does not change on-disk cache row shape.

``VaultScanCache.build_or_load`` fingerprints the whole vault (~40s on the seed)
on every call. Nightly maintain does that more than a dozen times. This module
wraps ``build_or_load`` for a single process so later callers skip the walk when
the in-memory cache is still valid.

Installed by ``ppa maintain`` only (``install_process_reuse``). Tests keep the
stock path unless they opt in. Writing adapters must call ``mark_vault_written``
so the next load incrementally refreshes.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

from archive_cli.vault_cache import VaultScanCache

logger = logging.getLogger("ppa.vault_cache")

_LOCK = threading.RLock()
_CACHES: dict[str, VaultScanCache] = {}
_DIRTY: set[str] = set()
_INSTALLED = False
_ORIG_BUILD = VaultScanCache.build_or_load


def _vault_key(vault: Path | str) -> str:
    return str(Path(vault).resolve())


def mark_vault_written(vault: Path | str) -> None:
    """Next ``build_or_load`` for this vault must refresh (fingerprint + incremental)."""

    key = _vault_key(vault)
    with _LOCK:
        _DIRTY.add(key)
    logger.info("vault-cache mark_written vault=%s", key)


def clear_process_cache() -> None:
    with _LOCK:
        _CACHES.clear()
        _DIRTY.clear()


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

    global _INSTALLED
    if not _INSTALLED:
        return
    VaultScanCache.build_or_load = _ORIG_BUILD  # type: ignore[method-assign]
    _INSTALLED = False
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

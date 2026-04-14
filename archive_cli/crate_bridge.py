"""Python implementations invoked from ``archive_crate`` (Phase 2.9 delegation).

Steps 10, 13–14, 18: ``rebuild_index``, ``build_person_index``, ``resolve_person_batch`` bridge
here so Rust can call a single stable module path without circular imports at import time.
"""

from __future__ import annotations

from typing import Any


def rebuild_index(
    *,
    workers: int | None = None,
    batch_size: int | None = None,
    commit_interval: int | None = None,
    progress_every: int | None = None,
    executor_kind: str | None = None,
    force_full: bool | None = None,
    disable_manifest_cache: bool | None = None,
    no_cache: bool | None = None,
) -> Any:
    """Full index rebuild via env-configured :class:`~archive_cli.loader.IndexLoader`."""

    from archive_cli.commands._resolve import resolve_store

    store = resolve_store()
    return store.loader.rebuild_with_metrics(
        workers=workers,
        batch_size=batch_size,
        commit_interval=commit_interval,
        progress_every=progress_every,
        executor_kind=executor_kind,
        force_full=force_full,
        disable_manifest_cache=disable_manifest_cache,
        no_cache=no_cache,
    )


def build_person_index(vault_path: str, cache_path: str | None = None) -> Any:
    """Return :class:`archive_crate.PersonResolutionIndex` (Rust Step 13).

    For Python-only :class:`hfa.identity_resolver.PersonIndex`, construct it directly.
    """

    import archive_crate

    return archive_crate.build_person_index(vault_path, cache_path)


def resolve_person_batch(vault_path: str, identifiers_list: list[dict[str, Any]]) -> Any:
    """Batch person resolution — Rust (Step 14) via :func:`archive_crate.resolve_person_batch`."""

    import archive_crate

    return archive_crate.resolve_person_batch(vault_path, identifiers_list)

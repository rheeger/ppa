"""Python implementation invoked from ``archive_crate`` for Phase 2.9 Step 18."""

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
    uid_allowlist: set[str] | frozenset[str] | list[str] | None = None,
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
        uid_allowlist=uid_allowlist,
    )

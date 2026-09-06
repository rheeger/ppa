"""Named warehouse bulk adapter. Private connections stay inside this module."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from archive_engine.errors import CapabilityUnavailableError


class _ClosedSnapshot:
    def __enter__(self) -> _ClosedSnapshot:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class WarehouseSnapshot:
    """Context-managed snapshot. The underlying connection is not public."""

    def __init__(self, opener: Callable[[], Any] | None):
        self._opener = opener
        self._conn: Any = None

    def __enter__(self) -> WarehouseSnapshot:
        if self._opener is None:
            return self
        self._conn = self._opener()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return None
        close = getattr(conn, "close", None)
        if callable(close):
            close()
        return None


class WarehouseAdapter:
    """Bulk warehouse operations. ``_connect`` is not part of the public API."""

    def __init__(
        self,
        index: Any,
        *,
        after_rebuild: Callable[[dict[str, Any], dict[str, Any]], None] | None = None,
    ) -> None:
        self._index = index
        self._after_rebuild = after_rebuild

    def bootstrap(self) -> dict[str, Any]:
        fn = getattr(self._index, "bootstrap", None)
        if not callable(fn):
            raise CapabilityUnavailableError("warehouse_bootstrap_unavailable")
        result = fn()
        return dict(result) if isinstance(result, dict) else {"result": result}

    def rebuild(self, **kwargs: Any) -> dict[str, Any]:
        allowed = {
            "workers",
            "batch_size",
            "commit_interval",
            "progress_every",
            "executor_kind",
            "force_full",
            "disable_manifest_cache",
            "no_cache",
            "uid_allowlist",
        }
        filtered = {key: value for key, value in kwargs.items() if key in allowed and value is not None}
        metrics_fn = getattr(self._index, "rebuild_with_metrics", None)
        if callable(metrics_fn):
            counts = metrics_fn(**filtered).counts
        else:
            rebuild_fn = getattr(self._index, "rebuild", None)
            if not callable(rebuild_fn):
                raise CapabilityUnavailableError("warehouse_rebuild_unavailable")
            counts = rebuild_fn()
        if self._after_rebuild is not None:
            self._after_rebuild(filtered, counts if isinstance(counts, dict) else {})
        return counts

    def status(self) -> dict[str, Any]:
        fn = getattr(self._index, "status", None)
        if not callable(fn):
            return {}
        result = fn()
        return dict(result) if isinstance(result, dict) else {"result": result}

    def snapshot(self) -> WarehouseSnapshot | _ClosedSnapshot:
        opener = getattr(self._index, "_connect", None)
        if not callable(opener):
            return _ClosedSnapshot()
        return WarehouseSnapshot(opener)

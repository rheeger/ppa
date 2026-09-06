"""Retrieval adapter. Serving vs index is chosen at construction, not per call."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from archive_engine.contracts import AccessContext


class RetrievalAdapter:
    """Instance-scoped retrieval surface. Does not inspect concrete index types."""

    def __init__(
        self,
        *,
        index: Any,
        access: AccessContext,
        serving_factory: Callable[[], Any] | None = None,
        policy_fields: Callable[[], dict[str, Any]] | None = None,
        authorize_rows: Callable[..., list[dict[str, Any]]] | None = None,
        on_close: Callable[[], None] | None = None,
    ) -> None:
        self._index = index
        self.access = access
        self._serving_factory = serving_factory
        self._policy_fields = policy_fields or (lambda: {})
        self._authorize_rows = authorize_rows
        self._on_close = on_close
        self._closed = False

    def serving_or_none(self) -> Any | None:
        if self._serving_factory is None:
            return None
        try:
            return self._serving_factory()
        except Exception as exc:
            if type(exc).__name__ == "ServingIndexUnavailableError":
                return None
            raise

    def _policy(self) -> dict[str, Any]:
        return dict(self._policy_fields())

    def _rows(self, rows: Any, *, limit: int | None = None) -> list[dict[str, Any]]:
        listed = list(rows or [])
        if self._authorize_rows is None:
            return listed if limit is None else listed[:limit]
        return self._authorize_rows(listed, limit=limit)

    def search(self, query: str, *, limit: int = 20, **kwargs: Any) -> list[dict[str, Any]]:
        serving = self.serving_or_none()
        if serving is not None:
            return list(serving.search(query, limit=limit, **kwargs, **self._policy()) or [])
        fetch_limit = kwargs.pop("fetch_limit", limit)
        return self._rows(self._index.search(query, limit=fetch_limit, **kwargs), limit=limit)

    def typed_query(self, **kwargs: Any) -> dict[str, Any]:
        serving = self.serving_or_none()
        if serving is not None and hasattr(serving, "typed_query"):
            payload = serving.typed_query(**kwargs, **self._policy())
            return dict(payload or {})
        rows = self.query_cards(
            type_filter=str(kwargs.get("filters", {}).get("type_filter", "") if isinstance(kwargs.get("filters"), dict) else kwargs.get("type_filter", "")),
            source_filter=str(kwargs.get("filters", {}).get("source_filter", "") if isinstance(kwargs.get("filters"), dict) else kwargs.get("source_filter", "")),
            people_filter=str(kwargs.get("filters", {}).get("people_filter", "") if isinstance(kwargs.get("filters"), dict) else kwargs.get("people_filter", "")),
            org_filter=str(kwargs.get("filters", {}).get("org_filter", "") if isinstance(kwargs.get("filters"), dict) else kwargs.get("org_filter", "")),
            start_date=str(kwargs.get("filters", {}).get("start_date", "") if isinstance(kwargs.get("filters"), dict) else kwargs.get("start_date", "")),
            end_date=str(kwargs.get("filters", {}).get("end_date", "") if isinstance(kwargs.get("filters"), dict) else kwargs.get("end_date", "")),
            limit=int(kwargs.get("page_size") or kwargs.get("limit") or 20),
            authorize_limit=int(kwargs.get("page_size") or kwargs.get("limit") or 20),
        )
        return {"rows": rows, "total_status": "unknown", "truncated": True}

    def query_cards(self, **kwargs: Any) -> list[dict[str, Any]]:
        authorize_limit = int(kwargs.pop("authorize_limit", kwargs.get("limit", 20)) or 20)
        serving = self.serving_or_none()
        if serving is not None:
            return self._rows(serving.query(**kwargs, **self._policy()) or [], limit=authorize_limit)
        query_fn = getattr(self._index, "query_cards", None)
        if not callable(query_fn):
            return []
        try:
            rows = query_fn(**kwargs)
        except TypeError:
            slim = {
                key: kwargs[key]
                for key in ("type_filter", "source_filter", "people_filter", "org_filter", "limit")
                if key in kwargs
            }
            rows = query_fn(**slim)
        return self._rows(rows, limit=authorize_limit)

    def graph(self, rel_path: str, *, hops: int = 2, **kwargs: Any) -> Any:
        serving = self.serving_or_none()
        if serving is not None:
            return serving.graph(rel_path, hops=hops, **kwargs, **self._policy())
        return self._index.graph(rel_path, hops=hops)

    def graph_bounded(self, rel_path: str, *, hops: int = 1, **kwargs: Any) -> dict[str, Any]:
        serving = self.serving_or_none()
        if serving is not None and hasattr(serving, "graph_bounded"):
            return dict(serving.graph_bounded(rel_path, hops=hops, **kwargs, **self._policy()) or {})
        graph = self.graph(rel_path, hops=hops, **kwargs)
        return {"graph": graph, "truncated": False, "truncation_reason": "", "nodes_visited": 0, "edges_emitted": 0}

    def timeline(self, **kwargs: Any) -> list[dict[str, Any]]:
        limit = int(kwargs.get("limit", 20) or 20)
        serving = self.serving_or_none()
        if serving is not None:
            return list(serving.timeline(**kwargs, **self._policy()) or [])
        return self._rows(self._index.timeline(**kwargs), limit=limit)

    def temporal_neighbors(self, timestamp: str, **kwargs: Any) -> Any:
        serving = self.serving_or_none()
        if serving is not None:
            return serving.temporal_neighbors(timestamp, **kwargs, **self._policy())
        return self._index.temporal_neighbors(timestamp, **kwargs)

    def pointers(self, uids: list[str]) -> dict[str, dict[str, Any]]:
        serving = self.serving_or_none()
        if serving is not None:
            return dict(serving.pointers(uids, **self._policy()) or {})
        fn = getattr(self._index, "card_stack_pointers", None)
        if callable(fn):
            return dict(fn(uids) or {})
        return {}

    def person_hit(self, name: str) -> dict[str, Any] | None:
        serving = self.serving_or_none()
        if serving is not None:
            hit = serving.person(name, **self._policy())
            return dict(hit) if hit else None
        fn = getattr(self._index, "person_path", None)
        if not callable(fn):
            return None
        rel = fn(name)
        if not rel:
            return None
        return {"rel_path": str(rel)}

    def vector(self, query_vector: list[float], **kwargs: Any) -> list[dict[str, Any]]:
        serving = self.serving_or_none()
        if serving is not None:
            return list(serving.vector(query_vector, **kwargs, **self._policy()) or [])
        limit = int(kwargs.get("limit", 20) or 20)
        return self._rows(self._index.vector_search(query_vector=query_vector, **kwargs), limit=limit)

    def hybrid(self, query: str, query_vector: list[float], **kwargs: Any) -> list[dict[str, Any]] | None:
        serving = self.serving_or_none()
        if serving is None:
            return None
        return list(serving.hybrid(query, query_vector, **kwargs, **self._policy()) or [])

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._on_close is not None:
            self._on_close()

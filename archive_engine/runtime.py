"""Instance-scoped engine runtime.

CLI and MCP share one composed object per archive: identity, access, exact
read, retrieval, warehouse, and providers. Two runtimes in one process do not
share handles or card bytes.
"""

from __future__ import annotations

from typing import Any

from archive_engine.adapters.providers import EmbeddingProviderAdapter
from archive_engine.adapters.retrieval import RetrievalAdapter
from archive_engine.adapters.warehouse import WarehouseAdapter
from archive_engine.contracts import AccessContext, ArchiveIdentity
from archive_engine.errors import IncompatibleStateError
from archive_engine.service import ArchiveEngineService


class ArchiveRuntime:
    """Composition root consumed by the store facade."""

    def __init__(
        self,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
        exact_read: ArchiveEngineService,
        retrieval: RetrievalAdapter,
        warehouse: WarehouseAdapter,
        providers: EmbeddingProviderAdapter,
    ) -> None:
        if access.archive_id != identity.archive_id:
            raise IncompatibleStateError("AccessContext.archive_id must match ArchiveIdentity.archive_id")
        self.identity = identity
        self.access = access
        self.exact_read = exact_read
        self.retrieval = retrieval
        self.warehouse = warehouse
        self.providers = providers
        self._closed = False

    def read(self, path_or_uid: str, *, access: AccessContext | None = None) -> dict[str, Any]:
        return self.exact_read.read(path_or_uid, access=access or self.access)

    def search(self, query: str, *, limit: int = 20, **kwargs: Any) -> dict[str, Any]:
        return {"rows": self.retrieval.search(query, limit=limit, **kwargs)}

    def query(self, **kwargs: Any) -> dict[str, Any]:
        return {"rows": self.retrieval.query_cards(**kwargs)}

    def typed_query(self, **kwargs: Any) -> dict[str, Any]:
        return self.retrieval.typed_query(**kwargs)

    def graph(self, rel_path: str, *, hops: int = 2, **kwargs: Any) -> Any:
        return self.retrieval.graph(rel_path, hops=hops, **kwargs)

    def rebuild(self, **kwargs: Any) -> dict[str, Any]:
        return self.warehouse.rebuild(**kwargs)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self.retrieval.close()

    def __enter__(self) -> ArchiveRuntime:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

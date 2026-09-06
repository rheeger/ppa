"""Narrow engine ports.

Adapters live in CLI/factory code. Core services accept these protocols and
never import CLI commands or MCP transport. Batch methods return receipts;
absence of an optional port means pending/unavailable, not success.
"""

from __future__ import annotations

from typing import Protocol

from archive_engine.contracts import AccessContext, AffectedContext, ExactReadResult


class UidPathLookup(Protocol):
    """Resolve a card UID to a vault-relative path. ``None`` means unknown."""

    def resolve_rel_path(self, uid: str) -> str | None: ...


class CanonicalReader(Protocol):
    """Contained canonical-card reader.

    Implementations must use ``archive_vault.paths.resolve_contained_path``.
    Escapes and missing files return ``(None, rel_or_empty)``.
    """

    def read_contained(self, user_path: str) -> tuple[str | None, str]: ...


class AffectedContextResolver(Protocol):
    """Optional burst/reconciliation resolver (algorithm owned by P01-B1)."""

    def resolve_affected(self, uid: str, revision: str) -> AffectedContext | None: ...


class ExactReadPort(Protocol):
    """Application-service exact read. Access context is always explicit."""

    def read_exact(self, path_or_uid: str, *, access: AccessContext | None = None) -> ExactReadResult: ...


class WarehouseSnapshot(Protocol):
    """Bounded warehouse cursor. A query must not retain a dangling connection."""

    def __enter__(self) -> WarehouseSnapshot: ...

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None: ...

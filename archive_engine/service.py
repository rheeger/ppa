"""Exact-read application service.

Callers inject lookup, a contained canonical reader, and AccessContext.
This module does not import CLI commands, MCP transport, or invent a second
path resolver.
"""

from __future__ import annotations

from archive_engine.contracts import AccessContext, AffectedContext, ArchiveIdentity, ExactReadResult
from archive_engine.errors import CapabilityUnavailableError, IncompatibleStateError
from archive_engine.ports import AffectedContextResolver, CanonicalReader, UidPathLookup


class ArchiveEngineService:
    """Instance-scoped engine. Two services in one process do not share state."""

    def __init__(
        self,
        *,
        identity: ArchiveIdentity,
        access: AccessContext,
        lookup: UidPathLookup,
        reader: CanonicalReader,
        affected_context: AffectedContextResolver | None = None,
    ) -> None:
        if not identity.archive_id:
            raise IncompatibleStateError("ArchiveIdentity.archive_id is required")
        if access.archive_id != identity.archive_id:
            raise IncompatibleStateError("AccessContext.archive_id must match ArchiveIdentity.archive_id")
        self._identity = identity
        self._access = access
        self._lookup = lookup
        self._reader = reader
        self._affected_context = affected_context

    @property
    def identity(self) -> ArchiveIdentity:
        return self._identity

    @property
    def access(self) -> AccessContext:
        return self._access

    def _bound_access(self, access: AccessContext | None) -> AccessContext:
        ctx = self._access if access is None else access
        if ctx.archive_id != self._identity.archive_id:
            raise IncompatibleStateError("AccessContext.archive_id must match ArchiveIdentity.archive_id")
        return ctx

    def resolve_affected(self, uid: str, revision: str) -> AffectedContext:
        if self._affected_context is None:
            return AffectedContext(uid=uid, revision=revision, status="reconciliation_pending")
        resolved = self._affected_context.resolve_affected(uid, revision)
        if resolved is None:
            return AffectedContext(uid=uid, revision=revision, status="reconciliation_pending")
        return resolved

    def require_affected_resolver(self) -> AffectedContextResolver:
        if self._affected_context is None:
            raise CapabilityUnavailableError("affected_context_resolver_unavailable")
        return self._affected_context

    def read_exact(self, path_or_uid: str, *, access: AccessContext | None = None) -> ExactReadResult:
        ctx = self._bound_access(access)
        if ctx.deny:
            include_rel = path_or_uid.endswith(".md")
            return ExactReadResult(
                path_or_uid=path_or_uid,
                content="",
                found=False,
                rel_path="",
                include_rel_path=include_rel,
            )
        if path_or_uid.endswith(".md"):
            content, rel = self._reader.read_contained(path_or_uid)
            return ExactReadResult(
                path_or_uid=path_or_uid,
                content=content or "",
                found=content is not None,
                rel_path=rel,
                include_rel_path=True,
            )
        rel_path = self._lookup.resolve_rel_path(path_or_uid)
        if rel_path is None:
            return ExactReadResult(path_or_uid=path_or_uid, content="", found=False)
        content, rel = self._reader.read_contained(str(rel_path))
        if content is None:
            return ExactReadResult(path_or_uid=path_or_uid, content="", found=False)
        return ExactReadResult(
            path_or_uid=path_or_uid,
            content=content,
            found=True,
            rel_path=rel or str(rel_path),
            include_rel_path=True,
        )

    def read(self, path_or_uid: str, *, access: AccessContext | None = None) -> dict[str, object]:
        """Store-compatible payload for CLI/MCP exact read."""

        return self.read_exact(path_or_uid, access=access).to_store_payload()

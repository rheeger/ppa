"""CLI composition root for the instance-scoped engine.

Legacy construction stays here so ``archive_engine`` does not import CLI
commands or MCP transport. Adapters reuse ``resolve_contained_path`` — they
do not invent a second path resolver.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any

from archive_engine.adapters.providers import EmbeddingProviderAdapter
from archive_engine.adapters.retrieval import RetrievalAdapter
from archive_engine.adapters.warehouse import WarehouseAdapter
from archive_engine.contracts import AccessContext, ArchiveIdentity, EmbeddingSpec
from archive_engine.runtime import ArchiveRuntime
from archive_engine.service import ArchiveEngineService
from archive_vault.paths import PathEscapeError, resolve_contained_path

from .errors import ServingIndexUnavailableError
from .index_config import (
    CHUNK_SCHEMA_VERSION,
    INDEX_SCHEMA_VERSION,
    get_default_embedding_model,
    get_default_embedding_version,
    get_vector_dimension,
)

TRUSTED_LOCAL_PRINCIPAL = "local-operator"
TRUSTED_LOCAL_PROFILE = "trusted-local"
DEFAULT_EGRESS_POLICY_REVISION = "p05a-unspecified"


def schema_binding_for(index_schema: str) -> str:
    schema = (index_schema or "ppa").strip() or "ppa"
    return f"warehouse:{schema}+index_schema_v{INDEX_SCHEMA_VERSION}"


def resolve_archive_identity(
    vault: Path,
    *,
    schema_binding: str,
    archive_id: str | None = None,
) -> ArchiveIdentity:
    from archive_engine.config import archive_identity_for

    explicit = (archive_id if archive_id is not None else os.environ.get("PPA_ARCHIVE_ID", "")).strip()
    return archive_identity_for(vault, schema_binding=schema_binding, archive_id=explicit)


def trusted_local_access(archive_id: str, *, profile: str | None = None) -> AccessContext:
    """Configured trusted-local context. Deny is explicit and false here."""

    return AccessContext(
        archive_id=archive_id,
        principal=TRUSTED_LOCAL_PRINCIPAL,
        profile=(profile or TRUSTED_LOCAL_PROFILE).strip() or TRUSTED_LOCAL_PROFILE,
        allowed_tools=(),
        allowed_sources=(),
        allowed_domains=(),
        egress_policy_revision=DEFAULT_EGRESS_POLICY_REVISION,
        deny=False,
        deny_reason="",
    )


def embedding_spec_from_env(*, provider_namespace: str | None = None) -> EmbeddingSpec:
    provider = (provider_namespace or os.environ.get("PPA_EMBEDDING_PROVIDER", "") or "hash").strip() or "hash"
    return EmbeddingSpec(
        provider_namespace=provider,
        model=get_default_embedding_model(),
        model_revision=str(get_default_embedding_version()),
        dimension=get_vector_dimension(),
        metric="cosine",
        normalization="none",
        chunk_schema=f"chunk_schema_v{CHUNK_SCHEMA_VERSION}",
    )


class ContainedCanonicalReader:
    """Canonical reader that delegates containment to ``resolve_contained_path``."""

    def __init__(self, vault: Path):
        self.vault = Path(vault)

    def read_contained(self, user_path: str) -> tuple[str | None, str]:
        try:
            path = resolve_contained_path(self.vault, user_path, purpose="read")
            rel = str(path.relative_to(Path(self.vault).resolve()))
        except (PathEscapeError, ValueError):
            return None, ""
        if not path.is_file():
            return None, rel
        return path.read_text(encoding="utf-8"), rel


class IndexUidPathLookup:
    """UID → rel_path using serving (when present) then the warehouse/index."""

    def __init__(
        self,
        index: Any,
        *,
        serving_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._index = index
        self._serving_factory = serving_factory

    def resolve_rel_path(self, uid: str) -> str | None:
        if self._serving_factory is not None:
            try:
                rel_path = self._serving_factory().read_path(uid)
            except ServingIndexUnavailableError:
                rel_path = None
            if rel_path:
                return str(rel_path)
        reader = getattr(self._index, "read_path_for_uid", None)
        if callable(reader):
            rel_path = reader(uid)
            if rel_path is None:
                return None
            return str(rel_path)
        return None


class EngineBurstBridge:
    """Adapts the P01 burst algorithm to the P08 connector key port.

    Algorithm stays in ``conversation_bursts``. This object only forwards.
    """

    def __init__(self) -> None:
        from archive_cli.conversation_bursts import BurstAffectedResolver, burst_key_for

        self._resolver = BurstAffectedResolver()
        self._burst_key_for = burst_key_for

    def resolve_affected(self, uid: str, revision: str):
        return self._resolver.resolve_affected(uid, revision)

    def resolve_burst_affected(self, thread_uid: str, **kwargs: Any):
        return self._resolver.resolve_burst_affected(thread_uid, **kwargs)

    def burst_keys_for(
        self,
        *,
        thread_uid: str,
        changed_message_ids: tuple[str, ...] | list[str],
        content_hashes: tuple[str, ...] | list[str] = (),
    ) -> tuple[str, ...]:
        ids = tuple(str(item) for item in changed_message_ids if str(item))
        hashes = tuple(str(item) for item in content_hashes if str(item)) or ids
        return (self._burst_key_for(ids or (thread_uid,), hashes or (thread_uid,)),)


def build_exact_read_service(
    *,
    vault: Path,
    index: Any,
    serving_factory: Callable[[], Any] | None = None,
    schema_binding: str | None = None,
    identity: ArchiveIdentity | None = None,
    access: AccessContext | None = None,
    lookup: IndexUidPathLookup | None = None,
    reader: ContainedCanonicalReader | None = None,
    affected_context: Any | None = None,
) -> ArchiveEngineService:
    binding = schema_binding or schema_binding_for("ppa")
    resolved_identity = identity or resolve_archive_identity(vault, schema_binding=binding)
    resolved_access = access or trusted_local_access(resolved_identity.archive_id)
    return ArchiveEngineService(
        identity=resolved_identity,
        access=resolved_access,
        lookup=lookup or IndexUidPathLookup(index, serving_factory=serving_factory),
        reader=reader or ContainedCanonicalReader(vault),
        affected_context=affected_context,
    )


def _after_rebuild(vault: Path) -> Callable[[dict[str, Any], dict[str, Any]], None]:
    def _hook(filtered: dict[str, Any], _counts: dict[str, Any]) -> None:
        try:
            from archive_engine.changes import acknowledge_materialized

            from .serving_index import close_serving_handles, mark_serving_index_dirty

            allowlist = filtered.get("uid_allowlist") or []
            dirty = [str(uid).strip() for uid in allowlist if str(uid).strip()]
            acknowledge_materialized(vault, uids=dirty or None)
            mark_serving_index_dirty(vault, "rebuild", dirty)
            close_serving_handles(vault=vault)
        except Exception:
            pass

    return _hook


def build_runtime(
    *,
    vault: Path,
    index: Any,
    access: AccessContext,
    identity: ArchiveIdentity | None = None,
    serving_factory: Callable[[], Any] | None = None,
    schema_binding: str | None = None,
    provider_factory: Callable[..., Any] | None = None,
    policy_fields: Callable[[], dict[str, Any]] | None = None,
    authorize_rows: Callable[..., list[dict[str, Any]]] | None = None,
    attach_bursts: bool = True,
) -> ArchiveRuntime:
    binding = schema_binding or schema_binding_for("ppa")
    resolved_identity = identity or resolve_archive_identity(vault, schema_binding=binding)
    burst_bridge = EngineBurstBridge() if attach_bursts else None
    exact_read = build_exact_read_service(
        vault=vault,
        index=index,
        serving_factory=serving_factory,
        schema_binding=binding,
        identity=resolved_identity,
        access=access,
        affected_context=burst_bridge,
    )
    from .serving_index import close_serving_handles

    retrieval = RetrievalAdapter(
        index=index,
        access=access,
        serving_factory=serving_factory,
        policy_fields=policy_fields,
        authorize_rows=authorize_rows,
        on_close=lambda: close_serving_handles(vault=vault),
    )
    warehouse = WarehouseAdapter(index, after_rebuild=_after_rebuild(vault) if serving_factory is not None else None)
    from .embedding_provider import get_embedding_provider

    providers = EmbeddingProviderAdapter(provider_factory or get_embedding_provider, access=access)

    def _drain() -> tuple[Any, ...]:
        if burst_bridge is None:
            return ()
        from archive_sync.connectors.replay import attach_resolver

        return tuple(attach_resolver(vault, burst_bridge))

    return ArchiveRuntime(
        identity=resolved_identity,
        access=access,
        exact_read=exact_read,
        retrieval=retrieval,
        warehouse=warehouse,
        providers=providers,
        drain_pending=_drain if attach_bursts else None,
    )

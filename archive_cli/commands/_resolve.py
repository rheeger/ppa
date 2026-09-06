"""Dependency resolution for PPA commands.

Replaces the repeated pattern in server.py where every tool function independently
calls get_vault() -> check is_dir() -> get_store() -> check error. This module
centralizes that into typed functions that raise on failure.

These functions are intentionally not cached — each command call gets a fresh store
to avoid stale state in long-running MCP serve sessions.

``get_vault`` / ``get_index`` / ``get_store`` live here so tests can monkeypatch
``archive_cli.commands._resolve`` without importing the MCP server module.
"""

from __future__ import annotations

import os
from pathlib import Path

from archive_engine.runtime import ArchiveRuntime

from ..embedding_provider import get_embedding_provider
from ..errors import IndexUnavailableError, VaultNotFoundError
from ..index_store import BaseArchiveIndex, get_archive_index
from ..store import DefaultArchiveStore, get_archive_store


def get_vault() -> Path:
    from archive_engine.config import current_instance_config

    bound = current_instance_config()
    if bound is not None:
        return Path(bound.storage.vault_path)
    return Path(os.environ.get("PPA_PATH", Path.home() / "Archive" / "vault"))


def get_index(vault: Path | None = None) -> BaseArchiveIndex:
    return get_archive_index(vault or get_vault())


def get_store(vault: Path | None = None) -> DefaultArchiveStore:
    from archive_cli.index_config import CHUNK_SCHEMA_VERSION, DEFAULT_VECTOR_DIMENSION, INDEX_SCHEMA_VERSION
    from archive_engine.config import bind_instance_config, current_secret_values, resolve_instance_config

    resolved_vault = vault or get_vault()
    config = resolve_instance_config(
        instance_dir=resolved_vault,
        allow_cwd_discovery=False,
        schema_version_hint=INDEX_SCHEMA_VERSION,
        chunk_schema_hint=CHUNK_SCHEMA_VERSION,
        vector_dimension_hint=DEFAULT_VECTOR_DIMENSION,
    )
    with bind_instance_config(config, secrets=current_secret_values()):
        return get_archive_store(
            vault=resolved_vault,
            index=get_index(resolved_vault),
            provider_factory=get_embedding_provider,
        )


def resolve_vault() -> Path:
    """Return vault path, raise VaultNotFoundError if missing."""
    v = get_vault()
    if not v.is_dir():
        raise VaultNotFoundError("Vault not found")
    return v


def resolve_store(vault: Path | None = None) -> DefaultArchiveStore:
    """Build store from env/config, raise VaultNotFoundError or IndexUnavailableError."""
    v = resolve_vault() if vault is None else vault
    if not v.is_dir():
        raise VaultNotFoundError("Vault not found")
    try:
        return get_store(v)
    except RuntimeError as exc:
        raise IndexUnavailableError(str(exc)) from exc


def resolve_runtime(vault: Path | None = None) -> ArchiveRuntime:
    """Instance-scoped engine for the current vault/store."""

    return resolve_store(vault).runtime


def resolve_index(vault: Path | None = None) -> BaseArchiveIndex:
    """Build index from env/config, raise VaultNotFoundError or IndexUnavailableError."""
    v = resolve_vault() if vault is None else vault
    if not v.is_dir():
        raise VaultNotFoundError("Vault not found")
    try:
        return get_index(v)
    except RuntimeError as exc:
        raise IndexUnavailableError(str(exc)) from exc

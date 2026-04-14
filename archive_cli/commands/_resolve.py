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

from ..embedding_provider import get_embedding_provider
from ..errors import IndexUnavailableError, VaultNotFoundError
from ..index_store import BaseArchiveIndex, get_archive_index
from ..store import DefaultArchiveStore, get_archive_store


def get_vault() -> Path:
    return Path(os.environ.get("PPA_PATH", Path.home() / "Archive" / "vault"))


def get_index(vault: Path | None = None) -> BaseArchiveIndex:
    return get_archive_index(vault or get_vault())


def get_store(vault: Path | None = None) -> DefaultArchiveStore:
    resolved_vault = vault or get_vault()
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


def resolve_index(vault: Path | None = None) -> BaseArchiveIndex:
    """Build index from env/config, raise VaultNotFoundError or IndexUnavailableError."""
    v = resolve_vault() if vault is None else vault
    if not v.is_dir():
        raise VaultNotFoundError("Vault not found")
    try:
        return get_index(v)
    except RuntimeError as exc:
        raise IndexUnavailableError(str(exc)) from exc

"""Dependency resolution for PPA commands.

Replaces the repeated pattern in server.py where every tool function independently
calls get_vault() -> check is_dir() -> _load_store() -> check error. This module
centralizes that into typed functions that raise on failure.

These functions are intentionally not cached — each command call gets a fresh store
to avoid stale state in long-running MCP serve sessions.

Implementation delegates to archive_mcp.server.get_vault / get_store / get_index so
tests that monkeypatch those symbols continue to work (lazy import avoids circular
import at module load time).
"""

from __future__ import annotations

from pathlib import Path

from ..errors import IndexUnavailableError, VaultNotFoundError
from ..index_store import BaseArchiveIndex
from ..store import DefaultArchiveStore


def resolve_vault() -> Path:
    """Return vault path, raise VaultNotFoundError if missing."""
    from archive_mcp import server as server_mod

    v = server_mod.get_vault()
    if not v.is_dir():
        raise VaultNotFoundError("Vault not found")
    return v


def resolve_store(vault: Path | None = None) -> DefaultArchiveStore:
    """Build store from env/config, raise VaultNotFoundError or IndexUnavailableError."""
    from archive_mcp import server as server_mod

    v = resolve_vault() if vault is None else vault
    if not v.is_dir():
        raise VaultNotFoundError("Vault not found")
    try:
        return server_mod.get_store(v)
    except RuntimeError as exc:
        raise IndexUnavailableError(str(exc)) from exc


def resolve_index(vault: Path | None = None) -> BaseArchiveIndex:
    """Build index from env/config, raise VaultNotFoundError or IndexUnavailableError."""
    from archive_mcp import server as server_mod

    v = resolve_vault() if vault is None else vault
    if not v.is_dir():
        raise VaultNotFoundError("Vault not found")
    try:
        return server_mod.get_index(v)
    except RuntimeError as exc:
        raise IndexUnavailableError(str(exc)) from exc

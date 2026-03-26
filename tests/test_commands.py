"""Unit tests for the PPA commands layer.

These tests exercise commands directly (bypassing MCP server.py) to verify:
- Commands return well-formed dicts
- Commands raise typed PpaError exceptions on failure
- Commands log appropriately (can be verified with caplog)

The existing test_server.py tests MCP tool behavior end-to-end through FakeIndex.
This file tests the commands layer in isolation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from test_server import FakeIndex, _seed_vault

import archive_mcp.server as server_mod
from archive_mcp.commands import read as read_cmd
from archive_mcp.commands import search as search_cmd
from archive_mcp.commands import status as status_cmd
from archive_mcp.commands._resolve import resolve_store
from archive_mcp.errors import (
    IndexUnavailableError,
    InvalidInputError,
    VaultNotFoundError,
)
from archive_mcp.health import run_health_checks
from archive_mcp.store import DefaultArchiveStore


@pytest.fixture
def tmp_vault(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", "postgresql://archive:archive@localhost:5432/archive")
    return vault


@pytest.fixture
def fake_index(monkeypatch: pytest.MonkeyPatch) -> FakeIndex:
    fake = FakeIndex()
    monkeypatch.setattr(server_mod, "get_index", lambda vault=None: fake)
    return fake


@pytest.fixture
def command_store(tmp_vault: Path, fake_index: FakeIndex) -> DefaultArchiveStore:
    return DefaultArchiveStore(vault=tmp_vault, index=fake_index)


def test_search_command_returns_dict(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.commands")
    result = search_cmd.search("Endaoment", limit=5, store=command_store, logger=log)
    assert isinstance(result, dict)
    assert "rows" in result
    assert len(result["rows"]) >= 1


def test_read_command_returns_dict(tmp_vault: Path, command_store: DefaultArchiveStore) -> None:
    _seed_vault(tmp_vault)
    log = logging.getLogger("test.commands")
    result = read_cmd.read("hfa-person-aaaabbbbcccc", store=command_store, logger=log)
    assert isinstance(result, dict)
    assert result.get("found") is True


def test_stats_command_returns_dict(fake_index: FakeIndex) -> None:
    log = logging.getLogger("test.commands")
    result = status_cmd.stats(index=fake_index, logger=log)
    assert isinstance(result, dict)
    assert result.get("total") == 2


def test_parse_paths_json_invalid_raises() -> None:
    with pytest.raises(InvalidInputError, match="Invalid JSON"):
        read_cmd.parse_paths_json("{not json")


def test_parse_paths_json_wrong_type_raises() -> None:
    with pytest.raises(InvalidInputError, match="must be a JSON array"):
        read_cmd.parse_paths_json('{"a": 1}')


def test_resolve_store_raises_vault_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(server_mod, "get_vault", lambda: Path("/nonexistent/ppa/vault"))
    with pytest.raises(VaultNotFoundError):
        resolve_store()


def test_resolve_store_raises_index_unavailable(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    (vault / "People").mkdir()
    monkeypatch.setattr(server_mod, "get_vault", lambda: vault)

    def _boom(vault=None):
        raise RuntimeError("index down")

    monkeypatch.setattr(server_mod, "get_store", _boom)
    with pytest.raises(IndexUnavailableError):
        resolve_store()


def test_health_report_flags_missing_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("archive_mcp.health.get_index_dsn", lambda: "")
    report = run_health_checks()
    assert report["ok"] is False
    assert report["checks"]["postgres"]["ok"] is False


def test_index_status_command_returns_dict(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.commands")
    result = status_cmd.index_status(store=command_store, logger=log)
    assert isinstance(result, dict)
    assert "card_count" in result or "schema_version" in result


def test_embedding_status_command_returns_dict(
    command_store: DefaultArchiveStore,
) -> None:
    log = logging.getLogger("test.commands")
    result = status_cmd.embedding_status(
        store=command_store,
        logger=log,
        embedding_model="archive-hash-dev",
        embedding_version=1,
    )
    assert isinstance(result, dict)


def test_hybrid_search_command_returns_dict(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.commands")
    result = search_cmd.hybrid_search(
        "test",
        store=command_store,
        logger=log,
        limit=2,
        embedding_model="archive-hash-dev",
        embedding_version=1,
    )
    assert isinstance(result, dict)
    assert "rows" in result


def test_read_logs_not_found_without_error(
    command_store: DefaultArchiveStore, caplog: pytest.LogCaptureFixture
) -> None:
    log = logging.getLogger("test.commands.read")
    with caplog.at_level(logging.INFO):
        result = read_cmd.read("hfa-person-does-not-exist", store=command_store, logger=log)
    assert result.get("found") is False

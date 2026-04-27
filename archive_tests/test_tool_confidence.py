"""Integration tests: confidence flows through the commands layer."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from test_server import FakeIndex, _seed_vault

import archive_cli.commands._resolve as resolve_mod
from archive_cli.commands import graph as graph_cmd
from archive_cli.commands import query as query_cmd
from archive_cli.commands import read as read_cmd
from archive_cli.commands import search as search_cmd
from archive_cli.server import _server_instructions
from archive_cli.store import DefaultArchiveStore


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
    monkeypatch.setattr(resolve_mod, "get_index", lambda vault=None: fake)
    return fake


@pytest.fixture
def command_store(tmp_vault: Path, fake_index: FakeIndex) -> DefaultArchiveStore:
    return DefaultArchiveStore(vault=tmp_vault, index=fake_index)


def test_search_includes_confidence(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.tc")
    result = search_cmd.search("x", limit=5, store=command_store, logger=log)
    assert "confidence" in result
    assert result["confidence"] in {"high", "medium", "low"}


def test_query_includes_confidence(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.tc")
    result = query_cmd.query(
        type_filter="person",
        source_filter="",
        people_filter="",
        org_filter="",
        limit=5,
        store=command_store,
        logger=log,
    )
    assert "confidence" in result


def test_hybrid_search_includes_confidence(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.tc")
    result = search_cmd.hybrid_search("q", store=command_store, logger=log, limit=5)
    assert "confidence" in result


def test_temporal_neighbors_includes_confidence(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.tc")
    result = graph_cmd.temporal_neighbors("2026-03-06T12:00:00Z", store=command_store, logger=log)
    assert "confidence" in result
    assert result["confidence"] == "low"


def test_knowledge_domain_fallback_includes_confidence(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.tc")
    result = graph_cmd.knowledge_domain("finance", store=command_store, logger=log)
    assert result.get("fallback") is True
    assert "confidence" in result


def test_read_found_high_confidence(tmp_vault: Path, command_store: DefaultArchiveStore) -> None:
    _seed_vault(tmp_vault)
    log = logging.getLogger("test.tc")
    result = read_cmd.read("hfa-person-aaaabbbbcccc", store=command_store, logger=log)
    assert result.get("confidence") == "high"


def test_read_not_found_low_confidence(command_store: DefaultArchiveStore) -> None:
    log = logging.getLogger("test.tc")
    result = read_cmd.read("missing-uid-zzzz", store=command_store, logger=log)
    assert result.get("confidence") == "low"


def test_sparse_results_log_gap(command_store: DefaultArchiveStore, fake_index: FakeIndex) -> None:
    log = logging.getLogger("test.tc")
    search_cmd.search("sparse", limit=5, store=command_store, logger=log)
    assert getattr(fake_index, "gap_log_calls", None)
    assert any(c for batch in fake_index.gap_log_calls for c in batch if c.get("gap_type") == "sparse_results")


def test_server_instructions_updated() -> None:
    assert "archive_temporal_neighbors" in _server_instructions
    assert "confidence" in _server_instructions.lower()
    assert "archive_knowledge" in _server_instructions

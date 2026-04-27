"""Tests for ppa maintain orchestration."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest import mock

import pytest

from archive_cli.commands.maintain import MaintenanceReport, run_maintenance


def _connect_ctx(conn):
    class CM:
        def __enter__(self):
            return conn

        def __exit__(self, *a):
            return False

    return CM()


def test_maintenance_nothing_to_do() -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = None
        elif "ingestion_log" in s:
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert rep.nothing_to_do is True


def test_maintenance_full_cycle(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()
    rows = [
        {"card_uid": f"u{i}", "action": "created", "source_adapter": "adapter", "logged_at": "2026-01-02T00:00:00Z"}
        for i in range(5)
    ]

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s and "SELECT" in s:
            m.fetchone.return_value = {"value": "2026-01-01T00:00:00Z"}
        elif "ingestion_log" in s and "COUNT" not in s.upper():
            m.fetchall.return_value = rows
        elif "enrichment_queue" in s:
            m.fetchone.return_value = {"c": 3}
        elif "retrieval_gaps" in s and "COUNT" in s.upper():
            m.fetchone.return_value = {"c": 1}
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.vault = tmp_path
    store.rebuild.return_value = {"cards": 10}

    class M:
        extracted_cards = 2

    monkeypatch.setattr(
        "archive_sync.extractors.runner.ExtractionRunner.run",
        lambda self: M(),
    )

    def fake_er(path, **kwargs):
        return {
            "places_created": 1,
            "places_merged": 0,
            "orgs_created": 1,
            "orgs_merged": 0,
            "persons_linked": 1,
        }

    monkeypatch.setattr(
        "archive_sync.extractors.entity_resolution.run_entity_resolution",
        fake_er,
    )
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert rep.new_cards_ingested == 5
    assert rep.cards_extracted == 2
    assert rep.entities_resolved == 3
    assert rep.cards_rebuilt == 10


def test_maintenance_idempotent() -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = {"value": "2026-12-31T23:59:59Z"}
        elif "ingestion_log" in s:
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert rep.nothing_to_do is True


def test_maintenance_partial_failure_extraction(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = {"value": ""}
        elif "ingestion_log" in s and "COUNT" not in s.upper():
            m.fetchall.return_value = [
                {"card_uid": "a", "action": "created", "source_adapter": "x", "logged_at": "t"},
            ]
        elif "enrichment_queue" in s:
            m.fetchone.return_value = {"c": 0}
        elif "retrieval_gaps" in s:
            m.fetchone.return_value = {"c": 0}
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.vault = tmp_path
    store.rebuild.return_value = {"cards": 1}

    def boom_run(self):
        raise RuntimeError("extract fail")

    monkeypatch.setattr("archive_sync.extractors.runner.ExtractionRunner.run", boom_run)
    monkeypatch.setattr(
        "archive_sync.extractors.entity_resolution.run_entity_resolution",
        lambda *a, **k: {
            "places_created": 0,
            "places_merged": 0,
            "orgs_created": 0,
            "orgs_merged": 0,
            "persons_linked": 0,
        },
    )
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert any(e.get("step") == "auto_extract" for e in rep.errors)


def test_maintenance_missing_extractor_module(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = {"value": ""}
        elif "ingestion_log" in s and "COUNT" not in s.upper():
            m.fetchall.return_value = [
                {"card_uid": "a", "action": "created", "source_adapter": "x", "logged_at": "t"},
            ]
        elif "enrichment_queue" in s:
            m.fetchone.return_value = {"c": 0}
        elif "retrieval_gaps" in s:
            m.fetchone.return_value = {"c": 0}
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.vault = tmp_path
    store.rebuild.return_value = {"cards": 0}
    monkeypatch.setattr("archive_cli.commands.maintain._try_import", lambda p: None)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert any("extractor registry import failed" in s for s in rep.skipped_steps)


def test_maintenance_missing_ingestion_table() -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = None
        elif "ingestion_log" in s:
            raise Exception('relation "ppa.ingestion_log" does not exist')
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert "ingestion_log missing" in rep.skipped_steps


def test_maintenance_watermark_update(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()
    commits: list[int] = []

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s and "SELECT" in s:
            m.fetchone.return_value = {"value": "2026-01-01T00:00:00Z"}
        elif "ingestion_log" in s and "COUNT" not in s.upper():
            m.fetchall.return_value = [
                {"card_uid": "a", "action": "updated", "source_adapter": "x", "logged_at": "2026-01-02T00:00:00Z"},
            ]
        elif "enrichment_queue" in s:
            m.fetchone.return_value = {"c": 0}
        elif "retrieval_gaps" in s:
            m.fetchone.return_value = {"c": 0}
        return m

    conn.execute.side_effect = exec_side
    conn.commit.side_effect = lambda: commits.append(1)
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.vault = tmp_path
    store.rebuild.return_value = {"cards": 1}
    monkeypatch.setattr("archive_cli.commands.maintain._try_import", lambda p: None)
    run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert commits


def test_maintenance_coverage_report_fields() -> None:
    r = MaintenanceReport(
        new_cards_ingested=1,
        cards_extracted=2,
        entities_resolved=3,
        cards_rebuilt=4,
        enrichment_queue_depth=5,
        retrieval_gaps_since_last=6,
    )
    d = r.to_dict()
    for k in (
        "new_cards_ingested",
        "cards_extracted",
        "entities_resolved",
        "cards_rebuilt",
        "enrichment_queue_depth",
        "retrieval_gaps_since_last",
        "errors",
        "skipped_steps",
        "nothing_to_do",
    ):
        assert k in d


def test_maintenance_dry_run(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = {"value": ""}
        elif "ingestion_log" in s and "COUNT" not in s.upper():
            m.fetchall.return_value = [
                {"card_uid": "a", "action": "created", "source_adapter": "x", "logged_at": "t"},
            ]
        elif "enrichment_queue" in s:
            m.fetchone.return_value = {"c": 0}
        elif "retrieval_gaps" in s:
            m.fetchone.return_value = {"c": 0}
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.vault = tmp_path
    monkeypatch.setattr("archive_cli.commands.maintain._try_import", lambda p: None)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=True)
    assert any("dry-run" in s for s in rep.skipped_steps)
    store.rebuild.assert_not_called()


def test_maintenance_error_report_includes_step_details() -> None:
    r = MaintenanceReport()
    r.errors.append({"step": "x", "error": "boom"})
    assert r.errors[0]["step"] == "x"


def test_maintenance_provider_unavailable_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = {"value": ""}
        elif "ingestion_log" in s:
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)

    def fake_resolve(*, refresh: bool = False):
        p = mock.Mock()
        p.name = "openai"
        p.model = "gpt-4o-mini"
        p.is_available.return_value = False
        return p

    monkeypatch.setenv("PPA_ENRICHMENT_MODEL", "openai:gpt-4o-mini")
    monkeypatch.setattr("archive_cli.providers.resolve_provider", fake_resolve)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=True)
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    assert any("provider unavailable" in s for s in rep.skipped_steps)


def test_maintenance_provider_unset_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = {"value": ""}
        elif "ingestion_log" in s:
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    import archive_cli.providers as providers_mod

    providers_mod.resolve_provider(refresh=True)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=True)
    assert any("PPA_ENRICHMENT_MODEL unset" in s for s in rep.skipped_steps)


def test_maintenance_provider_invalid_name_error_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = {"value": ""}
        elif "ingestion_log" in s:
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    monkeypatch.setenv("PPA_ENRICHMENT_MODEL", "bogus:model")
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=True)
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    import archive_cli.providers as providers_mod

    providers_mod.resolve_provider(refresh=True)
    assert any(e.get("step") == "resolve_provider" for e in rep.errors)

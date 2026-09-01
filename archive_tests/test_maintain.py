"""Tests for ppa maintain orchestration."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from archive_cli.commands.maintain import MaintenanceReport, run_maintenance
from archive_sync.adapters.base import BaseAdapter, FetchedBatch, deterministic_provenance
from archive_sync.processors.constants import PROCESSOR_MATERIALIZATION


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

    runner_kwargs: dict[str, Any] = {}

    class FakeRunner:
        def __init__(self, *args, **kwargs):
            runner_kwargs.update(kwargs)

        def run(self):
            return M()

    monkeypatch.setattr(
        "archive_sync.extractors.runner.ExtractionRunner",
        FakeRunner,
    )

    er_kwargs: dict[str, Any] = {}

    def fake_er(path, **kwargs):
        er_kwargs.update(kwargs)
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
    assert runner_kwargs.get("uid_allowlist") == {f"u{i}" for i in range(5)}
    assert er_kwargs.get("uid_allowlist") == {f"u{i}" for i in range(5)}
    store.rebuild.assert_called_once()
    assert store.rebuild.call_args.kwargs.get("force_full") is False
    assert store.rebuild.call_args.kwargs.get("uid_allowlist") == {f"u{i}" for i in range(5)}


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


def _join_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in (
        "People",
        "Finance",
        "Calendar",
        "EmailThreads",
        "Documents",
        "Attachments",
        "_templates",
        ".obsidian",
        "_meta",
    ):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "nicknames.json").write_text("{}", encoding="utf-8")
    return vault


class _JoinProofGmailAdapter(BaseAdapter):
    """Fixture Gmail adapter that records ingest kwargs and emits one dirty UID."""

    source_id = "gmail-messages"
    enable_person_resolution = False
    preload_existing_uid_index = False

    def __init__(self) -> None:
        self.ingest_kwargs: dict[str, Any] = {}

    def get_cursor_key(self, **kwargs) -> str:
        account = str(kwargs.get("account_email", "")).strip().lower()
        return f"{self.source_id}:{account}" if account else self.source_id

    def fetch(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> list[dict[str, Any]]:
        return [{"uid": "hfa-join-mail-1", "subject": "Join Proof", "sha": "join1"}]

    def fetch_batches(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> Iterable[FetchedBatch]:
        yield FetchedBatch(
            items=[{"uid": "hfa-join-mail-1", "subject": "Join Proof", "sha": "join1"}],
            cursor_patch={"gmail_history_id": "join-hist-1"},
            sequence=0,
        )

    def to_card(self, item: dict[str, Any]):
        from archive_vault.schema import EmailThreadCard

        card = EmailThreadCard(
            uid=str(item["uid"]),
            type="email_thread",
            source=["gmail-messages"],
            source_id=str(item.get("source_id", item["uid"])),
            created="2026-05-01",
            updated="2026-05-01",
            summary=str(item.get("subject", "Thread")),
            gmail_thread_id=str(item.get("source_id", item["uid"])),
            account_email="me@example.com",
            subject=str(item.get("subject", "Thread")),
            thread_body_sha=str(item.get("sha", "sha1")),
        )
        return card, deterministic_provenance(card, "gmail-messages"), ""

    def ingest(self, vault_path: str, dry_run: bool = False, **kwargs: Any):
        self.ingest_kwargs = dict(kwargs)
        return super().ingest(vault_path, dry_run=dry_run, **kwargs)


def test_maintain_catch_up_handoff_dirty_uids_to_real_executor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Section H join proof: catch-up + source updaters + processors → real wrappers."""

    vault = _join_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)

    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = None
        elif "ingestion_log" in s:
            m.fetchall.return_value = []
            m.fetchone.return_value = None
        else:
            m.fetchone.return_value = None
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side

    adapter = _JoinProofGmailAdapter()

    def _build_adapter(adapter_source_id: str) -> BaseAdapter:
        assert adapter_source_id == "gmail-messages"
        return adapter

    monkeypatch.setattr("archive_sync.source_updaters.runner.build_adapter", _build_adapter)
    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_file_hygiene",
        lambda *a, **k: ({"purged": 0}, {"cards_linked": 0, "cards_scanned": 0}, []),
    )

    store = mock.MagicMock()
    store.vault = vault
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.rebuild.return_value = {"cards": 1}

    rep = run_maintenance(
        store=store,
        logger=logging.getLogger("t"),
        dry_run=False,
        run_source_updaters=True,
        source_updater_keys=["gmail-messages:me@example.com"],
        apply_source_updaters=True,
        source_updater_catch_up=True,
        run_processors=True,
        apply_processors=True,
        processor_keys=[PROCESSOR_MATERIALIZATION],
    )

    assert not any(e.get("step") == "run_source_updaters" for e in rep.errors)
    assert not any(e.get("step") == "run_processors" for e in rep.errors)
    assert adapter.ingest_kwargs.get("catch_up") is True
    assert adapter.ingest_kwargs.get("gmail_promotion_gate") is True
    assert adapter.ingest_kwargs.get("quick_update") is True

    assert rep.source_updater_runs == 1
    su = rep.source_updater_reports[0]
    dirty = list(su.get("dirty_card_uids") or [])
    if not dirty and isinstance(su.get("batch"), dict):
        dirty = list(su["batch"].get("dirty_card_uids") or [])
    assert "hfa-join-mail-1" in dirty
    assert any("catch_up: gmail page cursor reset" in w for w in su.get("warnings") or [])

    assert rep.processor_runs == 1
    proc = rep.processor_reports[0]
    assert proc["executed"] is True
    executed_uids = {r["input_uid"] for r in proc.get("item_results") or []}
    assert "hfa-join-mail-1" in executed_uids
    assert any(r.get("status") == "complete" for r in proc.get("item_results") or [])

    rebuild_calls = [c.kwargs for c in store.rebuild.call_args_list]
    assert any(c.get("force_full") is False for c in rebuild_calls)


def test_maintain_processors_invoke_duplicate_linking_and_junk_purge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _join_vault(tmp_path)
    called: dict[str, Any] = {}

    def fake_purge(path, **kwargs):
        called["purge"] = {"vault": str(path), "dry_run": kwargs.get("dry_run")}
        return {"purged": 2, "dirty_uids": ["hfa-email-message-parent1"]}

    def fake_link(path, **kwargs):
        called["link"] = {
            "vault": str(path),
            "dry_run": kwargs.get("dry_run"),
            "incremental": kwargs.get("incremental"),
            "uid_allowlist": kwargs.get("uid_allowlist"),
        }
        return {
            "cards_linked": 3,
            "cards_scanned": 10,
            "hashes_reused": 8,
            "hashes_computed": 2,
            "groups": 1,
            "dirty_uids": ["hfa-document-aaa111aaa111"],
        }

    monkeypatch.setattr("archive_sync.junk_attachments.run_junk_attachment_purge", fake_purge)
    monkeypatch.setattr("archive_sync.file_identity.run_file_duplicate_linking", fake_link)

    store = mock.MagicMock()
    conn = mock.MagicMock()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "last_maintenance_at" in s:
            m.fetchone.return_value = None
        elif "ingestion_log" in s:
            m.fetchall.return_value = []
            m.fetchone.return_value = None
        else:
            m.fetchone.return_value = None
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    store.vault = vault
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.rebuild.return_value = {"cards": 0}

    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_processors",
        lambda *a, **k: (0, [], 0),
    )

    rep = run_maintenance(
        store=store,
        logger=logging.getLogger("t"),
        dry_run=False,
        run_processors=True,
        apply_processors=True,
    )
    assert called["purge"]["dry_run"] is False
    assert called["link"]["incremental"] is True
    assert called["link"]["dry_run"] is False
    assert called["link"]["uid_allowlist"] is None
    assert rep.junk_attachments_purged == 2
    assert rep.file_duplicates_linked == 3
    assert not any(e.get("step") == "file_hygiene" for e in rep.errors)

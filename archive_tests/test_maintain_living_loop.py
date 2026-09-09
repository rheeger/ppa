"""Living maintain loop: one apply command, report, isolated proof."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from archive_cli.commands.maintain import (
    APPLY_LOOP_STEPS,
    MaintenanceReport,
    apply_provider_failure_reason,
    format_maintain_human_summary,
    living_loop_ok,
    run_maintenance,
)

REPO_ROOT = Path(__file__).resolve().parent.parent


def _connect_ctx(conn):
    class CM:
        def __enter__(self):
            return conn

        def __exit__(self, *a):
            return False

    return CM()


def _empty_store(tmp_path: Path) -> mock.MagicMock:
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
        elif "enrichment_queue" in s:
            m.fetchone.return_value = {"c": 0}
        elif "retrieval_gaps" in s:
            m.fetchone.return_value = {"c": 0}
        else:
            m.fetchone.return_value = None
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn)
    store.vault = tmp_path
    return store


def test_maintain_help_describes_apply_loop() -> None:
    env = {**dict(**__import__("os").environ), "PYTHONDONTWRITEBYTECODE": "1"}
    env.pop("PPA_TEST_PG_DSN", None)
    proc = subprocess.run(
        [sys.executable, "-m", "archive_cli", "maintain", "--help"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    text = proc.stdout.lower()
    assert "--apply" in proc.stdout
    assert "pull" in text
    assert "publish" in text
    assert "tail ingestion ledger" not in text


def test_apply_loop_dry_run_lists_pull_process_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _empty_store(tmp_path)
    captured: dict[str, Any] = {}

    def fake_updaters(*_a, **kwargs):
        captured["updater_apply"] = kwargs.get("apply")
        return 2, [{"source_key": "contacts:google", "status": "success", "dirty_card_uids": []}], False

    def fake_processors(*_a, **kwargs):
        captured["processor_apply"] = kwargs.get("apply")
        return (1, [{"executed": False, "item_results": [], "report": {}}], 0)

    monkeypatch.setattr("archive_cli.commands.maintain._run_source_updaters", fake_updaters)
    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", fake_processors)
    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_file_hygiene",
        lambda *a, **k: ({"purged": 0}, {"cards_linked": 0, "cards_scanned": 0}, []),
    )
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=True, apply_loop=True)
    assert rep.apply_loop is True
    assert tuple(rep.planned_steps) == APPLY_LOOP_STEPS
    assert captured["updater_apply"] is False
    assert captured["processor_apply"] is False
    assert "serving_index_publish (dry-run)" in rep.skipped_steps
    assert "pull" in rep.human_summary.lower()
    assert "process" in rep.human_summary.lower()
    assert "publish" in rep.human_summary.lower()
    assert "no writes" in rep.human_summary.lower()


def test_apply_loop_implies_source_and_processor_apply(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _empty_store(tmp_path)
    captured: dict[str, Any] = {}

    def fake_updaters(*_a, **kwargs):
        captured["updater_apply"] = kwargs.get("apply")
        return 1, [{"source_key": "contacts:google", "status": "success", "dirty_card_uids": []}], False

    def fake_processors(*_a, **kwargs):
        captured["processor_apply"] = kwargs.get("apply")
        return (1, [{"executed": True, "item_results": [], "report": {}}], 0)

    monkeypatch.setattr("archive_cli.commands.maintain._run_source_updaters", fake_updaters)
    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", fake_processors)
    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_file_hygiene",
        lambda *a, **k: ({"purged": 0}, {"cards_linked": 0, "cards_scanned": 0}, []),
    )
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    import archive_cli.providers as providers_mod

    providers_mod.resolve_provider(refresh=True)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False, apply_loop=True)
    assert captured["updater_apply"] is True
    assert captured["processor_apply"] is True
    assert tuple(rep.planned_steps) == APPLY_LOOP_STEPS


def _ready_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    import archive_cli.providers as providers_mod

    providers_mod.resolve_provider(refresh=True)


def test_apply_loop_uses_default_maintain_source_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _empty_store(tmp_path)
    captured: dict[str, Any] = {}

    def fake_default(**kwargs):
        captured["default_kwargs"] = kwargs
        return ["contacts:google", "file-libraries:documents"]

    def fake_run(**kwargs):
        captured["source_keys"] = list(kwargs.get("source_keys") or [])
        reports = [
            mock.Mock(
                to_dict=lambda: {
                    "source_key": key,
                    "status": "success",
                    "dirty_card_uids": [],
                }
            )
            for key in captured["source_keys"]
        ]
        return mock.Mock(reports=reports, completion_state="success")

    monkeypatch.setattr("archive_sync.source_updaters.runner.default_maintain_source_keys", fake_default)
    monkeypatch.setattr("archive_sync.source_updaters.runner.run_source_updaters", fake_run)
    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", lambda *a, **k: (0, [], 0))
    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_file_hygiene",
        lambda *a, **k: ({"purged": 0}, {"cards_linked": 0, "cards_scanned": 0}, []),
    )
    monkeypatch.setenv("GOOGLE_ACCOUNT", "me@example.com")
    _ready_apply(monkeypatch)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False, apply_loop=True)
    assert captured["source_keys"] == ["contacts:google", "file-libraries:documents"]
    assert captured["default_kwargs"]["gmail_accounts"] == ("me@example.com",)
    assert "photos" not in " ".join(captured["source_keys"])
    assert rep.source_updater_runs == 2


def test_apply_loop_failed_source_is_named(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _empty_store(tmp_path)

    def fake_updaters(*_a, **kwargs):
        return (
            2,
            [
                {
                    "source_key": "gmail-messages:me@example.com",
                    "status": "failed",
                    "dirty_card_uids": [],
                    "errors": ["gmail history walk failed"],
                },
                {
                    "source_key": "contacts:google",
                    "status": "success",
                    "dirty_card_uids": ["hfa-person-fixture"],
                },
            ],
            True,
        )

    monkeypatch.setattr("archive_cli.commands.maintain._run_source_updaters", fake_updaters)
    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", lambda *a, **k: (0, [], 0))
    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_file_hygiene",
        lambda *a, **k: ({"purged": 0}, {"cards_linked": 0, "cards_scanned": 0}, []),
    )
    _ready_apply(monkeypatch)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False, apply_loop=True)
    assert rep.failed_sources == ["gmail-messages:me@example.com"]
    assert "gmail-messages:me@example.com" in rep.human_summary
    assert "Result: incomplete because a live source failed." in rep.human_summary
    assert living_loop_ok(rep) is False
    assert "ok." not in rep.human_summary.split("Result:")[-1]


def test_apply_loop_fixture_updater_writes_dirty_uids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from archive_tests.test_maintain import _JoinProofGmailAdapter, _join_vault

    vault = _join_vault(tmp_path)
    adapter = _JoinProofGmailAdapter()

    def _build_adapter(adapter_source_id: str):
        assert adapter_source_id == "gmail-messages"
        return adapter

    monkeypatch.setattr("archive_sync.source_updaters.runner.build_adapter", _build_adapter)
    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", lambda *a, **k: (0, [], 0))
    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_file_hygiene",
        lambda *a, **k: ({"purged": 0}, {"cards_linked": 0, "cards_scanned": 0}, []),
    )
    _ready_apply(monkeypatch)
    store = _empty_store(vault)
    store.vault = vault
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    rep = run_maintenance(
        store=store,
        logger=logging.getLogger("t"),
        dry_run=False,
        apply_loop=True,
        source_updater_keys=["gmail-messages:me@example.com"],
    )
    assert not any(item.get("step") == "run_source_updaters" for item in rep.errors)
    su = rep.source_updater_reports[0]
    dirty = list(su.get("dirty_card_uids") or [])
    if not dirty and isinstance(su.get("batch"), dict):
        dirty = list(su["batch"].get("dirty_card_uids") or [])
    assert "hfa-join-mail-1" in dirty
    assert "hfa-join-mail-1" in rep.publish_uids
    assert adapter.ingest_kwargs.get("catch_up") is not True

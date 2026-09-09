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

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


def test_apply_loop_fails_when_configured_enrichment_is_down(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _empty_store(tmp_path)
    ran = {"updaters": False, "processors": False}

    def boom_updaters(*_a, **_k):
        ran["updaters"] = True
        return 0, [], False

    def boom_processors(*_a, **_k):
        ran["processors"] = True
        return (0, [], 0)

    monkeypatch.setattr("archive_cli.commands.maintain._run_source_updaters", boom_updaters)
    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", boom_processors)
    monkeypatch.setenv("PPA_ENRICHMENT_MODEL", "openai:gpt-4o-mini")
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")

    def fake_resolve(*, refresh: bool = False):
        provider = mock.Mock()
        provider.name = "openai"
        provider.model = "gpt-4o-mini"
        provider.is_available.return_value = False
        return provider

    monkeypatch.setattr("archive_cli.providers.resolve_provider", fake_resolve)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False, apply_loop=True)
    assert ran["updaters"] is False
    assert ran["processors"] is False
    assert any(item.get("step") == "apply_providers" for item in rep.errors)
    assert "unavailable" in (rep.provider_reason or "").lower()
    assert "Result: incomplete because a required provider is down." in rep.human_summary
    assert living_loop_ok(rep) is False


def test_apply_provider_failure_reason_openai_embed_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "openai")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    reason = apply_provider_failure_reason()
    assert "OPENAI_API_KEY" in reason


def test_apply_loop_passes_dirty_uids_and_not_broad_llm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _empty_store(tmp_path)
    captured: dict[str, Any] = {}

    def fake_updaters(*_a, **_k):
        return (
            1,
            [{"source_key": "gmail-messages:me@example.com", "status": "success", "dirty_card_uids": ["hfa-email-dirty"]}],
            False,
        )

    def fake_processors(*_a, **kwargs):
        captured["extra"] = list(kwargs.get("extra_dirty_uids") or [])
        captured["source_reports"] = kwargs.get("source_updater_reports")
        captured["allow_broad_llm"] = kwargs.get("allow_broad_llm")
        captured["allow_full_embedding"] = kwargs.get("allow_full_embedding")
        return (
            1,
            [
                {
                    "executed": True,
                    "item_results": [
                        {
                            "processor_key": "email_typed_extraction",
                            "input_uid": "hfa-email-dirty",
                            "status": "complete",
                            "output_uids": ["hfa-purchase-derived"],
                            "receipt": {"outputs": [{"uid": "hfa-purchase-derived"}]},
                        }
                    ],
                    "report": {"warnings": [], "errors": [], "output_count": 1},
                }
            ],
            1,
        )

    monkeypatch.setattr("archive_cli.commands.maintain._run_source_updaters", fake_updaters)
    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", fake_processors)
    monkeypatch.setattr(
        "archive_cli.commands.maintain._run_file_hygiene",
        lambda *a, **k: ({"purged": 0}, {"cards_linked": 0, "cards_scanned": 0}, []),
    )
    _ready_apply(monkeypatch)
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False, apply_loop=True)
    from archive_sync.processors.dirty_io import dirty_uids_from_source_reports

    assert "hfa-email-dirty" in dirty_uids_from_source_reports(captured["source_reports"] or [])
    assert captured["allow_broad_llm"] is False
    assert captured["allow_full_embedding"] is False
    assert rep.cards_extracted == 1
    assert "hfa-purchase-derived" in rep.publish_uids


def test_dirty_email_extracts_derived_card(tmp_path: Path) -> None:
    from archive_cli.index_config import get_rebuild_workers
    from archive_sync.extractors.amazon import AmazonExtractor
    from archive_sync.extractors.registry import build_default_registry
    from archive_sync.extractors.runner import ExtractionRunner
    from archive_tests.archive_sync.extractors.conftest import write_email_to_vault
    from archive_tests.archive_sync.extractors.test_amazon import AMAZON_ORDER_BODY
    vault = (tmp_path / "vault").resolve()
    for name in (
        "People",
        "Email",
        "EmailThreads",
        "Transactions/Purchases",
        "Entities/Organizations",
        "_meta",
        "_templates",
        ".obsidian",
    ):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    email_uid = "hfa-email-message-liveloop1"
    fm = {
        "uid": email_uid,
        "type": "email_message",
        "source": ["gmail"],
        "source_id": "gmail.msg.liveloop1",
        "created": "2024-03-15",
        "updated": "2024-03-15",
        "summary": "Your Amazon.com order confirmation",
        "gmail_message_id": "msgid-liveloop1",
        "gmail_thread_id": "thread-liveloop1",
        "account_email": "me@example.com",
        "from_email": "auto-confirm@amazon.com",
        "to_emails": ["me@example.com"],
        "subject": "Your Amazon.com order confirmation",
        "sent_at": "2024-03-15T14:30:00-08:00",
        "people": [],
        "orgs": [],
        "tags": [],
    }
    write_email_to_vault(str(vault), "Email/2024-03/liveloop-amazon.md", fm, AMAZON_ORDER_BODY)
    runner = ExtractionRunner(
        str(vault),
        registry=build_default_registry(),
        dry_run=False,
        workers=get_rebuild_workers(),
        limit=1,
        uid_allowlist={email_uid},
    )
    metrics = runner.run()
    purchase_uid = AmazonExtractor().generate_derived_uid(email_uid, "112-1234567-1234567")
    assert int(getattr(metrics, "extracted_cards", 0) or 0) >= 1
    assert metrics.created
    assert metrics.created[0].output_uid == purchase_uid
    assert (vault / metrics.created[0].rel_path).is_file()


def test_embedding_processor_uses_hash_when_llm_is_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from archive_sync.processors.batch import ProcessorPlanItem
    from archive_sync.processors.constants import PROCESSOR_EMBEDDING
    from archive_sync.processors.runner import ExecuteContext, _execute_embedding

    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    called: dict[str, Any] = {}

    def fake_embed_pending(**kwargs):
        called["uid_allowlist"] = kwargs.get("uid_allowlist")
        return {
            "embedded": 0,
            "reused": 1,
            "reused_by_content": 1,
            "failed": 0,
            "selected": 1,
            "chunk_keys_by_uid": {"card-a": ["new-key"]},
            "completed_chunk_keys": ["new-key"],
            "failed_chunk_keys": [],
            "pending_chunk_keys": [],
        }

    ctx = ExecuteContext(
        vault_path=".",
        store=SimpleNamespace(embed_pending=fake_embed_pending),
        apply=True,
        dry_run=False,
        provider_available=False,
    )
    item = ProcessorPlanItem(
        processor_key=PROCESSOR_EMBEDDING,
        input_uid="card-a",
        current_input_hash="h",
        input_revision="h",
        processor_version="embedding-v1",
    )
    out = _execute_embedding(ctx, [item])
    assert called["uid_allowlist"] == {"card-a"}
    assert out.results[0].status == "complete"
    assert any("reused_by_content=1" in warning for warning in out.warnings)

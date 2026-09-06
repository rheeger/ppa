"""P03-D: one maintain path journals, processes, publishes, and serves only eligible work."""

from __future__ import annotations

import inspect
import logging
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from archive_cli.commands.maintain import (
    MaintenanceReport,
    _publish_serving_index,
    counts_from_processor_reports,
    eligible_checkpoint_for_report,
    failed_required_uids,
    run_maintenance,
)
from archive_engine.publication import PublicationReceipt
from archive_sync.source_updaters.batch import commit_cursor_after_persisted


def _connect_ctx(conn):
    class CM:
        def __enter__(self):
            return conn

        def __exit__(self, *a):
            return False

    return CM()


def _empty_conn() -> mock.MagicMock:
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
            m.fetchone.return_value = {"c": 0}
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    return conn


def _store(tmp_path: Path, conn=None) -> mock.MagicMock:
    store = mock.MagicMock()
    store.vault = tmp_path
    store.index.schema = "ppa"
    store.index._connect.return_value = _connect_ctx(conn or _empty_conn())
    return store


def _receipt(*, generation: str = "gen-1", ok: bool = True, acked: int = 1, eligible: int = 1) -> PublicationReceipt:
    return PublicationReceipt(
        generation_id=generation,
        mode="incremental",
        parent_generation="",
        base_generation="",
        snapshot_id="",
        source_watermark=eligible,
        cards=0,
        chunks=0,
        embeddings=0,
        tombstone_uids=(),
        replaced_uids=(),
        compacted=False,
        ok=ok,
        acked_watermark=acked,
        eligible_checkpoint=eligible,
        error="" if ok else "publish_failed",
    )


def test_maintain_source_has_no_duplicate_extract_chain() -> None:
    from archive_cli.commands.maintain import _run_processors

    src = inspect.getsource(run_maintenance)
    assert "ExtractionRunner" not in src
    assert "run_entity_resolution" not in src
    assert "store.rebuild" not in src
    assert 'default_processor_decision="typed_extraction"' in inspect.getsource(_run_processors)


def test_counts_match_real_receipts() -> None:
    reports = [
        {
            "item_results": [
                {
                    "processor_key": "email_typed_extraction",
                    "input_uid": "email-1",
                    "status": "complete",
                    "output_uids": ["purchase-1"],
                    "receipt": {"outputs": [{"uid": "purchase-1", "revision": "rev-a"}]},
                },
                {
                    "processor_key": "entity_resolution",
                    "input_uid": "purchase-1",
                    "status": "complete",
                    "output_uids": ["org-1"],
                    "receipt": {"outputs": [{"uid": "org-1"}]},
                },
                {
                    "processor_key": "materialization",
                    "input_uid": "purchase-1",
                    "status": "complete",
                    "output_uids": ["purchase-1"],
                },
                {
                    "processor_key": "email_typed_extraction",
                    "input_uid": "email-2",
                    "status": "failed",
                    "output_uids": [],
                    "error": "boom",
                },
            ]
        }
    ]
    counts = counts_from_processor_reports(reports)
    assert counts["cards_extracted"] == 1
    assert counts["entities_resolved"] == 1
    assert counts["cards_rebuilt"] == 1
    assert failed_required_uids(reports) == ["email-2"]


def test_failed_revision_is_not_published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli.index_store import PostgresArchiveIndex

    store = mock.MagicMock()
    store.vault = tmp_path
    store.index = mock.MagicMock(spec=PostgresArchiveIndex)
    called: dict[str, Any] = {}

    def fake_publish(checkpoint, context):
        called["dirty_uids"] = list(context.get("dirty_uids") or [])
        called["checkpoint"] = checkpoint
        return _receipt()

    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _vault: {"serving_index_ready": True, "serving_index_generation": "gen-old"},
    )
    monkeypatch.setattr("archive_cli.serving_index.read_dirty_uids", lambda _vault: [])
    monkeypatch.setattr("archive_engine.publication.publish", fake_publish)

    report = MaintenanceReport(
        cards_rebuilt=1,
        publish_uids=["ok-uid", "failed-uid"],
        processor_reports=[
            {
                "item_results": [
                    {
                        "processor_key": "materialization",
                        "input_uid": "ok-uid",
                        "status": "complete",
                        "output_uids": ["ok-uid"],
                    },
                    {
                        "processor_key": "materialization",
                        "input_uid": "failed-uid",
                        "status": "failed",
                        "output_uids": [],
                    },
                ]
            }
        ],
    )
    plan = eligible_checkpoint_for_report(store, report)
    assert "failed-uid" in plan["failed_uids"]
    assert "failed-uid" not in plan["dirty_uids"]
    assert plan["high_watermark"] <= plan["published_watermark"]
    _publish_serving_index(store, report, logging.getLogger("t"), dry_run=False)
    assert "failed-uid" not in called.get("dirty_uids", [])
    assert "ok-uid" in called.get("dirty_uids", [])


def test_create_update_delete_go_through_one_scheduler(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_processors(*_a, **kwargs):
        captured["extra"] = list(kwargs.get("extra_dirty_uids") or [])
        captured["apply"] = kwargs.get("apply")
        return (
            1,
            [
                {
                    "executed": True,
                    "item_results": [
                        {
                            "processor_key": "materialization",
                            "input_uid": uid,
                            "status": "complete",
                            "output_uids": [uid],
                        }
                        for uid in ("uid-create", "uid-update", "uid-delete")
                    ],
                    "report": {"warnings": ["materialization incremental rebuild cards=3"], "errors": []},
                }
            ],
            3,
        )

    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", fake_processors)
    monkeypatch.setattr("archive_cli.serving_index.read_dirty_uids", lambda _vault: ["uid-delete"])
    store = _store(tmp_path)
    conn = store.index._connect().__enter__()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "ingestion_log" in s and "COUNT" not in s.upper():
            m.fetchall.return_value = [
                {"card_uid": "uid-create", "action": "created", "source_adapter": "gmail", "logged_at": "t1"},
                {"card_uid": "uid-update", "action": "updated", "source_adapter": "gmail", "logged_at": "t2"},
            ]
        elif "last_maintenance_at" in s:
            m.fetchone.return_value = None
        else:
            m.fetchone.return_value = {"c": 0}
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=False)
    assert captured["apply"] is True
    assert {"uid-create", "uid-update", "uid-delete"} <= set(captured["extra"])
    assert rep.cards_rebuilt == 3
    store.rebuild.assert_not_called()


def test_dry_run_is_read_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_processors(*_a, **kwargs):
        captured["apply"] = kwargs.get("apply")
        return (1, [{"executed": False, "item_results": [], "report": {}}], 0)

    def boom_publish(*_a, **_k):
        raise AssertionError("dry-run must not publish")

    monkeypatch.setattr("archive_cli.commands.maintain._run_processors", fake_processors)
    monkeypatch.setattr("archive_engine.publication.publish", boom_publish)
    store = _store(tmp_path)
    conn = store.index._connect().__enter__()

    def exec_side(sql, params=None):
        m = mock.MagicMock()
        s = str(sql)
        if "ingestion_log" in s:
            m.fetchall.return_value = [
                {"card_uid": "uid-new", "action": "created", "source_adapter": "x", "logged_at": "t"},
            ]
        elif "last_maintenance_at" in s and "INSERT" in s:
            raise AssertionError("dry-run must not move last_maintenance_at")
        else:
            m.fetchone.return_value = None
            m.fetchall.return_value = []
        return m

    conn.execute.side_effect = exec_side
    rep = run_maintenance(store=store, logger=logging.getLogger("t"), dry_run=True)
    assert captured["apply"] is False
    assert any("dry-run" in step for step in rep.skipped_steps)
    store.rebuild.assert_not_called()


def test_noop_maintain_skips_publish(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli.index_store import PostgresArchiveIndex

    store = mock.MagicMock()
    store.vault = tmp_path
    store.index = mock.MagicMock(spec=PostgresArchiveIndex)
    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _vault: {"serving_index_ready": True, "serving_index_generation": "gen-keep"},
    )
    monkeypatch.setattr("archive_cli.serving_index.read_dirty_uids", lambda _vault: [])

    def boom(*_a, **_k):
        raise AssertionError("no-op must not publish")

    monkeypatch.setattr("archive_engine.publication.publish", boom)
    report = MaintenanceReport(nothing_to_do=True)
    _publish_serving_index(store, report, logging.getLogger("t"), dry_run=False)
    assert "serving_index_publish (clean)" in report.skipped_steps
    assert report.serving_index.get("serving_index_generation") == "gen-keep"


def test_source_cursor_moves_only_after_persist() -> None:
    before = {"history_id": "1"}
    dry = commit_cursor_after_persisted(
        side_effects_persisted=False,
        cursor_before=before,
        cursor_patch={"history_id": "2"},
    )
    applied = commit_cursor_after_persisted(
        side_effects_persisted=True,
        cursor_before=before,
        cursor_patch={"history_id": "2"},
    )
    assert dry == before
    assert applied["history_id"] == "2"


def test_partial_failure_preserves_pending_gap(tmp_path: Path) -> None:
    store = _store(tmp_path)
    report = MaintenanceReport(
        publish_uids=["ok-uid", "pending-uid"],
        processor_reports=[
            {
                "item_results": [
                    {
                        "processor_key": "materialization",
                        "input_uid": "ok-uid",
                        "status": "complete",
                        "output_uids": ["ok-uid"],
                    },
                    {
                        "processor_key": "materialization",
                        "input_uid": "pending-uid",
                        "status": "pending",
                        "output_uids": [],
                    },
                ]
            }
        ],
    )
    plan = eligible_checkpoint_for_report(store, report)
    assert "pending-uid" in plan["pending_uids"]
    assert "pending-uid" not in plan["dirty_uids"]
    assert "ok-uid" in plan["dirty_uids"]


def test_failed_publish_does_not_claim_served(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli.index_store import PostgresArchiveIndex

    store = mock.MagicMock()
    store.vault = tmp_path
    store.index = mock.MagicMock(spec=PostgresArchiveIndex)
    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _vault: {"serving_index_ready": False, "serving_index_generation": ""},
    )
    monkeypatch.setattr("archive_cli.serving_index.read_dirty_uids", lambda _vault: [])
    monkeypatch.setattr(
        "archive_engine.publication.publish",
        lambda *_a, **_k: _receipt(ok=False, acked=0, eligible=4),
    )
    report = MaintenanceReport(cards_rebuilt=1, publish_uids=["uid-a"])
    _publish_serving_index(store, report, logging.getLogger("t"), dry_run=False)
    assert report.publication.get("ok") is False
    assert any(e.get("step") == "serving_index_publish" for e in report.errors)
    assert report.published_watermark == 0


@pytest.mark.integration
def test_maintain_create_update_restart_queryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pgvector_dsn: str
) -> None:
    import uuid

    from archive_cli.index_store import PostgresArchiveIndex
    from archive_cli.serving_index import mark_serving_index_dirty
    from archive_cli.store import DefaultArchiveStore
    from archive_tests.acceptance.environment import inspect_warehouse_card, reset_serving_handle
    from archive_vault.provenance import ProvenanceEntry
    from archive_vault.schema import PersonCard
    from archive_vault.vault import write_card

    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    vault = tmp_path / "hf-archives"
    for name in ("People", "Email", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    uid = "hfa-person-p03dpipel01"
    card = PersonCard(
        uid=uid,
        type="person",
        source=["test.p03d"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary="P03 D Pipeline Person",
        first_name="P03",
        last_name="Pipeline",
        emails=[f"{uid}@example.test"],
        tags=["p03d", "synthetic"],
    )
    prov = {
        field: ProvenanceEntry("test.p03d", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name", "emails", "tags")
    }
    write_card(vault, "People/p03d-pipeline.md", card, body="create revision", provenance=prov)
    schema = f"p03d_{uuid.uuid4().hex[:10]}"
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "hash")
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(vault / "_meta" / "rust-search-index"))
    monkeypatch.setenv("PPA_STATEMENT_TIMEOUT_MS", "120000")
    index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
    index.schema = schema
    index.bootstrap()
    store = DefaultArchiveStore(vault=vault, index=index)
    mark_serving_index_dirty(vault, "p03d-create", [uid])
    first = run_maintenance(
        store=store,
        logger=logging.getLogger("t"),
        dry_run=False,
        run_processors=True,
        apply_processors=True,
        processor_keys=["materialization", "embedding"],
    )
    row = inspect_warehouse_card(pgvector_dsn, schema, uid)
    assert row is not None
    assert first.publication.get("ok") is True or "serving_index_publish" not in {e.get("step") for e in first.errors}
    write_card(vault, "People/p03d-pipeline.md", card, body="update revision", provenance=prov)
    mark_serving_index_dirty(vault, "p03d-update", [uid])
    second = run_maintenance(
        store=store,
        logger=logging.getLogger("t"),
        dry_run=False,
        run_processors=True,
        apply_processors=True,
        processor_keys=["materialization", "embedding"],
    )
    assert second.cards_rebuilt >= 1 or second.processor_runs == 1
    third = run_maintenance(
        store=store,
        logger=logging.getLogger("t"),
        dry_run=False,
        run_processors=True,
        apply_processors=True,
        processor_keys=["materialization", "embedding"],
    )
    assert third.nothing_to_do or "serving_index_publish (clean)" in third.skipped_steps
    reset_serving_handle()
    assert first.maintenance_run_id
    assert first.journal_watermark >= 0
    assert second.journal_watermark >= first.journal_watermark

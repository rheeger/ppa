"""Maintain nightly delta paths — seed-link catalog scope and updater partial success."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from archive_cli.seed_links import SeedLinkCatalog, build_seed_link_catalog, run_seed_link_backfill
from archive_sync.source_updaters.constants import RUN_STATUS_FAILED, RUN_STATUS_SUCCESS
from archive_sync.source_updaters.runner import run_source_updaters
from archive_sync.transient_retry import is_transient_error


def test_is_transient_error_broken_pipe() -> None:
    assert is_transient_error(BrokenPipeError(32, "Broken pipe"))
    assert is_transient_error(OSError("deadlock detected"))


def test_build_seed_link_catalog_reuse_existing() -> None:
    existing = SeedLinkCatalog(
        cards_by_uid={"uid-a": MagicMock(uid="uid-a")},
        cards_by_exact_slug={},
        cards_by_slug={},
        cards_by_type={},
        person_by_email={},
        person_by_phone={},
        person_by_handle={},
        person_by_alias={},
        email_threads_by_thread_id={},
        email_messages_by_thread_id={},
        email_messages_by_message_id={},
        email_attachments_by_message_id={},
        email_attachments_by_thread_id={},
        imessage_threads_by_chat_id={},
        imessage_messages_by_chat_id={},
        calendar_events_by_event_id={},
        calendar_events_by_ical_uid={},
        media_by_day={},
        events_by_day={},
        path_buckets={},
    )
    out = build_seed_link_catalog("/tmp/vault", catalog=existing)
    assert out is existing


def test_enqueue_seed_link_jobs_scoped_uses_uid_query(monkeypatch, tmp_path: Path) -> None:
    from archive_cli import seed_links as sl

    cache = MagicMock()
    cache.all_stems.return_value = {"person-a"}
    cache.frontmatter_rows_for_uids.return_value = [
        {
            "uid": "uid-a",
            "rel_path": "People/a.md",
            "frontmatter": {
                "uid": "uid-a",
                "type": "person",
                "slug": "person-a",
                "emails": ["a@example.com"],
            },
        }
    ]
    index = MagicMock()
    index.vault = str(tmp_path)
    index.schema = "ppa"
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    index._connect.return_value.__enter__.return_value = conn

    monkeypatch.setattr(sl, "get_modules_for_card_type", lambda _t: ["identity"])
    monkeypatch.setattr(sl, "_module_should_enqueue_fast", lambda *a, **k: True)
    monkeypatch.setattr(
        sl,
        "_sketch_from_frontmatter",
        lambda **kwargs: sl.SeedCardSketch(
            uid="uid-a",
            rel_path=kwargs["rel_path"],
            slug="person-a",
            card_type="person",
            summary="A",
            frontmatter=kwargs["frontmatter"],
            body="",
            content_hash="hash",
            activity_at="",
            wikilinks=[],
            emails={"a@example.com"},
        ),
    )

    result = sl.enqueue_seed_link_jobs(
        index,
        job_type="incremental",
        source_uids={"uid-a"},
        cache=cache,
    )

    cache.frontmatter_rows_for_uids.assert_called_once()
    cache.all_frontmatters.assert_not_called()
    assert result["prepared"] >= 1


def test_run_seed_link_backfill_builds_catalog_once(monkeypatch, tmp_path: Path) -> None:
    builds = {"count": 0}
    shared = SeedLinkCatalog(
        cards_by_uid={},
        cards_by_exact_slug={},
        cards_by_slug={},
        cards_by_type={},
        person_by_email={},
        person_by_phone={},
        person_by_handle={},
        person_by_alias={},
        email_threads_by_thread_id={},
        email_messages_by_thread_id={},
        email_messages_by_message_id={},
        email_attachments_by_message_id={},
        email_attachments_by_thread_id={},
        imessage_threads_by_chat_id={},
        imessage_messages_by_chat_id={},
        calendar_events_by_event_id={},
        calendar_events_by_ical_uid={},
        media_by_day={},
        events_by_day={},
        path_buckets={},
    )

    def _build(*args, **kwargs):
        builds["count"] += 1
        return shared

    monkeypatch.setattr("archive_cli.seed_links.VaultScanCache.build_or_load", lambda *a, **k: MagicMock())
    monkeypatch.setattr("archive_cli.seed_links.build_seed_link_catalog", _build)
    monkeypatch.setattr("archive_cli.seed_links._count_orphaned_links", lambda *a, **k: 0)
    monkeypatch.setattr(
        "archive_cli.seed_links.run_seed_link_enqueue",
        lambda *a, **k: {"prepared": 1, "enqueued": 1, "existing": 0},
    )
    worker_calls: list[dict] = []
    promo_calls: list[dict] = []

    def _workers(*args, **kwargs):
        worker_calls.append(kwargs)
        return {"workers": 1, "jobs_completed": 0, "jobs_failed": 0, "candidates": 0, "needs_review": 0, "auto_promoted": 0, "canonical_safe": 0, "llm_judged": 0, "module_metrics": {}}

    def _promo(*args, **kwargs):
        promo_calls.append(kwargs)
        return {"derived_edge": 0, "canonical_field": 0, "blocked": 0}

    monkeypatch.setattr("archive_cli.seed_links.run_seed_link_workers", _workers)
    monkeypatch.setattr("archive_cli.seed_links.run_seed_link_promotion_workers", _promo)
    monkeypatch.setattr("archive_cli.seed_links.run_seed_link_report", lambda *a, **k: {"orphaned_links_after": 0})

    index = MagicMock()
    index.vault = str(tmp_path)
    index.ensure_ready = MagicMock()

    run_seed_link_backfill(
        index,
        job_type="incremental",
        source_uids={"uid-a"},
        apply_promotions=True,
    )

    assert builds["count"] == 1
    assert worker_calls[0]["catalog"] is shared
    assert promo_calls[0]["catalog"] is shared


def test_run_source_updaters_partial_success_not_strict(monkeypatch, tmp_path: Path) -> None:
    from archive_sync.source_updaters import runner as sur

    reports = []

    def _fake_run(**kwargs):
        key = kwargs["source_key"]
        from archive_sync.source_updaters.batch import SourceUpdaterRunReport

        if key.endswith(":bad"):
            rep = SourceUpdaterRunReport(
                run_id="r1",
                source_key=key,
                source_type="test",
                archive_instance="inst",
                status=RUN_STATUS_FAILED,
                errors=["boom"],
            )
        else:
            rep = SourceUpdaterRunReport(
                run_id="r2",
                source_key=key,
                source_type="test",
                archive_instance="inst",
                status=RUN_STATUS_SUCCESS,
            )
        return sur.SourceUpdaterRunResult(report=rep, exit_hint=0 if rep.status == RUN_STATUS_SUCCESS else 1)

    monkeypatch.setattr(sur, "run_source_updater", _fake_run)

    multi = run_source_updaters(
        source_keys=["good:local", "bad:bad"],
        vault_path=tmp_path,
        apply=False,
        strict=False,
    )
    assert multi.exit_code == 0
    assert multi.completion_state == "partial"
    assert len(multi.reports) == 2


def test_run_source_updaters_strict_fails_on_error(monkeypatch, tmp_path: Path) -> None:
    from archive_sync.source_updaters import runner as sur

    def _fake_run(**kwargs):
        from archive_sync.source_updaters.batch import SourceUpdaterRunReport

        rep = SourceUpdaterRunReport(
            run_id="r1",
            source_key=kwargs["source_key"],
            source_type="test",
            archive_instance="inst",
            status=RUN_STATUS_FAILED,
            errors=["boom"],
        )
        return sur.SourceUpdaterRunResult(report=rep, exit_hint=1)

    monkeypatch.setattr(sur, "run_source_updater", _fake_run)

    multi = run_source_updaters(
        source_keys=["bad:local"],
        vault_path=tmp_path,
        apply=False,
        strict=True,
    )
    assert multi.exit_code == 1


def test_maintain_failed_allows_partial(monkeypatch) -> None:
    import importlib.util
    from pathlib import Path

    script = Path(__file__).resolve().parents[1] / "archive_scripts" / "ppa-maintain-nightly.py"
    spec = importlib.util.spec_from_file_location("ppa_maintain_nightly", script)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    report = {
        "source_updater_partial": True,
        "source_updater_reports": [{"source_key": "gmail-messages:x", "status": "failed"}],
    }
    assert mod.maintain_failed(report, strict=False) is None
    assert mod.maintain_failed(report, strict=True) is not None

"""Maintain nightly delta paths — seed-link catalog scope and updater partial success."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from archive_cli.seed_links import (
    SeedLinkCatalog,
    build_seed_link_catalog,
    expand_catalog_neighbor_closure,
    run_seed_link_backfill,
)
from archive_sync.source_updaters.constants import RUN_STATUS_FAILED, RUN_STATUS_SUCCESS
from archive_sync.source_updaters.runner import run_source_updaters
from archive_sync.transient_retry import is_transient_error


def test_is_transient_error_broken_pipe() -> None:
    assert is_transient_error(BrokenPipeError(32, "Broken pipe"))
    assert is_transient_error(OSError("deadlock detected"))


def test_build_seed_link_catalog_scoped_does_not_enumerate_all_notes(monkeypatch, tmp_path: Path) -> None:
    cache = MagicMock()
    cache.all_frontmatters.return_value = [("People/a.md", {"uid": "uid-all"})] * 999
    cache.frontmatter_rows_for_uids.return_value = [
        {
            "uid": "uid-a",
            "rel_path": "Email/2026/a.md",
            "frontmatter": {"uid": "uid-a", "type": "email_message", "slug": "a"},
        }
    ]
    cache.rel_path_to_uid.return_value = {"Email/2026/a.md": "uid-a"}
    cache.rel_paths_by_type.return_value = {"person": []}

    monkeypatch.setattr(
        "archive_cli.seed_links.expand_catalog_neighbor_closure",
        lambda _cache, uids, **kwargs: set(uids),
    )
    monkeypatch.setattr(
        "archive_cli.seed_links._sketch_from_frontmatter",
        lambda **kwargs: MagicMock(
            uid="uid-a",
            rel_path=kwargs["rel_path"],
            slug="a",
            card_type="email_message",
            summary="A",
            frontmatter=kwargs["frontmatter"],
            body="",
            content_hash="hash",
            activity_at="",
            wikilinks=[],
            emails=set(),
        ),
    )

    build_seed_link_catalog("/tmp/vault", cache=cache, catalog_uids={"uid-a"})
    cache.all_frontmatters.assert_not_called()
    cache.frontmatter_rows_for_uids.assert_called()


def test_expand_catalog_neighbor_closure_adds_wikilink_neighbor() -> None:
    cache = MagicMock()
    cache.frontmatter_rows_for_uids.side_effect = [
        [
            {
                "uid": "uid-a",
                "rel_path": "Email/2026/a.md",
                "frontmatter": {"uid": "uid-a", "type": "email_message", "people": ["[[person-b]]"]},
            }
        ],
        [],
    ]
    cache.wikilinks_for_rel_path.return_value = ["person-b"]
    cache.rel_path_for_slug.return_value = "People/b.md"
    cache.uid_for_rel_path.return_value = "uid-b"

    expanded = expand_catalog_neighbor_closure(cache, {"uid-a"})
    assert expanded == {"uid-a", "uid-b"}
    cache.rel_path_to_uid.assert_not_called()
    cache.rel_paths_by_type.assert_not_called()


def test_maintain_rebuilds_vault_cache_tier_2_after_updaters() -> None:
    import inspect

    from archive_cli.commands.maintain import run_maintenance

    src = inspect.getsource(run_maintenance)
    assert "rebuild_vault_cache_after_writes(store.vault, tier=2" in src


def test_expand_catalog_neighbor_closure_uses_serving_index(monkeypatch) -> None:
    cache = MagicMock()

    class _Handle:
        def neighbor_uids(self, uids, hops=1):
            assert set(uids) == {"uid-a"}
            assert hops == 1
            return ["uid-a", "uid-b", "uid-c"]

    monkeypatch.setattr("archive_cli.serving_index.get_serving_handle", lambda _vault: _Handle())
    expanded = expand_catalog_neighbor_closure(cache, {"uid-a"}, vault_path="/tmp/vault")
    assert expanded == {"uid-a", "uid-b", "uid-c"}
    cache.frontmatter_rows_for_uids.assert_not_called()
    cache.rel_path_to_uid.assert_not_called()


def test_run_source_updaters_defers_vault_cache_invalidation(monkeypatch, tmp_path: Path) -> None:
    from archive_sync.source_updaters import runner as sur

    events: list[str] = []

    monkeypatch.setattr(
        "archive_cli.vault_cache_runtime.begin_defer_vault_written",
        lambda: events.append("begin"),
    )
    monkeypatch.setattr(
        "archive_cli.vault_cache_runtime.end_defer_vault_written",
        lambda **kwargs: events.append("end"),
    )

    def _fake_run(**kwargs):
        from archive_sync.source_updaters.batch import SourceUpdaterRunReport

        rep = SourceUpdaterRunReport(
            run_id="r1",
            source_key=kwargs["source_key"],
            source_type="test",
            archive_instance="inst",
            status=RUN_STATUS_SUCCESS,
        )
        return sur.SourceUpdaterRunResult(report=rep, exit_hint=0)

    monkeypatch.setattr(sur, "run_source_updater", _fake_run)

    multi = run_source_updaters(
        source_keys=["good:local", "good2:local"],
        vault_path=tmp_path,
        apply=True,
        defer_vault_cache_invalidation=True,
    )
    assert multi.exit_code == 0
    assert events == ["begin", "end"]


def test_calendar_adapter_skips_person_index(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.adapters.calendar_events import CalendarEventsAdapter

    adapter = CalendarEventsAdapter()
    assert adapter.enable_person_resolution is False
    assert adapter.should_enable_person_resolution() is False

    built = {"n": 0}

    def _boom(*_a, **_k):
        built["n"] += 1
        raise AssertionError("PersonIndex should not be built for calendar ingest")

    monkeypatch.setattr("archive_vault.identity_resolver.PersonIndex", _boom)
    adapter.fetch = lambda vault_path, cursor, config=None, **kwargs: []  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_path), account_email="me@example.com", dry_run=True)
    assert built["n"] == 0
    assert result.created == 0


def test_person_index_load_uses_people_cache_slice(tmp_path: Path, monkeypatch) -> None:
    from archive_vault.identity_resolver import PersonIndex

    calls = {"iter_notes": 0, "cache": 0}

    def _fake_cache_rows(_vault):
        calls["cache"] += 1
        return [
            {
                "rel_path": "People/alice.md",
                "frontmatter": {
                    "uid": "hfa-person-alice",
                    "type": "person",
                    "source": ["test"],
                    "source_id": "alice@example.com",
                    "created": "2026-01-01",
                    "updated": "2026-01-01",
                    "summary": "Alice Example",
                    "first_name": "Alice",
                    "last_name": "Example",
                },
            }
        ]

    monkeypatch.setattr(PersonIndex, "_load_people_rows_from_cache", _fake_cache_rows)

    def _iter_notes_should_not_run(*_a, **_k):
        calls["iter_notes"] += 1
        yield from []

    monkeypatch.setattr("archive_vault.identity_resolver.iter_notes", _iter_notes_should_not_run)
    idx = PersonIndex(tmp_path, preload=True)
    assert calls["cache"] == 1
    assert calls["iter_notes"] == 0
    assert len(idx.records) == 1


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
    cache.all_stems.assert_not_called()
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
        return {
            "workers": 1,
            "jobs_completed": 0,
            "jobs_failed": 0,
            "candidates": 0,
            "needs_review": 0,
            "auto_promoted": 0,
            "canonical_safe": 0,
            "llm_judged": 0,
            "module_metrics": {},
        }

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

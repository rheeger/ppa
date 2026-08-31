"""Section D — source updater contract tests."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from archive_cli.source_updaters.cli import cmd_record_run, cmd_status
from archive_cli.validation_gates.constants import EXIT_BLOCKED, EXIT_REFUSED
from archive_sync.gmail_promotion.metrics import GmailPromotionBatchMetrics
from archive_sync.source_updaters.batch import (
    SourceUpdaterBatchSummary,
    SourceUpdaterRunReport,
    batch_summary_from_skip_details,
    commit_cursor_after_persisted,
    cursor_patch_may_commit,
)
from archive_sync.source_updaters.constants import (
    STALENESS_BLOCKED,
    STALENESS_FAILED,
    STALENESS_FRESH,
    STALENESS_NEVER_SYNCED,
    STALENESS_STALE,
)
from archive_sync.source_updaters.declarations import (
    expand_declarations,
    iter_declaration_templates,
    validate_all_declarations,
    validate_declaration,
)
from archive_sync.source_updaters.staleness import compute_staleness_state
from archive_sync.source_updaters.state_store import (
    SourceUpdaterStateRecord,
    SourceUpdaterStateStore,
    record_isolated_source_results,
)


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Attachments", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text(
        json.dumps(
            {
                "gmail-messages": {
                    "gmail_history_id": "100",
                    "last_sync": "2026-05-01T12:00:00+00:00",
                    "skip_details": {
                        "promotion_promoted": 2,
                        "promotion_suppressed": 5,
                        "promotion_quarantined": 1,
                        "promotion_observed": 8,
                    },
                },
                "calendar-events": {"sync_token": "cal-sync-1", "event_etag": "etag-9"},
                "imessage": {"last_completed_message_rowid": 4242},
                "photos": {"modified_at": "2026-04-01", "metadata_hash": "abc123"},
            }
        ),
        encoding="utf-8",
    )
    return vault


def test_declaration_validation_passes_for_templates() -> None:
    errors = validate_all_declarations()
    assert errors == {}
    for decl in iter_declaration_templates():
        assert validate_declaration(decl) == []


def test_state_persistence_meta_fallback(tmp_path: Path) -> None:
    meta = tmp_path / "source-updaters.json"
    store = SourceUpdaterStateStore(None, meta_path=meta)
    record = SourceUpdaterStateRecord(
        source_key="gmail-messages:test@example.com",
        source_type="gmail",
        last_success_at="2026-05-01T00:00:00+00:00",
    )
    store.upsert_state(record, last_run_status="success")
    loaded = store.get_state("gmail-messages:test@example.com")
    assert loaded is not None
    assert loaded.source_type == "gmail"
    assert loaded.staleness_state in (STALENESS_FRESH, STALENESS_STALE)


def test_batch_summary_from_gmail_promotion_skip_details() -> None:
    metrics = GmailPromotionBatchMetrics(
        observed=10,
        promoted=3,
        suppressed=6,
        quarantined=1,
    )
    summary = batch_summary_from_skip_details(metrics.to_skip_details(), observed=10)
    assert summary.promoted == 3
    assert summary.suppressed == 6
    assert summary.quarantined == 1
    assert summary.observed == 10


def test_cursor_patch_commits_only_after_persisted() -> None:
    before = {"gmail_history_id": "100"}
    patch = {"gmail_history_id": "200"}
    after = commit_cursor_after_persisted(
        side_effects_persisted=False,
        cursor_before=before,
        cursor_patch=patch,
    )
    assert after["gmail_history_id"] == "100"
    after_ok = commit_cursor_after_persisted(
        side_effects_persisted=True,
        cursor_before=before,
        cursor_patch=patch,
    )
    assert after_ok["gmail_history_id"] == "200"
    assert not cursor_patch_may_commit(side_effects_persisted=False)
    assert cursor_patch_may_commit(side_effects_persisted=True, batch_errors=[])


def test_one_source_failure_does_not_block_others(tmp_path: Path) -> None:
    meta = tmp_path / "su.json"
    good = SourceUpdaterRunReport(
        run_id="run-ok",
        source_key="calendar-events:a@x.com",
        source_type="calendar",
        status="success",
        batch=SourceUpdaterBatchSummary(promoted=1),
    )
    bad = SourceUpdaterRunReport(
        run_id="run-bad",
        source_key="bad-source",
        source_type="gmail",
        status="failed",
        errors=["simulated"],
    )

    class BrokenStore(SourceUpdaterStateStore):
        def record_run(self, report: SourceUpdaterRunReport) -> None:
            if report.source_key == "bad-source":
                raise RuntimeError("boom")
            super().record_run(report)

    broken = BrokenStore(None, meta_path=meta)
    good2 = SourceUpdaterRunReport(
        run_id="run-ok-2",
        source_key="photos:local",
        source_type="photos",
        status="success",
    )
    errs = record_isolated_source_results(broken, [good, bad, good2])
    assert len(errs) == 1
    assert "bad-source" in errs[0]
    assert broken.get_state("calendar-events:a@x.com") is not None


@pytest.mark.parametrize(
    "last_success,last_attempt,last_error,last_run_status,expected",
    [
        (None, None, "", "", STALENESS_NEVER_SYNCED),
        (
            datetime.now(timezone.utc).isoformat(),
            None,
            "",
            "success",
            STALENESS_FRESH,
        ),
        (
            (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
            None,
            "",
            "success",
            STALENESS_STALE,
        ),
        (
            datetime.now(timezone.utc).isoformat(),
            datetime.now(timezone.utc).isoformat(),
            "api error",
            "failed",
            STALENESS_FAILED,
        ),
        (
            None,
            datetime.now(timezone.utc).isoformat(),
            "oauth blocked",
            "blocked",
            STALENESS_BLOCKED,
        ),
    ],
)
def test_staleness_transitions(
    last_success: str | None,
    last_attempt: str | None,
    last_error: str,
    last_run_status: str,
    expected: str,
) -> None:
    assert (
        compute_staleness_state(
            last_success_at=last_success,
            last_attempt_at=last_attempt,
            last_error=last_error,
            last_run_status=last_run_status,
        )
        == expected
    )


def test_calendar_imessage_photos_cursor_metadata_in_batch() -> None:
    cal = batch_summary_from_skip_details(
        {"skipped_unchanged_threads": 3},
        observed=5,
        updated=2,
    )
    assert cal.unchanged == 3
    assert cal.observed == 5
    imessage = batch_summary_from_skip_details({}, observed=1, updated=1)
    assert imessage.updated == 1
    photos = batch_summary_from_skip_details(
        {"skipped_unchanged_threads": 10},
        observed=10,
        unchanged=10,
    )
    assert photos.unchanged == 10


def test_status_read_does_not_invoke_adapter_fetch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)

    def _boom(*_a, **_k):
        raise AssertionError("adapter fetch must not run for status read")

    monkeypatch.setattr(
        "archive_sync.adapters.gmail_messages.GmailMessagesAdapter.fetch",
        _boom,
    )
    args = argparse.Namespace(
        vault=str(vault),
        instance_role="fixture",
        format="json",
        gmail_account="",
        calendar_account="",
        snapshot_cursors=False,
    )
    rc = cmd_status(args)
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert "sources" in out
    assert len(out["sources"]) >= 5


def test_status_missing_vault_returns_blocked_exit_4(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    monkeypatch.setenv("PPA_PATH", "/nonexistent/vault-section-d")
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    args = argparse.Namespace(
        vault="",
        instance_role="",
        format="json",
        gmail_account="",
        calendar_account="",
        snapshot_cursors=False,
    )
    rc = cmd_status(args)
    assert rc == EXIT_BLOCKED
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    payload = json.loads(captured.out)
    assert payload["blocked"] is True


def test_record_run_without_opt_in_returns_refused_exit_3(capsys: pytest.CaptureFixture) -> None:
    args = argparse.Namespace(
        require_gate_evidence=True,
        allow_live_record=False,
        source_key="gmail-messages:t@e.com",
        source_type="gmail",
        run_id="",
        status="success",
        vault="",
        instance_role="",
        format="json",
        ladder_gate="synthetic_fixtures",
        observed=0,
        unchanged=0,
        promoted=0,
        suppressed=0,
        quarantined=0,
        updated=0,
        side_effects_persisted=False,
    )
    rc = cmd_record_run(args)
    assert rc == EXIT_REFUSED
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    payload = json.loads(captured.out)
    assert payload["refused"] is True


def test_gmail_fixture_reports_promotion_counts_from_section_c_metrics() -> None:
    metrics = GmailPromotionBatchMetrics(observed=4, promoted=1, suppressed=2, quarantined=1)
    summary = batch_summary_from_skip_details(metrics.to_skip_details())
    assert summary.promoted == 1
    assert summary.suppressed == 2
    assert summary.quarantined == 1


def test_expand_declarations_includes_required_sources() -> None:
    decls = expand_declarations(
        gmail_accounts=("me@example.com",),
        calendar_accounts=("cal@example.com",),
    )
    keys = {d.source_key for d in decls}
    assert "gmail-messages:me@example.com" in keys
    assert "calendar-events:cal@example.com" in keys
    assert "gmail-correspondents:me@example.com" in keys
    assert "imessage:local" in keys
    assert "photos:local" in keys
    assert "file-libraries:documents" in keys
    assert "beeper:local" in keys
    assert "contacts:google" in keys
    assert "github-history:local" in keys
    assert "health:apple-health" in keys
    assert "otter-transcripts:me@example.com" not in keys

    with_otter = expand_declarations(otter_accounts=("me@example.com",))
    assert "otter-transcripts:me@example.com" in {d.source_key for d in with_otter}

"""Section D Phase 2 — source updater execution tests."""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from archive_sync.adapters.base import BaseAdapter, FetchedBatch, IngestResult, deterministic_provenance
from archive_sync.source_updaters.batch import commit_cursor_after_persisted
from archive_sync.source_updaters.constants import (
    RUN_STATUS_BLOCKED,
    RUN_STATUS_FAILED,
    RUN_STATUS_SUCCESS,
    SECTION_D_EXECUTION_STATE,
)
from archive_sync.source_updaters.runner import (
    adapter_ingest_kwargs,
    apply_max_items_kwarg,
    batch_summary_from_ingest,
    build_adapter,
    classify_run_exception,
    parse_source_key,
    resolve_declaration,
    run_source_updater,
    run_source_updaters,
)
from archive_sync.source_updaters.state_store import SourceUpdaterStateStore
from archive_vault.schema import CalendarEventCard
from archive_vault.sync_state import load_sync_state, update_cursor


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Calendar", "EmailThreads", "Documents", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "nicknames.json").write_text("{}", encoding="utf-8")
    return vault


class _FixtureCalendarAdapter(BaseAdapter):
    source_id = "calendar-events"
    enable_person_resolution = False
    preload_existing_uid_index = False

    def __init__(self, items: list[dict[str, Any]] | None = None, *, fail: Exception | None = None) -> None:
        self._items = list(items or [])
        self._fail = fail

    def get_cursor_key(self, **kwargs) -> str:
        account = str(kwargs.get("account_email", "")).strip().lower()
        return f"{self.source_id}:{account}:primary" if account else self.source_id

    def fetch(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> list[dict[str, Any]]:
        if self._fail is not None:
            raise self._fail
        return list(self._items)

    def fetch_batches(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> Iterable[FetchedBatch]:
        if self._fail is not None:
            raise self._fail
        patch = {"sync_token": "token-after", "event_etag": "etag-2"}
        yield FetchedBatch(items=list(self._items), cursor_patch=patch, sequence=0)

    def to_card(self, item: dict[str, Any]):
        card = CalendarEventCard(
            uid=str(item["uid"]),
            type="calendar_event",
            source=["calendar-events"],
            source_id=str(item.get("source_id", item["uid"])),
            created="2026-05-01",
            updated="2026-05-01",
            summary=str(item.get("title", "Event")),
            calendar_id="primary",
            event_id=str(item.get("event_id", item["uid"])),
            event_etag=str(item.get("event_etag", "etag-1")),
            title=str(item.get("title", "Event")),
            account_email=str(item.get("account_email", "cal@example.com")),
            start_at="2026-05-01T10:00:00Z",
            end_at="2026-05-01T11:00:00Z",
        )
        return card, deterministic_provenance(card, "calendar-events"), ""


class _FixtureGmailAdapter(BaseAdapter):
    """Minimal Gmail-shaped adapter for promotion batch accounting tests."""

    source_id = "gmail-messages"
    enable_person_resolution = False
    preload_existing_uid_index = False

    def __init__(
        self,
        items: list[dict[str, Any]] | None = None,
        *,
        skip_details: dict[str, int] | None = None,
        fail: Exception | None = None,
    ) -> None:
        self._items = list(items or [])
        self._skip_details = dict(skip_details or {})
        self._fail = fail

    def get_cursor_key(self, **kwargs) -> str:
        account = str(kwargs.get("account_email", "")).strip().lower()
        return f"{self.source_id}:{account}" if account else self.source_id

    def fetch(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> list[dict[str, Any]]:
        if self._fail is not None:
            raise self._fail
        return list(self._items)

    def fetch_batches(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> Iterable[FetchedBatch]:
        if self._fail is not None:
            raise self._fail
        yield FetchedBatch(
            items=list(self._items),
            cursor_patch={"gmail_history_id": "200"},
            sequence=0,
            skipped_count=sum(self._skip_details.values()),
            skip_details=dict(self._skip_details),
        )

    def to_card(self, item: dict[str, Any]):
        # Reuse calendar card path shape via CalendarEventCard is wrong for gmail;
        # use CalendarEventCard only if we must — better DocumentCard via type document.
        from archive_vault.schema import DocumentCard

        card = DocumentCard(
            uid=str(item["uid"]),
            type="document",
            source=["gmail-messages"],
            source_id=str(item.get("source_id", item["uid"])),
            created="2026-05-01",
            updated="2026-05-01",
            summary=str(item.get("subject", "Thread")),
            title=str(item.get("subject", "Thread")),
            content_sha=str(item.get("sha", "sha1")),
        )
        return card, deterministic_provenance(card, "gmail-messages"), ""


def test_parse_and_resolve_gmail_calendar_keys() -> None:
    assert parse_source_key("gmail-messages:me@example.com") == ("gmail-messages", "me@example.com")
    decl = resolve_declaration("gmail-messages:me@example.com")
    assert decl.source_type == "gmail"
    assert decl.default_active_policy == "promotion_gated"
    cal = resolve_declaration("calendar-events:cal@example.com")
    assert cal.source_type == "calendar"


def test_calendar_dry_run_does_not_advance_cursor(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    cursor_key = "calendar-events:cal@example.com:primary"
    update_cursor(vault, cursor_key, {"sync_token": "token-before"})
    adapter = _FixtureCalendarAdapter(
        [
            {
                "uid": "hfa-cal-uid-1",
                "event_id": "evt-1",
                "title": "Standup",
                "account_email": "cal@example.com",
            }
        ]
    )
    meta = vault / "_meta" / "source-updaters.json"
    store = SourceUpdaterStateStore(None, meta_path=meta)
    result = run_source_updater(
        source_key="calendar-events:cal@example.com",
        vault_path=vault,
        apply=False,
        adapter=adapter,
        state_store=store,
        repo_root=tmp_path,
        archive_instance="fixture:test",
    )
    assert result.report.status == RUN_STATUS_SUCCESS
    assert result.report.cursor_before.get("sync_token") == "token-before"
    assert result.report.cursor_after.get("sync_token") == "token-before"
    assert load_sync_state(vault)[cursor_key]["sync_token"] == "token-before"
    assert result.report.batch.promoted >= 1
    assert result.report.batch.dirty_card_uids_count >= 1


def test_calendar_adapter_apply_without_live_oauth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Real CalendarEventsAdapter apply path, Google calls stubbed."""

    from archive_sync.adapters.calendar_events import CalendarEventsAdapter

    vault = _minimal_vault(tmp_path)
    adapter = CalendarEventsAdapter()

    def fake_list(params, *, account_email=""):
        return {
            "items": [
                {
                    "id": "event-apply-1",
                    "etag": '"etag-apply-1"',
                    "iCalUID": "ical-apply-1",
                    "summary": "Staging Proof Meeting",
                    "start": {"dateTime": "2026-08-24T15:00:00Z"},
                    "end": {"dateTime": "2026-08-24T16:00:00Z"},
                    "organizer": {"email": "me@example.com", "displayName": "Me"},
                    "attendees": [{"email": "me@example.com"}],
                    "status": "confirmed",
                }
            ],
            "nextPageToken": None,
        }

    monkeypatch.setattr(adapter, "_ensure_token_manager", lambda *a, **k: None)
    monkeypatch.setattr(adapter, "_list_events", fake_list)

    result = run_source_updater(
        source_key="calendar-events:me@example.com",
        vault_path=vault,
        apply=True,
        adapter=adapter,
        repo_root=tmp_path,
        max_items=1,
        archive_instance="fixture:calendar-apply",
    )
    assert result.report.status == RUN_STATUS_SUCCESS
    assert result.report.batch.promoted == 1
    assert result.report.batch.dirty_card_uids_count >= 1
    assert result.report.cursor_after.get("emitted_events") == 1
    assert result.report.cursor_after.get("last_sync")
    cards = list((vault / "Calendar").rglob("*.md"))
    assert len(cards) == 1
    assert "Staging Proof Meeting" in cards[0].read_text(encoding="utf-8")


def test_calendar_apply_advances_cursor_after_persist(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    cursor_key = "calendar-events:cal@example.com:primary"
    update_cursor(vault, cursor_key, {"sync_token": "token-before"})
    adapter = _FixtureCalendarAdapter(
        [
            {
                "uid": "hfa-cal-uid-2",
                "event_id": "evt-2",
                "title": "Review",
                "account_email": "cal@example.com",
                "event_etag": "etag-new",
            }
        ]
    )
    result = run_source_updater(
        source_key="calendar-events:cal@example.com",
        vault_path=vault,
        apply=True,
        adapter=adapter,
        repo_root=tmp_path,
    )
    assert result.report.status == RUN_STATUS_SUCCESS
    assert result.report.cursor_after.get("sync_token") == "token-after"
    live = load_sync_state(vault)[cursor_key]
    assert live.get("sync_token") == "token-after"
    assert result.report.batch.dirty_card_uids
    dirty_path = Path(result.report.artifact_paths["dirty_uids"])
    assert dirty_path.is_file()
    assert "hfa-cal-uid-2" in dirty_path.read_text(encoding="utf-8")


def test_gmail_promotion_batch_counts_and_cursor_safety(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    cursor_key = "gmail-messages:me@example.com"
    update_cursor(vault, cursor_key, {"gmail_history_id": "100"})
    adapter = _FixtureGmailAdapter(
        items=[{"uid": "hfa-mail-uid-1", "subject": "Receipt", "sha": "abc"}],
        skip_details={
            "promotion_observed": 4,
            "promotion_promoted": 1,
            "promotion_suppressed": 2,
            "promotion_quarantined": 1,
        },
    )
    dry = run_source_updater(
        source_key="gmail-messages:me@example.com",
        vault_path=vault,
        apply=False,
        adapter=adapter,
        repo_root=tmp_path,
    )
    assert dry.report.batch.promoted == 1
    assert dry.report.batch.suppressed == 2
    assert dry.report.batch.quarantined == 1
    assert dry.report.cursor_after.get("gmail_history_id") == "100"

    adapter2 = _FixtureGmailAdapter(
        items=[{"uid": "hfa-mail-uid-2", "subject": "Receipt 2", "sha": "def"}],
        skip_details={
            "promotion_observed": 4,
            "promotion_promoted": 1,
            "promotion_suppressed": 2,
            "promotion_quarantined": 1,
        },
    )
    applied = run_source_updater(
        source_key="gmail-messages:me@example.com",
        vault_path=vault,
        apply=True,
        adapter=adapter2,
        repo_root=tmp_path,
    )
    assert applied.report.cursor_after.get("gmail_history_id") == "200"
    assert load_sync_state(vault)[cursor_key].get("gmail_history_id") == "200"


def test_one_source_failure_does_not_block_others(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    good = _FixtureCalendarAdapter(
        [{"uid": "hfa-ok-1", "event_id": "e1", "title": "Ok", "account_email": "a@x.com"}]
    )
    bad = _FixtureGmailAdapter(fail=PermissionError("oauth token revoked"))

    def factory(adapter_source_id: str) -> BaseAdapter:
        if adapter_source_id == "gmail-messages":
            return bad
        return good

    multi = run_source_updaters(
        source_keys=["gmail-messages:me@example.com", "calendar-events:a@x.com"],
        vault_path=vault,
        apply=False,
        adapter_factory=factory,
        repo_root=tmp_path,
    )
    assert len(multi.reports) == 2
    statuses = {r.source_key: r.status for r in multi.reports}
    assert statuses["gmail-messages:me@example.com"] == RUN_STATUS_BLOCKED
    assert statuses["calendar-events:a@x.com"] == RUN_STATUS_SUCCESS
    assert multi.exit_code == 4


def test_auth_failure_classified_blocked() -> None:
    assert classify_run_exception(PermissionError("oauth refresh failed")) == RUN_STATUS_BLOCKED
    assert classify_run_exception(RuntimeError("Token refresh failed: invalid_scope")) == RUN_STATUS_BLOCKED
    assert classify_run_exception(RuntimeError("HTTP Error 400: invalid_scope")) == RUN_STATUS_BLOCKED
    assert classify_run_exception(RuntimeError("network timeout")) == RUN_STATUS_FAILED


def test_cursor_patch_helper_still_gates_unpersisted() -> None:
    before = {"gmail_history_id": "1"}
    after = commit_cursor_after_persisted(
        side_effects_persisted=False,
        cursor_before=before,
        cursor_patch={"gmail_history_id": "2"},
    )
    assert after == before


def test_batch_summary_from_ingest_maps_created_to_promoted() -> None:
    result = IngestResult(created=3, merged=1, skipped=2)
    result.skip_details = {"skipped_unchanged_threads": 2}
    summary = batch_summary_from_ingest(result, dirty_card_uids=["a", "b"])
    assert summary.promoted == 3
    assert summary.updated == 1
    assert summary.unchanged == 2
    assert summary.dirty_card_uids_count == 2


def test_cli_run_missing_vault_exit_4(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    import argparse

    from archive_cli.source_updaters.cli import cmd_run
    from archive_cli.validation_gates.constants import EXIT_BLOCKED

    monkeypatch.setenv("PPA_PATH", "/nonexistent/vault-d2")
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    args = argparse.Namespace(
        apply=False,
        dry_run=True,
        source=["gmail-messages:me@example.com"],
        sources="",
        vault="",
        instance_role="",
        format="json",
        gmail_account="",
        calendar_account="",
        ladder_gate="synthetic_fixtures",
        run_id="",
        max_items=None,
        catch_up=False,
        reset_cursor=False,
        confirm_production=False,
    )
    rc = cmd_run(args)
    assert rc == EXIT_BLOCKED
    out = capsys.readouterr()
    assert "Traceback" not in out.err
    assert json.loads(out.out)["blocked"] is True


def test_execution_state_constant() -> None:
    assert SECTION_D_EXECUTION_STATE == "source_updater_execution_complete"


class _RecordingGmailAdapter(_FixtureGmailAdapter):
    def __init__(self) -> None:
        super().__init__(items=[])
        self.ingest_kwargs: dict[str, Any] = {}

    def ingest(self, vault_path: str, dry_run: bool = False, **kwargs: Any) -> IngestResult:
        self.ingest_kwargs = dict(kwargs)
        return IngestResult()


def test_gmail_catch_up_resets_cursor_and_keeps_promotion_gate(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    adapter = _RecordingGmailAdapter()
    result = run_source_updater(
        source_key="gmail-messages:me@example.com",
        vault_path=vault,
        apply=False,
        adapter=adapter,
        repo_root=tmp_path,
        catch_up=True,
    )
    assert adapter.ingest_kwargs.get("gmail_promotion_gate") is True
    assert adapter.ingest_kwargs.get("catch_up") is True
    assert adapter.ingest_kwargs.get("quick_update") is True
    assert "max_threads" not in adapter.ingest_kwargs
    assert "catch_up: gmail page cursor reset" in " ".join(result.report.warnings)
    assert "gmail_promotion_gate=true" in result.report.warnings


def test_gmail_uncapped_catch_up_does_not_disable_promotion_gate() -> None:
    decl = resolve_declaration("gmail-messages:me@example.com")
    kwargs = adapter_ingest_kwargs(decl, apply=True, catch_up=True)
    assert kwargs["gmail_promotion_gate"] is True
    assert kwargs["catch_up"] is True
    assert kwargs["quick_update"] is True


def test_gmail_catch_up_with_max_items_still_bounds_and_keeps_gate(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    adapter = _RecordingGmailAdapter()
    result = run_source_updater(
        source_key="gmail-messages:me@example.com",
        vault_path=vault,
        apply=False,
        adapter=adapter,
        repo_root=tmp_path,
        catch_up=True,
        max_items=25,
    )
    assert adapter.ingest_kwargs.get("gmail_promotion_gate") is True
    assert adapter.ingest_kwargs.get("catch_up") is True
    assert adapter.ingest_kwargs.get("max_threads") == 25
    warnings = result.report.warnings
    assert "gmail_promotion_gate=true" in warnings
    assert any("max_threads=25" in w for w in warnings)


def test_gmail_max_items_without_catch_up_still_keeps_promotion_gate(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    adapter = _RecordingGmailAdapter()
    run_source_updater(
        source_key="gmail-messages:me@example.com",
        vault_path=vault,
        apply=False,
        adapter=adapter,
        repo_root=tmp_path,
        max_items=10,
    )
    assert adapter.ingest_kwargs.get("gmail_promotion_gate") is True
    assert adapter.ingest_kwargs.get("max_threads") == 10
    assert adapter.ingest_kwargs.get("catch_up") is not True


_LIVE_ADAPTERS: list[tuple[str, str, str]] = [
    ("gmail-messages:me@example.com", "gmail-messages", "GmailMessagesAdapter"),
    ("calendar-events:cal@example.com", "calendar-events", "CalendarEventsAdapter"),
    ("imessage:local", "imessage", "IMessageAdapter"),
    ("otter-transcripts:me@example.com", "otter-transcripts", "OtterTranscriptsAdapter"),
    ("file-libraries:documents", "file-libraries", "FileLibrariesAdapter"),
    ("photos:apple-photos", "photos", "PhotosAdapter"),
    ("beeper:local", "beeper", "BeeperAdapter"),
    ("contacts:google", "contacts", "ContactsAdapter"),
    ("github-history:local", "github-history", "GitHubHistoryAdapter"),
    ("gmail-correspondents:me@example.com", "gmail-correspondents", "GmailCorrespondentsAdapter"),
]


@pytest.mark.parametrize("source_key,adapter_source_id,class_name", _LIVE_ADAPTERS)
def test_live_keys_resolve_and_build_adapter(source_key: str, adapter_source_id: str, class_name: str) -> None:
    decl = resolve_declaration(source_key)
    assert decl.adapter_source_id == adapter_source_id
    adapter = build_adapter(adapter_source_id)
    assert type(adapter).__name__ == class_name


@pytest.mark.parametrize(
    "source_key",
    [
        "copilot-finance:local",
        "linkedin:local",
        "notion-people:local",
        "notion-staff:local",
        "health:apple-health",
        "apple-health:apple-health",
        "medical-records:local",
        "contacts:apple",
        "contacts:vcf",
        "seed-people:local",
    ],
)
def test_export_keys_are_not_executable(source_key: str) -> None:
    with pytest.raises(ValueError, match="not executable"):
        resolve_declaration(source_key)


def test_adapter_ingest_kwargs_per_live_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IMESSAGE_SNAPSHOT_DIR", raising=False)
    monkeypatch.delenv("PPA_IMESSAGE_SNAPSHOT_DIR", raising=False)
    monkeypatch.delenv("PPA_GITHUB_STAGE_DIR", raising=False)
    monkeypatch.delenv("HFA_GITHUB_STAGE_DIR", raising=False)

    gmail = adapter_ingest_kwargs(resolve_declaration("gmail-messages:me@example.com"), apply=False)
    assert gmail == {"account_email": "me@example.com", "gmail_promotion_gate": True}

    calendar = adapter_ingest_kwargs(resolve_declaration("calendar-events:cal@example.com"), apply=False)
    assert calendar == {"account_email": "cal@example.com", "calendar_id": "primary"}

    imessage = adapter_ingest_kwargs(resolve_declaration("imessage:local"), apply=False)
    assert imessage == {"source_label": "local"}
    assert "account_email" not in imessage
    assert "snapshot_dir" not in imessage

    otter = adapter_ingest_kwargs(resolve_declaration("otter-transcripts:me@example.com"), apply=False)
    assert otter == {"account_email": "me@example.com"}

    documents = adapter_ingest_kwargs(resolve_declaration("file-libraries:documents"), apply=False)
    assert documents == {"roots": ["documents"]}
    assert "account_email" not in documents

    photos = adapter_ingest_kwargs(resolve_declaration("photos:apple-photos"), apply=False)
    assert photos == {"source_label": "apple-photos"}
    assert "account_email" not in photos

    beeper = adapter_ingest_kwargs(resolve_declaration("beeper:local"), apply=False)
    assert beeper["exclude_account_prefixes"] == ["imessage", "bluebubbles", "bluebubble"]
    assert "account_email" not in beeper

    contacts = adapter_ingest_kwargs(resolve_declaration("contacts:google"), apply=False)
    assert contacts == {"sources": ["google"]}
    assert "account_email" not in contacts
    assert contacts["sources"] == ["google"]

    github = adapter_ingest_kwargs(resolve_declaration("github-history:local"), apply=False)
    assert github == {}
    assert "account_email" not in github
    assert "stage_dir" not in github

    correspondents = adapter_ingest_kwargs(
        resolve_declaration("gmail-correspondents:me@example.com"), apply=False
    )
    assert correspondents == {"account_email": "me@example.com"}


def test_imessage_snapshot_dir_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_IMESSAGE_SNAPSHOT_DIR", "/tmp/imessage-snap")
    kwargs = adapter_ingest_kwargs(resolve_declaration("imessage:local"), apply=False)
    assert kwargs["snapshot_dir"] == "/tmp/imessage-snap"
    assert kwargs["source_label"] == "local"


def test_github_run_without_stage_dir_fails_contract(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_GITHUB_STAGE_DIR", raising=False)
    monkeypatch.delenv("HFA_GITHUB_STAGE_DIR", raising=False)
    vault = _minimal_vault(tmp_path)
    result = run_source_updater(
        source_key="github-history:local",
        vault_path=vault,
        apply=False,
        repo_root=tmp_path,
    )
    assert result.exit_hint == 1
    assert result.report.status == RUN_STATUS_FAILED
    assert any("stage-dir" in err for err in result.report.errors)


def test_github_stage_dir_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_GITHUB_STAGE_DIR", "/tmp/github-stage")
    kwargs = adapter_ingest_kwargs(resolve_declaration("github-history:local"), apply=False)
    assert kwargs["stage_dir"] == "/tmp/github-stage"


def test_github_stage_dir_from_cli_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_GITHUB_STAGE_DIR", "/tmp/github-stage-env")
    kwargs = adapter_ingest_kwargs(
        resolve_declaration("github-history:local"),
        apply=False,
        stage_dir="/tmp/github-stage-cli",
    )
    assert kwargs["stage_dir"] == "/tmp/github-stage-cli"


def test_github_stage_dir_cli_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_GITHUB_STAGE_DIR", raising=False)
    monkeypatch.delenv("HFA_GITHUB_STAGE_DIR", raising=False)
    kwargs = adapter_ingest_kwargs(
        resolve_declaration("github-history:local"),
        apply=False,
        stage_dir="~/Archive/github-stage",
    )
    assert kwargs["stage_dir"] == "~/Archive/github-stage"


def test_run_source_updater_passes_github_stage_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_GITHUB_STAGE_DIR", raising=False)
    monkeypatch.delenv("HFA_GITHUB_STAGE_DIR", raising=False)
    vault = _minimal_vault(tmp_path)
    adapter = _RecordingGithubAdapter()
    stage = str(tmp_path / "github-stage")
    run_source_updater(
        source_key="github-history:local",
        vault_path=vault,
        apply=False,
        adapter=adapter,
        repo_root=tmp_path,
        stage_dir=stage,
        max_items=25,
    )
    assert adapter.ingest_kwargs.get("stage_dir") == stage
    assert adapter.ingest_kwargs.get("max_items") == 25
    assert adapter.ingest_kwargs.get("catch_up") is not True


def test_source_updaters_run_parser_accepts_stage_dir() -> None:
    import argparse

    from archive_cli.source_updaters.cli import add_parser

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    add_parser(sub)
    args = parser.parse_args(
        [
            "source-updaters",
            "run",
            "--source",
            "github-history:local",
            "--apply",
            "--stage-dir",
            "/tmp/github-stage",
            "--max-items",
            "50",
        ]
    )
    assert args.stage_dir == "/tmp/github-stage"
    assert args.apply is True
    assert args.max_items == 50
    assert args.source == ["github-history:local"]
    assert getattr(args, "catch_up", False) is False


class _RecordingGithubAdapter(BaseAdapter):
    source_id = "github-history"
    enable_person_resolution = False
    preload_existing_uid_index = False

    def __init__(self) -> None:
        self.ingest_kwargs: dict[str, Any] = {}

    def fetch(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> list[dict[str, Any]]:
        return []

    def to_card(self, item: dict[str, Any]):
        raise NotImplementedError

    def ingest(self, vault_path: str, dry_run: bool = False, **kwargs: Any) -> IngestResult:
        self.ingest_kwargs = dict(kwargs)
        return IngestResult()


def test_max_items_maps_per_adapter() -> None:
    expected = {
        "gmail-messages": "max_threads",
        "calendar-events": "max_events",
        "imessage": "max_messages",
        "otter-transcripts": "max_meetings",
        "file-libraries": "max_files",
        "photos": "max_assets",
        "beeper": "max_threads",
        "github-history": "max_items",
        "gmail-correspondents": "max_messages",
        "contacts": "max_items",
    }
    for adapter_source_id, key in expected.items():
        mapped = apply_max_items_kwarg(adapter_source_id, {}, 12)
        assert mapped == {key: 12}


def test_build_adapter_refuses_export_ids() -> None:
    with pytest.raises(ValueError, match="No executable adapter"):
        build_adapter("apple-health")
    with pytest.raises(ValueError, match="No executable adapter"):
        build_adapter("copilot-finance")

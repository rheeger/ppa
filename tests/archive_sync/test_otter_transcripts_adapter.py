"""Archive-sync Otter transcript adapter tests."""

from __future__ import annotations

import json

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.otter_transcripts import (OtterApiClient, OtterMcpClient,
                                        OtterTranscriptsAdapter)
from hfa.schema import CalendarEventCard, MeetingTranscriptCard, PersonCard
from hfa.vault import read_note, write_card


class _FakeOtterClient:
    def __init__(self, pages: list[dict], details: dict[str, dict], transcripts: dict[str, dict]) -> None:
        self._pages = list(pages)
        self._details = details
        self._transcripts = transcripts
        self.list_calls = 0

    def list_meetings(self, *, page_size=25, page_token=None, updated_after=None, start_after=None, end_before=None):
        response = self._pages[self.list_calls]
        self.list_calls += 1
        return response

    def get_meeting_detail(self, meeting_id: str):
        return self._details[meeting_id]

    def get_transcript(self, meeting_id: str):
        return self._transcripts[meeting_id]


def _seed_person(tmp_vault):
    person = PersonCard(
        uid="hfa-person-robbie123456",
        type="person",
        source=["contacts.apple"],
        source_id="robbie@example.com",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Robbie Heeger",
        emails=["robbie@example.com"],
    )
    write_card(tmp_vault, "People/robbie-heeger.md", person, provenance=deterministic_provenance(person, "contacts.apple"))
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        '{\n  "email:robbie@example.com": "[[robbie-heeger]]"\n}',
        encoding="utf-8",
    )


def _seed_event(tmp_vault):
    event = CalendarEventCard(
        uid="hfa-calendar-event-123456",
        type="calendar_event",
        source=["google.calendar"],
        source_id="primary:event-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board Sync",
        account_email="robbie@example.com",
        calendar_id="primary",
        event_id="event-1",
        ical_uid="ical-1",
        title="Board Sync",
        start_at="2026-03-10T15:00:00Z",
        end_at="2026-03-10T16:00:00Z",
        conference_url="https://meet.google.com/abc-defg-hij",
        attendee_emails=["robbie@example.com"],
    )
    write_card(tmp_vault, "Calendar/2026-03/board-sync.md", event, provenance=deterministic_provenance(event, "google.calendar"))


def _read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_otter_api_client_uses_api_key_search_endpoints(monkeypatch):
    monkeypatch.setenv("OTTER_AI_API_KEY", "test-otter-key")
    monkeypatch.delenv("OTTER_API_CLIENT_ID", raising=False)
    monkeypatch.delenv("OTTER_API_CLIENT_SECRET", raising=False)
    client = OtterApiClient()
    calls = []

    def fake_request(path, *, params=None, allow_force_refresh=True):
        calls.append((path, params, allow_force_refresh))
        return {"speeches": []}

    client._request_json = fake_request  # type: ignore[method-assign]
    client.list_meetings(updated_after="2026-03-10T00:00:00Z", end_before="2026-03-11T00:00:00Z")
    client.get_meeting_detail("meeting-1")

    assert client.api_base_url == "https://otter.ai/forward/api/v1"
    assert calls[0][0] == "/speeches"
    assert calls[0][1]["created_after"] == "2026/03/10"
    assert calls[0][1]["created_before"] == "2026/03/11"
    assert calls[0][2] is False
    assert calls[1][0] == "/speeches/meeting-1"
    assert calls[1][2] is False
    monkeypatch.delenv("OTTER_AI_API_KEY", raising=False)


def test_otter_mcp_client_discovers_tools_and_calls_mcporter(monkeypatch):
    client = OtterMcpClient(mcporter_bin="/tmp/mcporter", server_name="otter_meeting_mcp")

    def fake_run_mcporter(*args):
        if args == ("list", "otter_meeting_mcp"):
            return "list_recent_meetings\nget_meeting\nget_transcript\n", ""
        if args[0] == "call":
            if args[1] == "otter_meeting_mcp.get_user_info":
                return json.dumps({"name": "Endaoment Admin", "email": "admin@endaoment.org"}), ""
            if args[1] == "otter_meeting_mcp.list_recent_meetings":
                return json.dumps({"meetings": [{"otid": "meeting-1"}]}), ""
            if args[1] == "otter_meeting_mcp.get_meeting":
                return json.dumps({"otid": "meeting-1", "title": "Board Sync"}), ""
            if args[1] == "otter_meeting_mcp.get_transcript":
                return json.dumps({"transcript": [{"speaker": "Robbie", "text": "hello"}]}), ""
        raise AssertionError(args)

    client._run_mcporter = fake_run_mcporter  # type: ignore[method-assign]

    meetings = client.list_meetings()
    detail = client.get_meeting_detail("meeting-1")
    transcript = client.get_transcript("meeting-1")

    assert meetings["meetings"][0]["otid"] == "meeting-1"
    assert detail["title"] == "Board Sync"
    assert transcript["transcript"][0]["text"] == "hello"


def test_build_client_prefers_mcp_when_requested(monkeypatch):
    monkeypatch.setenv("OTTER_FETCH_MODE", "mcp")
    adapter = OtterTranscriptsAdapter()
    client = adapter._build_client()
    assert isinstance(client, OtterMcpClient)
    monkeypatch.delenv("OTTER_FETCH_MODE", raising=False)


def test_build_client_prefers_mcp_when_mcporter_is_available(monkeypatch):
    monkeypatch.setenv("OTTER_AI_API_KEY", "legacy-api-key")
    monkeypatch.setenv("MCPORTER_CMD", "/tmp/mcporter")
    adapter = OtterTranscriptsAdapter()
    client = adapter._build_client()
    assert isinstance(client, OtterMcpClient)
    monkeypatch.delenv("OTTER_AI_API_KEY", raising=False)
    monkeypatch.delenv("MCPORTER_CMD", raising=False)


def test_fetch_supports_otid_rows_and_share_urls(tmp_vault):
    _seed_person(tmp_vault)
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    client = _FakeOtterClient(
        pages=[
            {
                "speeches": [
                    {
                        "otid": "meeting-1",
                        "title": "Board Sync",
                        "created_at": "2026-03-10T16:10:00Z",
                    }
                ]
            }
        ],
        details={
            "meeting-1": {
                "otid": "meeting-1",
                "title": "Board Sync",
                "created_at": "2026-03-10T15:00:00Z",
                "duration_seconds": 3600,
                "share_url": "https://otter.ai/u/meeting-1",
                "transcript": [
                    {
                        "speaker": "Robbie Heeger",
                        "text": "Let's review the board agenda.",
                        "start_time": 5,
                    }
                ],
            }
        },
        transcripts={"meeting-1": {"otid": "meeting-1", "share_url": "https://otter.ai/u/meeting-1", "transcript": []}},
    )
    adapter._build_client = lambda: client  # type: ignore[method-assign]

    items = adapter.fetch(str(tmp_vault), {}, account_email="robbie@example.com", max_meetings=10)
    transcript_items = [item for item in items if item.get("kind") == "meeting_transcript"]
    assert len(transcript_items) == 1
    assert transcript_items[0]["otter_meeting_id"] == "meeting-1"
    assert transcript_items[0]["meeting_url"] == "https://otter.ai/u/meeting-1"
    assert "Robbie Heeger" in transcript_items[0]["body"]


def test_fetch_hydrates_transcript_and_matches_calendar_event(tmp_vault):
    _seed_person(tmp_vault)
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    client = _FakeOtterClient(
        pages=[
            {
                "meetings": [
                    {
                        "id": "meeting-1",
                        "updated_at": "2026-03-10T16:10:00Z",
                    }
                ],
                "nextPageToken": None,
            }
        ],
        details={
            "meeting-1": {
                "title": "Board Sync",
                "status": "completed",
                "start_at": "2026-03-10T15:00:00Z",
                "end_at": "2026-03-10T16:00:00Z",
                "updated_at": "2026-03-10T16:10:00Z",
                "meeting_url": "https://otter.ai/u/meeting-1",
                "conference_url": "https://meet.google.com/abc-defg-hij?authuser=0",
                "event_id": "event-1",
                "participants": [{"name": "Robbie Heeger", "email": "robbie@example.com"}],
                "host": {"name": "Robbie Heeger", "email": "robbie@example.com"},
            }
        },
        transcripts={
            "meeting-1": {
                "summary": "Covered board prep.",
                "action_items": ["Send agenda"],
                "segments": [
                    {
                        "speaker_name": "Robbie Heeger",
                        "start_at": "2026-03-10T15:00:05Z",
                        "text": "Let's review the board agenda.",
                    }
                ],
            }
        },
    )
    adapter._build_client = lambda: client  # type: ignore[method-assign]

    items = adapter.fetch(str(tmp_vault), {}, account_email="robbie@example.com", max_meetings=10)
    transcript_items = [item for item in items if item.get("kind") == "meeting_transcript"]
    backlink_items = [item for item in items if item.get("kind") == "calendar_backlink"]
    assert len(transcript_items) == 1
    assert len(backlink_items) == 1
    assert transcript_items[0]["calendar_events"] == ["[[board-sync]]"]
    assert transcript_items[0]["people"] == ["[[robbie-heeger]]"]
    assert transcript_items[0]["speaker_emails"] == []
    assert "## Transcript" in transcript_items[0]["body"]


def test_stage_transcripts_writes_manifest_and_stage_files(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    stage_dir = tmp_path / "otter-stage"
    client = _FakeOtterClient(
        pages=[{"meetings": [{"id": "meeting-1"}], "nextPageToken": None}],
        details={
            "meeting-1": {
                "title": "Board Sync",
                "status": "completed",
                "start_at": "2026-03-10T15:00:00Z",
                "end_at": "2026-03-10T16:00:00Z",
                "updated_at": "2026-03-10T16:10:00Z",
                "meeting_url": "https://otter.ai/u/meeting-1",
                "transcript_url": "https://otter.ai/u/meeting-1/transcript",
                "conference_url": "https://meet.google.com/abc-defg-hij",
                "ical_uid": "ical-1",
                "participants": [{"name": "Robbie Heeger", "email": "robbie@example.com"}],
            }
        },
        transcripts={
            "meeting-1": {
                "summary": "Covered board prep.",
                "actionItems": [{"text": "Send agenda"}],
                "segments": [
                    {
                        "speaker_name": "Robbie Heeger",
                        "start_at": "2026-03-10T15:00:05Z",
                        "text": "Let's review the board agenda.",
                    }
                ],
            }
        },
    )
    adapter._build_client = lambda: client  # type: ignore[method-assign]

    manifest = adapter.stage_transcripts(
        str(tmp_vault),
        stage_dir,
        account_email="robbie@example.com",
        max_meetings=10,
    )
    assert manifest["counts"]["meetings"] == 1
    assert (stage_dir / "manifest.json").exists()
    assert (stage_dir / "meetings.jsonl").exists()
    assert (stage_dir / "_meta" / "extract-state.json").exists()
    rows = _read_jsonl(stage_dir / "meetings.jsonl")
    assert len(rows) == 1
    assert rows[0]["kind"] == "meeting_transcript"
    assert rows[0]["calendar_events"] == ["[[board-sync]]"]
    assert not (tmp_vault / "MeetingTranscripts").exists()


def test_import_stage_writes_meeting_transcript_note_and_event_backlink(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    stage_dir = tmp_path / "otter-stage"
    client = _FakeOtterClient(
        pages=[{"meetings": [{"id": "meeting-1"}], "nextPageToken": None}],
        details={
            "meeting-1": {
                "title": "Board Sync",
                "status": "completed",
                "start_at": "2026-03-10T15:00:00Z",
                "end_at": "2026-03-10T16:00:00Z",
                "updated_at": "2026-03-10T16:10:00Z",
                "meeting_url": "https://otter.ai/u/meeting-1",
                "transcript_url": "https://otter.ai/u/meeting-1/transcript",
                "conference_url": "https://meet.google.com/abc-defg-hij",
                "ical_uid": "ical-1",
                "participants": [{"name": "Robbie Heeger", "email": "robbie@example.com"}],
            }
        },
        transcripts={
            "meeting-1": {
                "summary": "Covered board prep.",
                "actionItems": [{"text": "Send agenda"}],
                "segments": [
                    {
                        "speaker_name": "Robbie Heeger",
                        "start_at": "2026-03-10T15:00:05Z",
                        "text": "Let's review the board agenda.",
                    }
                ],
            }
        },
    )
    adapter._build_client = lambda: client  # type: ignore[method-assign]
    adapter.stage_transcripts(str(tmp_vault), stage_dir, account_email="robbie@example.com", max_meetings=10)

    result = adapter.ingest(str(tmp_vault), stage_dir=str(stage_dir))
    assert result.created == 1
    assert result.merged == 1

    transcript_rel = next((tmp_vault / "MeetingTranscripts").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, body, _ = read_note(tmp_vault, str(transcript_rel))
    assert frontmatter["type"] == "meeting_transcript"
    assert frontmatter["otter_meeting_id"] == "meeting-1"
    assert frontmatter["calendar_events"] == ["[[board-sync]]"]
    assert frontmatter["transcript_body_sha"]
    assert "## Summary" in body
    assert "## Action Items" in body
    assert "## Transcript" in body

    event_frontmatter, _, _ = read_note(tmp_vault, "Calendar/2026-03/board-sync.md")
    assert event_frontmatter["meeting_transcripts"] == [f"[[{frontmatter['uid']}]]"]


def test_import_stage_matches_calendar_event_by_title_and_start_time(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    stage_dir = tmp_path / "otter-stage"
    client = _FakeOtterClient(
        pages=[{"meetings": [{"id": "meeting-1"}], "nextPageToken": None}],
        details={
            "meeting-1": {
                "title": "Board Sync",
                "status": "completed",
                "start_at": "2026-03-10T15:00:00Z",
                "end_at": "2026-03-10T16:00:00Z",
                "updated_at": "2026-03-10T16:10:00Z",
                "meeting_url": "https://otter.ai/u/meeting-1",
                "conference_url": "https://otter.ai/u/meeting-1",
                "participants": [{"name": "Robbie Heeger", "email": "robbie@example.com"}],
                "host": {"name": "Robbie Heeger", "email": "robbie@example.com"},
            }
        },
        transcripts={
            "meeting-1": {
                "summary": "Covered board prep.",
                "actionItems": [{"text": "Send agenda"}],
                "segments": [
                    {
                        "speaker_name": "Robbie Heeger",
                        "start_at": "2026-03-10T15:00:05Z",
                        "text": "Let's review the board agenda.",
                    }
                ],
            }
        },
    )
    adapter._build_client = lambda: client  # type: ignore[method-assign]

    adapter.stage_transcripts(str(tmp_vault), stage_dir, account_email="robbie@example.com", max_meetings=10)
    rows = _read_jsonl(stage_dir / "meetings.jsonl")
    assert rows[0]["calendar_events"] == ["[[board-sync]]"]

    result = adapter.ingest(str(tmp_vault), stage_dir=str(stage_dir))
    assert result.created == 1
    assert result.merged == 1

    event_frontmatter, _, _ = read_note(tmp_vault, "Calendar/2026-03/board-sync.md")
    assert event_frontmatter["meeting_transcripts"]


def test_import_stage_rematches_existing_rows_without_calendar_links(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    stage_dir = tmp_path / "otter-stage"
    stage_dir.mkdir()
    (stage_dir / "_meta").mkdir()
    (stage_dir / "meetings.jsonl").write_text(
        json.dumps(
            {
                "kind": "meeting_transcript",
                "otter_meeting_id": "meeting-1",
                "title": "Board Sync",
                "summary": "Board Sync",
                "start_at": "2026-03-10T15:00:00Z",
                "end_at": "2026-03-10T16:00:00Z",
                "conference_url": "https://otter.ai/u/meeting-1",
                "participant_emails": ["robbie@example.com"],
                "speaker_emails": [],
                "host_email": "robbie@example.com",
                "calendar_events": [],
                "body": "## Transcript\n\nhello",
                "created": "2026-03-10",
                "updated": "2026-03-10",
                "transcript_body_sha": "abc123",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = adapter.ingest(str(tmp_vault), stage_dir=str(stage_dir))
    assert result.created == 1
    assert result.merged == 1

    transcript_rel = next((tmp_vault / "MeetingTranscripts").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, _, _ = read_note(tmp_vault, str(transcript_rel))
    assert frontmatter["calendar_events"] == ["[[board-sync]]"]

    event_frontmatter, _, _ = read_note(tmp_vault, "Calendar/2026-03/board-sync.md")
    assert event_frontmatter["meeting_transcripts"] == [f"[[{frontmatter['uid']}]]"]


def test_relink_stage_updates_existing_transcript_and_calendar_event(tmp_vault, tmp_path):
    _seed_person(tmp_vault)
    _seed_event(tmp_vault)
    transcript = MeetingTranscriptCard(
        uid="hfa-meeting-transcript-abc123def456",
        type="meeting_transcript",
        source=["otter.meeting"],
        source_id="meeting-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board Sync",
        otter_meeting_id="meeting-1",
        title="Board Sync",
        start_at="2026-03-10T15:00:00Z",
        end_at="2026-03-10T16:00:00Z",
        conference_url="https://otter.ai/u/meeting-1",
        participant_emails=["robbie@example.com"],
        transcript_body_sha="abc123",
    )
    write_card(
        tmp_vault,
        "MeetingTranscripts/2026-03/hfa-meeting-transcript-abc123def456.md",
        transcript,
        provenance=deterministic_provenance(transcript, "otter.meeting"),
    )

    stage_dir = tmp_path / "otter-stage"
    stage_dir.mkdir()
    (stage_dir / "meetings.jsonl").write_text(
        json.dumps(
            {
                "kind": "meeting_transcript",
                "otter_meeting_id": "meeting-1",
                "title": "Board Sync",
                "summary": "Board Sync",
                "start_at": "2026-03-10T15:00:00Z",
                "conference_url": "https://otter.ai/u/meeting-1",
                "participant_emails": ["robbie@example.com"],
                "calendar_events": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    adapter = OtterTranscriptsAdapter()
    stats = adapter.relink_stage(str(tmp_vault), stage_dir)
    assert stats["matches_found"] == 1
    assert stats["transcripts_updated"] == 1
    assert stats["calendar_events_updated"] == 1

    transcript_frontmatter, _, _ = read_note(tmp_vault, "MeetingTranscripts/2026-03/hfa-meeting-transcript-abc123def456.md")
    assert transcript_frontmatter["calendar_events"] == ["[[board-sync]]"]

    event_frontmatter, _, _ = read_note(tmp_vault, "Calendar/2026-03/board-sync.md")
    assert event_frontmatter["meeting_transcripts"] == ["[[hfa-meeting-transcript-abc123def456]]"]


def test_relink_existing_links_duplicate_multi_account_calendar_events(tmp_vault):
    _seed_person(tmp_vault)
    personal_event = CalendarEventCard(
        uid="hfa-calendar-event-personal",
        type="calendar_event",
        source=["calendar.event"],
        source_id="rheeger@gmail.com:primary:event-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board Sync",
        account_email="rheeger@gmail.com",
        calendar_id="primary",
        event_id="event-1",
        ical_uid="ical-1",
        title="Board Sync",
        start_at="2026-03-10T15:00:00Z",
        end_at="2026-03-10T16:00:00Z",
        attendee_emails=["robbie@example.com"],
    )
    work_event = CalendarEventCard(
        uid="hfa-calendar-event-work",
        type="calendar_event",
        source=["calendar.event"],
        source_id="robbie@endaoment.org:primary:event-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board Sync",
        account_email="robbie@endaoment.org",
        calendar_id="primary",
        event_id="event-1",
        ical_uid="ical-1",
        title="Board Sync",
        start_at="2026-03-10T15:00:00Z",
        end_at="2026-03-10T16:00:00Z",
        attendee_emails=["robbie@example.com"],
    )
    transcript = MeetingTranscriptCard(
        uid="hfa-meeting-transcript-abc123def456",
        type="meeting_transcript",
        source=["otter.meeting"],
        source_id="meeting-1",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Board Sync",
        otter_meeting_id="meeting-1",
        title="Board Sync",
        start_at="2026-03-10T15:00:00Z",
        end_at="2026-03-10T16:00:00Z",
        ical_uid="ical-1",
        participant_emails=["robbie@example.com"],
        transcript_body_sha="abc123",
    )
    write_card(
        tmp_vault,
        "Calendar/2026-03/board-sync-personal.md",
        personal_event,
        provenance=deterministic_provenance(personal_event, "calendar.event"),
    )
    write_card(
        tmp_vault,
        "Calendar/2026-03/board-sync-work.md",
        work_event,
        provenance=deterministic_provenance(work_event, "calendar.event"),
    )
    write_card(
        tmp_vault,
        "MeetingTranscripts/2026-03/hfa-meeting-transcript-abc123def456.md",
        transcript,
        provenance=deterministic_provenance(transcript, "otter.meeting"),
    )

    adapter = OtterTranscriptsAdapter()
    stats = adapter.relink_existing(str(tmp_vault))

    assert stats["matches_found"] == 1
    assert stats["transcripts_updated"] == 1
    assert stats["calendar_events_updated"] == 2

    transcript_frontmatter, _, _ = read_note(tmp_vault, "MeetingTranscripts/2026-03/hfa-meeting-transcript-abc123def456.md")
    assert transcript_frontmatter["calendar_events"] == ["[[board-sync-personal]]", "[[board-sync-work]]"]

    personal_frontmatter, _, _ = read_note(tmp_vault, "Calendar/2026-03/board-sync-personal.md")
    work_frontmatter, _, _ = read_note(tmp_vault, "Calendar/2026-03/board-sync-work.md")
    assert personal_frontmatter["meeting_transcripts"] == ["[[hfa-meeting-transcript-abc123def456]]"]
    assert work_frontmatter["meeting_transcripts"] == ["[[hfa-meeting-transcript-abc123def456]]"]


def test_stage_transcripts_resumes_from_extract_state(tmp_vault, tmp_path):
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    stage_dir = tmp_path / "otter-stage"
    initial_client = _FakeOtterClient(
        pages=[
            {"meetings": [{"id": "meeting-1", "updated_at": "2026-03-10T16:10:00Z"}], "nextPageToken": "page-2"},
            {"meetings": [{"id": "meeting-2", "updated_at": "2026-03-11T16:10:00Z"}], "nextPageToken": None},
        ],
        details={
            "meeting-1": {
                "title": "Board Sync",
                "status": "completed",
                "start_at": "2026-03-10T15:00:00Z",
                "end_at": "2026-03-10T16:00:00Z",
                "updated_at": "2026-03-10T16:10:00Z",
            },
            "meeting-2": {
                "title": "Team Sync",
                "status": "completed",
                "start_at": "2026-03-11T15:00:00Z",
                "end_at": "2026-03-11T16:00:00Z",
                "updated_at": "2026-03-11T16:10:00Z",
            },
        },
        transcripts={
            "meeting-1": {"segments": [{"speaker_name": "Robbie Heeger", "start_at": "2026-03-10T15:00:05Z", "text": "one"}]},
            "meeting-2": {"segments": [{"speaker_name": "Robbie Heeger", "start_at": "2026-03-11T15:00:05Z", "text": "two"}]},
        },
    )
    adapter._build_client = lambda: initial_client  # type: ignore[method-assign]
    manifest = adapter.stage_transcripts(str(tmp_vault), stage_dir, max_meetings=1)
    assert manifest["counts"]["meetings"] == 1

    state = json.loads((stage_dir / "_meta" / "extract-state.json").read_text(encoding="utf-8"))
    assert state["complete"] is False
    assert state["page_token"] == "page-2"

    second_adapter = OtterTranscriptsAdapter()
    resume_client = _FakeOtterClient(
        pages=[{"meetings": [{"id": "meeting-2", "updated_at": "2026-03-11T16:10:00Z"}], "nextPageToken": None}],
        details={
            "meeting-2": {
                "title": "Team Sync",
                "status": "completed",
                "start_at": "2026-03-11T15:00:00Z",
                "end_at": "2026-03-11T16:00:00Z",
                "updated_at": "2026-03-11T16:10:00Z",
            }
        },
        transcripts={
            "meeting-2": {"segments": [{"speaker_name": "Robbie Heeger", "start_at": "2026-03-11T15:00:05Z", "text": "two"}]}
        },
    )
    second_adapter._build_client = lambda: resume_client  # type: ignore[method-assign]
    manifest = second_adapter.stage_transcripts(str(tmp_vault), stage_dir)
    assert manifest["counts"]["meetings"] == 2
    rows = _read_jsonl(stage_dir / "meetings.jsonl")
    assert [row["otter_meeting_id"] for row in rows] == ["meeting-1", "meeting-2"]


def test_stage_transcripts_walks_mcp_day_windows_backward(tmp_vault, tmp_path):
    class _FakeMcpClient(OtterMcpClient):
        def __init__(self):
            pass

    client = _FakeMcpClient()
    calls = []
    pages = {
        "2026-03-11": {"results": [{"id": "meeting-1", "title": "Day One"}]},
        "2026-03-10": {"results": [{"id": "meeting-2", "title": "Day Two"}]},
        "2026-03-09": {"results": []},
    }

    def list_meetings(*, page_size=25, page_token=None, updated_after=None, start_after=None, end_before=None):
        calls.append((updated_after, start_after, end_before))
        return pages[updated_after]

    client.list_meetings = list_meetings  # type: ignore[method-assign]
    client.get_meeting_detail = lambda meeting_id: {"id": meeting_id, "title": meeting_id, "start_at": "2026-03-10T15:00:00Z"}  # type: ignore[method-assign]
    client.get_transcript = lambda meeting_id: {"transcript": [{"speaker": "Robbie", "text": meeting_id, "start_time": 5}]}  # type: ignore[method-assign]

    adapter = OtterTranscriptsAdapter()
    adapter._build_client = lambda: client  # type: ignore[method-assign]
    stage_dir = tmp_path / "otter-stage"

    manifest = adapter.stage_transcripts(
        str(tmp_vault),
        stage_dir,
        updated_after="2026-03-09",
        end_before="2026-03-11",
        max_meetings=10,
    )

    assert manifest["counts"]["meetings"] == 2
    assert calls == [
        ("2026-03-11", "2026-03-11", "2026-03-11"),
        ("2026-03-10", "2026-03-10", "2026-03-10"),
        ("2026-03-09", "2026-03-09", "2026-03-09"),
    ]


def test_quick_update_skips_unchanged_meetings(tmp_vault):
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    initial_client = _FakeOtterClient(
        pages=[{"meetings": [{"id": "meeting-1", "updated_at": "2026-03-10T16:10:00Z"}], "nextPageToken": None}],
        details={
            "meeting-1": {
                "title": "Board Sync",
                "status": "completed",
                "start_at": "2026-03-10T15:00:00Z",
                "end_at": "2026-03-10T16:00:00Z",
                "updated_at": "2026-03-10T16:10:00Z",
            }
        },
        transcripts={
            "meeting-1": {
                "segments": [
                    {
                        "speaker_name": "Robbie Heeger",
                        "start_at": "2026-03-10T15:00:05Z",
                        "text": "Let's review the board agenda.",
                    }
                ]
            }
        },
    )
    adapter._build_client = lambda: initial_client  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="robbie@example.com", max_meetings=10)
    assert result.created == 1

    second_adapter = OtterTranscriptsAdapter()
    unchanged_client = _FakeOtterClient(
        pages=[{"meetings": [{"id": "meeting-1", "updated_at": "2026-03-10T16:10:00Z"}], "nextPageToken": None}],
        details={},
        transcripts={},
    )
    second_adapter._build_client = lambda: unchanged_client  # type: ignore[method-assign]

    result = second_adapter.ingest(
        str(tmp_vault),
        account_email="robbie@example.com",
        max_meetings=10,
        quick_update=True,
    )
    assert result.created == 0
    assert result.merged == 0
    assert result.skipped == 1
    assert result.skip_details["skipped_unchanged_meetings"] == 1


def test_to_card_returns_meeting_transcript_card():
    adapter = OtterTranscriptsAdapter()
    card, _, body = adapter.to_card(
        {
            "kind": "meeting_transcript",
            "otter_meeting_id": "meeting-1",
            "title": "Board Sync",
            "start_at": "2026-03-10T15:00:00Z",
            "calendar_events": ["[[board-sync]]"],
            "transcript_body_sha": "abc123",
            "body": "## Transcript\n\nHello",
        }
    )
    assert isinstance(card, MeetingTranscriptCard)
    assert card.title == "Board Sync"
    assert card.calendar_events == ["[[board-sync]]"]
    assert body == "## Transcript\n\nHello"


def test_to_card_returns_calendar_event_card_for_backlink(tmp_vault):
    _seed_event(tmp_vault)
    adapter = OtterTranscriptsAdapter()
    frontmatter, _, _ = read_note(tmp_vault, "Calendar/2026-03/board-sync.md")
    card, provenance, body = adapter.to_card(
        {
            "kind": "calendar_backlink",
            "frontmatter": frontmatter,
            "meeting_transcript_ref": "[[hfa-meeting-transcript-abc123def456]]",
        }
    )
    assert isinstance(card, CalendarEventCard)
    assert card.meeting_transcripts == ["[[hfa-meeting-transcript-abc123def456]]"]
    assert provenance["meeting_transcripts"].source == "otter.meeting"
    assert body == ""

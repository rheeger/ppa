"""Archive-sync Calendar event adapter tests."""

from __future__ import annotations

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.calendar_events import CalendarEventsAdapter, _event_uid
from hfa.schema import CalendarEventCard, EmailMessageCard, EmailThreadCard
from hfa.vault import read_note, write_card


def test_fetch_links_calendar_events_to_existing_email_cards(tmp_vault):
    thread = EmailThreadCard(
        uid="hfa-email-thread-111111111111",
        type="email_thread",
        source=["gmail.thread"],
        source_id="me@example.com:thread-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting thread",
        gmail_thread_id="thread-1",
        account_email="me@example.com",
        invite_ical_uids=["event-uid-1"],
        invite_event_id_hints=["event-google-1"],
    )
    message = EmailMessageCard(
        uid="hfa-email-message-111111111111",
        type="email_message",
        source=["gmail.message"],
        source_id="me@example.com:message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting invite",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        account_email="me@example.com",
        invite_ical_uid="event-uid-1",
        invite_event_id_hint="event-google-1",
    )
    write_card(tmp_vault, "EmailThreads/2026-03/hfa-email-thread-111111111111.md", thread, provenance=deterministic_provenance(thread, "gmail.thread"))
    write_card(tmp_vault, "Email/2026-03/hfa-email-message-111111111111.md", message, provenance=deterministic_provenance(message, "gmail.message"))

    adapter = CalendarEventsAdapter()
    responses = iter(
        [
            {
                "items": [
                    {
                        "id": "event-google-1",
                        "iCalUID": "event-uid-1",
                        "summary": "Board Meeting",
                        "start": {"dateTime": "2026-03-08T15:00:00Z"},
                        "end": {"dateTime": "2026-03-08T16:00:00Z"},
                        "organizer": {"email": "alice@example.com", "displayName": "Alice Example"},
                        "attendees": [{"email": "me@example.com"}],
                        "status": "confirmed",
                    }
                ],
                "nextPageToken": None,
            }
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]

    items = adapter.fetch(str(tmp_vault), {}, account_email="me@example.com", max_events=10)
    assert items[0]["source_messages"] == ["[[hfa-email-message-111111111111]]"]
    assert items[0]["source_threads"] == ["[[hfa-email-thread-111111111111]]"]


def test_to_card_returns_calendar_event_card():
    adapter = CalendarEventsAdapter()
    card, _, _ = adapter.to_card(
        {
            "event_id": "event-google-1",
            "calendar_id": "primary",
            "account_email": "me@example.com",
            "title": "Board Meeting",
            "start_at": "2026-03-08T15:00:00Z",
            "end_at": "2026-03-08T16:00:00Z",
            "source_messages": ["[[hfa-email-message-111111111111]]"],
        }
    )
    assert isinstance(card, CalendarEventCard)
    assert card.title == "Board Meeting"
    assert card.source_messages == ["[[hfa-email-message-111111111111]]"]
    assert card.uid == _event_uid("me@example.com", "primary", "event-google-1")
    assert card.source_id == "me@example.com:primary:event-google-1"


def test_fetch_links_calendar_events_only_to_same_account_invites(tmp_vault):
    personal_thread = EmailThreadCard(
        uid="hfa-email-thread-personal",
        type="email_thread",
        source=["gmail.thread"],
        source_id="one@example.com:thread-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting thread",
        gmail_thread_id="thread-1",
        account_email="one@example.com",
        invite_ical_uids=["event-uid-1"],
        invite_event_id_hints=["event-google-1"],
    )
    personal_message = EmailMessageCard(
        uid="hfa-email-message-personal",
        type="email_message",
        source=["gmail.message"],
        source_id="one@example.com:message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting invite",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        account_email="one@example.com",
        invite_ical_uid="event-uid-1",
        invite_event_id_hint="event-google-1",
    )
    work_thread = EmailThreadCard(
        uid="hfa-email-thread-work",
        type="email_thread",
        source=["gmail.thread"],
        source_id="two@example.com:thread-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting thread",
        gmail_thread_id="thread-1",
        account_email="two@example.com",
        invite_ical_uids=["event-uid-1"],
        invite_event_id_hints=["event-google-1"],
    )
    work_message = EmailMessageCard(
        uid="hfa-email-message-work",
        type="email_message",
        source=["gmail.message"],
        source_id="two@example.com:message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting invite",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        account_email="two@example.com",
        invite_ical_uid="event-uid-1",
        invite_event_id_hint="event-google-1",
    )
    write_card(
        tmp_vault,
        "EmailThreads/2026-03/hfa-email-thread-personal.md",
        personal_thread,
        provenance=deterministic_provenance(personal_thread, "gmail.thread"),
    )
    write_card(
        tmp_vault,
        "Email/2026-03/hfa-email-message-personal.md",
        personal_message,
        provenance=deterministic_provenance(personal_message, "gmail.message"),
    )
    write_card(
        tmp_vault,
        "EmailThreads/2026-03/hfa-email-thread-work.md",
        work_thread,
        provenance=deterministic_provenance(work_thread, "gmail.thread"),
    )
    write_card(
        tmp_vault,
        "Email/2026-03/hfa-email-message-work.md",
        work_message,
        provenance=deterministic_provenance(work_message, "gmail.message"),
    )

    adapter = CalendarEventsAdapter()
    responses = iter(
        [
            {
                "items": [
                    {
                        "id": "event-google-1",
                        "iCalUID": "event-uid-1",
                        "summary": "Board Meeting",
                        "start": {"dateTime": "2026-03-08T15:00:00Z"},
                        "end": {"dateTime": "2026-03-08T16:00:00Z"},
                        "organizer": {"email": "alice@example.com", "displayName": "Alice Example"},
                        "attendees": [{"email": "one@example.com"}],
                        "status": "confirmed",
                    }
                ],
                "nextPageToken": None,
            }
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]

    items = adapter.fetch(str(tmp_vault), {}, account_email="one@example.com", max_events=10)
    assert items[0]["source_messages"] == ["[[hfa-email-message-personal]]"]
    assert items[0]["source_threads"] == ["[[hfa-email-thread-personal]]"]


def test_fetch_falls_back_to_http_when_gws_calendar_project_is_not_enabled(tmp_vault):
    adapter = CalendarEventsAdapter()
    adapter._gws = lambda args: (_ for _ in ()).throw(RuntimeError('{"error":{"reason":"accessNotConfigured"}}'))  # type: ignore[method-assign]
    adapter._calendar_events_list_http = lambda params: {  # type: ignore[method-assign]
        "items": [
            {
                "id": "event-google-1",
                "iCalUID": "event-uid-1",
                "summary": "Board Meeting",
                "start": {"dateTime": "2026-03-08T15:00:00Z"},
                "end": {"dateTime": "2026-03-08T16:00:00Z"},
                "organizer": {"email": "alice@example.com", "displayName": "Alice Example"},
                "attendees": [{"email": "me@example.com"}],
                "status": "confirmed",
            }
        ],
        "nextPageToken": None,
    }

    items = adapter.fetch(str(tmp_vault), {}, account_email="me@example.com", max_events=10)
    assert items[0]["event_id"] == "event-google-1"
    assert items[0]["title"] == "Board Meeting"


def test_fetch_falls_back_to_http_when_gws_calendar_project_permission_is_forbidden(tmp_vault):
    adapter = CalendarEventsAdapter()
    adapter._gws = lambda args: (_ for _ in ()).throw(  # type: ignore[method-assign]
        RuntimeError(
            '{"error":{"code":403,"reason":"forbidden","message":"Caller does not have required permission to use project claude-gmail-mcp-485803. Grant the caller the roles/serviceusage.serviceUsageConsumer role."}}'
        )
    )
    adapter._calendar_events_list_http = lambda params: {  # type: ignore[method-assign]
        "items": [
            {
                "id": "event-google-1",
                "iCalUID": "event-uid-1",
                "summary": "Board Meeting",
                "start": {"dateTime": "2026-03-08T15:00:00Z"},
                "end": {"dateTime": "2026-03-08T16:00:00Z"},
                "organizer": {"email": "alice@example.com", "displayName": "Alice Example"},
                "attendees": [{"email": "me@example.com"}],
                "status": "confirmed",
            }
        ],
        "nextPageToken": None,
    }

    items = adapter.fetch(str(tmp_vault), {}, account_email="me@example.com", max_events=10)
    assert items[0]["event_id"] == "event-google-1"
    assert items[0]["title"] == "Board Meeting"


def test_ingest_writes_calendar_event_note(tmp_vault):
    adapter = CalendarEventsAdapter()
    responses = iter(
        [
            {
                "items": [
                    {
                        "id": "event-google-1",
                        "etag": "\"etag-1\"",
                        "iCalUID": "event-uid-1",
                        "summary": "Board Meeting",
                        "description": "Quarterly review",
                        "location": "Zoom",
                        "start": {"dateTime": "2026-03-08T15:00:00Z"},
                        "end": {"dateTime": "2026-03-08T16:00:00Z"},
                        "organizer": {"email": "alice@example.com", "displayName": "Alice Example"},
                        "attendees": [{"email": "me@example.com"}],
                        "status": "confirmed",
                        "hangoutLink": "https://meet.google.com/example",
                    }
                ],
                "nextPageToken": None,
            }
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]

    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_events=10)
    assert result.created == 1

    event_rel = next((tmp_vault / "Calendar").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, _, _ = read_note(tmp_vault, str(event_rel))
    assert frontmatter["title"] == "Board Meeting"
    assert frontmatter["conference_url"] == "https://meet.google.com/example"
    assert frontmatter["event_etag"]
    assert frontmatter["event_body_sha"]


def test_quick_update_skips_unchanged_events(tmp_vault):
    adapter = CalendarEventsAdapter()
    initial = iter(
        [
            {
                "items": [
                    {
                        "id": "event-google-1",
                        "etag": "\"etag-1\"",
                        "iCalUID": "event-uid-1",
                        "summary": "Board Meeting",
                        "start": {"dateTime": "2026-03-08T15:00:00Z"},
                        "end": {"dateTime": "2026-03-08T16:00:00Z"},
                        "organizer": {"email": "alice@example.com", "displayName": "Alice Example"},
                        "attendees": [{"email": "me@example.com"}],
                        "status": "confirmed",
                    }
                ],
                "nextPageToken": None,
            }
        ]
    )
    adapter._gws = lambda args: next(initial)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_events=10)
    assert result.created == 1

    (tmp_vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")

    unchanged = iter(
        [
            {
                "items": [
                    {
                        "id": "event-google-1",
                        "etag": "\"etag-1\"",
                        "iCalUID": "event-uid-1",
                        "summary": "Board Meeting",
                        "start": {"dateTime": "2026-03-08T15:00:00Z"},
                        "end": {"dateTime": "2026-03-08T16:00:00Z"},
                        "organizer": {"email": "alice@example.com", "displayName": "Alice Example"},
                        "attendees": [{"email": "me@example.com"}],
                        "status": "confirmed",
                    }
                ],
                "nextPageToken": None,
            }
        ]
    )
    adapter = CalendarEventsAdapter()
    adapter._gws = lambda args: next(unchanged)  # type: ignore[method-assign]
    result = adapter.ingest(
        str(tmp_vault),
        account_email="me@example.com",
        max_events=10,
        quick_update=True,
    )
    assert result.created == 0
    assert result.merged == 0
    assert result.skipped == 1
    assert result.skip_details["skipped_unchanged_events"] == 1

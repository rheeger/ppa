"""Archive-sync Gmail message adapter tests."""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter, _attachment_uid, _message_uid, _thread_uid
from archive_vault.schema import EmailAttachmentCard, EmailMessageCard, EmailThreadCard, PersonCard
from archive_vault.vault import read_note, write_card


def _b64(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("utf-8").rstrip("=")


def _message(
    *,
    message_id: str,
    thread_id: str,
    internal_date: str,
    subject: str,
    body: str,
    from_value: str,
    to_value: str,
    snippet: str,
    attachment: dict | None = None,
    invite_ics: str | None = None,
    event_id_hint: str | None = None,
    html_body: str | None = None,
    sender_value: str | None = None,
    calendar_attachment_only: bool = False,
) -> dict:
    headers = [
        {"name": "From", "value": from_value},
        {"name": "To", "value": to_value},
        {"name": "Subject", "value": subject},
        {"name": "Date", "value": "Sat, 08 Mar 2026 01:00:00 +0000"},
        {"name": "Message-ID", "value": f"<{message_id}@example.com>"},
    ]
    if sender_value:
        headers.append({"name": "Sender", "value": sender_value})
    if event_id_hint:
        headers.append({"name": "X-Goog-Calendar-EventId", "value": event_id_hint})
    parts = [{"mimeType": "text/plain", "body": {"data": _b64(body)}}]
    if attachment:
        parts.append(
            {
                "mimeType": attachment["mime_type"],
                "filename": attachment["filename"],
                "body": {"attachmentId": attachment["attachment_id"], "size": attachment["size_bytes"]},
                "headers": [{"name": "Content-Disposition", "value": "attachment"}],
            }
        )
    if invite_ics:
        if calendar_attachment_only:
            parts.append(
                {
                    "mimeType": "text/calendar",
                    "filename": "invite.ics",
                    "body": {"attachmentId": "calendar-attachment", "size": len(invite_ics)},
                    "headers": [{"name": "Content-Type", "value": "text/calendar; method=REQUEST"}],
                }
            )
        else:
            parts.append({"mimeType": "text/calendar", "body": {"data": _b64(invite_ics)}})
    if html_body:
        parts.append({"mimeType": "text/html", "body": {"data": _b64(html_body)}})
    return {
        "id": message_id,
        "threadId": thread_id,
        "internalDate": internal_date,
        "snippet": snippet,
        "labelIds": ["INBOX"],
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": headers,
            "parts": parts,
        },
    }


def _thread(thread_id: str, *messages: dict, history_id: str = "") -> dict:
    payload = {"id": thread_id, "messages": list(messages)}
    if history_id:
        payload["historyId"] = history_id
    return payload


def _normalize_items(items: list[dict]) -> list[dict]:
    return json.loads(json.dumps(items, sort_keys=True))


def _hash_tree(root: Path) -> dict[str, str]:
    return {str(path.relative_to(root)): path.read_text(encoding="utf-8") for path in sorted(root.rglob("*.md"))}


def test_fetch_resumes_with_page_thread_cursor(tmp_vault):
    adapter = GmailMessagesAdapter()
    responses = iter(
        [
            {"threads": [{"id": "t1"}, {"id": "t2"}], "nextPageToken": "page-2"},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="First",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                ),
            ),
            _thread(
                "t2",
                _message(
                    message_id="m2",
                    thread_id="t2",
                    internal_date="1710003600000",
                    subject="Second",
                    body="hello two",
                    from_value="Bob Example <bob@example.com>",
                    to_value="me@example.com",
                    snippet="hello two",
                ),
            ),
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    cursor = {"page_token": None}

    first = adapter.fetch(
        str(tmp_vault),
        cursor,
        account_email="me@example.com",
        max_threads=1,
        max_messages=1,
        max_attachments=None,
        page_size=2,
    )
    assert [item["kind"] for item in first] == ["thread", "message"]
    assert cursor["page_thread_ids"] == ["t1", "t2"]
    assert cursor["page_index"] == 1
    assert cursor["page_next_token"] == "page-2"

    second = adapter.fetch(
        str(tmp_vault),
        cursor,
        account_email="me@example.com",
        max_threads=1,
        max_messages=1,
        max_attachments=None,
        page_size=2,
    )
    assert [item["kind"] for item in second] == ["thread", "message"]
    assert second[1]["message_id"] == "m2"


def test_fetch_matches_single_and_multi_worker_modes(tmp_vault):
    thread_ids = [f"t{i}" for i in range(1, 7)]
    threads = {
        thread_id: _thread(
            thread_id,
            _message(
                message_id=f"m{idx}",
                thread_id=thread_id,
                internal_date=str(1710000000000 + idx * 1000),
                subject=f"Thread {idx}",
                body=f"hello {idx}",
                from_value=f"Sender {idx} <sender{idx}@example.com>",
                to_value="me@example.com",
                snippet=f"hello {idx}",
            ),
        )
        for idx, thread_id in enumerate(thread_ids, start=1)
    }

    def gws_response(args):
        if args[:4] == ["gmail", "users", "threads", "list"]:
            return {"threads": [{"id": thread_id} for thread_id in thread_ids], "nextPageToken": None}
        if args[:4] == ["gmail", "users", "threads", "get"]:
            params = json.loads(args[-1])
            return threads[params["id"]]
        raise AssertionError(f"Unexpected gws args: {args}")

    single_adapter = GmailMessagesAdapter()
    single_adapter._gws = gws_response  # type: ignore[method-assign]
    single = single_adapter.fetch(
        str(tmp_vault),
        {},
        account_email="me@example.com",
        max_threads=6,
        max_messages=6,
        max_attachments=None,
        page_size=6,
        workers=1,
    )

    multi_adapter = GmailMessagesAdapter()
    multi_adapter._gws = gws_response  # type: ignore[method-assign]
    multi = multi_adapter.fetch(
        str(tmp_vault),
        {},
        account_email="me@example.com",
        max_threads=6,
        max_messages=6,
        max_attachments=None,
        page_size=6,
        workers=4,
    )

    assert _normalize_items(single) == _normalize_items(multi)


def test_fetch_parallel_thread_get_is_faster(tmp_vault):
    thread_ids = [f"t{i}" for i in range(1, 13)]
    threads = {
        thread_id: _thread(
            thread_id,
            _message(
                message_id=f"m{idx}",
                thread_id=thread_id,
                internal_date=str(1710000000000 + idx * 1000),
                subject=f"Thread {idx}",
                body=f"hello {idx}",
                from_value=f"Sender {idx} <sender{idx}@example.com>",
                to_value="me@example.com",
                snippet=f"hello {idx}",
            ),
        )
        for idx, thread_id in enumerate(thread_ids, start=1)
    }

    def gws_response(args):
        if args[:4] == ["gmail", "users", "threads", "list"]:
            return {"threads": [{"id": thread_id} for thread_id in thread_ids], "nextPageToken": None}
        if args[:4] == ["gmail", "users", "threads", "get"]:
            time.sleep(0.02)
            params = json.loads(args[-1])
            return threads[params["id"]]
        raise AssertionError(f"Unexpected gws args: {args}")

    single_adapter = GmailMessagesAdapter()
    single_adapter._gws = gws_response  # type: ignore[method-assign]
    started = time.perf_counter()
    single_adapter.fetch(
        str(tmp_vault),
        {},
        account_email="me@example.com",
        max_threads=12,
        max_messages=12,
        max_attachments=None,
        page_size=12,
        workers=1,
    )
    single_elapsed = time.perf_counter() - started

    multi_adapter = GmailMessagesAdapter()
    multi_adapter._gws = gws_response  # type: ignore[method-assign]
    started = time.perf_counter()
    multi_adapter.fetch(
        str(tmp_vault),
        {},
        account_email="me@example.com",
        max_threads=12,
        max_messages=12,
        max_attachments=None,
        page_size=12,
        workers=4,
    )
    multi_elapsed = time.perf_counter() - started

    assert multi_elapsed < single_elapsed * 0.8


def test_to_card_returns_thread_message_and_attachment_cards():
    adapter = GmailMessagesAdapter()

    thread_card, _, _ = adapter.to_card(
        {
            "kind": "thread",
            "thread_id": "t1",
            "created": "2026-03-08",
            "subject": "Project thread",
            "participants": ["alice@example.com", "bob@example.com"],
            "messages": ["[[hfa-email-message-a]]"],
            "people": [],
        }
    )
    assert isinstance(thread_card, EmailThreadCard)

    message_card, _, body = adapter.to_card(
        {
            "kind": "message",
            "message_id": "m1",
            "thread_id": "t1",
            "account_email": "me@example.com",
            "thread": "[[hfa-email-thread-a]]",
            "created": "2026-03-08",
            "from_email": "alice@example.com",
            "to_emails": ["me@example.com"],
            "participant_emails": ["alice@example.com", "me@example.com"],
            "subject": "Project thread",
            "snippet": "hello",
            "invite_ical_uid": "uid-1",
            "invite_event_id_hint": "event-1",
            "invite_method": "REQUEST",
            "body": "hello world",
        }
    )
    assert isinstance(message_card, EmailMessageCard)
    assert message_card.invite_ical_uid == "uid-1"
    assert body == "hello world"

    attachment_card, _, _ = adapter.to_card(
        {
            "kind": "attachment",
            "message_id": "m1",
            "thread_id": "t1",
            "attachment_id": "a1",
            "created": "2026-03-08",
            "filename": "contract.pdf",
            "mime_type": "application/pdf",
        }
    )
    assert isinstance(attachment_card, EmailAttachmentCard)
    assert attachment_card.filename == "contract.pdf"


def test_gmail_uids_and_source_ids_are_account_scoped():
    adapter = GmailMessagesAdapter()

    thread_card, _, _ = adapter.to_card(
        {
            "kind": "thread",
            "thread_id": "thread-1",
            "account_email": "one@example.com",
            "created": "2026-03-08",
            "subject": "Scoped thread",
        }
    )
    other_thread_card, _, _ = adapter.to_card(
        {
            "kind": "thread",
            "thread_id": "thread-1",
            "account_email": "two@example.com",
            "created": "2026-03-08",
            "subject": "Scoped thread",
        }
    )
    assert thread_card.uid == _thread_uid("one@example.com", "thread-1")
    assert other_thread_card.uid == _thread_uid("two@example.com", "thread-1")
    assert thread_card.uid != other_thread_card.uid
    assert thread_card.source_id == "one@example.com:thread-1"

    message_card, _, _ = adapter.to_card(
        {
            "kind": "message",
            "message_id": "message-1",
            "thread_id": "thread-1",
            "account_email": "one@example.com",
            "thread": f"[[{thread_card.uid}]]",
            "created": "2026-03-08",
        }
    )
    other_message_card, _, _ = adapter.to_card(
        {
            "kind": "message",
            "message_id": "message-1",
            "thread_id": "thread-1",
            "account_email": "two@example.com",
            "thread": f"[[{other_thread_card.uid}]]",
            "created": "2026-03-08",
        }
    )
    assert message_card.uid == _message_uid("one@example.com", "message-1")
    assert other_message_card.uid == _message_uid("two@example.com", "message-1")
    assert message_card.uid != other_message_card.uid
    assert message_card.source_id == "one@example.com:message-1"

    attachment_card, _, _ = adapter.to_card(
        {
            "kind": "attachment",
            "message_id": "message-1",
            "thread_id": "thread-1",
            "attachment_id": "attachment-1",
            "account_email": "one@example.com",
            "created": "2026-03-08",
        }
    )
    other_attachment_card, _, _ = adapter.to_card(
        {
            "kind": "attachment",
            "message_id": "message-1",
            "thread_id": "thread-1",
            "attachment_id": "attachment-1",
            "account_email": "two@example.com",
            "created": "2026-03-08",
        }
    )
    assert attachment_card.uid == _attachment_uid("one@example.com", "message-1", "attachment-1")
    assert other_attachment_card.uid == _attachment_uid("two@example.com", "message-1", "attachment-1")
    assert attachment_card.uid != other_attachment_card.uid
    assert attachment_card.source_id == "one@example.com:message-1:attachment-1"


def test_ingest_updates_thread_card_incrementally(tmp_vault):
    person = PersonCard(
        uid="hfa-person-abc123def456",
        type="person",
        source=["contacts.apple"],
        source_id="alice@example.com",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Alice Example",
        emails=["alice@example.com"],
    )
    write_card(
        tmp_vault,
        "People/alice-example.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        '{\n  "_comment": "Alias -> canonical person wikilink",\n  "email:alice@example.com": "[[alice-example]]"\n}',
        encoding="utf-8",
    )

    adapter = GmailMessagesAdapter()

    first_run = iter(
        [
            {"threads": [{"id": "t1"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="Project thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                    attachment={
                        "attachment_id": "a1",
                        "filename": "contract.pdf",
                        "mime_type": "application/pdf",
                        "size_bytes": 123,
                    },
                ),
            ),
        ]
    )
    adapter._gws = lambda args: next(first_run)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=10, max_messages=10)
    assert result.created == 3

    second_run = iter(
        [
            {"threads": [{"id": "t1"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="Project thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                    attachment={
                        "attachment_id": "a1",
                        "filename": "contract.pdf",
                        "mime_type": "application/pdf",
                        "size_bytes": 123,
                    },
                ),
                _message(
                    message_id="m2",
                    thread_id="t1",
                    internal_date="1710007200000",
                    subject="Project thread",
                    body="hello two",
                    from_value="me@example.com",
                    to_value="Alice Example <alice@example.com>",
                    snippet="hello two",
                    invite_ics="BEGIN:VCALENDAR\nMETHOD:REQUEST\nBEGIN:VEVENT\nUID:event-uid-1\nSUMMARY:Board Meeting\nDTSTART:20260308T150000Z\nDTEND:20260308T160000Z\nEND:VEVENT\nEND:VCALENDAR",
                    event_id_hint="event-google-1",
                ),
            ),
        ]
    )
    adapter._gws = lambda args: next(second_run)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=10, max_messages=10)
    assert result.merged >= 2
    assert result.created >= 1

    thread_rel = next((tmp_vault / "EmailThreads").rglob("*.md")).relative_to(tmp_vault)
    thread_frontmatter, _, _ = read_note(tmp_vault, str(thread_rel))
    assert thread_frontmatter["message_count"] == 2
    assert len(thread_frontmatter["messages"]) == 2
    assert thread_frontmatter["invite_ical_uids"] == ["event-uid-1"]
    assert thread_frontmatter["thread_body_sha"]

    message_files = sorted((tmp_vault / "Email").rglob("*.md"))
    assert len(message_files) == 2
    message_cards = [read_note(tmp_vault, str(path.relative_to(tmp_vault))) for path in message_files]
    invite_frontmatter, invite_body, _ = next(
        (frontmatter, body, provenance)
        for frontmatter, body, provenance in message_cards
        if frontmatter.get("invite_event_id_hint") == "event-google-1"
    )
    assert invite_frontmatter["invite_event_id_hint"] == "event-google-1"
    assert invite_frontmatter["message_body_sha"]
    assert invite_body == "hello two"

    attachment_rel = next((tmp_vault / "EmailAttachments").rglob("*.md")).relative_to(tmp_vault)
    attachment_frontmatter, _, _ = read_note(tmp_vault, str(attachment_rel))
    assert attachment_frontmatter["attachment_metadata_sha"]


def test_ingest_resumes_from_mid_page_cursor(tmp_vault):
    adapter = GmailMessagesAdapter()

    first_run = iter(
        [
            {"threads": [{"id": "t1"}, {"id": "t2"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="First thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                ),
            ),
        ]
    )
    adapter._gws = lambda args: next(first_run)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=1, max_messages=1, page_size=2)
    assert result.created == 2

    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    cursor = state["gmail-messages:me@example.com"]
    assert cursor["page_thread_ids"] == ["t1", "t2"]
    assert cursor["page_index"] == 1

    second_run = iter(
        [
            _thread(
                "t2",
                _message(
                    message_id="m2",
                    thread_id="t2",
                    internal_date="1710003600000",
                    subject="Second thread",
                    body="hello two",
                    from_value="Bob Example <bob@example.com>",
                    to_value="me@example.com",
                    snippet="hello two",
                ),
            ),
        ]
    )
    adapter._gws = lambda args: next(second_run)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=1, max_messages=1, page_size=2)
    assert result.created == 2

    thread_files = sorted((tmp_vault / "EmailThreads").rglob("*.md"))
    message_files = sorted((tmp_vault / "Email").rglob("*.md"))
    assert len(thread_files) == 2
    assert len(message_files) == 2


def test_quick_update_skips_unchanged_thread_fetches(tmp_vault):
    adapter = GmailMessagesAdapter()
    initial = iter(
        [
            {"threads": [{"id": "t1", "historyId": "h1"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="First thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                ),
                history_id="h1",
            ),
        ]
    )
    adapter._gws = lambda args: next(initial)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=10, max_messages=10)
    assert result.created == 2

    fetched_thread_ids: list[str] = []

    def gws_response(args):
        if args[:4] == ["gmail", "users", "threads", "list"]:
            return {
                "threads": [
                    {"id": "t1", "historyId": "h1"},
                    {"id": "t2", "historyId": "h2"},
                ],
                "nextPageToken": None,
            }
        if args[:4] == ["gmail", "users", "threads", "get"]:
            params = json.loads(args[-1])
            fetched_thread_ids.append(params["id"])
            if params["id"] == "t1":
                raise AssertionError("quick update should not fetch unchanged thread t1")
            return _thread(
                "t2",
                _message(
                    message_id="m2",
                    thread_id="t2",
                    internal_date="1710003600000",
                    subject="Second thread",
                    body="hello two",
                    from_value="Bob Example <bob@example.com>",
                    to_value="me@example.com",
                    snippet="hello two",
                ),
                history_id="h2",
            )
        raise AssertionError(f"Unexpected gws args: {args}")

    adapter = GmailMessagesAdapter()
    adapter._gws = gws_response  # type: ignore[method-assign]
    items = adapter.fetch(
        str(tmp_vault),
        {},
        account_email="me@example.com",
        max_threads=10,
        max_messages=10,
        max_attachments=10,
        page_size=25,
        quick_update=True,
    )
    assert [item["kind"] for item in items] == ["thread", "message"]
    assert fetched_thread_ids == ["t2"]


def test_quick_update_only_emits_changed_messages_and_attachments(tmp_vault):
    adapter = GmailMessagesAdapter()
    initial = iter(
        [
            {"threads": [{"id": "t1", "historyId": "h1"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="Project thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                    attachment={
                        "attachment_id": "a1",
                        "filename": "contract.pdf",
                        "mime_type": "application/pdf",
                        "size_bytes": 123,
                    },
                ),
                history_id="h1",
            ),
        ]
    )
    adapter._gws = lambda args: next(initial)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=10, max_messages=10)
    assert result.created == 3

    changed = iter(
        [
            {"threads": [{"id": "t1", "historyId": "h2"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="Project thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                    attachment={
                        "attachment_id": "a1",
                        "filename": "contract.pdf",
                        "mime_type": "application/pdf",
                        "size_bytes": 123,
                    },
                ),
                _message(
                    message_id="m2",
                    thread_id="t1",
                    internal_date="1710003600000",
                    subject="Project thread",
                    body="hello two",
                    from_value="me@example.com",
                    to_value="Alice Example <alice@example.com>",
                    snippet="hello two",
                ),
                history_id="h2",
            ),
        ]
    )
    adapter = GmailMessagesAdapter()
    adapter._gws = lambda args: next(changed)  # type: ignore[method-assign]
    items = adapter.fetch(
        str(tmp_vault),
        {},
        account_email="me@example.com",
        max_threads=10,
        max_messages=10,
        max_attachments=10,
        page_size=25,
        quick_update=True,
    )
    assert [item["kind"] for item in items] == ["thread", "message"]
    assert [item["message_id"] for item in items if item["kind"] == "message"] == ["m2"]


def test_quick_update_reports_skip_details_on_ingest(tmp_vault):
    adapter = GmailMessagesAdapter()
    initial = iter(
        [
            {"threads": [{"id": "t1", "historyId": "h1"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="First thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                ),
                history_id="h1",
            ),
        ]
    )
    adapter._gws = lambda args: next(initial)  # type: ignore[method-assign]
    adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=10, max_messages=10)

    (tmp_vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")

    unchanged = iter(
        [
            {"threads": [{"id": "t1", "historyId": "h1"}], "nextPageToken": None},
        ]
    )
    adapter = GmailMessagesAdapter()
    adapter._gws = lambda args: next(unchanged)  # type: ignore[method-assign]
    result = adapter.ingest(
        str(tmp_vault),
        account_email="me@example.com",
        max_threads=10,
        max_messages=10,
        quick_update=True,
    )
    assert result.created == 0
    assert result.merged == 0
    assert result.skipped == 1
    assert result.skip_details["skipped_unchanged_threads"] == 1


def test_gws_with_retry_retries_rate_limit_errors(monkeypatch):
    adapter = GmailMessagesAdapter()
    calls = {"count": 0}

    def flaky(args):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError('{"error":{"code":403,"reason":"rateLimitExceeded","message":"Quota exceeded"}}')
        return {"ok": True}

    monkeypatch.setattr(adapter, "_gws", flaky)
    result = adapter._gws_with_retry(["gmail", "users", "threads", "list", "--params", "{}"], attempts=2)
    assert result == {"ok": True}
    assert calls["count"] == 2


def test_gws_with_retry_retries_failed_precondition_errors(monkeypatch):
    adapter = GmailMessagesAdapter()
    calls = {"count": 0}

    def flaky(args):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError(
                '{"error":{"code":400,"reason":"failedPrecondition","message":"Precondition check failed."}}'
            )
        return {"ok": True}

    monkeypatch.setattr(adapter, "_gws", flaky)
    result = adapter._gws_with_retry(["gmail", "users", "threads", "get", "--params", "{}"], attempts=2)
    assert result == {"ok": True}
    assert calls["count"] == 2


def test_gws_falls_back_to_http_for_project_permission_errors(monkeypatch):
    adapter = GmailMessagesAdapter()

    class DummyManager:
        def build_env(self, env=None, *, force_refresh=False):
            return {}

        def get_access_token(self, *, force_refresh=False):
            return "token"

    adapter._token_manager = DummyManager()
    adapter._token_manager_key = ("gmail", "robbie@givingtree.tech")

    def fake_run(*args, **kwargs):
        return type(
            "Proc",
            (),
            {
                "returncode": 1,
                "stderr": 'RuntimeError: {"error":{"code":403,"reason":"forbidden","message":"Caller does not have required permission to use project claude-gmail-mcp-485803. Grant the caller the roles/serviceusage.serviceUsageConsumer role."}}',
                "stdout": "",
            },
        )()

    monkeypatch.setattr("archive_sync.adapters.gmail_messages.subprocess.run", fake_run)
    monkeypatch.setattr(adapter, "_gmail_http_json", lambda args: {"threads": [{"id": "t1"}]})

    result = adapter._gws(["gmail", "users", "threads", "list", "--params", '{"userId":"me","maxResults":1}'])
    assert result == {"threads": [{"id": "t1"}]}


def test_attachment_cap_does_not_leave_orphaned_attachment_links(tmp_vault):
    adapter = GmailMessagesAdapter()
    responses = iter(
        [
            {"threads": [{"id": "t1", "historyId": "h1"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="Project thread",
                    body="hello one",
                    from_value="Alice Example <alice@example.com>",
                    to_value="me@example.com",
                    snippet="hello one",
                    attachment={
                        "attachment_id": "a1",
                        "filename": "contract-1.pdf",
                        "mime_type": "application/pdf",
                        "size_bytes": 123,
                    },
                ),
                _message(
                    message_id="m2",
                    thread_id="t1",
                    internal_date="1710003600000",
                    subject="Project thread",
                    body="hello two",
                    from_value="Bob Example <bob@example.com>",
                    to_value="me@example.com",
                    snippet="hello two",
                    attachment={
                        "attachment_id": "a2",
                        "filename": "contract-2.pdf",
                        "mime_type": "application/pdf",
                        "size_bytes": 456,
                    },
                ),
                history_id="h1",
            ),
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    result = adapter.ingest(
        str(tmp_vault),
        account_email="me@example.com",
        max_threads=10,
        max_messages=10,
        max_attachments=1,
    )
    assert result.created == 4

    attachment_files = sorted((tmp_vault / "EmailAttachments").rglob("*.md"))
    assert len(attachment_files) == 1
    attachment_link = f"[[{attachment_files[0].stem}]]"

    message_files = sorted((tmp_vault / "Email").rglob("*.md"))
    message_frontmatters = [read_note(tmp_vault, str(path.relative_to(tmp_vault)))[0] for path in message_files]
    attachments_by_message = {
        frontmatter["gmail_message_id"]: list(frontmatter.get("attachments", []))
        for frontmatter in message_frontmatters
    }
    assert attachments_by_message["m1"] == [attachment_link]
    assert attachments_by_message["m2"] == []


def test_extracts_invite_data_from_calendar_attachment_fetch():
    adapter = GmailMessagesAdapter()
    message = _message(
        message_id="m-ics",
        thread_id="t-ics",
        internal_date="1710007200000",
        subject="Invitation: Board Meeting @ Tue Mar 10, 2026 3pm",
        body="calendar invite",
        from_value="calendar-notification@google.com",
        to_value="me@example.com",
        snippet="calendar invite",
        invite_ics="BEGIN:VCALENDAR\nMETHOD:REQUEST\nBEGIN:VEVENT\nUID:event-uid-1@google.com\nSUMMARY:Board Meeting\nDTSTART:20260310T150000Z\nDTEND:20260310T160000Z\nEND:VEVENT\nEND:VCALENDAR",
        event_id_hint="event-google-1",
        calendar_attachment_only=True,
    )
    adapter._gws = lambda args: {
        "data": _b64(
            "BEGIN:VCALENDAR\nMETHOD:REQUEST\nBEGIN:VEVENT\nUID:event-uid-1@google.com\nSUMMARY:Board Meeting\nDTSTART:20260310T150000Z\nDTEND:20260310T160000Z\nEND:VEVENT\nEND:VCALENDAR"
        )
    }  # type: ignore[method-assign]
    record, _ = adapter._message_records(  # type: ignore[misc]
        message,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        identity_cache=type("DummyCache", (), {"resolve": lambda self, prefix, value: None})(),  # type: ignore
        thread_uid="hfa-thread",
    )
    assert record["invite_ical_uid"] == "event-uid-1@google.com"
    assert record["invite_event_id_hint"] == "event-google-1"
    assert record["invite_method"] == "REQUEST"


def test_extracts_google_calendar_html_without_ics():
    adapter = GmailMessagesAdapter()
    html_body = """
    <html><body>
    <meta itemprop="eventId/googleCalendar" content="appointment-event-123" />
    <span itemprop="name">OSO (Carl+Ray) (Robbie Heeger)</span>
    <time itemprop="startDate" datetime="2024-11-26T18:00:00Z"></time>
    <time itemprop="endDate" datetime="2024-11-26T18:30:00Z"></time>
    <div>You have an upcoming appointment with ray@karibalabs.co</div>
    <div>Powered by Google Calendar appointment scheduling.</div>
    </body></html>
    """
    message = _message(
        message_id="m-reminder",
        thread_id="t-reminder",
        internal_date="1732557610000",
        subject="Reminder: OSO (Carl+Ray) (Robbie Heeger) @ Tue Nov 26, 2024 1pm - 1:30pm (EST) (ray@karibalabs.co)",
        body="You have an upcoming appointment with ray@karibalabs.co",
        from_value='"ray@karibalabs.co (Google Calendar)" <calendar-notification@google.com>',
        to_value="me@example.com",
        snippet="You have an upcoming appointment",
        html_body=html_body,
        sender_value="Google Calendar <calendar-notification@google.com>",
    )
    record, _ = adapter._message_records(  # type: ignore[misc]
        message,
        account_email="me@example.com",
        own_emails={"me@example.com"},
        identity_cache=type("DummyCache", (), {"resolve": lambda self, prefix, value: None})(),  # type: ignore
        thread_uid="hfa-thread",
    )
    assert record["invite_event_id_hint"] == "appointment-event-123"
    assert record["invite_title"] == "OSO (Carl+Ray) (Robbie Heeger)"
    assert record["invite_method"] == "REMINDER"
    assert record["invite_start_at"] == "2024-11-26T18:00:00Z"
    assert record["invite_end_at"] == "2024-11-26T18:30:00Z"

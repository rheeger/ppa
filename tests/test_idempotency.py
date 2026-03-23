"""PPA idempotency tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.calendar_events import CalendarEventsAdapter
from archive_sync.adapters.contacts import ContactsAdapter
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter
from archive_sync.adapters.linkedin import LinkedInAdapter
from hfa.schema import EmailMessageCard, EmailThreadCard
from hfa.uid import generate_uid
from hfa.vault import read_note, write_card


@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text("[]", encoding="utf-8")
    (meta / "enrichment-log.json").write_text("[]", encoding="utf-8")
    (meta / "llm-cache.json").write_text("{}", encoding="utf-8")
    (meta / "nicknames.json").write_text(json.dumps({"robert": ["rob", "robbie"]}), encoding="utf-8")
    return vault


def _write_fixtures(tmp_path: Path) -> dict[str, Path]:
    vcf_path = tmp_path / "contacts.vcf"
    vcf_path.write_text(
        "\n".join(
            [
                "BEGIN:VCARD",
                "VERSION:3.0",
                "FN:Jane Smith",
                "EMAIL;TYPE=HOME:jane@example.com",
                "ORG:Endaoment",
                "END:VCARD",
            ]
        ),
        encoding="utf-8",
    )
    linkedin_path = tmp_path / "linkedin.csv"
    linkedin_path.write_text(
        "First Name,Last Name,Email Address,Company,Position,Connected On\n"
        "Jane,Smith,jane@example.com,Endaoment,VP,2024-01-01\n",
        encoding="utf-8",
    )
    return {"vcf": vcf_path, "linkedin": linkedin_path}


def _run_import(vault: Path, fixtures: dict[str, Path]) -> None:
    contacts = ContactsAdapter()
    contacts._fetch_google = lambda: []  # type: ignore[method-assign]
    contacts._fetch_vcf_files = lambda: contacts._parse_vcf(str(fixtures["vcf"]))  # type: ignore[method-assign]
    contacts.ingest(str(vault), sources=["vcf"])
    LinkedInAdapter().ingest(str(vault), csv_path=str(fixtures["linkedin"]))


def _gmail_message(
    *,
    message_id: str,
    thread_id: str,
    internal_date: str,
    subject: str,
    body: str,
    from_value: str,
    to_value: str,
    snippet: str,
) -> dict:
    return {
        "id": message_id,
        "threadId": thread_id,
        "internalDate": internal_date,
        "snippet": snippet,
        "labelIds": ["INBOX"],
        "payload": {
            "mimeType": "multipart/mixed",
            "headers": [
                {"name": "From", "value": from_value},
                {"name": "To", "value": to_value},
                {"name": "Subject", "value": subject},
                {"name": "Date", "value": "Sat, 08 Mar 2026 01:00:00 +0000"},
                {"name": "Message-ID", "value": f"<{message_id}@example.com>"},
            ],
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {
                        "data": "aGVsbG8="
                        if body == "hello"
                        else "d29ybGQ="
                    },
                }
            ],
        },
    }


def _gmail_thread(thread_id: str, *messages: dict) -> dict:
    return {"id": thread_id, "messages": list(messages)}


def _hash_markdown_tree(root: Path, *dirs: str) -> dict[str, str]:
    files: list[Path] = []
    for dir_name in dirs:
        target = root / dir_name
        if target.exists():
            files.extend(sorted(target.rglob("*.md")))
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }


def test_uid_is_deterministic():
    assert generate_uid("person", "linkedin", "jane@example.com") == generate_uid(
        "person", "linkedin", "jane@example.com"
    )


def test_double_import_does_not_grow_people_count(tmp_vault, tmp_path):
    fixtures = _write_fixtures(tmp_path)
    _run_import(tmp_vault, fixtures)
    first_count = len(list((tmp_vault / "People").glob("*.md")))
    _run_import(tmp_vault, fixtures)
    second_count = len(list((tmp_vault / "People").glob("*.md")))
    assert first_count == second_count == 1


def test_double_import_does_not_duplicate_linkedin_body(tmp_vault, tmp_path):
    fixtures = _write_fixtures(tmp_path)
    _run_import(tmp_vault, fixtures)
    _run_import(tmp_vault, fixtures)
    _, body, _ = read_note(tmp_vault, "People/jane-smith.md")
    assert body == "Connected on: 2024-01-01"


def test_wipe_and_reimport_produces_same_card_hashes(tmp_vault, tmp_path):
    fixtures = _write_fixtures(tmp_path)
    _run_import(tmp_vault, fixtures)
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((tmp_vault / "People").glob("*.md"))
    }

    for path in (tmp_vault / "People").glob("*.md"):
        path.unlink()
    (tmp_vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (tmp_vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")

    _run_import(tmp_vault, fixtures)
    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((tmp_vault / "People").glob("*.md"))
    }
    assert before == after


def test_gmail_messages_double_import_preserves_note_hashes(tmp_vault):
    adapter = GmailMessagesAdapter()

    def run_once():
        responses = iter(
            [
                {"threads": [{"id": "t1"}], "nextPageToken": None},
                _gmail_thread(
                    "t1",
                    _gmail_message(
                        message_id="m1",
                        thread_id="t1",
                        internal_date="1710000000000",
                        subject="Project thread",
                        body="hello",
                        from_value="Alice Example <alice@example.com>",
                        to_value="me@example.com",
                        snippet="hello",
                    ),
                    _gmail_message(
                        message_id="m2",
                        thread_id="t1",
                        internal_date="1710003600000",
                        subject="Project thread",
                        body="world",
                        from_value="me@example.com",
                        to_value="Alice Example <alice@example.com>",
                        snippet="world",
                    ),
                ),
            ]
        )
        adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
        return adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=10, max_messages=10)

    first = run_once()
    assert first.created == 3
    before = _hash_markdown_tree(tmp_vault, "Email", "EmailThreads", "EmailAttachments")

    second = run_once()
    assert second.created == 0
    assert second.merged >= 3
    after = _hash_markdown_tree(tmp_vault, "Email", "EmailThreads", "EmailAttachments")
    assert before == after


def test_calendar_events_double_import_preserves_note_hashes(tmp_vault):
    thread = EmailThreadCard(
        uid="hfa-email-thread-111111111111",
        type="email_thread",
        source=["gmail.thread"],
        source_id="thread-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting thread",
        gmail_thread_id="thread-1",
        invite_ical_uids=["event-uid-1"],
        invite_event_id_hints=["event-google-1"],
    )
    message = EmailMessageCard(
        uid="hfa-email-message-111111111111",
        type="email_message",
        source=["gmail.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board meeting invite",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        invite_ical_uid="event-uid-1",
        invite_event_id_hint="event-google-1",
    )
    write_card(
        tmp_vault,
        "EmailThreads/2026-03/hfa-email-thread-111111111111.md",
        thread,
        provenance=deterministic_provenance(thread, "gmail.thread"),
    )
    write_card(
        tmp_vault,
        "Email/2026-03/hfa-email-message-111111111111.md",
        message,
        provenance=deterministic_provenance(message, "gmail.message"),
    )

    adapter = CalendarEventsAdapter()

    def run_once():
        responses = iter(
            [
                {
                    "items": [
                        {
                            "id": "event-google-1",
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
        return adapter.ingest(str(tmp_vault), account_email="me@example.com", max_events=10)

    first = run_once()
    assert first.created == 1
    before = _hash_markdown_tree(tmp_vault, "Calendar")

    second = run_once()
    assert second.created == 0
    assert second.merged == 1
    after = _hash_markdown_tree(tmp_vault, "Calendar")
    assert before == after

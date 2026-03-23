from __future__ import annotations

import json
import subprocess
from pathlib import Path

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.calendar_events import _event_uid
from archive_sync.adapters.gmail_messages import _attachment_uid, _message_uid, _thread_uid
from hfa.schema import (
    CalendarEventCard,
    EmailAttachmentCard,
    EmailMessageCard,
    EmailThreadCard,
)
from hfa.vault import read_note, write_card


def test_google_account_scope_migration_rewrites_ids_paths_and_links(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    vault = tmp_path / "hf-archives"
    (vault / "_meta").mkdir(parents=True)
    (vault / "_meta" / "sync-state.json").write_text(json.dumps({"gmail-messages:rheeger@gmail.com": {"page_token": None}}), encoding="utf-8")

    old_thread_uid = "hfa-email-thread-oldthread"
    old_message_uid = "hfa-email-message-oldmessage"
    old_attachment_uid = "hfa-email-attachment-oldattachment"
    old_event_uid = "hfa-calendar-event-oldevent"

    thread = EmailThreadCard(
        uid=old_thread_uid,
        type="email_thread",
        source=["gmail.thread"],
        source_id="thread-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board thread",
        gmail_thread_id="thread-1",
        account_email="rheeger@gmail.com",
        messages=[f"[[{old_message_uid}]]"],
        calendar_events=[f"[[{old_event_uid}]]"],
    )
    message = EmailMessageCard(
        uid=old_message_uid,
        type="email_message",
        source=["gmail.message"],
        source_id="message-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board invite",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        account_email="rheeger@gmail.com",
        thread=f"[[{old_thread_uid}]]",
        attachments=[f"[[{old_attachment_uid}]]"],
        calendar_events=[f"[[{old_event_uid}]]"],
        from_email="alice@example.com",
        to_emails=["rheeger@gmail.com"],
        participant_emails=["alice@example.com", "rheeger@gmail.com"],
    )
    attachment = EmailAttachmentCard(
        uid=old_attachment_uid,
        type="email_attachment",
        source=["gmail.attachment"],
        source_id="message-1:attachment-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="agenda.pdf",
        gmail_message_id="message-1",
        gmail_thread_id="thread-1",
        attachment_id="attachment-1",
        account_email="rheeger@gmail.com",
        message=f"[[{old_message_uid}]]",
        thread=f"[[{old_thread_uid}]]",
        filename="agenda.pdf",
    )
    event = CalendarEventCard(
        uid=old_event_uid,
        type="calendar_event",
        source=["calendar.event"],
        source_id="primary:event-1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Board Sync",
        account_email="rheeger@gmail.com",
        calendar_id="primary",
        event_id="event-1",
        title="Board Sync",
        start_at="2026-03-08T15:00:00Z",
        end_at="2026-03-08T16:00:00Z",
        source_messages=[f"[[{old_message_uid}]]"],
        source_threads=[f"[[{old_thread_uid}]]"],
    )

    write_card(vault, f"EmailThreads/2026-03/{old_thread_uid}.md", thread, provenance=deterministic_provenance(thread, "gmail.thread"))
    write_card(vault, f"Email/2026-03/{old_message_uid}.md", message, provenance=deterministic_provenance(message, "gmail.message"))
    write_card(vault, f"EmailAttachments/2026-03/{old_attachment_uid}.md", attachment, provenance=deterministic_provenance(attachment, "gmail.attachment"))
    write_card(vault, f"Calendar/2026-03/{old_event_uid}.md", event, provenance=deterministic_provenance(event, "calendar.event"))
    (vault / "Scratch").mkdir()
    (vault / "Scratch" / "reference.md").write_text(
        f"---\nuid: scratch-note\nsource_id: scratch\ncreated: 2026-03-08\nupdated: 2026-03-08\ntype: person\nsummary: scratch\n---\n[[{old_thread_uid}]]\n[[{old_event_uid}]]\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            str(repo_root / ".venv" / "bin" / "python"),
            str(repo_root / "scripts" / "ppa-migrate-google-account-scope.py"),
            "--vault",
            str(vault),
            "--account-email",
            "rheeger@gmail.com",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout

    new_thread_uid = _thread_uid("rheeger@gmail.com", "thread-1")
    new_message_uid = _message_uid("rheeger@gmail.com", "message-1")
    new_attachment_uid = _attachment_uid("rheeger@gmail.com", "message-1", "attachment-1")
    new_event_uid = _event_uid("rheeger@gmail.com", "primary", "event-1")

    assert not (vault / f"EmailThreads/2026-03/{old_thread_uid}.md").exists()
    assert not (vault / f"Email/2026-03/{old_message_uid}.md").exists()
    assert not (vault / f"EmailAttachments/2026-03/{old_attachment_uid}.md").exists()
    assert not (vault / f"Calendar/2026-03/{old_event_uid}.md").exists()

    thread_frontmatter, _, _ = read_note(vault, f"EmailThreads/2026-03/{new_thread_uid}.md")
    message_frontmatter, _, _ = read_note(vault, f"Email/2026-03/{new_message_uid}.md")
    attachment_frontmatter, _, _ = read_note(vault, f"EmailAttachments/2026-03/{new_attachment_uid}.md")
    event_frontmatter, _, _ = read_note(vault, f"Calendar/2026-03/{new_event_uid}.md")

    assert thread_frontmatter["source_id"] == "rheeger@gmail.com:thread-1"
    assert thread_frontmatter["messages"] == [f"[[{new_message_uid}]]"]
    assert message_frontmatter["source_id"] == "rheeger@gmail.com:message-1"
    assert message_frontmatter["thread"] == f"[[{new_thread_uid}]]"
    assert message_frontmatter["attachments"] == [f"[[{new_attachment_uid}]]"]
    assert attachment_frontmatter["source_id"] == "rheeger@gmail.com:message-1:attachment-1"
    assert attachment_frontmatter["message"] == f"[[{new_message_uid}]]"
    assert event_frontmatter["source_id"] == "rheeger@gmail.com:primary:event-1"
    assert event_frontmatter["source_messages"] == [f"[[{new_message_uid}]]"]
    assert event_frontmatter["source_threads"] == [f"[[{new_thread_uid}]]"]

    scratch = (vault / "Scratch" / "reference.md").read_text(encoding="utf-8")
    assert f"[[{new_thread_uid}]]" in scratch
    assert f"[[{new_event_uid}]]" in scratch

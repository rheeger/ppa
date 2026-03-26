from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_calendar_spool_import_links_to_existing_email_cards(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Photos").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    for name, payload in {
        "identity-map.json": {},
        "sync-state.json": {},
        "dedup-candidates.json": [],
        "enrichment-log.json": [],
        "llm-cache.json": {},
        "nicknames.json": {},
    }.items():
        (meta / name).write_text(json.dumps(payload), encoding="utf-8")

    bootstrap = (
        "from pathlib import Path\n"
        "import json\n"
        "from archive_sync.adapters.base import deterministic_provenance\n"
        "from hfa.schema import EmailMessageCard, EmailThreadCard\n"
        "from hfa.vault import write_card\n"
        f"vault = Path({str(vault)!r})\n"
        "thread = EmailThreadCard(uid='hfa-email-thread-111111111111', type='email_thread', source=['gmail.thread'], source_id='me@example.com:thread-1', created='2026-03-08', updated='2026-03-08', summary='Board meeting thread', gmail_thread_id='thread-1', account_email='me@example.com', invite_ical_uids=['event-uid-1'], invite_event_id_hints=['event-google-1'])\n"
        "message = EmailMessageCard(uid='hfa-email-message-111111111111', type='email_message', source=['gmail.message'], source_id='me@example.com:message-1', created='2026-03-08', updated='2026-03-08', summary='Board meeting invite', gmail_message_id='message-1', gmail_thread_id='thread-1', account_email='me@example.com', invite_ical_uid='event-uid-1', invite_event_id_hint='event-google-1')\n"
        "write_card(vault, 'EmailThreads/2026-03/hfa-email-thread-111111111111.md', thread, provenance=deterministic_provenance(thread, 'gmail.thread'))\n"
        "write_card(vault, 'Email/2026-03/hfa-email-message-111111111111.md', message, provenance=deterministic_provenance(message, 'gmail.message'))\n"
    )
    subprocess.run([sys.executable, "-c", bootstrap], check=True)

    spool = tmp_path / "calendar-spool"
    (spool / "events").mkdir(parents=True)
    (spool / "_meta").mkdir()
    (spool / "_meta" / "manifest.json").write_text(
        json.dumps(
            {
                "account_email": "me@example.com",
                "calendar_id": "primary",
            }
        ),
        encoding="utf-8",
    )
    (spool / "events" / "event-google-1.json").write_text(
        json.dumps(
            {
                "id": "event-google-1",
                "etag": '"etag-1"',
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
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "ppa-calendar-import-spool.py"),
            "--vault",
            str(vault),
            "--spool-dir",
            str(spool),
            "--account-email",
            "me@example.com",
            "--calendar-id",
            "primary",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    event_files = sorted((vault / "Calendar").rglob("*.md"))
    assert len(event_files) == 1
    content = event_files[0].read_text(encoding="utf-8")
    assert "[[hfa-email-message-111111111111]]" in content
    assert "[[hfa-email-thread-111111111111]]" in content


def test_calendar_spool_import_rejects_manifest_account_mismatch(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    vault = tmp_path / "hf-archives"
    vault.mkdir()

    spool = tmp_path / "calendar-spool"
    (spool / "events").mkdir(parents=True)
    (spool / "_meta").mkdir()
    (spool / "_meta" / "manifest.json").write_text(
        json.dumps(
            {
                "account_email": "wrong@example.com",
                "calendar_id": "primary",
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "ppa-calendar-import-spool.py"),
            "--vault",
            str(vault),
            "--spool-dir",
            str(spool),
            "--account-email",
            "me@example.com",
            "--calendar-id",
            "primary",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "Spool manifest mismatch" in (result.stderr + result.stdout)

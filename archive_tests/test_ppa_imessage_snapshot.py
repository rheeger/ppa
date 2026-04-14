"""Apple Messages snapshot script tests."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def _build_messages_store(messages_dir: Path) -> Path:
    messages_dir.mkdir(parents=True, exist_ok=True)
    db_path = messages_dir / "chat.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE message (guid TEXT, text TEXT, handle_id INTEGER, is_from_me INTEGER, date INTEGER, service TEXT);
        CREATE TABLE chat (guid TEXT, chat_identifier TEXT, display_name TEXT, service_name TEXT);
        CREATE TABLE handle (id TEXT);
        CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
        CREATE TABLE chat_handle_join (chat_id INTEGER, handle_id INTEGER);
        CREATE TABLE attachment (guid TEXT, filename TEXT, transfer_name TEXT, mime_type TEXT, total_bytes INTEGER, uti TEXT);
        CREATE TABLE message_attachment_join (message_id INTEGER, attachment_id INTEGER);
        """
    )
    conn.execute("INSERT INTO handle(ROWID, id) VALUES (1, 'alice@example.com')")
    conn.execute(
        "INSERT INTO chat(ROWID, guid, chat_identifier, display_name, service_name) VALUES (?, ?, ?, ?, ?)",
        (1, "chat-guid-1", "alice@example.com", "Alice Chat", "iMessage"),
    )
    conn.execute("INSERT INTO chat_handle_join(chat_id, handle_id) VALUES (1, 1)")
    conn.execute(
        "INSERT INTO message(ROWID, guid, text, handle_id, is_from_me, date, service) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (1, "message-guid-1", "hello one", 1, 0, 800000000, "iMessage"),
    )
    conn.execute("INSERT INTO chat_message_join(chat_id, message_id) VALUES (1, 1)")
    attachment_path = messages_dir / "Attachments" / "photo.jpg"
    attachment_path.parent.mkdir(parents=True, exist_ok=True)
    attachment_path.write_bytes(b"fake-jpeg")
    conn.execute(
        "INSERT INTO attachment(ROWID, guid, filename, transfer_name, mime_type, total_bytes, uti) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            1,
            "attachment-guid-1",
            str(attachment_path),
            "photo.jpg",
            "image/jpeg",
            attachment_path.stat().st_size,
            "public.jpeg",
        ),
    )
    conn.execute("INSERT INTO message_attachment_join(message_id, attachment_id) VALUES (1, 1)")
    conn.commit()
    conn.close()
    return attachment_path


def test_hfa_imessage_snapshot_builds_bundle(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "archive_scripts" / "ppa-imessage-snapshot.py"
    messages_dir = tmp_path / "Messages"
    _build_messages_store(messages_dir)
    output_dir = tmp_path / "snapshot"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root / "skills")

    result = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--messages-dir",
            str(messages_dir),
            "--output-dir",
            str(output_dir),
            "--source-label",
            "macbook-air",
        ],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    assert (output_dir / "chat.db").exists()
    assert (output_dir / "inspection.json").exists()
    assert (output_dir / "attachments-manifest.json").exists()
    inspection = json.loads((output_dir / "inspection.json").read_text(encoding="utf-8"))
    assert inspection["message_count"] == 1
    assert inspection["chat_count"] == 1
    assert inspection["attachment_count"] == 1
    assert inspection["source_label"] == "macbook-air"

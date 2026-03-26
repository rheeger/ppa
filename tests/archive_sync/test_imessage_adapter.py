"""Archive-sync Apple Messages adapter tests."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path

from archive_sync.adapters.imessage import IMessageAdapter
from hfa.schema import IMessageAttachmentCard, IMessageMessageCard, IMessageThreadCard
from hfa.vault import read_note


def _create_snapshot(snapshot_dir: Path) -> tuple[Path, Path]:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    db_path = snapshot_dir / "chat.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE message (
            guid TEXT,
            text TEXT,
            attributedBody BLOB,
            handle_id INTEGER,
            is_from_me INTEGER,
            date INTEGER,
            date_edited INTEGER,
            date_retracted INTEGER,
            service TEXT,
            subject TEXT,
            associated_message_guid TEXT,
            associated_message_type TEXT,
            associated_message_emoji TEXT,
            expressive_send_style_id TEXT,
            balloon_bundle_id TEXT
        );
        CREATE TABLE chat (
            guid TEXT,
            chat_identifier TEXT,
            display_name TEXT,
            room_name TEXT,
            service_name TEXT
        );
        CREATE TABLE handle (
            id TEXT
        );
        CREATE TABLE chat_message_join (
            chat_id INTEGER,
            message_id INTEGER
        );
        CREATE TABLE chat_handle_join (
            chat_id INTEGER,
            handle_id INTEGER
        );
        CREATE TABLE attachment (
            guid TEXT,
            filename TEXT,
            transfer_name TEXT,
            mime_type TEXT,
            total_bytes INTEGER,
            uti TEXT
        );
        CREATE TABLE message_attachment_join (
            message_id INTEGER,
            attachment_id INTEGER
        );
        """
    )
    conn.execute("INSERT INTO handle(ROWID, id) VALUES (1, 'alice@example.com')")
    conn.execute("INSERT INTO handle(ROWID, id) VALUES (2, '+1 (650) 555-1212')")
    conn.execute(
        "INSERT INTO chat(ROWID, guid, chat_identifier, display_name, room_name, service_name) VALUES (?, ?, ?, ?, ?, ?)",
        (1, "chat-guid-1", "alice@example.com", "Alice Chat", "", "iMessage"),
    )
    conn.execute("INSERT INTO chat_handle_join(chat_id, handle_id) VALUES (1, 1)")
    conn.execute("INSERT INTO chat_handle_join(chat_id, handle_id) VALUES (1, 2)")
    conn.commit()
    conn.close()

    attachment_path = snapshot_dir / "attachments" / "photo.jpg"
    attachment_path.parent.mkdir(parents=True, exist_ok=True)
    attachment_path.write_bytes(b"fake-jpeg")
    (snapshot_dir / "attachments-manifest.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "original_path": str(attachment_path),
                        "exported_path": "attachments/photo.jpg",
                        "size_bytes": attachment_path.stat().st_size,
                    }
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (snapshot_dir / "snapshot-meta.json").write_text(
        json.dumps({"source_label": "macbook-air", "exported_at": "2026-03-08T00:00:00Z"}, indent=2),
        encoding="utf-8",
    )
    return db_path, attachment_path


def _insert_message(
    db_path: Path,
    *,
    rowid: int,
    guid: str,
    text: str = "",
    attributed_body: bytes | None = None,
    handle_id: int = 1,
    is_from_me: int = 0,
    date_value: int = 800000000,
    attachment_path: str = "",
    associated_message_guid: str = "",
    associated_message_type: str = "",
) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO message(
            ROWID, guid, text, attributedBody, handle_id, is_from_me, date,
            date_edited, date_retracted, service, subject, associated_message_guid,
            associated_message_type, associated_message_emoji, expressive_send_style_id, balloon_bundle_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rowid,
            guid,
            text,
            attributed_body,
            handle_id,
            is_from_me,
            date_value,
            0,
            0,
            "iMessage",
            "",
            associated_message_guid,
            associated_message_type,
            "",
            "",
            "",
        ),
    )
    conn.execute("INSERT INTO chat_message_join(chat_id, message_id) VALUES (1, ?)", (rowid,))
    if attachment_path:
        conn.execute(
            "INSERT INTO attachment(ROWID, guid, filename, transfer_name, mime_type, total_bytes, uti) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                rowid,
                f"attachment-guid-{rowid}",
                attachment_path.replace(str(Path.home()), "~"),
                Path(attachment_path).name,
                "image/jpeg",
                9,
                "public.jpeg",
            ),
        )
        conn.execute("INSERT INTO message_attachment_join(message_id, attachment_id) VALUES (?, ?)", (rowid, rowid))
    conn.commit()
    conn.close()


def _normalize_items(items: list[dict]) -> list[tuple]:
    normalized = []
    for item in items:
        payload = {key: value for key, value in item.items() if key != "_cursor"}
        normalized.append(
            (
                payload.get("kind", ""),
                payload.get("chat_id", ""),
                payload.get("message_id", ""),
                payload.get("attachment_id", ""),
                json.dumps(payload, sort_keys=True, default=str),
            )
        )
    return sorted(normalized)


def test_to_card_returns_thread_message_and_attachment_cards():
    adapter = IMessageAdapter()
    thread_card, _, _ = adapter.to_card(
        {
            "kind": "thread",
            "chat_id": "chat-guid-1",
            "created": "2026-03-08",
            "display_name": "Alice Chat",
            "participant_handles": ["alice@example.com"],
            "messages": ["[[hfa-imessage-message-1]]"],
        }
    )
    assert isinstance(thread_card, IMessageThreadCard)

    message_card, _, body = adapter.to_card(
        {
            "kind": "message",
            "message_id": "message-guid-1",
            "chat_id": "chat-guid-1",
            "thread": "[[hfa-imessage-thread-1]]",
            "participant_handles": ["alice@example.com"],
            "body": "hello world",
        }
    )
    assert isinstance(message_card, IMessageMessageCard)
    assert body == "hello world"

    attachment_card, _, _ = adapter.to_card(
        {
            "kind": "attachment",
            "message_id": "message-guid-1",
            "chat_id": "chat-guid-1",
            "attachment_id": "attachment-guid-1",
            "filename": "photo.jpg",
        }
    )
    assert isinstance(attachment_card, IMessageAttachmentCard)
    assert attachment_card.filename == "photo.jpg"


def test_ingest_resumes_and_updates_thread_incrementally(tmp_vault, tmp_path):
    snapshot_dir = tmp_path / "imessage-snapshot"
    db_path, attachment_path = _create_snapshot(snapshot_dir)
    _insert_message(
        db_path,
        rowid=1,
        guid="message-guid-1",
        text="hello one",
        date_value=800000000,
        attachment_path=str(attachment_path),
    )

    adapter = IMessageAdapter()
    result = adapter.ingest(
        str(tmp_vault),
        snapshot_dir=str(snapshot_dir),
        source_label="macbook-air",
        max_messages=1,
    )
    assert result.created == 3

    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert state["imessage:macbook-air"]["last_completed_message_rowid"] == 1

    _insert_message(
        db_path,
        rowid=2,
        guid="message-guid-2",
        text="",
        attributed_body=b"streamtyped\x01NSString\x01second hello world\x01NSDictionary\x01__kIMMessagePartAttributeName",
        handle_id=1,
        is_from_me=1,
        date_value=800000600,
        associated_message_guid="p:0/message-guid-1",
        associated_message_type="2000",
    )

    result = adapter.ingest(
        str(tmp_vault),
        snapshot_dir=str(snapshot_dir),
        source_label="macbook-air",
        max_messages=10,
    )
    assert result.created == 1
    assert result.merged >= 1

    thread_rel = next((tmp_vault / "IMessageThreads").rglob("*.md")).relative_to(tmp_vault)
    thread_frontmatter, _, _ = read_note(tmp_vault, str(thread_rel))
    assert thread_frontmatter["message_count"] == 2
    assert thread_frontmatter["attachment_count"] == 1
    assert len(thread_frontmatter["messages"]) == 2
    assert thread_frontmatter["participant_handles"] == ["alice@example.com", "+16505551212"]
    assert thread_frontmatter["thread_body_sha"]

    message_files = sorted((tmp_vault / "IMessage").rglob("*.md"))
    assert len(message_files) == 2
    message_cards = [read_note(tmp_vault, str(path.relative_to(tmp_vault))) for path in message_files]
    incoming_frontmatter, incoming_body, _ = next(
        card for card in message_cards if card[0]["imessage_message_id"] == "message-guid-1"
    )
    outgoing_frontmatter, outgoing_body, _ = next(
        card for card in message_cards if card[0]["imessage_message_id"] == "message-guid-2"
    )
    assert incoming_body == "hello one"
    assert "second hello world" in outgoing_body
    assert outgoing_frontmatter["associated_message_guid"] == "p:0/message-guid-1"
    assert outgoing_frontmatter["associated_message_type"] == "2000"

    attachment_rel = next((tmp_vault / "IMessageAttachments").rglob("*.md")).relative_to(tmp_vault)
    attachment_frontmatter, _, _ = read_note(tmp_vault, str(attachment_rel))
    assert attachment_frontmatter["exported_path"] == "attachments/photo.jpg"
    assert attachment_frontmatter["thread"].startswith("[[hfa-imessage-thread-")


def test_fetch_matches_between_single_and_multi_worker_modes(tmp_vault, tmp_path):
    snapshot_dir = tmp_path / "imessage-snapshot-workers"
    db_path, attachment_path = _create_snapshot(snapshot_dir)
    for rowid in range(1, 7):
        _insert_message(
            db_path,
            rowid=rowid,
            guid=f"message-guid-{rowid}",
            text=f"hello {rowid}",
            date_value=800000000 + rowid,
            attachment_path=str(attachment_path) if rowid % 2 == 0 else "",
        )

    adapter = IMessageAdapter()
    single = adapter.fetch(
        str(tmp_vault),
        {},
        snapshot_dir=str(snapshot_dir),
        source_label="macbook-air",
        max_messages=6,
        workers=1,
    )
    multi = adapter.fetch(
        str(tmp_vault),
        {},
        snapshot_dir=str(snapshot_dir),
        source_label="macbook-air",
        max_messages=6,
        workers=4,
    )
    assert _normalize_items(single) == _normalize_items(multi)


def test_fetch_parallel_processing_is_faster_for_slow_row_builder(tmp_vault, tmp_path, monkeypatch):
    snapshot_dir = tmp_path / "imessage-snapshot-slow"
    db_path, _ = _create_snapshot(snapshot_dir)
    for rowid in range(1, 13):
        _insert_message(
            db_path,
            rowid=rowid,
            guid=f"message-guid-{rowid}",
            text=f"hello {rowid}",
            date_value=800000000 + rowid,
        )

    original = IMessageAdapter._prepare_message_bundle

    def slow_prepare(self, *, row, chat, participant_handles, sender_handle, attachments):
        time.sleep(0.02)
        return original(
            self,
            row=row,
            chat=chat,
            participant_handles=participant_handles,
            sender_handle=sender_handle,
            attachments=attachments,
        )

    monkeypatch.setattr(IMessageAdapter, "_prepare_message_bundle", slow_prepare)

    adapter = IMessageAdapter()
    started = time.perf_counter()
    adapter.fetch(
        str(tmp_vault),
        {},
        snapshot_dir=str(snapshot_dir),
        source_label="macbook-air",
        max_messages=12,
        workers=1,
    )
    single_elapsed = time.perf_counter() - started

    started = time.perf_counter()
    adapter.fetch(
        str(tmp_vault),
        {},
        snapshot_dir=str(snapshot_dir),
        source_label="macbook-air",
        max_messages=12,
        workers=4,
    )
    multi_elapsed = time.perf_counter() - started

    assert multi_elapsed < single_elapsed * 0.8

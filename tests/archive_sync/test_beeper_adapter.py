"""Archive-sync Beeper adapter tests."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from archive_sync.adapters.beeper import BeeperAdapter
from hfa.vault import read_note


def _normalize_items(items: list[dict]) -> list[tuple]:
    normalized: list[tuple] = []
    for item in items:
        payload = {key: value for key, value in item.items() if key != "_cursor"}
        normalized.append(
            (
                payload.get("kind", ""),
                payload.get("room_id", ""),
                payload.get("event_id", ""),
                payload.get("attachment_id", ""),
                json.dumps(payload, sort_keys=True, default=str),
            )
        )
    return sorted(normalized)


def _create_index_db(base_dir: Path) -> tuple[Path, Path]:
    base_dir.mkdir(parents=True, exist_ok=True)
    db_path = base_dir / "index.db"
    media_root = base_dir / "media"
    media_root.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE threads (
            threadID TEXT PRIMARY KEY,
            accountID TEXT NOT NULL,
            thread TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        );
        CREATE TABLE participants (
            account_id TEXT NOT NULL,
            room_id TEXT NOT NULL,
            id TEXT NOT NULL,
            full_name TEXT,
            nickname TEXT,
            img_url TEXT,
            is_verified INTEGER,
            cannot_message INTEGER,
            is_self INTEGER,
            is_network_bot INTEGER,
            added_by TEXT,
            is_admin INTEGER,
            is_pending INTEGER,
            has_exited INTEGER
        );
        CREATE TABLE participant_identifiers (
            account_id TEXT NOT NULL,
            participant_id TEXT NOT NULL,
            identifier TEXT NOT NULL,
            identifier_type TEXT NOT NULL
        );
        CREATE TABLE mx_room_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            roomID TEXT NOT NULL,
            eventID TEXT NOT NULL,
            senderContactID TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            isEdited INTEGER NOT NULL,
            lastEditionID TEXT,
            isDeleted INTEGER NOT NULL,
            inReplyToID TEXT,
            type TEXT NOT NULL,
            isSentByMe INTEGER NOT NULL,
            protocol TEXT,
            lastEditionTimestamp INTEGER,
            message TEXT NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()
    return db_path, media_root


def _insert_thread(
    db_path: Path,
    *,
    room_id: str,
    account_id: str,
    timestamp_ms: int,
    thread_type: str = "single",
    title: str = "",
) -> None:
    thread_payload = {
        "id": room_id,
        "type": thread_type,
        "title": title or None,
        "description": "",
        "createdAt": "2026-03-01T12:00:00Z",
        "timestamp": timestamp_ms,
        "extra": {"protocol": account_id.split(".", 1)[0], "bridgeName": account_id.split(".", 1)[0]},
    }
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO threads(threadID, accountID, thread, timestamp) VALUES (?, ?, ?, ?)",
        (room_id, account_id, json.dumps(thread_payload), timestamp_ms),
    )
    conn.commit()
    conn.close()


def _insert_participant(
    db_path: Path,
    *,
    account_id: str,
    room_id: str,
    participant_id: str,
    full_name: str,
    is_self: bool,
    identifiers: list[tuple[str, str]] | None = None,
) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO participants(
            account_id, room_id, id, full_name, nickname, img_url, is_verified,
            cannot_message, is_self, is_network_bot, added_by, is_admin, is_pending, has_exited
        ) VALUES (?, ?, ?, ?, NULL, NULL, 0, 0, ?, 0, NULL, 0, 0, 0)
        """,
        (account_id, room_id, participant_id, full_name, 1 if is_self else 0),
    )
    for identifier_type, identifier in identifiers or []:
        conn.execute(
            "INSERT INTO participant_identifiers(account_id, participant_id, identifier, identifier_type) VALUES (?, ?, ?, ?)",
            (account_id, participant_id, identifier, identifier_type),
        )
    conn.commit()
    conn.close()


def _insert_message(
    db_path: Path,
    *,
    room_id: str,
    event_id: str,
    sender_id: str,
    timestamp_ms: int,
    message_type: str,
    is_sent_by_me: bool,
    text: str = "",
    attachments: list[dict] | None = None,
) -> None:
    payload = {
        "id": event_id,
        "threadID": room_id,
        "eventID": event_id,
        "senderID": sender_id,
        "timestamp": timestamp_ms,
        "text": text,
        "isDeleted": False,
        "isSender": is_sent_by_me,
        "attachments": attachments or [],
        "extra": {"type": message_type, "isE2EE": False, "replyThreadID": None},
    }
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        INSERT INTO mx_room_messages(
            roomID, eventID, senderContactID, timestamp, isEdited, lastEditionID, isDeleted,
            inReplyToID, type, isSentByMe, protocol, lastEditionTimestamp, message
        ) VALUES (?, ?, ?, ?, 0, NULL, 0, NULL, ?, ?, NULL, NULL, ?)
        """,
        (room_id, event_id, sender_id, timestamp_ms, message_type, 1 if is_sent_by_me else 0, json.dumps(payload)),
    )
    conn.commit()
    conn.close()


def test_fetch_batches_builds_thread_message_and_attachment_items(tmp_vault, tmp_path):
    db_path, media_root = _create_index_db(tmp_path / "beeper-index")
    media_file = media_root / "local.beeper.com" / "media-1"
    media_file.parent.mkdir(parents=True, exist_ok=True)
    media_file.write_bytes(b"img")
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        json.dumps({"email:friend@example.com": "[[Jane Friend]]"}),
        encoding="utf-8",
    )

    _insert_thread(
        db_path,
        room_id="!room-1:beeper.local",
        account_id="googlechat",
        timestamp_ms=1710001000000,
    )
    _insert_participant(
        db_path,
        account_id="googlechat",
        room_id="!room-1:beeper.local",
        participant_id="@heegs:beeper.com",
        full_name="heegs",
        is_self=True,
    )
    _insert_participant(
        db_path,
        account_id="googlechat",
        room_id="!room-1:beeper.local",
        participant_id="@friend:beeper.local",
        full_name="Jane Friend",
        is_self=False,
        identifiers=[("email", "friend@example.com")],
    )
    _insert_message(
        db_path,
        room_id="!room-1:beeper.local",
        event_id="$event-1",
        sender_id="@friend:beeper.local",
        timestamp_ms=1710000000000,
        message_type="TEXT",
        is_sent_by_me=False,
        text="hello from jane",
    )
    _insert_message(
        db_path,
        room_id="!room-1:beeper.local",
        event_id="$event-2",
        sender_id="@heegs:beeper.com",
        timestamp_ms=1710001000000,
        message_type="IMAGE",
        is_sent_by_me=True,
        attachments=[
            {
                "id": "mxc://local.beeper.com/media-1",
                "type": "img",
                "fileName": "photo.jpg",
                "mimeType": "image/jpeg",
                "fileSize": 3,
                "srcURL": "mxc://local.beeper.com/media-1",
                "size": {"width": 100, "height": 200},
                "extra": {"caption": "photo caption"},
            }
        ],
    )

    adapter = BeeperAdapter()
    batches = list(
        adapter.fetch_batches(
            str(tmp_vault),
            {},
            db_path=str(db_path),
            media_root=str(media_root),
            max_threads=1,
            batch_size=1,
        )
    )
    assert len(batches) == 2
    assert [batch.sequence for batch in batches] == [0, 1]
    person_items = [item for item in batches[0].items if item["kind"] == "person"]
    items = batches[1].items
    thread_items = [item for item in items if item["kind"] == "thread"]
    message_items = [item for item in items if item["kind"] == "message"]
    attachment_items = [item for item in items if item["kind"] == "attachment"]

    assert len(person_items) == 1
    assert person_items[0]["emails"] == ["friend@example.com"]
    assert len(thread_items) == 1
    assert len(message_items) == 2
    assert len(attachment_items) == 1
    assert thread_items[0]["summary"] == "Jane Friend"
    assert thread_items[0]["people"] == ["[[Jane Friend]]"]
    assert thread_items[0]["message_count"] == 2
    assert attachment_items[0]["cached_path"] == str(media_file)
    assert message_items[0]["summary"] == "hello from jane"
    incoming = next(item for item in message_items if item["sender_id"] == "@friend:beeper.local")
    outgoing = next(item for item in message_items if item["sender_id"] == "@heegs:beeper.com")
    assert incoming["sender_person"] == "[[Jane Friend]]"
    assert outgoing["sender_person"] == ""


def test_ingest_resumes_by_thread_cursor_and_writes_notes(tmp_vault, tmp_path):
    db_path, media_root = _create_index_db(tmp_path / "beeper-index-resume")

    _insert_thread(
        db_path,
        room_id="!room-1:beeper.local",
        account_id="linkedin",
        timestamp_ms=1710000000000,
    )
    _insert_participant(
        db_path,
        account_id="linkedin",
        room_id="!room-1:beeper.local",
        participant_id="@heegs:beeper.com",
        full_name="heegs",
        is_self=True,
    )
    _insert_participant(
        db_path,
        account_id="linkedin",
        room_id="!room-1:beeper.local",
        participant_id="@candis:beeper.local",
        full_name="Kandis Canonica",
        is_self=False,
        identifiers=[("username", "kandisa")],
    )
    _insert_message(
        db_path,
        room_id="!room-1:beeper.local",
        event_id="$event-1",
        sender_id="@candis:beeper.local",
        timestamp_ms=1710000000000,
        message_type="TEXT",
        is_sent_by_me=False,
        text="hey there",
    )

    _insert_thread(
        db_path,
        room_id="!room-2:beeper.local",
        account_id="discordgo",
        timestamp_ms=1711000000000,
    )
    _insert_participant(
        db_path,
        account_id="discordgo",
        room_id="!room-2:beeper.local",
        participant_id="@heegs:beeper.com",
        full_name="heegs",
        is_self=True,
    )
    _insert_participant(
        db_path,
        account_id="discordgo",
        room_id="!room-2:beeper.local",
        participant_id="@pedroyan:beeper.local",
        full_name="PedroYan",
        is_self=False,
        identifiers=[("username", "pedroyan")],
    )
    _insert_message(
        db_path,
        room_id="!room-2:beeper.local",
        event_id="$event-2",
        sender_id="@pedroyan:beeper.local",
        timestamp_ms=1711000000000,
        message_type="TEXT",
        is_sent_by_me=False,
        text="discord ping",
    )

    adapter = BeeperAdapter()
    first_result = adapter.ingest(
        str(tmp_vault),
        db_path=str(db_path),
        media_root=str(media_root),
        max_threads=1,
        batch_size=1,
    )
    assert first_result.created == 3

    sync_state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert sync_state["beeper:single:all"]["last_completed_thread_id"] == "!room-1:beeper.local"

    second_result = adapter.ingest(
        str(tmp_vault),
        db_path=str(db_path),
        media_root=str(media_root),
        max_threads=1,
        batch_size=1,
    )
    assert second_result.created == 3

    sync_state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert sync_state["beeper:single:all"]["last_completed_thread_id"] == "!room-2:beeper.local"

    thread_files = sorted((tmp_vault / "BeeperThreads").rglob("*.md"))
    message_files = sorted((tmp_vault / "Beeper").rglob("*.md"))
    people_files = sorted((tmp_vault / "People").rglob("*.md"))
    assert len(thread_files) == 2
    assert len(message_files) == 2
    assert len(people_files) == 2

    thread_frontmatter, _, _ = read_note(tmp_vault, str(thread_files[0].relative_to(tmp_vault)))
    assert thread_frontmatter["type"] == "beeper_thread"
    assert thread_frontmatter["message_count"] == 1


def test_fetch_batches_matches_between_single_and_multi_worker_modes(tmp_vault, tmp_path):
    db_path, media_root = _create_index_db(tmp_path / "beeper-index-workers")
    media_file = media_root / "local.beeper.com" / "media-1"
    media_file.parent.mkdir(parents=True, exist_ok=True)
    media_file.write_bytes(b"img")

    for room_number in range(1, 4):
        room_id = f"!room-{room_number}:beeper.local"
        _insert_thread(
            db_path,
            room_id=room_id,
            account_id="discordgo",
            timestamp_ms=1710000000000 + room_number * 1000,
        )
        _insert_participant(
            db_path,
            account_id="discordgo",
            room_id=room_id,
            participant_id="@heegs:beeper.com",
            full_name="heegs",
            is_self=True,
        )
        _insert_participant(
            db_path,
            account_id="discordgo",
            room_id=room_id,
            participant_id=f"@friend-{room_number}:beeper.local",
            full_name=f"Friend {room_number}",
            is_self=False,
            identifiers=[("username", f"friend-{room_number}")],
        )
        _insert_message(
            db_path,
            room_id=room_id,
            event_id=f"$event-{room_number}-1",
            sender_id=f"@friend-{room_number}:beeper.local",
            timestamp_ms=1710000000000 + room_number * 1000,
            message_type="TEXT",
            is_sent_by_me=False,
            text=f"hello {room_number}",
        )
        _insert_message(
            db_path,
            room_id=room_id,
            event_id=f"$event-{room_number}-2",
            sender_id="@heegs:beeper.com",
            timestamp_ms=1710000000500 + room_number * 1000,
            message_type="IMAGE",
            is_sent_by_me=True,
            attachments=[
                {
                    "id": f"mxc://local.beeper.com/media-{room_number}",
                    "type": "img",
                    "fileName": f"photo-{room_number}.jpg",
                    "mimeType": "image/jpeg",
                    "fileSize": 3,
                    "srcURL": "mxc://local.beeper.com/media-1",
                    "size": {"width": 100, "height": 200},
                    "extra": {"caption": f"caption {room_number}"},
                }
            ],
        )

    adapter = BeeperAdapter()
    single = list(
        adapter.fetch_batches(
            str(tmp_vault),
            {},
            db_path=str(db_path),
            media_root=str(media_root),
            batch_size=3,
            workers=1,
        )
    )
    multi = list(
        adapter.fetch_batches(
            str(tmp_vault),
            {},
            db_path=str(db_path),
            media_root=str(media_root),
            batch_size=3,
            workers=4,
        )
    )
    assert [batch.sequence for batch in single] == [0, 1]
    assert [batch.sequence for batch in multi] == [0, 1]
    assert _normalize_items(single[0].items) == _normalize_items(multi[0].items)
    assert _normalize_items(single[1].items) == _normalize_items(multi[1].items)


def test_fetch_batches_resolves_sender_person_from_provider_handle_and_name(tmp_vault, tmp_path):
    db_path, media_root = _create_index_db(tmp_path / "beeper-index-identity")
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        json.dumps(
            {
                "discord:pedroyan": "[[Pedro Yan]]",
                "name:pedroyan": "[[Pedro Yan]]",
            }
        ),
        encoding="utf-8",
    )

    _insert_thread(
        db_path,
        room_id="!room-discord:beeper.local",
        account_id="discordgo",
        timestamp_ms=1712000000000,
    )
    _insert_participant(
        db_path,
        account_id="discordgo",
        room_id="!room-discord:beeper.local",
        participant_id="@heegs:beeper.com",
        full_name="heegs",
        is_self=True,
    )
    _insert_participant(
        db_path,
        account_id="discordgo",
        room_id="!room-discord:beeper.local",
        participant_id="@pedroyan:beeper.local",
        full_name="PedroYan",
        is_self=False,
        identifiers=[("username", "pedroyan")],
    )
    _insert_message(
        db_path,
        room_id="!room-discord:beeper.local",
        event_id="$event-discord-1",
        sender_id="@pedroyan:beeper.local",
        timestamp_ms=1712000000000,
        message_type="TEXT",
        is_sent_by_me=False,
        text="discord hello",
    )

    batches = list(
        BeeperAdapter().fetch_batches(
            str(tmp_vault),
            {},
            db_path=str(db_path),
            media_root=str(media_root),
            account_ids=["discordgo"],
            max_threads=1,
            batch_size=1,
        )
    )
    assert len(batches) == 2
    person_items = [item for item in batches[0].items if item["kind"] == "person"]
    message_items = [item for item in batches[1].items if item["kind"] == "message"]
    thread_items = [item for item in batches[1].items if item["kind"] == "thread"]
    assert len(person_items) == 1
    assert thread_items[0]["people"] == ["[[Pedro Yan]]"]
    assert message_items[0]["sender_person"] == "[[Pedro Yan]]"

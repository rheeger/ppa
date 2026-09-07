from __future__ import annotations

from archive_engine.thread_projection import (
    aggregate_messages,
    drain_pending,
    pending_receipt,
    pending_thread_uids,
    persist_pending_receipt,
    project_thread,
)
from archive_vault.vault import read_note


def test_aggregate_detects_earliest_only_change() -> None:
    first = aggregate_messages(
        [
            {"uid": "m2", "sent_at": "2024-01-02T00:00:00"},
            {"uid": "m3", "sent_at": "2024-01-03T00:00:00"},
        ]
    )
    second = aggregate_messages(
        [
            {"uid": "m1", "sent_at": "2024-01-01T00:00:00"},
            {"uid": "m2", "sent_at": "2024-01-02T00:00:00"},
            {"uid": "m3", "sent_at": "2024-01-03T00:00:00"},
        ]
    )
    assert second["first_message_at"] < first["first_message_at"]
    assert second["message_count"] == 3


def test_retry_after_child_exists_is_idempotent_recount(tmp_path) -> None:
    vault = tmp_path / "vault"
    dest = vault / "Messages" / "thread.md"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        "---\n"
        "uid: hfa-imessage-thread-t1\n"
        "type: imessage_thread\n"
        "source: [imessage]\n"
        "source_id: t1\n"
        "created: '2024-01-01'\n"
        "updated: '2024-01-01'\n"
        "summary: thread\n"
        "imessage_chat_id: chat-t1\n"
        "message_count: 0\n"
        "---\n",
        encoding="utf-8",
    )
    msgs = [
        {"uid": "m1", "sent_at": "2024-01-01T10:00:00"},
        {"uid": "m2", "sent_at": "2024-01-02T10:00:00"},
    ]
    first = project_thread(vault, "Messages/thread.md", msgs)
    second = project_thread(vault, "Messages/thread.md", msgs)
    fm, _b, _p = read_note(vault, "Messages/thread.md")
    assert int(fm["message_count"]) == 2
    assert first["updated"] is True
    assert second["updated"] is False


def test_pending_receipt_drains_through_projection(tmp_path) -> None:
    vault = tmp_path / "vault"
    dest = vault / "Messages" / "thread.md"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        "---\n"
        "uid: hfa-imessage-thread-t1\n"
        "type: imessage_thread\n"
        "source: [imessage]\n"
        "source_id: t1\n"
        "created: '2024-01-01'\n"
        "updated: '2024-01-01'\n"
        "summary: thread\n"
        "imessage_chat_id: chat-t1\n"
        "message_count: 0\n"
        "---\n",
        encoding="utf-8",
    )
    persist_pending_receipt(vault, pending_receipt("hfa-imessage-thread-t1", "rev-1"))
    assert pending_thread_uids(vault) == ["hfa-imessage-thread-t1"]
    rows = [
        {
            "rel_path": "Messages/thread.md",
            "frontmatter": {"uid": "hfa-imessage-thread-t1", "type": "imessage_thread"},
        },
        {
            "rel_path": "Messages/m1.md",
            "frontmatter": {
                "uid": "m1",
                "type": "imessage_message",
                "thread": "[[hfa-imessage-thread-t1]]",
                "sent_at": "2024-01-01T10:00:00",
            },
        },
    ]
    result = drain_pending(vault, rows)
    assert result["drained"] == 1
    assert pending_thread_uids(vault) == []
    fm, _b, _p = read_note(vault, "Messages/thread.md")
    assert int(fm["message_count"]) == 1

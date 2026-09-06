"""P01-B1 conversation bursts: short answers stay citable without rekeying history."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from archive_cli.chunk_builders import BurstAffectedResolver
from archive_cli.chunking import render_chunks_for_card
from archive_cli.conversation_bursts import (
    BURST_CHUNK_TYPE,
    burst_key_for,
    resolve_burst_affected,
    segment_conversation_bursts,
)
from archive_cli.index_config import BURST_ALGORITHM_VERSION, CHUNK_SCHEMA_VERSION
from archive_engine.contracts import AffectedContext

REPO = Path(__file__).resolve().parents[1]

ANSWER = "use port 8432"
THREAD_BODY = "Thread fixture linking messages."
ANSWER_MESSAGE_ID = "msg-answer"


def _msg(
    message_id: str,
    text: str,
    *,
    author: str = "Ada",
    timestamp: str = "",
    revision: str = "",
) -> dict[str, str]:
    payload = {
        "message_id": message_id,
        "text": text,
        "author": author,
        "timestamp": timestamp,
    }
    if revision:
        payload["revision"] = revision
    return payload


def long_thread_messages(*, include_answer: bool = True, extra: list[dict[str, str]] | None = None) -> list[dict[str, str]]:
    messages = [
        _msg(f"msg-fill-{index:03d}", f"status update {index} still waiting on deploy", timestamp=f"2026-03-10T10:{index:02d}:00+00:00")
        for index in range(24)
    ]
    if include_answer:
        messages.insert(
            12,
            _msg(
                ANSWER_MESSAGE_ID,
                ANSWER,
                author="Bea",
                timestamp="2026-03-10T10:12:30+00:00",
                revision="rev-answer-1",
            ),
        )
    if extra:
        messages.extend(extra)
    return messages


def thread_frontmatter(
    *,
    card_type: str = "email_thread",
    uid: str = "",
    messages: list[dict[str, str]] | None = None,
) -> dict[str, object]:
    prefix = {
        "email_thread": "hfa-email-thread-p01b1",
        "imessage_thread": "hfa-imessage-thread-p01b1",
        "beeper_thread": "hfa-beeper-thread-p01b1",
    }[card_type]
    source = {
        "email_thread": ["gmail"],
        "imessage_thread": ["imessage"],
        "beeper_thread": ["beeper"],
    }[card_type]
    card: dict[str, object] = {
        "uid": uid or prefix,
        "type": card_type,
        "source": source,
        "source_id": "thread-p01b1",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Deploy port question",
        "subject": "Which port should we use?",
        "participants": ["ada@example.com", "bea@example.com"],
        "participant_handles": ["ada@example.com", "bea@example.com"],
        "conversation_messages": messages if messages is not None else long_thread_messages(),
    }
    if card_type == "email_thread":
        card["gmail_thread_id"] = "thread-p01b1"
    elif card_type == "imessage_thread":
        card["imessage_chat_id"] = "chat-p01b1"
        card["display_name"] = "Which port should we use?"
    else:
        card["beeper_room_id"] = "room-p01b1"
        card["thread_title"] = "Which port should we use?"
    return card


def burst_chunks(frontmatter: dict[str, object], body: str = THREAD_BODY) -> list[dict[str, object]]:
    return [chunk for chunk in render_chunks_for_card(frontmatter, body) if chunk["chunk_type"] == BURST_CHUNK_TYPE]


def recall_answer(chunks: list[dict[str, object]]) -> float:
    return 1.0 if any(ANSWER in str(chunk.get("content", "")) for chunk in chunks) else 0.0


def measure_burst_recall(card_type: str = "email_thread") -> dict[str, object]:
    frontmatter = thread_frontmatter(card_type=card_type)
    chunks = render_chunks_for_card(frontmatter, THREAD_BODY)
    before = [chunk for chunk in chunks if chunk["chunk_type"] != BURST_CHUNK_TYPE]
    after = chunks
    bursts = [chunk for chunk in chunks if chunk["chunk_type"] == BURST_CHUNK_TYPE]
    hit = next(chunk for chunk in bursts if ANSWER in str(chunk["content"]))
    return {
        "card_type": card_type,
        "before_recall_at_1": recall_answer(before),
        "after_recall_at_1": recall_answer(after),
        "burst_count": len(bursts),
        "hit_burst_key": hit["burst_key"],
        "hit_message_ids": list(hit["message_ids"]),
        "hit_has_prefix": "channel:" in str(hit["content"]) and "subject:" in str(hit["content"]),
        "raw_windows_preserved": any("thread" in str(chunk["chunk_type"]) or chunk["chunk_type"] == "body" for chunk in before),
    }


def test_chunk_schema_and_serving_format_untouched():
    schema = (REPO / "archive_crate/src/serving_index/schema.rs").read_text(encoding="utf-8")
    freeze = (REPO / "archive_tests/acceptance/data/p01b_format_freeze.json").read_text(encoding="utf-8")
    assert CHUNK_SCHEMA_VERSION == 6
    assert BURST_ALGORITHM_VERSION == "p01b1-burst-1"
    assert "pub const SERVING_INDEX_FORMAT_VERSION: u32 = 2;" in schema
    assert 'pub const VECTOR_IMPL: &str = "ivf_centroids_v2";' in schema
    assert '"serving_index_format_version": 2' in freeze
    assert '"vector_impl": "ivf_centroids_v2"' in freeze


def test_short_answer_is_missing_from_legacy_windows_then_found_in_burst():
    report = measure_burst_recall("email_thread")
    assert report["before_recall_at_1"] == 0.0
    assert report["after_recall_at_1"] == 1.0
    assert ANSWER_MESSAGE_ID in report["hit_message_ids"]
    assert report["hit_has_prefix"] is True


@pytest.mark.parametrize("card_type", ["email_thread", "imessage_thread", "beeper_thread"])
def test_conversation_channels_emit_prefixed_bursts(card_type: str):
    report = measure_burst_recall(card_type)
    assert report["after_recall_at_1"] == 1.0
    assert report["before_recall_at_1"] == 0.0


def test_append_does_not_rekey_earlier_bursts():
    base = thread_frontmatter()
    before_keys = [chunk["burst_key"] for chunk in burst_chunks(base)]
    appended = deepcopy(base)
    appended["conversation_messages"] = long_thread_messages(
        extra=[_msg("msg-later", "shipping tomorrow", timestamp="2026-03-10T11:00:00+00:00")]
    )
    after_keys = [chunk["burst_key"] for chunk in burst_chunks(appended)]
    assert before_keys
    assert before_keys == after_keys[: len(before_keys)]
    assert len(after_keys) == len(before_keys) + 1


def test_edit_retires_only_changed_burst():
    base = thread_frontmatter()
    previous = [chunk["burst_key"] for chunk in burst_chunks(base)]
    edited = deepcopy(base)
    messages = list(edited["conversation_messages"])
    for item in messages:
        if item["message_id"] == ANSWER_MESSAGE_ID:
            item["text"] = "use port 9000"
            item["revision"] = "rev-answer-2"
    edited["conversation_messages"] = messages
    current = burst_chunks(edited)
    current_keys = [chunk["burst_key"] for chunk in current]
    result = resolve_burst_affected(
        thread_uid=str(base["uid"]),
        changed_message_ids=[ANSWER_MESSAGE_ID],
        previous_burst_keys=previous,
        current_messages=messages,
        channel="email",
    )
    assert result.status == "resolved"
    retired = set(previous) - set(current_keys)
    assert set(result.retired_burst_keys) == retired
    assert result.replacement_burst_keys
    assert retired.isdisjoint(current_keys)
    stable = set(previous) & set(current_keys)
    assert len(stable) == len(previous) - 1


def test_delete_retires_removed_burst():
    base = thread_frontmatter()
    previous = [chunk["burst_key"] for chunk in burst_chunks(base)]
    remaining = [item for item in base["conversation_messages"] if item["message_id"] != ANSWER_MESSAGE_ID]
    deleted = {**base, "conversation_messages": remaining}
    current_keys = [chunk["burst_key"] for chunk in burst_chunks(deleted)]
    result = resolve_burst_affected(
        thread_uid=str(base["uid"]),
        changed_message_ids=[ANSWER_MESSAGE_ID],
        previous_burst_keys=previous,
        current_messages=remaining,
        channel="email",
    )
    assert ANSWER not in "\n".join(str(chunk["content"]) for chunk in burst_chunks(deleted))
    assert set(result.retired_burst_keys) == set(previous) - set(current_keys)


def test_email_uses_one_message_per_burst():
    messages = long_thread_messages()
    bursts = segment_conversation_bursts(messages, channel="email")
    assert [list(item.message_ids) for item in bursts] == [[item["message_id"]] for item in sorted(messages, key=lambda row: (row["timestamp"], row["message_id"]))]


def test_chat_groups_same_author_until_gap_or_author_change():
    messages = [
        _msg("m1", "ping", author="Ada", timestamp="2026-03-10T10:00:00+00:00"),
        _msg("m2", "still here", author="Ada", timestamp="2026-03-10T10:02:00+00:00"),
        _msg("m3", "yes", author="Bea", timestamp="2026-03-10T10:02:10+00:00"),
        _msg("m4", "back later", author="Ada", timestamp="2026-03-10T10:20:00+00:00"),
    ]
    bursts = segment_conversation_bursts(messages, channel="imessage")
    assert [list(item.message_ids) for item in bursts] == [["m1", "m2"], ["m3"], ["m4"]]


def test_missing_timestamps_do_not_split_on_time():
    messages = [
        _msg("m1", "one", author="Ada"),
        _msg("m2", "two", author="Ada"),
        _msg("m3", "three", author="Bea"),
    ]
    bursts = segment_conversation_bursts(messages, channel="imessage")
    assert [list(item.message_ids) for item in bursts] == [["m1", "m2"], ["m3"]]


def test_boilerplate_is_emitted_but_not_embed_eligible():
    frontmatter = thread_frontmatter(
        messages=[
            _msg("r1", "Liked a message", author="Ada", timestamp="2026-03-10T10:00:00+00:00"),
            _msg("r2", "yes", author="Bea", timestamp="2026-03-10T10:00:05+00:00"),
        ]
    )
    chunks = burst_chunks(frontmatter)
    liked = next(chunk for chunk in chunks if "Liked a message" in str(chunk["content"]) or "liked a message" in str(chunk["content"]).lower())
    yes = next(chunk for chunk in chunks if str(chunk["content"]).endswith("yes") or "\nyes" in str(chunk["content"]))
    assert liked["embed_eligible"] is False
    assert yes["embed_eligible"] is True


def test_wikilink_messages_and_plain_body_do_not_invent_bursts():
    frontmatter = {
        "uid": "hfa-email-thread-links",
        "type": "email_thread",
        "source": ["gmail"],
        "source_id": "thread-links",
        "created": "2026-03-10",
        "updated": "2026-03-10",
        "summary": "Linked thread",
        "subject": "Linked thread",
        "gmail_thread_id": "thread-links",
        "messages": ["[[hfa-email-message-1]]", "[[hfa-email-message-2]]"],
    }
    assert burst_chunks(frontmatter, THREAD_BODY) == []


def test_burst_transcript_marker_is_required():
    body = "[2026-03-10T10:00:00+00:00|Ada|mid=m1]\nuse port 8432\n"
    frontmatter = thread_frontmatter(messages=[])
    del frontmatter["conversation_messages"]
    assert burst_chunks(frontmatter, body) == []
    marked = "## burst-transcript\n[2026-03-10T10:00:00+00:00|Ada|mid=m1]\nuse port 8432\n"
    chunks = burst_chunks(frontmatter, marked)
    assert chunks
    assert ANSWER in str(chunks[0]["content"])


def test_burst_keys_are_stable_for_same_identities():
    messages = [_msg("m1", "hello"), _msg("m2", "world")]
    first = [item.burst_key for item in segment_conversation_bursts(messages, channel="email")]
    second = [item.burst_key for item in segment_conversation_bursts(messages, channel="email")]
    assert first == second
    assert all(key.startswith("burst-") and len(key) == 30 for key in first)
    hashes = [__import__("hashlib").sha256(item["text"].encode()).hexdigest() for item in messages]
    assert first[0] == burst_key_for(["m1"], [hashes[0]])


def test_affected_resolver_uid_only_stays_pending():
    resolver = BurstAffectedResolver()
    pending = resolver.resolve_affected("hfa-email-thread-p01b1", "rev-1")
    assert isinstance(pending, AffectedContext)
    assert pending.status == "reconciliation_pending"
    rich = resolver.resolve_burst_affected(
        "hfa-email-thread-p01b1",
        changed_message_ids=(ANSWER_MESSAGE_ID,),
        previous_burst_keys=("burst-old",),
        current_messages=tuple(long_thread_messages()),
        channel="email",
    )
    assert rich.status == "resolved"
    assert rich.replacement_burst_keys


def test_python_rust_burst_parity():
    archive_crate = pytest.importorskip("archive_crate")
    for card_type in ("email_thread", "imessage_thread", "beeper_thread"):
        frontmatter = thread_frontmatter(card_type=card_type)
        py_chunks = render_chunks_for_card(frontmatter, THREAD_BODY)
        rust_chunks = archive_crate.render_chunks_for_card(frontmatter, THREAD_BODY)
        assert rust_chunks == py_chunks


def test_empty_structured_messages_do_not_parse_fixture_body():
    frontmatter = thread_frontmatter(messages=[])
    assert burst_chunks(frontmatter, THREAD_BODY) == []

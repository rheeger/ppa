"""Conversation burst segmentation. Does not replace canonical message cards."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from archive_engine.contracts import AffectedContext

from .index_config import (
    BURST_ALGORITHM_VERSION,
    get_burst_chat_gap_seconds,
    get_burst_token_limit,
)

CONVERSATION_CARD_TYPES = frozenset(
    {"email_thread", "imessage_thread", "beeper_thread"}
)
BURST_CHUNK_TYPE = "conversation_burst"
TRANSCRIPT_MARKER = "## burst-transcript"

_BOILERPLATE_PREFIXES = (
    "liked a message",
    "loved a message",
    "emphasized a message",
    "reacted to",
    "tapback",
    "reacted ",
)


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\x00", "")).strip()


def _token_count(content: str) -> int:
    return max(len(content.split()), 1) if content.strip() else 0


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ConversationMessage:
    message_id: str
    timestamp: str
    author: str
    text: str
    source_uid: str = ""
    revision: str = ""

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> ConversationMessage | None:
        text = _clean(raw.get("text") or raw.get("body") or raw.get("content"))
        message_id = _clean(raw.get("message_id") or raw.get("uid") or raw.get("id"))
        if not text or not message_id:
            return None
        return cls(
            message_id=message_id,
            timestamp=_clean(raw.get("timestamp") or raw.get("sent_at") or raw.get("created")),
            author=_clean(raw.get("author") or raw.get("from_name") or raw.get("from") or "unknown"),
            text=text,
            source_uid=_clean(raw.get("source_uid") or raw.get("uid") or message_id),
            revision=_clean(raw.get("revision") or raw.get("source_revision") or raw.get("content_hash")),
        )


@dataclass(frozen=True)
class ConversationBurst:
    burst_key: str
    sequence: int
    message_ids: tuple[str, ...]
    source_uids: tuple[str, ...]
    source_revisions: tuple[str, ...]
    authors: tuple[str, ...]
    text: str
    embed_eligible: bool
    algorithm_version: str = BURST_ALGORITHM_VERSION


@dataclass(frozen=True)
class BurstAffectedResult:
    thread_uid: str
    affected_uids: tuple[str, ...]
    retired_burst_keys: tuple[str, ...]
    replacement_burst_keys: tuple[str, ...]
    status: str

    def to_affected_context(self, revision: str) -> AffectedContext:
        return AffectedContext(
            uid=self.thread_uid,
            revision=revision,
            related_uids=self.affected_uids,
            status="resolved" if self.status == "resolved" else "reconciliation_pending",
        )


def conversation_channel(card_type: str) -> str:
    if card_type.startswith("email"):
        return "email"
    if card_type.startswith("imessage"):
        return "imessage"
    if card_type.startswith("beeper"):
        return "beeper"
    return "chat"


def parse_timestamp(raw: str) -> datetime | None:
    text = _clean(raw)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def is_boilerplate(text: str) -> bool:
    lowered = _clean(text).casefold()
    return any(lowered.startswith(prefix) for prefix in _BOILERPLATE_PREFIXES)


def burst_key_for(message_ids: Sequence[str], content_hashes: Sequence[str]) -> str:
    payload = json.dumps(
        {
            "algorithm": BURST_ALGORITHM_VERSION,
            "content_hashes": list(content_hashes),
            "message_ids": list(message_ids),
        },
        sort_keys=True,
    )
    return "burst-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _structured_messages(frontmatter: Mapping[str, Any]) -> list[ConversationMessage]:
    raw = frontmatter.get("conversation_messages")
    if raw is None:
        raw = frontmatter.get("messages_json")
    if isinstance(raw, str):
        text = raw.strip()
        if text[:1] in "[{":
            try:
                raw = json.loads(text)
            except json.JSONDecodeError:
                return []
    if not isinstance(raw, list):
        return []
    out: list[ConversationMessage] = []
    for item in raw:
        if isinstance(item, Mapping):
            parsed = ConversationMessage.from_mapping(item)
            if parsed is not None:
                out.append(parsed)
    return out


_TRANSCRIPT_RE = re.compile(
    r"^\[(?P<ts>[^|\]]+)\|(?P<author>[^|\]]+)\|mid=(?P<mid>[^|\]]+)(?:\|rev=(?P<rev>[^|\]]+))?\]\s*$"
)


def _transcript_messages(body: str) -> list[ConversationMessage]:
    if TRANSCRIPT_MARKER not in body:
        return []
    start = body.find(TRANSCRIPT_MARKER)
    lines = body[start + len(TRANSCRIPT_MARKER) :].splitlines()
    messages: list[ConversationMessage] = []
    current: dict[str, str] | None = None
    body_lines: list[str] = []

    def flush() -> None:
        nonlocal current, body_lines
        if current is None:
            return
        parsed = ConversationMessage.from_mapping({**current, "text": "\n".join(body_lines)})
        if parsed is not None:
            messages.append(parsed)
        current = None
        body_lines = []

    for line in lines:
        match = _TRANSCRIPT_RE.match(line.strip())
        if match:
            flush()
            current = {
                "timestamp": match.group("ts").strip(),
                "author": match.group("author").strip(),
                "message_id": match.group("mid").strip(),
                "revision": (match.group("rev") or "").strip(),
            }
            continue
        if current is not None:
            body_lines.append(line)
    flush()
    return messages


def load_conversation_messages(frontmatter: Mapping[str, Any], body: str) -> list[ConversationMessage]:
    structured = _structured_messages(frontmatter)
    if structured:
        return structured
    return _transcript_messages(body)


def _sort_messages(messages: Sequence[ConversationMessage]) -> list[ConversationMessage]:
    return sorted(messages, key=lambda item: (item.timestamp, item.message_id))


def _gap_seconds(left: ConversationMessage, right: ConversationMessage) -> int | None:
    start = parse_timestamp(left.timestamp)
    end = parse_timestamp(right.timestamp)
    if start is None or end is None:
        return None
    return int((end - start).total_seconds())


def _pack_groups(groups: Sequence[Sequence[ConversationMessage]], token_limit: int) -> list[list[ConversationMessage]]:
    packed: list[list[ConversationMessage]] = []
    for group in groups:
        current: list[ConversationMessage] = []
        current_tokens = 0
        for message in group:
            tokens = _token_count(message.text)
            if current and current_tokens + tokens > token_limit:
                packed.append(current)
                current = [message]
                current_tokens = tokens
                continue
            current.append(message)
            current_tokens += tokens
        if current:
            packed.append(current)
    return packed


def segment_conversation_bursts(
    messages: Sequence[ConversationMessage] | Sequence[Mapping[str, Any]],
    *,
    channel: str,
) -> list[ConversationBurst]:
    parsed: list[ConversationMessage] = []
    for item in messages:
        if isinstance(item, ConversationMessage):
            parsed.append(item)
        elif isinstance(item, Mapping):
            mapped = ConversationMessage.from_mapping(item)
            if mapped is not None:
                parsed.append(mapped)
    ordered = _sort_messages(parsed)
    if not ordered:
        return []
    token_limit = get_burst_token_limit()
    gap_limit = get_burst_chat_gap_seconds()
    if channel == "email":
        groups = [[item] for item in ordered]
    else:
        groups = []
        current: list[ConversationMessage] = []
        for message in ordered:
            if not current:
                current = [message]
                continue
            previous = current[-1]
            author_changed = previous.author.casefold() != message.author.casefold()
            gap = _gap_seconds(previous, message)
            if author_changed or (gap is not None and gap > gap_limit):
                groups.append(current)
                current = [message]
                continue
            current.append(message)
        if current:
            groups.append(current)
    bursts: list[ConversationBurst] = []
    for sequence, group in enumerate(_pack_groups(groups, token_limit)):
        hashes = [_content_hash(item.text) for item in group]
        ids = tuple(item.message_id for item in group)
        bursts.append(
            ConversationBurst(
                burst_key=burst_key_for(ids, hashes),
                sequence=sequence,
                message_ids=ids,
                source_uids=tuple(item.source_uid or item.message_id for item in group),
                source_revisions=tuple(item.revision for item in group if item.revision),
                authors=tuple(item.author for item in group),
                text="\n".join(item.text for item in group),
                embed_eligible=any(not is_boilerplate(item.text) for item in group),
            )
        )
    return bursts


def burst_prefix(frontmatter: Mapping[str, Any], *, channel: str) -> str:
    sources = frontmatter.get("source")
    if isinstance(sources, list):
        source = _clean(sources[0] if sources else channel)
    else:
        source = _clean(sources) or channel
    participants = frontmatter.get("participants") or frontmatter.get("participant_handles") or []
    if not isinstance(participants, list):
        participants = [participants]
    lines = [
        f"channel: {channel}",
        f"subject: {_clean(frontmatter.get('subject') or frontmatter.get('display_name') or frontmatter.get('summary'))}",
        f"participants: {', '.join(_clean(item) for item in participants if _clean(item))}",
        f"source: {source}",
    ]
    return "\n".join(lines).rstrip() + "\n\n"


def burst_chunk_records(
    frontmatter: Mapping[str, Any],
    body: str,
    *,
    card_type: str,
) -> list[dict[str, Any]]:
    channel = conversation_channel(card_type)
    bursts = segment_conversation_bursts(load_conversation_messages(frontmatter, body), channel=channel)
    if not bursts:
        return []
    parent = _clean(frontmatter.get("uid"))
    prefix = burst_prefix(frontmatter, channel=channel)
    records = []
    for burst in bursts:
        records.append(
            {
                "chunk_type": BURST_CHUNK_TYPE,
                "burst_key": burst.burst_key,
                "burst_sequence": burst.sequence,
                "message_ids": list(burst.message_ids),
                "parent_thread": parent,
                "algorithm_version": burst.algorithm_version,
                "embed_eligible": burst.embed_eligible,
                "source_revisions": list(burst.source_revisions),
                "content": prefix + burst.text,
            }
        )
    return records


def resolve_burst_affected(
    *,
    thread_uid: str,
    changed_message_ids: Sequence[str] = (),
    previous_burst_keys: Sequence[str] = (),
    current_messages: Sequence[ConversationMessage] | Sequence[Mapping[str, Any]] = (),
    channel: str,
) -> BurstAffectedResult:
    if not thread_uid:
        return BurstAffectedResult(
            thread_uid="",
            affected_uids=(),
            retired_burst_keys=(),
            replacement_burst_keys=(),
            status="reconciliation_pending",
        )
    current = segment_conversation_bursts(current_messages, channel=channel)
    current_keys = [item.burst_key for item in current]
    previous = [str(item) for item in previous_burst_keys if str(item)]
    retired = tuple(key for key in previous if key not in current_keys)
    changed = {str(item) for item in changed_message_ids if str(item)}
    replacement = tuple(
        item.burst_key for item in current if changed.intersection(item.message_ids)
    )
    affected = [thread_uid]
    for item in current:
        for uid, message_id in zip(item.source_uids, item.message_ids, strict=False):
            if message_id in changed and uid not in affected:
                affected.append(uid)
    for message_id in changed:
        if message_id not in affected:
            affected.append(message_id)
    return BurstAffectedResult(
        thread_uid=thread_uid,
        affected_uids=tuple(affected),
        retired_burst_keys=retired,
        replacement_burst_keys=replacement,
        status="resolved",
    )


class BurstAffectedResolver:
    """P01-B1 burst invalidation. uid/revision-only resolve stays pending."""

    def resolve_affected(self, uid: str, revision: str) -> AffectedContext | None:
        if not uid:
            return None
        return AffectedContext(uid=uid, revision=revision, status="reconciliation_pending")

    def resolve_burst_affected(
        self,
        thread_uid: str,
        *,
        changed_message_ids: Sequence[str] = (),
        previous_burst_keys: Sequence[str] = (),
        current_messages: Sequence[Mapping[str, Any]] | Sequence[ConversationMessage] = (),
        channel: str,
    ) -> BurstAffectedResult:
        return resolve_burst_affected(
            thread_uid=thread_uid,
            changed_message_ids=changed_message_ids,
            previous_burst_keys=previous_burst_keys,
            current_messages=current_messages,
            channel=channel,
        )

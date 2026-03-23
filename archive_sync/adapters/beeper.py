"""Beeper DM adapter backed by the local BeeperTexts SQLite index."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any

from .base import (BaseAdapter, FetchedBatch, IngestResult,
                           deterministic_provenance)
from hfa.config import load_config
from hfa.identity import IdentityCache
from hfa.identity_resolver import merge_into_existing
from hfa.provenance import ProvenanceEntry, compute_input_hash
from hfa.schema import (BeeperAttachmentCard, BeeperMessageCard,
                        BeeperThreadCard, PersonCard)
from hfa.slugger import normalize_for_slug
from hfa.sync_state import load_sync_state, update_cursor
from hfa.uid import generate_uid
from hfa.vault import write_card

THREAD_SOURCE = "beeper.thread"
MESSAGE_SOURCE = "beeper.message"
ATTACHMENT_SOURCE = "beeper.attachment"
DEFAULT_DB_PATH = Path.home() / "Library" / "Application Support" / "BeeperTexts" / "index.db"
DEFAULT_MEDIA_ROOT = Path.home() / "Library" / "Application Support" / "BeeperTexts" / "media"
WHITESPACE_RE = re.compile(r"\s+")


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return WHITESPACE_RE.sub(" ", str(value).strip())


def _parse_csv(value: str | list[str] | tuple[str, ...] | None, *, default: list[str] | None = None) -> list[str]:
    if value is None:
        return list(default or [])
    if isinstance(value, (list, tuple)):
        raw_items = [str(item) for item in value]
    else:
        raw_items = str(value).split(",")
    return [item.strip() for item in raw_items if item and item.strip()]


def _json_load(value: str | bytes | None, default: Any) -> Any:
    if value in (None, "", b""):
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _parse_timestamp(value: Any) -> str:
    if value in (None, "", 0):
        return ""
    if isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    raw = _clean(value)
    if not raw:
        return ""
    if raw.isdigit():
        parsed = datetime.fromtimestamp(int(raw) / 1000, tz=timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _date_bucket(value: str) -> str:
    if len(value) >= 10:
        return value[:10]
    return date.today().isoformat()


def _preview_text(value: str, limit: int = 120) -> str:
    cleaned = _clean(value)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(limit - 1, 1)].rstrip() + "…"


def _chunked(values: list[str], size: int = 900) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _thread_uid(room_id: str) -> str:
    return generate_uid("beeper-thread", THREAD_SOURCE, room_id)


def _message_uid(event_id: str) -> str:
    return generate_uid("beeper-message", MESSAGE_SOURCE, event_id)


def _attachment_uid(event_id: str, attachment_id: str) -> str:
    return generate_uid("beeper-attachment", ATTACHMENT_SOURCE, f"{event_id}:{attachment_id}")


def _wikilink_from_uid(uid: str) -> str:
    return f"[[{uid}]]"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        cleaned = _clean(value)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        out.append(cleaned)
    return out


def _media_cache_path(src_url: str, media_root: Path) -> str:
    raw = _clean(src_url)
    if not raw.startswith("mxc://"):
        return ""
    without_query = raw.split("?", 1)[0]
    host_and_id = without_query.removeprefix("mxc://")
    host, _, media_id = host_and_id.partition("/")
    if not host or not media_id:
        return ""
    candidate = media_root / host / media_id
    return str(candidate) if candidate.exists() else ""


def _best_identifier(identifiers: list[tuple[str, str]]) -> str:
    priorities = {"email": 0, "phone": 1, "username": 2}
    if not identifiers:
        return ""
    ordered = sorted(identifiers, key=lambda item: (priorities.get(item[0], 99), item[1]))
    identifier_type, value = ordered[0]
    return f"{identifier_type}:{value}"


def _identifier_value(token: str) -> str:
    return token.split(":", 1)[1] if ":" in token else token


def _identity_prefix(account_id: str, identifier_type: str) -> str | None:
    normalized_account = _clean(account_id).split(".", 1)[0].lower()
    normalized_type = _clean(identifier_type).lower()
    if normalized_type == "email":
        return "email"
    if normalized_type == "phone":
        return "phone"
    if normalized_type == "username":
        if normalized_account == "linkedin":
            return "linkedin"
        if normalized_account == "twitter":
            return "twitter"
        if normalized_account == "instagramgo":
            return "instagram"
        if normalized_account == "instagram":
            return "instagram"
        if normalized_account == "telegram":
            return "telegram"
        if normalized_account == "discordgo":
            return "discord"
        if normalized_account == "discord":
            return "discord"
    return None


def _message_body(message_type: str, payload: dict[str, Any]) -> str:
    text = _clean(payload.get("text", ""))
    if text:
        return text

    attachments = payload.get("attachments") or []
    for attachment in attachments:
        caption = _clean((attachment.get("extra") or {}).get("caption", ""))
        if caption:
            return caption

    if message_type == "REACTION":
        reaction = _clean((payload.get("action") or {}).get("reactionKey", "")) or _clean(
            ((payload.get("extra") or {}).get("partialReactionContent") or {}).get("description", "")
        )
        related = _clean(payload.get("linkedMessageID", "")) or _clean(
            ((payload.get("extra") or {}).get("partialReactionContent") or {}).get("relatedEventID", "")
        )
        if reaction and related:
            return f"Reacted with {reaction} to {related}"
        if reaction:
            return f"Reacted with {reaction}"

    if message_type == "MEMBERSHIP":
        action = payload.get("action") or {}
        action_type = _clean(action.get("type", "")).replace("_", " ")
        participants = [
            _clean(participant.get("fullName", ""))
            for participant in action.get("participants", []) or []
            if isinstance(participant, dict)
        ]
        actor = _clean(action.get("actorParticipantID", ""))
        fragments = [fragment for fragment in [", ".join(participants), action_type, actor] if fragment]
        return " | ".join(fragments)

    return ""


def _message_body_sha(
    *,
    event_id: str,
    sent_at: str,
    message_type: str,
    sender_id: str,
    body: str,
    reaction_key: str,
    attachment_ids: list[str],
) -> str:
    return compute_input_hash(
        {
            "event_id": event_id,
            "sent_at": sent_at,
            "message_type": message_type,
            "sender_id": sender_id,
            "body": body,
            "reaction_key": reaction_key,
            "attachment_ids": attachment_ids,
        }
    )


def _attachment_metadata_sha(payload: dict[str, Any]) -> str:
    return compute_input_hash(
        {
            "event_id": _clean(payload.get("event_id", "")),
            "attachment_id": _clean(payload.get("attachment_id", "")),
            "filename": _clean(payload.get("filename", "")),
            "mime_type": _clean(payload.get("mime_type", "")),
            "size_bytes": int(payload.get("size_bytes", 0) or 0),
            "src_url": _clean(payload.get("src_url", "")),
            "cached_path": _clean(payload.get("cached_path", "")),
            "attachment_type": _clean(payload.get("attachment_type", "")),
            "width": int(payload.get("width", 0) or 0),
            "height": int(payload.get("height", 0) or 0),
            "duration_ms": int(payload.get("duration_ms", 0) or 0),
            "is_voice_note": bool(payload.get("is_voice_note", False)),
            "is_gif": bool(payload.get("is_gif", False)),
            "is_sticker": bool(payload.get("is_sticker", False)),
        }
    )


def _thread_body_sha(payload: list[dict[str, Any]]) -> str:
    return compute_input_hash({"messages": payload})


@dataclass(slots=True)
class ParticipantRecord:
    participant_id: str
    full_name: str
    is_self: bool
    identifiers: list[tuple[str, str]]

    def identifier_tokens(self) -> list[str]:
        return _dedupe([f"{identifier_type}:{value}" for identifier_type, value in self.identifiers])

    def best_identifier_token(self) -> str:
        return _best_identifier(self.identifiers)


@dataclass(slots=True)
class PlannedPersonWrite:
    card: PersonCard
    provenance: dict[str, ProvenanceEntry]
    body: str
    rel_path: Path
    wikilink: str


def _clone_identity_cache(cache: IdentityCache) -> IdentityCache:
    clone = object.__new__(IdentityCache)
    clone.vault_path = cache.vault_path
    clone.entries = dict(cache.entries)
    return clone


class BeeperIndex:
    """Readonly access to the BeeperTexts index database."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()
        if not self.db_path.exists():
            raise FileNotFoundError(f"Beeper index.db not found: {self.db_path}")
        self.conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    def list_threads(
        self,
        *,
        after_timestamp: int,
        after_thread_id: str,
        thread_types: list[str],
        account_ids: list[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        conditions = ["(timestamp > ? OR (timestamp = ? AND threadID > ?))"]
        params: list[Any] = [after_timestamp, after_timestamp, after_thread_id]
        if thread_types:
            placeholders = ",".join("?" for _ in thread_types)
            conditions.append(f"json_extract(thread, '$.type') IN ({placeholders})")
            params.extend(thread_types)
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            conditions.append(f"accountID IN ({placeholders})")
            params.extend(account_ids)
        params.append(limit)
        query = f"""
            SELECT threadID, accountID, timestamp, thread
            FROM threads
            WHERE {' AND '.join(conditions)}
            ORDER BY timestamp ASC, threadID ASC
            LIMIT ?
        """
        rows = self.conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]

    def participants_for_room(self, room_id: str) -> list[ParticipantRecord]:
        return self.participants_for_rooms([room_id]).get(room_id, [])

    def participants_for_rooms(self, room_ids: list[str]) -> dict[str, list[ParticipantRecord]]:
        if not room_ids:
            return {}
        records_by_room: dict[str, dict[str, ParticipantRecord]] = {}
        for batch in _chunked(room_ids):
            placeholders = ",".join("?" for _ in batch)
            rows = self.conn.execute(
                f"""
                SELECT p.room_id,
                       p.id AS participant_id,
                       p.full_name,
                       COALESCE(p.is_self, 0) AS is_self,
                       pi.identifier,
                       pi.identifier_type
                FROM participants p
                LEFT JOIN participant_identifiers pi
                  ON pi.account_id = p.account_id
                 AND pi.participant_id = p.id
                WHERE p.room_id IN ({placeholders})
                ORDER BY p.room_id ASC, p.id ASC, pi.identifier_type ASC, pi.identifier ASC
                """,
                tuple(batch),
            ).fetchall()
            for row in rows:
                room_id = _clean(row["room_id"])
                participant_id = _clean(row["participant_id"])
                if not room_id or not participant_id:
                    continue
                room_records = records_by_room.setdefault(room_id, {})
                record = room_records.get(participant_id)
                if record is None:
                    record = ParticipantRecord(
                        participant_id=participant_id,
                        full_name=_clean(row["full_name"]),
                        is_self=bool(row["is_self"]),
                        identifiers=[],
                    )
                    room_records[participant_id] = record
                identifier = _clean(row["identifier"])
                identifier_type = _clean(row["identifier_type"]).lower()
                if identifier and identifier_type and (identifier_type, identifier) not in record.identifiers:
                    record.identifiers.append((identifier_type, identifier))
        return {
            room_id: list(room_records.values())
            for room_id, room_records in records_by_room.items()
        }

    def messages_for_room(self, room_id: str) -> list[dict[str, Any]]:
        return self.messages_for_rooms([room_id]).get(room_id, [])

    def messages_for_rooms(self, room_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        if not room_ids:
            return {}
        mapping: dict[str, list[dict[str, Any]]] = {}
        for batch in _chunked(room_ids):
            placeholders = ",".join("?" for _ in batch)
            rows = self.conn.execute(
                f"""
                SELECT id,
                       roomID,
                       eventID,
                       senderContactID,
                       timestamp,
                       isEdited,
                       lastEditionID,
                       isDeleted,
                       inReplyToID,
                       type,
                       isSentByMe,
                       protocol,
                       lastEditionTimestamp,
                       message
                FROM mx_room_messages
                WHERE roomID IN ({placeholders})
                  AND type != 'HIDDEN'
                ORDER BY roomID ASC, timestamp ASC, id ASC
                """,
                tuple(batch),
            ).fetchall()
            for row in rows:
                room_id = _clean(row["roomID"])
                if not room_id:
                    continue
                mapping.setdefault(room_id, []).append(dict(row))
        return mapping


class BeeperAdapter(BaseAdapter):
    source_id = "beeper"
    preload_existing_uid_index = False
    parallel_person_matching = True
    parallel_person_match_default_workers = 8

    def _adapter_log(self, message: str, *, verbose: bool) -> None:
        if not verbose:
            return
        timestamp = datetime.now().isoformat(timespec="seconds")
        print(f"[{timestamp}] {self.source_id}: {message}", flush=True)

    @staticmethod
    def _worker_count(workers: Any) -> int:
        raw_value = workers if workers not in (None, "") else os.environ.get("HFA_BEEPER_WORKERS")
        default = max(1, min(8, os.cpu_count() or 1))
        if raw_value in (None, ""):
            return default
        try:
            return max(1, int(raw_value))
        except (TypeError, ValueError):
            return default

    def get_cursor_key(self, **kwargs) -> str:
        thread_types = "+".join(sorted(_parse_csv(kwargs.get("thread_types"), default=["single"]))) or "single"
        account_ids = "+".join(sorted(_parse_csv(kwargs.get("account_ids")))) or "all"
        return f"{self.source_id}:{thread_types}:{account_ids}"

    def fetch(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> list[dict[str, Any]]:
        raise NotImplementedError("BeeperAdapter uses fetch_batches()")

    def _resolve_people(
        self,
        cache: IdentityCache,
        *,
        account_id: str,
        participants: list[ParticipantRecord],
    ) -> list[str]:
        links: list[str] = []
        for participant in participants:
            if participant.is_self:
                continue
            resolved = self._resolve_participant_person(cache, account_id=account_id, participant=participant)
            if resolved and resolved not in links:
                links.append(resolved)
        return links

    def _participant_resolution_candidates(
        self,
        *,
        account_id: str,
        participant: ParticipantRecord,
    ) -> list[tuple[str, str]]:
        candidates: list[tuple[str, str]] = []
        for identifier_type, value in participant.identifiers:
            prefix = _identity_prefix(account_id, identifier_type)
            if prefix and value:
                candidates.append((prefix, value))
        if participant.full_name:
            candidates.append(("name", participant.full_name))
        deduped: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for prefix, value in candidates:
            key = (_clean(prefix).lower(), _clean(value))
            if not key[0] or not key[1] or key in seen:
                continue
            seen.add(key)
            deduped.append((key[0], key[1]))
        return deduped

    def _resolve_participant_person(
        self,
        cache: IdentityCache,
        *,
        account_id: str,
        participant: ParticipantRecord,
    ) -> str:
        for prefix, value in self._participant_resolution_candidates(account_id=account_id, participant=participant):
            resolved = cache.resolve(prefix, value)
            if resolved:
                return resolved
        return ""

    def _participants_from_thread_json(self, thread_payload: dict[str, Any]) -> list[ParticipantRecord]:
        items = (thread_payload.get("participants") or {}).get("items", []) or []
        records: list[ParticipantRecord] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            identifiers: list[tuple[str, str]] = []
            email = _clean(item.get("email", ""))
            if email:
                identifiers.append(("email", email))
            records.append(
                ParticipantRecord(
                    participant_id=_clean(item.get("id", "")),
                    full_name=_clean(item.get("fullName", "")),
                    is_self=bool(item.get("isSelf")),
                    identifiers=identifiers,
                )
            )
        return [record for record in records if record.participant_id]

    def _beeper_person_identity_aliases(self, card: PersonCard) -> dict[str, Any]:
        aliases = self._person_identity_aliases(card)
        aliases["instagram"] = card.instagram
        aliases["telegram"] = card.telegram
        aliases["discord"] = card.discord
        return aliases

    def _resolve_person_card_exact(self, cache: IdentityCache, card: PersonCard) -> str:
        candidates: list[tuple[str, str]] = []
        candidates.extend(("email", value) for value in card.emails)
        candidates.extend(("phone", value) for value in card.phones)
        for prefix, value in (
            ("linkedin", card.linkedin),
            ("twitter", card.twitter),
            ("instagram", card.instagram),
            ("telegram", card.telegram),
            ("discord", card.discord),
            ("name", card.summary),
        ):
            if _clean(value):
                candidates.append((prefix, _clean(value)))
        full_name = " ".join(part for part in [card.first_name, card.last_name] if _clean(part))
        if full_name:
            candidates.append(("name", full_name))
        for prefix, value in candidates:
            resolved = cache.resolve(prefix, value)
            if resolved:
                return resolved
        return ""

    def _beeper_person_rel_path(self, card: PersonCard) -> Path:
        base_slug = normalize_for_slug(card.summary)
        hash8 = hashlib.sha256(card.source_id.encode("utf-8")).hexdigest()[:8]
        return Path("People") / f"{base_slug}-{hash8}.md"

    def _prepare_person_write(self, item: dict[str, Any], *, cache: IdentityCache) -> tuple[PlannedPersonWrite | None, bool]:
        card, provenance, body = self.to_card(item)
        assert isinstance(card, PersonCard)
        existing_wikilink = self._resolve_person_card_exact(cache, card)
        if existing_wikilink:
            cache.upsert(existing_wikilink, self._beeper_person_identity_aliases(card))
            return None, True

        rel_path = self._beeper_person_rel_path(card)
        wikilink = f"[[{rel_path.stem}]]"
        cache.upsert(wikilink, self._beeper_person_identity_aliases(card))
        return PlannedPersonWrite(
            card=card,
            provenance=provenance,
            body=body,
            rel_path=rel_path,
            wikilink=wikilink,
        ), False

    def _person_handle_field(self, account_id: str) -> str:
        normalized_account = _clean(account_id).split(".", 1)[0].lower()
        if normalized_account == "linkedin":
            return "linkedin"
        if normalized_account == "twitter":
            return "twitter"
        if normalized_account in {"instagramgo", "instagram"}:
            return "instagram"
        if normalized_account == "telegram":
            return "telegram"
        if normalized_account in {"discordgo", "discord"}:
            return "discord"
        return ""

    def _participant_source_id(self, account_id: str, participant: ParticipantRecord) -> str:
        best_identifier = participant.best_identifier_token()
        if best_identifier:
            return f"{account_id}:{best_identifier}"
        return f"{account_id}:{participant.participant_id}"

    def _participant_person_item(
        self,
        *,
        account_id: str,
        protocol: str,
        participant: ParticipantRecord,
    ) -> dict[str, Any] | None:
        if participant.is_self:
            return None
        identifier_tokens = participant.identifier_tokens()
        summary = participant.full_name or _identifier_value(participant.best_identifier_token()) or participant.participant_id
        if not _clean(summary):
            return None
        first_name = ""
        last_name = ""
        name_parts = [part for part in participant.full_name.split() if part] if participant.full_name else []
        if name_parts:
            first_name = name_parts[0]
            if len(name_parts) > 1:
                last_name = " ".join(name_parts[1:])
        emails = [value for identifier_type, value in participant.identifiers if identifier_type == "email"]
        phones = [value for identifier_type, value in participant.identifiers if identifier_type == "phone"]
        handle_field = self._person_handle_field(account_id)
        handle_value = ""
        for identifier_type, value in participant.identifiers:
            if identifier_type != "username":
                continue
            prefix = _identity_prefix(account_id, identifier_type)
            if prefix == handle_field:
                handle_value = value
                break
        source_id = self._participant_source_id(account_id, participant)
        body_lines = [
            "Beeper participant observed in direct-message history.",
            "",
            f"Account: {account_id}",
            f"Protocol: {protocol}",
            f"Participant ID: {participant.participant_id}",
        ]
        if identifier_tokens:
            body_lines.append("Identifiers:")
            body_lines.extend([f"- {token}" for token in identifier_tokens])
        body = "\n".join(body_lines).strip()
        item = {
            "kind": "person",
            "account_id": account_id,
            "protocol": protocol,
            "participant_id": participant.participant_id,
            "summary": summary,
            "first_name": first_name,
            "last_name": last_name,
            "emails": emails,
            "phones": phones,
            "identifier_tokens": identifier_tokens,
            "source_id": source_id,
            "created": date.today().isoformat(),
            "body": body,
            "tags": _dedupe(["beeper", protocol]),
        }
        if handle_field and handle_value:
            item[handle_field] = handle_value
        return item

    def _thread_items(
        self,
        *,
        room_id: str,
        account_id: str,
        thread_payload: dict[str, Any],
        participants: list[ParticipantRecord],
        messages: list[dict[str, Any]],
        media_root: Path,
        identity_cache: IdentityCache,
    ) -> list[dict[str, Any]]:
        if not messages:
            return []

        thread_type = _clean(thread_payload.get("type", "")).lower()
        protocol = _clean((thread_payload.get("extra") or {}).get("protocol", "")) or account_id.split(".", 1)[0]
        bridge_name = _clean((thread_payload.get("extra") or {}).get("bridgeName", "")) or protocol
        people_links = self._resolve_people(identity_cache, account_id=account_id, participants=participants)

        participant_ids = _dedupe([participant.participant_id for participant in participants])
        participant_names = _dedupe([participant.full_name for participant in participants if participant.full_name])
        participant_identifiers = _dedupe(
            [token for participant in participants for token in participant.identifier_tokens()]
        )
        counterparts = [participant for participant in participants if not participant.is_self]
        resolved_people_by_participant_id = {
            participant.participant_id: self._resolve_participant_person(
                identity_cache,
                account_id=account_id,
                participant=participant,
            )
            for participant in participants
        }
        counterpart_ids = _dedupe([participant.participant_id for participant in counterparts])
        counterpart_names = _dedupe([participant.full_name for participant in counterparts if participant.full_name])
        counterpart_identifiers = _dedupe(
            [token for participant in counterparts for token in participant.identifier_tokens()]
        )
        counterpart_people = _dedupe(
            [
                resolved_people_by_participant_id.get(participant.participant_id, "")
                for participant in counterparts
                if resolved_people_by_participant_id.get(participant.participant_id, "")
            ]
        )
        single_counterpart_person = counterpart_people[0] if len(counterpart_people) == 1 else ""

        message_items: list[dict[str, Any]] = []
        attachment_items: list[dict[str, Any]] = []
        message_links: list[str] = []
        attachment_links: list[str] = []
        thread_body_messages: list[dict[str, Any]] = []

        participant_by_id = {participant.participant_id: participant for participant in participants}
        first_message_at = ""
        last_message_at = ""
        for row in messages:
            payload = _json_load(row.get("message"), {})
            event_id = _clean(row.get("eventID", "")) or _clean(payload.get("eventID", "")) or f"mx-row:{row.get('id')}"
            message_type = _clean(row.get("type", "")).upper() or _clean((payload.get("extra") or {}).get("type", "")).upper()
            sent_at = _parse_timestamp(row.get("timestamp") or payload.get("timestamp"))
            if sent_at and (not first_message_at or sent_at < first_message_at):
                first_message_at = sent_at
            if sent_at and sent_at > last_message_at:
                last_message_at = sent_at

            sender_id = _clean(payload.get("senderID", "")) or _clean(row.get("senderContactID", ""))
            sender_participant = participant_by_id.get(sender_id)
            sender_name = sender_participant.full_name if sender_participant else ""
            sender_identifier = _best_identifier(sender_participant.identifiers) if sender_participant else ""
            sender_person = (
                resolved_people_by_participant_id.get(sender_id, "")
                if sender_participant is not None
                else ""
            )
            if not sender_person and sender_participant is not None and not sender_participant.is_self and thread_type == "single":
                sender_person = single_counterpart_person
            reaction_key = _clean((payload.get("action") or {}).get("reactionKey", "")) or _clean(
                ((payload.get("extra") or {}).get("partialReactionContent") or {}).get("description", "")
            )
            body = _message_body(message_type, payload)

            attachment_uids: list[str] = []
            attachments = payload.get("attachments") or []
            for index, attachment in enumerate(attachments):
                attachment_id = _clean(attachment.get("id", "")) or _clean(attachment.get("srcURL", "")) or f"{event_id}:{index}"
                attachment_uid = _attachment_uid(event_id, attachment_id)
                attachment_uids.append(attachment_uid)
                attachment_payload = {
                    "kind": "attachment",
                    "event_id": event_id,
                    "room_id": room_id,
                    "attachment_id": attachment_id,
                    "account_id": account_id,
                    "protocol": protocol,
                    "message": _wikilink_from_uid(_message_uid(event_id)),
                    "thread": _wikilink_from_uid(_thread_uid(room_id)),
                    "attachment_type": _clean(attachment.get("type", "")).lower(),
                    "filename": _clean(attachment.get("fileName", "")),
                    "mime_type": _clean(attachment.get("mimeType", "")),
                    "size_bytes": int(attachment.get("fileSize", 0) or 0),
                    "src_url": _clean(attachment.get("srcURL", "")),
                    "cached_path": _media_cache_path(_clean(attachment.get("srcURL", "")), media_root),
                    "width": int(((attachment.get("size") or {}).get("width", 0) or 0)),
                    "height": int(((attachment.get("size") or {}).get("height", 0) or 0)),
                    "duration_ms": int(((attachment.get("extra") or {}).get("duration", 0) or 0)),
                    "is_voice_note": bool(attachment.get("isVoiceNote", False)),
                    "is_gif": bool(attachment.get("isGif", False)),
                    "is_sticker": bool(attachment.get("isSticker", False)),
                    "summary": _clean(attachment.get("fileName", "")) or attachment_id,
                    "created": _date_bucket(sent_at),
                    "people": list(people_links),
                }
                attachment_payload["attachment_metadata_sha"] = _attachment_metadata_sha(attachment_payload)
                attachment_items.append(attachment_payload)
                attachment_links.append(_wikilink_from_uid(attachment_uid))

            message_payload = {
                "kind": "message",
                "event_id": event_id,
                "room_id": room_id,
                "account_id": account_id,
                "protocol": protocol,
                "bridge_name": bridge_name,
                "thread": _wikilink_from_uid(_thread_uid(room_id)),
                "message_type": message_type,
                "sender_id": sender_id,
                "sender_name": sender_name,
                "sender_identifier": sender_identifier,
                "sender_person": sender_person,
                "is_from_me": bool(row.get("isSentByMe")),
                "sent_at": sent_at,
                "edited_at": _parse_timestamp(row.get("lastEditionTimestamp") or payload.get("editedTimestamp")),
                "deleted_at": sent_at if bool(row.get("isDeleted")) else "",
                "linked_message_event_id": _clean(payload.get("linkedMessageID", "")),
                "reply_to_event_id": _clean(row.get("inReplyToID", "")),
                "reaction_key": reaction_key,
                "has_attachments": bool(attachment_uids),
                "attachments": [_wikilink_from_uid(uid) for uid in attachment_uids],
                "body": body,
                "summary": _preview_text(body)
                or _clean(sender_name)
                or sender_identifier
                or message_type.lower()
                or event_id,
                "created": _date_bucket(sent_at),
                "people": list(people_links),
            }
            message_payload["message_body_sha"] = _message_body_sha(
                event_id=event_id,
                sent_at=sent_at,
                message_type=message_type,
                sender_id=sender_id,
                body=body,
                reaction_key=reaction_key,
                attachment_ids=attachment_uids,
            )
            message_items.append(message_payload)
            message_links.append(_wikilink_from_uid(_message_uid(event_id)))
            thread_body_messages.append(
                {
                    "event_id": event_id,
                    "sent_at": sent_at,
                    "sender_id": sender_id,
                    "message_type": message_type,
                    "reaction_key": reaction_key,
                    "body": body,
                    "attachment_ids": attachment_uids,
                }
            )

        if not first_message_at:
            first_message_at = _parse_timestamp(thread_payload.get("createdAt")) or _parse_timestamp(thread_payload.get("timestamp"))
        if not last_message_at:
            last_message_at = _parse_timestamp(thread_payload.get("timestamp")) or first_message_at

        thread_item = {
            "kind": "thread",
            "room_id": room_id,
            "account_id": account_id,
            "protocol": protocol,
            "bridge_name": bridge_name,
            "thread_type": thread_type,
            "thread_title": _clean(thread_payload.get("title", "")),
            "thread_description": _clean(thread_payload.get("description", "")),
            "participant_ids": participant_ids,
            "participant_names": participant_names,
            "participant_identifiers": participant_identifiers,
            "counterpart_ids": counterpart_ids,
            "counterpart_names": counterpart_names,
            "counterpart_identifiers": counterpart_identifiers,
            "messages": message_links,
            "attachments": attachment_links,
            "first_message_at": first_message_at,
            "last_message_at": last_message_at,
            "message_count": len(message_links),
            "attachment_count": len(attachment_links),
            "is_group": thread_type == "group",
            "has_attachments": bool(attachment_links),
            "thread_summary": _preview_text(
                "\n".join(item["body"] for item in thread_body_messages if item.get("body"))
            ),
            "thread_body_sha": _thread_body_sha(thread_body_messages),
            "summary": _clean(thread_payload.get("title", ""))
            or ", ".join(counterpart_names[:3])
            or ", ".join(counterpart_identifiers[:3])
            or room_id,
            "created": _date_bucket(first_message_at),
            "people": list(people_links),
        }

        items = [thread_item, *message_items, *attachment_items]
        return items

    def _build_thread_items(
        self,
        *,
        thread_row: dict[str, Any],
        participants_by_room: dict[str, list[ParticipantRecord]],
        messages_by_room: dict[str, list[dict[str, Any]]],
        media_root: Path,
        identity_cache: IdentityCache,
    ) -> list[dict[str, Any]]:
        room_id = _clean(thread_row.get("threadID", ""))
        account_id = _clean(thread_row.get("accountID", ""))
        thread_payload = _json_load(thread_row.get("thread"), {})
        participants = participants_by_room.get(room_id) or self._participants_from_thread_json(thread_payload)
        messages = messages_by_room.get(room_id, [])
        return self._thread_items(
            room_id=room_id,
            account_id=account_id,
            thread_payload=thread_payload,
            participants=participants,
            messages=messages,
            media_root=media_root,
            identity_cache=identity_cache,
        )

    def _build_person_items(
        self,
        *,
        threads: list[dict[str, Any]],
        participants_by_room: dict[str, list[ParticipantRecord]],
    ) -> list[dict[str, Any]]:
        person_items_by_source: dict[str, dict[str, Any]] = {}
        for thread_row in threads:
            room_id = _clean(thread_row.get("threadID", ""))
            account_id = _clean(thread_row.get("accountID", ""))
            thread_payload = _json_load(thread_row.get("thread"), {})
            protocol = _clean((thread_payload.get("extra") or {}).get("protocol", "")) or account_id.split(".", 1)[0]
            participants = participants_by_room.get(room_id) or self._participants_from_thread_json(thread_payload)
            for participant in participants:
                person_item = self._participant_person_item(
                    account_id=account_id,
                    protocol=protocol,
                    participant=participant,
                )
                if person_item is None:
                    continue
                person_items_by_source.setdefault(person_item["source_id"], person_item)
        return list(person_items_by_source.values())

    def _plan_person_writes(
        self,
        person_items: list[dict[str, Any]],
        *,
        batch_identity: IdentityCache,
    ) -> tuple[list[PlannedPersonWrite], int]:
        plans: list[PlannedPersonWrite] = []
        matched_existing = 0
        seen_sources: set[str] = set()
        for item in person_items:
            source_id = _clean(item.get("source_id", ""))
            if source_id in seen_sources:
                continue
            seen_sources.add(source_id)
            plan, matched = self._prepare_person_write(item, cache=batch_identity)
            if matched:
                matched_existing += 1
                continue
            if plan is not None:
                plans.append(plan)
        return plans, matched_existing

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        db_path: str | None = None,
        media_root: str | None = None,
        thread_types: str | list[str] | None = "single",
        account_ids: str | list[str] | None = None,
        max_threads: int | None = None,
        batch_size: int = 10,
        workers: int | None = None,
        **kwargs,
    ):
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)
        resolved_db_path = Path(db_path or DEFAULT_DB_PATH).expanduser().resolve()
        resolved_media_root = Path(media_root or DEFAULT_MEDIA_ROOT).expanduser().resolve()
        normalized_thread_types = _parse_csv(thread_types, default=["single"])
        normalized_account_ids = _parse_csv(account_ids)
        resolved_batch_size = max(
            1,
            int(batch_size or os.environ.get("HFA_BEEPER_BATCH_SIZE") or 10),
        )
        worker_count = self._worker_count(workers)
        last_completed_timestamp = int(cursor.get("last_completed_thread_timestamp", -1) or -1)
        last_completed_thread_id = _clean(cursor.get("last_completed_thread_id", ""))
        remaining = max_threads
        identity_cache = IdentityCache(vault_path)
        index = BeeperIndex(resolved_db_path)
        fetch_started_at = perf_counter()
        yielded_threads = 0
        yielded_items = 0
        sequence = 0
        yielded_batches = 0
        self._adapter_log(
            "fetch_batches start",
            verbose=verbose,
        )
        self._adapter_log(
            f"fetch_batches config: db_path={resolved_db_path} media_root={resolved_media_root} "
            f"batch_size={resolved_batch_size} workers={worker_count} "
            f"thread_types={normalized_thread_types} account_ids={normalized_account_ids or ['all']} "
            f"max_threads={remaining if remaining is not None else 'all'} progress_every={progress_every}",
            verbose=verbose,
        )
        try:
            while True:
                limit = resolved_batch_size if remaining is None else min(resolved_batch_size, remaining)
                if limit <= 0:
                    break
                batch_started_at = perf_counter()
                threads = index.list_threads(
                    after_timestamp=last_completed_timestamp,
                    after_thread_id=last_completed_thread_id,
                    thread_types=normalized_thread_types,
                    account_ids=normalized_account_ids,
                    limit=limit,
                )
                if not threads:
                    self._adapter_log("fetch_batches done: no more threads", verbose=verbose)
                    break

                room_ids = [_clean(thread_row.get("threadID", "")) for thread_row in threads if _clean(thread_row.get("threadID", ""))]
                load_started_at = perf_counter()
                participants_by_room = index.participants_for_rooms(room_ids)
                messages_by_room = index.messages_for_rooms(room_ids)
                person_items = self._build_person_items(threads=threads, participants_by_room=participants_by_room)
                total_message_rows = sum(len(messages_by_room.get(room_id, [])) for room_id in room_ids)
                self._adapter_log(
                    f"batch load done: sequence={sequence} threads={len(threads)} rooms={len(room_ids)} "
                    f"participants_rooms={len(participants_by_room)} message_rows={total_message_rows} "
                    f"person_candidates={len(person_items)} "
                    f"elapsed_s={perf_counter() - load_started_at:.2f}",
                    verbose=verbose,
                )

                if person_items:
                    self._adapter_log(
                        f"yield people batch: sequence={sequence} items={len(person_items)}",
                        verbose=verbose,
                    )
                    yield FetchedBatch(items=person_items, sequence=yielded_batches)
                    yielded_batches += 1
                    identity_cache = IdentityCache(vault_path)

                items: list[dict[str, Any]] = []
                skipped = 0
                skip_details: dict[str, int] = {}
                build_thread = lambda thread_row: self._build_thread_items(
                    thread_row=thread_row,
                    participants_by_room=participants_by_room,
                    messages_by_room=messages_by_room,
                    media_root=resolved_media_root,
                    identity_cache=identity_cache,
                )
                if worker_count > 1 and len(threads) > 1:
                    build_started_at = perf_counter()
                    self._adapter_log(
                        f"batch build start: sequence={sequence} threads={len(threads)} workers={min(worker_count, len(threads))}",
                        verbose=verbose,
                    )
                    with ThreadPoolExecutor(max_workers=max(1, min(worker_count, len(threads)))) as executor:
                        thread_results = list(executor.map(build_thread, threads))
                    self._adapter_log(
                        f"batch build done: sequence={sequence} threads={len(threads)} "
                        f"elapsed_s={perf_counter() - build_started_at:.2f}",
                        verbose=verbose,
                    )
                else:
                    thread_results = [build_thread(thread_row) for thread_row in threads]

                for thread_row, thread_items in zip(threads, thread_results, strict=True):
                    room_id = _clean(thread_row.get("threadID", ""))
                    last_completed_timestamp = int(thread_row.get("timestamp", 0) or 0)
                    last_completed_thread_id = room_id
                    if not thread_items:
                        skipped += 1
                        skip_details["empty_threads"] = skip_details.get("empty_threads", 0) + 1
                        continue
                    thread_items[-1]["_cursor"] = {
                        "db_path": str(resolved_db_path),
                        "media_root": str(resolved_media_root),
                        "thread_types": normalized_thread_types,
                        "account_ids": normalized_account_ids,
                        "last_completed_thread_timestamp": last_completed_timestamp,
                        "last_completed_thread_id": last_completed_thread_id,
                    }
                    items.extend(thread_items)
                    yielded_threads += 1
                    yielded_items += len(thread_items)
                    if progress_every and yielded_threads % progress_every == 0:
                        self._adapter_log(
                            f"fetch progress: threads={yielded_threads} items={yielded_items} "
                            f"last_thread_id={last_completed_thread_id}",
                            verbose=verbose,
                        )

                self._adapter_log(
                    f"yield batch: sequence={sequence} threads={len(threads)} items={len(items)} "
                    f"skipped={skipped} cumulative_threads={yielded_threads} cumulative_items={yielded_items} "
                    f"elapsed_s={perf_counter() - batch_started_at:.2f}",
                    verbose=verbose,
                )
                yield FetchedBatch(
                    items=items,
                    cursor_patch={
                        "db_path": str(resolved_db_path),
                        "media_root": str(resolved_media_root),
                        "thread_types": normalized_thread_types,
                        "account_ids": normalized_account_ids,
                        "last_completed_thread_timestamp": last_completed_timestamp,
                        "last_completed_thread_id": last_completed_thread_id,
                    },
                    sequence=yielded_batches,
                    skipped_count=skipped,
                    skip_details=skip_details,
                )
                yielded_batches += 1
                sequence += 1
                if remaining is not None:
                    remaining -= len(threads)
                    if remaining <= 0:
                        break
        finally:
            index.close()
            self._adapter_log(
                f"fetch_batches done: batches={yielded_batches} threads={yielded_threads} items={yielded_items} "
                f"elapsed_s={perf_counter() - fetch_started_at:.2f}",
                verbose=verbose,
            )

    def _apply_person_write(
        self,
        vault: Path,
        plan: PlannedPersonWrite,
        *,
        identity_cache: IdentityCache,
        result: IngestResult,
        dry_run: bool,
    ) -> None:
        aliases = self._beeper_person_identity_aliases(plan.card)
        target_path = vault / plan.rel_path
        if target_path.exists():
            if not dry_run:
                merge_into_existing(
                    vault,
                    plan.wikilink,
                    plan.card.model_dump(mode="python"),
                    plan.provenance,
                    plan.body,
                    identity_cache=identity_cache,
                    target_rel_path=plan.rel_path,
                )
            else:
                identity_cache.upsert(plan.wikilink, aliases)
            result.merged += 1
            return

        if not dry_run:
            write_card(vault, str(plan.rel_path), plan.card, body=plan.body, provenance=plan.provenance)
            identity_cache.upsert(plan.wikilink, aliases)
        result.created += 1

    def _apply_nonperson_item(
        self,
        vault: Path,
        item: dict[str, Any],
        *,
        result: IngestResult,
        dry_run: bool,
    ) -> None:
        card, provenance, body = self.to_card(item)
        rel_path = Path(self._card_rel_path(vault, card))
        target = vault / rel_path
        if target.exists():
            if not dry_run:
                self.merge_card(vault, rel_path, card, body, provenance)
            result.merged += 1
            return
        if not dry_run:
            write_card(vault, str(rel_path), card, body=body, provenance=provenance)
        result.created += 1

    def ingest(self, vault_path: str, dry_run: bool = False, **kwargs) -> IngestResult:
        vault = Path(vault_path)
        result = IngestResult()
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)
        cursor_key = self.get_cursor_key(**kwargs)
        cursor = load_sync_state(vault).get(cursor_key, {})
        if kwargs.get("ignore_cursor") or str(os.environ.get("HFA_BEEPER_IGNORE_CURSOR", "")).strip().lower() in {"1", "true", "yes", "on"}:
            cursor = {}
        if not isinstance(cursor, dict):
            cursor = {}
        identity_cache = IdentityCache(vault)
        resolved_db_path = Path(kwargs.get("db_path") or DEFAULT_DB_PATH).expanduser().resolve()
        resolved_media_root = Path(kwargs.get("media_root") or DEFAULT_MEDIA_ROOT).expanduser().resolve()
        normalized_thread_types = _parse_csv(kwargs.get("thread_types"), default=["single"])
        normalized_account_ids = _parse_csv(kwargs.get("account_ids"))
        batch_size = max(1, int(kwargs.get("batch_size") or os.environ.get("HFA_BEEPER_BATCH_SIZE") or 10))
        worker_count = self._worker_count(kwargs.get("workers"))
        max_threads = kwargs.get("max_threads")
        remaining = None if max_threads in (None, "") else max(0, int(max_threads))
        processed_successfully = 0
        seen_items = 0
        ingest_started_at = perf_counter()

        def _log(message: str) -> None:
            self._adapter_log(message, verbose=verbose)

        def _write_progress() -> None:
            if dry_run:
                return
            update_cursor(
                vault,
                cursor_key,
                {
                    **cursor,
                    "db_path": str(resolved_db_path),
                    "media_root": str(resolved_media_root),
                    "thread_types": normalized_thread_types,
                    "account_ids": normalized_account_ids,
                    "last_sync": datetime.now().isoformat(),
                    "seen": seen_items,
                    "processed": processed_successfully,
                    "last_processed_index": max(processed_successfully - 1, -1),
                    "created": result.created,
                    "merged": result.merged,
                    "conflicted": result.conflicted,
                    "skipped": result.skipped,
                    "skip_details": dict(sorted(result.skip_details.items())),
                    "errors": len(result.errors),
                },
            )

        _log(
            f"ingest start: db_path={resolved_db_path} media_root={resolved_media_root} "
            f"batch_size={batch_size} workers={worker_count} thread_types={normalized_thread_types} "
            f"account_ids={normalized_account_ids or ['all']} max_threads={remaining if remaining is not None else 'all'}"
        )
        index = BeeperIndex(resolved_db_path)
        last_completed_timestamp = int(cursor.get("last_completed_thread_timestamp", -1) or -1)
        last_completed_thread_id = _clean(cursor.get("last_completed_thread_id", ""))
        yielded_threads = 0
        try:
            while True:
                limit = batch_size if remaining is None else min(batch_size, remaining)
                if limit <= 0:
                    break
                threads = index.list_threads(
                    after_timestamp=last_completed_timestamp,
                    after_thread_id=last_completed_thread_id,
                    thread_types=normalized_thread_types,
                    account_ids=normalized_account_ids,
                    limit=limit,
                )
                if not threads:
                    break

                room_ids = [_clean(thread_row.get("threadID", "")) for thread_row in threads if _clean(thread_row.get("threadID", ""))]
                participants_by_room = index.participants_for_rooms(room_ids)
                messages_by_room = index.messages_for_rooms(room_ids)
                person_items = self._build_person_items(threads=threads, participants_by_room=participants_by_room)
                batch_identity = _clone_identity_cache(identity_cache)
                person_plans, matched_existing = self._plan_person_writes(person_items, batch_identity=batch_identity)

                build_thread = lambda thread_row: self._build_thread_items(
                    thread_row=thread_row,
                    participants_by_room=participants_by_room,
                    messages_by_room=messages_by_room,
                    media_root=resolved_media_root,
                    identity_cache=batch_identity,
                )
                if worker_count > 1 and len(threads) > 1:
                    with ThreadPoolExecutor(max_workers=max(1, min(worker_count, len(threads)))) as executor:
                        thread_results = list(executor.map(build_thread, threads))
                else:
                    thread_results = [build_thread(thread_row) for thread_row in threads]

                nonperson_items: list[dict[str, Any]] = []
                skipped = 0
                for thread_row, thread_items in zip(threads, thread_results, strict=True):
                    room_id = _clean(thread_row.get("threadID", ""))
                    last_completed_timestamp = int(thread_row.get("timestamp", 0) or 0)
                    last_completed_thread_id = room_id
                    if not thread_items:
                        skipped += 1
                        result.skipped += 1
                        result.skip_details["empty_threads"] = result.skip_details.get("empty_threads", 0) + 1
                        continue
                    nonperson_items.extend(thread_items)
                    yielded_threads += 1
                    if progress_every and yielded_threads % progress_every == 0:
                        _log(f"thread progress: threads={yielded_threads} last_thread_id={last_completed_thread_id}")

                seen_items += len(person_items) + len(nonperson_items)

                for plan in person_plans:
                    try:
                        self._apply_person_write(
                            vault,
                            plan,
                            identity_cache=batch_identity,
                            result=result,
                            dry_run=dry_run,
                        )
                        processed_successfully += 1
                    except Exception as exc:
                        result.errors.append(f"person {plan.card.source_id}: {exc}")
                processed_successfully += matched_existing
                result.merged += matched_existing

                for item in nonperson_items:
                    try:
                        self._apply_nonperson_item(vault, item, result=result, dry_run=dry_run)
                        processed_successfully += 1
                    except Exception as exc:
                        item_id = _clean(item.get("event_id", "")) or _clean(item.get("room_id", "")) or _clean(item.get("attachment_id", ""))
                        result.errors.append(f"item {item_id}: {exc}")

                identity_cache.entries = batch_identity.entries
                if not dry_run:
                    identity_cache.flush()
                    cursor.update(
                        {
                            "db_path": str(resolved_db_path),
                            "media_root": str(resolved_media_root),
                            "thread_types": normalized_thread_types,
                            "account_ids": normalized_account_ids,
                            "last_completed_thread_timestamp": last_completed_timestamp,
                            "last_completed_thread_id": last_completed_thread_id,
                        }
                    )
                    _write_progress()

                _log(
                    f"batch done: threads={len(threads)} person_candidates={len(person_items)} "
                    f"person_creates={len(person_plans)} person_matches={matched_existing} "
                    f"nonperson_items={len(nonperson_items)} skipped={skipped} "
                    f"created={result.created} merged={result.merged} errors={len(result.errors)}"
                )

                if remaining is not None:
                    remaining -= len(threads)
                    if remaining <= 0:
                        break
        finally:
            index.close()

        _log(
            f"ingest done: created={result.created} merged={result.merged} conflicted={result.conflicted} "
            f"skipped={result.skipped} errors={len(result.errors)} elapsed_s={perf_counter() - ingest_started_at:.2f}"
        )
        return result

    def to_card(self, item: dict[str, Any]) -> tuple[Any, dict[str, ProvenanceEntry], str]:
        today = date.today().isoformat()
        kind = _clean(item.get("kind", "")).lower()
        if kind == "thread":
            room_id = _clean(item.get("room_id", ""))
            card = BeeperThreadCard(
                uid=_thread_uid(room_id),
                type="beeper_thread",
                source=[THREAD_SOURCE],
                source_id=room_id,
                created=_clean(item.get("created", "")) or today,
                updated=today,
                summary=_clean(item.get("summary", "")) or room_id,
                people=list(item.get("people", [])),
                beeper_room_id=room_id,
                account_id=_clean(item.get("account_id", "")),
                protocol=_clean(item.get("protocol", "")),
                bridge_name=_clean(item.get("bridge_name", "")),
                thread_type=_clean(item.get("thread_type", "")),
                thread_title=_clean(item.get("thread_title", "")),
                thread_description=_clean(item.get("thread_description", "")),
                participant_ids=list(item.get("participant_ids", [])),
                participant_names=list(item.get("participant_names", [])),
                participant_identifiers=list(item.get("participant_identifiers", [])),
                counterpart_ids=list(item.get("counterpart_ids", [])),
                counterpart_names=list(item.get("counterpart_names", [])),
                counterpart_identifiers=list(item.get("counterpart_identifiers", [])),
                messages=list(item.get("messages", [])),
                attachments=list(item.get("attachments", [])),
                first_message_at=_clean(item.get("first_message_at", "")),
                last_message_at=_clean(item.get("last_message_at", "")),
                message_count=int(item.get("message_count", 0) or 0),
                attachment_count=int(item.get("attachment_count", 0) or 0),
                is_group=bool(item.get("is_group", False)),
                has_attachments=bool(item.get("has_attachments", False)),
                thread_summary=_clean(item.get("thread_summary", "")),
                thread_body_sha=_clean(item.get("thread_body_sha", "")),
            )
            return card, deterministic_provenance(card, THREAD_SOURCE), ""

        if kind == "person":
            source_id = _clean(item.get("source_id", ""))
            today_created = _clean(item.get("created", "")) or today
            card = PersonCard(
                uid=generate_uid("person", self.source_id, source_id),
                type="person",
                source=["beeper"],
                source_id=source_id,
                created=today_created,
                updated=today,
                summary=_clean(item.get("summary", "")) or source_id,
                first_name=_clean(item.get("first_name", "")),
                last_name=_clean(item.get("last_name", "")),
                emails=list(item.get("emails", [])),
                phones=list(item.get("phones", [])),
                linkedin=_clean(item.get("linkedin", "")),
                twitter=_clean(item.get("twitter", "")),
                instagram=_clean(item.get("instagram", "")),
                telegram=_clean(item.get("telegram", "")),
                discord=_clean(item.get("discord", "")),
                tags=list(item.get("tags", [])),
            )
            return card, deterministic_provenance(card, "beeper"), _clean(item.get("body", ""))

        if kind == "message":
            event_id = _clean(item.get("event_id", ""))
            card = BeeperMessageCard(
                uid=_message_uid(event_id),
                type="beeper_message",
                source=[MESSAGE_SOURCE],
                source_id=event_id,
                created=_clean(item.get("created", "")) or today,
                updated=today,
                summary=_clean(item.get("summary", "")) or event_id,
                people=list(item.get("people", [])),
                beeper_event_id=event_id,
                beeper_room_id=_clean(item.get("room_id", "")),
                account_id=_clean(item.get("account_id", "")),
                protocol=_clean(item.get("protocol", "")),
                bridge_name=_clean(item.get("bridge_name", "")),
                thread=_clean(item.get("thread", "")),
                message_type=_clean(item.get("message_type", "")),
                sender_id=_clean(item.get("sender_id", "")),
                sender_name=_clean(item.get("sender_name", "")),
                sender_identifier=_clean(item.get("sender_identifier", "")),
                sender_person=_clean(item.get("sender_person", "")),
                is_from_me=bool(item.get("is_from_me", False)),
                sent_at=_clean(item.get("sent_at", "")),
                edited_at=_clean(item.get("edited_at", "")),
                deleted_at=_clean(item.get("deleted_at", "")),
                linked_message_event_id=_clean(item.get("linked_message_event_id", "")),
                reply_to_event_id=_clean(item.get("reply_to_event_id", "")),
                reaction_key=_clean(item.get("reaction_key", "")),
                has_attachments=bool(item.get("has_attachments", False)),
                attachments=list(item.get("attachments", [])),
                message_body_sha=_clean(item.get("message_body_sha", "")),
            )
            return card, deterministic_provenance(card, MESSAGE_SOURCE), _clean(item.get("body", ""))

        if kind == "attachment":
            event_id = _clean(item.get("event_id", ""))
            attachment_id = _clean(item.get("attachment_id", ""))
            card = BeeperAttachmentCard(
                uid=_attachment_uid(event_id, attachment_id),
                type="beeper_attachment",
                source=[ATTACHMENT_SOURCE],
                source_id=f"{event_id}:{attachment_id}",
                created=_clean(item.get("created", "")) or today,
                updated=today,
                summary=_clean(item.get("summary", "")) or attachment_id,
                people=list(item.get("people", [])),
                beeper_event_id=event_id,
                beeper_room_id=_clean(item.get("room_id", "")),
                attachment_id=attachment_id,
                account_id=_clean(item.get("account_id", "")),
                protocol=_clean(item.get("protocol", "")),
                message=_clean(item.get("message", "")),
                thread=_clean(item.get("thread", "")),
                attachment_type=_clean(item.get("attachment_type", "")),
                filename=_clean(item.get("filename", "")),
                mime_type=_clean(item.get("mime_type", "")),
                size_bytes=int(item.get("size_bytes", 0) or 0),
                src_url=_clean(item.get("src_url", "")),
                cached_path=_clean(item.get("cached_path", "")),
                width=int(item.get("width", 0) or 0),
                height=int(item.get("height", 0) or 0),
                duration_ms=int(item.get("duration_ms", 0) or 0),
                is_voice_note=bool(item.get("is_voice_note", False)),
                is_gif=bool(item.get("is_gif", False)),
                is_sticker=bool(item.get("is_sticker", False)),
                attachment_metadata_sha=_clean(item.get("attachment_metadata_sha", "")),
            )
            return card, deterministic_provenance(card, ATTACHMENT_SOURCE), ""

        raise ValueError(f"Unsupported Beeper record kind: {kind}")

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        self._replace_generic_card(vault_path, rel_path, card, body, provenance)

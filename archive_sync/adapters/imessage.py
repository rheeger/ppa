"""Apple Messages snapshot adapter for HFA."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import Any

from archive_vault.identity import IdentityCache
from archive_vault.provenance import ProvenanceEntry, merge_provenance
from archive_vault.schema import (
    IMessageAttachmentCard,
    IMessageMessageCard,
    IMessageThreadCard,
    validate_card_permissive,
    validate_card_strict,
)
from archive_vault.thread_hash import compute_imessage_thread_body_sha_from_payload
from archive_vault.uid import generate_uid
from archive_vault.vault import read_note, write_card

from .base import BaseAdapter, deterministic_provenance

THREAD_SOURCE = "imessage.thread"
MESSAGE_SOURCE = "imessage.message"
ATTACHMENT_SOURCE = "imessage.attachment"
WHITESPACE_RE = re.compile(r"\s+")
PRINTABLE_BLOB_RE = re.compile(rb"[\x20-\x7E]{3,}")
ATTRIBUTED_BODY_NOISE = {
    "NSDictionary",
    "NSString",
    "NSNumber",
    "NSAttributedString",
    "NSMutableAttributedString",
    "NSMutableString",
    "NSMutable",
    "NSArray",
    "NSObject",
    "bplist00",
    "__kIMMessagePartAttributeName",
}
ATTRIBUTED_BODY_STOP_TOKENS = {
    "NSDictionary",
    "NSNumber",
    "NSValue",
    "NSData",
    "NSURL",
}
OPAQUE_PAYLOAD_PREFIXES = ("divvy://import/",)
CONTROL_CHAR_RE = re.compile(r"[\x00-\x1F\x7F-\x9F]+")


def _clean(value: str) -> str:
    return WHITESPACE_RE.sub(" ", value.strip())


def _string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _normalize_handle(value: str) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    if "@" in raw:
        return raw.lower()
    digits = re.sub(r"\D", "", raw)
    if digits:
        if raw.startswith("+") or (len(digits) == 11 and digits.startswith("1")):
            return f"+{digits}"
        if len(digits) == 10:
            return f"+1{digits}"
        return digits
    return raw.lower()


def _apple_time_to_iso(value: Any) -> str:
    raw = _string(value).strip()
    if not raw:
        return ""
    try:
        parsed = int(raw)
    except ValueError:
        return ""
    if parsed <= 0:
        return ""
    if abs(parsed) >= 10**16:
        seconds = parsed / 1_000_000_000
    elif abs(parsed) >= 10**13:
        seconds = parsed / 1_000_000
    elif abs(parsed) >= 10**10:
        seconds = parsed / 1_000
    else:
        seconds = float(parsed)
    apple_epoch = datetime(2001, 1, 1, tzinfo=timezone.utc)
    return (apple_epoch + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def _date_bucket(value: str) -> str:
    return value[:10] if len(value) >= 10 else date.today().isoformat()


def _is_opaque_payload_text(value: str) -> bool:
    cleaned = _clean(value)
    if not cleaned:
        return True
    if cleaned in {"￼", "�"}:
        return True
    if "__kIM" in cleaned:
        return True
    if cleaned == "streamtyped":
        return True
    if cleaned.startswith("NS."):
        return True
    if "GenericURL" in cleaned:
        return True
    if cleaned.startswith("X$versionY$archiverT$topX$objects"):
        return True
    if "$classname" in cleaned or "X$classes" in cleaned:
        return True
    if "$class" in cleaned:
        return True
    if any(cleaned.startswith(prefix) for prefix in OPAQUE_PAYLOAD_PREFIXES):
        return True
    return bool(re.fullmatch(r"\)?at_[0-9A-F_\-]+", cleaned, flags=re.IGNORECASE))


def _extract_attributed_text(value: Any) -> str:
    if value in (None, b"", ""):
        return ""
    blob = value if isinstance(value, (bytes, bytearray)) else _string(value).encode("utf-8", errors="ignore")
    structured_candidates: list[str] = []
    for marker in (b"NSString",):
        search_from = 0
        while True:
            marker_index = bytes(blob).find(marker, search_from)
            if marker_index < 0:
                break
            tail = bytes(blob)[marker_index + len(marker) : marker_index + len(marker) + 64]
            plus_index = tail.find(b"+")
            if plus_index == -1 or plus_index + 1 >= len(tail):
                search_from = marker_index + len(marker)
                continue
            cursor = plus_index + 1
            length = 0
            if tail[cursor] < 0x80:
                length = tail[cursor]
                cursor += 1
            elif tail[cursor] == 0x81 and cursor + 2 < len(tail):
                length = int.from_bytes(tail[cursor + 1 : cursor + 3], "little")
                cursor += 3
            else:
                search_from = marker_index + len(marker)
                continue
            payload_start = marker_index + len(marker) + cursor
            payload_end = payload_start + length
            if length <= 0 or payload_end > len(blob):
                search_from = marker_index + len(marker)
                continue
            payload = bytes(blob)[payload_start:payload_end].decode("utf-8", errors="ignore")
            payload = CONTROL_CHAR_RE.sub("", payload)
            payload = _clean(payload)
            if payload and not _is_opaque_payload_text(payload):
                structured_candidates.append(payload)
            search_from = marker_index + len(marker)
    if structured_candidates:
        structured = _clean(" ".join(dict.fromkeys(structured_candidates)))
        if structured and not _is_opaque_payload_text(structured):
            return structured

    chunks = [_clean(match.decode("utf-8", errors="ignore")) for match in PRINTABLE_BLOB_RE.findall(bytes(blob))]
    chunks = [chunk for chunk in chunks if chunk]
    if not chunks:
        return ""

    preferred: list[str] = []
    for index, chunk in enumerate(chunks):
        if chunk not in {"NSString", "NSMutableString"}:
            continue
        for next_chunk in chunks[index + 1 :]:
            if next_chunk in ATTRIBUTED_BODY_NOISE or next_chunk in ATTRIBUTED_BODY_STOP_TOKENS:
                break
            if next_chunk.startswith("__kIM"):
                break
            cleaned = re.sub(r"^\+\d+", "", next_chunk).strip()
            cleaned = _clean(cleaned)
            if cleaned and not _is_opaque_payload_text(cleaned):
                preferred.append(cleaned)
        if preferred:
            joined = _clean(" ".join(preferred))
            if joined and not _is_opaque_payload_text(joined):
                return joined

    fallback: list[str] = []
    for chunk in chunks:
        if chunk in ATTRIBUTED_BODY_NOISE or chunk in ATTRIBUTED_BODY_STOP_TOKENS:
            continue
        if chunk.startswith("__kIM"):
            continue
        cleaned = re.sub(r"^\+\d+", "", chunk).strip()
        cleaned = _clean(cleaned)
        if not cleaned or len(cleaned) < 3:
            continue
        if _is_opaque_payload_text(cleaned):
            continue
        fallback.append(cleaned)
    if not fallback:
        return ""
    fallback.sort(key=len, reverse=True)
    return fallback[0]


def _preview_text(value: str, limit: int = 120) -> str:
    cleaned = _clean(value)
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(limit - 1, 1)].rstrip() + "…"


def _thread_uid(chat_id: str) -> str:
    return generate_uid("imessage-thread", THREAD_SOURCE, chat_id)


def _message_uid(message_id: str) -> str:
    return generate_uid("imessage-message", MESSAGE_SOURCE, message_id)


def _attachment_uid(message_id: str, attachment_id: str) -> str:
    return generate_uid("imessage-attachment", ATTACHMENT_SOURCE, f"{message_id}:{attachment_id}")


def _wikilink_from_uid(uid: str) -> str:
    return f"[[{uid}]]"


def _merge_string_lists(existing: list[str], incoming: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for value in [*existing, *incoming]:
        if value in seen:
            continue
        seen.add(value)
        merged.append(value)
    return merged


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default
    return payload


def _chunked(values: list[Any], size: int = 900) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


class MessageSnapshot:
    """Readonly view over an exported Apple Messages snapshot bundle."""

    def __init__(self, snapshot_dir: str | Path):
        self.snapshot_dir = Path(snapshot_dir).expanduser().resolve()
        self.db_path = self.snapshot_dir / "chat.db"
        if not self.db_path.exists():
            raise FileNotFoundError(f"chat.db not found in snapshot dir: {self.snapshot_dir}")
        self.meta = _load_json(self.snapshot_dir / "snapshot-meta.json", {})
        self.attachment_manifest = _load_json(self.snapshot_dir / "attachments-manifest.json", {"entries": []})
        self._attachment_rel_by_original: dict[str, str] = {}
        entries = self.attachment_manifest.get("entries", []) if isinstance(self.attachment_manifest, dict) else []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            original_path = _clean(_string(entry.get("original_path", "")))
            exported_path = _clean(_string(entry.get("exported_path", "")))
            if original_path:
                self._attachment_rel_by_original[self._normalize_path_key(original_path)] = exported_path
        self.conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row
        self.tables = self._load_tables()
        self.columns = {table: self._load_columns(table) for table in self.tables}

    def close(self) -> None:
        self.conn.close()

    def _normalize_path_key(self, value: str) -> str:
        raw = _clean(value)
        if not raw:
            return ""
        try:
            return str(Path(raw).expanduser().resolve())
        except OSError:
            return raw

    def _load_tables(self) -> set[str]:
        rows = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return {str(row[0]) for row in rows}

    def _load_columns(self, table: str) -> set[str]:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(row[1]) for row in rows}

    def _table_exists(self, table: str) -> bool:
        return table in self.tables

    def _scalar(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        row = self.conn.execute(query, params).fetchone()
        return row[0] if row else None

    def snapshot_id(self) -> str:
        meta_id = _clean(_string(self.meta.get("snapshot_id", "")))
        if meta_id:
            return meta_id
        return str(int(self.db_path.stat().st_mtime_ns))

    def source_label(self) -> str:
        label = _clean(_string(self.meta.get("source_label", "")))
        return label or "local-messages"

    def max_message_rowid(self) -> int:
        value = self._scalar("SELECT COALESCE(MAX(ROWID), 0) FROM message")
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    def iter_messages(self, after_rowid: int, limit: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT ROWID AS message_rowid, * FROM message WHERE ROWID > ? ORDER BY ROWID LIMIT ?",
            (after_rowid, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def chats_for_messages(self, message_rowids: list[int]) -> dict[int, dict[str, Any]]:
        if not message_rowids or not self._table_exists("chat_message_join") or not self._table_exists("chat"):
            return {}
        mapping: dict[int, dict[str, Any]] = {}
        for batch in _chunked(message_rowids):
            placeholders = ",".join("?" for _ in batch)
            rows = self.conn.execute(
                f"""
                SELECT cmj.message_id, c.ROWID AS chat_rowid, c.*
                FROM chat_message_join cmj
                JOIN chat c ON c.ROWID = cmj.chat_id
                WHERE cmj.message_id IN ({placeholders})
                ORDER BY cmj.message_id, c.ROWID
                """,
                tuple(batch),
            ).fetchall()
            for row in rows:
                message_id = int(row["message_id"])
                if message_id not in mapping:
                    mapping[message_id] = dict(row)
        return mapping

    def chat_for_message(self, message_rowid: int) -> dict[str, Any]:
        if not self._table_exists("chat_message_join") or not self._table_exists("chat"):
            return {}
        row = self.conn.execute(
            """
            SELECT c.ROWID AS chat_rowid, c.*
            FROM chat_message_join cmj
            JOIN chat c ON c.ROWID = cmj.chat_id
            WHERE cmj.message_id = ?
            ORDER BY c.ROWID
            LIMIT 1
            """,
            (message_rowid,),
        ).fetchone()
        return dict(row) if row else {}

    def participants_for_chat(self, chat_rowid: int) -> list[str]:
        if not chat_rowid or not self._table_exists("chat_handle_join") or not self._table_exists("handle"):
            return []
        rows = self.conn.execute(
            """
            SELECT h.id
            FROM chat_handle_join chj
            JOIN handle h ON h.ROWID = chj.handle_id
            WHERE chj.chat_id = ?
            ORDER BY h.ROWID
            """,
            (chat_rowid,),
        ).fetchall()
        return [normalized for row in rows if (normalized := _normalize_handle(_string(row[0])))]

    def participants_for_chats(self, chat_rowids: list[int]) -> dict[int, list[str]]:
        if not chat_rowids or not self._table_exists("chat_handle_join") or not self._table_exists("handle"):
            return {}
        mapping: dict[int, list[str]] = {}
        for batch in _chunked(chat_rowids):
            placeholders = ",".join("?" for _ in batch)
            rows = self.conn.execute(
                f"""
                SELECT chj.chat_id, h.id
                FROM chat_handle_join chj
                JOIN handle h ON h.ROWID = chj.handle_id
                WHERE chj.chat_id IN ({placeholders})
                ORDER BY chj.chat_id, h.ROWID
                """,
                tuple(batch),
            ).fetchall()
            for row in rows:
                chat_id = int(row["chat_id"])
                normalized = _normalize_handle(_string(row["id"]))
                if not normalized:
                    continue
                mapping.setdefault(chat_id, [])
                if normalized not in mapping[chat_id]:
                    mapping[chat_id].append(normalized)
        return mapping

    def handle_for_rowid(self, handle_rowid: Any) -> str:
        if not handle_rowid or not self._table_exists("handle"):
            return ""
        row = self.conn.execute("SELECT id FROM handle WHERE ROWID = ?", (handle_rowid,)).fetchone()
        return _normalize_handle(_string(row[0])) if row else ""

    def handles_for_rowids(self, handle_rowids: list[int]) -> dict[int, str]:
        if not handle_rowids or not self._table_exists("handle"):
            return {}
        mapping: dict[int, str] = {}
        for batch in _chunked(handle_rowids):
            placeholders = ",".join("?" for _ in batch)
            rows = self.conn.execute(
                f"SELECT ROWID, id FROM handle WHERE ROWID IN ({placeholders})",
                tuple(batch),
            ).fetchall()
            for row in rows:
                normalized = _normalize_handle(_string(row["id"]))
                if normalized:
                    mapping[int(row["ROWID"])] = normalized
        return mapping

    def attachments_for_message(self, message_rowid: int) -> list[dict[str, Any]]:
        if not self._table_exists("message_attachment_join") or not self._table_exists("attachment"):
            return []
        rows = self.conn.execute(
            """
            SELECT a.ROWID AS attachment_rowid, a.*
            FROM message_attachment_join maj
            JOIN attachment a ON a.ROWID = maj.attachment_id
            WHERE maj.message_id = ?
            ORDER BY a.ROWID
            """,
            (message_rowid,),
        ).fetchall()
        attachments: list[dict[str, Any]] = []
        for row in rows:
            data = dict(row)
            original_path = _clean(_string(data.get("filename", "")))
            exported_path = self._attachment_rel_by_original.get(self._normalize_path_key(original_path), "")
            attachments.append(
                {
                    "attachment_id": _clean(_string(data.get("guid", "")))
                    or f"attachment-rowid:{data.get('attachment_rowid')}",
                    "filename": Path(original_path).name
                    if original_path
                    else _clean(_string(data.get("transfer_name", ""))),
                    "transfer_name": _clean(_string(data.get("transfer_name", ""))),
                    "mime_type": _clean(_string(data.get("mime_type", ""))),
                    "uti": _clean(_string(data.get("uti", ""))),
                    "size_bytes": int(data.get("total_bytes", 0) or 0),
                    "original_path": original_path,
                    "exported_path": exported_path,
                }
            )
        return attachments

    def attachments_for_messages(self, message_rowids: list[int]) -> dict[int, list[dict[str, Any]]]:
        if (
            not message_rowids
            or not self._table_exists("message_attachment_join")
            or not self._table_exists("attachment")
        ):
            return {}
        mapping: dict[int, list[dict[str, Any]]] = {}
        for batch in _chunked(message_rowids):
            placeholders = ",".join("?" for _ in batch)
            rows = self.conn.execute(
                f"""
                SELECT maj.message_id, a.ROWID AS attachment_rowid, a.*
                FROM message_attachment_join maj
                JOIN attachment a ON a.ROWID = maj.attachment_id
                WHERE maj.message_id IN ({placeholders})
                ORDER BY maj.message_id, a.ROWID
                """,
                tuple(batch),
            ).fetchall()
            for row in rows:
                message_id = int(row["message_id"])
                data = dict(row)
                original_path = _clean(_string(data.get("filename", "")))
                exported_path = self._attachment_rel_by_original.get(self._normalize_path_key(original_path), "")
                mapping.setdefault(message_id, []).append(
                    {
                        "attachment_id": _clean(_string(data.get("guid", "")))
                        or f"attachment-rowid:{data.get('attachment_rowid')}",
                        "filename": Path(original_path).name
                        if original_path
                        else _clean(_string(data.get("transfer_name", ""))),
                        "transfer_name": _clean(_string(data.get("transfer_name", ""))),
                        "mime_type": _clean(_string(data.get("mime_type", ""))),
                        "uti": _clean(_string(data.get("uti", ""))),
                        "size_bytes": int(data.get("total_bytes", 0) or 0),
                        "original_path": original_path,
                        "exported_path": exported_path,
                    }
                )
        return mapping

    def inspection_report(self, *, top_chat_limit: int = 20) -> dict[str, Any]:
        counts = {
            "message_count": int(self._scalar("SELECT COUNT(*) FROM message") or 0),
            "chat_count": int(self._scalar("SELECT COUNT(*) FROM chat") or 0) if self._table_exists("chat") else 0,
            "handle_count": int(self._scalar("SELECT COUNT(*) FROM handle") or 0)
            if self._table_exists("handle")
            else 0,
            "attachment_count": int(self._scalar("SELECT COUNT(*) FROM attachment") or 0)
            if self._table_exists("attachment")
            else 0,
            "latest_message_rowid": self.max_message_rowid(),
        }
        earliest_raw = self._scalar("SELECT MIN(date) FROM message")
        latest_raw = self._scalar("SELECT MAX(date) FROM message")
        service_breakdown: dict[str, int] = {}
        if self._table_exists("chat_message_join") and self._table_exists("chat"):
            for row in self.conn.execute(
                """
                SELECT COALESCE(NULLIF(m.service, ''), NULLIF(c.service_name, ''), 'unknown') AS service,
                       COUNT(*) AS count
                FROM message m
                LEFT JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
                LEFT JOIN chat c ON c.ROWID = cmj.chat_id
                GROUP BY service
                ORDER BY count DESC, service ASC
                """
            ).fetchall():
                service_breakdown[_clean(_string(row[0])) or "unknown"] = int(row[1] or 0)
        top_chats: list[dict[str, Any]] = []
        if self._table_exists("chat_message_join") and self._table_exists("chat"):
            for row in self.conn.execute(
                """
                SELECT c.ROWID AS chat_rowid,
                       c.guid,
                       c.chat_identifier,
                       c.display_name,
                       COUNT(*) AS message_count
                FROM chat c
                JOIN chat_message_join cmj ON cmj.chat_id = c.ROWID
                GROUP BY c.ROWID, c.guid, c.chat_identifier, c.display_name
                ORDER BY message_count DESC, c.ROWID ASC
                LIMIT ?
                """,
                (top_chat_limit,),
            ).fetchall():
                top_chats.append(
                    {
                        "chat_rowid": int(row["chat_rowid"]),
                        "guid": _clean(_string(row["guid"])),
                        "chat_identifier": _clean(_string(row["chat_identifier"])),
                        "display_name": _clean(_string(row["display_name"])),
                        "message_count": int(row["message_count"] or 0),
                    }
                )
        return {
            "snapshot_id": self.snapshot_id(),
            "source_label": self.source_label(),
            "snapshot_dir": str(self.snapshot_dir),
            "chat_db_path": str(self.db_path),
            "chat_db_size_bytes": self.db_path.stat().st_size,
            "earliest_message_at": _apple_time_to_iso(earliest_raw),
            "latest_message_at": _apple_time_to_iso(latest_raw),
            "service_breakdown": service_breakdown,
            "top_chats": top_chats,
            **counts,
        }


class IMessageAdapter(BaseAdapter):
    source_id = "imessage"
    preload_existing_uid_index = False

    def get_cursor_key(self, **kwargs) -> str:
        source_label = _clean(_string(kwargs.get("source_label", ""))).lower()
        return f"{self.source_id}:{source_label}" if source_label else self.source_id

    def _resolve_people(self, cache: IdentityCache, handles: list[str]) -> list[str]:
        links: list[str] = []
        for handle in handles:
            normalized = _normalize_handle(handle)
            if not normalized:
                continue
            prefix = "email" if "@" in normalized else "phone"
            resolved = cache.resolve(prefix, normalized)
            if resolved and resolved not in links:
                links.append(resolved)
        return links

    def _thread_item(
        self,
        *,
        chat_id: str,
        service: str,
        chat_identifier: str,
        display_name: str,
        participant_handles: list[str],
        people_links: list[str],
        message_uid: str,
        attachment_uids: list[str],
        sent_at: str,
        thread_body_messages: list[dict[str, Any]],
    ) -> dict[str, Any]:
        thread_body_sha = compute_imessage_thread_body_sha_from_payload(thread_body_messages)
        return {
            "kind": "thread",
            "chat_id": chat_id,
            "service": service,
            "chat_identifier": chat_identifier,
            "display_name": display_name,
            "participant_handles": participant_handles,
            "people": people_links,
            "messages": [_wikilink_from_uid(message_uid)],
            "attachments": [_wikilink_from_uid(uid) for uid in attachment_uids],
            "first_message_at": sent_at,
            "last_message_at": sent_at,
            "message_count": 1,
            "attachment_count": len(attachment_uids),
            "is_group": len(participant_handles) > 1,
            "has_attachments": bool(attachment_uids),
            "thread_body_sha": thread_body_sha,
            "_thread_body_messages": list(thread_body_messages),
            "created": _date_bucket(sent_at),
        }

    def _merge_thread_item(self, existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        existing["participant_handles"] = _merge_string_lists(
            list(existing.get("participant_handles", [])),
            list(incoming.get("participant_handles", [])),
        )
        existing["people"] = _merge_string_lists(
            list(existing.get("people", [])),
            list(incoming.get("people", [])),
        )
        existing["messages"] = _merge_string_lists(
            list(existing.get("messages", [])),
            list(incoming.get("messages", [])),
        )
        existing["attachments"] = _merge_string_lists(
            list(existing.get("attachments", [])),
            list(incoming.get("attachments", [])),
        )
        if incoming.get("service") and not existing.get("service"):
            existing["service"] = incoming["service"]
        if incoming.get("chat_identifier") and not existing.get("chat_identifier"):
            existing["chat_identifier"] = incoming["chat_identifier"]
        if incoming.get("display_name") and not existing.get("display_name"):
            existing["display_name"] = incoming["display_name"]

        first_values = [
            value for value in [existing.get("first_message_at", ""), incoming.get("first_message_at", "")] if value
        ]
        last_values = [
            value for value in [existing.get("last_message_at", ""), incoming.get("last_message_at", "")] if value
        ]
        if first_values:
            existing["first_message_at"] = min(first_values)
        if last_values:
            existing["last_message_at"] = max(last_values)

        existing["message_count"] = len(existing.get("messages", []))
        existing["attachment_count"] = len(existing.get("attachments", []))
        existing["is_group"] = len(existing.get("participant_handles", [])) > 1
        existing["has_attachments"] = bool(existing.get("attachments"))
        existing["created"] = _date_bucket(existing.get("first_message_at", "") or existing.get("created", ""))
        existing_messages = list(existing.get("_thread_body_messages", []))
        incoming_messages = list(incoming.get("_thread_body_messages", []))
        merged_messages = [*existing_messages]
        seen_message_ids = {
            str(item.get("message_id", "")).strip()
            for item in existing_messages
            if isinstance(item, dict) and str(item.get("message_id", "")).strip()
        }
        for item in incoming_messages:
            if not isinstance(item, dict):
                continue
            message_id = str(item.get("message_id", "")).strip()
            if message_id and message_id in seen_message_ids:
                continue
            if message_id:
                seen_message_ids.add(message_id)
            merged_messages.append(item)
        existing["_thread_body_messages"] = merged_messages
        existing["thread_body_sha"] = compute_imessage_thread_body_sha_from_payload(merged_messages)
        return existing

    def _prepare_message_bundle(
        self,
        *,
        row: dict[str, Any],
        chat: dict[str, Any],
        participant_handles: list[str],
        sender_handle: str,
        attachments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        message_rowid = int(row.get("message_rowid", 0) or 0)
        raw_message_id = _clean(_string(row.get("guid", ""))) or f"message-rowid:{message_rowid}"
        chat_rowid = int(chat.get("chat_rowid", 0) or 0)
        raw_chat_id = _clean(_string(chat.get("guid", ""))) or f"chat-rowid:{chat_rowid or 'unknown'}"
        merged_handles = list(participant_handles)
        if sender_handle and sender_handle not in merged_handles:
            merged_handles.append(sender_handle)
        merged_handles = _merge_string_lists([], merged_handles)
        service = _clean(_string(row.get("service", ""))) or _clean(_string(chat.get("service_name", "")))
        sent_at = _apple_time_to_iso(row.get("date"))
        attachment_ids = [item["attachment_id"] for item in attachments]
        attachment_uids = [_attachment_uid(raw_message_id, attachment_id) for attachment_id in attachment_ids]
        chat_identifier = _clean(_string(chat.get("chat_identifier", ""))) or (
            merged_handles[0] if len(merged_handles) == 1 else ""
        )
        display_name = _clean(_string(chat.get("display_name", ""))) or _clean(_string(chat.get("room_name", "")))
        return {
            "message_rowid": message_rowid,
            "message_id": raw_message_id,
            "chat_id": raw_chat_id,
            "service": service,
            "participant_handles": merged_handles,
            "sender_handle": sender_handle,
            "sent_at": sent_at,
            "edited_at": _apple_time_to_iso(row.get("date_edited")),
            "deleted_at": _apple_time_to_iso(row.get("date_retracted")),
            "subject": _clean(_string(row.get("subject", ""))),
            "associated_message_guid": _clean(_string(row.get("associated_message_guid", ""))),
            "associated_message_type": _clean(_string(row.get("associated_message_type", ""))),
            "associated_message_emoji": _clean(_string(row.get("associated_message_emoji", ""))),
            "expressive_send_style_id": _clean(_string(row.get("expressive_send_style_id", ""))),
            "balloon_bundle_id": _clean(_string(row.get("balloon_bundle_id", ""))),
            "has_attachments": bool(attachments),
            "attachments": attachments,
            "attachment_uids": attachment_uids,
            "body": _clean(_string(row.get("text", ""))) or _extract_attributed_text(row.get("attributedBody")),
            "chat_identifier": chat_identifier,
            "display_name": display_name,
            "is_from_me": bool(row.get("is_from_me")),
            "created": _date_bucket(sent_at),
        }

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        snapshot_dir: str = "",
        source_label: str = "local-messages",
        max_messages: int | None = 100,
        workers: int | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        if not snapshot_dir:
            raise ValueError("snapshot_dir is required")
        snapshot = MessageSnapshot(snapshot_dir)
        try:
            saved_rowid = int(cursor.get("last_completed_message_rowid", 0) or 0)
            max_rowid = snapshot.max_message_rowid()
            if saved_rowid > max_rowid:
                saved_rowid = 0
            rows = snapshot.iter_messages(saved_rowid, max_messages or 100)
            cache = IdentityCache(vault_path)
            worker_count = max(1, int(workers or os.environ.get("HFA_IMESSAGE_WORKERS") or min(8, os.cpu_count() or 1)))
            message_rowids = [int(row.get("message_rowid", 0) or 0) for row in rows]
            chat_by_message = snapshot.chats_for_messages(message_rowids)
            chat_rowids = sorted({int(chat.get("chat_rowid", 0) or 0) for chat in chat_by_message.values() if chat})
            participants_by_chat = snapshot.participants_for_chats(chat_rowids)
            handle_ids = sorted(
                {int(row.get("handle_id", 0) or 0) for row in rows if row.get("handle_id") not in (None, "", 0)}
            )
            handles_by_rowid = snapshot.handles_for_rowids(handle_ids)
            attachments_by_message = snapshot.attachments_for_messages(message_rowids)

            contexts = []
            for row in rows:
                message_rowid = int(row.get("message_rowid", 0) or 0)
                chat = chat_by_message.get(message_rowid, {})
                chat_rowid = int(chat.get("chat_rowid", 0) or 0)
                sender_handle = ""
                if not bool(row.get("is_from_me")):
                    handle_rowid = row.get("handle_id")
                    if handle_rowid not in (None, "", 0):
                        sender_handle = handles_by_rowid.get(int(handle_rowid), "")
                contexts.append(
                    {
                        "row": row,
                        "chat": chat,
                        "participant_handles": list(participants_by_chat.get(chat_rowid, [])),
                        "sender_handle": sender_handle,
                        "attachments": attachments_by_message.get(message_rowid, []),
                    }
                )

            prepare = partial(
                IMessageAdapter._prepare_message_bundle,
                self,
            )
            if worker_count > 1 and len(contexts) > 1:
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    bundles = list(executor.map(lambda context: prepare(**context), contexts))
            else:
                bundles = [prepare(**context) for context in contexts]

            thread_items_by_chat: dict[str, dict[str, Any]] = {}
            message_and_attachment_items: list[dict[str, Any]] = []
            for bundle in bundles:
                raw_message_id = bundle["message_id"]
                raw_chat_id = bundle["chat_id"]
                participant_handles = list(bundle["participant_handles"])
                sender_handle = bundle["sender_handle"]
                people_links = self._resolve_people(cache, [sender_handle, *participant_handles])
                message_uid = _message_uid(raw_message_id)
                thread_item = self._thread_item(
                    chat_id=raw_chat_id,
                    service=bundle["service"],
                    chat_identifier=bundle["chat_identifier"],
                    display_name=bundle["display_name"],
                    participant_handles=participant_handles,
                    people_links=people_links,
                    message_uid=message_uid,
                    attachment_uids=list(bundle["attachment_uids"]),
                    sent_at=bundle["sent_at"],
                    thread_body_messages=[
                        {
                            "message_id": raw_message_id,
                            "sent_at": bundle["sent_at"],
                            "sender_handle": sender_handle,
                            "subject": bundle["subject"],
                            "body": bundle["body"],
                            "type": "imessage_message",
                        }
                    ],
                )
                existing_thread = thread_items_by_chat.get(raw_chat_id)
                if existing_thread is None:
                    thread_items_by_chat[raw_chat_id] = thread_item
                else:
                    thread_items_by_chat[raw_chat_id] = self._merge_thread_item(existing_thread, thread_item)
                message_item = {
                    "kind": "message",
                    "message_id": raw_message_id,
                    "chat_id": raw_chat_id,
                    "thread": _wikilink_from_uid(_thread_uid(raw_chat_id)),
                    "service": bundle["service"],
                    "sender_handle": sender_handle,
                    "participant_handles": participant_handles,
                    "is_from_me": bundle["is_from_me"],
                    "sent_at": bundle["sent_at"],
                    "edited_at": bundle["edited_at"],
                    "deleted_at": bundle["deleted_at"],
                    "subject": bundle["subject"],
                    "associated_message_guid": bundle["associated_message_guid"],
                    "associated_message_type": bundle["associated_message_type"],
                    "associated_message_emoji": bundle["associated_message_emoji"],
                    "expressive_send_style_id": bundle["expressive_send_style_id"],
                    "balloon_bundle_id": bundle["balloon_bundle_id"],
                    "has_attachments": bundle["has_attachments"],
                    "attachments": [_wikilink_from_uid(uid) for uid in bundle["attachment_uids"]],
                    "people": people_links,
                    "body": bundle["body"],
                    "summary": _preview_text(bundle["body"]) or sender_handle or raw_message_id,
                    "created": bundle["created"],
                }
                message_and_attachment_items.append(message_item)
                for attachment, _attachment_uid in zip(bundle["attachments"], bundle["attachment_uids"], strict=True):
                    message_and_attachment_items.append(
                        {
                            "kind": "attachment",
                            "message_id": raw_message_id,
                            "chat_id": raw_chat_id,
                            "attachment_id": attachment["attachment_id"],
                            "message": _wikilink_from_uid(message_uid),
                            "thread": _wikilink_from_uid(_thread_uid(raw_chat_id)),
                            "filename": attachment["filename"],
                            "transfer_name": attachment["transfer_name"],
                            "mime_type": attachment["mime_type"],
                            "uti": attachment["uti"],
                            "size_bytes": attachment["size_bytes"],
                            "original_path": attachment["original_path"],
                            "exported_path": attachment["exported_path"],
                            "people": people_links,
                            "created": bundle["created"],
                        }
                    )
                if message_and_attachment_items:
                    message_and_attachment_items[-1]["_cursor"] = {
                        "snapshot_id": snapshot.snapshot_id(),
                        "source_label": _clean(source_label) or snapshot.source_label(),
                        "last_completed_message_rowid": bundle["message_rowid"],
                        "last_completed_message_id": raw_message_id,
                        "last_completed_chat_id": raw_chat_id,
                        "snapshot_max_message_rowid": max_rowid,
                    }
            return [*thread_items_by_chat.values(), *message_and_attachment_items]
        finally:
            snapshot.close()

    def to_card(self, item: dict[str, Any]) -> tuple[Any, dict[str, ProvenanceEntry], str]:
        today = date.today().isoformat()
        kind = _clean(_string(item.get("kind", "")))
        if kind == "thread":
            chat_id = _clean(_string(item.get("chat_id", "")))
            card = IMessageThreadCard(
                uid=_thread_uid(chat_id),
                type="imessage_thread",
                source=[THREAD_SOURCE],
                source_id=chat_id,
                created=_clean(_string(item.get("created", ""))) or today,
                updated=today,
                summary=_clean(_string(item.get("display_name", "")))
                or _clean(_string(item.get("chat_identifier", "")))
                or chat_id,
                people=list(item.get("people", [])),
                imessage_chat_id=chat_id,
                service=_clean(_string(item.get("service", ""))),
                chat_identifier=_clean(_string(item.get("chat_identifier", ""))),
                display_name=_clean(_string(item.get("display_name", ""))),
                participant_handles=list(item.get("participant_handles", [])),
                messages=list(item.get("messages", [])),
                attachments=list(item.get("attachments", [])),
                first_message_at=_clean(_string(item.get("first_message_at", ""))),
                last_message_at=_clean(_string(item.get("last_message_at", ""))),
                message_count=int(item.get("message_count", 0) or 0),
                attachment_count=int(item.get("attachment_count", 0) or 0),
                is_group=bool(item.get("is_group", False)),
                has_attachments=bool(item.get("has_attachments", False)),
                thread_body_sha=_clean(_string(item.get("thread_body_sha", ""))),
            )
            return card, deterministic_provenance(card, THREAD_SOURCE), ""

        if kind == "message":
            message_id = _clean(_string(item.get("message_id", "")))
            body = _clean(_string(item.get("body", "")))
            card = IMessageMessageCard(
                uid=_message_uid(message_id),
                type="imessage_message",
                source=[MESSAGE_SOURCE],
                source_id=message_id,
                created=_clean(_string(item.get("created", ""))) or today,
                updated=today,
                summary=_clean(_string(item.get("summary", ""))) or message_id,
                people=list(item.get("people", [])),
                imessage_message_id=message_id,
                imessage_chat_id=_clean(_string(item.get("chat_id", ""))),
                thread=_clean(_string(item.get("thread", ""))),
                service=_clean(_string(item.get("service", ""))),
                sender_handle=_clean(_string(item.get("sender_handle", ""))),
                participant_handles=list(item.get("participant_handles", [])),
                is_from_me=bool(item.get("is_from_me", False)),
                sent_at=_clean(_string(item.get("sent_at", ""))),
                edited_at=_clean(_string(item.get("edited_at", ""))),
                deleted_at=_clean(_string(item.get("deleted_at", ""))),
                subject=_clean(_string(item.get("subject", ""))),
                associated_message_guid=_clean(_string(item.get("associated_message_guid", ""))),
                associated_message_type=_clean(_string(item.get("associated_message_type", ""))),
                associated_message_emoji=_clean(_string(item.get("associated_message_emoji", ""))),
                expressive_send_style_id=_clean(_string(item.get("expressive_send_style_id", ""))),
                balloon_bundle_id=_clean(_string(item.get("balloon_bundle_id", ""))),
                has_attachments=bool(item.get("has_attachments", False)),
                attachments=list(item.get("attachments", [])),
            )
            return card, deterministic_provenance(card, MESSAGE_SOURCE), body

        if kind == "attachment":
            message_id = _clean(_string(item.get("message_id", "")))
            attachment_id = _clean(_string(item.get("attachment_id", "")))
            card = IMessageAttachmentCard(
                uid=_attachment_uid(message_id, attachment_id),
                type="imessage_attachment",
                source=[ATTACHMENT_SOURCE],
                source_id=f"{message_id}:{attachment_id}",
                created=_clean(_string(item.get("created", ""))) or today,
                updated=today,
                summary=_clean(_string(item.get("filename", "")))
                or _clean(_string(item.get("transfer_name", "")))
                or attachment_id,
                people=list(item.get("people", [])),
                imessage_message_id=message_id,
                imessage_chat_id=_clean(_string(item.get("chat_id", ""))),
                attachment_id=attachment_id,
                message=_clean(_string(item.get("message", ""))),
                thread=_clean(_string(item.get("thread", ""))),
                filename=_clean(_string(item.get("filename", ""))),
                transfer_name=_clean(_string(item.get("transfer_name", ""))),
                mime_type=_clean(_string(item.get("mime_type", ""))),
                uti=_clean(_string(item.get("uti", ""))),
                size_bytes=int(item.get("size_bytes", 0) or 0),
                original_path=_clean(_string(item.get("original_path", ""))),
                exported_path=_clean(_string(item.get("exported_path", ""))),
            )
            return card, deterministic_provenance(card, ATTACHMENT_SOURCE), ""

        raise ValueError(f"Unsupported iMessage record kind: {kind}")

    def merge_card(self, vault_path, rel_path, card, body, provenance) -> None:
        if card.type == "imessage_thread":
            frontmatter, existing_body, existing_provenance = read_note(vault_path, str(rel_path))
            existing_card = validate_card_permissive(frontmatter)
            merged_data = existing_card.model_dump(mode="python")
            incoming = card.model_dump(mode="python")
            changed = False

            for field_name in ("source", "people", "orgs", "tags", "participant_handles", "messages", "attachments"):
                existing_values = merged_data.get(field_name, [])
                incoming_values = incoming.get(field_name, [])
                if not isinstance(existing_values, list) or not isinstance(incoming_values, list):
                    continue
                merged_values = _merge_string_lists(existing_values, incoming_values)
                if merged_values != existing_values:
                    merged_data[field_name] = merged_values
                    changed = True

            for field_name in ("service", "chat_identifier", "display_name"):
                incoming_value = incoming.get(field_name, "")
                if incoming_value and merged_data.get(field_name) != incoming_value and not merged_data.get(field_name):
                    merged_data[field_name] = incoming_value
                    changed = True

            first_values = [
                value
                for value in [merged_data.get("first_message_at", ""), incoming.get("first_message_at", "")]
                if value
            ]
            if first_values:
                first_message_at = min(first_values)
                if merged_data.get("first_message_at") != first_message_at:
                    merged_data["first_message_at"] = first_message_at
                    changed = True

            last_values = [
                value
                for value in [merged_data.get("last_message_at", ""), incoming.get("last_message_at", "")]
                if value
            ]
            if last_values:
                last_message_at = max(last_values)
                if merged_data.get("last_message_at") != last_message_at:
                    merged_data["last_message_at"] = last_message_at
                    changed = True

            message_count = max(
                len(merged_data.get("messages", [])),
                int(merged_data.get("message_count", 0) or 0),
                int(incoming.get("message_count", 0) or 0),
            )
            if merged_data.get("message_count") != message_count:
                merged_data["message_count"] = message_count
                changed = True

            attachment_count = max(
                len(merged_data.get("attachments", [])),
                int(merged_data.get("attachment_count", 0) or 0),
                int(incoming.get("attachment_count", 0) or 0),
            )
            if merged_data.get("attachment_count") != attachment_count:
                merged_data["attachment_count"] = attachment_count
                changed = True

            has_attachments = (
                bool(merged_data.get("attachments")) or bool(incoming.get("has_attachments")) or attachment_count > 0
            )
            if merged_data.get("has_attachments") != has_attachments:
                merged_data["has_attachments"] = has_attachments
                changed = True

            # Recomputing thread_body_sha requires rereading every linked message note in the
            # thread, which becomes prohibitively expensive during large resume imports.
            # Keep the existing non-empty hash during merge; initial thread creation still sets it.
            incoming_thread_sha = str(incoming.get("thread_body_sha", "")).strip()
            if not str(merged_data.get("thread_body_sha", "")).strip() and incoming_thread_sha:
                merged_data["thread_body_sha"] = incoming_thread_sha
                changed = True

            if changed:
                merged_data["updated"] = date.today().isoformat()

            merged_card = validate_card_strict(merged_data)
            merged_provenance = merge_provenance(existing_provenance, provenance)
            write_card(vault_path, str(rel_path), merged_card, body=body or existing_body, provenance=merged_provenance)
            return

        self._replace_generic_card(vault_path, rel_path, card, body, provenance)

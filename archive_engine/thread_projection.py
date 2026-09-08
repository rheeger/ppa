"""Revision-aware thread aggregates. Dirty scopes reuse OutputReceipt."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from archive_engine.contracts import OutputReceipt, OutputRevision

PROCESSOR = "thread-projection"
PROCESSOR_VERSION = "p31.g.1"


def parse_instant(raw: str) -> tuple[str, bool]:
    """Return (sortable_key, known). Unknown/empty sorts last and is not a bound."""

    text = str(raw or "").strip()
    if not text:
        return "", False
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            cleaned = text.replace("Z", "+00:00") if text.endswith("Z") else text
            dt = (
                datetime.fromisoformat(cleaned)
                if "T" in cleaned or "+" in cleaned
                else datetime.strptime(text[:19], fmt)
            )
            return dt.isoformat(), True
        except ValueError:
            continue
    return text, True


def aggregate_messages(messages: list[dict[str, Any]]) -> dict[str, Any]:
    known: list[str] = []
    for row in messages:
        key, ok = parse_instant(str(row.get("sent_at") or row.get("created") or ""))
        if ok and key:
            known.append(key)
    known.sort()
    return {
        "message_count": len(messages),
        "first_message_at": known[0] if known else "",
        "last_message_at": known[-1] if known else "",
        "member_uids": tuple(str(row.get("uid") or "") for row in messages if row.get("uid")),
    }


def pending_receipt(thread_uid: str, revision: str, *, status: str = "pending") -> OutputReceipt:
    return OutputReceipt(
        processor=PROCESSOR,
        processor_version=PROCESSOR_VERSION,
        input_uid=thread_uid,
        input_revision=revision or "unknown",
        status=status,  # type: ignore[arg-type]
        outputs=(OutputRevision(uid=thread_uid, revision=revision or "unknown"),),
    )


def pending_thread_uids(vault: Path) -> list[str]:
    from archive_engine.journaled_state import THREAD_RECEIPTS_REL, load_json_state

    payload = load_json_state(Path(vault), THREAD_RECEIPTS_REL)
    out: list[str] = []
    for item in payload.get("receipts") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("status") or "") != "pending":
            continue
        uid = str(item.get("input_uid") or "").strip()
        if uid:
            out.append(uid)
    return out


def mark_receipts_complete(vault: Path, uids: list[str]) -> None:
    from archive_engine.journaled_state import (
        THREAD_RECEIPTS_REL,
        THREAD_RECEIPTS_UID,
        load_json_state,
        persist_json_state,
    )

    wanted = {str(uid) for uid in uids if str(uid).strip()}
    if not wanted:
        return
    payload = load_json_state(Path(vault), THREAD_RECEIPTS_REL)
    receipts = []
    changed = False
    for item in payload.get("receipts") or []:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        if str(row.get("input_uid") or "") in wanted and str(row.get("status") or "") == "pending":
            row["status"] = "complete"
            changed = True
        receipts.append(row)
    if changed:
        persist_json_state(
            Path(vault),
            uid=THREAD_RECEIPTS_UID,
            rel=THREAD_RECEIPTS_REL,
            payload={"receipts": receipts},
            source="thread-projection",
        )


def drain_pending(vault: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Converge pending parent scopes from live membership, then complete receipts."""

    pending = pending_thread_uids(vault)
    if not pending:
        return {"updated_threads": 0, "pending": [], "applied": True, "drained": 0}
    wanted = set(pending)
    scoped: list[dict[str, Any]] = []
    for row in rows:
        fm = row.get("frontmatter") or {}
        card_type = str(fm.get("type") or "")
        uid = str(fm.get("uid") or "")
        if card_type == "imessage_thread" and uid in wanted:
            scoped.append(row)
        elif card_type == "imessage_message":
            scoped.append(row)
    result = project_from_rows(vault, scoped)
    completed = [uid for uid in pending if uid not in result["pending"]]
    mark_receipts_complete(vault, completed)
    return {**result, "drained": len(completed)}


def persist_pending_receipt(vault: Path, receipt: OutputReceipt) -> Any:
    from archive_engine.journaled_state import (
        THREAD_RECEIPTS_REL,
        THREAD_RECEIPTS_UID,
        load_json_state,
        persist_json_state,
    )

    payload = load_json_state(Path(vault), THREAD_RECEIPTS_REL)
    receipts = {str(item.get("input_uid")): item for item in (payload.get("receipts") or []) if isinstance(item, dict)}
    receipts[receipt.input_uid] = receipt.to_payload()
    persist_json_state(
        Path(vault),
        uid=THREAD_RECEIPTS_UID,
        rel=THREAD_RECEIPTS_REL,
        payload={"receipts": list(receipts.values())},
        source="thread-projection",
    )
    return receipt


def project_thread(vault: Path, thread_rel: str, messages: list[dict[str, Any]]) -> dict[str, Any]:
    """Write derived parent fields only when they change. Idempotent recount."""

    from archive_vault.vault import read_note, update_frontmatter_fields

    derived = aggregate_messages(messages)
    frontmatter, _body, _prov = read_note(vault, thread_rel)
    updates: dict[str, Any] = {}
    for field in ("message_count", "first_message_at", "last_message_at"):
        current = frontmatter.get(field)
        incoming = derived[field]
        if field == "message_count":
            if int(current or 0) != int(incoming or 0):
                updates[field] = incoming
        elif str(current or "") != str(incoming or ""):
            updates[field] = incoming
    if updates:
        update_frontmatter_fields(vault, thread_rel, updates)
    return {"rel_path": thread_rel, "updated": bool(updates), "derived": derived, "applied": updates}


def project_from_rows(vault: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Group message rows by thread wikilink and converge each parent."""

    from archive_vault.canon.wikilink import parse as parse_wikilink
    from archive_vault.vault import read_note_by_uid

    by_thread: dict[str, list[dict[str, Any]]] = defaultdict(list)
    threads = [row for row in rows if str((row.get("frontmatter") or {}).get("type") or "") == "imessage_thread"]
    messages = [row for row in rows if str((row.get("frontmatter") or {}).get("type") or "") == "imessage_message"]
    thread_rel = {
        str((row.get("frontmatter") or {}).get("uid") or ""): str(row.get("rel_path") or "") for row in threads
    }
    pending: list[str] = []
    updated = 0
    for row in messages:
        fm = row.get("frontmatter") or {}
        ref = parse_wikilink(str(fm.get("thread") or ""))
        if not ref:
            continue
        by_thread[ref].append(fm)
    for uid, msgs in by_thread.items():
        rel = thread_rel.get(uid)
        if not rel:
            found = read_note_by_uid(vault, uid)
            rel = found[0] if found else ""
        if not rel:
            pending.append(uid)
            continue
        result = project_thread(vault, rel, msgs)
        updated += int(result["updated"])
    return {"updated_threads": updated, "pending": pending, "applied": True}

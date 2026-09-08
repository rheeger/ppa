"""Journaled JSON state files. Preview never writes; apply goes through ChangeJournal."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from archive_vault.change_journal import (
    OPERATION_CREATE,
    OPERATION_UPDATE,
    ChangeJournal,
)
from archive_vault.paths import normalize_vault_rel

IDENTITY_PROPOSALS_REL = "_meta/identity-proposals.json"
IDENTITY_PROPOSALS_UID = "hfa-identity-proposals"
CONVERSATION_PROPOSALS_REL = "_meta/conversation-proposals.json"
CONVERSATION_PROPOSALS_UID = "hfa-conversation-proposals"
SCAN_REJECTIONS_REL = "_meta/scan-rejections.json"
SCAN_REJECTIONS_UID = "hfa-scan-rejections"
THREAD_RECEIPTS_REL = "_meta/thread-projection-receipts.json"
THREAD_RECEIPTS_UID = "hfa-thread-projection-receipts"
PUBLICATION_CAPTURE_REL = "_meta/publication-capture.json"
PUBLICATION_CAPTURE_UID = "hfa-publication-capture"


def load_json_state(vault: Path, rel: str) -> dict[str, Any]:
    path = Path(vault) / rel
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def persist_json_state(
    vault: Path,
    *,
    uid: str,
    rel: str,
    payload: dict[str, Any],
    source: str,
) -> Any:
    root = Path(vault)
    rel_norm = str(normalize_vault_rel(rel))
    body = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    operation = OPERATION_UPDATE if (root / rel_norm).is_file() else OPERATION_CREATE
    with ChangeJournal(root) as journal:
        return journal.apply_mutation(
            uid=uid,
            rel_path=rel_norm,
            operation=operation,
            content=body.encode("utf-8"),
            source=source,
        )


def merge_identity_proposals(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, Any]] = []
    for item in existing + incoming:
        uids = [str(uid) for uid in (item.get("uids") or []) if str(uid).strip()]
        if len(uids) < 2:
            continue
        pair = tuple(sorted(uids[:2]))
        if pair in seen:
            continue
        seen.add(pair)
        out.append({**item, "uids": list(pair), "proposal_id": f"{pair[0]}::{pair[1]}"})
    return out

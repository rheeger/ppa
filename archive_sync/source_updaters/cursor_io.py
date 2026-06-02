"""Read-only cursor helpers (no source sync)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from archive_vault.sync_state import load_sync_state


def read_adapter_cursor(vault_path: str | Path, adapter_source_id: str) -> dict[str, Any]:
    """Load cursor payload from vault sync-state without fetching."""

    state = load_sync_state(vault_path)
    cursor = state.get(adapter_source_id, {})
    return dict(cursor) if isinstance(cursor, dict) else {}


def summarize_cursor_payload(cursor: dict[str, Any]) -> str:
    if not cursor:
        return "(empty)"
    for key in (
        "gmail_history_id",
        "history_id",
        "sync_token",
        "last_completed_message_rowid",
        "modified_at",
        "metadata_hash",
        "watermark_hash",
        "last_sync",
    ):
        if cursor.get(key) not in (None, ""):
            return f"{key}={cursor[key]}"
    if cursor.get("processed") is not None:
        return f"processed={cursor['processed']}"
    return f"keys={','.join(sorted(cursor.keys())[:5])}"

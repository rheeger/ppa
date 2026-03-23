"""Sync-state helpers for HFA adapters."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def _state_path(vault_path: str | Path) -> Path:
    return Path(vault_path) / "_meta" / "sync-state.json"


def load_sync_state(vault_path: str | Path) -> dict[str, dict[str, Any]]:
    """Load sync-state data, returning an empty state on first run."""

    path = _state_path(vault_path)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def save_sync_state(vault_path: str | Path, state: dict[str, dict[str, Any]]) -> None:
    """Atomically persist sync-state data."""

    path = _state_path(vault_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="sync-state-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def update_cursor(vault_path: str | Path, source_key: str, cursor_data: dict[str, Any]) -> None:
    """Merge cursor data into a source entry and save."""

    state = load_sync_state(vault_path)
    current = state.get(source_key, {})
    if not isinstance(current, dict):
        current = {}
    current.update(cursor_data)
    state[source_key] = current
    save_sync_state(vault_path, state)

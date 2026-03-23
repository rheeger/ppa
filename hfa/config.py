"""Vault-level HFA configuration."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, fields
from pathlib import Path


@dataclass
class PPAConfig:
    merge_threshold: int = 90
    conflict_threshold: int = 75
    fuzzy_name_threshold: float = 85.0
    finance_min_amount: float = 20.0
    dedup_sweep_auto_merge: bool = True
    max_enrichment_log_entries: int = 100
    imessage_thread_body_sha_cache_enabled: bool = True
    gmail_thread_body_sha_cache_enabled: bool = True
    calendar_event_body_sha_cache_enabled: bool = True
    otter_transcript_body_sha_cache_enabled: bool = True


def _config_path(vault_path: str | Path) -> Path:
    return Path(vault_path) / "_meta" / "ppa-config.json"


def load_config(vault_path: str | Path) -> PPAConfig:
    """Load config from disk, ignoring unknown keys."""

    path = _config_path(vault_path)
    if not path.exists():
        return PPAConfig()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return PPAConfig()
    if not isinstance(payload, dict):
        return PPAConfig()
    known = {field.name for field in fields(PPAConfig)}
    filtered = {key: value for key, value in payload.items() if key in known}
    return PPAConfig(**filtered)


def save_config(vault_path: str | Path, config: PPAConfig) -> None:
    """Atomically persist config to disk."""

    path = _config_path(vault_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="ppa-config-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(asdict(config), handle, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

"""Vault initialization script tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def test_hfa_init_vault_creates_expected_layout(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "archive_scripts" / "ppa-init-vault.sh"
    vault = tmp_path / "hf-archives"
    env = os.environ.copy()
    env["PYTHON"] = sys.executable

    result = subprocess.run(
        ["bash", str(script_path), str(vault)],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    assert (vault / "People").is_dir()
    assert (vault / "Finance").is_dir()
    assert (vault / "Photos").is_dir()
    assert (vault / "Attachments").is_dir()
    assert (vault / "_meta").is_dir()

    expected_files = {
        "identity-map.json",
        "sync-state.json",
        "own-emails.json",
        "dedup-candidates.json",
        "enrichment-log.json",
        "llm-cache.json",
        "ppa-config.json",
        "llm-config.json",
        "nicknames.json",
    }
    actual_files = {path.name for path in (vault / "_meta").iterdir() if path.is_file()}
    assert expected_files.issubset(actual_files)

    ppa_config = json.loads((vault / "_meta" / "ppa-config.json").read_text(encoding="utf-8"))
    assert ppa_config["merge_threshold"] == 90
    assert ppa_config["imessage_thread_body_sha_cache_enabled"] is True
    assert ppa_config["gmail_thread_body_sha_cache_enabled"] is True
    assert ppa_config["calendar_event_body_sha_cache_enabled"] is True
    llm_config = json.loads((vault / "_meta" / "llm-config.json").read_text(encoding="utf-8"))
    assert llm_config["primary"]["provider"] == "gemini"

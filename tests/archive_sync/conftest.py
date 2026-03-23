"""Fixtures and path bootstrap for archive-sync tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Photos").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text("[]", encoding="utf-8")
    (meta / "enrichment-log.json").write_text("[]", encoding="utf-8")
    (meta / "llm-cache.json").write_text("{}", encoding="utf-8")
    (meta / "own-emails.json").write_text('["me@example.com"]', encoding="utf-8")
    (meta / "nicknames.json").write_text(json.dumps({"robert": ["rob", "robbie"]}), encoding="utf-8")
    (meta / "ppa-config.json").write_text(json.dumps({"finance_min_amount": 20.0}), encoding="utf-8")
    return vault

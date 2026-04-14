"""Post-import script tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card


@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
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
    (meta / "nicknames.json").write_text(json.dumps({"robert": ["rob", "robbie"]}), encoding="utf-8")
    (meta / "ppa-config.json").write_text(json.dumps({"max_enrichment_log_entries": 5}), encoding="utf-8")
    return vault


def test_hfa_post_import_logs_run(tmp_vault):
    person = PersonCard(
        uid="hfa-person-aaaabbbbcccc",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com"],
        company="Endaoment",
    )
    write_card(
        tmp_vault,
        "People/jane-smith.md",
        person,
        provenance={
            "summary": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
            "emails": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
            "company": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        },
    )

    repo_root = Path(__file__).resolve().parent.parent
    script_path = repo_root / "archive_scripts" / "ppa-post-import.sh"
    env = os.environ.copy()
    env["PPA_PATH"] = str(tmp_vault)
    env["PYTHON"] = sys.executable

    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    assert "Post-import complete" in result.stdout

    log = json.loads((tmp_vault / "_meta" / "enrichment-log.json").read_text(encoding="utf-8"))
    assert len(log) == 1
    assert "dedup-sweep" in log[0]["dedup"]
    assert "validate:" in log[0]["validate"]
    assert "Archive Doctor Stats" in log[0]["stats"]

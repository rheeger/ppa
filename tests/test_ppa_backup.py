"""Backup script tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from hfa.provenance import ProvenanceEntry
from hfa.schema import PersonCard
from hfa.vault import write_card


@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "_meta").mkdir()
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    return vault


def test_hfa_backup_creates_encrypted_artifacts_only(tmp_vault, tmp_path):
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
    backup_root = tmp_path / "backups"
    script_path = repo_root / "scripts" / "ppa-backup.sh"
    env = os.environ.copy()
    env["PPA_PATH"] = str(tmp_vault)
    env["PPA_BACKUP_BASE"] = str(backup_root)
    env["PPA_BACKUP_PASSPHRASE"] = "test-passphrase"

    result = subprocess.run(
        ["bash", str(script_path)],
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0
    assert "ppa-backup:" in result.stdout
    artifacts = list((backup_root / "artifacts").glob("*/ppa-backup.tar.enc"))
    assert len(artifacts) == 1
    latest_dir = backup_root / "latest"
    assert (latest_dir / "ppa-backup.tar.enc").exists()
    assert (latest_dir / "ppa-backup.manifest.json.enc").exists()
    assert (latest_dir / "ppa-backup.tar.enc.sha256").exists()
    assert not (latest_dir / "People" / "jane-smith.md").exists()

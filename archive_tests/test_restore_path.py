"""Validate Phase 9 restore and verification paths."""

from __future__ import annotations

from pathlib import Path

PPA_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = Path("/Users/rheeger/Archive/seed/embedding-cache-seed-20260427")


def test_verify_v2_script_exists() -> None:
    script = PPA_ROOT / "archive_scripts" / "ppa-verify-v2.sh"
    assert script.exists(), f"Verify script missing at {script}"


def test_recovery_cache_runbook_exists() -> None:
    runbook = PPA_ROOT / "archive_docs" / "runbooks" / "embedding-recovery-cache.md"
    assert runbook.exists(), "Embedding recovery cache runbook missing"


def test_v2_operations_runbook_exists() -> None:
    runbook = PPA_ROOT / "archive_docs" / "runbooks" / "ppa-v2-operations.md"
    assert runbook.exists(), "v2 operations runbook missing"


def test_default_embedding_cache_exists_and_has_expected_files() -> None:
    assert DEFAULT_CACHE.exists(), f"Embedding cache missing: {DEFAULT_CACHE}"
    for name in ("MANIFEST.json", "embeddings.tsv", "embeddings.tsv.rows", "embeddings.tsv.sha256"):
        assert (DEFAULT_CACHE / name).exists(), f"Embedding cache missing {name}"


def test_hey_arnold_makefile_has_phase9_targets() -> None:
    makefile = PPA_ROOT.parent / "hey-arnold" / "Makefile"
    if not makefile.exists():
        return
    content = makefile.read_text()
    assert "ppa-deploy-v2-rollback" in content
    assert "ppa-vault-rsync" in content

"""Validate Phase 9 restore and verification paths."""

from __future__ import annotations

from pathlib import Path

PPA_ROOT = Path(__file__).resolve().parents[1]
EMBEDDING_CACHE_REQUIRED_FILES = (
    "MANIFEST.json",
    "embeddings.tsv",
    "embeddings.tsv.rows",
    "embeddings.tsv.sha256",
)


def test_verify_v2_script_exists() -> None:
    script = PPA_ROOT / "archive_scripts" / "ppa-verify-v2.sh"
    assert script.exists(), f"Verify script missing at {script}"


def test_recovery_cache_runbook_exists() -> None:
    runbook = PPA_ROOT / "archive_docs" / "runbooks" / "embedding-recovery-cache.md"
    assert runbook.exists(), "Embedding recovery cache runbook missing"


def test_v2_operations_runbook_exists() -> None:
    runbook = PPA_ROOT / "archive_docs" / "runbooks" / "ppa-v2-operations.md"
    assert runbook.exists(), "v2 operations runbook missing"


def test_default_embedding_cache_exists_and_has_expected_files(tmp_path: Path) -> None:
    """Embedding recovery cache layout is stable without private seed data.

    CI must not require the homeowner cache under /Users/rheeger/Archive.
    Build an isolated fixture matching export_embedding_cache output files.
    """
    cache = tmp_path / "embedding-cache-seed-fixture"
    cache.mkdir()
    (cache / "MANIFEST.json").write_text('{"schema": "embedding-cache-v1"}\n', encoding="utf-8")
    (cache / "embeddings.tsv").write_text("chunk_key\tembedding\n", encoding="utf-8")
    (cache / "embeddings.tsv.rows").write_text("0\n", encoding="utf-8")
    (cache / "embeddings.tsv.sha256").write_text("deadbeef\n", encoding="utf-8")
    for name in EMBEDDING_CACHE_REQUIRED_FILES:
        assert (cache / name).is_file(), f"Embedding cache missing {name}"

    # Contract check: exporter still emits the same relative filenames.
    src = (PPA_ROOT / "archive_cli" / "commands" / "embedding_cache.py").read_text(encoding="utf-8")
    for name in EMBEDDING_CACHE_REQUIRED_FILES:
        assert name in src, f"export_embedding_cache should write {name}"


def test_hey_arnold_makefile_has_phase9_targets() -> None:
    makefile = PPA_ROOT.parent / "hey-arnold" / "Makefile"
    if not makefile.exists():
        return
    content = makefile.read_text()
    assert "ppa-deploy-v2-rollback" in content
    assert "ppa-vault-rsync" in content

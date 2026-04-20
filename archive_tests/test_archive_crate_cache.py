"""Vault cache parity: Rust `archive_crate` vs Python `archive_cli.vault_cache` (tier 1 and tier 2)."""

from __future__ import annotations

import json
import sqlite3
import zlib
from pathlib import Path

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate
from archive_cli.vault_cache import VaultScanCache, _compute_vault_fingerprint, _frontmatter_hash_stable
from archive_tests.fixtures import load_fixture_vault
from archive_vault.schema import validate_card_permissive
from archive_vault.vault import iter_note_paths


def test_vault_fingerprint_matches_python(tmp_path):
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    vault_s = str(vault)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    stats_py, fp_py = _compute_vault_fingerprint(vault, rel_paths)
    stats_r, fp_r = archive_crate.vault_fingerprint(vault_s)
    assert fp_r == fp_py
    assert dict(stats_r) == stats_py


def test_build_vault_cache_tier1_row_parity(tmp_path):
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    vault_s = str(vault)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]

    py_cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0, no_cache=True)
    assert py_cache.note_count() == len(rel_paths)

    rust_path = tmp_path / "rust-cache.sqlite3"
    out = archive_crate.build_vault_cache(vault_s, str(rust_path), 1)
    assert out["note_count"] == len(rel_paths)
    assert out["fingerprint"] == py_cache.vault_fingerprint()

    conn = sqlite3.connect(str(rust_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT rel_path, uid, card_type, slug, mtime_ns, file_size, "
        "frontmatter_json, frontmatter_hash FROM notes ORDER BY rel_path"
    ).fetchall()
    conn.close()

    for row in rows:
        rel = row["rel_path"]
        fm = py_cache.frontmatter_for_rel_path(rel)
        assert json.loads(row["frontmatter_json"]) == fm
        assert row["frontmatter_json"] == json.dumps(fm, sort_keys=True, default=str)
        card = validate_card_permissive(fm)
        assert row["uid"] == str(card.uid).strip()
        assert row["card_type"] == str(card.type or "")
        assert row["slug"] == Path(rel).stem
        st_py = py_cache.file_stats()[rel]
        assert row["mtime_ns"] == st_py[0]
        assert row["file_size"] == st_py[1]
        assert row["frontmatter_hash"] == _frontmatter_hash_stable(fm)


def test_build_vault_cache_incremental_basic(tmp_path):
    """Rust incremental build produces same note_count and fingerprint as full build."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    vault_s = str(vault)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]

    cache_path = tmp_path / "incr-cache.sqlite3"
    out_full = archive_crate.build_vault_cache(vault_s, str(cache_path), 2)
    assert out_full["note_count"] == len(rel_paths)

    out_incr = archive_crate.build_vault_cache_incremental(vault_s, str(cache_path), 2)
    assert out_incr["note_count"] == len(rel_paths)
    assert out_incr["fingerprint"] == out_full["fingerprint"]
    assert out_incr["unchanged"] == len(rel_paths)
    assert out_incr["rebuilt"] == 0
    assert out_incr["deleted"] == 0


def test_build_vault_cache_incremental_detects_change(tmp_path):
    """Rust incremental build detects a changed file and rebuilds only that note."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    vault_s = str(vault)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]

    cache_path = tmp_path / "incr-cache2.sqlite3"
    archive_crate.build_vault_cache(vault_s, str(cache_path), 2)

    target = next(vault.rglob("*.md"))
    content = target.read_text()
    target.write_text(content + "\n<!-- changed -->\n")

    out_incr = archive_crate.build_vault_cache_incremental(vault_s, str(cache_path), 2)
    assert out_incr["rebuilt"] >= 1
    assert out_incr["unchanged"] == len(rel_paths) - out_incr["rebuilt"]
    assert out_incr["note_count"] == len(rel_paths)


def test_build_vault_cache_incremental_detects_delete(tmp_path):
    """Rust incremental build purges deleted notes and adds new ones."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    vault_s = str(vault)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    n_orig = len(rel_paths)

    cache_path = tmp_path / "incr-cache3.sqlite3"
    archive_crate.build_vault_cache(vault_s, str(cache_path), 2)

    target = next(vault.rglob("*.md"))
    target.unlink()

    out_incr = archive_crate.build_vault_cache_incremental(vault_s, str(cache_path), 2)
    assert out_incr["deleted"] >= 1
    assert out_incr["note_count"] == n_orig - 1


def test_build_vault_cache_incremental_detects_add(tmp_path):
    """Rust incremental build picks up newly added notes."""
    import shutil
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    vault_s = str(vault)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    n_orig = len(rel_paths)

    cache_path = tmp_path / "incr-cache4.sqlite3"
    archive_crate.build_vault_cache(vault_s, str(cache_path), 2)

    src = next(vault.rglob("People/*.md"))
    dest = vault / "Documents" / "new-note-for-incr.md"
    shutil.copy2(src, dest)

    out_incr = archive_crate.build_vault_cache_incremental(vault_s, str(cache_path), 2)
    assert out_incr["rebuilt"] >= 1
    assert out_incr["note_count"] == n_orig + 1


def test_build_vault_cache_tier2_row_parity(tmp_path):
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    vault_s = str(vault)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]

    py_cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0, no_cache=True)
    assert py_cache.note_count() == len(rel_paths)
    assert py_cache.tier() == 2

    rust_path = tmp_path / "rust-cache-t2.sqlite3"
    out = archive_crate.build_vault_cache(vault_s, str(rust_path), 2)
    assert out["note_count"] == len(rel_paths)
    assert out["fingerprint"] == py_cache.vault_fingerprint()

    conn = sqlite3.connect(str(rust_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT rel_path, uid, card_type, slug, mtime_ns, file_size, "
        "frontmatter_json, frontmatter_hash, body_compressed, content_hash, "
        "wikilinks_json, raw_content_sha256 FROM notes ORDER BY rel_path"
    ).fetchall()
    conn.close()

    for row in rows:
        rel = row["rel_path"]
        fm = py_cache.frontmatter_for_rel_path(rel)
        assert json.loads(row["frontmatter_json"]) == fm
        assert row["frontmatter_json"] == json.dumps(fm, sort_keys=True, default=str)
        card = validate_card_permissive(fm)
        assert row["uid"] == str(card.uid).strip()
        assert row["card_type"] == str(card.type or "")
        assert row["slug"] == Path(rel).stem
        st_py = py_cache.file_stats()[rel]
        assert row["mtime_ns"] == st_py[0]
        assert row["file_size"] == st_py[1]
        assert row["frontmatter_hash"] == _frontmatter_hash_stable(fm)

        body = py_cache.body_for_rel_path(rel)
        assert zlib.decompress(row["body_compressed"]).decode("utf-8") == body
        assert row["content_hash"] == py_cache.content_hash_for_rel_path(rel)
        assert json.loads(row["wikilinks_json"]) == py_cache.wikilinks_for_rel_path(rel)
        assert row["raw_content_sha256"] == py_cache.raw_content_sha256_for_rel_path(rel)

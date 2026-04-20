"""Tests for archive_cli.vault_cache (SQLite vault scan cache)."""

from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path

import pytest

from archive_cli.scanner import _build_manifest_rows_from_canonical, _collect_canonical_rows
from archive_cli.test_slice import SliceConfig, slice_seed_vault
from archive_cli.vault_cache import CACHE_FILENAME, VaultScanCache, _compute_vault_fingerprint
from archive_tests.fixtures import load_fixture_vault
from archive_vault.vault import iter_note_paths, read_note_frontmatter_file


def test_cache_build_tier1(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert cache.note_count() == len(list(iter_note_paths(vault)))
    assert cache.tier() == 1
    assert cache.uid_to_rel_path()
    assert cache.rel_paths_by_type()
    some = next(iter(cache.all_rel_paths()))
    assert "uid" in cache.frontmatter_for_rel_path(some)


def test_cache_build_tier2(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    some = next(iter(cache.all_rel_paths()))
    assert cache.body_for_rel_path(some)
    assert isinstance(cache.wikilinks_for_rel_path(some), list)
    ch = cache.content_hash_for_rel_path(some)
    assert len(ch) == 64
    assert cache.raw_content_sha256_for_rel_path(some)


def test_cache_hit(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    c1 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    n1 = c1.note_count()
    t0 = pytest.importorskip("time").monotonic()
    c2 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert pytest.importorskip("time").monotonic() - t0 < 1.0
    assert c2.note_count() == n1
    assert c2.is_cache_hit


def test_cache_miss_on_file_change(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    some = next(vault.rglob("*.md"))
    some.touch()
    c2 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert not c2.is_cache_hit


def test_cache_miss_on_add(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    src = next(vault.rglob("People/*.md"))
    dest = vault / "Documents" / "extra-note.md"
    shutil.copy2(src, dest)
    c2 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert c2.note_count() == len(list(iter_note_paths(vault)))
    assert not c2.is_cache_hit


def test_cache_miss_on_delete(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    first = next(vault.rglob("*.md"))
    first.unlink()
    c2 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert not c2.is_cache_hit


def test_no_cache_flag(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0, no_cache=True)
    assert cache.note_count() == len(list(iter_note_paths(vault)))
    assert not (vault / "_meta" / CACHE_FILENAME).exists()


def test_tier2_accessor_raises_on_tier1(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    some = next(iter(cache.all_rel_paths()))
    with pytest.raises(ValueError, match="tier 2"):
        cache.body_for_rel_path(some)


def test_frontmatter_roundtrip(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    for rel_path in cache.all_rel_paths():
        fm = cache.frontmatter_for_rel_path(rel_path)
        disk = read_note_frontmatter_file(vault / rel_path, vault_root=vault).frontmatter
        assert fm == disk


def test_file_stats_parity(tmp_path: Path) -> None:
    from archive_cli.scanner import _vault_paths_and_fingerprint

    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    stats_scan, _fp = _vault_paths_and_fingerprint(vault, rel_paths)
    cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert cache.file_stats() == stats_scan


def test_fingerprint_parity(tmp_path: Path) -> None:
    from archive_cli.scanner import _vault_paths_and_fingerprint

    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    _stats, fp_scan = _vault_paths_and_fingerprint(vault, rel_paths)
    cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert cache.vault_fingerprint() == fp_scan
    stats2, fp2 = _compute_vault_fingerprint(vault, rel_paths)
    assert fp2 == fp_scan
    assert stats2 == cache.file_stats()


@pytest.mark.skipif(os.name == "nt", reason="chmod semantics differ on Windows")
def test_read_only_vault_fallback(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    meta = vault / "_meta"
    mode = meta.stat().st_mode
    os.chmod(meta, stat.S_IRUSR | stat.S_IXUSR)
    try:
        cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
        assert cache.note_count() == len(list(iter_note_paths(vault)))
    finally:
        os.chmod(meta, stat.S_IRWXU)
        meta.chmod(mode)


@pytest.mark.integration
def test_slice_seed_with_cache(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    out = tmp_path / "slice_out"
    cfg = SliceConfig(target_percent=5.0, cluster_cap=500)
    r1 = slice_seed_vault(vault, out, cfg, progress_every=0)
    cache_path = vault / "_meta" / CACHE_FILENAME
    assert cache_path.exists()
    out2 = tmp_path / "slice_out2"
    r2 = slice_seed_vault(vault, out2, cfg, progress_every=0)
    assert r1.selected_card_count == r2.selected_card_count
    assert r1.cards_by_type.keys() == r2.cards_by_type.keys()


def test_slice_seed_no_cache(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    out = tmp_path / "slice_out"
    cfg = SliceConfig(target_percent=5.0, cluster_cap=500)
    slice_seed_vault(vault, out, cfg, progress_every=0, no_cache=True)
    assert not (vault / "_meta" / CACHE_FILENAME).exists()


def test_incremental_rebuild_on_change(tmp_path: Path) -> None:
    """After changing one file, incremental rebuild should produce the same result as full."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    c1 = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    n1 = c1.note_count()
    assert n1 > 0

    target = next(vault.rglob("*.md"))
    content = target.read_text()
    target.write_text(content + "\n<!-- touched -->\n")

    c2 = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    assert not c2.is_cache_hit
    assert c2.note_count() == n1


def test_incremental_rebuild_on_add(tmp_path: Path) -> None:
    """Adding a new note should show up via incremental rebuild."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    c1 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    n1 = c1.note_count()

    src = next(vault.rglob("People/*.md"))
    dest = vault / "Documents" / "incremental-add-test.md"
    import shutil
    shutil.copy2(src, dest)

    c2 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert not c2.is_cache_hit
    assert c2.note_count() == n1 + 1
    all_paths = c2.all_rel_paths()
    assert any("incremental-add-test" in p for p in all_paths)


def test_incremental_rebuild_on_delete(tmp_path: Path) -> None:
    """Deleting a note should remove it from cache via incremental rebuild."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    c1 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    n1 = c1.note_count()

    target = next(vault.rglob("*.md"))
    target_rel = target.relative_to(vault).as_posix()
    target.unlink()

    c2 = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert not c2.is_cache_hit
    assert c2.note_count() == n1 - 1
    assert target_rel not in c2.all_rel_paths()


def test_incremental_unchanged_preserves_data(tmp_path: Path) -> None:
    """With no disk changes but a forced fingerprint mismatch, unchanged rows survive."""
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    c1 = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    some = next(iter(c1.all_rel_paths()))
    body1 = c1.body_for_rel_path(some)
    ch1 = c1.content_hash_for_rel_path(some)

    import sqlite3 as _sqlite3
    cache_path = VaultScanCache.cache_path_for_vault(vault)
    conn = _sqlite3.connect(str(cache_path), timeout=10.0)
    conn.execute(
        "INSERT INTO cache_meta (key, value) VALUES ('vault_fingerprint', 'fake') "
        "ON CONFLICT(key) DO UPDATE SET value = 'fake'"
    )
    conn.commit()
    conn.close()

    c2 = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    assert not c2.is_cache_hit
    assert c2.note_count() == c1.note_count()
    assert c2.body_for_rel_path(some) == body1
    assert c2.content_hash_for_rel_path(some) == ch1


@pytest.mark.integration
def test_build_manifest_with_tier2_cache(tmp_path: Path) -> None:
    from archive_cli.index_config import CHUNK_SCHEMA_VERSION, INDEX_SCHEMA_VERSION
    from archive_cli.projections.registry import PROJECTION_REGISTRY_VERSION

    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    rows, _slug, _dup_c, _dup_rows, _fp, file_stats = _collect_canonical_rows(
        vault, workers=1, progress_every=0, cache=cache
    )
    versions = (INDEX_SCHEMA_VERSION, CHUNK_SCHEMA_VERSION, PROJECTION_REGISTRY_VERSION)
    m_cached = _build_manifest_rows_from_canonical(rows, vault, file_stats, versions, cache=cache)
    m_disk = _build_manifest_rows_from_canonical(rows, vault, file_stats, versions, cache=None)
    assert [r.content_hash for r in m_cached] == [r.content_hash for r in m_disk]

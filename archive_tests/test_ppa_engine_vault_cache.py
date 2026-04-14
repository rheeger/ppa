"""``PPA_ENGINE=rust`` integration: Rust-backed vault cache disk build matches Python."""

from __future__ import annotations

import shutil

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

from archive_cli.vault_cache import CACHE_FILENAME, VaultScanCache
from archive_tests.fixtures import load_fixture_vault
from archive_vault.vault import iter_note_paths


def test_build_or_load_ppa_engine_rust_matches_python_disk(tmp_path, monkeypatch):
    """With ``PPA_ENGINE=rust``, on-disk cache matches a pure-Python in-memory build."""

    monkeypatch.setenv("PPA_ENGINE", "rust")
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    n = len(list(iter_note_paths(vault)))

    py_only = VaultScanCache.build_or_load(vault, tier=2, progress_every=0, no_cache=True)
    assert py_only.note_count() == n

    meta = vault / "_meta"
    if meta.is_dir():
        shutil.rmtree(meta)

    rust_disk = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    assert rust_disk.note_count() == n
    assert rust_disk.vault_fingerprint() == py_only.vault_fingerprint()
    assert (vault / "_meta" / CACHE_FILENAME).is_file()

    some = next(iter(rust_disk.all_rel_paths()))
    assert rust_disk.body_for_rel_path(some) == py_only.body_for_rel_path(some)

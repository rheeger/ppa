"""Rust scanner helpers vs ``archive_cli.scanner``."""

from __future__ import annotations

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate
from archive_cli.scanner import _vault_paths_and_fingerprint, cards_by_type_from_cache_path
from archive_cli.vault_cache import VaultScanCache
from archive_tests.fixtures import load_fixture_vault
from archive_vault.vault import iter_note_paths


def test_vault_paths_and_fingerprint_matches_python(tmp_path):
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    stats_py, fp_py = _vault_paths_and_fingerprint(vault, rel_paths)
    stats_r, fp_r = archive_crate.vault_paths_and_fingerprint(str(vault), rel_paths)
    assert fp_r == fp_py
    assert dict(stats_r) == stats_py


def test_cards_by_type_from_cache_path_matches_rel_paths_by_type(tmp_path):
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    cache_path = VaultScanCache.cache_path_for_vault(vault)
    by_py = cache.rel_paths_by_type()
    by_helper = cards_by_type_from_cache_path(cache_path)
    assert by_helper == by_py
    filtered = cards_by_type_from_cache_path(cache_path, ["email_message"])
    assert set(filtered.keys()) <= {"email_message"}
    assert filtered.get("email_message") == by_py.get("email_message")


def test_cards_by_type_alias_matches_cards_by_type_from_cache(tmp_path):
    """Phase 2.9 Step 7 — plan name ``cards_by_type`` vs ``cards_by_type_from_cache``."""
    import archive_crate

    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    cache_path = VaultScanCache.cache_path_for_vault(vault)
    a = dict(archive_crate.cards_by_type_from_cache(str(cache_path)))
    b = dict(archive_crate.cards_by_type(str(cache_path)))
    c = dict(archive_crate.cards_by_type(str(cache_path), ["email_message"]))
    assert a == b
    assert set(c.keys()) <= {"email_message"}

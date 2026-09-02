"""Process-local vault-cache reuse (does not change on-disk cache version)."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.vault_cache import VaultScanCache
from archive_cli.vault_cache_runtime import (
    clear_process_cache,
    install_process_reuse,
    mark_vault_written,
    process_reuse_installed,
    uninstall_process_reuse,
)
from archive_tests.fixtures import load_fixture_vault


@pytest.fixture(autouse=True)
def _reset_runtime() -> None:
    uninstall_process_reuse()
    clear_process_cache()
    yield
    uninstall_process_reuse()
    clear_process_cache()


def test_process_reuse_skips_second_fingerprint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    install_process_reuse()
    assert process_reuse_installed()
    first = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    walks = {"n": 0}

    def boom(*_a, **_k):
        walks["n"] += 1
        raise AssertionError("fingerprint walk must not run on process-hit")

    monkeypatch.setattr("archive_cli.vault_cache._compute_fingerprint_with_paths", boom)
    second = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert walks["n"] == 0
    assert second is first


def test_mark_written_forces_refresh(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    install_process_reuse()
    first = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    mark_vault_written(vault)
    second = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert second is not first
    assert second.note_count() == first.note_count()

"""Process-local vault-cache reuse (does not change on-disk cache version)."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.vault_cache import VaultScanCache
from archive_cli.vault_cache_runtime import (
    begin_defer_vault_written,
    clear_process_cache,
    end_defer_vault_written,
    flush_deferred_vault_written,
    install_process_reuse,
    mark_vault_written,
    peek_process_cache,
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
    assert peek_process_cache(vault) is first
    uninstall_process_reuse()
    assert peek_process_cache(vault) is None


def test_mark_written_forces_refresh(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    install_process_reuse()
    first = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    mark_vault_written(vault)
    second = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert second is not first
    assert second.note_count() == first.note_count()


def test_deferred_mark_vault_written_emits_uids_on_flush(tmp_path: Path, monkeypatch) -> None:
    from archive_cli.errors import ServingIndexUnavailableError
    from archive_cli.serving_index import read_dirty_uids

    root = tmp_path / "rust-search-index"
    root.mkdir()
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    monkeypatch.setattr(
        "archive_cli.serving_index._crate",
        lambda: (_ for _ in ()).throw(ServingIndexUnavailableError("serving_index_unavailable")),
    )
    begin_defer_vault_written()
    mark_vault_written(tmp_path, uids=["uid-deferred-a", "uid-deferred-b"])
    assert read_dirty_uids(tmp_path) == []
    end_defer_vault_written(flush=False)
    assert flush_deferred_vault_written() == 1
    assert read_dirty_uids(tmp_path) == ["uid-deferred-a", "uid-deferred-b"]


def test_known_writes_refresh_skips_fingerprint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_sync.adapters.base import deterministic_provenance
    from archive_vault.schema import PersonCard
    from archive_vault.vault import write_card

    vault = tmp_path / "vault"
    (vault / "People").mkdir(parents=True)
    card = PersonCard(
        uid="hfa-person-abc123def456",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Jane Example",
        emails=["jane@example.com"],
    )
    write_card(
        vault,
        "People/jane-example.md",
        card,
        provenance=deterministic_provenance(card, "contacts.apple"),
    )
    install_process_reuse()
    first = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    mark_vault_written(vault, uids=[card.uid], rel_paths=["People/jane-example.md"])

    def boom(*_a, **_k):
        raise AssertionError("fingerprint walk must not run for known writes")

    monkeypatch.setattr("archive_cli.vault_cache._compute_fingerprint_with_paths", boom)
    second = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert second is first
    assert second.rel_path_for_uid(card.uid) == "People/jane-example.md"


def test_deferred_mark_vault_written_batches_invalidation(tmp_path: Path) -> None:
    vault = load_fixture_vault(tmp_path / "vault", include_graphs=True)
    install_process_reuse()
    first = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    begin_defer_vault_written()
    mark_vault_written(vault)
    mark_vault_written(vault)
    second = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert second is first
    end_defer_vault_written(flush=False)
    assert flush_deferred_vault_written() == 1
    third = VaultScanCache.build_or_load(vault, tier=1, progress_every=0)
    assert third is not first

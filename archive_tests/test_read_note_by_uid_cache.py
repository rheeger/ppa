"""read_note_by_uid must never dump the vault when a scan cache exists."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.vault_cache import VaultScanCache
from archive_cli.vault_cache_runtime import (
    clear_process_cache,
    install_process_reuse,
    peek_process_cache,
    uninstall_process_reuse,
)
from archive_sync.adapters.base import deterministic_provenance
from archive_vault.schema import PersonCard
from archive_vault.vault import read_note_by_uid, write_card


@pytest.fixture(autouse=True)
def _reset_runtime() -> None:
    uninstall_process_reuse()
    clear_process_cache()
    yield
    uninstall_process_reuse()
    clear_process_cache()


def _person_vault(tmp_path: Path) -> tuple[Path, PersonCard]:
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
        body="hello",
        provenance=deterministic_provenance(card, "contacts.apple"),
    )
    return vault, card


def test_read_note_by_uid_uses_warm_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault, card = _person_vault(tmp_path)
    install_process_reuse()
    VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    assert peek_process_cache(vault) is not None

    def boom(*_a, **_k):
        raise AssertionError("iter_parsed_notes must not run when the warm cache has the uid")

    monkeypatch.setattr("archive_vault.vault.iter_parsed_notes", boom)
    match = read_note_by_uid(vault, card.uid)
    assert match is not None
    assert str(match[0]) == "People/jane-example.md"
    assert match[1]["type"] == "person"
    assert match[2] == "hello"


def test_read_note_by_uid_does_not_scan_when_sidecar_cache_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    vault, card = _person_vault(tmp_path)
    cache_path = vault / "_meta" / "vault-scan-cache.sqlite3"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"not a sqlite database")

    def boom(*_a, **_k):
        raise AssertionError("cache file present: must not scan the vault")

    monkeypatch.setattr("archive_vault.vault.iter_parsed_notes", boom)
    assert read_note_by_uid(vault, card.uid) is None


def test_iter_parsed_notes_does_not_walk_when_cache_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    vault, _card = _person_vault(tmp_path)
    cache_path = vault / "_meta" / "vault-scan-cache.sqlite3"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(b"not a sqlite database")

    def boom(*_a, **_k):
        raise AssertionError("cache file present: must not walk the vault")

    monkeypatch.setenv("PPA_ENGINE", "rust")
    monkeypatch.setattr("archive_vault.vault._iter_parsed_notes_python_walk", boom)
    from archive_vault.vault import iter_parsed_notes

    assert list(iter_parsed_notes(vault)) == []


def test_entity_resolution_uses_warm_cache_not_walk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    vault, card = _person_vault(tmp_path)
    install_process_reuse()
    VaultScanCache.build_or_load(vault, tier=2, progress_every=0)

    def boom(*_a, **_k):
        raise AssertionError("warm cache present: must not walk or open crate sidecar")

    monkeypatch.setattr("archive_sync.extractors.entity_resolution.iter_parsed_notes", boom)
    monkeypatch.setattr("archive_crate.frontmatter_dicts_from_cache", boom, raising=False)
    from archive_sync.extractors.entity_resolution import iter_derived_card_dicts

    rows = iter_derived_card_dicts(str(vault), card_types=frozenset({"person"}))
    assert any(row.get("uid") == card.uid for row in rows)


def test_load_email_stubs_uses_warm_cache_not_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    vault, _card = _person_vault(tmp_path)
    install_process_reuse()
    VaultScanCache.build_or_load(vault, tier=2, progress_every=0)

    def boom(*_a, **_k):
        raise AssertionError("warm cache present: must not open sidecar sqlite")

    monkeypatch.setattr(
        "archive_sync.llm_enrichment.threads.email_message_stubs_from_sqlite", boom
    )
    from archive_sync.llm_enrichment.threads import load_email_stubs_for_vault

    assert load_email_stubs_for_vault(vault) == []


def test_read_note_by_uid_walks_only_when_no_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    vault, card = _person_vault(tmp_path)
    walked = {"n": 0}
    orig = read_note_by_uid.__globals__["iter_parsed_notes"]

    def counted(vault_arg):
        walked["n"] += 1
        return orig(vault_arg)

    monkeypatch.setattr("archive_vault.vault.iter_parsed_notes", counted)
    match = read_note_by_uid(vault, card.uid)
    assert match is not None
    assert walked["n"] == 1

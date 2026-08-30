"""Going-forward dirty-UID contract: persist host UID, never phantom person UIDs."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from archive_sync.adapters.base import BaseAdapter, FetchedBatch, deterministic_provenance
from archive_sync.source_updaters.runner import run_source_updater
from archive_vault.identity import upsert_identity_map
from archive_vault.identity_resolver import ResolveResult
from archive_vault.schema import PersonCard
from archive_vault.vault import read_note, write_card


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Calendar", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "nicknames.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "dedup-candidates.json").write_text("[]", encoding="utf-8")
    return vault


class _FixturePersonAdapter(BaseAdapter):
    source_id = "contacts"
    enable_person_resolution = True
    preload_existing_uid_index = False

    def __init__(self, items: list[dict[str, Any]] | None = None) -> None:
        self._items = list(items or [])

    def fetch(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> list[dict[str, Any]]:
        return list(self._items)

    def fetch_batches(self, vault_path: str, cursor: dict[str, Any], config=None, **kwargs) -> Iterable[FetchedBatch]:
        yield FetchedBatch(items=list(self._items), sequence=0)

    def to_card(self, item: dict[str, Any]):
        card = PersonCard(
            uid=str(item["uid"]),
            type="person",
            source=[str(item.get("source", "contacts.google"))],
            source_id=str(item.get("source_id", item["uid"])),
            created="2026-03-06",
            updated="2026-03-06",
            summary=str(item.get("summary", "Person")),
            emails=list(item.get("emails", [])),
            phones=list(item.get("phones", [])),
            company=str(item.get("company", "")),
            title=str(item.get("title", "")),
        )
        return card, deterministic_provenance(card, card.source[0]), str(item.get("body", ""))


def _run_contacts(vault: Path, adapter: _FixturePersonAdapter, tmp_path: Path):
    return run_source_updater(
        source_key="contacts:google",
        vault_path=vault,
        apply=True,
        adapter=adapter,
        repo_root=tmp_path,
        archive_instance="fixture:dirty-uid",
    )


def test_person_create_dirties_created_uid(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    created_uid = "hfa-person-created0001"
    adapter = _FixturePersonAdapter(
        [
            {
                "uid": created_uid,
                "source": "contacts.google",
                "source_id": "created@example.com",
                "summary": "Created Person",
                "emails": ["created@example.com"],
            }
        ]
    )
    result = _run_contacts(vault, adapter, tmp_path)
    dirty = result.report.batch.dirty_card_uids
    assert result.report.batch.promoted == 1
    assert created_uid in dirty
    frontmatter, _, _ = read_note(vault, "People/created-person.md")
    assert frontmatter["uid"] == created_uid


def test_person_merge_dirties_host_uid_not_incoming(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    host_uid = "hfa-person-host0000001"
    incoming_uid = "hfa-person-incoming001"
    host = PersonCard(
        uid=host_uid,
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        emails=["jane@example.com"],
    )
    write_card(vault, "People/jane-smith.md", host, provenance=deterministic_provenance(host, "contacts.apple"))
    upsert_identity_map(vault, "[[jane-smith]]", {"emails": ["jane@example.com"], "name": "Jane Smith"})
    adapter = _FixturePersonAdapter(
        [
            {
                "uid": incoming_uid,
                "source": "contacts.google",
                "source_id": "jane@example.com",
                "summary": "Jane Smith",
                "emails": ["jane@example.com", "j.smith@corp.com"],
            }
        ]
    )
    result = _run_contacts(vault, adapter, tmp_path)
    dirty = result.report.batch.dirty_card_uids
    assert result.report.batch.updated == 1
    assert host_uid in dirty
    assert incoming_uid not in dirty
    frontmatter, _, _ = read_note(vault, "People/jane-smith.md")
    assert frontmatter["uid"] == host_uid
    assert "j.smith@corp.com" in frontmatter["emails"]


def test_person_conflict_does_not_dirty_incoming_uid(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = _minimal_vault(tmp_path)
    host_uid = "hfa-person-hostconflict"
    incoming_uid = "hfa-person-phantom0001"
    host = PersonCard(
        uid=host_uid,
        type="person",
        source=["contacts.apple"],
        source_id="host@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Host Person",
        emails=["host@example.com"],
    )
    write_card(vault, "People/host-person.md", host, provenance=deterministic_provenance(host, "contacts.apple"))

    def fake_resolve(*args: Any, **kwargs: Any) -> ResolveResult:
        return ResolveResult("conflict", "[[host-person]]", 80, ["fuzzy_name"])

    monkeypatch.setattr("archive_sync.adapters.base.resolve_person", fake_resolve)
    adapter = _FixturePersonAdapter(
        [
            {
                "uid": incoming_uid,
                "source": "contacts.google",
                "source_id": "incoming@example.com",
                "summary": "Incoming Person",
                "emails": ["incoming@example.com"],
            }
        ]
    )
    result = _run_contacts(vault, adapter, tmp_path)
    dirty = result.report.batch.dirty_card_uids
    assert incoming_uid not in dirty
    assert host_uid not in dirty
    people_cards = list((vault / "People").glob("*.md"))
    assert len(people_cards) == 1
    frontmatter, _, _ = read_note(vault, "People/host-person.md")
    assert frontmatter["uid"] == host_uid
    candidates = (vault / "_meta" / "dedup-candidates.json").read_text(encoding="utf-8")
    assert incoming_uid in candidates
    assert "[[host-person]]" in candidates

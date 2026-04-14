"""Test bootstrap and fixtures for the HFA shared library."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard


@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance").mkdir()
    (vault / "Photos").mkdir()
    (vault / "Attachments").mkdir()
    (vault / "_templates").mkdir()
    (vault / ".obsidian").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    (meta / "identity-map.json").write_text("{}", encoding="utf-8")
    (meta / "sync-state.json").write_text("{}", encoding="utf-8")
    (meta / "dedup-candidates.json").write_text("[]", encoding="utf-8")
    (meta / "enrichment-log.json").write_text("[]", encoding="utf-8")
    (meta / "llm-cache.json").write_text("{}", encoding="utf-8")
    (meta / "nicknames.json").write_text(
        json.dumps({"robert": ["rob", "robbie"], "jennifer": ["jen", "jenny"]}),
        encoding="utf-8",
    )
    return vault


@pytest.fixture
def sample_person_card() -> PersonCard:
    return PersonCard(
        uid="hfa-person-abc123def456",
        type="person",
        source=["contacts.apple"],
        source_id="jane@example.com",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Jane Smith",
        first_name="Jane",
        last_name="Smith",
        emails=["jane@example.com"],
        phones=["+15550123"],
        company="Endaoment",
        title="VP Partnerships",
        linkedin="janesmith",
        tags=["endaoment"],
    )


@pytest.fixture
def sample_person_provenance() -> dict[str, ProvenanceEntry]:
    return {
        "summary": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "first_name": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "last_name": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "emails": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "phones": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "company": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "title": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "linkedin": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "linkedin_url": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
        "tags": ProvenanceEntry("contacts.apple", "2026-03-06", "deterministic"),
    }


class MockLLMProvider:
    name = "mock"

    def __init__(self, model: str = "mock-v1", response: str = "UNSURE"):
        self.model = model
        self.response = response
        self.calls: list[tuple[str, int]] = []

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        self.calls.append((prompt, max_tokens))
        return self.response

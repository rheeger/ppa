"""Shared ingest pipeline tests."""

import json
from datetime import date

from archive_sync.adapters.base import BaseAdapter, FetchedBatch, deterministic_provenance
from hfa.provenance import read_provenance
from hfa.schema import PersonCard
from hfa.vault import read_note, write_card


class MockAdapter(BaseAdapter):
    source_id = "mock-adapter"

    def __init__(self, items):
        self.items = items

    def fetch(self, vault_path, cursor, config=None, **kwargs):
        return list(self.items)

    def to_card(self, item):
        card = PersonCard(
            uid=item["uid"],
            type="person",
            source=[item["source"]],
            source_id=item["source_id"],
            created="2026-03-06",
            updated="2026-03-06",
            summary=item["summary"],
            emails=item.get("emails", []),
            phones=item.get("phones", []),
            company=item.get("company", ""),
            title=item.get("title", ""),
            tags=item.get("tags", []),
        )
        return card, deterministic_provenance(card, item["source"]), item.get("body", "")


class CheckpointAdapter(MockAdapter):
    def to_card(self, item):
        if item.get("explode"):
            raise ValueError("boom")
        return super().to_card(item)


class BatchCheckpointAdapter(MockAdapter):
    def fetch(self, vault_path, cursor, config=None, **kwargs):
        return []

    def fetch_batches(self, vault_path, cursor, config=None, **kwargs):
        yield FetchedBatch(
            items=[
                {
                    "uid": "hfa-person-555555555555",
                    "source": "contacts.apple",
                    "source_id": "batch-one@example.com",
                    "summary": "Batch One",
                    "emails": ["batch-one@example.com"],
                }
            ],
            cursor_patch={"batch_marker": "one"},
            sequence=0,
        )
        yield FetchedBatch(
            items=[
                {
                    "explode": True,
                }
            ],
            cursor_patch={"batch_marker": "two"},
            sequence=1,
        )

    def to_card(self, item):
        if item.get("explode"):
            raise ValueError("boom")
        return super().to_card(item)


class ParallelMockAdapter(MockAdapter):
    parallel_person_matching = True
    parallel_person_match_default_workers = 4
    parallel_person_match_default_chunk_size = 32


def test_ingest_creates_cards(tmp_vault):
    adapter = MockAdapter(
        [
            {
                "uid": "hfa-person-111111111111",
                "source": "contacts.apple",
                "source_id": "jane@example.com",
                "summary": "Jane Smith",
                "emails": ["jane@example.com"],
            }
        ]
    )
    result = adapter.ingest(str(tmp_vault))
    assert result.created == 1
    assert (tmp_vault / "People" / "jane-smith.md").exists()


def test_ingest_merges_duplicates(tmp_vault):
    adapter = MockAdapter(
        [
            {
                "uid": "hfa-person-111111111111",
                "source": "contacts.apple",
                "source_id": "jane@example.com",
                "summary": "Jane Smith",
                "emails": ["jane@example.com"],
            },
            {
                "uid": "hfa-person-222222222222",
                "source": "linkedin",
                "source_id": "jane@example.com",
                "summary": "Jane Smith",
                "emails": ["jane@example.com", "j.smith@corp.com"],
                "tags": ["linkedin"],
            },
        ]
    )
    result = adapter.ingest(str(tmp_vault))
    frontmatter, _, _ = read_note(tmp_vault, "People/jane-smith.md")
    assert result.created == 1
    assert result.merged == 1
    assert frontmatter["source"] == ["contacts.apple", "linkedin"]
    assert frontmatter["emails"] == ["jane@example.com", "j.smith@corp.com"]


def test_parallel_ingest_merges_same_batch_duplicates(tmp_vault):
    adapter = ParallelMockAdapter(
        [
            {
                "uid": "hfa-person-111111111111",
                "source": "contacts.apple",
                "source_id": "jane@example.com",
                "summary": "Jane Smith",
                "emails": ["jane@example.com"],
            },
            {
                "uid": "hfa-person-222222222222",
                "source": "linkedin",
                "source_id": "jane@example.com",
                "summary": "Jane Smith",
                "emails": ["jane@example.com", "j.smith@corp.com"],
                "tags": ["linkedin"],
            },
        ]
    )
    result = adapter.ingest(str(tmp_vault), workers=4, chunk_size=32)
    frontmatter, _, _ = read_note(tmp_vault, "People/jane-smith.md")
    assert result.created == 1
    assert result.merged == 1
    assert frontmatter["source"] == ["contacts.apple", "linkedin"]
    assert frontmatter["emails"] == ["jane@example.com", "j.smith@corp.com"]


def test_ingest_logs_conflicts(tmp_vault):
    existing = PersonCard(
        uid="hfa-person-existing0001",
        type="person",
        source=["contacts.apple"],
        source_id="robbie@endaoment.org",
        created="2026-03-06",
        updated="2026-03-06",
        summary="Robbie Heeger",
        emails=["robbie@endaoment.org"],
        company="Endaoment",
    )
    write_card(tmp_vault, "People/robbie-heeger.md", existing, provenance=deterministic_provenance(existing, "contacts.apple"))

    adapter = MockAdapter(
        [
            {
                "uid": "hfa-person-conflict0001",
                "source": "notion",
                "source_id": "robert@endaoment.org",
                "summary": "Robert Heeger",
                "emails": ["robert@endaoment.org"],
                "company": "Endaoment",
            }
        ]
    )
    result = adapter.ingest(str(tmp_vault))
    assert result.conflicted == 1
    assert "confidence" in (tmp_vault / "_meta" / "dedup-candidates.json").read_text(encoding="utf-8")


def test_ingest_dry_run_writes_nothing(tmp_vault):
    adapter = MockAdapter(
        [
            {
                "uid": "hfa-person-333333333333",
                "source": "contacts.apple",
                "source_id": "dry@example.com",
                "summary": "Dry Run",
                "emails": ["dry@example.com"],
            }
        ]
    )
    result = adapter.ingest(str(tmp_vault), dry_run=True)
    assert result.created == 1
    assert list((tmp_vault / "People").glob("*.md")) == []
    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert state == {}


def test_ingest_persists_cursor_checkpoint_after_successful_item(tmp_vault):
    adapter = CheckpointAdapter(
        [
            {
                "uid": "hfa-person-444444444444",
                "source": "contacts.apple",
                "source_id": "checkpoint@example.com",
                "summary": "Checkpoint Person",
                "emails": ["checkpoint@example.com"],
                "_cursor": {"last_completed_source_id": "checkpoint@example.com"},
            },
            {
                "explode": True,
            },
        ]
    )
    result = adapter.ingest(str(tmp_vault))
    assert result.created == 1
    assert len(result.errors) == 1
    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert state["mock-adapter"]["last_completed_source_id"] == "checkpoint@example.com"
    assert state["mock-adapter"]["processed"] == 1


def test_ingest_persists_batch_cursor_only_for_successful_batch(tmp_vault):
    adapter = BatchCheckpointAdapter([])
    result = adapter.ingest(str(tmp_vault))
    assert result.created == 1
    assert len(result.errors) == 1

    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert state["mock-adapter"]["batch_marker"] == "one"
    assert state["mock-adapter"]["processed"] == 1

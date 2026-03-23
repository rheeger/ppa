"""Seed people adapter tests."""

from __future__ import annotations

import json

from archive_sync.adapters.seed_people import SeedPeopleAdapter
from hfa.vault import read_note


def test_seed_people_normalizes_legacy_frontmatter(tmp_path):
    source_dir = tmp_path / "People"
    source_dir.mkdir()
    note = source_dir / "robbie-heeger.md"
    note.write_text(
        "\n".join(
            [
                "---",
                'uid: "hfa-person-robbie-heeger"',
                'source: "vcf"',
                'source_id: "rheeger@gmail.com"',
                'created: "2026-03-02"',
                'updated: "2026-03-02"',
                'summary: "Robbie Heeger"',
                'email: "rheeger@gmail.com"',
                'phone: "+16507992364"',
                "family: true",
                'relationship: "self"',
                'favorite_color: "blue"',
                "---",
                "",
                "## Notes",
                "",
                "- hello",
            ]
        ),
        encoding="utf-8",
    )

    item = SeedPeopleAdapter().fetch("", {}, source_dir=str(source_dir))[0]
    card, provenance, body = SeedPeopleAdapter().to_card(item)

    assert card.source == ["contacts.apple"]
    assert card.emails == ["rheeger@gmail.com"]
    assert card.phones == ["+16507992364"]
    assert "family" in card.tags
    assert card.relationship_type == "self"
    assert provenance["emails"].source == "contacts.apple"
    assert "## Legacy Metadata" in body
    assert "favorite_color" in body


def test_seed_people_ingest_populates_identity_map_and_sync_state(tmp_vault, tmp_path):
    source_dir = tmp_path / "People"
    source_dir.mkdir()
    (source_dir / "jenny-souza.md").write_text(
        "\n".join(
            [
                "---",
                'source: "vcf"',
                'source_id: "jenny@example.com"',
                'summary: "Jenny Souza"',
                'email: "jenny@example.com"',
                'phone: "456"',
                'company: "Endaoment"',
                'title: "Ops"',
                "---",
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = SeedPeopleAdapter().ingest(str(tmp_vault), source_dir=str(source_dir))

    assert result.created == 1
    frontmatter, _, _ = read_note(tmp_vault, "People/jenny-souza.md")
    assert frontmatter["source"] == ["contacts.apple"]
    payload = json.loads((tmp_vault / "_meta" / "identity-map.json").read_text(encoding="utf-8"))
    assert payload["email:jenny@example.com"] == "[[jenny-souza]]"
    state = json.loads((tmp_vault / "_meta" / "sync-state.json").read_text(encoding="utf-8"))
    assert state["seed-people"]["processed"] == 1

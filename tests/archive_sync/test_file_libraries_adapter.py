"""Archive-sync file library adapter tests."""

from __future__ import annotations

import json
from pathlib import Path

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.file_libraries import FileLibrariesAdapter
from hfa.schema import PersonCard
from hfa.vault import read_note, write_card


def _seed_person(tmp_vault: Path) -> None:
    person = PersonCard(
        uid="hfa-person-abc123def456",
        type="person",
        source=["contacts.apple"],
        source_id="alice@example.com",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Alice Example",
        emails=["alice@example.com"],
    )
    write_card(
        tmp_vault,
        "People/alice-example.md",
        person,
        provenance=deterministic_provenance(person, "contacts.apple"),
    )
    (tmp_vault / "_meta" / "identity-map.json").write_text(
        json.dumps(
            {
                "_comment": "Alias -> canonical person wikilink",
                "name:alice example": "[[alice-example]]",
                "email:alice@example.com": "[[alice-example]]",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_ingest_creates_document_card_and_resolves_people(tmp_vault: Path, tmp_path: Path):
    _seed_person(tmp_vault)
    docs_root = tmp_path / "docs"
    target = docs_root / "Work" / "Endaoment" / "Board"
    target.mkdir(parents=True)
    doc_path = target / "meeting-notes.txt"
    doc_path.write_text("Board notes\nAlice Example\nalice@example.com\nEndaoment budget review", encoding="utf-8")

    adapter = FileLibrariesAdapter()
    result = adapter.ingest(str(tmp_vault), roots=[str(docs_root)], quick_update=True)

    assert result.created == 1
    rel_path = next((tmp_vault / "Documents").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, body, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["type"] == "document"
    assert frontmatter["people"] == ["[[alice-example]]"]
    assert "Endaoment" in frontmatter["orgs"]
    assert "work" in frontmatter["tags"]
    assert "board" in frontmatter["tags"]
    assert frontmatter["text_source"] == "plain"
    assert frontmatter["extension"] == "txt"
    assert frontmatter["content_sha"]
    assert frontmatter["metadata_sha"]
    assert "Resolved people: [[alice-example]]" in body
    assert "Extracted text:" in body


def test_stage_documents_writes_manifest_without_touching_vault(tmp_vault: Path, tmp_path: Path):
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    (docs_root / "endaoment-overview.md").write_text("# Endaoment Overview\n\nCharitable infrastructure", encoding="utf-8")
    stage_dir = tmp_path / "stage"

    adapter = FileLibrariesAdapter()
    manifest = adapter.stage_documents(str(tmp_vault), stage_dir, roots=[str(docs_root)], verbose=False)

    assert manifest["emitted_documents"] == 1
    assert (stage_dir / "manifest.json").exists()
    assert any(path.suffix == ".jsonl" for path in stage_dir.iterdir())
    assert not any((tmp_vault / "Documents").rglob("*.md")) if (tmp_vault / "Documents").exists() else True


def test_stage_documents_links_known_person_mentions_from_body(tmp_vault: Path, tmp_path: Path):
    _seed_person(tmp_vault)
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    doc_path = docs_root / "memo.txt"
    doc_path.write_text("We met with Alice Example yesterday to review the Endaoment budget.", encoding="utf-8")
    stage_dir = tmp_path / "stage"

    adapter = FileLibrariesAdapter()
    manifest = adapter.stage_documents(str(tmp_vault), stage_dir, roots=[str(docs_root)], verbose=False)

    stage_file = Path(next(iter(manifest["stage_files"].values())))
    payload = json.loads(stage_file.read_text(encoding="utf-8").splitlines()[0])
    assert payload["people"] == ["[[alice-example]]"]


def test_markdown_heading_titles_are_cleaned_and_orgs_can_come_from_content(tmp_vault: Path, tmp_path: Path):
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    doc_path = docs_root / "solawave-timeline.md"
    doc_path.write_text(
        "# Timeline of Events: Robert Heeger Trust & UVVU Inc. (Solawave)\n\n"
        "This document outlines the relationship with UVVU Inc. (now Solawave Inc.).",
        encoding="utf-8",
    )

    adapter = FileLibrariesAdapter()
    result = adapter.ingest(str(tmp_vault), roots=[str(docs_root)], quick_update=True)

    assert result.created == 1
    rel_path = next((tmp_vault / "Documents").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, _, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["summary"] == "Timeline of Events: Robert Heeger Trust & UVVU Inc. (Solawave)"
    assert frontmatter["title"] == "Timeline of Events: Robert Heeger Trust & UVVU Inc. (Solawave)"
    assert "UVVU" in frontmatter["orgs"]
    assert "Solawave" in frontmatter["orgs"]


def test_quick_update_skips_unchanged_documents(tmp_vault: Path, tmp_path: Path):
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    doc_path = docs_root / "endaoment-overview.md"
    doc_path.write_text("# Endaoment Overview\n\nCharitable infrastructure", encoding="utf-8")
    adapter = FileLibrariesAdapter()

    first = adapter.ingest(str(tmp_vault), roots=[str(docs_root)], quick_update=True)
    second = adapter.ingest(str(tmp_vault), roots=[str(docs_root)], quick_update=True)

    assert first.created == 1
    assert second.created == 0
    assert second.merged == 0
    assert second.skipped == 1
    assert second.skip_details["skipped_unchanged_documents"] == 1


def test_import_from_stage_writes_document_cards(tmp_vault: Path, tmp_path: Path):
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    (docs_root / "endaoment-overview.md").write_text("# Endaoment Overview\n\nCharitable infrastructure", encoding="utf-8")
    stage_dir = tmp_path / "stage"
    adapter = FileLibrariesAdapter()

    adapter.stage_documents(str(tmp_vault), stage_dir, roots=[str(docs_root)], verbose=False)
    result = adapter.ingest(str(tmp_vault), stage_dir=str(stage_dir))

    assert result.created == 1
    rel_path = next((tmp_vault / "Documents").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, _, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["summary"] == "Endaoment Overview"


def test_ingest_eml_extracts_subject_and_participants(tmp_vault: Path, tmp_path: Path):
    _seed_person(tmp_vault)
    docs_root = tmp_path / "mailbox"
    docs_root.mkdir()
    eml_path = docs_root / "invite.eml"
    eml_path.write_text(
        "\n".join(
            [
                "From: Robbie Heeger <rheeger@gmail.com>",
                "To: Alice Example <alice@example.com>",
                "Subject: Endaoment dinner invite",
                "Date: Tue, 11 Mar 2026 18:00:00 +0000",
                "MIME-Version: 1.0",
                "Content-Type: text/plain; charset=utf-8",
                "",
                "Alice - dinner tomorrow at Endaoment.",
            ]
        ),
        encoding="utf-8",
    )

    adapter = FileLibrariesAdapter()
    result = adapter.ingest(str(tmp_vault), roots=[str(docs_root)], quick_update=True)

    assert result.created == 1
    rel_path = next((tmp_vault / "Documents").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, body, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["document_type"] == "email_export"
    assert frontmatter["summary"] == "Endaoment dinner invite"
    assert frontmatter["authors"] == ["Robbie Heeger"]
    assert frontmatter["counterparties"] == ["Alice Example"]
    assert frontmatter["people"] == ["[[alice-example]]"]
    assert frontmatter["document_date"] == "2026-03-11"
    assert frontmatter["emails"] == ["rheeger@gmail.com", "alice@example.com"]
    assert frontmatter["extraction_status"] == "content_extracted"
    assert "Authors: Robbie Heeger" in body


def test_ingest_ics_extracts_date_range_and_location(tmp_vault: Path, tmp_path: Path):
    docs_root = tmp_path / "calendar"
    docs_root.mkdir()
    ics_path = docs_root / "appointment.ics"
    ics_path.write_text(
        "\n".join(
            [
                "BEGIN:VCALENDAR",
                "BEGIN:VEVENT",
                "SUMMARY:CVS vaccine appointment",
                "DTSTART:20231207T160000",
                "DTEND:20231207T163000",
                "LOCATION:CVS Pharmacy, 218 Myrtle Ave, Brooklyn, NY",
                "DESCRIPTION:Manage your appointment https://www.cvs.com/vaccine",
                "END:VEVENT",
                "END:VCALENDAR",
            ]
        ),
        encoding="utf-8",
    )

    adapter = FileLibrariesAdapter()
    result = adapter.ingest(str(tmp_vault), roots=[str(docs_root)], quick_update=True)

    assert result.created == 1
    rel_path = next((tmp_vault / "Documents").rglob("*.md")).relative_to(tmp_vault)
    frontmatter, body, _ = read_note(tmp_vault, str(rel_path))
    assert frontmatter["document_type"] == "calendar_invite"
    assert frontmatter["document_date"] == "2023-12-07"
    assert frontmatter["date_start"].startswith("2023-12-07T16:00:00")
    assert frontmatter["date_end"].startswith("2023-12-07T16:30:00")
    assert frontmatter["location"] == "CVS Pharmacy, 218 Myrtle Ave, Brooklyn, NY"
    assert frontmatter["websites"] == ["https://www.cvs.com/vaccine"]
    assert frontmatter["orgs"] == ["CVS Pharmacy"]
    assert "Location: CVS Pharmacy, 218 Myrtle Ave, Brooklyn, NY" in body

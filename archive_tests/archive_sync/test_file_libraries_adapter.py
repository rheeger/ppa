"""Archive-sync file library adapter tests."""

from __future__ import annotations

import json
from pathlib import Path

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.adapters.file_libraries import FileLibrariesAdapter
from archive_vault.schema import DocumentCard, PersonCard
from archive_vault.vault import read_note, write_card


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
    (docs_root / "endaoment-overview.md").write_text(
        "# Endaoment Overview\n\nCharitable infrastructure", encoding="utf-8"
    )
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


def test_load_existing_hashes_uses_vault_scan_cache_not_rglob(tmp_vault: Path, monkeypatch):
    card = DocumentCard(
        uid="hfa-document-abc123def456",
        type="document",
        source=["file.library"],
        source_id="custom:endaoment-overview.md",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Endaoment Overview",
        metadata_sha="abc123metadata",
    )
    write_card(
        tmp_vault,
        "Documents/endaoment-overview.md",
        card,
        provenance=deterministic_provenance(card, "file.library"),
    )
    skipped = DocumentCard(
        uid="hfa-document-skipped00001",
        type="document",
        source=["file.library"],
        source_id="custom:outside.md",
        created="2026-03-10",
        updated="2026-03-10",
        summary="Outside Documents",
        metadata_sha="outsidehash",
    )
    write_card(
        tmp_vault,
        "Finance/outside-document.md",
        skipped,
        provenance=deterministic_provenance(skipped, "file.library"),
    )

    orig_rglob = Path.rglob

    def _rglob(self, pattern, *args, **kwargs):
        if str(pattern) == "*.md":
            raise AssertionError(f"vault markdown rglob is forbidden: {self} pattern={pattern!r}")
        return orig_rglob(self, pattern, *args, **kwargs)

    monkeypatch.setattr(Path, "rglob", _rglob)

    hashes = FileLibrariesAdapter()._load_existing_hashes(str(tmp_vault))
    assert hashes == {"custom:endaoment-overview.md": "abc123metadata"}
    assert "custom:outside.md" not in hashes


def test_import_from_stage_writes_document_cards(tmp_vault: Path, tmp_path: Path):
    docs_root = tmp_path / "docs"
    docs_root.mkdir()
    (docs_root / "endaoment-overview.md").write_text(
        "# Endaoment Overview\n\nCharitable infrastructure", encoding="utf-8"
    )
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


def _minimal_text_pdf(text: str) -> bytes:
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>",
    ]
    stream = f"BT /F1 24 Tf 72 720 Td ({text}) Tj ET".encode("latin-1")
    objects.append(b"<< /Length %d >>\nstream\n" % len(stream) + stream + b"\nendstream")
    objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    out = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for index, body in enumerate(objects, 1):
        offsets.append(len(out))
        out.extend(f"{index} 0 obj\n".encode())
        out.extend(body)
        out.extend(b"\nendobj\n")
    xref = len(out)
    out.extend(f"xref\n0 {len(objects) + 1}\n".encode())
    out.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        out.extend(f"{offset:010d} 00000 n \n".encode())
    out.extend(f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
    return bytes(out)


def test_extract_payload_pdf_uses_anydoc(tmp_path: Path):
    from archive_sync.adapters.file_libraries import _extract_payload

    pdf_path = tmp_path / "hello.pdf"
    pdf_path.write_bytes(_minimal_text_pdf("Hello anydoc PDF"))
    payload = _extract_payload(pdf_path)
    assert payload["text_source"] == "anydoc"
    assert "Hello anydoc PDF" in payload["text"]
    assert "<" not in payload["text"]


def test_extract_payload_html_converts_to_markdown(tmp_path: Path):
    from archive_sync.adapters.file_libraries import _extract_payload

    html_path = tmp_path / "epic-note.htm"
    html_path.write_text(
        "<html><head><style>body{color:red}</style></head>"
        "<body><h1>Office Visit</h1><p>Patient seen 2024-01-15.</p></body></html>",
        encoding="utf-8",
    )
    payload = _extract_payload(html_path)
    assert payload["text_source"] == "html2text"
    assert "Office Visit" in payload["text"]
    assert "Patient seen 2024-01-15" in payload["text"]
    assert "<style>" not in payload["text"]
    assert "<p>" not in payload["text"]


def test_extract_payload_pdf_falls_back_when_anydoc_unavailable(tmp_path: Path, monkeypatch):
    from archive_sync.adapters import file_libraries as fl

    pdf_path = tmp_path / "hello.pdf"
    pdf_path.write_bytes(_minimal_text_pdf("Hello fallback PDF"))
    monkeypatch.setattr(fl, "_try_anydoc", lambda path: None)
    payload = fl._extract_payload(pdf_path)
    assert payload["text_source"] == "pdf"
    assert "Hello fallback PDF" in payload["text"]

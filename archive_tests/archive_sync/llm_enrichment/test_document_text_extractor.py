"""Unit tests for document_text_extractor (no vault fixtures)."""

from __future__ import annotations

from pathlib import Path

from archive_sync.llm_enrichment.document_text_extractor import (
    extract_markdown_text,
    needs_markitdown_extraction,
    resolve_source_file,
)


def test_needs_markitdown_plain_rtf() -> None:
    assert needs_markitdown_extraction({"text_source": "plain", "extension": "rtf"}) is True


def test_needs_markitdown_idempotent() -> None:
    assert needs_markitdown_extraction({"text_source": "markitdown", "extension": "rtf"}) is False
    assert needs_markitdown_extraction({"text_source": "anydoc", "extension": "pdf"}) is False
    assert needs_markitdown_extraction({"text_source": "anydoc_hosted", "extension": "pdf"}) is False
    assert needs_markitdown_extraction({"text_source": "html2text", "extension": "htm"}) is False
    assert needs_markitdown_extraction({"text_source": "plain", "extension": "txt"}) is False


def test_needs_extraction_for_legacy_pdf() -> None:
    assert needs_markitdown_extraction({"text_source": "pdf", "extension": "pdf"}) is True


def test_needs_markitdown_metadata_only() -> None:
    assert needs_markitdown_extraction({"text_source": "pdf", "quality_flags": ["metadata_only"]}) is True


def test_resolve_source_file(tmp_path: Path) -> None:
    f = tmp_path / "a" / "b.txt"
    f.parent.mkdir(parents=True)
    f.write_text("x", encoding="utf-8")
    got = resolve_source_file(str(tmp_path), "a/b.txt")
    assert got == f.resolve()


def test_resolve_source_file_roots_label(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.adapters import file_libraries as fl

    f = tmp_path / "scans" / "note.pdf"
    f.parent.mkdir(parents=True)
    f.write_bytes(b"%PDF")
    monkeypatch.setitem(fl.ROOTS, "documents", tmp_path)
    assert resolve_source_file("documents", "scans/note.pdf") == f.resolve()


def test_resolve_source_file_gdrive_label(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.adapters import file_libraries as fl

    f = tmp_path / "tax.pdf"
    f.write_bytes(b"%PDF")
    monkeypatch.setitem(fl.ROOTS, "gdrive.personal", tmp_path)
    assert resolve_source_file("gdrive.personal", "tax.pdf") == f.resolve()


def test_resolve_source_file_custom_root(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.adapters import file_libraries as fl

    f = tmp_path / "chart.pdf"
    f.write_bytes(b"%PDF")
    monkeypatch.setitem(fl.CUSTOM_ROOTS, "custom:requested record", tmp_path)
    assert resolve_source_file("custom:requested record", "chart.pdf") == f.resolve()


def test_resolve_source_file_unknown_label() -> None:
    assert resolve_source_file("documents-not-a-real-root", "a.pdf") is None


def test_resolve_source_file_skips_office_lock(tmp_path: Path) -> None:
    f = tmp_path / "~$lock.docx"
    f.write_bytes(b"x")
    assert resolve_source_file(str(tmp_path), "~$lock.docx") is None


def test_resolve_source_file_missing(tmp_path: Path) -> None:
    assert resolve_source_file(str(tmp_path), "nope.pdf") is None


def test_extract_markdown_html(tmp_path: Path) -> None:
    p = tmp_path / "t.html"
    p.write_text("<html><body><p>Hello</p></body></html>", encoding="utf-8")
    out = extract_markdown_text(p)
    assert "Hello" in out


def test_convert_document_tries_anydoc_for_images(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    from archive_sync.llm_enrichment.document_text_extractor import convert_document_to_markdown

    seen: dict[str, object] = {}

    def _fake_to_markdown(path: str, **kwargs):
        seen["path"] = path
        seen["kwargs"] = kwargs
        return "# OCR image\n"

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    img = tmp_path / "scan.png"
    img.write_bytes(b"\x89PNG\r\n")
    text, source = convert_document_to_markdown(img)
    assert source == "anydoc"
    assert "OCR image" in text
    assert seen["kwargs"]["ocr"] == "reject"

"""Unit tests for document_text_extractor (no vault fixtures)."""

from __future__ import annotations

from pathlib import Path

from archive_sync.llm_enrichment.document_text_extractor import (
    extract_markdown_text, needs_markitdown_extraction, resolve_source_file)


def test_needs_markitdown_plain_rtf() -> None:
    assert needs_markitdown_extraction({"text_source": "plain", "extension": "rtf"}) is True


def test_needs_markitdown_idempotent() -> None:
    assert needs_markitdown_extraction({"text_source": "markitdown", "extension": "rtf"}) is False


def test_needs_markitdown_metadata_only() -> None:
    assert needs_markitdown_extraction({"text_source": "pdf", "quality_flags": ["metadata_only"]}) is True


def test_resolve_source_file(tmp_path: Path) -> None:
    f = tmp_path / "a" / "b.txt"
    f.parent.mkdir(parents=True)
    f.write_text("x", encoding="utf-8")
    got = resolve_source_file(str(tmp_path), "a/b.txt")
    assert got == f.resolve()


def test_extract_markdown_html(tmp_path: Path) -> None:
    p = tmp_path / "t.html"
    p.write_text("<html><body><p>Hello</p></body></html>", encoding="utf-8")
    out = extract_markdown_text(p)
    assert "Hello" in out

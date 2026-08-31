"""Shared document extract library. Never calls Firecrawl."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_sync.extract_cache import reset_extract_cache_for_tests
from archive_sync.document_extract import (
    STATUS_EXTRACTED,
    STATUS_NEEDS_OCR,
    STATUS_NON_DOC,
    STATUS_SUPPRESSED,
    STATUS_TINY_IMAGE,
    bytes_sha256,
    extract_from_bytes,
    extract_from_path,
    is_skippable_non_doc,
    is_suppressed_classification,
    is_tiny_image,
)


class _NeedsOcrError(Exception):
    pass


@pytest.fixture(autouse=True)
def _isolate_extract_cache(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PPA_ANYDOC_EXTRACT_CACHE", str(tmp_path / "anydoc-extract-cache.sqlite"))
    reset_extract_cache_for_tests()
    yield
    reset_extract_cache_for_tests()


def test_skips_audio_video_archives() -> None:
    assert is_skippable_non_doc("song.mp3", "audio/mpeg") is True
    assert is_skippable_non_doc("clip.mp4", "video/mp4") is True
    assert is_skippable_non_doc("bundle.zip", "application/zip") is True
    assert is_skippable_non_doc("scan.pdf", "application/pdf") is False


def test_tiny_inline_image_skip() -> None:
    assert is_tiny_image("logo.png", 12_000, "image/png") is True
    assert is_tiny_image("scan.png", 80_000, "image/png") is False
    data = b"\x89PNG" + b"x" * 100
    result = extract_from_bytes(
        data, filename="logo.png", mime_type="image/png", is_inline=True
    )
    assert result.status == STATUS_TINY_IMAGE
    assert result.extracted_text_sha == bytes_sha256(data)


def test_suppressed_skip() -> None:
    assert is_suppressed_classification("marketing") is True
    assert is_suppressed_classification("transactional_receipt") is False
    result = extract_from_bytes(b"%PDF-1.4", filename="ad.pdf", skip_extract=True)
    assert result.status == STATUS_SUPPRESSED


def test_sha_skip_does_not_convert(tmp_path: Path, monkeypatch) -> None:
    data = b"%PDF-1.4 tiny"
    path = tmp_path / "scan.pdf"
    path.write_bytes(data)
    sha = bytes_sha256(data)
    called = {"n": 0}

    def _boom(*_args, **_kwargs):
        called["n"] += 1
        raise AssertionError("should not convert")

    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        _boom,
    )
    result = extract_from_path(
        path,
        existing_sha=sha,
        existing_status=STATUS_EXTRACTED,
        existing_text="already extracted",
        existing_text_source="anydoc",
    )
    assert result.status == STATUS_EXTRACTED
    assert result.text == "already extracted"
    assert result.reason == "unchanged"
    assert called["n"] == 0


def test_local_first_then_hosted(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls: list[str] = []

    def _fake_to_markdown(path: str, **kwargs):
        calls.append(str(kwargs.get("ocr")))
        if kwargs.get("ocr") == "reject":
            raise _NeedsOcrError("pages")
        return "# Hosted page"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    path = tmp_path / "scan.pdf"
    path.write_bytes(b"%PDF-1.4 fixture")
    result = extract_from_path(path, filename="scan.pdf", mime_type="application/pdf")
    assert result.status == STATUS_EXTRACTED
    assert result.text_source == "anydoc_hosted"
    assert "Hosted page" in result.text
    assert calls == ["reject", "hosted"]


def test_needs_ocr_without_key(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
    monkeypatch.setattr("archive_sync.anydoc_ocr.firecrawl_key_path", lambda: tmp_path / "missing.txt")

    def _fake_to_markdown(path: str, **kwargs):
        assert kwargs.get("ocr") == "reject"
        raise _NeedsOcrError("pages")

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    path = tmp_path / "scan.pdf"
    path.write_bytes(b"%PDF-1.4 fixture")
    result = extract_from_path(path, filename="scan.pdf")
    assert result.status == STATUS_NEEDS_OCR


def test_safe_filename_truncates_gmail_tokens() -> None:
    from archive_sync.document_extract import safe_filename

    token = "ANGjdJ9" + ("x" * 200) + ".pdf"
    name = safe_filename(token)
    assert len(name) < 80
    assert name.endswith(".pdf")
    assert name.startswith("att-")


def test_mp3_is_non_doc() -> None:
    result = extract_from_bytes(b"ID3", filename="voicemail.mp3", mime_type="audio/mpeg")
    assert result.status == STATUS_NON_DOC


def test_unsupported_types_skip_quietly(monkeypatch) -> None:
    from archive_sync.document_extract import is_extractable

    called = {"n": 0}

    def _boom(*_args, **_kwargs):
        called["n"] += 1
        raise AssertionError("should not convert unsupported types")

    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        _boom,
    )
    for name, mime in (
        ("invite.ics", "text/calendar"),
        ("logo.psd", "image/vnd.adobe.photoshop"),
        ("note.eml", "message/rfc822"),
        ("ANGjdJ9xxxxxxxxxxxxxxxx", "application/octet-stream"),
        ("", "application/pdf"),
    ):
        assert is_extractable(name, mime) is False
        result = extract_from_bytes(b"xxxx", filename=name, mime_type=mime)
        assert result.status == STATUS_NON_DOC
        assert result.reason in {"unsupported", "non_doc"}
    assert called["n"] == 0


def test_convert_does_not_call_markitdown_for_unknown(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.document_extract import UnsupportedExtract, convert_document_to_markdown

    def _boom(*_args, **_kwargs):
        raise AssertionError("markitdown should not run")

    monkeypatch.setattr("markitdown.MarkItDown", _boom)
    mystery = tmp_path / "file.psd"
    mystery.write_bytes(b"8BPS")
    try:
        convert_document_to_markdown(mystery)
        raise AssertionError("expected UnsupportedExtract")
    except UnsupportedExtract:
        pass

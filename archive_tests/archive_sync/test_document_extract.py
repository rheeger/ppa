"""Shared document extract library. Never calls Firecrawl."""

from __future__ import annotations

from pathlib import Path

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


def test_mp3_is_non_doc() -> None:
    result = extract_from_bytes(b"ID3", filename="voicemail.mp3", mime_type="audio/mpeg")
    assert result.status == STATUS_NON_DOC

"""Unit tests for anydoc hosted-OCR selection. Never calls Firecrawl."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_sync.anydoc_ocr import (
    anydoc_ocr_kwargs,
    anydoc_ocr_mode,
    load_firecrawl_api_key,
    reset_ocr_reject_log,
)


@pytest.fixture(autouse=True)
def _isolate_firecrawl_key(monkeypatch, tmp_path: Path) -> None:
    key_file = tmp_path / "firecrawl_key.txt"
    monkeypatch.delenv("FIRECRAWL_API_KEY", raising=False)
    monkeypatch.setattr("archive_sync.anydoc_ocr.firecrawl_key_path", lambda: key_file)
    reset_ocr_reject_log()


def test_ocr_reject_when_key_absent() -> None:
    assert load_firecrawl_api_key() == ""
    assert anydoc_ocr_mode() == "reject"
    assert anydoc_ocr_kwargs() == {"ocr": "reject"}


def test_ocr_hosted_when_env_key(monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    assert load_firecrawl_api_key() == "fc-test-env"
    assert anydoc_ocr_mode() == "hosted"
    assert anydoc_ocr_kwargs() == {"ocr": "hosted", "api_key": "fc-test-env"}


def test_ocr_hosted_when_key_file(tmp_path: Path, monkeypatch) -> None:
    key_file = tmp_path / "firecrawl_key.txt"
    key_file.write_text("fc-test-file\n", encoding="utf-8")
    monkeypatch.setattr("archive_sync.anydoc_ocr.firecrawl_key_path", lambda: key_file)
    assert load_firecrawl_api_key() == "fc-test-file"
    assert anydoc_ocr_mode() == "hosted"
    assert anydoc_ocr_kwargs()["ocr"] == "hosted"
    assert anydoc_ocr_kwargs()["api_key"] == "fc-test-file"


def test_ocr_env_wins_over_key_file(tmp_path: Path, monkeypatch) -> None:
    key_file = tmp_path / "firecrawl_key.txt"
    key_file.write_text("fc-test-file\n", encoding="utf-8")
    monkeypatch.setattr("archive_sync.anydoc_ocr.firecrawl_key_path", lambda: key_file)
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    assert load_firecrawl_api_key() == "fc-test-env"


def test_missing_key_logs_once(caplog) -> None:
    import logging

    caplog.set_level(logging.WARNING, logger="ppa.anydoc_ocr")
    assert anydoc_ocr_mode() == "reject"
    assert anydoc_ocr_mode() == "reject"
    messages = [r.message for r in caplog.records if "hosted OCR disabled" in r.message]
    assert len(messages) == 1


def test_try_anydoc_passes_hosted_kwargs(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    from archive_sync.adapters import file_libraries as fl

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    seen: dict[str, object] = {}

    def _fake_to_markdown(path: str, **kwargs):
        seen["path"] = path
        seen["kwargs"] = kwargs
        return "# Hello\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    payload = fl._try_anydoc(pdf)
    assert payload is not None
    assert payload["text_source"] == "anydoc"
    assert "Hello" in payload["text"]
    assert seen["kwargs"] == {"ocr": "hosted", "api_key": "fc-test-env"}


def test_try_anydoc_passes_reject_without_key(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    from archive_sync.adapters import file_libraries as fl

    seen: dict[str, object] = {}

    def _fake_to_markdown(path: str, **kwargs):
        seen["kwargs"] = kwargs
        return "# Local\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    payload = fl._try_anydoc(pdf)
    assert payload is not None
    assert seen["kwargs"] == {"ocr": "reject"}


def test_convert_document_uses_ocr_kwargs(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    from archive_sync.llm_enrichment.document_text_extractor import convert_document_to_markdown

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    seen: dict[str, object] = {}

    def _fake_to_markdown(path: str, **kwargs):
        seen["kwargs"] = kwargs
        return "# Scan text\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    text, source = convert_document_to_markdown(pdf)
    assert source == "anydoc"
    assert "Scan text" in text
    assert seen["kwargs"]["ocr"] == "hosted"

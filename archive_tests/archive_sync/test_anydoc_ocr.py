"""Unit tests for local-first anydoc OCR. Never calls Firecrawl."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_sync.anydoc_ocr import (
    anydoc_hosted_ocr_kwargs,
    anydoc_ocr_kwargs,
    anydoc_ocr_mode,
    hosted_ocr_available,
    load_firecrawl_api_key,
    reset_ocr_reject_log,
    to_markdown_local_first,
)


class _NeedsOcrError(Exception):
    pass


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
    assert hosted_ocr_available() is False
    assert anydoc_hosted_ocr_kwargs() is None


def test_first_kwargs_always_reject_even_with_key(monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    assert load_firecrawl_api_key() == "fc-test-env"
    assert anydoc_ocr_mode() == "reject"
    assert anydoc_ocr_kwargs() == {"ocr": "reject"}
    assert hosted_ocr_available() is True
    assert anydoc_hosted_ocr_kwargs() == {"ocr": "hosted", "api_key": "fc-test-env"}


def test_hosted_kwargs_from_key_file(tmp_path: Path, monkeypatch) -> None:
    key_file = tmp_path / "firecrawl_key.txt"
    key_file.write_text("fc-test-file\n", encoding="utf-8")
    monkeypatch.setattr("archive_sync.anydoc_ocr.firecrawl_key_path", lambda: key_file)
    assert anydoc_ocr_kwargs() == {"ocr": "reject"}
    assert anydoc_hosted_ocr_kwargs() == {"ocr": "hosted", "api_key": "fc-test-file"}


def test_ocr_env_wins_over_key_file(tmp_path: Path, monkeypatch) -> None:
    key_file = tmp_path / "firecrawl_key.txt"
    key_file.write_text("fc-test-file\n", encoding="utf-8")
    monkeypatch.setattr("archive_sync.anydoc_ocr.firecrawl_key_path", lambda: key_file)
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    assert load_firecrawl_api_key() == "fc-test-env"


def test_missing_key_logs_once(caplog) -> None:
    import logging

    caplog.set_level(logging.WARNING, logger="ppa.anydoc_ocr")
    assert anydoc_hosted_ocr_kwargs() is None
    assert anydoc_hosted_ocr_kwargs() is None
    messages = [r.message for r in caplog.records if "hosted OCR disabled" in r.message]
    assert len(messages) == 1


def test_local_first_never_hosted_on_success(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls: list[dict[str, object]] = []

    def _fake_to_markdown(path: str, **kwargs):
        calls.append(dict(kwargs))
        return "# Local\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    text, source = to_markdown_local_first(pdf)
    assert source == "anydoc"
    assert "Local" in text
    assert calls == [{"ocr": "reject"}]


def test_needs_ocr_then_hosted(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls: list[dict[str, object]] = []

    def _fake_to_markdown(path: str, **kwargs):
        calls.append(dict(kwargs))
        if kwargs.get("ocr") == "reject":
            raise _NeedsOcrError("pages 1-2")
        return "# Hosted scan\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    text, source = to_markdown_local_first(pdf)
    assert source == "anydoc_hosted"
    assert "Hosted scan" in text
    assert calls[0]["ocr"] == "reject"
    assert calls[1]["ocr"] == "hosted"
    assert calls[1]["api_key"] == "fc-test-env"


def test_needs_ocr_without_key_stays_reject(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    def _fake_to_markdown(path: str, **kwargs):
        assert kwargs.get("ocr") == "reject"
        raise _NeedsOcrError("pages 1")

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    with pytest.raises(_NeedsOcrError):
        to_markdown_local_first(pdf)


def test_needs_ocr_allow_hosted_false(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")

    def _fake_to_markdown(path: str, **kwargs):
        assert kwargs.get("ocr") == "reject"
        raise _NeedsOcrError("pages 1")

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    with pytest.raises(_NeedsOcrError):
        to_markdown_local_first(pdf, allow_hosted=False)


def test_try_anydoc_local_first_with_key(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    from archive_sync.adapters import file_libraries as fl

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    seen: list[dict[str, object]] = []

    def _fake_to_markdown(path: str, **kwargs):
        seen.append(dict(kwargs))
        return "# Hello\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    payload = fl._try_anydoc(pdf)
    assert payload is not None
    assert payload["text_source"] == "anydoc"
    assert "Hello" in payload["text"]
    assert seen == [{"ocr": "reject"}]


def test_try_anydoc_needs_ocr_then_hosted(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    from archive_sync.adapters import file_libraries as fl

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    seen: list[dict[str, object]] = []

    def _fake_to_markdown(path: str, **kwargs):
        seen.append(dict(kwargs))
        if kwargs.get("ocr") == "reject":
            raise _NeedsOcrError("scan")
        return "# Hosted\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    payload = fl._try_anydoc(pdf)
    assert payload is not None
    assert payload["text_source"] == "anydoc_hosted"
    assert seen[0]["ocr"] == "reject"
    assert seen[1]["ocr"] == "hosted"


def test_convert_document_uses_reject_first(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    from archive_sync.llm_enrichment.document_text_extractor import convert_document_to_markdown

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    seen: list[dict[str, object]] = []

    def _fake_to_markdown(path: str, **kwargs):
        seen.append(dict(kwargs))
        return "# Scan text\n"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    text, source = convert_document_to_markdown(pdf)
    assert source == "anydoc"
    assert "Scan text" in text
    assert seen[0]["ocr"] == "reject"
    assert all(call["ocr"] != "hosted" or i > 0 for i, call in enumerate(seen))

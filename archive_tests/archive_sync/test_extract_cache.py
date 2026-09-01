"""Hosted-OCR extract cache + in-flight SHA lock. Never calls Firecrawl."""

from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from archive_sync.anydoc_ocr import to_markdown_local_first
from archive_sync.attachment_text import AttachmentJob, extract_job
from archive_sync.document_extract import (
    STATUS_EXTRACTED,
    STATUS_FAILED,
    STATUS_NEEDS_OCR,
    STATUS_NON_DOC,
    STATUS_TINY_IMAGE,
    bytes_sha256,
    extract_from_bytes,
    extract_from_path,
)
from archive_sync.extract_cache import (
    ExtractCache,
    default_extract_cache_path,
    get_extract_cache,
    reset_extract_cache_for_tests,
    seed_card,
)


class _NeedsOcrError(Exception):
    pass


class _HostedError(Exception):
    pass


class _EncryptedError(Exception):
    pass


@pytest.fixture(autouse=True)
def _isolate_extract_cache(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("PPA_ANYDOC_EXTRACT_CACHE", str(tmp_path / "anydoc-extract-cache.sqlite"))
    reset_extract_cache_for_tests()
    yield
    reset_extract_cache_for_tests()


def _patch_hosted_anydoc(
    monkeypatch, *, hosted_return: str = "# Hosted once", fail_hosted: BaseException | None = None
):
    """Mock anydoc: local always NeedsOcr; hosted is counted. Never hits the network."""

    import anydoc

    calls: list[str] = []
    lock = threading.Lock()

    def _record(ocr: str) -> None:
        with lock:
            calls.append(ocr)

    def _fake_to_markdown(_path: str, **kwargs):
        ocr = str(kwargs.get("ocr"))
        _record(ocr)
        if ocr == "reject":
            raise _NeedsOcrError("pages")
        if fail_hosted is not None:
            raise fail_hosted
        return hosted_return

    def _fake_to_markdown_bytes(_data: bytes, **kwargs):
        return _fake_to_markdown("bytes", **kwargs)

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", _fake_to_markdown_bytes)
    return calls


def _patch_local_anydoc(monkeypatch, *, text: str = "# Local"):
    import anydoc

    calls: list[str] = []

    def _fake_to_markdown(_path: str, **kwargs):
        calls.append(str(kwargs.get("ocr")))
        return text

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", lambda _data, **kwargs: _fake_to_markdown("bytes", **kwargs))
    return calls


def test_cache_path_override_uses_tmp_sqlite(tmp_path: Path) -> None:
    expected = tmp_path / "anydoc-extract-cache.sqlite"
    assert default_extract_cache_path() == expected
    cache = get_extract_cache()
    assert cache._path == expected
    cache.put(bytes_sha256(b"x" * 8), "# Tmp", "anydoc")
    assert expected.is_file()


def test_same_bytes_twice_sequential_hosted_once(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch)
    data = b"%PDF-1.4 duplicate-scan"
    first = tmp_path / "a.pdf"
    second = tmp_path / "b.pdf"
    first.write_bytes(data)
    second.write_bytes(data)

    r1 = extract_from_path(first, filename="a.pdf")
    r2 = extract_from_path(second, filename="b.pdf")

    assert r1.status == STATUS_EXTRACTED
    assert r1.text_source == "anydoc_hosted"
    assert r2.status == STATUS_EXTRACTED
    assert r2.reason == "hash_reuse"
    assert r2.text == r1.text
    assert r2.text_source == "anydoc_hosted"
    assert r2.extracted_text_sha == bytes_sha256(data)
    assert calls.count("hosted") == 1
    assert calls.count("reject") == 1


def test_same_bytes_24_concurrent_workers_hosted_once(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch, hosted_return="# Heeger hosted")
    data = b"%PDF-1.4 Heeger.pdf shared-bytes"
    path = tmp_path / "Heeger.pdf"
    path.write_bytes(data)
    barrier = threading.Barrier(24)

    def _worker() -> object:
        barrier.wait(timeout=5)
        return extract_from_bytes(data, filename="Heeger.pdf", mime_type="application/pdf")

    with ThreadPoolExecutor(max_workers=24) as pool:
        results = list(pool.map(lambda _: _worker(), range(24)))

    assert calls.count("hosted") == 1
    assert all(item.status == STATUS_EXTRACTED for item in results)
    assert all(item.text == "# Heeger hosted" for item in results)
    assert all(item.text_source == "anydoc_hosted" for item in results)
    assert get_extract_cache().inflight_waits >= 1


def test_same_sha_document_and_attachment_hosted_once(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch)
    data = b"%PDF-1.4 shared-on-two-card-types"
    doc = tmp_path / "Heeger.pdf"
    doc.write_bytes(data)
    uid = "hfa-email-attachment-same-sha"
    from archive_sync.attachment_text import cache_attachment_bytes

    cache_attachment_bytes(tmp_path, uid, "Heeger.pdf", data)

    doc_result = extract_from_path(doc, filename="Heeger.pdf")
    att_result = extract_job(
        tmp_path,
        AttachmentJob(uid=uid, filename="Heeger.pdf", mime_type="application/pdf"),
    )

    assert doc_result.status == STATUS_EXTRACTED
    assert att_result.status == STATUS_EXTRACTED
    assert att_result.reason == "hash_reuse"
    assert att_result.text == doc_result.text
    assert calls.count("hosted") == 1


def test_preseeded_sqlite_row_hosted_zero(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    data = b"%PDF-1.4 already-cached-in-sqlite"
    sha = bytes_sha256(data)
    cache = get_extract_cache()
    assert cache.put(sha, "# Pre-seeded markdown", "anydoc_hosted") is True
    reset_extract_cache_for_tests()

    def _boom(*_args, **_kwargs):
        raise AssertionError("anydoc/hosted must not run on pre-seeded cache")

    monkeypatch.setattr(anydoc, "to_markdown", _boom)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", _boom)
    path = tmp_path / "copy.pdf"
    path.write_bytes(data)
    result = extract_from_path(path, filename="copy.pdf")
    assert result.reason == "hash_reuse"
    assert result.text == "# Pre-seeded markdown"
    assert result.text_source == "anydoc_hosted"
    assert get_extract_cache().stats()["puts"] == 0


def test_restart_after_crash_second_process_hosted_zero(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    data = b"%PDF-1.4 crash-then-restart"
    path = tmp_path / "scan.pdf"
    path.write_bytes(data)
    calls = _patch_hosted_anydoc(monkeypatch, hosted_return="# Paid OCR")

    first = extract_from_path(path, filename="scan.pdf")
    assert first.text_source == "anydoc_hosted"
    assert calls.count("hosted") == 1
    db_path = tmp_path / "anydoc-extract-cache.sqlite"
    assert db_path.is_file()

    reset_extract_cache_for_tests()
    calls.clear()
    second = extract_from_path(path, filename="scan.pdf")
    assert second.reason == "hash_reuse"
    assert second.text == "# Paid OCR"
    assert calls.count("hosted") == 0
    assert get_extract_cache().get(bytes_sha256(data)) is not None


def test_direct_sqlite_row_survives_new_connection(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    data = b"%PDF-1.4 raw-sqlite-insert"
    sha = bytes_sha256(data)
    db = tmp_path / "anydoc-extract-cache.sqlite"
    get_extract_cache()
    reset_extract_cache_for_tests()
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO extracts (sha256, markdown, text_source, status, created_at) VALUES (?, ?, ?, ?, ?)",
        (sha, "# From previous process", "anydoc_hosted", "content_extracted", "2026-08-30T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    def _boom(*_args, **_kwargs):
        raise AssertionError("hosted must not run when sqlite already has the sha")

    monkeypatch.setattr(anydoc, "to_markdown", _boom)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", _boom)
    path = tmp_path / "scan.pdf"
    path.write_bytes(data)
    result = extract_from_path(path, filename="scan.pdf")
    assert result.reason == "hash_reuse"
    assert result.text == "# From previous process"


def test_different_bytes_hosted_twice(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch)
    a = tmp_path / "a.pdf"
    b = tmp_path / "b.pdf"
    a.write_bytes(b"%PDF-1.4 file-a")
    b.write_bytes(b"%PDF-1.4 file-b")

    r1 = extract_from_path(a, filename="a.pdf")
    r2 = extract_from_path(b, filename="b.pdf")

    assert r1.reason != "hash_reuse"
    assert r2.reason != "hash_reuse"
    assert r1.text == r2.text
    assert calls.count("hosted") == 2
    assert calls.count("reject") == 2


def test_local_anydoc_success_hosted_zero(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_local_anydoc(monkeypatch, text="# Local extract")
    path = tmp_path / "native.pdf"
    path.write_bytes(b"%PDF-1.4 native-text")
    result = extract_from_path(path, filename="native.pdf")
    assert result.status == STATUS_EXTRACTED
    assert result.text_source == "anydoc"
    assert "Local extract" in result.text
    assert calls == ["reject"]
    assert get_extract_cache().get(bytes_sha256(path.read_bytes())) is not None


def test_needs_ocr_cache_miss_then_hit(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch, hosted_return="# Hosted scan")
    data = b"%PDF-1.4 needs-ocr-then-reuse"
    path = tmp_path / "scan.pdf"
    path.write_bytes(data)

    miss = extract_from_path(path, filename="scan.pdf")
    assert miss.text_source == "anydoc_hosted"
    assert calls.count("hosted") == 1

    hit = extract_from_bytes(data, filename="scan.pdf", mime_type="application/pdf")
    assert hit.reason == "hash_reuse"
    assert hit.text == "# Hosted scan"
    assert calls.count("hosted") == 1


def test_local_reject_does_not_cache_success(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")

    def _fake_to_markdown(_path: str, **kwargs):
        raise _EncryptedError("encrypted / permission")

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", lambda *_a, **_k: _fake_to_markdown("bytes"))
    data = b"%PDF-1.4 encrypted"
    path = tmp_path / "secret.pdf"
    path.write_bytes(data)
    result = extract_from_path(path, filename="secret.pdf")
    assert result.status in {STATUS_NON_DOC, STATUS_FAILED}
    assert get_extract_cache().get(bytes_sha256(data)) is None
    assert get_extract_cache().stats()["puts"] == 0


def test_hosted_403_does_not_cache_retry_allowed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch, fail_hosted=_HostedError("403 permission denied"))
    data = b"%PDF-1.4 hosted-forbidden"
    path = tmp_path / "scan.pdf"
    path.write_bytes(data)

    first = extract_from_path(path, filename="scan.pdf")
    assert first.status == STATUS_NEEDS_OCR
    assert get_extract_cache().get(bytes_sha256(data)) is None
    assert calls.count("hosted") == 1

    second = extract_from_path(path, filename="scan.pdf")
    assert second.status == STATUS_NEEDS_OCR
    assert get_extract_cache().get(bytes_sha256(data)) is None
    assert calls.count("hosted") == 2


def test_failed_hosted_does_not_cache_retry_allowed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch, fail_hosted=RuntimeError("firecrawl 500"))
    data = b"%PDF-1.4 hosted-failed"
    path = tmp_path / "scan.pdf"
    path.write_bytes(data)
    first = extract_from_path(path, filename="scan.pdf")
    assert first.status == STATUS_NEEDS_OCR
    assert get_extract_cache().get(bytes_sha256(data)) is None
    second = extract_from_path(path, filename="scan.pdf")
    assert second.status == STATUS_NEEDS_OCR
    assert calls.count("hosted") == 2


def test_unsupported_filetype_never_calls_hosted(monkeypatch) -> None:
    import anydoc

    called = {"n": 0}

    def _boom(*_args, **_kwargs):
        called["n"] += 1
        raise AssertionError("hosted/local convert must not run")

    monkeypatch.setattr(anydoc, "to_markdown", _boom)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", _boom)
    result = extract_from_bytes(b"BEGIN:VCALENDAR", filename="invite.ics", mime_type="text/calendar")
    assert result.status == STATUS_NON_DOC
    assert called["n"] == 0
    assert get_extract_cache().stats()["puts"] == 0
    assert get_extract_cache().get(bytes_sha256(b"BEGIN:VCALENDAR")) is None


def test_tiny_inline_image_skips_hosted(monkeypatch) -> None:
    import anydoc

    called = {"n": 0}

    def _boom(*_args, **_kwargs):
        called["n"] += 1
        raise AssertionError("tiny inline image must not convert")

    monkeypatch.setattr(anydoc, "to_markdown", _boom)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", _boom)
    data = b"\x89PNG" + b"x" * 100
    result = extract_from_bytes(data, filename="logo.png", mime_type="image/png", is_inline=True)
    assert result.status == STATUS_TINY_IMAGE
    assert called["n"] == 0
    assert get_extract_cache().get(bytes_sha256(data)) is None


def test_vault_card_seed_is_a_hit(tmp_path: Path, monkeypatch) -> None:
    import anydoc

    called = {"n": 0}

    def _boom(*_args, **_kwargs):
        called["n"] += 1
        raise AssertionError("hosted/local convert must not run on hash hit")

    monkeypatch.setattr(anydoc, "to_markdown", _boom)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", _boom)
    data = b"%PDF-1.4 already-on-another-card"
    sha = bytes_sha256(data)
    seeded = seed_card(
        get_extract_cache(),
        {
            "content_sha": sha,
            "extracted_text_sha": sha,
            "extraction_status": STATUS_EXTRACTED,
            "text_source": "anydoc_hosted",
        },
        "# From vault card",
    )
    assert seeded == 1

    path = tmp_path / "copy.pdf"
    path.write_bytes(data)
    result = extract_from_path(path, filename="copy.pdf")
    assert result.reason == "hash_reuse"
    assert result.text == "# From vault card"
    assert result.text_source == "anydoc_hosted"
    assert called["n"] == 0


def test_empty_or_failed_status_is_not_cached() -> None:
    cache = get_extract_cache()
    sha = bytes_sha256(b"%PDF-1.4 no-store")
    assert cache.put(sha, "", "anydoc_hosted") is False
    assert cache.put(sha, "# Text", "anydoc_hosted", status="failed") is False
    assert cache.put(sha, "# Text", "anydoc_hosted", status="needs_ocr") is False
    assert cache.get(sha) is None


def test_put_rejects_unknown_text_source() -> None:
    cache = get_extract_cache()
    sha = bytes_sha256(b"%PDF-1.4 unknown-source")
    assert cache.put(sha, "# Text", "firecrawl_direct") is False
    assert cache.get(sha) is None


def test_to_markdown_local_first_cache_before_hosted(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    calls = _patch_hosted_anydoc(monkeypatch, hosted_return="# First hosted")
    pdf = tmp_path / "scan.pdf"
    pdf.write_bytes(b"%PDF-1.4 local-first-cache")
    text, source = to_markdown_local_first(pdf)
    assert source == "anydoc_hosted"
    assert "First hosted" in text
    assert calls == ["reject", "hosted"]
    calls.clear()
    text2, source2 = to_markdown_local_first(pdf)
    assert source2 == "anydoc_hosted"
    assert text2 == text
    assert calls == []


def test_inflight_waiter_reuses_owner_result(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test-env")
    import anydoc

    hosted_started = threading.Event()
    release_hosted = threading.Event()
    calls: list[str] = []
    lock = threading.Lock()

    def _fake_to_markdown(_path: str, **kwargs):
        ocr = str(kwargs.get("ocr"))
        with lock:
            calls.append(ocr)
        if ocr == "reject":
            raise _NeedsOcrError("pages")
        hosted_started.set()
        assert release_hosted.wait(timeout=5)
        return "# Owner result"

    monkeypatch.setattr(anydoc, "to_markdown", _fake_to_markdown)
    monkeypatch.setattr(anydoc, "to_markdown_bytes", lambda _d, **kw: _fake_to_markdown("bytes", **kw))
    data = b"%PDF-1.4 inflight-owner"
    path = tmp_path / "scan.pdf"
    path.write_bytes(data)

    results: list[object] = []

    def _owner() -> None:
        results.append(extract_from_path(path, filename="scan.pdf"))

    def _waiter() -> None:
        results.append(extract_from_bytes(data, filename="scan.pdf", mime_type="application/pdf"))

    t1 = threading.Thread(target=_owner)
    t2 = threading.Thread(target=_waiter)
    t1.start()
    assert hosted_started.wait(timeout=5)
    t2.start()
    time.sleep(0.1)
    release_hosted.set()
    t1.join(timeout=5)
    t2.join(timeout=5)
    assert len(results) == 2
    assert calls.count("hosted") == 1
    assert all(getattr(item, "text", None) == "# Owner result" for item in results)


def test_extract_cache_wal_and_single_connection(tmp_path: Path) -> None:
    cache = ExtractCache(tmp_path / "manual.sqlite")
    sha = bytes_sha256(b"%PDF-1.4 wal")
    assert cache.put(sha, "# WAL", "anydoc") is True
    assert cache.get(sha) is not None
    mode = cache._conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert str(mode).lower() == "wal"
    cache.close()

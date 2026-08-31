"""Email-attachment extract. Markdown stays on the attachment card. Never calls Firecrawl."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_sync.extract_cache import reset_extract_cache_for_tests
from archive_sync.adapters.gmail_messages import GmailMessagesAdapter, _attachment_uid
from archive_sync.attachment_text import (
    ATTACHMENTS_LIST_SENTINEL,
    ATTACHMENTS_SECTION_HEADING,
    ATTACHMENTS_SECTION_SENTINEL,
    STATUS_ALREADY_CACHED,
    STATUS_EXTRACTED,
    STATUS_FETCHED,
    STATUS_MISSING,
    STATUS_NON_DOC,
    AttachmentExtraction,
    AttachmentJob,
    bytes_sha256,
    cache_attachment_bytes,
    extract_job,
    extract_jobs,
    is_skippable_non_doc,
    merge_message_body,
    preserve_message_attachments_section,
    render_attachments_section,
    resolve_local_attachment,
    run_attachment_fetch,
    run_attachment_text_extraction,
    strip_attachments_section,
    strip_ocr_dump_section,
)
from archive_vault.schema import EmailAttachmentCard
from archive_vault.vault import read_note, write_card


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
    assert is_skippable_non_doc("winmail.dat", "application/ms-tnef") is True
    assert is_skippable_non_doc("invite.ics", "text/calendar") is True
    assert is_skippable_non_doc("scan.pdf", "application/pdf") is False
    assert is_skippable_non_doc("photo.jpg", "image/jpeg") is False
    assert is_skippable_non_doc("notes.html", "text/html") is False


def test_message_section_is_filename_list_only() -> None:
    raw = "Hello from the email.\n\nMore body."
    section = render_attachments_section(
        [
            AttachmentExtraction(
                status=STATUS_EXTRACTED,
                text="# Receipt\n\nTotal $12",
                filename="receipt.pdf",
                uid="hfa-email-attachment-abc",
            )
        ]
    )
    merged = merge_message_body(raw, section)
    assert ATTACHMENTS_SECTION_HEADING in merged
    assert ATTACHMENTS_LIST_SENTINEL in merged
    assert "[[hfa-email-attachment-abc]] receipt.pdf" in merged
    assert "Total $12" not in merged
    assert "# Receipt" not in merged
    assert strip_attachments_section(merged) == raw


def test_strips_legacy_ocr_dump() -> None:
    dump = (
        "Please see attached.\n\n"
        f"{ATTACHMENTS_SECTION_SENTINEL}\n{ATTACHMENTS_SECTION_HEADING}\n\n"
        "### receipt.pdf\n\n# Receipt\n\nTotal $12"
    )
    assert "Total $12" not in strip_ocr_dump_section(dump)
    preserved = preserve_message_attachments_section("Please see attached.", dump)
    assert "Total $12" not in preserved
    assert "Please see attached." in preserved


def test_extract_html_local_no_anydoc(tmp_path: Path) -> None:
    uid = "hfa-email-attachment-htmlfix01"
    html = tmp_path / "page.html"
    html.write_text("<html><body><h1>Invoice 42</h1><p>Paid</p></body></html>", encoding="utf-8")
    cached = cache_attachment_bytes(tmp_path, uid, "page.html", html.read_bytes())
    assert resolve_local_attachment(tmp_path, uid, "page.html") == cached
    result = extract_job(
        tmp_path,
        AttachmentJob(uid=uid, filename="page.html", mime_type="text/html"),
    )
    assert result.status == STATUS_EXTRACTED
    assert "Invoice 42" in result.text
    assert result.text_source == "html2text"
    assert result.extracted_text_sha == bytes_sha256(html.read_bytes())


def test_extract_skips_unchanged_sha(tmp_path: Path, monkeypatch) -> None:
    uid = "hfa-email-attachment-sha001"
    data = b"%PDF-1.4 tiny"
    cache_attachment_bytes(tmp_path, uid, "scan.pdf", data)
    sha = bytes_sha256(data)

    def _boom(*_args, **_kwargs):
        raise AssertionError("should not convert")

    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        _boom,
    )
    result = extract_job(
        tmp_path,
        AttachmentJob(
            uid=uid,
            filename="scan.pdf",
            mime_type="application/pdf",
            existing_sha=sha,
            existing_status=STATUS_EXTRACTED,
            existing_text="already extracted",
            existing_text_source="anydoc",
        ),
    )
    assert result.status == STATUS_EXTRACTED
    assert result.text == "already extracted"
    assert result.reason == "unchanged"


def test_extract_pdf_uses_shared_convert(tmp_path: Path, monkeypatch) -> None:
    uid = "hfa-email-attachment-pdf001"
    data = b"%PDF-1.4 fixture"
    cache_attachment_bytes(tmp_path, uid, "scan.pdf", data)
    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: ("# Scanned page\n\nHello", "anydoc"),
    )
    result = extract_job(
        tmp_path,
        AttachmentJob(uid=uid, filename="scan.pdf", mime_type="application/pdf"),
    )
    assert result.status == STATUS_EXTRACTED
    assert result.text_source == "anydoc"
    assert "Hello" in result.text
    assert result.extracted_text_sha == bytes_sha256(data)


def test_extract_skips_missing_without_fetch(tmp_path: Path) -> None:
    result = extract_job(
        tmp_path,
        AttachmentJob(
            uid="hfa-email-attachment-missing",
            filename="gone.pdf",
            mime_type="application/pdf",
        ),
    )
    assert result.status == STATUS_MISSING


def test_extract_mp3_is_non_doc(tmp_path: Path) -> None:
    result = extract_job(
        tmp_path,
        AttachmentJob(uid="hfa-email-attachment-audio", filename="voicemail.mp3", mime_type="audio/mpeg"),
    )
    assert result.status == STATUS_NON_DOC


def test_extract_jobs_concurrent(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: (f"text for {Path(path).name}", "anydoc"),
    )
    jobs = []
    for i in range(3):
        uid = f"hfa-email-attachment-par{i:03d}"
        cache_attachment_bytes(tmp_path, uid, f"f{i}.pdf", b"%PDF-1.4 " + str(i).encode())
        jobs.append(AttachmentJob(uid=uid, filename=f"f{i}.pdf", mime_type="application/pdf"))
    results = extract_jobs(tmp_path, jobs, workers=3)
    assert [item.status for item in results] == [STATUS_EXTRACTED] * 3
    assert {item.filename for item in results} == {"f0.pdf", "f1.pdf", "f2.pdf"}


def test_run_backfill_writes_attachment_not_message_ocr(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.adapters.base import deterministic_provenance

    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: ("# Gift agreement excerpt", "anydoc"),
    )
    uid = "hfa-email-attachment-backfill1"
    msg_uid = "hfa-email-message-backfill1"
    att = EmailAttachmentCard(
        uid=uid,
        type="email_attachment",
        source=["gmail.attachment"],
        source_id="me@example.com:m1:a1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="scan.pdf",
        gmail_message_id="m1",
        gmail_thread_id="t1",
        attachment_id="a1",
        account_email="me@example.com",
        message=f"[[{msg_uid}]]",
        filename="scan.pdf",
        mime_type="application/pdf",
        size_bytes=12,
    )
    write_card(
        tmp_path,
        f"EmailAttachments/2026-03/{uid}.md",
        att,
        body="",
        provenance=deterministic_provenance(att, "gmail.attachment"),
    )
    from archive_vault.schema import EmailMessageCard

    msg = EmailMessageCard(
        uid=msg_uid,
        type="email_message",
        source=["gmail.message"],
        source_id="me@example.com:m1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="Scan",
        gmail_message_id="m1",
        gmail_thread_id="t1",
        account_email="me@example.com",
        subject="Scan",
        attachments=[f"[[{uid}]]"],
        has_attachments=True,
    )
    write_card(
        tmp_path,
        f"Email/2026-03/{msg_uid}.md",
        msg,
        body="Please see attached.",
        provenance=deterministic_provenance(msg, "gmail.message"),
    )
    cache_attachment_bytes(tmp_path, uid, "scan.pdf", b"%PDF-1.4 backfill")
    out = run_attachment_text_extraction(tmp_path, dry_run=False)
    assert out["ok"] == 1
    att_fm, att_body, _ = read_note(tmp_path, f"EmailAttachments/2026-03/{uid}.md")
    assert att_fm["extraction_status"] == STATUS_EXTRACTED
    assert att_fm["text_source"] == "anydoc"
    assert att_fm["extracted_text_sha"] == bytes_sha256(b"%PDF-1.4 backfill")
    assert "Gift agreement" in att_body
    _msg_fm, msg_body, _ = read_note(tmp_path, f"Email/2026-03/{msg_uid}.md")
    assert "Please see attached." in msg_body
    assert f"[[{uid}]] scan.pdf" in msg_body
    assert "Gift agreement" not in msg_body
    assert ATTACHMENTS_SECTION_SENTINEL not in msg_body

    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: (_ for _ in ()).throw(AssertionError("re-ocr")),
    )
    again = run_attachment_text_extraction(tmp_path, dry_run=False)
    assert again["processed"] == 0


def test_gmail_apply_does_not_dump_ocr_on_email(tmp_vault: Path, monkeypatch) -> None:
    from archive_tests.archive_sync.test_gmail_messages_adapter import _message, _thread

    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: ("# Contract markdown", "anydoc"),
    )
    uid = _attachment_uid("me@example.com", "m1", "a1")
    cache_attachment_bytes(tmp_vault, uid, "contract.pdf", b"%PDF-1.4 contract")
    adapter = GmailMessagesAdapter()
    responses = iter(
        [
            {"threads": [{"id": "t1"}], "nextPageToken": None},
            _thread(
                "t1",
                _message(
                    message_id="m1",
                    thread_id="t1",
                    internal_date="1710000000000",
                    subject="Contract",
                    body="see attached",
                    from_value="Alice <alice@example.com>",
                    to_value="me@example.com",
                    snippet="see attached",
                    attachment={
                        "attachment_id": "a1",
                        "filename": "contract.pdf",
                        "mime_type": "application/pdf",
                        "size_bytes": 16,
                    },
                ),
            ),
        ]
    )
    adapter._gws = lambda args: next(responses)  # type: ignore[method-assign]
    result = adapter.ingest(str(tmp_vault), account_email="me@example.com", max_threads=10, max_messages=10)
    assert result.created >= 3
    att_rel = next((tmp_vault / "EmailAttachments").rglob("*.md")).relative_to(tmp_vault)
    att_fm, att_body, _ = read_note(tmp_vault, str(att_rel))
    assert att_fm["extraction_status"] == STATUS_EXTRACTED
    assert "Contract markdown" in att_body
    msg_rel = next((tmp_vault / "Email").rglob("*.md")).relative_to(tmp_vault)
    _msg_fm, msg_body, _ = read_note(tmp_vault, str(msg_rel))
    assert "see attached" in msg_body
    assert "Contract markdown" not in msg_body
    assert "[[%s]] contract.pdf" % uid in msg_body


def test_to_card_passes_extraction_fields() -> None:
    adapter = GmailMessagesAdapter()
    card, _, body = adapter.to_card(
        {
            "kind": "attachment",
            "message_id": "m1",
            "thread_id": "t1",
            "attachment_id": "a1",
            "account_email": "me@example.com",
            "created": "2026-03-08",
            "filename": "scan.pdf",
            "mime_type": "application/pdf",
            "extraction_status": "content_extracted",
            "text_source": "anydoc",
            "extracted_text_sha": "abc123",
            "body": "# Extracted",
        }
    )
    assert isinstance(card, EmailAttachmentCard)
    assert card.extraction_status == "content_extracted"
    assert card.text_source == "anydoc"
    assert card.extracted_text_sha == "abc123"
    assert body == "# Extracted"


def test_query_planner_hints_email_attachment() -> None:
    from archive_cli.query_planner import DeterministicQueryPlanner

    plan = DeterministicQueryPlanner().plan("find the email attachment for the k-1")
    assert "email_attachment" in plan.inferred.type_hints


def test_permission_403_does_not_write_card_or_retry(tmp_path: Path) -> None:
    from archive_sync.adapters.gmail_http_errors import GmailPermissionDenied
    from archive_sync.attachment_text import STATUS_FETCH_DENIED, _write_attachment_extraction

    calls = {"n": 0}

    def fetch(_mid, _aid, _acct):
        calls["n"] += 1
        raise GmailPermissionDenied("Permission denied", reason="forbidden")

    result = extract_job(
        tmp_path,
        AttachmentJob(
            uid="hfa-email-attachment-forbid1",
            filename="secret.pdf",
            mime_type="application/pdf",
            message_id="m1",
            attachment_id="a1",
            account_email="me@example.com",
        ),
        fetch_bytes=fetch,
    )
    assert result.status == STATUS_FETCH_DENIED
    assert calls["n"] == 1
    from archive_sync.adapters.base import deterministic_provenance
    from archive_vault.schema import EmailAttachmentCard

    uid = "hfa-email-attachment-forbid1"
    card = EmailAttachmentCard(
        uid=uid,
        type="email_attachment",
        source=["gmail.attachment"],
        source_id="me@example.com:m1:a1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="secret.pdf",
        gmail_message_id="m1",
        gmail_thread_id="t1",
        attachment_id="a1",
        account_email="me@example.com",
        filename="secret.pdf",
        mime_type="application/pdf",
    )
    rel = f"EmailAttachments/2026-03/{uid}.md"
    write_card(tmp_path, rel, card, "", provenance=deterministic_provenance(card, "gmail.attachment"))
    out = _write_attachment_extraction(tmp_path, rel, result, dry_run=False)
    assert out.get("written") is False
    fm, body, _ = read_note(tmp_path, rel)
    assert not fm.get("extraction_status")
    assert body == ""
    from archive_sync.extract_cache import get_extract_cache

    assert get_extract_cache().stats()["puts"] == 0


def test_incremental_write_before_batch_ends(tmp_path: Path, monkeypatch) -> None:
    """First card is on disk before a later extract fails — SIGTERM-safe."""

    import threading

    from archive_sync.adapters.base import deterministic_provenance
    from archive_vault.schema import EmailAttachmentCard, EmailMessageCard

    first_written = threading.Event()
    release_second = threading.Event()

    def _convert(path, **kwargs):
        name = Path(path).name
        if name == "second.pdf":
            assert first_written.wait(timeout=5)
            raise RuntimeError("simulated crash after first write")
        return "# First OCR", "anydoc_hosted"

    monkeypatch.setattr("archive_sync.document_extract.convert_document_to_markdown", _convert)
    monkeypatch.setattr("archive_cli.index_config.get_gmail_api_workers", lambda: 2)

    cards = []
    for i, filename in enumerate(("first.pdf", "second.pdf"), start=1):
        uid = f"hfa-email-attachment-incr{i}"
        msg_uid = f"hfa-email-message-incr{i}"
        att = EmailAttachmentCard(
            uid=uid,
            type="email_attachment",
            source=["gmail.attachment"],
            source_id=f"me@example.com:m{i}:a{i}",
            created="2026-03-08",
            updated="2026-03-08",
            summary=filename,
            gmail_message_id=f"m{i}",
            gmail_thread_id=f"t{i}",
            attachment_id=f"a{i}",
            account_email="me@example.com",
            message=f"[[{msg_uid}]]",
            filename=filename,
            mime_type="application/pdf",
            size_bytes=20,
        )
        write_card(
            tmp_path,
            f"EmailAttachments/2026-03/{uid}.md",
            att,
            body="",
            provenance=deterministic_provenance(att, "gmail.attachment"),
        )
        msg = EmailMessageCard(
            uid=msg_uid,
            type="email_message",
            source=["gmail.message"],
            source_id=f"me@example.com:m{i}",
            created="2026-03-08",
            updated="2026-03-08",
            summary=filename,
            gmail_message_id=f"m{i}",
            gmail_thread_id=f"t{i}",
            account_email="me@example.com",
            subject=filename,
            attachments=[f"[[{uid}]]"],
            has_attachments=True,
        )
        write_card(
            tmp_path,
            f"Email/2026-03/{msg_uid}.md",
            msg,
            body="see attached",
            provenance=deterministic_provenance(msg, "gmail.message"),
        )
        cache_attachment_bytes(tmp_path, uid, filename, b"%PDF-1.4 " + filename.encode())
        cards.append((uid, filename))

    orig_write = write_card

    def _spy_write(vault, rel_path, card, body="", provenance=None, **kwargs):
        out = orig_write(vault, rel_path, card, body, provenance=provenance, **kwargs)
        if "incr1" in str(rel_path) and body:
            first_written.set()
            release_second.set()
        return out

    monkeypatch.setattr("archive_sync.attachment_text.write_card", _spy_write)
    out = run_attachment_text_extraction(tmp_path, dry_run=False)
    fm, body, _ = read_note(tmp_path, "EmailAttachments/2026-03/hfa-email-attachment-incr1.md")
    assert fm["extraction_status"] == STATUS_EXTRACTED
    assert "First OCR" in body
    assert out["ok"] >= 1
    assert first_written.is_set()


def test_successful_fetch_persists_bytes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: ("# Fetched", "anydoc"),
    )
    data = b"%PDF-1.4 fetched"
    result = extract_job(
        tmp_path,
        AttachmentJob(
            uid="hfa-email-attachment-okfetch",
            filename="ok.pdf",
            mime_type="application/pdf",
            message_id="m1",
            attachment_id="a1",
            account_email="me@example.com",
        ),
        fetch_bytes=lambda *_args: data,
    )
    assert result.status == STATUS_EXTRACTED
    cached = resolve_local_attachment(tmp_path, "hfa-email-attachment-okfetch", "ok.pdf")
    assert cached is not None
    assert cached.read_bytes() == data


def test_fetch_only_writes_bytes_without_extract(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: (_ for _ in ()).throw(AssertionError("must not extract")),
    )
    data = b"%PDF-1.4 fetch-only"
    result = extract_job(
        tmp_path,
        AttachmentJob(
            uid="hfa-email-attachment-fetchonly",
            filename="ok.pdf",
            mime_type="application/pdf",
            message_id="m1",
            attachment_id="a1",
            account_email="me@example.com",
        ),
        fetch_bytes=lambda *_args: data,
        fetch_only=True,
    )
    assert result.status == STATUS_FETCHED
    cached = resolve_local_attachment(tmp_path, "hfa-email-attachment-fetchonly", "ok.pdf")
    assert cached is not None
    assert cached.read_bytes() == data


def test_fetch_only_skips_bytes_already_on_disk(tmp_path: Path, monkeypatch) -> None:
    calls = {"n": 0}

    def fetch(*_args):
        calls["n"] += 1
        raise AssertionError("should not refetch")

    uid = "hfa-email-attachment-resume1"
    cache_attachment_bytes(tmp_path, uid, "ok.pdf", b"%PDF-1.4 already")
    result = extract_job(
        tmp_path,
        AttachmentJob(
            uid=uid,
            filename="ok.pdf",
            mime_type="application/pdf",
            message_id="m1",
            attachment_id="a1",
            account_email="me@example.com",
        ),
        fetch_bytes=fetch,
        fetch_only=True,
    )
    assert result.status == STATUS_ALREADY_CACHED
    assert calls["n"] == 0


def test_run_attachment_fetch_does_not_write_cards(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.adapters.base import deterministic_provenance

    uid = "hfa-email-attachment-fetchrun1"
    att = EmailAttachmentCard(
        uid=uid,
        type="email_attachment",
        source=["gmail.attachment"],
        source_id="me@example.com:m1:a1",
        created="2026-03-08",
        updated="2026-03-08",
        summary="scan.pdf",
        gmail_message_id="m1",
        gmail_thread_id="t1",
        attachment_id="a1",
        account_email="me@example.com",
        filename="scan.pdf",
        mime_type="application/pdf",
        size_bytes=20,
    )
    rel = f"EmailAttachments/2026-03/{uid}.md"
    write_card(tmp_path, rel, att, "", provenance=deterministic_provenance(att, "gmail.attachment"))
    monkeypatch.setattr(
        "archive_sync.document_extract.convert_document_to_markdown",
        lambda path, **kwargs: (_ for _ in ()).throw(AssertionError("must not extract")),
    )
    out = run_attachment_fetch(
        tmp_path,
        fetch_bytes=lambda *_args: b"%PDF-1.4 fetched-run",
    )
    assert out["downloaded"] == 1
    assert out["fetch_only"] is True
    cached = resolve_local_attachment(tmp_path, uid, "scan.pdf")
    assert cached is not None
    assert cached.read_bytes() == b"%PDF-1.4 fetched-run"
    fm, body, _ = read_note(tmp_path, rel)
    assert not fm.get("extraction_status")
    assert body == ""

    again = run_attachment_fetch(
        tmp_path,
        fetch_bytes=lambda *_args: (_ for _ in ()).throw(AssertionError("resume")),
    )
    assert again["downloaded"] == 0
    assert again["already_cached"] == 1

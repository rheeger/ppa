"""Email-attachment anydoc extraction. Never calls Firecrawl."""

from __future__ import annotations

from pathlib import Path

from archive_sync.adapters.gmail_messages import GmailMessagesAdapter, _attachment_uid
from archive_sync.attachment_text import (
    ATTACHMENTS_SECTION_HEADING,
    ATTACHMENTS_SECTION_SENTINEL,
    STATUS_EXTRACTED,
    STATUS_MISSING,
    STATUS_NON_DOC,
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
    run_attachment_text_extraction,
    strip_attachments_section,
)
from archive_vault.schema import EmailAttachmentCard
from archive_vault.vault import read_note, write_card


def test_skips_audio_video_archives() -> None:
    assert is_skippable_non_doc("song.mp3", "audio/mpeg") is True
    assert is_skippable_non_doc("clip.mp4", "video/mp4") is True
    assert is_skippable_non_doc("bundle.zip", "application/zip") is True
    assert is_skippable_non_doc("winmail.dat", "application/ms-tnef") is True
    assert is_skippable_non_doc("invite.ics", "text/calendar") is True
    assert is_skippable_non_doc("scan.pdf", "application/pdf") is False
    assert is_skippable_non_doc("photo.jpg", "image/jpeg") is False
    assert is_skippable_non_doc("notes.html", "text/html") is False


def test_message_section_roundtrip() -> None:
    from archive_sync.attachment_text import AttachmentExtraction

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
    assert ATTACHMENTS_SECTION_SENTINEL in merged
    assert "Total $12" in merged
    assert strip_attachments_section(merged) == raw
    preserved = preserve_message_attachments_section("Hello from the email.\n\nMore body.", merged)
    assert "Total $12" in preserved
    replaced = preserve_message_attachments_section(
        merge_message_body("new body", render_attachments_section(
            [
                AttachmentExtraction(
                    status=STATUS_EXTRACTED,
                    text="updated",
                    filename="receipt.pdf",
                    uid="hfa-email-attachment-abc",
                )
            ]
        )),
        merged,
    )
    assert "updated" in replaced
    assert "Total $12" not in replaced


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
    called = {"n": 0}

    def _boom(path: Path):
        called["n"] += 1
        raise AssertionError("should not convert")

    monkeypatch.setattr(
        "archive_sync.attachment_text.convert_document_to_markdown",
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
    assert called["n"] == 0


def test_extract_pdf_uses_convert_document(tmp_path: Path, monkeypatch) -> None:
    uid = "hfa-email-attachment-pdf001"
    data = b"%PDF-1.4 fixture"
    cache_attachment_bytes(tmp_path, uid, "scan.pdf", data)
    monkeypatch.setattr(
        "archive_sync.attachment_text.convert_document_to_markdown",
        lambda path: ("# Scanned page\n\nHello", "anydoc"),
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
        "archive_sync.attachment_text.convert_document_to_markdown",
        lambda path: (f"text for {path.name}", "anydoc"),
    )
    jobs = []
    for i in range(3):
        uid = f"hfa-email-attachment-par{i:03d}"
        cache_attachment_bytes(tmp_path, uid, f"f{i}.pdf", b"%PDF-1.4 " + str(i).encode())
        jobs.append(AttachmentJob(uid=uid, filename=f"f{i}.pdf", mime_type="application/pdf"))
    results = extract_jobs(tmp_path, jobs, workers=3)
    assert [item.status for item in results] == [STATUS_EXTRACTED] * 3
    assert {item.filename for item in results} == {"f0.pdf", "f1.pdf", "f2.pdf"}


def test_run_backfill_writes_message_section(tmp_path: Path, monkeypatch) -> None:
    from archive_sync.adapters.base import deterministic_provenance

    monkeypatch.setattr(
        "archive_sync.attachment_text.convert_document_to_markdown",
        lambda path: ("# Gift agreement excerpt", "anydoc"),
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
    assert ATTACHMENTS_SECTION_HEADING in msg_body
    assert "Gift agreement" in msg_body

    # Incremental: same bytes must not convert again.
    monkeypatch.setattr(
        "archive_sync.attachment_text.convert_document_to_markdown",
        lambda path: (_ for _ in ()).throw(AssertionError("re-ocr")),
    )
    again = run_attachment_text_extraction(tmp_path, dry_run=False)
    assert again["processed"] == 0


def test_gmail_apply_uses_local_file(tmp_vault: Path, monkeypatch) -> None:
    from archive_tests.archive_sync.test_gmail_messages_adapter import _message, _thread

    monkeypatch.setattr(
        "archive_sync.attachment_text.convert_document_to_markdown",
        lambda path: ("# Contract markdown", "anydoc"),
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
    assert ATTACHMENTS_SECTION_HEADING in msg_body
    assert "Contract markdown" in msg_body


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

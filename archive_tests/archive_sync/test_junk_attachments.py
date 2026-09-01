"""Junk email-attachment emit filter and incremental purge."""

from __future__ import annotations

from pathlib import Path

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.junk_attachments import (
    classify_email_attachment,
    run_junk_attachment_purge,
    should_emit_email_attachment,
)
from archive_vault.schema import EmailAttachmentCard, EmailMessageCard, validate_card_strict
from archive_vault.vault import read_note, write_card


def test_should_not_emit_inline_logo_tiny_raster_image001_or_angjd() -> None:
    assert (
        should_emit_email_attachment(filename="logo.png", mime_type="image/png", size_bytes=4000, is_inline=True)
        is False
    )
    assert (
        should_emit_email_attachment(filename="pixel.gif", mime_type="image/gif", size_bytes=800, is_inline=False)
        is False
    )
    assert (
        should_emit_email_attachment(filename="image001.png", mime_type="image/png", size_bytes=12_000, is_inline=False)
        is False
    )
    assert (
        should_emit_email_attachment(
            filename="ANGjdJ9xxxxxxxxxxxxxxxx",
            mime_type="application/octet-stream",
            size_bytes=200,
        )
        is False
    )
    assert (
        should_emit_email_attachment(filename="signature.png", mime_type="image/png", size_bytes=8_000, is_inline=False)
        is False
    )
    assert should_emit_email_attachment(filename="invoice.pdf", mime_type="application/pdf", size_bytes=40_000) is True
    assert (
        should_emit_email_attachment(filename="scan.png", mime_type="image/png", size_bytes=250_000, is_inline=False)
        is True
    )


def test_classify_keeps_extracted_and_documents() -> None:
    action, reason = classify_email_attachment(
        {
            "filename": "image001.png",
            "mime_type": "image/png",
            "size_bytes": 8000,
            "extraction_status": "content_extracted",
            "text_source": "anydoc",
        },
        body="real ocr text",
    )
    assert action == "keep"
    assert reason == "keep_extracted"
    action, reason = classify_email_attachment(
        {"filename": "contract.docx", "mime_type": "application/vnd.openxmlformats", "size_bytes": 9000}
    )
    assert action == "keep"
    assert reason == "keep_document"


def _att(uid: str, filename: str, **kwargs) -> EmailAttachmentCard:
    return EmailAttachmentCard(
        uid=uid,
        type="email_attachment",
        source=["gmail.attachment"],
        source_id=uid,
        created="2026-01-01",
        updated="2026-01-01",
        gmail_message_id="m1",
        gmail_thread_id="t1",
        attachment_id=uid[-8:],
        filename=filename,
        message="[[hfa-email-message-parent1parent]]",
        mime_type=str(kwargs.get("mime_type") or "image/png"),
        size_bytes=int(kwargs.get("size_bytes") or 4000),
        is_inline=bool(kwargs.get("is_inline", False)),
    )


def test_purge_deletes_junk_and_keeps_pdf(tmp_path: Path, monkeypatch) -> None:
    vault = tmp_path / "vault"
    (vault / "EmailAttachments" / "2026-01").mkdir(parents=True)
    (vault / "Email" / "2026-01").mkdir(parents=True)
    (vault / "Attachments" / "hfa-email-attachment-junk111junk111").mkdir(parents=True)
    monkeypatch.setenv("PPA_FILE_IDENTITY_DB", str(tmp_path / "id.sqlite"))

    junk = _att(
        "hfa-email-attachment-junk111junk111",
        "image001.png",
        mime_type="image/png",
        size_bytes=9000,
    )
    keep = EmailAttachmentCard(
        uid="hfa-email-attachment-keep111keep111",
        type="email_attachment",
        source=["gmail.attachment"],
        source_id="keep",
        created="2026-01-01",
        updated="2026-01-01",
        gmail_message_id="m1",
        gmail_thread_id="t1",
        attachment_id="keep1111",
        filename="invoice.pdf",
        mime_type="application/pdf",
        size_bytes=40_000,
        message="[[hfa-email-message-parent1parent]]",
    )
    parent = EmailMessageCard(
        uid="hfa-email-message-parent1parent",
        type="email_message",
        source=["gmail.message"],
        source_id="m1",
        created="2026-01-01",
        updated="2026-01-01",
        gmail_message_id="m1",
        gmail_thread_id="t1",
        attachments=[
            "[[hfa-email-attachment-junk111junk111]]",
            "[[hfa-email-attachment-keep111keep111]]",
        ],
        has_attachments=True,
    )
    write_card(
        vault,
        "EmailAttachments/2026-01/hfa-email-attachment-junk111junk111.md",
        validate_card_strict(junk.model_dump()),
        "",
        deterministic_provenance(junk, "test"),
    )
    write_card(
        vault,
        "EmailAttachments/2026-01/hfa-email-attachment-keep111keep111.md",
        validate_card_strict(keep.model_dump()),
        "",
        deterministic_provenance(keep, "test"),
    )
    write_card(
        vault,
        "Email/2026-01/hfa-email-message-parent1parent.md",
        validate_card_strict(parent.model_dump()),
        "- [[hfa-email-attachment-junk111junk111]] image001.png\n- [[hfa-email-attachment-keep111keep111]] invoice.pdf",
        deterministic_provenance(parent, "test"),
    )
    (vault / "Attachments" / "hfa-email-attachment-junk111junk111" / "image001.png").write_bytes(b"x")

    from archive_cli.vault_cache import VaultScanCache

    VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    out = run_junk_attachment_purge(vault, dry_run=False, store=None)
    assert out["purged"] == 1
    assert not (vault / "EmailAttachments" / "2026-01" / "hfa-email-attachment-junk111junk111.md").exists()
    assert (vault / "EmailAttachments" / "2026-01" / "hfa-email-attachment-keep111keep111.md").exists()
    assert not (vault / "Attachments" / "hfa-email-attachment-junk111junk111").exists()
    fm, _, _ = read_note(vault, "Email/2026-01/hfa-email-message-parent1parent.md")
    assert fm["attachments"] == ["[[hfa-email-attachment-keep111keep111]]"]

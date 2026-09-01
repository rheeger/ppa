"""Same-bytes duplicate wikilinks for documents and email attachments."""

from __future__ import annotations

from pathlib import Path

from archive_sync.adapters.base import deterministic_provenance
from archive_sync.file_identity import (
    FileIdentityIndex,
    register_ingested_file,
    run_file_duplicate_linking,
    source_sha_from_frontmatter,
)
from archive_vault.schema import DocumentCard, EmailAttachmentCard, validate_card_strict
from archive_vault.vault import read_note, write_card


def _doc(uid: str, sha: str = "", filename: str = "Heeger.pdf") -> DocumentCard:
    return DocumentCard(
        uid=uid,
        type="document",
        source=["file-library"],
        source_id=uid,
        created="2026-01-01",
        updated="2026-01-01",
        library_root="documents",
        relative_path=filename,
        filename=filename,
        content_sha=sha,
    )


def _att(uid: str, sha: str = "", filename: str = "Heeger.pdf") -> EmailAttachmentCard:
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
        extracted_text_sha=sha,
        content_sha=sha,
    )


def _write(vault: Path, rel: str, card, body: str = "body") -> None:
    write_card(vault, rel, validate_card_strict(card.model_dump()), body, deterministic_provenance(card, "test"))


def test_source_sha_prefers_content_sha() -> None:
    assert source_sha_from_frontmatter({"content_sha": "a" * 64, "extracted_text_sha": "b" * 64}) == "a" * 64
    assert source_sha_from_frontmatter({"extracted_text_sha": "b" * 64}) == "b" * 64
    assert source_sha_from_frontmatter({"content_sha": "short"}) == ""


def test_same_bytes_get_bidirectional_links(tmp_path: Path, monkeypatch) -> None:
    vault = tmp_path / "vault"
    (vault / "Documents" / "2026-01").mkdir(parents=True)
    (vault / "EmailAttachments" / "2026-01").mkdir(parents=True)
    src = tmp_path / "src"
    src.mkdir()
    payload = b"%PDF-1.4 same-bytes\n"
    (src / "a.pdf").write_bytes(payload)
    (src / "b.pdf").write_bytes(payload)
    (src / "c.pdf").write_bytes(b"%PDF-1.4 different\n")

    monkeypatch.setenv("PPA_FILE_IDENTITY_DB", str(tmp_path / "id.sqlite"))
    monkeypatch.setenv("PPA_ANYDOC_EXTRACT_CACHE", str(tmp_path / "extract.sqlite"))

    from archive_sync.llm_enrichment import document_text_extractor as dte

    monkeypatch.setattr(
        dte,
        "resolve_source_file",
        lambda root, rel: src / Path(rel).name if (src / Path(rel).name).is_file() else None,
    )

    a = _doc("hfa-document-aaa111aaa111", filename="a.pdf")
    b = _doc("hfa-document-bbb222bbb222", filename="b.pdf")
    c = _doc("hfa-document-ccc333ccc333", filename="c.pdf")
    _write(vault, "Documents/2026-01/hfa-document-aaa111aaa111.md", a)
    _write(vault, "Documents/2026-01/hfa-document-bbb222bbb222.md", b)
    _write(vault, "Documents/2026-01/hfa-document-ccc333ccc333.md", c)

    from archive_cli.vault_cache import VaultScanCache

    VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    out = run_file_duplicate_linking(vault, dry_run=False, identity_db=tmp_path / "id.sqlite")
    assert out["groups"] == 1
    assert out["cards_linked"] == 2

    fm_a, _, _ = read_note(vault, "Documents/2026-01/hfa-document-aaa111aaa111.md")
    fm_b, _, _ = read_note(vault, "Documents/2026-01/hfa-document-bbb222bbb222.md")
    fm_c, _, _ = read_note(vault, "Documents/2026-01/hfa-document-ccc333ccc333.md")
    assert "[[hfa-document-bbb222bbb222]]" in (fm_a.get("duplicates") or [])
    assert "[[hfa-document-aaa111aaa111]]" in (fm_b.get("duplicates") or [])
    assert not (fm_c.get("duplicates") or [])


def test_third_copy_adds_links(tmp_path: Path, monkeypatch) -> None:
    vault = tmp_path / "vault"
    (vault / "Documents" / "2026-01").mkdir(parents=True)
    (vault / "EmailAttachments" / "2026-01").mkdir(parents=True)
    monkeypatch.setenv("PPA_FILE_IDENTITY_DB", str(tmp_path / "id.sqlite"))
    sha = "ab" * 32
    a = _doc("hfa-document-aaa111aaa111", sha=sha)
    b = _att("hfa-email-attachment-bbb222bbb222", sha=sha)
    _write(vault, "Documents/2026-01/hfa-document-aaa111aaa111.md", a)
    _write(vault, "EmailAttachments/2026-01/hfa-email-attachment-bbb222bbb222.md", b)
    identity = FileIdentityIndex(tmp_path / "id.sqlite")
    identity.put(sha, a.uid, "Documents/2026-01/hfa-document-aaa111aaa111.md")
    identity.put(sha, b.uid, "EmailAttachments/2026-01/hfa-email-attachment-bbb222bbb222.md")
    _write_identity = register_ingested_file(
        vault,
        uid=a.uid,
        rel_path="Documents/2026-01/hfa-document-aaa111aaa111.md",
        sha256=sha,
        identity=identity,
    )
    assert b.uid in _write_identity
    c = _att("hfa-email-attachment-ccc333ccc333", sha=sha)
    _write(vault, "EmailAttachments/2026-01/hfa-email-attachment-ccc333ccc333.md", c)
    peers = register_ingested_file(
        vault,
        uid=c.uid,
        rel_path="EmailAttachments/2026-01/hfa-email-attachment-ccc333ccc333.md",
        sha256=sha,
        identity=identity,
    )
    assert set(peers) == {a.uid, b.uid}
    fm_a, _, _ = read_note(vault, "Documents/2026-01/hfa-document-aaa111aaa111.md")
    assert "[[hfa-email-attachment-ccc333ccc333]]" in (fm_a.get("duplicates") or [])


def test_incremental_links_missing_dups_without_stamping_unique(tmp_path: Path, monkeypatch) -> None:
    vault = tmp_path / "vault"
    (vault / "Documents" / "2026-01").mkdir(parents=True)
    monkeypatch.setenv("PPA_FILE_IDENTITY_DB", str(tmp_path / "id.sqlite"))
    sha = "ef" * 32
    a = _doc("hfa-document-aaa111aaa111", sha=sha)
    b = _doc("hfa-document-bbb222bbb222", sha=sha)
    unique = _doc("hfa-document-ccc333ccc333", sha="", filename="solo.pdf")
    _write(vault, "Documents/2026-01/hfa-document-aaa111aaa111.md", a)
    _write(vault, "Documents/2026-01/hfa-document-bbb222bbb222.md", b)
    _write(vault, "Documents/2026-01/hfa-document-ccc333ccc333.md", unique)

    src = tmp_path / "src"
    src.mkdir()
    (src / "solo.pdf").write_bytes(b"%PDF-1.4 unique-bytes\n")
    from archive_sync.llm_enrichment import document_text_extractor as dte

    monkeypatch.setattr(
        dte,
        "resolve_source_file",
        lambda root, rel: src / Path(rel).name if (src / Path(rel).name).is_file() else None,
    )
    from archive_cli.vault_cache import VaultScanCache

    VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    out = run_file_duplicate_linking(vault, dry_run=False, identity_db=tmp_path / "id.sqlite", incremental=True)
    assert out["groups"] == 1
    assert out["cards_linked"] == 2
    fm_unique, _, _ = read_note(vault, "Documents/2026-01/hfa-document-ccc333ccc333.md")
    assert not (fm_unique.get("content_sha") or "")
    assert unique.uid not in out["dirty_uids"]


def test_junk_purged_uid_is_not_linked(tmp_path: Path) -> None:
    identity = FileIdentityIndex(tmp_path / "id.sqlite")
    sha = "cd" * 32
    identity.put(sha, "hfa-email-attachment-keep111keep111", "EmailAttachments/x.md")
    identity.put(sha, "hfa-email-attachment-junk222junk222", "EmailAttachments/y.md")
    identity.drop_uids(["hfa-email-attachment-junk222junk222"])
    assert identity.uids_for_sha(sha) == ["hfa-email-attachment-keep111keep111"]

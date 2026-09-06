"""P07-D: encrypted backup restore into a new root, then rebuilt evidence."""

from __future__ import annotations

import tarfile
import uuid
from pathlib import Path

import pytest

from archive_engine.corrections import CorrectionCommandRequest, execute_correction_command
from archive_engine.errors import CapabilityUnavailableError
from archive_engine.recovery import (
    ENCRYPTION_FORMAT,
    ENCRYPTION_TOOL,
    ActiveRootRestoreError,
    EncryptionUnavailableError,
    MissingBackupError,
    TamperError,
    WrongKeyError,
    create_encrypted_bundle,
    encryption_capability,
    restore_encrypted_bundle,
    snapshot_tree_hashes,
    verify_encrypted_bundle,
)
from archive_sync.adapters.base import deterministic_provenance
from archive_vault.paths import PathEscapeError
from archive_vault.schema import FinanceCard, PersonCard
from archive_vault.vault import read_note, read_note_by_uid, write_card

DINNER_UID = "hfa-finance-p07dinner02"
DINNER_REL = "Finance/2026-09/hfa-finance-p07dinner02.md"
PERSON_UID = "hfa-person-p07dname001"
PERSON_REL = "People/p07d-name.md"
WINNER_UID = "hfa-person-p07dwin0001"
LOSER_UID = "hfa-person-p07dlose0001"
PASSPHRASE = "p07d-fixture-only"


def _vault(tmp_path: Path) -> Path:
    vault = tmp_path / "source-vault"
    (vault / "People").mkdir(parents=True)
    (vault / "Finance" / "2026-09").mkdir(parents=True)
    (vault / "Attachments").mkdir()
    meta = vault / "_meta"
    meta.mkdir()
    for name, payload in (
        ("identity-map.json", "{}"),
        ("sync-state.json", "{}"),
        ("own-emails.json", "[]"),
        ("nicknames.json", "{}"),
        ("ppa-config.json", "{}"),
        ("llm-config.json", '{"primary": {"provider": "gemini", "model": "fixture"}}'),
    ):
        (meta / name).write_text(payload + "\n", encoding="utf-8")
    (vault / "Attachments" / "receipt.bin").write_bytes(b"p07d-attachment")
    (meta / "vault-scan-cache.sqlite3").write_bytes(b"stale-cache")
    (vault / ".env").write_text("OPENAI_API_KEY=sk-not-for-backup\n", encoding="utf-8")
    return vault


def _write_dinner(vault: Path, amount: float) -> None:
    card = FinanceCard(
        uid=DINNER_UID,
        type="finance",
        source=["amex"],
        source_id="amex:p07d-dinner",
        created="2026-09-01",
        updated="2026-09-01",
        summary="P07-D Dinner",
        amount=amount,
        currency="USD",
        counterparty="Restaurant",
        note="synthetic restore dinner",
    )
    write_card(vault, DINNER_REL, card, body="dinner", provenance=deterministic_provenance(card, "amex"))


def _write_people(vault: Path) -> None:
    winner = PersonCard(
        uid=WINNER_UID,
        type="person",
        source=["contacts.apple"],
        source_id="alex@p07d.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary="Alex Winner",
        first_name="Alex",
        last_name="Winner",
        emails=["alex@p07d.test"],
    )
    loser = PersonCard(
        uid=LOSER_UID,
        type="person",
        source=["contacts.apple"],
        source_id="blake@p07d.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary="Blake Loser",
        first_name="Blake",
        last_name="Loser",
        emails=["blake@p07d.test"],
    )
    named = PersonCard(
        uid=PERSON_UID,
        type="person",
        source=["contacts.apple"],
        source_id="p07dname@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary="Casey Name",
        first_name="Casey",
        last_name="Name",
        emails=["p07dname@example.test"],
    )
    write_card(vault, "People/alex-winner.md", winner, body="alex", provenance=deterministic_provenance(winner, "contacts.apple"))
    write_card(vault, "People/blake-loser.md", loser, body="blake", provenance=deterministic_provenance(loser, "contacts.apple"))
    write_card(vault, PERSON_REL, named, body="casey", provenance=deterministic_provenance(named, "contacts.apple"))


def _prepare_story(vault: Path) -> None:
    _write_dinner(vault, 42.0)
    _write_people(vault)
    execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="apply_override",
            uid=DINNER_UID,
            field="amount",
            value=18.5,
            author="p07d",
            reason="paper receipt",
            rel_path=DINNER_REL,
        ),
    )
    execute_correction_command(
        vault,
        CorrectionCommandRequest(
            action="merge_identities",
            winner_uid=WINNER_UID,
            loser_uid=LOSER_UID,
            author="p07d",
            reason="fixture merge",
        ),
    )


def _openssl_encrypt(src: Path, dest: Path, passphrase: str) -> None:
    import os
    import subprocess
    import tempfile

    handle = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    handle.write(passphrase)
    handle.close()
    try:
        subprocess.run(
            [
                "openssl",
                "enc",
                "-aes-256-cbc",
                "-pbkdf2",
                "-salt",
                "-pass",
                f"file:{handle.name}",
                "-in",
                str(src),
                "-out",
                str(dest),
            ],
            check=True,
        )
    finally:
        os.unlink(handle.name)


def test_encryption_capability_records_openssl() -> None:
    cap = encryption_capability()
    assert cap["tool"] == ENCRYPTION_TOOL
    assert cap["format"] == ENCRYPTION_FORMAT
    assert cap["available"] is True
    assert cap["openssl_path"]


def test_missing_openssl_does_not_write_plaintext(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    backup = tmp_path / "backups"
    backup.mkdir()
    monkeypatch.setattr("archive_engine.recovery.shutil.which", lambda name: None)
    with pytest.raises((EncryptionUnavailableError, CapabilityUnavailableError), match="openssl"):
        create_encrypted_bundle(vault, backup, passphrase=PASSPHRASE)
    assert not list(backup.rglob("*.tar"))
    assert not list(backup.rglob("*.md"))
    assert not list(backup.rglob("*.enc"))


def test_roundtrip_preserves_correction_identity_and_attachment(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _prepare_story(vault)
    assert read_note(vault, DINNER_REL)[0]["amount"] == 18.5
    source_hashes = snapshot_tree_hashes(vault)
    backup = tmp_path / "backups"
    created = create_encrypted_bundle(vault, backup, passphrase=PASSPHRASE)
    assert created["encryption_tool"] == ENCRYPTION_TOOL
    assert Path(created["archive"]).is_file()
    assert not list(backup.rglob("*.tar"))
    dest = tmp_path / "restored"
    receipt = restore_encrypted_bundle(
        dest=dest,
        passphrase=PASSPHRASE,
        backup_base=backup,
        active_root=vault,
        source_vault=vault,
        source_hashes=source_hashes,
    )
    assert receipt.active_root_untouched is True
    assert snapshot_tree_hashes(vault) == source_hashes
    assert read_note(dest, DINNER_REL)[0]["amount"] == 18.5
    loser = read_note_by_uid(dest, LOSER_UID)
    assert loser is not None
    assert loser[1].get("redirect_to") == WINNER_UID
    assert (dest / "Attachments" / "receipt.bin").read_bytes() == b"p07d-attachment"
    assert not (dest / ".env").exists()
    assert not (dest / "_meta" / "vault-scan-cache.sqlite3").exists()
    assert receipt.checkpoint.get("status") == "available"
    assert receipt.scale_profile == "fixture"
    assert "production recovery claim" in receipt.notes


def test_wrong_key_fails(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    backup = tmp_path / "backups"
    create_encrypted_bundle(vault, backup, passphrase=PASSPHRASE)
    with pytest.raises(WrongKeyError):
        restore_encrypted_bundle(dest=tmp_path / "wrong", passphrase="not-the-key", backup_base=backup, active_root=vault)


def test_tamper_fails(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    backup = tmp_path / "backups"
    created = create_encrypted_bundle(vault, backup, passphrase=PASSPHRASE)
    archive = Path(created["archive"])
    data = bytearray(archive.read_bytes())
    data[-8] ^= 0xFF
    archive.write_bytes(data)
    with pytest.raises(TamperError):
        restore_encrypted_bundle(dest=tmp_path / "tamper", passphrase=PASSPHRASE, backup_base=backup, active_root=vault)


def test_missing_file_fails(tmp_path: Path) -> None:
    with pytest.raises(MissingBackupError):
        verify_encrypted_bundle(backup_base=tmp_path / "missing")


def test_path_escape_rejected(tmp_path: Path) -> None:
    tar_path = tmp_path / "evil.tar"
    with tarfile.open(tar_path, "w") as tf:
        escape = tmp_path / "payload.md"
        escape.write_text("nope\n", encoding="utf-8")
        tf.add(escape, arcname="../outside.md")
        info = tarfile.TarInfo(name="People/leak.md")
        info.type = tarfile.SYMTYPE
        info.linkname = "/etc/passwd"
        tf.addfile(info)
    enc = tmp_path / "bundle" / "ppa-backup.tar.enc"
    enc.parent.mkdir()
    _openssl_encrypt(tar_path, enc, PASSPHRASE)
    digest = __import__("hashlib").sha256(enc.read_bytes()).hexdigest()
    (enc.parent / "ppa-backup.tar.enc.sha256").write_text(f"{digest}  {enc}\n", encoding="utf-8")
    dest = tmp_path / "escaped"
    with pytest.raises(PathEscapeError):
        restore_encrypted_bundle(dest=dest, passphrase=PASSPHRASE, archive_file=enc, active_root=tmp_path / "active-other")
    assert not (tmp_path / "outside.md").exists()
    if dest.exists():
        assert not (dest / "People" / "leak.md").exists()


def test_active_root_refused(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    _write_dinner(vault, 42.0)
    backup = tmp_path / "backups"
    create_encrypted_bundle(vault, backup, passphrase=PASSPHRASE)
    with pytest.raises(ActiveRootRestoreError):
        restore_encrypted_bundle(dest=vault, passphrase=PASSPHRASE, backup_base=backup, active_root=vault)


@pytest.mark.integration
def test_no_index_rebuild_queries_corrected_story(
    tmp_path: Path,
    pgvector_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    vault = _vault(tmp_path)
    _prepare_story(vault)
    backup = tmp_path / "backups"
    create_encrypted_bundle(vault, backup, passphrase=PASSPHRASE)
    dest = tmp_path / "restored"
    receipt = restore_encrypted_bundle(
        dest=dest,
        passphrase=PASSPHRASE,
        backup_base=backup,
        active_root=vault,
        source_vault=vault,
        strip_reconstructible=True,
    )
    assert not (dest / "_meta" / "vault-scan-cache.sqlite3").exists()
    schema = f"p07d_{uuid.uuid4().hex[:8]}"
    serving = tmp_path / "serving"
    monkeypatch.setenv("PPA_PATH", str(dest))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    monkeypatch.setenv("PPA_EMBEDDING_PROVIDER", "hash")
    monkeypatch.setenv("PPA_EMBEDDING_MODEL", "archive-hash-dev")
    monkeypatch.setenv("PPA_EMBEDDING_VERSION", "1")
    monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(serving))
    monkeypatch.setenv("PPA_BOOTSTRAP_FORCE", "1")
    monkeypatch.setenv("PPA_ENGINE", "rust")
    from archive_cli.commands.recovery import activate_restored_archive

    activated = activate_restored_archive(
        vault=dest,
        logger=__import__("logging").getLogger("ppa.test.p07d"),
        receipt=receipt,
        query_uid=DINNER_UID,
        query_text="P07-D Dinner",
    )
    content = str((activated.get("query") or {}).get("read") or {})
    assert "18.5" in content or "18.50" in content
    read_payload = (activated.get("query") or {}).get("read") or {}
    assert read_payload.get("found") is True
    search_rows = ((activated.get("query") or {}).get("search") or {}).get("rows") or []
    uids = [str(row.get("uid") or row.get("card_uid") or "") for row in search_rows]
    assert DINNER_UID in uids or any("Dinner" in str(row) for row in search_rows)

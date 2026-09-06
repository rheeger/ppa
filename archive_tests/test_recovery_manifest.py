"""P07-A recoverability inventory and versioned manifest tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_engine.recovery_manifest import (
    CHECKPOINT_UNAVAILABLE_REASON,
    MANIFEST_FORMAT_VERSION,
    STATE_OWNERS,
    ArtifactPresence,
    CorruptRequiredStateError,
    IncompatibleManifestError,
    MissingRequiredStateError,
    RecoveryClass,
    SecretMaterialError,
    UnknownRequiredStateError,
    generate_manifest,
    inventory_state_owners,
    read_manifest,
    validate_manifest,
    write_manifest,
)

REQUIRED_META = (
    "identity-map.json",
    "sync-state.json",
    "own-emails.json",
    "nicknames.json",
    "ppa-config.json",
    "llm-config.json",
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _synthetic_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "archive"
    (vault / "People").mkdir(parents=True)
    (vault / "Attachments").mkdir()
    (vault / "_meta").mkdir()
    (vault / "People" / "jane-smith.md").write_text(
        "---\nuid: hfa-person-aaaabbbbcccc\ntype: person\nsummary: Jane Smith\n---\nJane\n",
        encoding="utf-8",
    )
    (vault / "Attachments" / "receipt.pdf").write_bytes(b"%PDF-1.4 fixture\n")
    _write_json(vault / "_meta" / "identity-map.json", {"email:jane@example.com": "[[Jane Smith]]"})
    _write_json(vault / "_meta" / "sync-state.json", {"gmail": {"cursor": "hist-1"}})
    _write_json(vault / "_meta" / "own-emails.json", ["jane@example.com"])
    _write_json(vault / "_meta" / "nicknames.json", {"robert": ["rob"]})
    _write_json(vault / "_meta" / "ppa-config.json", {"finance_min_amount": 20.0})
    _write_json(
        vault / "_meta" / "llm-config.json",
        {"primary": {"provider": "gemini", "model": "gemini-2.0-flash-lite"}},
    )
    _write_json(vault / "_meta" / "enrichment-log.json", [])
    (vault / "_meta" / "credentials.json").write_text(
        json.dumps({"refresh_token": "super-secret-refresh"}),
        encoding="utf-8",
    )
    (vault / ".env").write_text("OPENAI_API_KEY=sk-test-not-for-manifest\n", encoding="utf-8")
    return vault


def test_inventory_classifies_every_owner() -> None:
    owners = inventory_state_owners()
    assert owners is STATE_OWNERS
    assert owners
    owner_ids = [owner.owner_id for owner in owners]
    assert len(owner_ids) == len(set(owner_ids))
    for owner in owners:
        assert owner.classification in RecoveryClass
        assert owner.presence in ArtifactPresence
        assert owner.location
        assert owner.code_owner
        assert owner.recovery_strategy
    required_files = [owner for owner in owners if owner.presence is ArtifactPresence.REQUIRED]
    assert {owner.location for owner in required_files if owner.location.startswith("_meta/")} == {
        f"_meta/{name}" for name in REQUIRED_META
    }
    logical_decision = {
        owner.owner_id
        for owner in owners
        if owner.presence is ArtifactPresence.LOGICAL and owner.classification is RecoveryClass.DECISION_CRITICAL
    }
    assert logical_decision >= {
        "pg.card_corpus_state",
        "pg.email_corpus_decisions",
        "pg.link_decisions",
        "pg.review_actions",
        "pg.source_updater_state",
    }
    secrets = {owner.owner_id for owner in owners if owner.classification is RecoveryClass.SECRET_MATERIAL}
    assert secrets >= {"secret.oauth-tokens", "secret.llm-keys", "secret.backup-passphrase"}
    assert "vault.meta.canonical-decisions" in owner_ids
    assert "vault.meta.change-journal" in owner_ids


def test_generate_and_validate_roundtrip_on_fixture_files(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    manifest = generate_manifest(vault)
    dest = tmp_path / "recovery-manifest.json"
    write_manifest(manifest, dest)
    loaded = read_manifest(dest)

    assert loaded["format_version"] == MANIFEST_FORMAT_VERSION
    assert loaded["archive_id"]["status"] == "unavailable"
    assert loaded["archive_id"]["reason"] == CHECKPOINT_UNAVAILABLE_REASON
    assert loaded["archive_id"]["value"] is None
    assert loaded["checkpoint"] == loaded["archive_id"]
    assert loaded["missing_state_policy"] == "reject_required"

    paths = {item["rel_path"] for item in loaded["artifacts"]}
    assert "People/jane-smith.md" in paths
    assert "Attachments/receipt.pdf" in paths
    assert "_meta/identity-map.json" in paths
    assert "_meta/credentials.json" not in paths
    assert ".env" not in paths
    jane = next(item for item in loaded["artifacts"] if item["rel_path"] == "People/jane-smith.md")
    assert jane["classification"] == "canonical"
    assert jane["presence"] == "required"
    assert jane["size"] == (vault / "People" / "jane-smith.md").stat().st_size
    assert len(jane["sha256"]) == 64

    excluded = {item["rel_path"] for item in loaded["exclusions"]}
    assert excluded == {"_meta/credentials.json", ".env"}
    for item in loaded["exclusions"]:
        assert item["sha256"] is None
        assert item["size"] is None
        assert "super-secret" not in json.dumps(item)
        assert "sk-test" not in json.dumps(item)
    serialized = dest.read_text(encoding="utf-8")
    assert "super-secret-refresh" not in serialized
    assert "sk-test-not-for-manifest" not in serialized

    validate_manifest(loaded, vault)


def test_missing_required_state_is_rejected(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    (vault / "_meta" / "identity-map.json").unlink()
    with pytest.raises(MissingRequiredStateError, match="identity-map"):
        generate_manifest(vault)


def test_corrupt_required_state_is_rejected(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    manifest = generate_manifest(vault)
    (vault / "People" / "jane-smith.md").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(CorruptRequiredStateError, match="jane-smith"):
        validate_manifest(manifest, vault)


def test_corrupt_required_json_is_rejected(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    (vault / "_meta" / "sync-state.json").write_text("{not-json", encoding="utf-8")
    with pytest.raises(CorruptRequiredStateError, match="sync-state"):
        generate_manifest(vault)


def test_unknown_required_meta_is_rejected(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    _write_json(vault / "_meta" / "mystery-decisions.json", {"uid": "x"})
    with pytest.raises(UnknownRequiredStateError, match="mystery-decisions"):
        generate_manifest(vault)


def test_unknown_required_file_after_manifest_is_rejected(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    manifest = generate_manifest(vault)
    (vault / "Finance").mkdir()
    (vault / "Finance" / "dinner.md").write_text("---\ntype: finance\n---\n", encoding="utf-8")
    with pytest.raises(UnknownRequiredStateError, match="dinner"):
        validate_manifest(manifest, vault)


def test_secret_json_keys_are_rejected(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    _write_json(
        vault / "_meta" / "llm-config.json",
        {"primary": {"provider": "gemini", "api_key": "should-not-be-here"}},
    )
    with pytest.raises(SecretMaterialError, match="api_key"):
        generate_manifest(vault)


def test_incompatible_manifest_version_is_rejected(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    manifest = generate_manifest(vault)
    manifest["format_version"] = 99
    with pytest.raises(IncompatibleManifestError, match="version"):
        validate_manifest(manifest, vault)


def test_hygiene_rollback_kit_is_decision_critical(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    kit = vault / "_artifacts" / "hygiene-rollback-kit" / "run-1"
    kit.mkdir(parents=True)
    (kit / "Email/spam.md").parent.mkdir(parents=True)
    (kit / "Email/spam.md").write_text("preimage\n", encoding="utf-8")
    manifest = generate_manifest(vault)
    record = next(
        item for item in manifest["artifacts"] if item["rel_path"].endswith("Email/spam.md")
    )
    assert record["classification"] == "decision_critical"
    assert record["presence"] == "required"
    validate_manifest(manifest, vault)


def test_sqlite_wal_is_disposable_not_unknown(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    (vault / "_meta" / "vault-scan-cache.sqlite3-wal").write_bytes(b"wal")
    manifest = generate_manifest(vault)
    wal = next(item for item in manifest["artifacts"] if item["rel_path"].endswith("-wal"))
    assert wal["classification"] == "disposable"
    validate_manifest(manifest, vault)


def test_journal_checkpoint_binds_when_present(tmp_path: Path) -> None:
    from archive_engine.changes import CHECKPOINT_REASON
    from archive_vault.provenance import ProvenanceEntry
    from archive_vault.schema import PersonCard
    from archive_vault.vault import write_card

    vault = _synthetic_vault(tmp_path)
    card = PersonCard(
        uid="hfa-person-p07bind0001",
        type="person",
        source=["acceptance.p07b"],
        source_id="p07bind@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary="P07 Bind",
        first_name="P07",
        last_name="Bind",
    )
    write_card(
        vault,
        "People/p07-bind.md",
        card,
        body="bind",
        provenance={
            "summary": ProvenanceEntry("acceptance.p07b", "2026-09-06", "deterministic"),
            "first_name": ProvenanceEntry("acceptance.p07b", "2026-09-06", "deterministic"),
            "last_name": ProvenanceEntry("acceptance.p07b", "2026-09-06", "deterministic"),
        },
    )
    manifest = generate_manifest(vault)
    assert manifest["archive_id"]["status"] == "available"
    assert manifest["archive_id"]["reason"] == CHECKPOINT_REASON
    assert manifest["archive_id"]["value"]
    assert manifest["checkpoint"]["status"] == "available"
    assert manifest["checkpoint"]["reason"] == CHECKPOINT_REASON
    assert manifest["checkpoint"]["value"]["high_watermark"] >= 1
    journal = next(item for item in manifest["artifacts"] if item["rel_path"] == "_meta/change-journal.sqlite3")
    assert journal["classification"] == "decision_critical"
    validate_manifest(manifest, vault)


def test_exclusion_cannot_carry_hashes(tmp_path: Path) -> None:
    vault = _synthetic_vault(tmp_path)
    manifest = generate_manifest(vault)
    manifest["exclusions"][0]["sha256"] = "abc"
    with pytest.raises(SecretMaterialError, match="leaked"):
        validate_manifest(manifest, vault)

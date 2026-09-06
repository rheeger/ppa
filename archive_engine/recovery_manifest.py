"""Versioned recoverability inventory and archive manifest (P07-A).

Enumerates a synthetic archive's canonical and decision-critical files, hashes
them, and rejects missing, corrupt, or unknown required state. Credential
material is excluded from the payload. Archive identity and journal checkpoint
are placeholders until P02 integration.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

MANIFEST_FORMAT_NAME = "ppa.recovery_manifest"
MANIFEST_FORMAT_VERSION = 1
CHECKPOINT_UNAVAILABLE_REASON = "awaiting_p02_journal_integration"
MISSING_STATE_POLICY = "reject_required"

# Inspected card-family roots from archive_vault/card_contracts.py plus
# ppa-init-vault.sh (Attachments is excluded from card walks but is canonical).
CANONICAL_FAMILY_PREFIXES: tuple[str, ...] = (
    "People",
    "Finance",
    "Medical",
    "Vaccinations",
    "EmailThreads",
    "Email",
    "EmailAttachments",
    "IMessageThreads",
    "IMessage",
    "IMessageAttachments",
    "BeeperThreads",
    "Beeper",
    "BeeperAttachments",
    "Calendar",
    "Photos",
    "Documents",
    "MeetingTranscripts",
    "GitRepos",
    "GitCommits",
    "GitThreads",
    "GitMessages",
    "Transactions",
    "Entities",
    "Knowledge",
    "Agent",
    "Attachments",
)

HYGIENE_ROLLBACK_PREFIX = "_artifacts/hygiene-rollback-kit"

SECRET_BASENAMES = frozenset(
    {
        "credentials.json",
        "client_secret.json",
        "gcp-oauth.keys.json",
        "gemini_key.txt",
        ".env",
    }
)
SECRET_NAME_MARKERS = (
    "passphrase",
    "refresh-token",
    "refresh_token",
    "service-account",
    "service_account",
)
SECRET_SUFFIXES = (".pem", ".key", ".p12", ".pfx")
SECRET_JSON_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "api-key",
        "refresh_token",
        "access_token",
        "id_token",
        "client_secret",
        "password",
        "passphrase",
        "private_key",
        "openai_api_key",
        "gemini_api_key",
        "authorization",
    }
)


class RecoveryClass(str, Enum):
    CANONICAL = "canonical"
    DECISION_CRITICAL = "decision_critical"
    RECONSTRUCTIBLE = "reconstructible"
    SECRET_REFERENCE = "secret_reference"
    SECRET_MATERIAL = "secret_material"
    DISPOSABLE = "disposable"


class ArtifactPresence(str, Enum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    EXCLUDED = "excluded"
    LOGICAL = "logical"


class RecoveryManifestError(Exception):
    """Fail-closed recovery inventory or validation error."""


class MissingRequiredStateError(RecoveryManifestError):
    """A catalogued required artifact is absent."""


class CorruptRequiredStateError(RecoveryManifestError):
    """A required artifact failed hash or parse verification."""


class UnknownRequiredStateError(RecoveryManifestError):
    """A vault file looks required but has no recovery classification."""


class SecretMaterialError(RecoveryManifestError):
    """Credential material would have entered the ordinary manifest."""


class IncompatibleManifestError(RecoveryManifestError):
    """Manifest format or required versions cannot be read."""


@dataclass(frozen=True, slots=True)
class StateOwner:
    owner_id: str
    classification: RecoveryClass
    presence: ArtifactPresence
    location: str
    code_owner: str
    recovery_strategy: str
    notes: str = ""


@dataclass(frozen=True, slots=True)
class PathClass:
    classification: RecoveryClass
    presence: ArtifactPresence
    owner_id: str


_META_FILES: dict[str, PathClass] = {
    "identity-map.json": PathClass(
        RecoveryClass.DECISION_CRITICAL, ArtifactPresence.REQUIRED, "vault.meta.identity-map"
    ),
    "sync-state.json": PathClass(
        RecoveryClass.DECISION_CRITICAL, ArtifactPresence.REQUIRED, "vault.meta.sync-state"
    ),
    "own-emails.json": PathClass(
        RecoveryClass.DECISION_CRITICAL, ArtifactPresence.REQUIRED, "vault.meta.own-emails"
    ),
    "nicknames.json": PathClass(
        RecoveryClass.DECISION_CRITICAL, ArtifactPresence.REQUIRED, "vault.meta.nicknames"
    ),
    "ppa-config.json": PathClass(
        RecoveryClass.DECISION_CRITICAL, ArtifactPresence.REQUIRED, "vault.meta.ppa-config"
    ),
    "llm-config.json": PathClass(
        RecoveryClass.SECRET_REFERENCE, ArtifactPresence.REQUIRED, "vault.meta.llm-config"
    ),
    "dedup-candidates.json": PathClass(
        RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.meta.dedup-candidates"
    ),
    "enrichment-log.json": PathClass(
        RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.meta.enrichment-log"
    ),
    "llm-cache.json": PathClass(
        RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.meta.llm-cache"
    ),
    "validation-report.json": PathClass(
        RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.meta.validation-report"
    ),
    "processors.json": PathClass(
        RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.meta.processors"
    ),
    "source-updaters.json": PathClass(
        RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.meta.source-updaters"
    ),
    "vault-scan-cache.sqlite3": PathClass(
        RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.meta.vault-scan-cache"
    ),
    "query-embed-cache.sqlite": PathClass(
        RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.meta.query-embed-cache"
    ),
    "benchmark-sample.json": PathClass(
        RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.meta.benchmark-sample"
    ),
    "canonical-decisions.json": PathClass(
        RecoveryClass.DECISION_CRITICAL, ArtifactPresence.REQUIRED, "vault.meta.canonical-decisions"
    ),
    "change-journal.sqlite3": PathClass(
        RecoveryClass.DECISION_CRITICAL, ArtifactPresence.REQUIRED, "vault.meta.change-journal"
    ),
    "change-journal.lock": PathClass(
        RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.meta.change-journal-lock"
    ),
}

STATE_OWNERS: tuple[StateOwner, ...] = (
    StateOwner(
        "vault.canonical.cards",
        RecoveryClass.CANONICAL,
        ArtifactPresence.REQUIRED,
        "<card-family>/**/*.md",
        "archive_vault/vault.py",
        "Copy and hash every markdown card. Restore cannot invent missing cards.",
        "Families inspected from archive_vault/card_contracts.py.",
    ),
    StateOwner(
        "vault.canonical.attachments",
        RecoveryClass.CANONICAL,
        ArtifactPresence.REQUIRED,
        "Attachments/** and *Attachments/**",
        "archive_vault/vault.py",
        "Copy and hash attachment binaries. Not reconstructible from markdown.",
        "Attachments is in EXCLUDED_DIRS for card walks; it is still canonical storage.",
    ),
    StateOwner(
        "vault.meta.identity-map",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.REQUIRED,
        "_meta/identity-map.json",
        "archive_vault/identity.py",
        "Required vault file. Preserve alias→canonical redirects.",
    ),
    StateOwner(
        "vault.meta.sync-state",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.REQUIRED,
        "_meta/sync-state.json",
        "archive_vault/sync_state.py",
        "Required vault file. Source cursors are unsafe to replay without this.",
    ),
    StateOwner(
        "vault.meta.own-emails",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.REQUIRED,
        "_meta/own-emails.json",
        "archive_sync/adapters/gmail_correspondents.py",
        "Required vault file. Account/self aliases affect identity and ingest.",
    ),
    StateOwner(
        "vault.meta.nicknames",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.REQUIRED,
        "_meta/nicknames.json",
        "archive_vault/identity_resolver.py",
        "Required vault file. Resolution aliases are not derived from cards alone.",
    ),
    StateOwner(
        "vault.meta.ppa-config",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.REQUIRED,
        "_meta/ppa-config.json",
        "archive_vault/config.py",
        "Required vault file. Thresholds change merge/dedup behavior.",
    ),
    StateOwner(
        "vault.meta.llm-config",
        RecoveryClass.SECRET_REFERENCE,
        ArtifactPresence.REQUIRED,
        "_meta/llm-config.json",
        "archive_vault/llm_provider.py",
        "Required provider/model references only. Reject if credential keys appear.",
    ),
    StateOwner(
        "vault.meta.dedup-candidates",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_meta/dedup-candidates.json",
        "archive_doctor/handler.py",
        "Regenerate from a doctor/validate pass.",
    ),
    StateOwner(
        "vault.meta.enrichment-log",
        RecoveryClass.DISPOSABLE,
        ArtifactPresence.OPTIONAL,
        "_meta/enrichment-log.json",
        "archive_scripts/ppa-post-import.sh",
        "Debug log. Do not restore as authoritative.",
    ),
    StateOwner(
        "vault.meta.llm-cache",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_meta/llm-cache.json",
        "archive_vault/llm_provider.py",
        "Recompute from provider calls. Optional acceleration.",
    ),
    StateOwner(
        "vault.meta.validation-report",
        RecoveryClass.DISPOSABLE,
        ArtifactPresence.OPTIONAL,
        "_meta/validation-report.json",
        "archive_doctor/handler.py",
        "Regenerate via validate.",
    ),
    StateOwner(
        "vault.meta.processors",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_meta/processors.json",
        "archive_sync/processors/runner.py",
        "Rebuild status. Clear expired leases; do not resurrect a dead worker.",
    ),
    StateOwner(
        "vault.meta.source-updaters",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_meta/source-updaters.json",
        "archive_cli/source_updaters/cli.py",
        "Status snapshot. Authoritative cursors live in sync-state.json.",
    ),
    StateOwner(
        "vault.meta.vault-scan-cache",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_meta/vault-scan-cache.sqlite3",
        "archive_cli/vault_cache.py",
        "Rebuild from the vault. Do not copy a live WAL as a checkpoint.",
    ),
    StateOwner(
        "vault.meta.query-embed-cache",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_meta/query-embed-cache.sqlite",
        "archive_cli/index_config.py",
        "Rebuild query embeddings.",
    ),
    StateOwner(
        "vault.meta.serving-index",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_meta/rust-search-index/",
        "archive_cli/index_config.py",
        "Rebuild via publication after P02/P03. Restore must work without it.",
    ),
    StateOwner(
        "vault.meta.benchmark-sample",
        RecoveryClass.DISPOSABLE,
        ArtifactPresence.OPTIONAL,
        "_meta/benchmark-sample.json",
        "archive_cli/benchmark.py",
        "Benchmark artifact only.",
    ),
    StateOwner(
        "vault.meta.canonical-decisions",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.OPTIONAL,
        "_meta/canonical-decisions.json",
        "archive_vault/decisions.py",
        "When present, hashed as required content. Human field overrides and source conflicts.",
        "Absence is fine until a correction exists. Do not reconstruct from Postgres.",
    ),
    StateOwner(
        "vault.meta.change-journal",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.OPTIONAL,
        "_meta/change-journal.sqlite3",
        "archive_vault/change_journal.py",
        "When present, hashed as required content. P02 event spine and recovery checkpoint.",
        "Absence keeps archive_id/checkpoint unavailable. Do not copy a live WAL as a checkpoint.",
    ),
    StateOwner(
        "vault.meta.change-journal-lock",
        RecoveryClass.DISPOSABLE,
        ArtifactPresence.OPTIONAL,
        "_meta/change-journal.lock",
        "archive_vault/change_journal.py",
        "Instance lock. Do not restore as authoritative.",
    ),
    StateOwner(
        "vault.meta.change-journal-staged",
        RecoveryClass.DISPOSABLE,
        ArtifactPresence.OPTIONAL,
        "_meta/change-journal/staged/",
        "archive_vault/change_journal.py",
        "Prepared mutation staging. Incomplete prepares stay prepared, never invented success.",
    ),
    StateOwner(
        "vault.hygiene.rollback-kit",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.REQUIRED,
        "_artifacts/hygiene-rollback-kit/<decision_run_id>/",
        "archive_cli/corpus_hygiene/apply.py",
        "When present, required. Preimages of suppressed markdown.",
        "Present files are required; absence of the directory is not an error.",
    ),
    StateOwner(
        "vault.templates",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.OPTIONAL,
        "_templates/",
        "archive_scripts/ppa-init-vault.sh",
        "Recreate from package/init templates.",
    ),
    StateOwner(
        "vault.obsidian",
        RecoveryClass.DISPOSABLE,
        ArtifactPresence.OPTIONAL,
        ".obsidian/",
        "archive_vault/vault.py",
        "Editor metadata. Not archive meaning.",
    ),
    StateOwner(
        "vault.sqlite-wal",
        RecoveryClass.DISPOSABLE,
        ArtifactPresence.OPTIONAL,
        "_meta/*.{sqlite,sqlite3}-{wal,shm}",
        "archive_cli/vault_cache.py",
        "Never treat a live WAL as a consistent checkpoint.",
    ),
    StateOwner(
        "pg.cards",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.LOGICAL,
        "postgres:cards",
        "archive_cli/schema_ddl.py",
        "Rebuild from canonical markdown.",
    ),
    StateOwner(
        "pg.projections-chunks-embeddings",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.LOGICAL,
        "postgres:chunks,embeddings,edges,typed projections",
        "archive_cli/schema_ddl.py",
        "Rebuild from cards. Optional acceleration dump is not canonical.",
    ),
    StateOwner(
        "pg.card_corpus_state",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.LOGICAL,
        "postgres:card_corpus_state",
        "archive_cli/corpus_hygiene/state_store.py",
        "Export into a versioned canonical decision record (P07-B). Not reconstructible.",
        "Migration 006. Suppression/quarantine currently exists only in Postgres.",
    ),
    StateOwner(
        "pg.email_corpus_decisions",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.LOGICAL,
        "postgres:email_corpus_decisions",
        "archive_cli/corpus_hygiene/state_store.py",
        "Export into a versioned canonical decision record (P07-B).",
        "Migration 006. Human/policy corpus decisions.",
    ),
    StateOwner(
        "pg.link_decisions",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.LOGICAL,
        "postgres:link_decisions",
        "archive_cli/schema_ddl.py",
        "Human overrides are not reconstructible. Export with review_actions (P07-B).",
        "Auto scores can be recomputed; review overrides cannot.",
    ),
    StateOwner(
        "pg.review_actions",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.LOGICAL,
        "postgres:review_actions",
        "archive_cli/schema_ddl.py",
        "Export human linker reviews into canonical decision records (P07-B).",
    ),
    StateOwner(
        "pg.source_updater_state",
        RecoveryClass.DECISION_CRITICAL,
        ArtifactPresence.LOGICAL,
        "postgres:source_updater_state",
        "archive_sync/source_updaters/state_store.py",
        "Cursors also live in sync-state.json. Export PG cursor_payload if it is not a copy.",
        "Migration 007.",
    ),
    StateOwner(
        "pg.processor_state",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.LOGICAL,
        "postgres:processor_state,processor_runs,processor_input_state",
        "archive_sync/processors/state_store.py",
        "Rebuild via maintain. Clear expired leases. P03 receipts become the invalidation seam.",
        "Migration 008.",
    ),
    StateOwner(
        "pg.rebuild_checkpoint",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.LOGICAL,
        "postgres:rebuild_checkpoint",
        "archive_cli/loader.py",
        "Instance-local. Do not restore a foreign worker as in-progress.",
    ),
    StateOwner(
        "pg.link_jobs",
        RecoveryClass.RECONSTRUCTIBLE,
        ArtifactPresence.LOGICAL,
        "postgres:link_jobs,link_candidates,link_evidence,promotion_queue",
        "archive_cli/schema_ddl.py",
        "Requeue from current cards after restore. Clear claimed leases.",
    ),
    StateOwner(
        "secret.oauth-tokens",
        RecoveryClass.SECRET_MATERIAL,
        ArtifactPresence.EXCLUDED,
        "env / 1Password / ~/.gmail-mcp / token caches",
        "archive_auth/token_manager.py",
        "Exclude values. Preserve provider/account names only. Recover credentials separately.",
    ),
    StateOwner(
        "secret.llm-keys",
        RecoveryClass.SECRET_MATERIAL,
        ArtifactPresence.EXCLUDED,
        "~/.ppa/gemini_key.txt and provider env vars",
        "archive_vault/llm_provider.py",
        "Exclude key files and env values from ordinary manifests.",
    ),
    StateOwner(
        "secret.backup-passphrase",
        RecoveryClass.SECRET_MATERIAL,
        ArtifactPresence.EXCLUDED,
        "PPA_BACKUP_PASSPHRASE / 1Password ref",
        "archive_scripts/ppa-backup-encrypt.sh",
        "Unlock credentials stay outside the archive bundle.",
    ),
)


def inventory_state_owners() -> tuple[StateOwner, ...]:
    """Return the inspected recovery catalog. No live vault is read."""

    return STATE_OWNERS


def unavailable_binding() -> dict[str, Any]:
    return {
        "status": "unavailable",
        "reason": CHECKPOINT_UNAVAILABLE_REASON,
        "value": None,
    }


def _recovery_bindings(vault_root: Path) -> dict[str, Any]:
    """Adopt P02 journal bindings when the event spine is present.

    Does not create a journal during inventory. A missing journal keeps the
    P07-A unavailable placeholder.
    """

    journal_path = vault_root / "_meta" / "change-journal.sqlite3"
    if not journal_path.is_file():
        return {
            "archive_id": unavailable_binding(),
            "checkpoint": unavailable_binding(),
        }
    try:
        from archive_engine.changes import recovery_checkpoint_binding
    except ImportError:
        return {
            "archive_id": unavailable_binding(),
            "checkpoint": unavailable_binding(),
        }
    return recovery_checkpoint_binding(vault_root)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def is_secret_rel_path(rel_path: str) -> bool:
    name = Path(rel_path).name.lower()
    if name in SECRET_BASENAMES or name.startswith(".env."):
        return True
    if name.endswith(SECRET_SUFFIXES):
        return True
    return any(marker in name for marker in SECRET_NAME_MARKERS)


def _posix_rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _has_prefix(rel_path: str, prefix: str) -> bool:
    return rel_path == prefix or rel_path.startswith(prefix + "/")


def classify_rel_path(rel_path: str) -> PathClass | None:
    """Classify a vault-relative path, or return None when it is unknown-required."""

    if is_secret_rel_path(rel_path):
        return PathClass(RecoveryClass.SECRET_MATERIAL, ArtifactPresence.EXCLUDED, "secret.vault-filename")
    if _has_prefix(rel_path, HYGIENE_ROLLBACK_PREFIX):
        return PathClass(
            RecoveryClass.DECISION_CRITICAL,
            ArtifactPresence.REQUIRED,
            "vault.hygiene.rollback-kit",
        )
    if _has_prefix(rel_path, "_artifacts"):
        return PathClass(RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.artifacts.staging")
    if _has_prefix(rel_path, "_templates"):
        return PathClass(RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.templates")
    if _has_prefix(rel_path, ".obsidian"):
        return PathClass(RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.obsidian")
    if rel_path.startswith("_meta/"):
        rest = rel_path[len("_meta/") :]
        if rest in _META_FILES:
            return _META_FILES[rest]
        if rest.startswith("rust-search-index/") or rest == "rust-search-index":
            return PathClass(RecoveryClass.RECONSTRUCTIBLE, ArtifactPresence.OPTIONAL, "vault.meta.serving-index")
        if rest.startswith("change-journal/"):
            return PathClass(
                RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.meta.change-journal-staged"
            )
        if rest.endswith(("-wal", "-shm")):
            return PathClass(RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.sqlite-wal")
        if rest.endswith("-dirty-uids.txt"):
            return PathClass(RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.meta.maintain-dirty")
        if rest.startswith("_bench_") or rest.endswith(".tmp"):
            return PathClass(RecoveryClass.DISPOSABLE, ArtifactPresence.OPTIONAL, "vault.meta.disposable")
        return None
    first = rel_path.split("/", 1)[0]
    if first in CANONICAL_FAMILY_PREFIXES or any(_has_prefix(rel_path, prefix) for prefix in CANONICAL_FAMILY_PREFIXES):
        return PathClass(RecoveryClass.CANONICAL, ArtifactPresence.REQUIRED, "vault.canonical.cards")
    return None


def _iter_vault_paths(vault_root: Path) -> Iterator[Path]:
    for dirpath, dirnames, filenames in os.walk(vault_root, followlinks=False):
        dirnames[:] = sorted(name for name in dirnames if name not in {".git"})
        root = Path(dirpath)
        for name in sorted(filenames):
            yield root / name
        for name in dirnames:
            child = root / name
            if child.is_symlink():
                yield child


def _json_secret_keys(payload: Any) -> list[str]:
    found: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            lowered = str(key).lower()
            if lowered in SECRET_JSON_KEYS and value not in (None, "", [], {}):
                found.append(str(key))
            found.extend(_json_secret_keys(value))
    elif isinstance(payload, list):
        for item in payload:
            found.extend(_json_secret_keys(item))
    return found


def _reject_secret_content(rel_path: str, path: Path) -> None:
    if path.suffix.lower() != ".json":
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return
    keys = _json_secret_keys(payload)
    if keys:
        raise SecretMaterialError(f"credential keys {sorted(set(keys))} in {rel_path}")


def _validate_required_json(rel_path: str, path: Path) -> None:
    if path.suffix.lower() != ".json":
        return
    try:
        json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CorruptRequiredStateError(f"corrupt required JSON {rel_path}: {exc}") from exc


def _artifact_record(
    *,
    rel_path: str,
    classification: RecoveryClass,
    presence: ArtifactPresence,
    owner_id: str,
    size: int,
    digest: str,
) -> dict[str, Any]:
    return {
        "rel_path": rel_path,
        "classification": classification.value,
        "presence": presence.value,
        "owner_id": owner_id,
        "size": size,
        "sha256": digest,
    }


def _exclusion_record(rel_path: str, reason: str) -> dict[str, Any]:
    return {
        "rel_path": rel_path,
        "classification": RecoveryClass.SECRET_MATERIAL.value,
        "presence": ArtifactPresence.EXCLUDED.value,
        "reason": reason,
        "size": None,
        "sha256": None,
    }


def _logical_owner_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for owner in STATE_OWNERS:
        if owner.presence is not ArtifactPresence.LOGICAL:
            continue
        records.append(
            {
                "owner_id": owner.owner_id,
                "classification": owner.classification.value,
                "presence": owner.presence.value,
                "location": owner.location,
                "materialized": False,
                "recovery_strategy": owner.recovery_strategy,
                "notes": owner.notes,
            }
        )
    return records


def generate_manifest(vault_root: Path | str) -> dict[str, Any]:
    """Walk *vault_root* and emit a versioned recovery manifest."""

    root = Path(vault_root).resolve()
    if not root.is_dir():
        raise RecoveryManifestError(f"vault root is not a directory: {root}")

    artifacts: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []

    for path in _iter_vault_paths(root):
        rel_path = _posix_rel(path, root)
        if path.is_symlink():
            if is_secret_rel_path(rel_path):
                exclusions.append(_exclusion_record(rel_path, "credential_filename"))
                continue
            raise UnknownRequiredStateError(f"symlink is not inventoried: {rel_path}")
        if not path.is_file():
            raise UnknownRequiredStateError(f"non-regular path is not inventoried: {rel_path}")
        classified = classify_rel_path(rel_path)
        if classified is None:
            raise UnknownRequiredStateError(f"unknown required state: {rel_path}")
        if classified.presence is ArtifactPresence.EXCLUDED:
            exclusions.append(_exclusion_record(rel_path, "credential_filename"))
            continue
        _reject_secret_content(rel_path, path)
        if classified.presence is ArtifactPresence.REQUIRED:
            _validate_required_json(rel_path, path)
        artifacts.append(
            _artifact_record(
                rel_path=rel_path,
                classification=classified.classification,
                presence=classified.presence,
                owner_id=classified.owner_id,
                size=path.stat().st_size,
                digest=sha256_file(path),
            )
        )

    missing = [
        owner.location
        for owner in STATE_OWNERS
        if owner.presence is ArtifactPresence.REQUIRED
        and owner.location.startswith("_meta/")
        and not (root / owner.location).is_file()
    ]
    if missing:
        raise MissingRequiredStateError(f"missing required state: {missing}")

    artifacts.sort(key=lambda item: item["rel_path"])
    exclusions.sort(key=lambda item: item["rel_path"])
    bindings = _recovery_bindings(root)
    return {
        "format_name": MANIFEST_FORMAT_NAME,
        "format_version": MANIFEST_FORMAT_VERSION,
        "archive_id": bindings["archive_id"],
        "checkpoint": bindings["checkpoint"],
        "missing_state_policy": MISSING_STATE_POLICY,
        "required_versions": {"recovery_manifest": MANIFEST_FORMAT_VERSION},
        "artifacts": artifacts,
        "exclusions": exclusions,
        "logical_owners": _logical_owner_records(),
    }


def write_manifest(manifest: Mapping[str, Any], dest: Path | str) -> Path:
    path = Path(dest)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def read_manifest(path: Path | str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise IncompatibleManifestError("manifest root must be an object")
    return payload


def _require_binding(field_name: str, payload: Mapping[str, Any]) -> None:
    binding = payload.get(field_name)
    if not isinstance(binding, Mapping):
        raise IncompatibleManifestError(f"{field_name} binding is required")
    status = binding.get("status")
    if status == "unavailable":
        if binding.get("reason") != CHECKPOINT_UNAVAILABLE_REASON:
            raise IncompatibleManifestError(f"{field_name} reason must remain {CHECKPOINT_UNAVAILABLE_REASON}")
        if binding.get("value") is not None:
            raise IncompatibleManifestError(f"{field_name} value must be null while unavailable")
        return
    if status == "available":
        if binding.get("value") in (None, ""):
            raise IncompatibleManifestError(f"{field_name} available binding is empty")
        return
    raise IncompatibleManifestError(f"{field_name} status must be available or unavailable")


def validate_manifest(manifest: Mapping[str, Any], vault_root: Path | str) -> None:
    """Verify *manifest* against files under *vault_root*. Fail closed."""

    if manifest.get("format_name") != MANIFEST_FORMAT_NAME:
        raise IncompatibleManifestError("unsupported recovery manifest format")
    version = manifest.get("format_version")
    if version != MANIFEST_FORMAT_VERSION:
        raise IncompatibleManifestError(f"unsupported recovery manifest version: {version}")
    required_versions = manifest.get("required_versions")
    if not isinstance(required_versions, Mapping):
        raise IncompatibleManifestError("required_versions missing")
    if required_versions.get("recovery_manifest") != MANIFEST_FORMAT_VERSION:
        raise IncompatibleManifestError("required recovery_manifest version mismatch")
    if manifest.get("missing_state_policy") != MISSING_STATE_POLICY:
        raise IncompatibleManifestError("missing_state_policy must reject required state")
    _require_binding("archive_id", manifest)
    _require_binding("checkpoint", manifest)

    root = Path(vault_root).resolve()
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, list):
        raise IncompatibleManifestError("artifacts must be a list")
    exclusions = manifest.get("exclusions")
    if not isinstance(exclusions, list):
        raise IncompatibleManifestError("exclusions must be a list")

    for exclusion in exclusions:
        if not isinstance(exclusion, Mapping):
            raise IncompatibleManifestError("exclusion must be an object")
        if exclusion.get("sha256") is not None or exclusion.get("size") is not None:
            raise SecretMaterialError(f"exclusion leaked content: {exclusion.get('rel_path')}")
        rel_path = str(exclusion.get("rel_path") or "")
        if rel_path and not is_secret_rel_path(rel_path):
            raise SecretMaterialError(f"exclusion is not a credential path: {rel_path}")

    expected = {str(item["rel_path"]): item for item in artifacts if isinstance(item, Mapping) and "rel_path" in item}
    live = generate_manifest(root)
    live_artifacts = {item["rel_path"]: item for item in live["artifacts"]}

    for rel_path, record in expected.items():
        presence = record.get("presence")
        live_record = live_artifacts.get(rel_path)
        if live_record is None:
            if presence == ArtifactPresence.REQUIRED.value:
                raise MissingRequiredStateError(f"missing required state: {rel_path}")
            continue
        if live_record["sha256"] != record.get("sha256") or live_record["size"] != record.get("size"):
            if presence == ArtifactPresence.REQUIRED.value:
                raise CorruptRequiredStateError(f"corrupt required state: {rel_path}")
            raise CorruptRequiredStateError(f"corrupt optional state: {rel_path}")

    for rel_path, live_record in live_artifacts.items():
        if rel_path not in expected and live_record["presence"] == ArtifactPresence.REQUIRED.value:
            raise UnknownRequiredStateError(f"unknown required state: {rel_path}")

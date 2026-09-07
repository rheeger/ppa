"""Encrypted backup restore into a new root (P07-D).

Uses the existing openssl AES-256-CBC + PBKDF2 envelope. Cryptography stays in
the installed openssl binary — this module never implements a cipher and never
falls back to a plaintext archive. Extract uses P05 contained-path rules.
Restore writes a new owned destination and refuses to touch the active root.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_engine.errors import CapabilityUnavailableError, EngineError
from archive_engine.recovery_manifest import (
    RecoveryClass,
    classify_rel_path,
    generate_manifest,
    is_secret_rel_path,
    sha256_file,
    validate_manifest,
    write_manifest,
)
from archive_vault.paths import PathEscapeError, atomic_write_contained, normalize_vault_rel

ENCRYPTION_TOOL = "openssl"
ENCRYPTION_FORMAT = "openssl-enc-aes-256-cbc-pbkdf2"
SCALE_PROFILE_FIXTURE = "fixture"
BUNDLE_ARCHIVE_NAME = "ppa-backup.tar.enc"
BUNDLE_MANIFEST_NAME = "ppa-backup.manifest.json.enc"
BUNDLE_CHECKSUM_NAME = "ppa-backup.tar.enc.sha256"
RECOVERY_MANIFEST_NAME = "ppa-recovery-manifest.json"
RESTORE_RECEIPT_FORMAT = "ppa.restore_receipt"
RESTORE_RECEIPT_VERSION = 1

_SCRIPT_ENCRYPT = "ppa-backup-encrypt.sh"
_SCRIPT_RESTORE = "ppa-backup-restore.sh"


class RecoveryError(EngineError):
    """Fail-closed backup or restore error."""


class EncryptionUnavailableError(CapabilityUnavailableError):
    """Required encryption or integrity tool is missing. No plaintext fallback."""


class TamperError(RecoveryError):
    """Checksum or required hash does not match the encrypted envelope."""


class WrongKeyError(RecoveryError):
    """Passphrase does not decrypt the envelope."""


class MissingBackupError(RecoveryError):
    """Required backup artifact is absent."""


class ActiveRootRestoreError(RecoveryError):
    """Restore destination overlaps the active or source vault."""


class RestoreActivationError(RecoveryError):
    """Restored tree failed validation before it could be activated."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def encrypt_script_path() -> Path:
    return repo_root() / "archive_scripts" / _SCRIPT_ENCRYPT


def restore_script_path() -> Path:
    return repo_root() / "archive_scripts" / _SCRIPT_RESTORE


def _which(name: str) -> str | None:
    return shutil.which(name)


def sha256_command() -> list[str] | None:
    """Return an argv that behaves like ``sha256sum FILE``."""

    if _which("sha256sum"):
        return ["sha256sum"]
    if _which("gsha256sum"):
        return ["gsha256sum"]
    if _which("shasum"):
        return ["shasum", "-a", "256"]
    return None


def encryption_capability() -> dict[str, Any]:
    openssl = _which(ENCRYPTION_TOOL)
    sha = sha256_command()
    return {
        "tool": ENCRYPTION_TOOL,
        "format": ENCRYPTION_FORMAT,
        "openssl_path": openssl,
        "openssl_available": bool(openssl),
        "sha256_command": sha,
        "available": bool(openssl and sha),
    }


def require_encryption_tools() -> dict[str, Any]:
    """Fail closed when openssl or a sha256 tool is missing. No plaintext path."""

    cap = encryption_capability()
    if not cap["openssl_available"]:
        raise EncryptionUnavailableError("openssl is required for encrypted backup; refusing plaintext fallback")
    if not cap["sha256_command"]:
        raise EncryptionUnavailableError(
            "sha256sum or shasum is required for backup integrity; refusing plaintext fallback"
        )
    return cap


def _write_secret_file(passphrase: str) -> str:
    handle = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    try:
        os.chmod(handle.name, 0o600)
        handle.write(passphrase)
        handle.flush()
    finally:
        handle.close()
    return handle.name


def file_sha256(path: Path | str) -> str:
    return sha256_file(Path(path))


def verify_checksum_file(archive: Path, checksum_file: Path) -> str:
    """Return the expected digest after confirming the archive bytes match."""

    if not checksum_file.is_file():
        raise MissingBackupError(f"checksum file not found: {checksum_file}")
    if not archive.is_file():
        raise MissingBackupError(f"encrypted archive not found: {archive}")
    line = checksum_file.read_text(encoding="utf-8").strip().split()
    if not line:
        raise TamperError("checksum file is empty")
    expected = line[0].strip().lower()
    actual = file_sha256(archive)
    if actual != expected:
        raise TamperError(f"archive sha256 mismatch: expected {expected} got {actual}")
    return actual


def _overlaps(left: Path, right: Path) -> bool:
    a = left.resolve()
    b = right.resolve()
    return a == b or a in b.parents or b in a.parents


def assert_new_restore_root(
    dest: Path | str,
    *,
    active_root: Path | str | None = None,
    source_vault: Path | str | None = None,
) -> Path:
    """Refuse destinations that would overwrite or delete an active/source vault."""

    dest_path = Path(dest).expanduser()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_resolved = dest_path.resolve() if dest_path.exists() else dest_path.absolute()
    forbidden: list[tuple[str, Path]] = []
    env_active = os.environ.get("PPA_PATH", "").strip()
    for label, raw in (
        ("destination", dest_resolved),
        ("active_root", Path(active_root).resolve() if active_root else None),
        ("source_vault", Path(source_vault).resolve() if source_vault else None),
        ("PPA_PATH", Path(env_active).resolve() if env_active else None),
    ):
        if raw is None:
            continue
        if label != "destination":
            forbidden.append((label, raw))
    for label, root in forbidden:
        if _overlaps(dest_resolved, root):
            raise ActiveRootRestoreError(f"restore destination {dest_resolved} overlaps {label} {root}")
    return dest_resolved


def snapshot_tree_hashes(root: Path | str) -> dict[str, str]:
    """Hash regular files under *root* without following directory symlinks."""

    base = Path(root)
    if not base.is_dir():
        return {}
    out: dict[str, str] = {}
    for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
        dirnames[:] = [name for name in dirnames if not (Path(dirpath) / name).is_symlink()]
        for name in filenames:
            path = Path(dirpath) / name
            if path.is_symlink() or not path.is_file():
                continue
            rel = path.relative_to(base).as_posix()
            out[rel] = file_sha256(path)
    return out


def _normalize_tar_name(name: str) -> str:
    raw = name.replace("\\", "/")
    if raw in {".", "./"}:
        return ""
    if raw.startswith("./"):
        raw = raw[2:]
    return raw


def _reject_tar_member(member: tarfile.TarInfo) -> None:
    name = member.name or ""
    if member.issym() or member.islnk():
        raise PathEscapeError(f"symlink or hardlink rejected: {name}")
    if member.ischr() or member.isblk() or member.isfifo():
        raise PathEscapeError(f"device or fifo rejected: {name}")
    if getattr(member, "isdev", lambda: False)():
        raise PathEscapeError(f"device rejected: {name}")
    if member.isdir():
        return
    if not member.isfile():
        raise PathEscapeError(f"non-regular tar member rejected: {name}")


def contained_extract_tarfile(tf: tarfile.TarFile, dest: Path | str) -> list[str]:
    """Extract regular files only, through P05 contained writes."""

    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)
    dest_path = dest_path.resolve()
    written: list[str] = []
    for member in tf:
        rel = _normalize_tar_name(member.name)
        _reject_tar_member(member)
        if member.isdir() or not rel:
            if rel:
                normalize_vault_rel(rel)
            continue
        if is_secret_rel_path(rel):
            continue
        normalize_vault_rel(rel)
        handle = tf.extractfile(member)
        if handle is None:
            raise RestoreActivationError(f"tar member could not be read: {member.name}")
        atomic_write_contained(dest_path, rel, handle.read())
        written.append(rel)
    return written


def contained_extract_tar_path(archive: Path | str, dest: Path | str) -> list[str]:
    with tarfile.open(archive, "r:*") as tf:
        return contained_extract_tarfile(tf, dest)


def decrypt_and_contained_extract(
    archive: Path | str,
    dest: Path | str,
    *,
    passphrase: str | None = None,
    passphrase_file: str | Path | None = None,
) -> list[str]:
    """Decrypt an openssl envelope and extract through P05 path rules."""

    cap = require_encryption_tools()
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)
    secret_file = str(passphrase_file) if passphrase_file else _write_secret_file(str(passphrase or ""))
    owns_secret = passphrase_file is None
    try:
        proc = subprocess.Popen(
            [
                cap["openssl_path"],
                "enc",
                "-d",
                "-aes-256-cbc",
                "-pbkdf2",
                "-pass",
                f"file:{secret_file}",
                "-in",
                str(archive),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout is not None
        try:
            with tarfile.open(fileobj=proc.stdout, mode="r|") as tf:
                written = contained_extract_tarfile(tf, dest_path)
        except (tarfile.TarError, OSError) as exc:
            proc.kill()
            proc.communicate()
            raise WrongKeyError(f"encrypted archive did not decrypt to a readable tar: {exc}") from exc
        except Exception:
            proc.kill()
            proc.communicate()
            raise
        stderr = proc.communicate()[1]
        if proc.returncode != 0:
            detail = (stderr or b"").decode("utf-8", errors="replace").strip()
            raise WrongKeyError(detail or "openssl decrypt failed")
        return written
    finally:
        if owns_secret:
            try:
                os.unlink(secret_file)
            except OSError:
                pass


def strip_reconstructible_artifacts(vault: Path | str) -> list[str]:
    """Remove optional reconstructible files so restore cannot lean on saved indexes."""

    root = Path(vault)
    removed: list[str] = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        for name in filenames:
            path = Path(dirpath) / name
            if path.is_symlink() or not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            classified = classify_rel_path(rel)
            if classified is None:
                continue
            if classified.classification is RecoveryClass.RECONSTRUCTIBLE:
                path.unlink()
                removed.append(rel)
        for name in list(dirnames):
            child = Path(dirpath) / name
            rel = child.relative_to(root).as_posix()
            classified = classify_rel_path(rel)
            if classified and classified.classification is RecoveryClass.RECONSTRUCTIBLE and child.is_dir():
                shutil.rmtree(child)
                dirnames.remove(name)
                removed.append(rel)
    return sorted(removed)


def _latest_artifact_dir(backup_base: Path) -> Path:
    artifacts = backup_base / "artifacts"
    if artifacts.is_dir():
        stamped = sorted(path for path in artifacts.iterdir() if path.is_dir())
        if stamped:
            return stamped[-1]
    latest = backup_base / "latest"
    if latest.is_dir():
        return latest
    raise MissingBackupError(f"no backup artifacts under {backup_base}")


def discover_bundle(
    backup_base: Path | str | None = None, *, archive_file: Path | str | None = None
) -> dict[str, Path]:
    if archive_file:
        archive = Path(archive_file)
        parent = archive.parent
        checksum = parent / BUNDLE_CHECKSUM_NAME
        if not checksum.is_file():
            checksum = Path(str(archive) + ".sha256")
        recovery_manifest = parent / RECOVERY_MANIFEST_NAME
        enc_manifest = parent / BUNDLE_MANIFEST_NAME
        return {
            "archive": archive,
            "checksum": checksum,
            "legacy_manifest": enc_manifest,
            "recovery_manifest": recovery_manifest,
            "artifact_dir": parent,
        }
    if not backup_base:
        raise MissingBackupError("backup_base or archive_file is required")
    base = Path(backup_base)
    artifact_dir = _latest_artifact_dir(base)
    return {
        "archive": artifact_dir / BUNDLE_ARCHIVE_NAME,
        "checksum": artifact_dir / BUNDLE_CHECKSUM_NAME,
        "legacy_manifest": artifact_dir / BUNDLE_MANIFEST_NAME,
        "recovery_manifest": artifact_dir / RECOVERY_MANIFEST_NAME,
        "artifact_dir": artifact_dir,
    }


def create_encrypted_bundle(
    vault: Path | str,
    backup_base: Path | str,
    *,
    passphrase: str,
    include_reconstructible: bool = False,
) -> dict[str, Any]:
    """Create an encrypted vault bundle via the existing encrypt script."""

    cap = require_encryption_tools()
    vault_path = Path(vault).resolve()
    if not vault_path.is_dir():
        raise RecoveryError(f"vault is not a directory: {vault_path}")
    backup_path = Path(backup_base)
    backup_path.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    backup_started_at = _utc_now()
    manifest = generate_manifest(vault_path)
    script = encrypt_script_path()
    if not script.is_file():
        raise RecoveryError(f"encrypt script not found: {script}")
    env = os.environ.copy()
    env["PPA_PATH"] = str(vault_path)
    env["PPA_BACKUP_BASE"] = str(backup_path)
    env["PPA_BACKUP_PASSPHRASE"] = passphrase
    env.pop("PPA_BACKUP_PASSPHRASE_FILE", None)
    env.pop("PPA_BACKUP_PASSPHRASE_OP_REF", None)
    if not include_reconstructible:
        env["PPA_BACKUP_CANONICAL_ONLY"] = "1"
    result = subprocess.run(
        ["bash", str(script)],
        cwd=str(repo_root()),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        raise RecoveryError(f"ppa-backup-encrypt failed: {(result.stderr or result.stdout or '').strip()}")
    bundle = discover_bundle(backup_path)
    if not bundle["archive"].is_file():
        raise RecoveryError("encrypt script did not produce an encrypted archive")
    write_manifest(manifest, bundle["artifact_dir"] / RECOVERY_MANIFEST_NAME)
    latest = backup_path / "latest"
    if latest.is_dir():
        write_manifest(manifest, latest / RECOVERY_MANIFEST_NAME)
    digest = file_sha256(bundle["archive"])
    return {
        "ok": True,
        "encryption_tool": cap["tool"],
        "encryption_format": cap["format"],
        "openssl_path": cap["openssl_path"],
        "archive": str(bundle["archive"]),
        "checksum": str(bundle["checksum"]),
        "recovery_manifest": str(bundle["artifact_dir"] / RECOVERY_MANIFEST_NAME),
        "archive_sha256": digest,
        "checkpoint": manifest.get("checkpoint"),
        "archive_id": manifest.get("archive_id"),
        "script_stdout": result.stdout.strip(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "backup_started_at": backup_started_at,
        "scale_profile": SCALE_PROFILE_FIXTURE,
        "include_reconstructible": include_reconstructible,
    }


def verify_encrypted_bundle(
    *,
    backup_base: Path | str | None = None,
    archive_file: Path | str | None = None,
    passphrase: str | None = None,
) -> dict[str, Any]:
    """Verify checksum and, when a passphrase is given, that the envelope decrypts."""

    cap = require_encryption_tools()
    bundle = discover_bundle(backup_base, archive_file=archive_file)
    if not bundle["archive"].is_file():
        raise MissingBackupError(f"encrypted archive not found: {bundle['archive']}")
    digest = verify_checksum_file(bundle["archive"], bundle["checksum"])
    decrypt_ok = False
    if passphrase is not None:
        secret_file = _write_secret_file(passphrase)
        try:
            probe = subprocess.run(
                [
                    cap["openssl_path"],
                    "enc",
                    "-d",
                    "-aes-256-cbc",
                    "-pbkdf2",
                    "-pass",
                    f"file:{secret_file}",
                    "-in",
                    str(bundle["archive"]),
                    "-out",
                    os.devnull,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if probe.returncode != 0:
                raise WrongKeyError((probe.stderr or probe.stdout or "openssl decrypt failed").strip())
            decrypt_ok = True
        finally:
            try:
                os.unlink(secret_file)
            except OSError:
                pass
    payload: dict[str, Any] = {
        "ok": True,
        "encryption_tool": cap["tool"],
        "encryption_format": cap["format"],
        "archive": str(bundle["archive"]),
        "archive_sha256": digest,
        "decrypt_verified": decrypt_ok,
    }
    if bundle["recovery_manifest"].is_file():
        payload["recovery_manifest"] = str(bundle["recovery_manifest"])
    return payload


def restore_encrypted_bundle(
    *,
    dest: Path | str,
    passphrase: str,
    backup_base: Path | str | None = None,
    archive_file: Path | str | None = None,
    active_root: Path | str | None = None,
    source_vault: Path | str | None = None,
    strip_reconstructible: bool = True,
    source_hashes: Mapping[str, str] | None = None,
) -> "RestoreReceipt":
    """Verify, decrypt, and contained-extract into a new root. Never writes *active_root*."""

    started = time.monotonic()
    cap = require_encryption_tools()
    dest_path = assert_new_restore_root(dest, active_root=active_root, source_vault=source_vault)
    if dest_path.exists() and any(dest_path.iterdir()):
        raise ActiveRootRestoreError(f"restore destination is not empty: {dest_path}")
    dest_path.mkdir(parents=True, exist_ok=True)
    bundle = discover_bundle(backup_base, archive_file=archive_file)
    digest = verify_checksum_file(bundle["archive"], bundle["checksum"])
    written = decrypt_and_contained_extract(bundle["archive"], dest_path, passphrase=passphrase)
    if not written:
        raise RestoreActivationError("restore extracted zero files")
    removed = strip_reconstructible_artifacts(dest_path) if strip_reconstructible else []
    checkpoint: dict[str, Any] = {}
    archive_id: dict[str, Any] = {}
    if bundle["recovery_manifest"].is_file():
        recovery_manifest = json.loads(bundle["recovery_manifest"].read_text(encoding="utf-8"))
        if strip_reconstructible:
            recovery_manifest["artifacts"] = [
                item
                for item in recovery_manifest.get("artifacts", [])
                if item.get("classification") != RecoveryClass.RECONSTRUCTIBLE.value
            ]
        validate_manifest(recovery_manifest, dest_path)
        checkpoint = dict(recovery_manifest.get("checkpoint") or {})
        archive_id = dict(recovery_manifest.get("archive_id") or {})
    else:
        live = generate_manifest(dest_path)
        checkpoint = dict(live.get("checkpoint") or {})
        archive_id = dict(live.get("archive_id") or {})
    active_untouched = True
    if source_hashes and source_vault:
        after = snapshot_tree_hashes(source_vault)
        active_untouched = after == dict(source_hashes)
        if not active_untouched:
            raise RestoreActivationError("source/active vault hashes changed during restore")
    elif source_vault and dest_path.resolve() != Path(source_vault).resolve():
        active_untouched = True
    elapsed = round(time.monotonic() - started, 3)
    return RestoreReceipt(
        status="restored",
        restore_root=str(dest_path),
        source_vault=str(Path(source_vault).resolve()) if source_vault else "",
        active_root=str(Path(active_root).resolve()) if active_root else os.environ.get("PPA_PATH", ""),
        active_root_untouched=active_untouched,
        archive_sha256=digest,
        encryption_tool=cap["tool"],
        encryption_format=cap["format"],
        openssl_path=str(cap["openssl_path"] or ""),
        checkpoint=checkpoint,
        archive_id=archive_id,
        extracted_files=len(written),
        stripped_reconstructible=removed,
        elapsed_seconds=elapsed,
        rto_seconds=elapsed,
        rpo_seconds=0.0,
        scale_profile=SCALE_PROFILE_FIXTURE,
        rebuilt=False,
        notes=(
            "Fixture RPO/RTO only. This duration is the isolated restore of a synthetic "
            "archive, not a production recovery claim."
        ),
    )


@dataclass
class RestoreReceipt:
    status: str
    restore_root: str
    source_vault: str = ""
    active_root: str = ""
    active_root_untouched: bool = True
    archive_sha256: str = ""
    encryption_tool: str = ENCRYPTION_TOOL
    encryption_format: str = ENCRYPTION_FORMAT
    openssl_path: str = ""
    checkpoint: dict[str, Any] = field(default_factory=dict)
    archive_id: dict[str, Any] = field(default_factory=dict)
    extracted_files: int = 0
    stripped_reconstructible: list[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    rto_seconds: float = 0.0
    rpo_seconds: float = 0.0
    scale_profile: str = SCALE_PROFILE_FIXTURE
    rebuilt: bool = False
    query: dict[str, Any] = field(default_factory=dict)
    notes: str = ""

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["format_name"] = RESTORE_RECEIPT_FORMAT
        payload["format_version"] = RESTORE_RECEIPT_VERSION
        return payload


def write_restore_receipt(receipt: RestoreReceipt, dest: Path | str) -> Path:
    path = Path(dest)
    path.parent.mkdir(parents=True, exist_ok=True)
    sanitized = receipt.to_payload()
    text = json.dumps(sanitized, indent=2, sort_keys=True)
    lowered = text.lower()
    for banned in ("passphrase", "password", "api_key", "refresh_token", "sk-"):
        if banned in lowered:
            raise RecoveryError(f"restore receipt leaked secret marker: {banned}")
    path.write_text(text + "\n", encoding="utf-8")
    return path

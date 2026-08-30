"""Canonical archive instance identity for validation gate evidence and guards."""

from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urlparse

from ..index_config import _ppa_env
from .constants import PRODUCTION_INSTANCE_ROLE


def dsn_descriptor(index_dsn: str | None) -> str:
    """Return a stable, non-secret descriptor for a Postgres DSN."""

    raw = (index_dsn or "").strip()
    if not raw:
        return "no-dsn"
    parsed = urlparse(raw)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    db = (parsed.path or "/").lstrip("/") or "postgres"
    return f"{host}:{port}/{db}"


def resolve_instance_role(instance_role: str | None = None) -> str:
    """Return the active archive instance role from CLI override or env."""

    return (instance_role or _ppa_env("PPA_ARCHIVE_INSTANCE_ROLE", default="")).strip()


def derive_archive_instance(
    *,
    vault_path: str,
    index_dsn: str | None,
    index_schema: str,
    instance_role: str | None = None,
) -> str:
    """Derive a stable archive instance label from existing config inputs."""

    explicit = _ppa_env("PPA_ARCHIVE_INSTANCE", default="").strip()
    if explicit:
        base = explicit
    else:
        vault_stem = Path(vault_path).expanduser().resolve().name or "vault"
        schema = (index_schema or "ppa").strip() or "ppa"
        base = f"{schema}@{dsn_descriptor(index_dsn)}@{vault_stem}"

    role = resolve_instance_role(instance_role)
    if role:
        return f"{role}:{base}"
    return base


def is_production_instance(archive_instance: str, *, instance_role: str | None = None) -> bool:
    """Return True when the label/env identifies the production archive instance."""

    role = resolve_instance_role(instance_role).lower()
    if role == PRODUCTION_INSTANCE_ROLE:
        return True

    label = archive_instance.strip().lower()
    if label.startswith(f"{PRODUCTION_INSTANCE_ROLE}:"):
        return True

    explicit = _ppa_env("PPA_ARCHIVE_INSTANCE", default="").strip().lower()
    return bool(explicit and explicit == label and role == PRODUCTION_INSTANCE_ROLE)


def instance_fingerprint(
    *,
    vault_path: str,
    index_dsn: str | None,
    index_schema: str,
) -> str:
    """Short hash fingerprint for drift detection without storing secrets."""

    payload = "|".join(
        [
            str(Path(vault_path).expanduser().resolve()),
            dsn_descriptor(index_dsn),
            (index_schema or "ppa").strip(),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

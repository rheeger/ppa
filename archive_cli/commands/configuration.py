"""Instance configuration services (P09-B).

CLI registration is P09-C. These functions are imported and tested directly.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from archive_engine.config import (
    CONFIG_SCHEMA_VERSION,
    bind_instance_config,
    current_secret_values,
    explain_instance_config,
    resolve_instance_config,
    rollback_instance_candidate,
    write_instance_candidate,
)
from archive_engine.errors import ConfigError
from archive_engine.scopes import SavedScope, parse_saved_scopes


def validate_configuration(
    *,
    config_path: str | Path | None = None,
    instance_dir: str | Path | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
    allow_cwd_discovery: bool = False,
) -> dict[str, Any]:
    config = resolve_instance_config(
        cli_overrides=cli_overrides,
        environ=environ,
        config_path=config_path,
        instance_dir=instance_dir,
        allow_cwd_discovery=allow_cwd_discovery,
    )
    scopes = parse_saved_scopes(config.scopes)
    return {
        "ok": True,
        "schema_version": config.schema_version,
        "archive_id": config.identity.archive_id,
        "canonical_root": config.identity.canonical_root,
        "index_schema": config.storage.index_schema,
        "fingerprint": config.fingerprint(),
        "scope_names": [scope.name for scope in scopes],
        "origins": [{"field": item.field, "origin": item.origin, "source": item.source} for item in config.origins],
    }


def explain_configuration(
    *,
    config_path: str | Path | None = None,
    instance_dir: str | Path | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
    allow_cwd_discovery: bool = False,
) -> dict[str, Any]:
    config = resolve_instance_config(
        cli_overrides=cli_overrides,
        environ=environ,
        config_path=config_path,
        instance_dir=instance_dir,
        allow_cwd_discovery=allow_cwd_discovery,
    )
    explained = explain_instance_config(config, secrets=current_secret_values())
    blob = json.dumps(explained)
    for secret in current_secret_values().values():
        if secret and secret in blob:
            raise ConfigError("explain leaked a secret value")
    return explained


def migrate_legacy_configuration(vault: str | Path) -> dict[str, Any]:
    """Write a versioned candidate from `_meta/ppa-config.json`. Keep the old file."""

    root = Path(vault).expanduser().resolve()
    legacy = root / "_meta" / "ppa-config.json"
    config = resolve_instance_config(instance_dir=root, allow_cwd_discovery=False)
    payload = {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "identity": config.identity.to_payload(),
        "entity": {"name": config.entity.name, "entity_type": config.entity.entity_type},
        "storage": {
            "vault_path": config.storage.vault_path,
            "index_schema": config.storage.index_schema,
            "index_dsn_ref": config.storage.index_dsn_ref.to_payload(),
        },
        "engine": {"native": config.engine.native, "format_compatibility": config.engine.format_compatibility},
        "embeddings": config.embeddings.to_payload(),
        "access": {
            "principal": config.access.principal,
            "profile": config.access.profile,
            "allowed_sources": list(config.access.allowed_sources),
            "allowed_domains": list(config.access.allowed_domains),
            "egress_policy_revision": config.access.egress_policy_revision,
        },
        "vault_tuning": {
            "merge_threshold": config.vault_tuning.merge_threshold,
            "conflict_threshold": config.vault_tuning.conflict_threshold,
            "fuzzy_name_threshold": config.vault_tuning.fuzzy_name_threshold,
            "finance_min_amount": config.vault_tuning.finance_min_amount,
        },
        "scopes": [SavedScope.from_payload(item).to_payload() for item in config.scopes] if config.scopes else [],
    }
    candidate = write_instance_candidate(root, payload)
    return {
        "ok": True,
        "legacy_path": str(legacy) if legacy.exists() else "",
        "legacy_preserved": legacy.exists(),
        "candidate_path": str(candidate),
        "archive_id": config.identity.archive_id,
    }


def rollback_legacy_migration(vault: str | Path) -> dict[str, Any]:
    root = Path(vault).expanduser().resolve()
    removed = rollback_instance_candidate(root)
    legacy = root / "_meta" / "ppa-config.json"
    return {"ok": True, "candidate_removed": removed, "legacy_preserved": legacy.exists()}


def bind_resolved_configuration(
    *,
    config_path: str | Path | None = None,
    instance_dir: str | Path | None = None,
    cli_overrides: Mapping[str, Any] | None = None,
    environ: Mapping[str, str] | None = None,
):
    config = resolve_instance_config(
        cli_overrides=cli_overrides,
        environ=environ,
        config_path=config_path,
        instance_dir=instance_dir,
        allow_cwd_discovery=False,
    )
    return bind_instance_config(config, secrets=current_secret_values())

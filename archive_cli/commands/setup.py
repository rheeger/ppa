"""Shared setup service for a new independent archive (P09-C).

Guided and ``--non-interactive --from SPEC --apply`` share this module.
A missing interactive answer is never approval. Existing roots are not
overwritten by default. Fixture-only SPECs; no personal accounts or seed path.
"""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from archive_engine.config import CONFIG_SCHEMA_VERSION, resolve_instance_config
from archive_engine.errors import ConfigError
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import OrganizationCard, PersonCard
from archive_vault.vault import write_card

logger = logging.getLogger("ppa.setup")

FIXTURE_PERSON_UID = "hfa-person-p09c-ada"
FIXTURE_PERSON_NAME = "Ada Example"
FIXTURE_ORG_UID = "hfa-organization-p09d-acme"
ALLOWED_FIXTURES = frozenset({"sample.fixture"})
SEED_MARKERS = (
    "hf-archives-seed",
    "/Archive/seed/",
    "/Archive/vault",
    "/Users/rheeger/Archive/",
)


class SetupError(ConfigError):
    """Setup refused to proceed."""


def _as_path(raw: object) -> Path:
    return Path(str(raw or "")).expanduser()


def load_setup_spec(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SetupError("setup SPEC must be a JSON object")
    return normalize_setup_spec(payload)


def normalize_setup_spec(payload: Mapping[str, Any]) -> dict[str, Any]:
    root = str(payload.get("root") or "").strip()
    if not root:
        raise SetupError("setup SPEC root is required")
    resolved = str(_as_path(root).resolve())
    if any(marker in resolved for marker in SEED_MARKERS):
        raise SetupError("setup SPEC must not point at a seed or production vault")
    fixture = str(payload.get("fixture") or "sample.fixture").strip() or "sample.fixture"
    if fixture not in ALLOWED_FIXTURES:
        raise SetupError(f"setup SPEC fixture {fixture!r} is not a fixture-only source")
    entity_type = str(payload.get("entity_type") or "person").strip() or "person"
    if entity_type not in {"person", "organization", "household", "admin", "other"}:
        raise SetupError(f"invalid entity_type {entity_type!r}")
    return {
        "root": resolved,
        "entity_name": str(payload.get("entity_name") or FIXTURE_PERSON_NAME).strip() or FIXTURE_PERSON_NAME,
        "entity_type": entity_type,
        "index_schema": str(payload.get("index_schema") or "ppa_fixture").strip() or "ppa_fixture",
        "fixture": fixture,
        "embedding_provider": str(payload.get("embedding_provider") or "hash").strip() or "hash",
        "index_dsn_ref": str(payload.get("index_dsn_ref") or "PPA_INDEX_DSN").strip() or "PPA_INDEX_DSN",
    }


def detect_capabilities(*, vault: Path | None = None, environ: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Honest capability report. A manifest file is never treated as freshness."""

    import os
    from shutil import which

    env = os.environ if environ is None else environ
    native: dict[str, Any] = {"status": "unavailable", "path": "", "reason": "archive_crate not imported"}
    try:
        import archive_crate

        native = {
            "status": "available",
            "path": str(Path(getattr(archive_crate, "__file__", "") or "").resolve()),
            "reason": "",
        }
    except Exception as exc:  # pragma: no cover - missing wheel
        native["reason"] = str(exc)

    dsn = str(env.get("PPA_INDEX_DSN") or "").strip()
    if not dsn:
        warehouse = {"status": "pending", "reason": "PPA_INDEX_DSN unset; bootstrap/maintain will explain recovery"}
    elif "127.0.0.1" not in dsn and "localhost" not in dsn:
        warehouse = {"status": "unavailable", "reason": "non-loopback warehouse DSN is refused for fixture setup"}
    else:
        warehouse = {"status": "available", "reason": "loopback DSN present (secret not shown)"}

    encrypt = Path(__file__).resolve().parents[2] / "archive_scripts" / "ppa-backup-encrypt.sh"
    backup = {
        "status": "available" if which("openssl") and encrypt.is_file() else "unavailable",
        "tool": "openssl",
        "script": str(encrypt) if encrypt.is_file() else "",
        "reason": "" if which("openssl") and encrypt.is_file() else "openssl or ppa-backup-encrypt.sh missing",
    }
    auth = {
        "status": "pending",
        "reason": "fixture setup does not connect live accounts; use connect verify after credentials exist",
    }
    manifest_exists = bool(vault and ((vault / "ppa.json").exists() or (vault / "_meta" / "ppa-instance.json").exists()))
    return {
        "native": native,
        "warehouse": warehouse,
        "auth": auth,
        "backup_tool": backup,
        "fresh": False,
        "freshness_reason": "a config manifest is not a freshness signal",
        "manifest_exists": manifest_exists,
        "production_proven": False,
    }


def plan_setup(spec: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_setup_spec(spec)
    root = Path(normalized["root"])
    existing = root.exists() and any(root.iterdir())
    capabilities = detect_capabilities(vault=root if root.exists() else None)
    return {
        "ok": True,
        "apply": False,
        "would_write": not existing,
        "blocked": existing,
        "reason": "existing root is not overwritten by default" if existing else "",
        "root": str(root),
        "entity_name": normalized["entity_name"],
        "entity_type": normalized["entity_type"],
        "index_schema": normalized["index_schema"],
        "fixture": normalized["fixture"],
        "fixture_person_uid": FIXTURE_PERSON_UID,
        "writes": [
            "ppa.json instance config",
            f"People/{FIXTURE_PERSON_UID}.md",
            "sample.fixture email_message cards",
        ],
        "providers": False,
        "source_credentials": False,
        "capabilities": capabilities,
        "next_steps": ["bootstrap-postgres if warehouse pending", "maintain", f"read {FIXTURE_PERSON_UID}"],
        "production_proven": False,
    }


def _refuse_existing_root(root: Path) -> None:
    if root.exists() and any(root.iterdir()):
        raise SetupError(f"refusing to overwrite existing root: {root}")


def _write_instance_config(spec: Mapping[str, Any]) -> Path:
    root = Path(spec["root"])
    payload = {
        "schema_version": CONFIG_SCHEMA_VERSION,
        "identity": {},
        "entity": {"name": spec["entity_name"], "entity_type": spec["entity_type"]},
        "storage": {
            "vault_path": str(root),
            "index_schema": spec["index_schema"],
            "index_dsn_ref": {"name": spec["index_dsn_ref"], "provider": "env"},
        },
        "engine": {"native": "rust", "format_compatibility": "serving-index-v1"},
        "embeddings": {
            "provider": spec["embedding_provider"],
            "model": "archive-hash-dev",
            "dimension": 8,
            "model_revision": "1",
        },
        "access": {"principal": "local-operator", "profile": "trusted-local"},
        "scopes": _default_scopes(spec),
    }
    path = root / "ppa.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _default_scopes(spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Person and organization instances get distinct saved scopes."""

    entity_type = str(spec.get("entity_type") or "person")
    name = str(spec.get("entity_name") or FIXTURE_PERSON_NAME)
    if entity_type == "organization":
        return [
            {
                "name": "org-ops",
                "sources": ["sample", "test"],
                "orgs": [name],
                "retrieval_profile": "current_ops",
            }
        ]
    return [
        {
            "name": "personal-fixture",
            "sources": ["sample", "test"],
            "people": [name],
            "retrieval_profile": "historical",
        }
    ]


def _write_fixture_person(root: Path, spec: Mapping[str, Any]) -> str:
    (root / "People").mkdir(parents=True, exist_ok=True)
    card = PersonCard(
        uid=FIXTURE_PERSON_UID,
        type="person",
        source=["test"],
        source_id="ada@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=FIXTURE_PERSON_NAME,
        first_name=FIXTURE_PERSON_NAME.split(" ", 1)[0],
        last_name=FIXTURE_PERSON_NAME.split(" ", 1)[-1],
        emails=["ada@example.test"],
    )
    entry = ProvenanceEntry("test", "2026-09-06", "deterministic")
    write_card(
        root,
        f"People/{FIXTURE_PERSON_UID}.md",
        card,
        body=f"{FIXTURE_PERSON_NAME} is the fixture person for an independent archive.",
        provenance={
            "summary": entry,
            "first_name": entry,
            "last_name": entry,
            "emails": entry,
        },
    )
    return FIXTURE_PERSON_UID


def _write_fixture_organization(root: Path, spec: Mapping[str, Any]) -> str:
    dest = root / "Entities" / "Organizations"
    dest.mkdir(parents=True, exist_ok=True)
    card = OrganizationCard(
        uid=FIXTURE_ORG_UID,
        type="organization",
        source=["test"],
        source_id="acme.example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=spec["entity_name"],
        name=str(spec["entity_name"]),
        org_type="company",
        domain="example.test",
    )
    entry = ProvenanceEntry("test", "2026-09-06", "deterministic")
    write_card(
        root,
        f"Entities/Organizations/{FIXTURE_ORG_UID}.md",
        card,
        body=f"{spec['entity_name']} is the fixture organization for an independent archive.",
        provenance={
            "summary": entry,
            "name": entry,
            "org_type": entry,
            "domain": entry,
        },
    )
    return FIXTURE_ORG_UID


def _import_sample_fixture(root: Path, spec: Mapping[str, Any]) -> dict[str, Any]:
    from archive_cli.engine_factory import resolve_archive_identity, schema_binding_for, trusted_local_access
    from archive_sync.connectors import sample as _sample  # noqa: F401
    from archive_sync.connectors.runtime import ContainedVaultWriter, execute_connector

    identity = resolve_archive_identity(root, schema_binding=schema_binding_for(spec["index_schema"]))
    access = trusted_local_access(identity.archive_id)
    result = execute_connector(
        "sample.fixture",
        identity=identity,
        access=access,
        writer=ContainedVaultWriter(root),
        cursor={},
        run_id="p09c-setup",
    )
    return {"uids": list(result.uids), "created_count": result.created_count}


def apply_setup(spec: Mapping[str, Any]) -> dict[str, Any]:
    normalized = normalize_setup_spec(spec)
    root = Path(normalized["root"])
    _refuse_existing_root(root)
    root.mkdir(parents=True, exist_ok=True)
    try:
        config_path = _write_instance_config(normalized)
        person_uid = _write_fixture_person(root, normalized)
        org_uid = ""
        if normalized["entity_type"] == "organization":
            org_uid = _write_fixture_organization(root, normalized)
        imported = _import_sample_fixture(root, normalized)
        resolved = resolve_instance_config(instance_dir=root, allow_cwd_discovery=False)
        capabilities = detect_capabilities(vault=root)
        return {
            "ok": True,
            "applied": True,
            "root": str(root),
            "archive_id": resolved.identity.archive_id,
            "config_path": str(config_path),
            "entity_type": normalized["entity_type"],
            "fixture_person_uid": person_uid,
            "fixture_org_uid": org_uid,
            "imported": imported,
            "capabilities": capabilities,
            "fresh": False,
            "production_proven": False,
        }
    except Exception:
        if root.exists() and not any(root.rglob("*.md")):
            shutil.rmtree(root, ignore_errors=True)
        raise


def collect_guided_spec(
    *,
    defaults: Mapping[str, Any] | None = None,
    input_fn: Callable[[str], str],
) -> dict[str, Any] | None:
    """Prompt for a plan. Empty/EOF answers abort — they are not apply."""

    base = dict(defaults or {})
    try:
        root = input_fn(f"Archive root [{base.get('root', '')}]: ").strip() or str(base.get("root") or "")
        if not root:
            return None
        name = input_fn(f"Entity name [{base.get('entity_name', FIXTURE_PERSON_NAME)}]: ").strip()
        entity_type = input_fn(f"Entity type [{base.get('entity_type', 'person')}]: ").strip()
        confirm = input_fn("Type apply to write this archive, or leave blank to abort: ").strip()
    except EOFError:
        return None
    if confirm != "apply":
        return None
    payload = {
        "root": root,
        "entity_name": name or base.get("entity_name") or FIXTURE_PERSON_NAME,
        "entity_type": entity_type or base.get("entity_type") or "person",
        "index_schema": base.get("index_schema") or "ppa_fixture",
        "fixture": base.get("fixture") or "sample.fixture",
        "embedding_provider": base.get("embedding_provider") or "hash",
    }
    return normalize_setup_spec(payload)


def run_setup(
    *,
    spec: Mapping[str, Any] | None = None,
    spec_path: str | Path | None = None,
    apply: bool = False,
    non_interactive: bool = False,
    input_fn: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    if spec_path is not None:
        spec = load_setup_spec(spec_path)
    if non_interactive:
        if spec is None:
            raise SetupError("--non-interactive requires --from SPEC")
        planned = plan_setup(spec)
        if not apply:
            planned["reason"] = planned.get("reason") or "review only; pass --apply to write"
            return planned
        if planned["blocked"]:
            raise SetupError(planned["reason"])
        return apply_setup(spec)
    if spec is None:
        collected = collect_guided_spec(defaults={}, input_fn=input_fn or input)
        if collected is None:
            return {"ok": True, "applied": False, "reason": "no interactive approval"}
        spec = collected
        apply = True
    planned = plan_setup(spec)
    if not apply:
        return planned
    if planned["blocked"]:
        raise SetupError(planned["reason"])
    return apply_setup(spec)

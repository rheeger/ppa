"""Contributor SDK check command (P08-D).

P09 registers this module on the product CLI later. Run it today as
``python -m archive_sync.connectors.cli``. Manifests are validated before the
package is imported. Incompatible versions never reach the writer.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from archive_cli.engine_factory import trusted_local_access
from archive_engine.contracts import AccessContext, ArchiveIdentity
from archive_engine.errors import IncompatibleContractError
from archive_sync.adapter_contracts import ADAPTER_SPECS
from archive_sync.connectors.contracts import parse_manifest
from archive_sync.connectors.registry import get_connector_factory, known_connectors, register_connector
from archive_sync.connectors.runtime import ContainedVaultWriter, execute_connector, execute_from_manifest
from archive_sync.source_updaters.constants import EXECUTABLE_ADAPTER_SOURCE_IDS, EXPORT_ADAPTER_SOURCE_IDS

logger = logging.getLogger("ppa.connectors")

SDK_MIGRATED_ADAPTERS = frozenset({"gmail-messages", "calendar-events"})
SDK_NATIVE_CONNECTORS = frozenset({"sample.fixture"})
INFERRED_SOURCE_FIELDS = frozenset({"same_person", "authorized_charge", "inferred_link", "same_as"})


def remaining_legacy_adapters() -> dict[str, list[str]]:
    """Adapters that still use the pre-SDK ingest path. Honest coverage, not a claim."""

    executable = sorted(item for item in EXECUTABLE_ADAPTER_SOURCE_IDS if item not in SDK_MIGRATED_ADAPTERS)
    export = sorted(EXPORT_ADAPTER_SOURCE_IDS - {"health"})
    specified = set(ADAPTER_SPECS)
    leftover = sorted(specified - SDK_MIGRATED_ADAPTERS - set(executable) - set(export))
    return {
        "sdk_migrated": sorted(SDK_MIGRATED_ADAPTERS),
        "sdk_native": sorted(SDK_NATIVE_CONNECTORS),
        "legacy_executable": executable,
        "legacy_export": export,
        "legacy_other": leftover,
        "stable_seam": "archive_sync.connectors.legacy.adapter_for_source + register_connector",
    }


def load_manifest(package_dir: Path) -> dict[str, object]:
    path = Path(package_dir) / "manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise IncompatibleContractError("manifest.json must be an object")
    return payload


def load_fixtures(package_dir: Path) -> dict[str, Any]:
    path = Path(package_dir) / "fixtures.json"
    if not path.is_file():
        return {"records": [], "negatives": []}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise IncompatibleContractError("fixtures.json must be an object")
    return payload


def import_connector_module(package_dir: Path):
    path = Path(package_dir) / "connector.py"
    spec = importlib.util.spec_from_file_location("ppa_contributor_connector", path)
    if spec is None or spec.loader is None:
        raise IncompatibleContractError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def ensure_registered(module: Any, requested_id: str) -> None:
    if requested_id in known_connectors():
        return
    factory = getattr(module, "factory", None) or getattr(module, "CONNECTOR_FACTORY", None)
    if factory is None:
        raise IncompatibleContractError("connector.py must expose factory() or call register_connector")
    register_connector(requested_id, factory)


def quality_report(
    *,
    result,
    fixtures: Mapping[str, Any],
    owned_fields: tuple[str, ...],
) -> dict[str, Any]:
    required = [str(item) for item in fixtures.get("required_owned") or owned_fields if str(item)]
    owned = required
    missing: list[str] = []
    inferred: list[str] = []
    for proposal in result.proposals:
        for field in owned:
            value = proposal.card.get(field)
            if value in ("", [], None):
                missing.append(f"{proposal.card.get('uid')}:{field}")
        for field in INFERRED_SOURCE_FIELDS:
            if proposal.card.get(field) not in ("", [], None, False):
                inferred.append(f"{proposal.card.get('uid')}:{field}")
        methods = {
            field: str((proposal.provenance.get(field) or {}).get("method") or "") for field in proposal.provenance
        }
        for field, method in methods.items():
            if method not in {"", "deterministic", "human"} and field in owned:
                inferred.append(f"{proposal.card.get('uid')}:{field}:method={method}")
    expected_skip = [
        item.get("provider_object_id") for item in fixtures.get("negatives") or [] if isinstance(item, Mapping)
    ]
    emitted_ids = [str(proposal.identity.provider_object_id) for proposal in result.proposals]
    false_promotions = [item for item in expected_skip if item in emitted_ids]
    coverage = 1.0
    if owned and result.proposals:
        coverage = max(0.0, 1.0 - (len(missing) / (len(owned) * len(result.proposals))))
    return {
        "owned_field_coverage": round(coverage, 3),
        "missing_owned_fields": missing,
        "inferred_source_fields": inferred,
        "false_promotions": false_promotions,
        "ok": not missing and not inferred and not false_promotions and coverage >= 0.99,
    }


def check_package(
    package_dir: Path,
    *,
    vault: Path,
    identity: ArchiveIdentity,
    access: AccessContext,
    run_id: str = "p08d",
) -> dict[str, Any]:
    """Validate manifest, then replay the package through the SDK."""

    payload = load_manifest(package_dir)
    parse_manifest(payload)
    fixtures = load_fixtures(package_dir)
    writer = ContainedVaultWriter(vault)
    module = import_connector_module(package_dir)
    requested_id = str(payload.get("connector_id") or "")
    ensure_registered(module, requested_id)
    get_connector_factory(requested_id)
    first = execute_connector(
        requested_id,
        identity=identity,
        access=access,
        writer=writer,
        cursor={},
        run_id=f"{run_id}-write",
    )
    replay = execute_connector(
        requested_id,
        identity=identity,
        access=access,
        writer=writer,
        cursor={},
        run_id=f"{run_id}-replay",
    )
    quality = quality_report(
        result=first,
        fixtures=fixtures,
        owned_fields=first.manifest.deterministic_fields_owned,
    )
    accounts = {proposal.identity.account_scope for proposal in first.proposals}
    providers = {proposal.identity.provider_object_id for proposal in first.proposals}
    return {
        "status": "compatible" if quality["ok"] and replay.created_count == 0 else "failed",
        "connector_id": requested_id,
        "created_count": first.created_count,
        "replay_created_count": replay.created_count,
        "uids": list(first.uids),
        "accounts": sorted(accounts),
        "provider_object_ids": sorted(providers),
        "distinct_accounts_same_provider": len(accounts) >= 2 and len(providers) == 1,
        "quality": quality,
        "committed_cursor": dict(first.committed_cursor or {}),
        "write_attempts": writer.write_attempts,
    }


def reject_incompatible_manifest(
    payload: Mapping[str, object],
    *,
    identity: ArchiveIdentity,
    access: AccessContext,
    writer: ContainedVaultWriter,
) -> dict[str, Any]:
    attempts = writer.write_attempts
    try:
        execute_from_manifest(payload, identity=identity, access=access, writer=writer, cursor={})
    except IncompatibleContractError as exc:
        if writer.write_attempts != attempts:
            raise IncompatibleContractError("incompatible manifest reached the writer") from exc
        return {"rejected": True, "reason": str(exc), "write_attempts": writer.write_attempts}
    raise IncompatibleContractError("incompatible manifest was accepted")


def check_migrated_manifests() -> dict[str, str]:
    from archive_sync.connectors.legacy import manifest_for_source

    verdicts: dict[str, str] = {}
    for source_id in sorted(SDK_MIGRATED_ADAPTERS):
        parse_manifest(manifest_for_source(source_id).to_payload())
        verdicts[source_id] = "compatible"
    return verdicts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m archive_sync.connectors.cli",
        description="Replay a connector package and print a compatibility verdict. JSON on stdout.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    check = sub.add_parser("check", help="Validate a contributor package and replay its fixtures")
    check.add_argument("--package", required=True, help="Directory with manifest.json, connector.py, fixtures.json")
    check.add_argument("--vault", required=True, help="Owned disposable vault")
    check.add_argument("--archive-id", default="aid-p08d")
    check.add_argument("--output", default="", help="Optional JSON verdict path")
    legacy = sub.add_parser("legacy-list", help="Print remaining legacy adapters")
    legacy.add_argument("--output", default="")
    return parser


def _identity(vault: Path, archive_id: str) -> ArchiveIdentity:
    return ArchiveIdentity(
        archive_id=archive_id,
        canonical_root=str(Path(vault).resolve()),
        schema_binding="warehouse:ppa+index_schema_v9",
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stderr, format="%(asctime)s [%(name)s] %(levelname)s %(message)s"
    )
    args = build_parser().parse_args(argv)
    if args.command == "legacy-list":
        payload = remaining_legacy_adapters()
        text = json.dumps(payload, indent=2, sort_keys=True)
        print(text)
        if args.output:
            Path(args.output).write_text(text + "\n", encoding="utf-8")
        return 0
    vault = Path(args.vault)
    vault.mkdir(parents=True, exist_ok=True)
    identity = _identity(vault, args.archive_id)
    access = trusted_local_access(identity.archive_id)
    result = check_package(Path(args.package), vault=vault, identity=identity, access=access)
    result["legacy"] = remaining_legacy_adapters()
    result["migrated_manifests"] = check_migrated_manifests()
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0 if result.get("status") == "compatible" else 1


if __name__ == "__main__":
    raise SystemExit(main())

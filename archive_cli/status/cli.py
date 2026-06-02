"""``ppa status`` / ``ppa readiness`` CLI for Section F observability."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from archive_cli.config import load_archive_config
from archive_cli.errors import IndexUnavailableError, VaultNotFoundError
from archive_cli.validation_gates.constants import (
    EXIT_BLOCKED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
)
from archive_cli.validation_gates.instance_identity import derive_archive_instance

from .aggregate import build_blocked_status, build_production_status
from .text import format_status_text


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _emit(payload: dict, args: argparse.Namespace) -> None:
    if getattr(args, "format", "json") == "json":
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        print(format_status_text(payload))


def _resolve_store(
    args: argparse.Namespace,
) -> tuple[object, str, str] | tuple[int, dict]:
    from archive_cli.commands._resolve import resolve_store, resolve_vault

    vault_override = (getattr(args, "vault", None) or "").strip()
    vault_path = Path(vault_override) if vault_override else None
    try:
        store = resolve_store(vault_path)
    except VaultNotFoundError as exc:
        payload = build_blocked_status(
            reason="vault_not_found",
            message=str(exc),
            vault_path=str(vault_override or load_archive_config().vault_path or ""),
        )
        return EXIT_BLOCKED, payload
    except IndexUnavailableError:
        vault = vault_path if vault_path is not None else resolve_vault()
        if vault is None or not vault.is_dir():
            payload = build_blocked_status(
                reason="index_unavailable",
                message="PPA_INDEX_DSN is required when vault path is not configured",
                vault_path=str(vault_override or ""),
            )
            return EXIT_BLOCKED, payload

        class _VaultOnlyStore:
            def __init__(self, path: Path) -> None:
                self.vault = path

        store = _VaultOnlyStore(vault)
        cfg = load_archive_config()
        archive_instance = derive_archive_instance(
            vault_path=str(vault),
            index_dsn=cfg.index_dsn or "",
            index_schema="ppa",
            instance_role=getattr(args, "instance_role", None) or None,
        )
        return store, archive_instance, "ppa"

    cfg = load_archive_config()
    archive_instance = derive_archive_instance(
        vault_path=str(store.vault),
        index_dsn=cfg.index_dsn,
        index_schema=store.index.schema,
        instance_role=getattr(args, "instance_role", None) or None,
    )
    return store, archive_instance, store.index.schema


def cmd_status(args: argparse.Namespace) -> int:
    resolved = _resolve_store(args)
    if isinstance(resolved[0], int):
        code, payload = resolved
        _emit(payload, args)
        return code

    store, archive_instance, schema = resolved
    index = getattr(store, "index", None)
    if index is None or not hasattr(index, "_connect"):
        payload = build_blocked_status(
            reason="database_unavailable",
            message="Section F status requires index connection for gate/corpus evidence",
            archive_instance=archive_instance,
            vault_path=str(getattr(store, "vault", "")),
        )
        _emit(payload, args)
        return EXIT_BLOCKED

    try:
        with index._connect() as conn:
            payload = build_production_status(
                store=store,
                archive_instance=archive_instance,
                conn=conn,
                schema=schema,
                require_production_soak=getattr(args, "require_soak", False),
                include_index_status=not getattr(args, "skip_index", False),
            )
    except (IndexUnavailableError, OSError) as exc:
        payload = build_blocked_status(
            reason="database_unavailable",
            message=str(exc),
            archive_instance=archive_instance,
            vault_path=str(getattr(store, "vault", "")),
        )
        _emit(payload, args)
        return EXIT_BLOCKED

    if getattr(args, "write_maintenance_report", False):
        from .maintenance_report import maintenance_report_from_status, write_maintenance_status_report

        run_id = str(payload.get("archive", {}).get("instance") or "status")[:32]
        report = maintenance_report_from_status(payload, run_id=run_id)
        paths = write_maintenance_status_report(_repo_root(), report)
        payload = dict(payload)
        payload["maintenance_report_paths"] = paths

    _emit(payload, args)
    return EXIT_SUCCESS


def cmd_readiness(args: argparse.Namespace) -> int:
    resolved = _resolve_store(args)
    if isinstance(resolved[0], int):
        code, payload = resolved
        _emit(payload, args)
        return code

    store, archive_instance, schema = resolved
    index = getattr(store, "index", None)
    if index is None or not hasattr(index, "_connect"):
        payload = build_blocked_status(
            reason="database_unavailable",
            message="readiness requires index connection for gate evidence",
            archive_instance=archive_instance,
            vault_path=str(getattr(store, "vault", "")),
        )
        _emit(payload, args)
        return EXIT_BLOCKED

    try:
        with index._connect() as conn:
            payload = build_production_status(
                store=store,
                archive_instance=archive_instance,
                conn=conn,
                schema=schema,
                require_production_soak=getattr(args, "require_soak", True),
                include_index_status=False,
            )
    except (IndexUnavailableError, OSError) as exc:
        payload = build_blocked_status(
            reason="database_unavailable",
            message=str(exc),
            archive_instance=archive_instance,
            vault_path=str(getattr(store, "vault", "")),
        )
        _emit(payload, args)
        return EXIT_BLOCKED

    readiness = payload.get("v3_readiness") or {}
    if getattr(args, "format", "json") == "json":
        print(json.dumps(readiness, indent=2, sort_keys=True, default=str))
    else:
        slim = {
            "archive": payload.get("archive"),
            "v3_readiness": readiness,
            "errors": payload.get("errors"),
            "warnings": payload.get("warnings"),
        }
        print(format_status_text(slim))
    return EXIT_SUCCESS if readiness.get("ready") else EXIT_VALIDATION_FAILED


def patch_status_parser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default=None,
        help="Section F production status (json or text). Omit for legacy index status JSON.",
    )
    parser.add_argument("--vault", default="", help="Vault path override")
    parser.add_argument("--instance-role", default="", help="Archive instance role prefix")
    parser.add_argument(
        "--require-soak",
        action="store_true",
        help="Require production soak gate evidence in v3 readiness section",
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Omit derived index status from Section F payload",
    )
    parser.add_argument(
        "--write-maintenance-report",
        action="store_true",
        help="Write append-only logs/maintenance report from this status read (no mutation)",
    )


def add_readiness_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "readiness",
        help="Evaluate v3 readiness from Sections B/D/E/G evidence (Section F)",
    )
    parser.add_argument("--format", choices=["json", "text"], default="json")
    parser.add_argument("--vault", default="")
    parser.add_argument("--instance-role", default="")
    parser.add_argument(
        "--require-soak",
        action="store_true",
        default=True,
        help="Require production soak gate (default: true)",
    )
    parser.add_argument(
        "--no-require-soak",
        action="store_false",
        dest="require_soak",
        help="Do not require production soak gate evidence",
    )
    parser.set_defaults(func=cmd_readiness)


def dispatch(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        print("readiness command misconfigured", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE
    return int(func(args))

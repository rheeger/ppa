"""Named parser registration for product commands (P09-C).

``archive_cli.__main__`` calls ``register_product_commands`` and
``dispatch_product_command``. New setup/config/connect/recovery parsers live
here instead of growing the main function.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

from archive_cli.errors import PpaError

_log = logging.getLogger("ppa.cli")

_PRODUCT_COMMANDS = frozenset(
    {
        "setup",
        "config",
        "connect",
        "connector",
        "backup",
        "verify-backup",
        "restore",
        "activate-restore",
        "instance-status",
        "analytics",
    }
)


def _print_json(data: object) -> None:
    print(json.dumps(data, indent=2, default=str))


def _fail(exc: Exception) -> None:
    print(str(exc), file=sys.stderr)
    raise SystemExit(1)


def register_product_commands(subparsers: argparse._SubParsersAction) -> None:
    """Register setup, config, connect, connector, and recovery parsers."""

    setup = subparsers.add_parser(
        "setup",
        help="Initialize a new fixture archive (writes only with --apply; no live accounts)",
        description=(
            "Create one independent archive from a fixture SPEC. "
            "Writes vault files only after explicit --apply or an interactive 'apply' answer. "
            "Does not call providers, does not use live accounts, and does not require source credentials. "
            "Existing roots are not overwritten by default."
        ),
    )
    setup.add_argument("--from", dest="spec_path", default="", help="Fixture-only JSON SPEC")
    setup.add_argument("--apply", action="store_true", help="Write the reviewed plan (required with --non-interactive)")
    setup.add_argument("--non-interactive", action="store_true", help="Require --from SPEC; never treat silence as apply")

    config = subparsers.add_parser(
        "config",
        help="Validate, explain, or migrate instance config (explain is read-only; migrate writes a candidate)",
    )
    config_sub = config.add_subparsers(dest="config_command", required=True)
    validate = config_sub.add_parser("validate", help="Validate instance config (read-only; no providers)")
    validate.add_argument("--instance-dir", default="", help="Explicit instance directory")
    validate.add_argument("--config", dest="config_path", default="", help="Explicit config path")
    explain = config_sub.add_parser("explain", help="Print redacted effective config (read-only; secrets omitted)")
    explain.add_argument("--instance-dir", default="", help="Explicit instance directory")
    explain.add_argument("--config", dest="config_path", default="", help="Explicit config path")
    migrate = config_sub.add_parser("migrate", help="Write a versioned candidate; keeps the old file (writes candidate only)")
    migrate.add_argument("vault", help="Vault root containing _meta/ppa-config.json")
    rollback = config_sub.add_parser("rollback", help="Remove the instance candidate; keep the legacy file")
    rollback.add_argument("vault", help="Vault root")

    connect = subparsers.add_parser(
        "connect",
        help="Verify a source without connecting personal accounts (fixture verify writes sample cards; live Google stays pending)",
    )
    connect_sub = connect.add_subparsers(dest="connect_command", required=True)
    verify = connect_sub.add_parser(
        "verify",
        help="Verify fixture or report pending live auth (no live Google; fixture may write sample cards)",
    )
    verify.add_argument("--source", default="sample.fixture", help="sample.fixture or a live source id")
    verify.add_argument("--vault", default="", help="Owned vault for fixture verify")

    connector = subparsers.add_parser(
        "connector",
        help="Contributor SDK check (may write fixture cards into --vault; no live credentials)",
    )
    connector_sub = connector.add_subparsers(dest="connector_command", required=True)
    check = connector_sub.add_parser("check", help="Replay a contributor package (writes fixture cards into --vault)")
    check.add_argument("--package", required=True)
    check.add_argument("--vault", required=True, help="Owned disposable vault")
    check.add_argument("--archive-id", default="aid-p09c")
    check.add_argument("--output", default="")
    legacy = connector_sub.add_parser("legacy-list", help="Print remaining legacy adapters (read-only)")
    legacy.add_argument("--output", default="")

    backup = subparsers.add_parser(
        "backup",
        help="Create an encrypted vault bundle (writes backup artifacts; requires openssl + passphrase; no providers)",
    )
    backup.add_argument("--vault", required=True)
    backup.add_argument("--backup-base", required=True)
    backup.add_argument("--passphrase", required=True)
    backup.add_argument("--include-reconstructible", action="store_true")

    verify_backup = subparsers.add_parser(
        "verify-backup",
        help="Verify an encrypted backup (read-only; optional decrypt with passphrase)",
    )
    verify_backup.add_argument("--backup-base", default="")
    verify_backup.add_argument("--archive-file", default="")
    verify_backup.add_argument("--passphrase", default="")

    restore = subparsers.add_parser(
        "restore",
        help="Restore into a new root (writes dest; refuses to overwrite the active vault)",
    )
    restore.add_argument("--dest", required=True)
    restore.add_argument("--passphrase", required=True)
    restore.add_argument("--backup-base", default="")
    restore.add_argument("--archive-file", default="")
    restore.add_argument("--active-root", default="")
    restore.add_argument("--source-vault", default="")

    activate = subparsers.add_parser(
        "activate-restore",
        help="Rebuild derived indexes after restore (writes warehouse/serving; requires DSN)",
    )
    activate.add_argument("--vault", required=True)
    activate.add_argument("--query-uid", default="")
    activate.add_argument("--query-text", default="")

    status = subparsers.add_parser(
        "instance-status",
        help="Report native/warehouse/auth/backup capability (read-only; never claims fresh from a manifest)",
        description="Read-only capability report. A config manifest is not a freshness signal.",
    )
    status.add_argument("--instance-dir", default="", help="Optional instance directory")

    analytics = subparsers.add_parser(
        "analytics",
        help="Typed query, neighbor context, and deterministic workflows (read-only JSON)",
        description=(
            "Read-only evidence clients. Saved scopes narrow; they never widen access. "
            "Workflows return facts, citations, and completeness — not advice or FX. "
            "CLI and MCP share the same request/result contract."
        ),
    )
    analytics_sub = analytics.add_subparsers(dest="analytics_command", required=True)
    query = analytics_sub.add_parser("query", help="Typed filter query with optional saved scope")
    _add_query_flags(query)
    context = analytics_sub.add_parser("context", help="Expand matched hits with labeled neighbor context")
    context.add_argument("--cards-json", default="", help="Isolated fixture cards JSON")
    context.add_argument("--hit-uid", default="hfa-email-message-p04breply01")
    context.add_argument("--saved-scope", dest="saved_scope_name", default="")
    context.add_argument("--scopes-json", default="")
    subscriptions = analytics_sub.add_parser("subscriptions", help="Subscription lifecycle (last-observed, not current)")
    subscriptions.add_argument("--cards-json", default="", help="Isolated fixture cards JSON")
    subscriptions.add_argument("--saved-scope", dest="saved_scope_name", default="")
    subscriptions.add_argument("--scopes-json", default="")
    trip = analytics_sub.add_parser("trip-costs", help="Reconciled trip membership and costs (no FX)")
    trip.add_argument("--cards-json", default="", help="Isolated fixture cards JSON")
    trip.add_argument("--saved-scope", dest="saved_scope_name", default="")
    trip.add_argument("--scopes-json", default="")
    changes = analytics_sub.add_parser("changes-since", help="Journal changes since a checkpoint")
    changes.add_argument("--records-json", default="", help="Isolated journal records JSON")
    changes.add_argument("--snapshot-id", default="p10d-journal")
    changes.add_argument("--after-sequence", dest="after_sequence", type=int, default=0)
    changes.add_argument("--cursor", default="")


def _add_query_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--type", dest="type_filter", default="")
    parser.add_argument("--source", dest="source_filter", default="")
    parser.add_argument("--people", dest="people_filter", default="")
    parser.add_argument("--org", dest="org_filter", default="")
    parser.add_argument("--start", dest="start_date", default="")
    parser.add_argument("--end", dest="end_date", default="")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--saved-scope", dest="saved_scope_name", default="")
    parser.add_argument("--scopes-json", default="", help="Saved-scope catalog JSON (fixture or instance)")
    parser.add_argument("--snapshot", default="")
    parser.add_argument("--warehouse-checkpoint", dest="warehouse_checkpoint", default="")


def dispatch_product_command(args: argparse.Namespace) -> bool:
    """Handle a registered product command. Returns False if not one of ours."""

    command = getattr(args, "command", None)
    if command not in _PRODUCT_COMMANDS:
        return False
    try:
        payload = _dispatch(args)
    except (PpaError, ValueError, OSError) as exc:
        _fail(exc)
    except Exception as exc:
        from archive_engine.errors import ConfigError, QueryValidationError

        if isinstance(exc, (ConfigError, QueryValidationError)):
            _fail(exc)
        raise
    _print_json(payload)
    return True


def _dispatch(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "setup":
        from archive_cli.commands.setup import run_setup

        if args.non_interactive and not args.spec_path:
            raise ValueError("--non-interactive requires --from SPEC")
        if args.non_interactive and not args.apply:
            spec_path = args.spec_path
            from archive_cli.commands.setup import load_setup_spec, plan_setup

            planned = plan_setup(load_setup_spec(spec_path))
            planned["reason"] = planned.get("reason") or "review only; pass --apply to write"
            return planned
        return run_setup(
            spec_path=args.spec_path or None,
            apply=bool(args.apply),
            non_interactive=bool(args.non_interactive),
        )

    if args.command == "config":
        from archive_cli.commands import configuration as config_cmd

        instance_dir = getattr(args, "instance_dir", "") or None
        config_path = getattr(args, "config_path", "") or None
        if args.config_command == "validate":
            return config_cmd.validate_configuration(instance_dir=instance_dir, config_path=config_path)
        if args.config_command == "explain":
            return config_cmd.explain_configuration(instance_dir=instance_dir, config_path=config_path)
        if args.config_command == "migrate":
            return config_cmd.migrate_legacy_configuration(args.vault)
        return config_cmd.rollback_legacy_migration(args.vault)

    if args.command == "connect":
        return _connect_verify(source=args.source, vault=args.vault)

    if args.command == "connector":
        from archive_cli.engine_factory import trusted_local_access
        from archive_sync.connectors.cli import check_package, remaining_legacy_adapters

        if args.connector_command == "legacy-list":
            payload = remaining_legacy_adapters()
            if args.output:
                Path(args.output).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload
        from archive_cli.engine_factory import resolve_archive_identity, schema_binding_for

        vault = Path(args.vault)
        vault.mkdir(parents=True, exist_ok=True)
        identity = resolve_archive_identity(vault, schema_binding=schema_binding_for("ppa"), archive_id=args.archive_id)
        result = check_package(
            Path(args.package),
            vault=vault,
            identity=identity,
            access=trusted_local_access(identity.archive_id),
        )
        result["legacy"] = remaining_legacy_adapters()
        if args.output:
            Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return result

    if args.command == "backup":
        from archive_cli.commands.recovery import backup_archive

        return backup_archive(
            vault=Path(args.vault),
            backup_base=Path(args.backup_base),
            passphrase=args.passphrase,
            logger=_log,
            include_reconstructible=bool(args.include_reconstructible),
        )
    if args.command == "verify-backup":
        from archive_cli.commands.recovery import verify_backup

        return verify_backup(
            logger=_log,
            backup_base=Path(args.backup_base) if args.backup_base else None,
            archive_file=Path(args.archive_file) if args.archive_file else None,
            passphrase=args.passphrase or None,
        )
    if args.command == "restore":
        from archive_cli.commands.recovery import restore_archive

        return restore_archive(
            dest=Path(args.dest),
            passphrase=args.passphrase,
            logger=_log,
            backup_base=Path(args.backup_base) if args.backup_base else None,
            archive_file=Path(args.archive_file) if args.archive_file else None,
            active_root=Path(args.active_root) if args.active_root else None,
            source_vault=Path(args.source_vault) if args.source_vault else None,
        )
    if args.command == "activate-restore":
        from archive_cli.commands.recovery import activate_restored_archive

        return activate_restored_archive(
            vault=Path(args.vault),
            logger=_log,
            query_uid=args.query_uid,
            query_text=args.query_text,
        )

    if args.command == "analytics":
        from archive_cli.commands.analytics import dispatch_analytics_args

        return dispatch_analytics_args(args)

    from archive_cli.commands.setup import detect_capabilities

    vault = Path(args.instance_dir).expanduser() if args.instance_dir else None
    return detect_capabilities(vault=vault)


def _connect_verify(*, source: str, vault: str) -> dict[str, Any]:
    from archive_cli.commands.setup import ALLOWED_FIXTURES, apply_setup, detect_capabilities

    if source in ALLOWED_FIXTURES:
        if not vault:
            raise ValueError("connect verify --source sample.fixture requires --vault")
        result = apply_setup(
            {
                "root": vault,
                "entity_name": "Ada Example",
                "entity_type": "person",
                "index_schema": "ppa_fixture",
                "fixture": source,
            }
        )
        result["source"] = source
        result["live_google"] = False
        return result
    capabilities = detect_capabilities()
    return {
        "ok": True,
        "source": source,
        "status": "pending",
        "reason": "live source credentials are not used by fixture setup; no Google auth attempted",
        "capabilities": capabilities,
        "live_google": False,
        "production_proven": False,
    }

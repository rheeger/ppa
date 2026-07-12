"""``ppa source-updaters`` CLI — declarations, health, and execution (Section D)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from archive_cli.config import load_archive_config
from archive_cli.errors import IndexUnavailableError, VaultNotFoundError
from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.constants import (
    EXIT_BLOCKED,
    EXIT_REFUSED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
    GATE_SYNTHETIC_FIXTURES,
)
from archive_cli.validation_gates.guards import GateRefusalError, refuse
from archive_cli.validation_gates.instance_identity import derive_archive_instance
from archive_sync.source_updaters.constants import SECTION_D_COMPLETION_STATE, SECTION_D_EXECUTION_STATE
from archive_sync.source_updaters.declarations import (
    expand_declarations,
    iter_declaration_templates,
    validate_all_declarations,
)
from archive_sync.source_updaters.report import write_source_updater_report
from archive_sync.source_updaters.runner import default_maintain_source_keys, run_source_updater, run_source_updaters
from archive_sync.source_updaters.snapshot import snapshot_all_declarations, status_payload_for_declarations
from archive_sync.source_updaters.state_store import SourceUpdaterStateStore


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _emit(payload: dict, args: argparse.Namespace) -> None:
    if getattr(args, "format", "json") == "json":
        print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    else:
        for key, value in sorted(payload.items()):
            print(f"{key}: {value}")


def _resolve_environment(
    args: argparse.Namespace,
) -> tuple[object, str, SourceUpdaterStateStore] | int:
    from archive_cli.commands._resolve import resolve_store, resolve_vault

    vault_override = (getattr(args, "vault", None) or "").strip()
    vault_path = Path(vault_override) if vault_override else None
    try:
        store = resolve_store(vault_path)
    except VaultNotFoundError as exc:
        _emit({"blocked": True, "reason": "vault_not_found", "message": str(exc)}, args)
        return EXIT_BLOCKED
    except IndexUnavailableError:
        vault = vault_path if vault_path is not None else resolve_vault()
        if vault is None or not vault.is_dir():
            _emit(
                {
                    "blocked": True,
                    "reason": "index_unavailable",
                    "message": "PPA_INDEX_DSN is required when vault path is not configured",
                },
                args,
            )
            return EXIT_BLOCKED

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
        meta_path = vault / "_meta" / "source-updaters.json"
        state_store = SourceUpdaterStateStore(None, meta_path=meta_path)
        return store, archive_instance, state_store

    cfg = load_archive_config()
    archive_instance = derive_archive_instance(
        vault_path=str(store.vault),
        index_dsn=cfg.index_dsn,
        index_schema=store.index.schema,
        instance_role=getattr(args, "instance_role", None) or None,
    )
    meta_path = Path(store.vault) / "_meta" / "source-updaters.json"
    state_store: SourceUpdaterStateStore
    try:
        with store.index._connect() as conn:
            state_store = SourceUpdaterStateStore(conn, store.index.schema, meta_path=meta_path)
            state_store.ensure_tables()
    except (IndexUnavailableError, AttributeError, OSError):
        state_store = SourceUpdaterStateStore(None, meta_path=meta_path)
    return store, archive_instance, state_store


def _declarations_for_args(args: argparse.Namespace) -> list:
    gmail = tuple(a.strip() for a in (getattr(args, "gmail_account", None) or "").split(",") if a.strip())
    calendar = tuple(a.strip() for a in (getattr(args, "calendar_account", None) or "").split(",") if a.strip())
    if gmail or calendar:
        return expand_declarations(gmail_accounts=gmail, calendar_accounts=calendar)
    return list(iter_declaration_templates())


def cmd_status(args: argparse.Namespace) -> int:
    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, archive_instance, state_store = resolved
    validation_errors = validate_all_declarations()
    decls = _declarations_for_args(args)
    if getattr(args, "snapshot_cursors", False):
        snapshot_all_declarations(state_store, decls, vault_path=str(store.vault))
    payload = status_payload_for_declarations(
        decls,
        state_store,
        vault_path=str(store.vault),
        archive_instance=archive_instance,
        engine_mode=ppa_engine(),
    )
    payload["completion_state"] = SECTION_D_COMPLETION_STATE
    payload["declaration_validation_errors"] = validation_errors
    _emit(payload, args)
    return EXIT_SUCCESS


def cmd_report(args: argparse.Namespace) -> int:
    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, archive_instance, state_store = resolved
    source_key = (args.source or "").strip()
    if not source_key:
        _emit({"error": "source_key required", "usage": "--source gmail-messages:<account>"}, args)
        return EXIT_RUNTIME_FAILURE
    last_run = state_store.get_last_run(source_key)
    state = state_store.get_state(source_key)
    payload = {
        "completion_state": SECTION_D_COMPLETION_STATE,
        "archive_instance": archive_instance,
        "source_key": source_key,
        "state": state.to_dict() if state else None,
        "last_run": last_run,
    }
    _emit(payload, args)
    return EXIT_SUCCESS


def cmd_run(args: argparse.Namespace) -> int:
    """Execute one or more source updaters. Dry-run by default; --apply persists and advances cursors."""

    apply = bool(getattr(args, "apply", False))

    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, archive_instance, state_store = resolved

    if apply and (
        "production:" in archive_instance
        or archive_instance.startswith("production")
        or getattr(args, "instance_role", "") == "production"
    ):
        if not getattr(args, "confirm_production", False):
            try:
                refuse(
                    "Production source-updater apply requires --confirm-production",
                    reason="missing_production_confirmation",
                )
            except GateRefusalError as exc:
                _emit({"refused": True, "reason": exc.reason, "message": str(exc)}, args)
                return EXIT_REFUSED

    sources = [s.strip() for s in (getattr(args, "source", None) or []) if str(s).strip()]
    if not sources and getattr(args, "sources", None):
        sources = [s.strip() for s in str(args.sources).split(",") if s.strip()]
    if not sources:
        gmail = tuple(a.strip() for a in (getattr(args, "gmail_account", "") or "").split(",") if a.strip())
        calendar = tuple(a.strip() for a in (getattr(args, "calendar_account", "") or "").split(",") if a.strip())
        sources = default_maintain_source_keys(gmail_accounts=gmail, calendar_accounts=calendar)
    if not sources:
        _emit(
            {
                "error": "no executable sources",
                "usage": "ppa source-updaters run --source gmail-messages:<account> [--dry-run|--apply]",
            },
            args,
        )
        return EXIT_VALIDATION_FAILED

    if len(sources) == 1:
        one = run_source_updater(
            source_key=sources[0],
            vault_path=str(store.vault),
            apply=apply,
            archive_instance=archive_instance,
            engine_mode=ppa_engine(),
            ladder_gate=getattr(args, "ladder_gate", None) or GATE_SYNTHETIC_FIXTURES,
            run_id=getattr(args, "run_id", "") or "",
            repo_root=_repo_root(),
            state_store=state_store,
            max_items=getattr(args, "max_items", None),
        )
        payload = {
            "completion_state": SECTION_D_EXECUTION_STATE,
            "apply": apply,
            "dry_run": not apply,
            **one.report.to_dict(),
        }
        _emit(payload, args)
        if one.exit_hint == 4:
            return EXIT_BLOCKED
        if one.exit_hint == 2:
            return EXIT_VALIDATION_FAILED
        if one.exit_hint == 1:
            return EXIT_RUNTIME_FAILURE
        return EXIT_SUCCESS

    multi = run_source_updaters(
        source_keys=sources,
        vault_path=str(store.vault),
        apply=apply,
        archive_instance=archive_instance,
        engine_mode=ppa_engine(),
        ladder_gate=getattr(args, "ladder_gate", None) or GATE_SYNTHETIC_FIXTURES,
        repo_root=_repo_root(),
        state_store=state_store,
    )
    payload = multi.to_dict()
    payload["apply"] = apply
    payload["dry_run"] = not apply
    _emit(payload, args)
    if multi.exit_code == 4:
        return EXIT_BLOCKED
    if multi.exit_code == 2:
        return EXIT_VALIDATION_FAILED
    if multi.exit_code == 1:
        return EXIT_RUNTIME_FAILURE
    return EXIT_SUCCESS


def cmd_record_run(args: argparse.Namespace) -> int:
    """Record a run from fixture/operator input; requires gate opt-in for live mutation paths."""

    if getattr(args, "require_gate_evidence", False) and not args.allow_live_record:
        try:
            refuse(
                "source updater record-run requires --allow-live-record",
                reason="missing_source_updater_opt_in",
            )
        except GateRefusalError as exc:
            _emit({"refused": True, "reason": exc.reason, "message": str(exc)}, args)
            return EXIT_REFUSED

    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    _store, archive_instance, state_store = resolved

    from archive_sync.source_updaters.batch import SourceUpdaterBatchSummary, SourceUpdaterRunReport
    from archive_sync.source_updaters.batch import commit_cursor_after_persisted

    batch = SourceUpdaterBatchSummary(
        observed=args.observed,
        unchanged=args.unchanged,
        promoted=args.promoted,
        suppressed=args.suppressed,
        quarantined=args.quarantined,
        updated=args.updated,
    )
    report = SourceUpdaterRunReport(
        run_id=args.run_id or f"source-run-{args.source_key}",
        source_key=args.source_key,
        source_type=args.source_type,
        archive_instance=archive_instance,
        status=args.status,
        batch=batch,
        engine_mode=ppa_engine(),
        ladder_gate=args.ladder_gate or GATE_SYNTHETIC_FIXTURES,
    )
    if not args.side_effects_persisted:
        report.cursor_before = {"history_id": "1"}
        report.cursor_after = commit_cursor_after_persisted(
            side_effects_persisted=False,
            cursor_before=report.cursor_before,
            cursor_patch={"history_id": "2"},
        )
    else:
        report.cursor_after = {"history_id": "2"}

    paths = write_source_updater_report(_repo_root(), report)
    state_store.record_run(report)
    _emit({"recorded": True, "run_id": report.run_id, "artifact_paths": paths}, args)
    return EXIT_SUCCESS


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "source-updaters",
        help="Source updater declarations, health, and execution (Section D)",
    )
    sub = parser.add_subparsers(dest="source_updaters_command", required=True)

    p_status = sub.add_parser("status", help="List declarations and last known state (no sync)")
    p_status.add_argument("--vault", default="")
    p_status.add_argument("--instance-role", default="")
    p_status.add_argument("--format", choices=["text", "json"], default="json")
    p_status.add_argument("--gmail-account", default="", help="Comma-separated accounts to expand gmail declarations")
    p_status.add_argument("--calendar-account", default="", help="Comma-separated calendar accounts")
    p_status.add_argument(
        "--snapshot-cursors",
        action="store_true",
        help="Persist cursor read from sync-state into source_updater_state (no adapter fetch)",
    )
    p_status.set_defaults(func=cmd_status)

    p_report = sub.add_parser("report", help="Last run report for one source_key")
    p_report.add_argument("--source", required=True, help="source_key e.g. gmail-messages:me@example.com")
    p_report.add_argument("--vault", default="")
    p_report.add_argument("--instance-role", default="")
    p_report.add_argument("--format", choices=["text", "json"], default="json")
    p_report.set_defaults(func=cmd_report)

    p_run = sub.add_parser("run", help="Run source updater(s); dry-run by default")
    p_run.add_argument(
        "--source",
        action="append",
        default=[],
        help="source_key (repeatable), e.g. gmail-messages:me@example.com",
    )
    p_run.add_argument("--sources", default="", help="Comma-separated source_keys")
    p_run.add_argument("--vault", default="")
    p_run.add_argument("--instance-role", default="")
    p_run.add_argument("--format", choices=["text", "json"], default="json")
    p_run.add_argument("--gmail-account", default="", help="Expand gmail sources when --source omitted")
    p_run.add_argument("--calendar-account", default="", help="Expand calendar sources when --source omitted")
    p_run.add_argument("--ladder-gate", default=GATE_SYNTHETIC_FIXTURES)
    p_run.add_argument("--run-id", default="")
    p_run.add_argument("--max-items", type=int, default=None, help="Cap threads/events fetched")
    p_run.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Plan sync without writing cards or advancing cursors (default)",
    )
    p_run.add_argument(
        "--apply",
        action="store_true",
        help="Persist side effects and commit cursor after success",
    )
    p_run.add_argument(
        "--confirm-production",
        action="store_true",
        help="Required for apply when archive_instance is production",
    )
    p_run.set_defaults(func=cmd_run)

    p_record = sub.add_parser("record-run", help="Record a source run (testing/fixtures)")
    p_record.add_argument("--source-key", required=True)
    p_record.add_argument("--source-type", required=True)
    p_record.add_argument("--run-id", default="")
    p_record.add_argument("--status", default="success")
    p_record.add_argument("--vault", default="")
    p_record.add_argument("--instance-role", default="")
    p_record.add_argument("--format", choices=["text", "json"], default="json")
    p_record.add_argument("--ladder-gate", default=GATE_SYNTHETIC_FIXTURES)
    p_record.add_argument("--observed", type=int, default=0)
    p_record.add_argument("--unchanged", type=int, default=0)
    p_record.add_argument("--promoted", type=int, default=0)
    p_record.add_argument("--suppressed", type=int, default=0)
    p_record.add_argument("--quarantined", type=int, default=0)
    p_record.add_argument("--updated", type=int, default=0)
    p_record.add_argument(
        "--side-effects-persisted",
        action="store_true",
        help="When set, cursor_after may advance; otherwise cursor stays at cursor_before",
    )
    p_record.add_argument(
        "--allow-live-record",
        action="store_true",
        help="Opt-in expensive flag for record-run without gate refusal",
    )
    p_record.add_argument(
        "--require-gate-evidence",
        action="store_true",
        default=False,
        help="When set, record-run requires --allow-live-record",
    )
    p_record.set_defaults(func=cmd_record_run, require_gate_evidence=True)


def dispatch(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        print("source-updaters subcommand required", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE
    return int(func(args))

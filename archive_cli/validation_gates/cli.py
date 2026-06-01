"""``ppa gates`` CLI for validation ladder gate evidence and guards."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from archive_cli.config import load_archive_config
from archive_cli.ppa_engine import ppa_engine

from .constants import (
    EXIT_REFUSED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
    GATE_RUN_STATUS_PASSED,
    LADDER_GATES,
    PRODUCTION_INSTANCE_ROLE,
)
from .gate_registry import GateRegistry
from .guards import GateRefusalError, guard_expensive_work_opt_in, guard_production_apply
from .instance_identity import derive_archive_instance
from .readiness import evaluate_readiness
from .report import GateRunReport, write_gate_report


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_store(args: argparse.Namespace) -> tuple[Any, str, str, str]:
    from archive_cli.commands._resolve import resolve_store

    store = resolve_store(getattr(args, "vault", None))
    cfg = load_archive_config()
    archive_instance = derive_archive_instance(
        vault_path=str(store.vault),
        index_dsn=cfg.index_dsn,
        index_schema=store.index.schema,
        instance_role=getattr(args, "instance_role", None) or None,
    )
    return store, cfg.index_dsn or "", store.index.schema, archive_instance


def cmd_gate_status(args: argparse.Namespace) -> int:
    store, _, _, archive_instance = _resolve_store(args)
    with store.index._connect() as conn:
        registry = GateRegistry(conn, store.index.schema)
        registry.ensure_table()
        runs = registry.list_runs(archive_instance=archive_instance if not args.all_instances else None, limit=args.limit)
    payload = {
        "archive_instance": archive_instance,
        "engine_mode": ppa_engine(),
        "runs": [run.to_dict() for run in runs],
    }
    print(json.dumps(payload, indent=2, default=str))
    return EXIT_SUCCESS


def cmd_readiness(args: argparse.Namespace) -> int:
    store, _, _, archive_instance = _resolve_store(args)
    with store.index._connect() as conn:
        registry = GateRegistry(conn, store.index.schema)
        result = evaluate_readiness(
            registry,
            archive_instance=archive_instance,
            require_production_soak=args.require_soak,
        )
    print(json.dumps(result.to_dict(), indent=2, default=str))
    return EXIT_SUCCESS if result.ready else EXIT_VALIDATION_FAILED


def cmd_gate_record(args: argparse.Namespace) -> int:
    store, _, schema, archive_instance = _resolve_store(args)
    with store.index._connect() as conn:
        registry = GateRegistry(conn, store.index.schema)
        record = registry.create_run(
            gate=args.gate,
            archive_instance=archive_instance,
            vault_path=str(store.vault),
            index_schema=schema,
            engine_mode=ppa_engine(),
            policy_version=args.policy_version,
            input_hash=args.input_hash,
            run_id=args.run_id or None,
        )
        record = registry.complete_run(
            record.run_id,
            status=GATE_RUN_STATUS_PASSED if args.passed else args.status,
            reviewed=args.reviewed,
            approved=args.approved,
            report_path=args.report_path,
            summary_path=args.summary_path,
            error=args.error,
            applied=args.applied,
        )
    report = GateRunReport(
        run_id=record.run_id if record else args.run_id or "",
        gate=args.gate,
        ladder_gate=args.gate,
        archive_instance=archive_instance,
        vault_path=str(store.vault),
        index_schema=schema,
        engine_mode=ppa_engine(),
        policy_version=args.policy_version,
        overall_status=GATE_RUN_STATUS_PASSED if args.passed else args.status,
        next_recommended_gate=_next_gate(args.gate),
    )
    paths = write_gate_report(_repo_root(), report)
    print(json.dumps({"record": record.to_dict() if record else {}, "artifacts": paths}, indent=2, default=str))
    return EXIT_SUCCESS


def _next_gate(current_gate: str) -> str:
    try:
        idx = LADDER_GATES.index(current_gate)
    except ValueError:
        return ""
    if idx + 1 >= len(LADDER_GATES):
        return ""
    return LADDER_GATES[idx + 1]


def cmd_guard_production_apply(args: argparse.Namespace) -> int:
    """Exercise production apply guards without mutating live archive state."""

    store, _, _, archive_instance = _resolve_store(args)
    try:
        with store.index._connect() as conn:
            registry = GateRegistry(conn, store.index.schema)
            guard_production_apply(
                registry,
                decision_run_id=args.decision_run_id,
                archive_instance=archive_instance,
                confirm_production=args.confirm_production,
                instance_role=getattr(args, "instance_role", None) or None,
            )
    except GateRefusalError as exc:
        print(json.dumps({"refused": True, "reason": exc.reason, "message": str(exc)}, indent=2))
        return EXIT_REFUSED
    print(json.dumps({"refused": False, "archive_instance": archive_instance, "decision_run_id": args.decision_run_id}, indent=2))
    return EXIT_SUCCESS


def cmd_guard_expensive(args: argparse.Namespace) -> int:
    try:
        guard_expensive_work_opt_in(args.flag, True)
    except GateRefusalError as exc:
        print(json.dumps({"refused": True, "reason": exc.reason, "message": str(exc)}, indent=2))
        return EXIT_REFUSED
    print(json.dumps({"refused": False, "flag": args.flag}, indent=2))
    return EXIT_SUCCESS


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("gates", help="Validation ladder gate evidence and safety guards")
    sub = parser.add_subparsers(dest="gates_command", required=True)

    p_status = sub.add_parser("status", help="List recorded validation gate runs")
    p_status.add_argument("--vault", default="", help="Vault path override")
    p_status.add_argument("--instance-role", default="", help="Optional archive instance role prefix")
    p_status.add_argument("--all-instances", action="store_true")
    p_status.add_argument("--limit", type=int, default=50)
    p_status.set_defaults(func=cmd_gate_status)

    p_ready = sub.add_parser("readiness", help="Evaluate readiness from gate evidence")
    p_ready.add_argument("--vault", default="")
    p_ready.add_argument("--instance-role", default="")
    p_ready.add_argument("--require-soak", action="store_true")
    p_ready.set_defaults(func=cmd_readiness)

    p_record = sub.add_parser("record", help="Record gate evidence (operator/testing)")
    p_record.add_argument("--gate", required=True, choices=LADDER_GATES)
    p_record.add_argument("--vault", default="")
    p_record.add_argument("--instance-role", default="")
    p_record.add_argument("--run-id", default="")
    p_record.add_argument("--policy-version", default="")
    p_record.add_argument("--input-hash", default="")
    p_record.add_argument("--status", default=GATE_RUN_STATUS_PASSED)
    p_record.add_argument("--passed", action="store_true", help="Mark run passed (default when --status omitted)")
    p_record.add_argument("--reviewed", action="store_true")
    p_record.add_argument("--approved", action="store_true")
    p_record.add_argument("--applied", action="store_true")
    p_record.add_argument("--report-path", default="")
    p_record.add_argument("--summary-path", default="")
    p_record.add_argument("--error", default="")
    p_record.set_defaults(func=cmd_gate_record, passed=True)

    p_apply = sub.add_parser(
        "guard-production-apply",
        help="Exercise production apply refusal guards",
    )
    p_apply.add_argument("--decision-run-id", required=True)
    p_apply.add_argument("--vault", default="")
    p_apply.add_argument(
        "--instance-role",
        default="",
        help=f"Archive instance role prefix (default: PPA_ARCHIVE_INSTANCE_ROLE or {PRODUCTION_INSTANCE_ROLE!r} for guards)",
    )
    p_apply.add_argument(
        "--confirm-production",
        action="store_true",
        help="Explicit confirmation required for production apply guard checks",
    )
    p_apply.set_defaults(func=cmd_guard_production_apply)

    p_exp = sub.add_parser("guard-expensive", help="Exercise expensive-work opt-in guards")
    p_exp.add_argument(
        "--flag",
        required=True,
        choices=("full_reclassification", "full_embedding_regeneration", "all_linker_rerun"),
    )
    p_exp.set_defaults(func=cmd_guard_expensive)


def dispatch(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        print("gates subcommand required", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE
    return int(func(args))

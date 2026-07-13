"""``ppa processors`` CLI — declarations, staleness plans, and execution (Section E)."""

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
    GATE_SYNTHETIC_FIXTURES,
)
from archive_cli.validation_gates.guards import GateRefusalError, guard_expensive_work_opt_in, refuse
from archive_cli.validation_gates.instance_identity import derive_archive_instance
from archive_sync.processors.constants import (
    BROAD_LLM_PROCESSOR_KEYS,
    PROCESSOR_EMAIL_TYPED_EXTRACTION,
    PROCESSOR_EMBEDDING,
    PROCESSOR_LINKERS,
    SECTION_E_COMPLETION_STATE,
    SECTION_E_EXECUTION_STATE,
)
from archive_sync.processors.declarations import declaration_for_key, iter_processor_declarations, validate_all_declarations
from archive_sync.processors.runner import run_processors
from archive_sync.processors.state_store import ProcessorStateStore
from archive_sync.processors.status import status_payload


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
) -> tuple[object, str, ProcessorStateStore] | int:
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
        meta_path = vault / "_meta" / "processors.json"
        state_store = ProcessorStateStore(None, meta_path=meta_path)
        return store, archive_instance, state_store

    cfg = load_archive_config()
    archive_instance = derive_archive_instance(
        vault_path=str(store.vault),
        index_dsn=cfg.index_dsn,
        index_schema=store.index.schema,
        instance_role=getattr(args, "instance_role", None) or None,
    )
    meta_path = Path(store.vault) / "_meta" / "processors.json"
    try:
        # Keep connection open for the CLI command lifetime (same as source-updaters).
        conn = store.index._connect()
        state_store = ProcessorStateStore(conn, store.index.schema, meta_path=meta_path)
        state_store.ensure_tables()
        conn.commit()
        setattr(store, "_processor_conn", conn)
    except (IndexUnavailableError, AttributeError, OSError):
        state_store = ProcessorStateStore(None, meta_path=meta_path)
    return store, archive_instance, state_store


def _guard_expensive_opt_ins(processor_key: str, args: argparse.Namespace) -> None:
    """Refuse full-corpus expensive work without Section G opt-in flags."""

    if getattr(args, "require_full_embedding_opt_in", False) and processor_key == PROCESSOR_EMBEDDING:
        guard_expensive_work_opt_in("full_embedding_regeneration", getattr(args, "allow_full_embedding", False))
    if getattr(args, "require_all_linkers_opt_in", False) and processor_key == PROCESSOR_LINKERS:
        guard_expensive_work_opt_in("all_linker_rerun", getattr(args, "allow_all_linkers", False))
    if (
        processor_key in BROAD_LLM_PROCESSOR_KEYS
        and processor_key != PROCESSOR_EMAIL_TYPED_EXTRACTION
        and getattr(args, "apply", False)
        and not getattr(args, "allow_broad_llm", False)
    ):
        refuse(
            f"processor {processor_key} requires --allow-broad-llm",
            reason="missing_broad_llm_opt_in",
        )


def cmd_status(args: argparse.Namespace) -> int:
    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    _store, archive_instance, state_store = resolved
    payload = status_payload(
        state_store,
        archive_instance=archive_instance,
        engine_mode=ppa_engine(),
    )
    payload["completion_state"] = SECTION_E_COMPLETION_STATE
    _emit(payload, args)
    return EXIT_SUCCESS


def cmd_plan(args: argparse.Namespace) -> int:
    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, archive_instance, state_store = resolved

    dirty_path = (getattr(args, "dirty_uids", None) or "").strip()
    dirty_uid_list = [u.strip() for u in (getattr(args, "dirty_uid", None) or []) if u.strip()]
    processor_keys = None
    if getattr(args, "processor", None):
        processor_keys = [args.processor.strip()]

    result = run_processors(
        dirty_uids_path=Path(dirty_path) if dirty_path else None,
        dirty_uids=dirty_uid_list or None,
        vault_path=str(store.vault),
        store=store,
        state_store=state_store,
        processor_keys=processor_keys,
        apply=False,
        dry_run=True,
        run_id=args.run_id or "processor-plan-dry-run",
        archive_instance=archive_instance,
        engine_mode=ppa_engine(),
        ladder_gate=args.ladder_gate or GATE_SYNTHETIC_FIXTURES,
        repo_root=_repo_root(),
        default_card_type=getattr(args, "card_type", None) or "email_thread",
        default_processor_decision=getattr(args, "processor_decision", None) or "",
    )
    payload = {
        "completion_state": SECTION_E_COMPLETION_STATE,
        "execution_state": SECTION_E_EXECUTION_STATE,
        "archive_instance": archive_instance,
        "dry_run": True,
        "executed": False,
        "plan": result.report.plan.to_dict(),
        "artifact_paths": result.artifact_paths,
        "declaration_validation_errors": validate_all_declarations(),
    }
    _emit(payload, args)
    return EXIT_SUCCESS


def cmd_run(args: argparse.Namespace) -> int:
    """Dry-run/plan by default; ``--apply`` executes pending/stale work."""

    processor_key = (args.processor or "").strip()
    if not processor_key:
        _emit({"error": "processor key required", "usage": "--processor materialization"}, args)
        return EXIT_RUNTIME_FAILURE
    decl = declaration_for_key(processor_key)
    if decl is None:
        _emit({"error": f"unknown processor: {processor_key}"}, args)
        return EXIT_RUNTIME_FAILURE

    apply = bool(getattr(args, "apply", False))
    if apply:
        try:
            _guard_expensive_opt_ins(processor_key, args)
        except GateRefusalError as exc:
            _emit({"refused": True, "reason": exc.reason, "message": str(exc)}, args)
            return EXIT_REFUSED

    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, archive_instance, state_store = resolved

    provider_available = None
    if getattr(args, "require_provider", False) or (
        apply and decl.llm_dependent and processor_key != PROCESSOR_EMAIL_TYPED_EXTRACTION
    ):
        from archive_cli.providers import resolve_provider

        provider = resolve_provider(refresh=True)
        provider_available = bool(provider is not None and provider.is_available())
        if getattr(args, "require_provider", False) and not provider_available:
            _emit(
                {
                    "blocked": True,
                    "reason": "provider_unavailable",
                    "message": "LLM/provider unavailable for llm_dependent processor",
                    "processor_key": processor_key,
                },
                args,
            )
            return EXIT_BLOCKED

    dirty_path = (getattr(args, "dirty_uids", None) or "").strip()
    result = run_processors(
        dirty_uids_path=Path(dirty_path) if dirty_path else None,
        vault_path=str(store.vault),
        store=store,
        state_store=state_store,
        processor_keys=[processor_key],
        apply=apply,
        dry_run=not apply,
        allow_full_embedding=bool(getattr(args, "allow_full_embedding", False)),
        allow_all_linkers=bool(getattr(args, "allow_all_linkers", False)),
        allow_broad_llm=bool(getattr(args, "allow_broad_llm", False)),
        provider_available=provider_available,
        run_id=args.run_id or f"processor-run-{processor_key}",
        archive_instance=archive_instance,
        engine_mode=ppa_engine(),
        ladder_gate=args.ladder_gate or GATE_SYNTHETIC_FIXTURES,
        decision_run_id=getattr(args, "decision_run_id", "") or "",
        repo_root=_repo_root(),
    )
    payload = {
        "completion_state": SECTION_E_COMPLETION_STATE,
        "execution_state": SECTION_E_EXECUTION_STATE,
        "executed": result.executed,
        "plan_only": not result.executed,
        "processor_key": processor_key,
        "artifact_paths": result.artifact_paths,
        "plan": result.report.plan.to_dict(),
        "report": result.report.to_dict(),
        "item_results": [r.to_dict() for r in result.item_results],
        "output_count": result.report.output_count,
    }
    _emit(payload, args)
    if result.report.status == "failed":
        return EXIT_RUNTIME_FAILURE
    return EXIT_SUCCESS


def cmd_declarations(args: argparse.Namespace) -> int:
    payload = {
        "completion_state": SECTION_E_COMPLETION_STATE,
        "declarations": [d.to_dict() for d in iter_processor_declarations()],
        "declaration_validation_errors": validate_all_declarations(),
    }
    _emit(payload, args)
    return EXIT_SUCCESS


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "processors",
        help="Processor DAG declarations, staleness, and execution (Section E)",
    )
    sub = parser.add_subparsers(dest="processors_command", required=True)

    p_decl = sub.add_parser("declarations", help="List processor declarations (no execution)")
    p_decl.add_argument("--format", choices=["text", "json"], default="json")
    p_decl.set_defaults(func=cmd_declarations)

    p_status = sub.add_parser("status", help="Processor health from durable state (no execution)")
    p_status.add_argument("--vault", default="")
    p_status.add_argument("--instance-role", default="")
    p_status.add_argument("--format", choices=["text", "json"], default="json")
    p_status.set_defaults(func=cmd_status)

    p_plan = sub.add_parser("plan", help="Dry-run staleness plan for dirty inputs (no execution)")
    p_plan.add_argument("--vault", default="")
    p_plan.add_argument("--instance-role", default="")
    p_plan.add_argument("--format", choices=["text", "json"], default="json")
    p_plan.add_argument(
        "--dirty-uids",
        default="",
        help="dirty_uids.jsonl (one UID/line) or JSON snapshot list",
    )
    p_plan.add_argument("--dirty-uid", action="append", default=[], help="Inline dirty UID (repeatable)")
    p_plan.add_argument("--card-type", default="email_thread")
    p_plan.add_argument("--corpus-state", default="active")
    p_plan.add_argument("--processor-decision", default="typed_extraction")
    p_plan.add_argument("--body-sha", default="")
    p_plan.add_argument("--processor", default="", help="Limit plan to one processor key")
    p_plan.add_argument("--run-id", default="")
    p_plan.add_argument("--ladder-gate", default=GATE_SYNTHETIC_FIXTURES)
    p_plan.set_defaults(func=cmd_plan)

    p_run = sub.add_parser(
        "run",
        help="Plan by default; execute pending/stale work with --apply",
    )
    p_run.add_argument("--processor", required=True)
    p_run.add_argument("--vault", default="")
    p_run.add_argument("--instance-role", default="")
    p_run.add_argument("--format", choices=["text", "json"], default="json")
    p_run.add_argument("--run-id", default="")
    p_run.add_argument("--decision-run-id", default="")
    p_run.add_argument("--ladder-gate", default=GATE_SYNTHETIC_FIXTURES)
    p_run.add_argument(
        "--dirty-uids",
        default="",
        help="dirty_uids.jsonl (one UID/line) or JSON snapshot list",
    )
    p_run.add_argument("--apply", action="store_true", help="Execute pending/stale processor work")
    p_run.add_argument("--dry-run", action="store_true", help="Force plan-only (default without --apply)")
    p_run.add_argument("--allow-full-embedding", action="store_true")
    p_run.add_argument("--allow-all-linkers", action="store_true")
    p_run.add_argument("--allow-broad-llm", action="store_true")
    p_run.add_argument(
        "--require-full-embedding-opt-in",
        action="store_true",
        help="Refuse embedding apply without --allow-full-embedding (exit 3)",
    )
    p_run.add_argument(
        "--require-all-linkers-opt-in",
        action="store_true",
        help="Refuse linkers apply without --allow-all-linkers (exit 3)",
    )
    p_run.add_argument("--require-provider", action="store_true", help="Fail with exit 4 if provider missing")
    p_run.set_defaults(func=cmd_run)


def dispatch(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        print("processors subcommand required", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE
    return int(func(args))

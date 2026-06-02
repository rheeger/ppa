"""``ppa processors`` CLI — declarations, staleness plans, and status (Section E)."""

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
from archive_sync.processors.batch import ProcessorRunReport
from archive_sync.processors.constants import (
    BROAD_LLM_PROCESSOR_KEYS,
    EXPENSIVE_PROCESSOR_KEYS,
    SECTION_E_COMPLETION_STATE,
)
from archive_sync.processors.declarations import declaration_for_key, iter_processor_declarations, validate_all_declarations
from archive_sync.processors.plan import build_processor_plan
from archive_sync.processors.report import write_processor_report
from archive_sync.processors.staleness import ProcessorInputSnapshot
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
        with store.index._connect() as conn:
            state_store = ProcessorStateStore(conn, store.index.schema, meta_path=meta_path)
            state_store.ensure_tables()
    except (IndexUnavailableError, AttributeError, OSError):
        state_store = ProcessorStateStore(None, meta_path=meta_path)
    return store, archive_instance, state_store


def _load_inputs_from_file(path: Path) -> list[ProcessorInputSnapshot]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw if isinstance(raw, list) else raw.get("inputs", [])
    snapshots: list[ProcessorInputSnapshot] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        snapshots.append(
            ProcessorInputSnapshot(
                input_uid=str(item.get("input_uid") or ""),
                card_type=str(item.get("card_type") or ""),
                corpus_state=str(item.get("corpus_state") or "active"),
                processor_decision=str(item.get("processor_decision") or ""),
                field_values=dict(item.get("field_values") or {}),
                source_dirty=bool(item.get("source_dirty", False)),
                upstream_complete=bool(item.get("upstream_complete", True)),
                recorded_input_hash=str(item.get("recorded_input_hash") or ""),
                recorded_processor_version=str(item.get("recorded_processor_version") or ""),
                recorded_corpus_state=str(item.get("recorded_corpus_state") or ""),
                output_exists=bool(item.get("output_exists", False)),
                output_failed=bool(item.get("output_failed", False)),
                upstream_output_hash=str(item.get("upstream_output_hash") or ""),
                recorded_upstream_output_hash=str(item.get("recorded_upstream_output_hash") or ""),
            )
        )
    return snapshots


def _guard_processor_run(processor_key: str, args: argparse.Namespace) -> None:
    if processor_key in EXPENSIVE_PROCESSOR_KEYS:
        if processor_key == "embedding":
            guard_expensive_work_opt_in("full_embedding_regeneration", getattr(args, "allow_full_embedding", False))
        elif processor_key == "linkers":
            guard_expensive_work_opt_in("all_linker_rerun", getattr(args, "allow_all_linkers", False))
    if processor_key in BROAD_LLM_PROCESSOR_KEYS and not getattr(args, "allow_broad_llm", False):
        refuse(
            f"processor {processor_key} requires --allow-broad-llm",
            reason="missing_broad_llm_opt_in",
        )
    if not getattr(args, "apply", False):
        refuse(
            "processor run requires --apply (default is plan/status only)",
            reason="missing_apply_flag",
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
    _store, archive_instance, state_store = resolved

    inputs: list[ProcessorInputSnapshot] = []
    dirty_path = (getattr(args, "dirty_uids", None) or "").strip()
    if dirty_path:
        inputs = _load_inputs_from_file(Path(dirty_path))
    elif getattr(args, "dirty_uid", None):
        for uid in args.dirty_uid:
            inputs.append(
                ProcessorInputSnapshot(
                    input_uid=uid.strip(),
                    card_type=args.card_type or "email_thread",
                    corpus_state=args.corpus_state or "active",
                    processor_decision=args.processor_decision or "",
                    source_dirty=True,
                    field_values={"body_sha": args.body_sha or uid, "thread_uid": uid},
                )
            )

    processor_keys = None
    if getattr(args, "processor", None):
        processor_keys = [args.processor.strip()]

    plan = build_processor_plan(inputs, processor_keys=processor_keys)
    report = ProcessorRunReport(
        run_id=args.run_id or "processor-plan-dry-run",
        processor_key=processor_keys[0] if processor_keys else "all",
        processor_version="",
        archive_instance=archive_instance,
        status="skipped",
        input_count=plan.input_count,
        dirty_count=plan.dirty_count,
        stale_count=plan.stale_count,
        skipped_count=plan.skipped_count,
        skip_reasons=plan.skip_reasons,
        stale_reasons=plan.stale_reasons,
        plan=plan,
        engine_mode=ppa_engine(),
        ladder_gate=args.ladder_gate or GATE_SYNTHETIC_FIXTURES,
    )
    paths = write_processor_report(_repo_root(), report)
    state_store.record_run(report)
    payload = {
        "completion_state": SECTION_E_COMPLETION_STATE,
        "archive_instance": archive_instance,
        "dry_run": True,
        "executed": False,
        "plan": plan.to_dict(),
        "artifact_paths": paths,
        "declaration_validation_errors": validate_all_declarations(),
    }
    _emit(payload, args)
    return EXIT_SUCCESS


def cmd_run(args: argparse.Namespace) -> int:
    """Plan-only by default; --apply with opt-in flags required for expensive/LLM processors."""

    processor_key = (args.processor or "").strip()
    if not processor_key:
        _emit({"error": "processor key required", "usage": "--processor embedding"}, args)
        return EXIT_RUNTIME_FAILURE
    decl = declaration_for_key(processor_key)
    if decl is None:
        _emit({"error": f"unknown processor: {processor_key}"}, args)
        return EXIT_RUNTIME_FAILURE

    try:
        _guard_processor_run(processor_key, args)
    except GateRefusalError as exc:
        _emit({"refused": True, "reason": exc.reason, "message": str(exc)}, args)
        return EXIT_REFUSED

    resolved = _resolve_environment(args)
    if isinstance(resolved, int):
        return resolved
    _store, archive_instance, state_store = resolved

    if getattr(args, "require_provider", False) or decl.llm_dependent:
        from archive_cli.providers import resolve_provider

        provider = resolve_provider(refresh=True)
        if provider is None or not provider.is_available():
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

    # Section E first slice: record plan only — no broad processor execution.
    inputs: list[ProcessorInputSnapshot] = []
    if getattr(args, "dirty_uids", None):
        inputs = _load_inputs_from_file(Path(args.dirty_uids))
    plan = build_processor_plan(inputs, processor_keys=[processor_key])
    report = ProcessorRunReport(
        run_id=args.run_id or f"processor-run-{processor_key}",
        processor_key=processor_key,
        processor_version=decl.processor_version,
        archive_instance=archive_instance,
        status="skipped",
        input_count=plan.input_count,
        dirty_count=plan.dirty_count,
        stale_count=plan.stale_count,
        skipped_count=plan.skipped_count,
        skip_reasons=plan.skip_reasons,
        stale_reasons=plan.stale_reasons,
        plan=plan,
        warnings=["Section E: run records plan only; processor execution not invoked"],
        engine_mode=ppa_engine(),
        ladder_gate=args.ladder_gate or GATE_SYNTHETIC_FIXTURES,
        decision_run_id=args.decision_run_id or "",
    )
    paths = write_processor_report(_repo_root(), report)
    state_store.record_run(report)
    _emit(
        {
            "completion_state": SECTION_E_COMPLETION_STATE,
            "executed": False,
            "plan_only": True,
            "processor_key": processor_key,
            "artifact_paths": paths,
            "plan": plan.to_dict(),
        },
        args,
    )
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
        help="Processor DAG declarations, staleness, and status (Section E)",
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
    p_plan.add_argument("--dirty-uids", default="", help="JSON file with input snapshots or dirty UID list")
    p_plan.add_argument("--dirty-uid", action="append", default=[], help="Inline dirty UID (repeatable)")
    p_plan.add_argument("--card-type", default="email_thread")
    p_plan.add_argument("--corpus-state", default="active")
    p_plan.add_argument("--processor-decision", default="typed_extraction")
    p_plan.add_argument("--body-sha", default="")
    p_plan.add_argument("--processor", default="", help="Limit plan to one processor key")
    p_plan.add_argument("--run-id", default="")
    p_plan.add_argument("--ladder-gate", default=GATE_SYNTHETIC_FIXTURES)
    p_plan.set_defaults(func=cmd_plan)

    p_run = sub.add_parser("run", help="Record processor run plan (execution requires opt-in flags)")
    p_run.add_argument("--processor", required=True)
    p_run.add_argument("--vault", default="")
    p_run.add_argument("--instance-role", default="")
    p_run.add_argument("--format", choices=["text", "json"], default="json")
    p_run.add_argument("--run-id", default="")
    p_run.add_argument("--decision-run-id", default="")
    p_run.add_argument("--ladder-gate", default=GATE_SYNTHETIC_FIXTURES)
    p_run.add_argument("--dirty-uids", default="")
    p_run.add_argument("--apply", action="store_true", help="Required to proceed past plan-only refusal")
    p_run.add_argument("--allow-full-embedding", action="store_true")
    p_run.add_argument("--allow-all-linkers", action="store_true")
    p_run.add_argument("--allow-broad-llm", action="store_true")
    p_run.add_argument("--require-provider", action="store_true", help="Fail with exit 4 if provider missing")
    p_run.set_defaults(func=cmd_run)


def dispatch(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        print("processors subcommand required", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE
    return int(func(args))

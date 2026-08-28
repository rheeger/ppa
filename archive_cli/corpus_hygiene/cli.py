"""``ppa corpus-hygiene`` CLI — dry-run census and staging apply/rollback (Section B)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from archive_cli.config import load_archive_config
from archive_cli.errors import IndexUnavailableError, VaultNotFoundError
from archive_cli.ppa_engine import ppa_engine
from archive_cli.store import DefaultArchiveStore
from archive_cli.validation_gates.constants import (
    EXIT_BLOCKED,
    EXIT_REFUSED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    EXIT_VALIDATION_FAILED,
    GATE_LOCAL_SEED_DRY_RUN,
    GATE_SYNTHETIC_FIXTURES,
)
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.guards import GateRefusalError, guard_expensive_work_opt_in
from archive_cli.validation_gates.instance_identity import derive_archive_instance

from .apply import apply_from_decisions_path
from .census import (
    CensusContext,
    load_card_classifications_from_db,
    load_threads_from_vault_cache,
    run_email_census_dry_run,
)
from .constants import SECTION_B_APPLY_COMPLETION_STATE, SECTION_B_COMPLETION_STATE
from .guards import guard_corpus_hygiene_apply, guard_corpus_hygiene_rollback
from .restore_quarantine import restore_quarantine_from_decision_run
from .rollback import run_email_corpus_rollback


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_store(args: argparse.Namespace) -> DefaultArchiveStore:
    from archive_cli.commands._resolve import resolve_store

    vault_override = (getattr(args, "vault", None) or "").strip()
    vault_path = Path(vault_override) if vault_override else None
    return resolve_store(vault_path)


def _emit_cli_payload(payload: dict, args: argparse.Namespace) -> None:
    """Print structured JSON for operator-facing apply/rollback outcomes."""

    print(json.dumps(payload, indent=2, sort_keys=True))


def _resolve_apply_rollback_environment(
    args: argparse.Namespace,
) -> tuple[DefaultArchiveStore, str] | int:
    """Resolve vault, index, and archive instance; return exit code on dependency failure."""

    try:
        store = _resolve_store(args)
        cfg = load_archive_config()
        archive_instance = derive_archive_instance(
            vault_path=str(store.vault),
            index_dsn=cfg.index_dsn,
            index_schema=store.index.schema,
            instance_role=getattr(args, "instance_role", None) or None,
        )
        return store, archive_instance
    except VaultNotFoundError as exc:
        _emit_cli_payload(
            {"blocked": True, "reason": "vault_not_found", "message": str(exc)},
            args,
        )
        return EXIT_BLOCKED
    except IndexUnavailableError as exc:
        _emit_cli_payload(
            {"blocked": True, "reason": "index_unavailable", "message": str(exc)},
            args,
        )
        return EXIT_BLOCKED


def cmd_email_census(args: argparse.Namespace) -> int:
    if not args.dry_run:
        print("corpus-hygiene email census requires --dry-run", file=sys.stderr)
        return EXIT_REFUSED

    store = _resolve_store(args)
    cfg = load_archive_config()
    archive_instance = derive_archive_instance(
        vault_path=str(store.vault),
        index_dsn=cfg.index_dsn,
        index_schema=store.index.schema,
        instance_role=getattr(args, "instance_role", None) or None,
    )
    gate = GATE_LOCAL_SEED_DRY_RUN if args.seed_scale else GATE_SYNTHETIC_FIXTURES
    ladder = "Local seed dry-run" if args.seed_scale else "Small slice"

    decision_run_id = args.decision_run_id.strip() if args.decision_run_id else ""
    with store.index._connect() as conn:
        registry = GateRegistry(conn, store.index.schema)
        if not decision_run_id:
            record = registry.create_run(
                gate=gate,
                archive_instance=archive_instance,
                vault_path=str(store.vault),
                index_schema=store.index.schema,
                engine_mode=ppa_engine(),
                policy_version="email-promotion-v1",
            )
            decision_run_id = record.run_id
        card_rows = load_card_classifications_from_db(conn, store.index.schema)

    try:
        if args.allow_new_llm:
            guard_expensive_work_opt_in("full_reclassification", True)
    except GateRefusalError as exc:
        print(json.dumps({"refused": True, "reason": exc.reason, "message": str(exc)}, indent=2))
        return EXIT_REFUSED

    threads = load_threads_from_vault_cache(store.vault)
    context = CensusContext(
        vault_path=str(store.vault),
        index_schema=store.index.schema,
        archive_instance=archive_instance,
        engine_mode=ppa_engine(),
        gate=gate,
        ladder_gate=ladder,
        decision_run_id=decision_run_id,
        allow_new_llm=args.allow_new_llm,
    )

    with store.index._connect() as conn:
        registry = GateRegistry(conn, store.index.schema)
        result = run_email_census_dry_run(
            threads,
            context=context,
            card_classification_rows=card_rows,
            register_gate=registry,
            repo_root=_repo_root(),
        )

    payload = {
        "completion_state": SECTION_B_COMPLETION_STATE,
        "decision_run_id": decision_run_id,
        "classification_source_counts": result.classification_source_counts,
        "new_llm_call_count": result.new_llm_call_count,
        "corpus_counts": result.corpus_counts,
        "artifact_paths": result.artifact_paths,
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"decision_run_id: {decision_run_id}")
        print(f"threads_evaluated: {len(result.records)}")
        print(f"new_llm_calls: {result.new_llm_call_count}")
        for k, v in sorted(result.corpus_counts.items()):
            print(f"  {k}: {v}")
        if result.artifact_paths:
            print(f"report: {result.artifact_paths.get('report', '')}")
    return EXIT_SUCCESS


def cmd_email_apply(args: argparse.Namespace) -> int:
    resolved = _resolve_apply_rollback_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, archive_instance = resolved
    decision_run_id = args.decision_run_id.strip()
    repo_root = _repo_root()

    try:
        with store.index._connect() as conn:
            registry = GateRegistry(conn, store.index.schema)
            _record, decisions_path = guard_corpus_hygiene_apply(
                registry,
                decision_run_id=decision_run_id,
                archive_instance=archive_instance,
                repo_root=repo_root,
                confirm_production=args.confirm_production,
                instance_role=getattr(args, "instance_role", None) or None,
            )
            result = apply_from_decisions_path(
                conn,
                store.index.schema,
                decisions_path,
                decision_run_id=decision_run_id,
                archive_instance=archive_instance,
                vault_path=str(store.vault),
                engine_mode=ppa_engine(),
                repo_root=repo_root,
                registry=registry,
                ccs_only=bool(getattr(args, "ccs_only", False)),
            )
    except GateRefusalError as exc:
        _emit_cli_payload({"refused": True, "reason": exc.reason, "message": str(exc)}, args)
        return EXIT_REFUSED
    except (ValueError, FileNotFoundError) as exc:
        _emit_cli_payload({"error": str(exc), "validation_failed": True}, args)
        return EXIT_VALIDATION_FAILED

    payload = {
        "completion_state": SECTION_B_APPLY_COMPLETION_STATE,
        "decision_run_id": decision_run_id,
        "threads_applied": result.counts.threads_applied,
        "cards_updated": result.counts.cards_updated,
        "files_deleted": result.counts.files_deleted,
        "uids_purged": result.counts.uids_purged,
        "vault_markdown_deleted": result.vault_markdown_deleted,
        "artifact_paths": result.artifact_paths,
        "rollback_path": result.rollback_path,
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"applied decision_run_id: {decision_run_id}")
        print(f"cards_updated: {result.counts.cards_updated}")
        if result.artifact_paths:
            print(f"report: {result.artifact_paths.get('report', '')}")
    return EXIT_SUCCESS


def cmd_email_rollback(args: argparse.Namespace) -> int:
    resolved = _resolve_apply_rollback_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, archive_instance = resolved
    decision_run_id = args.decision_run_id.strip()

    try:
        with store.index._connect() as conn:
            registry = GateRegistry(conn, store.index.schema)
            guard_corpus_hygiene_rollback(
                registry,
                decision_run_id=decision_run_id,
                archive_instance=archive_instance,
                instance_role=getattr(args, "instance_role", None) or None,
            )
            result = run_email_corpus_rollback(
                conn,
                store.index.schema,
                decision_run_id=decision_run_id,
                archive_instance=archive_instance,
                vault_path=str(store.vault),
                engine_mode=ppa_engine(),
                repo_root=_repo_root(),
            )
    except GateRefusalError as exc:
        _emit_cli_payload({"refused": True, "reason": exc.reason, "message": str(exc)}, args)
        return EXIT_REFUSED
    except (ValueError, FileNotFoundError) as exc:
        _emit_cli_payload({"error": str(exc), "validation_failed": True}, args)
        return EXIT_VALIDATION_FAILED

    payload = {
        "decision_run_id": decision_run_id,
        "cards_restored": result.counts.cards_restored,
        "artifact_paths": result.artifact_paths,
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"rollback decision_run_id: {decision_run_id}")
        print(f"cards_restored: {result.counts.cards_restored}")
    return EXIT_SUCCESS


def cmd_email_restore_quarantine(args: argparse.Namespace) -> int:
    resolved = _resolve_apply_rollback_environment(args)
    if isinstance(resolved, int):
        return resolved
    store, _archive_instance = resolved
    source_vault = Path(args.source_vault).expanduser().resolve()
    if not source_vault.is_dir():
        _emit_cli_payload(
            {"error": f"source vault not found: {source_vault}", "validation_failed": True},
            args,
        )
        return EXIT_VALIDATION_FAILED
    skip_rebuild = bool(getattr(args, "skip_rebuild", False))
    try:
        counts = restore_quarantine_from_decision_run(
            decision_run_id=args.decision_run_id.strip(),
            repo_root=_repo_root(),
            source_vault=source_vault,
            dest_vault=Path(store.vault),
            rematerialize=not skip_rebuild,
            store=None if skip_rebuild else store,
            progress_every=int(getattr(args, "progress_every", 200) or 200),
        )
    except (ValueError, FileNotFoundError) as exc:
        _emit_cli_payload({"error": str(exc), "validation_failed": True}, args)
        return EXIT_VALIDATION_FAILED

    payload = {
        "decision_run_id": args.decision_run_id.strip(),
        "quarantine_threads": counts.quarantine_threads,
        "uids_requested": counts.uids_requested,
        "paths_mapped": counts.paths_mapped,
        "files_copied": counts.files_copied,
        "files_already_present": counts.files_already_present,
        "files_missing_from_source": counts.files_missing_from_source,
        "uids_unmapped": counts.uids_unmapped,
        "ledger_lines_dropped": counts.ledger_lines_dropped,
        "rematerialized": counts.rematerialized,
        "rematerialize_counts": counts.rematerialize_counts,
    }
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"restore-quarantine decision_run_id: {args.decision_run_id.strip()}")
        print(f"files_copied: {counts.files_copied}")
        print(f"ledger_lines_dropped: {counts.ledger_lines_dropped}")
        print(f"rematerialized: {counts.rematerialized}")
    return EXIT_SUCCESS


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "corpus-hygiene",
        help="Email corpus hygiene dry-run census and staging apply/rollback (Section B)",
    )
    sub = parser.add_subparsers(dest="corpus_hygiene_command", required=True)

    census = sub.add_parser("email", help="Email corpus hygiene commands")
    email_sub = census.add_subparsers(dest="email_command", required=True)

    p_census = email_sub.add_parser("census", help="Dry-run email corpus census")
    p_census.add_argument("--dry-run", action="store_true", default=True, help="Dry-run only (required)")
    p_census.add_argument("--vault", default="", help="Vault path override")
    p_census.add_argument("--instance-role", default="", help="Archive instance role prefix")
    p_census.add_argument("--format", choices=["text", "json"], default="text")
    p_census.add_argument("--decision-run-id", default="", help="Reuse an existing gate run id")
    p_census.add_argument(
        "--seed-scale",
        action="store_true",
        help="Record gate as local_seed_dry_run instead of small_slice",
    )
    p_census.add_argument(
        "--allow-new-llm",
        action="store_true",
        help="Opt-in to new LLM classification for missing threads (expensive)",
    )
    p_census.set_defaults(func=cmd_email_census)

    p_apply = email_sub.add_parser("apply", help="Apply a reviewed dry-run decision run (staging/slice)")
    p_apply.add_argument("--decision-run-id", required=True)
    p_apply.add_argument("--vault", default="")
    p_apply.add_argument("--instance-role", default="")
    p_apply.add_argument("--format", choices=["text", "json"], default="text")
    p_apply.add_argument(
        "--confirm-production",
        action="store_true",
        help="Required for production instance apply (Arnold gate chain)",
    )
    p_apply.add_argument(
        "--ccs-only",
        action="store_true",
        help="Write corpus state only; skip vault delete, index purge, and promotion ledger",
    )
    p_apply.set_defaults(func=cmd_email_apply)

    p_rb = email_sub.add_parser("rollback", help="Rollback a staging apply decision run")
    p_rb.add_argument("--decision-run-id", required=True)
    p_rb.add_argument("--vault", default="")
    p_rb.add_argument("--instance-role", default="")
    p_rb.add_argument("--format", choices=["text", "json"], default="text")
    p_rb.set_defaults(func=cmd_email_rollback)

    p_rq = email_sub.add_parser(
        "restore-quarantine",
        help="Copy vault-removed quarantine notes back from a read-only source vault",
    )
    p_rq.add_argument("--decision-run-id", required=True)
    p_rq.add_argument("--source-vault", required=True, help="Read-only vault that still has the notes")
    p_rq.add_argument("--vault", default="")
    p_rq.add_argument("--instance-role", default="")
    p_rq.add_argument("--format", choices=["text", "json"], default="text")
    p_rq.add_argument(
        "--skip-rebuild",
        action="store_true",
        help="Copy files and rewrite the ledger only; skip uid_allowlist rematerialize",
    )
    p_rq.add_argument("--progress-every", type=int, default=200)
    p_rq.set_defaults(func=cmd_email_restore_quarantine)


def dispatch(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        print("corpus-hygiene subcommand required", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE
    return int(func(args))

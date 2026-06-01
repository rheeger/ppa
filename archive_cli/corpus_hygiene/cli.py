"""``ppa corpus-hygiene`` CLI — dry-run census (Section B)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from archive_cli.config import load_archive_config
from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.constants import (
    EXIT_REFUSED,
    EXIT_RUNTIME_FAILURE,
    EXIT_SUCCESS,
    GATE_LOCAL_SEED_DRY_RUN,
    GATE_SYNTHETIC_FIXTURES,
)
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.guards import GateRefusalError, guard_expensive_work_opt_in
from archive_cli.validation_gates.instance_identity import derive_archive_instance

from .census import (
    CensusContext,
    load_card_classifications_from_db,
    load_threads_from_vault_cache,
    run_email_census_dry_run,
)
from .constants import SECTION_B_COMPLETION_STATE


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_store(args: argparse.Namespace):
    from archive_cli.commands._resolve import resolve_store

    return resolve_store(getattr(args, "vault", None) or None)


def cmd_email_census(args: argparse.Namespace) -> int:
    if not args.dry_run:
        print("corpus-hygiene email census requires --dry-run (apply not implemented)", file=sys.stderr)
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

    registry: GateRegistry | None = None
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


def cmd_email_apply(_args: argparse.Namespace) -> int:
    print("corpus-hygiene email apply is not implemented — dry-run only in Section B", file=sys.stderr)
    return EXIT_REFUSED


def cmd_email_rollback(_args: argparse.Namespace) -> int:
    print("corpus-hygiene email rollback is not implemented — dry-run only in Section B", file=sys.stderr)
    return EXIT_REFUSED


def add_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(
        "corpus-hygiene",
        help="Email corpus hygiene dry-run census (Section B)",
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

    p_apply = email_sub.add_parser("apply", help="Apply reviewed decision run (not yet available)")
    p_apply.add_argument("--decision-run-id", required=True)
    p_apply.set_defaults(func=cmd_email_apply)

    p_rb = email_sub.add_parser("rollback", help="Rollback decision run (not yet available)")
    p_rb.add_argument("--decision-run-id", required=True)
    p_rb.set_defaults(func=cmd_email_rollback)


def dispatch(args: argparse.Namespace) -> int:
    func = getattr(args, "func", None)
    if func is None:
        print("corpus-hygiene subcommand required", file=sys.stderr)
        return EXIT_RUNTIME_FAILURE
    return int(func(args))

"""Run the Archive MCP server or index maintenance commands."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from .benchmark import (
    BENCHMARK_PROFILES,
    DEFAULT_BENCHMARK_SOURCE_VAULT,
    benchmark_rebuild,
    benchmark_seed_links,
    build_benchmark_sample,
)
from .commands import explain
from .commands import graph as graph_cmd
from .commands import query as query_cmd
from .commands import read as read_cmd
from .commands import search as search_cmd
from .commands import status as status_cmd
from .commands._resolve import resolve_index, resolve_store
from .errors import PpaError
from .index_config import get_seed_links_enabled
from .log import configure_logging
from .server import (
    archive_bootstrap_postgres,
    archive_duplicate_uids,
    archive_embed_pending,
    archive_index_status,
    archive_link_candidate,
    archive_link_candidates,
    archive_link_quality_gate,
    archive_projection_explain,
    archive_projection_inventory,
    archive_projection_status,
    archive_review_link_candidate,
    archive_seed_link_backfill,
    archive_seed_link_enqueue,
    archive_seed_link_promote,
    archive_seed_link_refresh,
    archive_seed_link_report,
    archive_seed_link_surface,
    archive_seed_link_worker,
    mcp,
)
from .store import get_archive_store

_cli_log = logging.getLogger("ppa.cli")


def _print_json(data: object) -> None:
    print(json.dumps(data, indent=2, default=str))


def _cli_fail(exc: PpaError) -> None:
    print(str(exc), file=sys.stderr)
    raise SystemExit(1)


_SEED_LINKS_DISABLED_MSG = "Seed links are not enabled. Set PPA_SEED_LINKS_ENABLED=1 to enable."

_SEED_LINK_COMMANDS = frozenset(
    {
        "seed-link-surface",
        "seed-link-enqueue",
        "seed-link-backfill",
        "seed-link-refresh",
        "seed-link-worker",
        "seed-link-promote",
        "seed-link-report",
        "link-candidates",
        "link-candidate",
        "review-link-candidate",
        "link-quality-gate",
        "benchmark-seed-links",
    }
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive MCP server and index maintenance")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging on stderr")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("serve")
    rebuild_parser = subparsers.add_parser("rebuild-indexes")
    rebuild_parser.add_argument("--workers", type=int)
    rebuild_parser.add_argument("--batch-size", type=int)
    rebuild_parser.add_argument("--commit-interval", type=int)
    rebuild_parser.add_argument("--progress-every", type=int)
    rebuild_parser.add_argument("--executor", dest="executor_kind", choices=["serial", "thread", "process"])
    rebuild_parser.add_argument("--force-full-rebuild", action="store_true")
    rebuild_parser.add_argument("--disable-manifest-cache", action="store_true")
    subparsers.add_parser("index-status")
    subparsers.add_parser("projection-inventory")
    subparsers.add_parser("projection-status")
    projection_explain_parser = subparsers.add_parser("projection-explain")
    projection_explain_parser.add_argument("card_uid")
    subparsers.add_parser("bootstrap-postgres")
    embed_parser = subparsers.add_parser("embed-pending")
    embed_parser.add_argument("--limit", type=int, default=0)
    embed_parser.add_argument("--embedding-model", default="")
    embed_parser.add_argument("--embedding-version", type=int, default=0)
    subparsers.add_parser("seed-link-surface")
    seed_link_enqueue_parser = subparsers.add_parser("seed-link-enqueue")
    seed_link_enqueue_parser.add_argument("--modules", default="")
    seed_link_enqueue_parser.add_argument("--source-uids", default="")
    seed_link_enqueue_parser.add_argument("--job-type", default="seed_backfill")
    seed_link_enqueue_parser.add_argument("--reset-existing", action="store_true")
    seed_link_parser = subparsers.add_parser("seed-link-backfill")
    seed_link_parser.add_argument("--limit", type=int, default=0)
    seed_link_parser.add_argument("--modules", default="")
    seed_link_parser.add_argument("--workers", type=int, default=0)
    seed_link_parser.add_argument("--include-llm", action="store_true")
    seed_link_parser.add_argument("--no-apply-promotions", action="store_true")
    seed_link_refresh_parser = subparsers.add_parser("seed-link-refresh")
    seed_link_refresh_parser.add_argument("--source-uids", required=True)
    seed_link_refresh_parser.add_argument("--modules", default="")
    seed_link_refresh_parser.add_argument("--workers", type=int, default=0)
    seed_link_refresh_parser.add_argument("--include-llm", action="store_true")
    seed_link_refresh_parser.add_argument("--no-apply-promotions", action="store_true")
    seed_link_worker_parser = subparsers.add_parser("seed-link-worker")
    seed_link_worker_parser.add_argument("--limit", type=int, default=0)
    seed_link_worker_parser.add_argument("--modules", default="")
    seed_link_worker_parser.add_argument("--workers", type=int, default=0)
    seed_link_worker_parser.add_argument("--include-llm", action="store_true")
    seed_link_promote_parser = subparsers.add_parser("seed-link-promote")
    seed_link_promote_parser.add_argument("--limit", type=int, default=0)
    seed_link_promote_parser.add_argument("--workers", type=int, default=1)
    seed_link_report_parser = subparsers.add_parser("seed-link-report")
    seed_link_report_parser.add_argument("--no-rebuild-if-dirty", action="store_true")
    link_candidates_parser = subparsers.add_parser("link-candidates")
    link_candidates_parser.add_argument("--status", default="")
    link_candidates_parser.add_argument("--module-name", default="")
    link_candidates_parser.add_argument("--min-confidence", type=float, default=0.0)
    link_candidates_parser.add_argument("--limit", type=int, default=20)
    link_candidate_parser = subparsers.add_parser("link-candidate")
    link_candidate_parser.add_argument("candidate_id", type=int)
    link_review_parser = subparsers.add_parser("review-link-candidate")
    link_review_parser.add_argument("candidate_id", type=int)
    link_review_parser.add_argument("--reviewer", required=True)
    link_review_parser.add_argument("--action", required=True)
    link_review_parser.add_argument("--notes", default="")
    duplicate_uid_parser = subparsers.add_parser("duplicate-uids")
    duplicate_uid_parser.add_argument("--limit", type=int, default=20)
    subparsers.add_parser("link-quality-gate")

    sample_parser = subparsers.add_parser("build-benchmark-sample")
    sample_parser.add_argument("--source-vault", default=str(DEFAULT_BENCHMARK_SOURCE_VAULT))
    sample_parser.add_argument("--output-vault", required=True)
    sample_parser.add_argument("--per-group-limit", type=int, default=200)
    sample_parser.add_argument("--max-notes", type=int, default=5000)
    sample_parser.add_argument("--neighborhood-hops", type=int, default=1)
    sample_parser.add_argument("--oversample-factor", type=int, default=8)
    sample_parser.add_argument("--sample-percent", type=float, default=0.0)

    bench_parser = subparsers.add_parser("benchmark-rebuild")
    bench_parser.add_argument("--vault", required=True)
    bench_parser.add_argument("--schema", default="archive_benchmark")
    bench_parser.add_argument("--profile", choices=sorted(BENCHMARK_PROFILES), default="local-laptop")
    bench_parser.add_argument("--workers", type=int)
    bench_parser.add_argument("--batch-size", type=int)
    bench_parser.add_argument("--commit-interval", type=int)
    bench_parser.add_argument("--progress-every", type=int)
    bench_parser.add_argument("--executor", dest="executor_kind", choices=["serial", "thread", "process"])

    seed_bench_parser = subparsers.add_parser("benchmark-seed-links")
    seed_bench_parser.add_argument("--vault", required=True)
    seed_bench_parser.add_argument("--schema", default="archive_seed_links_benchmark")
    seed_bench_parser.add_argument("--profile", choices=sorted(BENCHMARK_PROFILES), default="local-laptop")
    seed_bench_parser.add_argument("--workers", type=int)
    seed_bench_parser.add_argument("--batch-size", type=int)
    seed_bench_parser.add_argument("--commit-interval", type=int)
    seed_bench_parser.add_argument("--progress-every", type=int)
    seed_bench_parser.add_argument("--executor", dest="executor_kind", choices=["serial", "thread", "process"])
    seed_bench_parser.add_argument("--include-llm", action="store_true")
    seed_bench_parser.add_argument("--apply-promotions", action="store_true")
    seed_bench_parser.add_argument("--modules", default="")
    seed_bench_parser.add_argument("--no-rebuild-first", action="store_true")

    migrate_parser = subparsers.add_parser("migrate")
    migrate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show pending migrations without applying",
    )
    subparsers.add_parser("migration-status")
    subparsers.add_parser("health")

    search_parser = subparsers.add_parser("search", help="Full-text search (JSON on stdout)")
    search_parser.add_argument("query")
    search_parser.add_argument("--limit", type=int, default=20)
    read_parser = subparsers.add_parser("read", help="Read one note by path or UID (JSON)")
    read_parser.add_argument("path_or_uid")
    read_many_parser = subparsers.add_parser("read-many", help="Read multiple notes (JSON)")
    read_many_parser.add_argument("paths", nargs="+", metavar="PATH_OR_UID")
    query_parser = subparsers.add_parser("query", help="Structured query (JSON)")
    query_parser.add_argument("--type", dest="type_filter", default="")
    query_parser.add_argument("--source", dest="source_filter", default="")
    query_parser.add_argument("--people", dest="people_filter", default="")
    query_parser.add_argument("--org", dest="org_filter", default="")
    query_parser.add_argument("--limit", type=int, default=20)
    graph_parser = subparsers.add_parser("graph", help="Wikilink graph from a note (JSON)")
    graph_parser.add_argument("note_path")
    graph_parser.add_argument("--hops", type=int, default=2)
    person_parser = subparsers.add_parser("person", help="Person profile by slug (JSON)")
    person_parser.add_argument("name")
    timeline_parser = subparsers.add_parser("timeline", help="Notes in date range (JSON)")
    timeline_parser.add_argument("--start", dest="start_date", default="")
    timeline_parser.add_argument("--end", dest="end_date", default="")
    timeline_parser.add_argument("--limit", type=int, default=20)
    subparsers.add_parser("stats", help="Vault/index stats (JSON)")
    subparsers.add_parser("validate", help="Validate all vault cards (JSON)")
    subparsers.add_parser("duplicates", help="Dedup candidates from _meta (JSON)")
    vec_parser = subparsers.add_parser("vector-search", help="Semantic search (JSON)")
    vec_parser.add_argument("query")
    vec_parser.add_argument("--limit", type=int, default=20)
    vec_parser.add_argument("--embedding-model", default="")
    vec_parser.add_argument("--embedding-version", type=int, default=0)
    vec_parser.add_argument("--type", dest="type_filter", default="")
    vec_parser.add_argument("--source", dest="source_filter", default="")
    vec_parser.add_argument("--people", dest="people_filter", default="")
    vec_parser.add_argument("--start-date", dest="start_date", default="")
    vec_parser.add_argument("--end-date", dest="end_date", default="")
    hybrid_parser = subparsers.add_parser("hybrid-search", help="Hybrid lexical+vector (JSON)")
    hybrid_parser.add_argument("query")
    hybrid_parser.add_argument("--limit", type=int, default=20)
    hybrid_parser.add_argument("--embedding-model", default="")
    hybrid_parser.add_argument("--embedding-version", type=int, default=0)
    hybrid_parser.add_argument("--type", dest="type_filter", default="")
    hybrid_parser.add_argument("--source", dest="source_filter", default="")
    hybrid_parser.add_argument("--people", dest="people_filter", default="")
    hybrid_parser.add_argument("--start-date", dest="start_date", default="")
    hybrid_parser.add_argument("--end-date", dest="end_date", default="")
    explain_parser = subparsers.add_parser("explain", help="Retrieval explain payload (JSON)")
    explain_parser.add_argument("query")
    explain_parser.add_argument("--mode", default="hybrid")
    explain_parser.add_argument("--limit", type=int, default=10)
    explain_parser.add_argument("--embedding-model", default="")
    explain_parser.add_argument("--embedding-version", type=int, default=0)
    emb_stat_parser = subparsers.add_parser("embedding-status", help="Embedding coverage (JSON)")
    emb_stat_parser.add_argument("--embedding-model", default="")
    emb_stat_parser.add_argument("--embedding-version", type=int, default=0)
    emb_back_parser = subparsers.add_parser("embedding-backlog", help="Pending embedding chunks (JSON)")
    emb_back_parser.add_argument("--limit", type=int, default=20)
    emb_back_parser.add_argument("--embedding-model", default="")
    emb_back_parser.add_argument("--embedding-version", type=int, default=0)
    subparsers.add_parser(
        "status",
        help="Index and runtime status as JSON (same as archive_status_json MCP tool)",
    )

    parser.set_defaults(command="serve")
    args = parser.parse_args()
    # Stderr-only logging for all subcommands; keep stdout for MCP JSON-RPC / CLI JSON. See archive_mcp/log.py.
    configure_logging(verbose=args.verbose)
    if args.command == "health":
        from .health import run_health_checks

        result = run_health_checks()
        print(json.dumps(result, indent=2))
        raise SystemExit(0 if result["ok"] else 1)
    if args.command == "search":
        try:
            store = resolve_store()
            out = search_cmd.search(args.query, limit=args.limit, store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "read":
        try:
            store = resolve_store()
            out = read_cmd.read(args.path_or_uid, store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "read-many":
        try:
            store = resolve_store()
            out = read_cmd.read_many(args.paths, store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "query":
        try:
            store = resolve_store()
            out = query_cmd.query(
                type_filter=args.type_filter,
                source_filter=args.source_filter,
                people_filter=args.people_filter,
                org_filter=args.org_filter,
                limit=args.limit,
                store=store,
                logger=_cli_log,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "graph":
        try:
            store = resolve_store()
            out = graph_cmd.graph(args.note_path, hops=args.hops, store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "person":
        try:
            store = resolve_store()
            out = graph_cmd.person(args.name, store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "timeline":
        try:
            store = resolve_store()
            out = graph_cmd.timeline(
                start_date=args.start_date,
                end_date=args.end_date,
                limit=args.limit,
                store=store,
                logger=_cli_log,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "stats":
        try:
            index = resolve_index()
            out = status_cmd.stats(index=index, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "validate":
        try:
            vault = resolve_store().vault
            out = status_cmd.validate(vault=vault, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "duplicates":
        try:
            vault = resolve_store().vault
            out = status_cmd.duplicates(vault=vault, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "vector-search":
        try:
            store = resolve_store()
            out = search_cmd.vector_search(
                args.query,
                store=store,
                logger=_cli_log,
                limit=args.limit,
                embedding_model=args.embedding_model,
                embedding_version=args.embedding_version,
                type_filter=args.type_filter,
                source_filter=args.source_filter,
                people_filter=args.people_filter,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "hybrid-search":
        try:
            store = resolve_store()
            out = search_cmd.hybrid_search(
                args.query,
                store=store,
                logger=_cli_log,
                limit=args.limit,
                embedding_model=args.embedding_model,
                embedding_version=args.embedding_version,
                type_filter=args.type_filter,
                source_filter=args.source_filter,
                people_filter=args.people_filter,
                start_date=args.start_date,
                end_date=args.end_date,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "explain":
        try:
            store = resolve_store()
            out = explain.retrieval_explain(
                args.query,
                store=store,
                logger=_cli_log,
                mode=args.mode,
                limit=args.limit,
                embedding_model=args.embedding_model,
                embedding_version=args.embedding_version,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "embedding-status":
        try:
            store = resolve_store()
            out = status_cmd.embedding_status(
                store=store,
                logger=_cli_log,
                embedding_model=args.embedding_model,
                embedding_version=args.embedding_version,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "embedding-backlog":
        try:
            store = resolve_store()
            out = status_cmd.embedding_backlog(
                store=store,
                logger=_cli_log,
                limit=args.limit,
                embedding_model=args.embedding_model,
                embedding_version=args.embedding_version,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "status":
        try:
            store = resolve_store()
            out = status_cmd.status_json(store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command in _SEED_LINK_COMMANDS and not get_seed_links_enabled():
        print(_SEED_LINKS_DISABLED_MSG)
        return
    if args.command == "embed-pending":
        kwargs: dict[str, object] = {}
        kwargs["limit"] = args.limit
        if args.embedding_model:
            kwargs["embedding_model"] = args.embedding_model
        if args.embedding_version:
            kwargs["embedding_version"] = args.embedding_version
        print(archive_embed_pending(**kwargs))
        return
    if args.command == "bootstrap-postgres":
        print(archive_bootstrap_postgres())
        return
    if args.command == "rebuild-indexes":
        store = get_archive_store()
        print(
            json.dumps(
                store.rebuild(
                    workers=args.workers,
                    batch_size=args.batch_size,
                    commit_interval=args.commit_interval,
                    progress_every=args.progress_every,
                    executor_kind=args.executor_kind,
                    force_full=bool(getattr(args, "force_full_rebuild", False)),
                    disable_manifest_cache=bool(getattr(args, "disable_manifest_cache", False)),
                ),
                indent=2,
            )
        )
        return
    if args.command == "index-status":
        print(archive_index_status())
        return
    if args.command == "projection-inventory":
        print(archive_projection_inventory())
        return
    if args.command == "projection-status":
        print(archive_projection_status())
        return
    if args.command == "projection-explain":
        print(archive_projection_explain(args.card_uid))
        return
    if args.command == "seed-link-surface":
        print(archive_seed_link_surface())
        return
    if args.command == "seed-link-enqueue":
        print(
            archive_seed_link_enqueue(
                modules=args.modules,
                source_uids=args.source_uids,
                job_type=args.job_type,
                reset_existing=bool(args.reset_existing),
            )
        )
        return
    if args.command == "seed-link-backfill":
        print(
            archive_seed_link_backfill(
                limit=args.limit,
                modules=args.modules,
                workers=args.workers,
                include_llm=bool(args.include_llm),
                apply_promotions=not bool(args.no_apply_promotions),
            )
        )
        return
    if args.command == "seed-link-refresh":
        print(
            archive_seed_link_refresh(
                source_uids=args.source_uids,
                modules=args.modules,
                workers=args.workers,
                include_llm=bool(args.include_llm),
                apply_promotions=not bool(args.no_apply_promotions),
            )
        )
        return
    if args.command == "seed-link-worker":
        print(
            archive_seed_link_worker(
                limit=args.limit,
                modules=args.modules,
                workers=args.workers,
                include_llm=bool(args.include_llm),
            )
        )
        return
    if args.command == "seed-link-promote":
        print(
            archive_seed_link_promote(
                limit=args.limit,
                workers=args.workers,
            )
        )
        return
    if args.command == "seed-link-report":
        print(
            archive_seed_link_report(
                rebuild_if_dirty=not bool(args.no_rebuild_if_dirty),
            )
        )
        return
    if args.command == "link-candidates":
        print(
            archive_link_candidates(
                status=args.status,
                module_name=args.module_name,
                min_confidence=args.min_confidence,
                limit=args.limit,
            )
        )
        return
    if args.command == "link-candidate":
        print(archive_link_candidate(args.candidate_id))
        return
    if args.command == "review-link-candidate":
        print(
            archive_review_link_candidate(
                candidate_id=args.candidate_id,
                reviewer=args.reviewer,
                action=args.action,
                notes=args.notes,
            )
        )
        return
    if args.command == "duplicate-uids":
        print(archive_duplicate_uids(limit=args.limit))
        return
    if args.command == "link-quality-gate":
        print(archive_link_quality_gate())
        return
    if args.command == "build-benchmark-sample":
        print(
            json.dumps(
                build_benchmark_sample(
                    source_vault=Path(args.source_vault),
                    output_vault=Path(args.output_vault),
                    per_group_limit=args.per_group_limit,
                    max_notes=args.max_notes,
                    neighborhood_hops=args.neighborhood_hops,
                    oversample_factor=args.oversample_factor,
                    sample_percent=args.sample_percent,
                ),
                indent=2,
            )
        )
        return
    if args.command == "benchmark-rebuild":
        print(
            json.dumps(
                benchmark_rebuild(
                    vault=Path(args.vault),
                    schema=args.schema,
                    profile=args.profile,
                    workers=args.workers,
                    batch_size=args.batch_size,
                    commit_interval=args.commit_interval,
                    progress_every=args.progress_every,
                    executor_kind=args.executor_kind,
                ),
                indent=2,
            )
        )
        return
    if args.command == "benchmark-seed-links":
        selected_modules = [item.strip() for item in args.modules.split(",") if item.strip()]
        print(
            json.dumps(
                benchmark_seed_links(
                    vault=Path(args.vault),
                    schema=args.schema,
                    profile=args.profile,
                    workers=args.workers,
                    batch_size=args.batch_size,
                    commit_interval=args.commit_interval,
                    progress_every=args.progress_every,
                    executor_kind=args.executor_kind,
                    include_llm=bool(args.include_llm),
                    apply_promotions=bool(args.apply_promotions),
                    modules=selected_modules or None,
                    rebuild_first=not bool(args.no_rebuild_first),
                ),
                indent=2,
            )
        )
        return
    if args.command == "migrate":
        store = get_archive_store()
        from .migrate import MigrationRunner

        with store.index._connect() as conn:
            runner = MigrationRunner(conn, store.index.schema)
            result = runner.run(dry_run=bool(args.dry_run))
            print(
                json.dumps(
                    {
                        "dry_run": bool(args.dry_run),
                        "applied": result.applied,
                        "already_applied": result.already_applied,
                        "failed": result.failed,
                        "error": result.error,
                        "elapsed_ms": result.elapsed_ms,
                    },
                    indent=2,
                )
            )
        return
    if args.command == "migration-status":
        store = get_archive_store()
        from .migrate import MigrationRunner

        with store.index._connect() as conn:
            runner = MigrationRunner(conn, store.index.schema)
            print(json.dumps(runner.status(), indent=2, default=str))
        return
    mcp.run()


if __name__ == "__main__":
    main()

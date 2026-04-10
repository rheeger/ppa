"""Run the Archive MCP server or index maintenance commands."""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import sys
from pathlib import Path

from archive_sync.llm_enrichment.defaults import (DEFAULT_ENRICH_EXTRACT_MODEL,
                                                  DEFAULT_ENRICH_TRIAGE_MODEL)

from .benchmark import (BENCHMARK_PROFILES, DEFAULT_BENCHMARK_SOURCE_VAULT,
                        benchmark_multi_size, benchmark_rebuild,
                        benchmark_seed_links, build_benchmark_sample)
from .commands import admin as admin_cmd
from .commands import explain
from .commands import graph as graph_cmd
from .commands import query as query_cmd
from .commands import read as read_cmd
from .commands import search as search_cmd
from .commands import seed_links as seed_cmd
from .commands import status as status_cmd
from .commands._resolve import resolve_index, resolve_store
from .errors import PpaError
from .index_config import get_seed_links_enabled
from .log import configure_logging
from .server import mcp

_cli_log = logging.getLogger("ppa.cli")


def _print_json(data: object) -> None:
    print(json.dumps(data, indent=2, default=str))


def _print_cli_result(data: object) -> None:
    """Print dict/list as JSON; pass through str for mocks and human one-liners."""
    if isinstance(data, str):
        print(data)
    else:
        _print_json(data)


def _cli_fail(exc: PpaError) -> None:
    print(str(exc), file=sys.stderr)
    raise SystemExit(1)


def _ppa_env_name_is_secret(name: str) -> bool:
    upper = name.upper()
    return any(s in upper for s in ("KEY", "SECRET", "TOKEN", "PASSWORD"))


def _emit_mcp_config() -> None:
    """Print a paste-ready MCP client JSON block from the current environment.

    Secrets are omitted by design: only `PPA_*` vars are included, and names
    matching *KEY*, *SECRET*, *TOKEN*, or *PASSWORD* are skipped so API keys
    never appear in terminal output or copied configs.
    """
    env: dict[str, str] = {}
    for key, value in os.environ.items():
        if not key.startswith("PPA_"):
            continue
        if _ppa_env_name_is_secret(key):
            continue
        env[key] = value
    server_name = os.environ.get("PPA_MCP_CONFIG_SERVER_NAME", "ppa").strip() or "ppa"
    args = ["serve"]
    tunnel = os.environ.get("PPA_MCP_TUNNEL_HOST", "").strip()
    if tunnel:
        args.extend(["--tunnel", tunnel])
    block = {
        "mcpServers": {
            server_name: {
                "command": "ppa",
                "args": args,
                "env": env,
            }
        }
    }
    print(json.dumps(block, indent=2))


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
    parser.add_argument(
        "--log-file",
        default="",
        metavar="PATH",
        help="Append duplicate ppa.* logs to PATH (same format as stderr; optional retention artifact)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        default=False,
        help="Skip vault scan cache; always read files from disk (slower but guaranteed fresh)",
    )
    subparsers = parser.add_subparsers(dest="command")

    serve_parser = subparsers.add_parser("serve", help="Start MCP server (stdio)")
    serve_parser.add_argument(
        "--tunnel",
        default="",
        metavar="USER@HOST",
        help="Manage SSH tunnel: localhost:PPA_TUNNEL_PORT -> remote 127.0.0.1:5432",
    )
    subparsers.add_parser(
        "mcp-config",
        help="Print paste-ready MCP JSON for this environment (no secrets)",
    )
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

    slice_parser = subparsers.add_parser("slice-seed", help="Generate a stratified test slice from a seed vault")
    slice_parser.add_argument("--config", required=True, help="Path to slice_config.json")
    slice_parser.add_argument("--output", required=True, help="Output directory for the slice vault")
    slice_parser.add_argument("--source-vault", default="", help="Source vault (overrides PPA_BENCHMARK_SOURCE_VAULT)")
    slice_parser.add_argument(
        "--build-image",
        action="store_true",
        help="Build a Docker image containing the slice for CI",
    )
    slice_parser.add_argument(
        "--image-tag",
        default="",
        help="Docker image tag (default: ppa-test-slice:<snapshot_date>)",
    )
    slice_parser.add_argument(
        "--progress-every",
        type=int,
        default=5000,
        metavar="N",
        help="Log scan/copy progress every N notes or files (default: 5000; use 500–2000 for noisy feedback)",
    )
    slice_parser.add_argument(
        "--target-percent",
        type=float,
        default=None,
        metavar="PCT",
        help="Override slice_config.json target_percent (e.g. 0.5 for a tiny smoke slice)",
    )
    slice_parser.add_argument(
        "--cluster-cap",
        type=int,
        default=None,
        metavar="N",
        help="Override slice_config.json cluster_cap",
    )
    slice_parser.add_argument(
        "--dangling-rounds",
        type=int,
        default=3,
        metavar="N",
        help="Resolve dangling wikilinks for up to N rounds after per-seed closure (default: 3)",
    )

    health_check_parser = subparsers.add_parser("health-check", help="Structural and behavioral index checks")
    health_check_parser.add_argument("--dsn", default="", help="Override PPA_INDEX_DSN")
    health_check_parser.add_argument("--manifest", default="", help="Path to slice_manifest.json")
    health_check_parser.add_argument("--report-format", choices=["json", "md", "both"], default="both")
    health_check_parser.add_argument("--report-dir", default=".", help="Directory for report files")

    bench_multi_parser = subparsers.add_parser("benchmark", help="Multi-size performance benchmark")
    bench_multi_parser.add_argument(
        "--slice-percent",
        type=float,
        action="append",
        dest="slice_percents",
        required=True,
    )
    bench_multi_parser.add_argument("--output", required=True, help="Output directory for JSON results")
    bench_multi_parser.add_argument("--profile", default="local-laptop")
    bench_multi_parser.add_argument("--schema-prefix", default="archive_bench_multi")

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
    tn_parser = subparsers.add_parser("temporal-neighbors", help="Cards near a timestamp (JSON)")
    tn_parser.add_argument("timestamp")
    tn_parser.add_argument("--direction", default="both", choices=("forward", "backward", "both"))
    tn_parser.add_argument("--limit", type=int, default=20)
    tn_parser.add_argument("--type", dest="type_filter", default="")
    tn_parser.add_argument("--source", dest="source_filter", default="")
    tn_parser.add_argument("--person", dest="people_filter", default="")
    kn_parser = subparsers.add_parser("knowledge", help="Knowledge card for domain or search fallback (JSON)")
    kn_parser.add_argument("domain")
    kn_parser.add_argument("--fallback-query", dest="fallback_query", default="")
    kn_parser.add_argument("--limit", type=int, default=5)
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

    extract_parser = subparsers.add_parser("extract-emails", help="Extract derived cards from email bodies")
    extract_parser.add_argument("--sender", default="", help="Filter to single extractor id (e.g. doordash)")
    extract_parser.add_argument("--dry-run", action="store_true")
    extract_parser.add_argument("--limit", type=int, default=0, help="Max matched emails to process (0 = no cap)")
    extract_parser.add_argument("--staging-dir", default="", help="Write to staging instead of vault")
    extract_parser.add_argument("--workers", type=int, default=4)
    extract_parser.add_argument("--batch-size", type=int, default=500)
    extract_parser.add_argument(
        "--full-report",
        action="store_true",
        help="After extraction, log staging summary to stderr (requires --staging-dir)",
    )
    extract_parser.add_argument(
        "--limit-vault-percent",
        type=float,
        default=0.0,
        help="Deterministic sample: process ~N%% of email_message cards (0 = full vault)",
    )
    extract_parser.add_argument(
        "--progress-every",
        type=int,
        default=5000,
        help="Log scan/extract progress every N emails or work items",
    )

    enrich_parser = subparsers.add_parser(
        "enrich-emails",
        help="LLM extraction from known-sender email threads (Phase 2.75 — no triage, writes staging)",
    )
    enrich_parser.add_argument("--staging-dir", default="_staging-llm", help="Output directory for derived cards")
    enrich_parser.add_argument(
        "--provider",
        default="ollama",
        choices=["ollama", "gemini"],
        help="LLM provider: ollama (local) or gemini (Google API, needs GEMINI_API_KEY)",
    )
    enrich_parser.add_argument(
        "--extract-model",
        default="",
        help=f"Model name (default: {DEFAULT_ENRICH_EXTRACT_MODEL} for ollama, gemini-2.0-flash for gemini)",
    )
    enrich_parser.add_argument(
        "--classify-model",
        default="",
        help="Model for classify stage (default: same as extract-model; use cheaper model for classify)",
    )
    enrich_parser.add_argument(
        "--base-url",
        default="http://localhost:11434",
        help="Ollama base URL (ignored for gemini provider)",
    )
    enrich_parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Concurrent extraction threads (default 4; gemini handles high concurrency natively)",
    )
    enrich_parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Log progress every N extracted threads",
    )
    enrich_parser.add_argument(
        "--full-report",
        action="store_true",
        help="After run, log staging summary to stderr (requires writable staging-dir)",
    )
    enrich_parser.add_argument(
        "--cache-db",
        default="_enrichment_cache.db",
        help="SQLite inference cache path (set empty to disable)",
    )
    enrich_parser.add_argument("--run-id", default="", help="Run id for cache + card comments")
    enrich_parser.add_argument("--limit-threads", type=int, default=0, help="Max threads to process (0 = all)")
    enrich_parser.add_argument(
        "--limit-vault-percent",
        type=float,
        default=0.0,
        help="Deterministic sample: process ~N%% of threads by thread_id hash (0 = all)",
    )
    enrich_parser.add_argument(
        "--no-gate",
        action="store_true",
        help="Skip known-sender gate — send ALL threads to extraction (uses more API calls)",
    )
    enrich_parser.add_argument(
        "--skip-classify",
        action="store_true",
        help="Skip Stage 1 LLM classify — only extract threads matching the domain gate (faster, fewer cards)",
    )
    enrich_parser.add_argument(
        "--classify-index-db",
        default="_classify_index.db",
        help="Persistent thread classification index path (stores classify results for reuse)",
    )
    enrich_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run triage + extraction but do not write cards",
    )

    census_parser = subparsers.add_parser("sender-census", help="Discover email types from a sender domain")
    census_parser.add_argument("--domain", required=True, help="Sender domain (e.g., doordash.com)")
    census_parser.add_argument(
        "--sample",
        type=int,
        default=100,
        help="Stratified sample size for example subjects (full domain list is always scanned)",
    )
    census_parser.add_argument(
        "--detail-rows",
        dest="detail_rows",
        type=int,
        default=8,
        help="Max rows per category in Sample Emails section (default: 8)",
    )
    census_parser.add_argument(
        "--top-from",
        dest="top_from",
        type=int,
        default=30,
        help="List top N from_email addresses by count (0=omit section)",
    )
    census_parser.add_argument(
        "--top-exact-subjects",
        dest="top_exact_subjects",
        type=int,
        default=40,
        help="List top N exact subject lines by count (0=omit)",
    )
    census_parser.add_argument(
        "--top-subject-shapes",
        dest="top_subject_shapes",
        type=int,
        default=40,
        help="List top N normalized subject shapes (0=omit)",
    )
    census_parser.add_argument(
        "--no-keyword-hits",
        dest="no_keyword_hits",
        action="store_true",
        help="Omit subject keyword hit counts",
    )
    census_parser.add_argument("--out", default="", help="Write output to file (default: stdout)")
    census_parser.add_argument("--vault", default="", help="Vault path (default: PPA_PATH)")

    sampler_parser = subparsers.add_parser("template-sampler", help="Sample email bodies by year for template era discovery")
    sampler_parser.add_argument("--domain", default="", help="Sender domain (e.g., doordash.com); required unless --batch")
    sampler_parser.add_argument("--category", default="", help="Filter by subject keyword (e.g., receipt)")
    sampler_parser.add_argument("--per-year", dest="per_year", type=int, default=3)
    sampler_parser.add_argument("--out-dir", dest="out_dir", default="", help="Output root; required unless --batch")
    sampler_parser.add_argument(
        "--batch",
        default="",
        help="JSON file: array of {name,domain,category?,out_dir} jobs — one Email/ walk, multiple outputs",
    )
    sampler_parser.add_argument("--vault", default="", help="Vault path (default: PPA_PATH)")

    resolve_parser = subparsers.add_parser("resolve-entities", help="Create Place/Org links from derived cards")
    resolve_parser.add_argument("--dry-run", action="store_true")
    resolve_parser.add_argument(
        "--type",
        dest="entity_type",
        choices=["place", "org", "person", "all"],
        default="all",
    )
    resolve_parser.add_argument(
        "--report-dir",
        default="",
        help="Write entity-resolution-report.json and entity-resolution-spot-check.md here",
    )

    staging_report_parser = subparsers.add_parser("staging-report", help="Inspect extraction staging output")
    staging_report_parser.add_argument("--staging-dir", required=True, help="Path to staging directory")
    staging_report_parser.add_argument("--json", action="store_true", help="Output JSON on stdout")

    promote_parser = subparsers.add_parser("promote-staging", help="Move staged derived cards into the vault")
    promote_parser.add_argument("--staging-dir", required=True, help="Path to staging directory")
    promote_parser.add_argument("--dry-run", action="store_true")

    parser.set_defaults(command="serve")
    args = parser.parse_args()
    if args.command == "serve" and not hasattr(args, "tunnel"):
        args.tunnel = ""
    # Stderr-only logging for all subcommands; keep stdout for MCP JSON-RPC / CLI JSON. See archive_mcp/log.py.
    configure_logging(verbose=args.verbose)
    log_file = str(getattr(args, "log_file", "") or "").strip()
    if log_file:
        from .log import attach_file_log

        attach_file_log(Path(log_file))
    if args.command == "mcp-config":
        _emit_mcp_config()
        return
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
    if args.command == "temporal-neighbors":
        try:
            store = resolve_store()
            out = graph_cmd.temporal_neighbors(
                args.timestamp,
                direction=args.direction,
                limit=args.limit,
                type_filter=args.type_filter,
                source_filter=args.source_filter,
                people_filter=args.people_filter,
                store=store,
                logger=_cli_log,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "knowledge":
        try:
            store = resolve_store()
            out = graph_cmd.knowledge_domain(
                args.domain,
                fallback_query=args.fallback_query,
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
    if args.command == "sender-census":
        try:
            from .commands._resolve import resolve_vault
            from .commands.census import run_sender_census

            vp = str(getattr(args, "vault", "") or "").strip() or str(resolve_vault())
            out_path = str(getattr(args, "out", "") or "").strip()
            text = run_sender_census(
                vault_path=vp,
                domain=str(args.domain),
                sample_size=int(args.sample),
                out_path=out_path,
                detail_rows_per_category=int(getattr(args, "detail_rows", 8) or 8),
                top_from_addresses=int(getattr(args, "top_from", 0) or 0),
                top_exact_subjects=int(getattr(args, "top_exact_subjects", 0) or 0),
                top_subject_shapes=int(getattr(args, "top_subject_shapes", 0) or 0),
                include_keyword_hits=not bool(getattr(args, "no_keyword_hits", False)),
            )
            if not out_path:
                print(text)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "template-sampler":
        try:
            from .commands._resolve import resolve_vault
            from .commands.template_sampler import (
                run_template_sampler, run_template_sampler_from_batch_file)

            vp = str(getattr(args, "vault", "") or "").strip() or str(resolve_vault())
            batch = str(getattr(args, "batch", "") or "").strip()
            if batch:
                dom = str(getattr(args, "domain", "") or "").strip()
                odir = str(getattr(args, "out_dir", "") or "").strip()
                cat = str(getattr(args, "category", "") or "").strip()
                if dom or odir or cat:
                    _cli_fail(
                        PpaError("--batch cannot be combined with --domain, --out-dir, or --category")
                    )
                result = run_template_sampler_from_batch_file(
                    vault_path=vp,
                    batch_path=batch,
                    per_year=int(getattr(args, "per_year", 3)),
                    base_dir=Path.cwd(),
                )
            else:
                dom = str(getattr(args, "domain", "") or "").strip()
                odir = str(getattr(args, "out_dir", "") or "").strip()
                if not dom or not odir:
                    _cli_fail(PpaError("--domain and --out-dir are required unless --batch is set"))
                result = run_template_sampler(
                    vault_path=vp,
                    domain=dom,
                    category=str(getattr(args, "category", "") or ""),
                    per_year=int(getattr(args, "per_year", 3)),
                    out_dir=odir,
                )
            _print_json(result)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "extract-emails":
        try:
            from archive_sync.extractors.registry import build_default_registry
            from archive_sync.extractors.runner import ExtractionRunner

            from .commands._resolve import resolve_vault

            vault = str(resolve_vault())
            registry = build_default_registry()
            vp = float(getattr(args, "limit_vault_percent", 0.0) or 0.0)
            runner = ExtractionRunner(
                vault_path=vault,
                registry=registry,
                staging_dir=(str(args.staging_dir).strip() or None),
                workers=int(args.workers),
                batch_size=int(args.batch_size),
                dry_run=bool(args.dry_run),
                sender_filter=(str(args.sender).strip().lower() or None),
                limit=(int(args.limit) if int(args.limit) > 0 else None),
                progress_every=int(getattr(args, "progress_every", 5000) or 5000),
                vault_percent=(vp if vp > 0 else None),
            )
            metrics = runner.run()
            _print_json(metrics.to_dict())
            if bool(getattr(args, "full_report", False)) and str(getattr(args, "staging_dir", "") or "").strip():
                from .commands.staging import emit_full_staging_report

                emit_full_staging_report(str(args.staging_dir).strip())
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "enrich-emails":
        try:
            from archive_sync.llm_enrichment.enrich_runner import \
                LlmEnrichmentRunner

            from .commands._resolve import resolve_vault

            vault = resolve_vault()
            cache_raw = str(getattr(args, "cache_db", "") or "").strip()
            cache_db = cache_raw if cache_raw else None
            lim = int(getattr(args, "limit_threads", 0) or 0)
            vp = float(getattr(args, "limit_vault_percent", 0.0) or 0.0)
            prov = str(getattr(args, "provider", "ollama") or "ollama").strip()
            ext_model = str(getattr(args, "extract_model", "") or "").strip()
            if not ext_model:
                ext_model = "gemini-2.5-flash" if prov == "gemini" else DEFAULT_ENRICH_EXTRACT_MODEL
            runner = LlmEnrichmentRunner(
                vault_path=vault,
                staging_dir=str(getattr(args, "staging_dir", "_staging-llm") or "_staging-llm"),
                extract_model=ext_model,
                classify_model=str(getattr(args, "classify_model", "") or "").strip(),
                provider_kind=prov,
                base_url=str(getattr(args, "base_url", "http://localhost:11434") or "http://localhost:11434"),
                cache_db=cache_db,
                run_id=str(getattr(args, "run_id", "") or ""),
                progress_every=int(getattr(args, "progress_every", 25) or 25),
                limit_threads=(lim if lim > 0 else None),
                vault_percent=(vp if vp > 0 else None),
                dry_run=bool(getattr(args, "dry_run", False)),
                workers=int(getattr(args, "workers", 4) or 4),
                no_gate=bool(getattr(args, "no_gate", False)),
                skip_classify=bool(getattr(args, "skip_classify", False)),
                classify_index_db=str(getattr(args, "classify_index_db", "") or "").strip() or None,
            )
            metrics = runner.run()
            _print_json(metrics.to_dict())
            if bool(getattr(args, "full_report", False)):
                sd = str(getattr(args, "staging_dir", "") or "").strip()
                if sd:
                    from .commands.staging import emit_full_staging_report

                    emit_full_staging_report(sd)
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(1) from exc
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "resolve-entities":
        try:
            from archive_sync.extractors.entity_resolution import \
                run_entity_resolution

            from .commands._resolve import resolve_vault

            vault = str(resolve_vault())
            out = run_entity_resolution(
                vault,
                entity_filter=str(args.entity_type),
                dry_run=bool(args.dry_run),
                report_dir=str(getattr(args, "report_dir", "") or "").strip(),
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "staging-report":
        try:
            from .commands.staging import (format_staging_report_markdown,
                                           staging_report,
                                           staging_report_to_jsonable)

            report = staging_report(str(args.staging_dir))
            if bool(getattr(args, "json", False)):
                _print_json(staging_report_to_jsonable(report))
            else:
                from archive_sync.extractors.field_metrics import \
                    compute_field_population

                fp = compute_field_population(Path(str(args.staging_dir)))
                print(format_staging_report_markdown(report, field_population=fp), file=sys.stderr)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "promote-staging":
        try:
            import dataclasses

            from archive_sync.extractors.promoter import promote_staging

            from .commands._resolve import resolve_vault

            vault = str(resolve_vault())
            result = promote_staging(
                vault_path=vault,
                staging_dir=str(args.staging_dir),
                dry_run=bool(getattr(args, "dry_run", False)),
            )
            _print_json(dataclasses.asdict(result))
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
        kwargs: dict[str, object] = {"limit": args.limit}
        if args.embedding_model:
            kwargs["embedding_model"] = args.embedding_model
        if args.embedding_version:
            kwargs["embedding_version"] = args.embedding_version
        try:
            store = resolve_store()
            result = admin_cmd.embed_pending(store=store, logger=_cli_log, **kwargs)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "bootstrap-postgres":
        try:
            vault = resolve_store().vault
            result = admin_cmd.bootstrap_postgres(vault=vault, logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "rebuild-indexes":
        try:
            store = resolve_store()
            store.index._no_cache = bool(getattr(args, "no_cache", False))
            result = admin_cmd.rebuild_indexes(
                store=store,
                logger=_cli_log,
                workers=args.workers,
                batch_size=args.batch_size,
                commit_interval=args.commit_interval,
                progress_every=args.progress_every,
                executor_kind=args.executor_kind,
                force_full=bool(getattr(args, "force_full_rebuild", False)),
                disable_manifest_cache=bool(getattr(args, "disable_manifest_cache", False)),
                no_cache=bool(getattr(args, "no_cache", False)),
            )
            _print_json(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "index-status":
        try:
            store = resolve_store()
            result = status_cmd.index_status(store=store, logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "projection-inventory":
        try:
            store = resolve_store()
            result = admin_cmd.projection_inventory(store=store, logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "projection-status":
        try:
            store = resolve_store()
            result = admin_cmd.projection_status(store=store, logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "projection-explain":
        try:
            store = resolve_store()
            result = admin_cmd.projection_explain(args.card_uid, store=store, logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "seed-link-surface":
        try:
            result = seed_cmd.seed_link_surface(logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "seed-link-enqueue":
        try:
            index = resolve_index()
            result = seed_cmd.seed_link_enqueue(
                index=index,
                logger=_cli_log,
                modules=args.modules,
                source_uids=args.source_uids,
                job_type=args.job_type,
                reset_existing=bool(args.reset_existing),
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "seed-link-backfill":
        try:
            index = resolve_index()
            result = seed_cmd.seed_link_backfill(
                index=index,
                logger=_cli_log,
                limit=args.limit,
                modules=args.modules,
                workers=args.workers,
                include_llm=bool(args.include_llm),
                apply_promotions=not bool(args.no_apply_promotions),
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "seed-link-refresh":
        try:
            index = resolve_index()
            result = seed_cmd.seed_link_refresh(
                index=index,
                logger=_cli_log,
                source_uids=args.source_uids,
                modules=args.modules,
                workers=args.workers,
                include_llm=bool(args.include_llm),
                apply_promotions=not bool(args.no_apply_promotions),
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "seed-link-worker":
        try:
            index = resolve_index()
            result = seed_cmd.seed_link_worker(
                index=index,
                logger=_cli_log,
                limit=args.limit,
                modules=args.modules,
                workers=args.workers,
                include_llm=bool(args.include_llm),
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "seed-link-promote":
        try:
            index = resolve_index()
            result = seed_cmd.seed_link_promote(
                index=index,
                logger=_cli_log,
                limit=args.limit,
                workers=args.workers,
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "seed-link-report":
        try:
            index = resolve_index()
            result = seed_cmd.seed_link_report(
                index=index,
                logger=_cli_log,
                rebuild_if_dirty=not bool(args.no_rebuild_if_dirty),
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "link-candidates":
        try:
            index = resolve_index()
            result = seed_cmd.link_candidates(
                index=index,
                logger=_cli_log,
                status=args.status,
                module_name=args.module_name,
                min_confidence=args.min_confidence,
                limit=args.limit,
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "link-candidate":
        try:
            index = resolve_index()
            result = seed_cmd.link_candidate(args.candidate_id, index=index, logger=_cli_log)
            _print_cli_result(result if result is not None else "Candidate not found")
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "review-link-candidate":
        try:
            index = resolve_index()
            result = seed_cmd.review_link_candidate(
                index=index,
                logger=_cli_log,
                candidate_id=args.candidate_id,
                reviewer=args.reviewer,
                action=args.action,
                notes=args.notes,
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "duplicate-uids":
        try:
            index = resolve_index()
            result = status_cmd.duplicate_uids(limit=args.limit, index=index, logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "link-quality-gate":
        try:
            index = resolve_index()
            result = seed_cmd.link_quality_gate(index=index, logger=_cli_log)
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
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
    if args.command == "slice-seed":
        from .test_slice import (build_slice_docker_image, load_slice_config,
                                 slice_seed_vault)

        cfg = load_slice_config(Path(args.config))
        if args.target_percent is not None:
            cfg.target_percent = float(args.target_percent)
        if args.cluster_cap is not None:
            cfg.cluster_cap = int(args.cluster_cap)
        src = Path(args.source_vault or os.environ.get("PPA_BENCHMARK_SOURCE_VAULT", str(DEFAULT_BENCHMARK_SOURCE_VAULT)))
        out = Path(args.output)
        res = slice_seed_vault(
            src,
            out,
            cfg,
            progress_every=int(args.progress_every),
            no_cache=bool(getattr(args, "no_cache", False)),
            dangling_rounds=int(args.dangling_rounds),
        )
        tag = str(args.image_tag or "").strip()
        if not tag:
            tag = f"ppa-test-slice:{cfg.snapshot_date or 'latest'}"
        if args.build_image:
            build_slice_docker_image(out, tag)
        payload = {
            "total_source_cards": res.total_source_cards,
            "selected_card_count": res.selected_card_count,
            "cards_by_type": res.cards_by_type,
            "orphaned_wikilinks": res.orphaned_wikilinks,
            "docker_tag": tag if args.build_image else "",
        }
        print(json.dumps(payload, indent=2, default=str))
        return
    if args.command == "health-check":
        from .commands import health_check as health_check_cmd
        from .index_config import get_index_dsn
        from .index_store import PostgresArchiveIndex

        dsn = str(args.dsn or get_index_dsn() or "")
        if not dsn:
            print("PPA_INDEX_DSN is required", file=sys.stderr)
            raise SystemExit(1)
        vault = Path(os.environ.get("PPA_PATH", "."))
        schema = os.environ.get("PPA_INDEX_SCHEMA", "archive_mcp")
        index = PostgresArchiveIndex(vault, dsn=dsn)
        index.schema = schema
        manifest: dict = {}
        if args.manifest:
            manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
        with index._connect() as conn:
            structural = health_check_cmd.run_structural_checks(conn, schema, manifest or None)
        behavioral = None
        if manifest:
            behavioral = health_check_cmd.run_behavioral_checks(index, manifest)
        health_check_cmd.write_reports(
            structural,
            behavioral,
            report_format=args.report_format,
            report_dir=args.report_dir,
        )
        ok = structural.ok if behavioral is None else structural.ok and behavioral.ok
        raise SystemExit(0 if ok else 1)
    if args.command == "benchmark":
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        src = Path(os.environ.get("PPA_BENCHMARK_SOURCE_VAULT", str(DEFAULT_BENCHMARK_SOURCE_VAULT)))
        payload = benchmark_multi_size(
            src,
            list(args.slice_percents),
            schema_prefix=str(args.schema_prefix),
            profile=str(args.profile),
        )
        out_path = out_dir / "benchmark-multi.json"
        out_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(json.dumps({"written": str(out_path)}, indent=2))
        return
    if args.command == "migrate":
        store = resolve_store()
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
        store = resolve_store()
        from .migrate import MigrationRunner

        with store.index._connect() as conn:
            runner = MigrationRunner(conn, store.index.schema)
            print(json.dumps(runner.status(), indent=2, default=str))
        return
    if args.command == "serve" and getattr(args, "tunnel", ""):
        from .tunnel import TunnelManager

        local_port = int(os.environ.get("PPA_TUNNEL_PORT", "5433"))
        remote_port = int(os.environ.get("PPA_TUNNEL_REMOTE_PORT", "5432"))
        tunnel_mgr = TunnelManager(args.tunnel, local_port=local_port, remote_port=remote_port)
        tunnel_mgr.start()
        atexit.register(tunnel_mgr.stop)
    mcp.run()


if __name__ == "__main__":
    main()

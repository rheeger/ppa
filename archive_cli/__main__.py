"""Run the Archive MCP server or index maintenance commands."""

from __future__ import annotations

import argparse
import atexit
import json
import logging
import os
import sys
import time
from pathlib import Path

from archive_sync.llm_enrichment.defaults import (
    DEFAULT_ENRICH_CARD_GEMINI_MODEL, DEFAULT_ENRICH_EXTRACT_MODEL)

from .benchmark import (BENCHMARK_PROFILES, DEFAULT_BENCHMARK_SOURCE_VAULT,
                        benchmark_multi_size, benchmark_rebuild,
                        benchmark_seed_links, build_benchmark_sample)
from .commands import admin as admin_cmd
from .commands import batch_embed as batch_embed_cmd
from .commands import explain
from .commands import graph as graph_cmd
from .commands import query as query_cmd
from .commands import read as read_cmd
from .commands import search as search_cmd
from .commands import seed_links as seed_cmd
from .commands import status as status_cmd
from .commands._resolve import resolve_index, resolve_store
from .errors import PpaError, VaultNotFoundError
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
    bootstrap_parser = subparsers.add_parser("bootstrap-postgres")
    bootstrap_parser.add_argument(
        "--force",
        action="store_true",
        help="Allow bootstrap even if schema is already populated (DROPs all typed projections!)",
    )
    embed_parser = subparsers.add_parser("embed-pending")
    embed_parser.add_argument("--limit", type=int, default=0)
    embed_parser.add_argument("--embedding-model", default="")
    embed_parser.add_argument("--embedding-version", type=int, default=0)
    embed_parser.add_argument(
        "--copy-from-schema",
        default="",
        help=(
            "Before calling the embedding provider, copy embeddings from another "
            "schema in the same database for any chunk_key that already exists "
            "there (deterministic chunk_key = same content => same key). "
            "Skips API calls + cost when re-bootstrapping a slice from a vault "
            "whose chunks already have embeddings in the source schema."
        ),
    )
    embed_estimate_parser = subparsers.add_parser(
        "embed-estimate",
        help="Estimate cost and time for embedding pending chunks",
    )
    embed_estimate_parser.add_argument("--embedding-model", type=str, default="")
    embed_estimate_parser.add_argument("--embedding-version", type=int, default=0)
    embed_gc_parser = subparsers.add_parser(
        "embed-gc",
        help="GC orphaned embeddings (chunk_key no longer in chunks). Idempotent.",
    )
    embed_gc_parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete orphans. Without --apply, prints counts only (dry-run).",
    )
    embed_batch_submit_parser = subparsers.add_parser(
        "embed-batch-submit",
        help="Submit pending chunks to OpenAI Batch API (50% discount, no TPM/TPD)",
    )
    embed_batch_submit_parser.add_argument("--embedding-model", type=str, default="")
    embed_batch_submit_parser.add_argument("--embedding-version", type=int, default=0)
    embed_batch_submit_parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Max batches to submit in one invocation (0 = until no pending remain)",
    )
    embed_batch_submit_parser.add_argument(
        "--requests-per-batch",
        type=int,
        default=50_000,
        help="Chunks per batch (OpenAI caps at 50,000 for /v1/embeddings)",
    )
    embed_batch_submit_parser.add_argument(
        "--no-context-prefix",
        action="store_true",
        help="Disable context prefix (defaults to retrieval.context.include_in_embeddings)",
    )
    embed_batch_submit_parser.add_argument("--artifact-dir", type=str, default="")
    embed_batch_poll_parser = subparsers.add_parser(
        "embed-batch-poll",
        help="Refresh status of in-flight OpenAI embedding batches",
    )
    embed_batch_ingest_parser = subparsers.add_parser(
        "embed-batch-ingest",
        help="Download completed batch outputs and write vectors into embeddings",
    )
    embed_batch_ingest_parser.add_argument("--artifact-dir", type=str, default="")
    embed_batch_ingest_parser.add_argument(
        "--write-batch-size",
        type=int,
        default=500,
        help="Rows per INSERT...ON CONFLICT during ingest",
    )
    embed_batch_ingest_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Parallel ingest workers (each batch is ~1.5 GB download)",
    )
    subparsers.add_parser(
        "embed-batch-status",
        help="Summary of OpenAI batch states + remaining pending chunks",
    )
    embed_cache_rotate_parser = subparsers.add_parser(
        "embed-cache-rotate",
        help=(
            "Move ingested *-out.jsonl files from _artifacts/_embedding-runs/batches/ "
            "into _artifacts/_embedding-recovery-cache/run-{ts}/ (warm cache for "
            "future re-ingest without OpenAI download). Keeps only the most recent run."
        ),
    )
    embed_cache_rotate_parser.add_argument("--artifact-dir", default="")
    embed_cache_rotate_parser.add_argument("--cache-dir", default="")
    embed_cache_rotate_parser.add_argument(
        "--keep",
        type=int,
        default=1,
        help="How many recent runs to retain (default 1)",
    )
    embed_cache_rotate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be moved/pruned without touching files",
    )
    _ = embed_batch_poll_parser  # placate linters
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

    slice_bootstrap_parser = subparsers.add_parser(
        "slice-bootstrap",
        help=(
            "End-to-end fresh slice into a Postgres schema, inheriting all the "
            "value-added work from a source schema (embeddings, classifications, "
            "IVFFlat index). Idempotent. Uses PPA_PATH (slice vault) and "
            "PPA_INDEX_SCHEMA (target schema) from env."
        ),
    )
    slice_bootstrap_parser.add_argument(
        "--copy-from-schema",
        default="",
        help=(
            "Source schema in the same DB to inherit data from. Embeddings, "
            "card_classifications, and the IVFFlat vector index are all carried "
            "over for any chunk_key / card_uid that exists in the new slice "
            "(zero API cost — the data is already paid for)."
        ),
    )
    slice_bootstrap_parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Rebuild workers (default: 4)",
    )
    slice_bootstrap_parser.add_argument(
        "--skip-rebuild",
        action="store_true",
        help="Skip the rebuild-indexes step (assume cards/chunks already populated).",
    )

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
    enrich_parser.add_argument("--staging-dir", default="_artifacts/_staging-llm", help="Output directory for derived cards")
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
        "--classify-workers",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Parallel Stage-1 classify threads (0 = auto: Gemini capped at 10, Ollama extract_workers×3). "
            "Lower this if you see empty Gemini responses / 429s."
        ),
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
        default="_artifacts/_enrichment_cache.db",
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
        default="_artifacts/_classify_index.db",
        help="Persistent thread classification index path (stores classify results for reuse)",
    )
    enrich_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run triage + extraction but do not write cards",
    )

    enrich_cards_parser = subparsers.add_parser(
        "enrich-cards",
        help="Phase 2.875 vault card enrichment (LLM summaries, entity + match staging)",
    )
    enrich_cards_parser.add_argument(
        "--workflow",
        default="email_thread",
        help="Workflow name: email_thread, imessage_thread, beeper_thread, finance, calendar_event, document",
    )
    enrich_cards_parser.add_argument("--vault", default="", help="Vault path (default: PPA_PATH)")
    enrich_cards_parser.add_argument(
        "--staging-dir",
        default="_artifacts/_staging-enrichment",
        help="entity_mentions.jsonl, match_candidates.jsonl, _metrics.json",
    )
    enrich_cards_parser.add_argument(
        "--provider",
        default="gemini",
        choices=["ollama", "gemini"],
        help="LLM provider",
    )
    enrich_cards_parser.add_argument(
        "--model",
        default="",
        help=f"Model id (default: {DEFAULT_ENRICH_CARD_GEMINI_MODEL} for gemini, Gemma default for ollama)",
    )
    enrich_cards_parser.add_argument(
        "--base-url",
        default="http://localhost:11434",
        help="Ollama base URL (ignored for gemini)",
    )
    enrich_cards_parser.add_argument(
        "--cache-db",
        default="_artifacts/_enrichment_cache.db",
        help="SQLite inference cache (empty string to disable)",
    )
    enrich_cards_parser.add_argument("--run-id", default="", help="Run id for cache + metrics")
    enrich_cards_parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Do not write vault cards or entity/match JSONL. Still calls the LLM; writes "
            "llm_enrichment_preview.jsonl + _metrics.json under --staging-dir. Does not write "
            "inference cache entries (so previews do not pollute the cache)."
        ),
    )
    enrich_cards_parser.add_argument("--progress-every", type=int, default=1)
    enrich_cards_parser.add_argument(
        "--vault-percent",
        type=float,
        default=0.0,
        help="Process ~N%% of cards by uid hash (0 = all)",
    )
    enrich_cards_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Stop after N successful enrichments (valid LLM JSON + counted). "
            "Use e.g. 25 for a preview batch before scaling up. 0 = no cap."
        ),
    )
    enrich_cards_parser.add_argument(
        "--workers",
        type=int,
        default=24,
        metavar="W",
        help=(
            "Parallelism: overlapping Gemini calls and (for imessage/beeper) parallel eligibility "
            "stub resolution. Ignored when --limit is set (preview runs stay sequential). Default 24."
        ),
    )
    enrich_cards_parser.add_argument(
        "--classify-index-db",
        default="_artifacts/_classify_index.db",
        metavar="PATH",
        help=(
            "Phase 2.75 thread classification SQLite (if this path exists, skip threads indexed as "
            "noise/marketing/automated; empty string to disable)"
        ),
    )
    enrich_cards_parser.add_argument(
        "--uid-filter-file",
        default="",
        metavar="PATH",
        help=(
            "Only process email_thread cards whose uid appears in this file (one uid per line; "
            "# comments ok). Use after curating a subset to re-run or iterate on prompts."
        ),
    )
    enrich_cards_parser.add_argument(
        "--no-skip-populated",
        action="store_true",
        help="Re-run even when thread_summary (or document description) is already populated",
    )

    extract_doc_parser = subparsers.add_parser(
        "extract-document-text",
        help="Re-extract document card bodies with markitdown (RTF/plain binary fixes)",
    )
    extract_doc_parser.add_argument("--vault", default="", help="Vault path (default: PPA_PATH)")
    extract_doc_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be converted without writing vault cards",
    )
    extract_doc_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Max eligible cards to process (0 = no cap)",
    )

    enrich_orch_parser = subparsers.add_parser(
        "enrich",
        help="Full vault enrichment orchestrator (extract-document-text → enrich-emails → enrich-cards × 4)",
    )
    enrich_orch_parser.add_argument(
        "--vault",
        required=True,
        metavar="PATH",
        help="Vault path (required — no PPA_PATH fallback)",
    )
    enrich_orch_parser.add_argument(
        "--run-id",
        default="",
        help="Run id for cache + manifest (default: enrich-YYYYMMDD-xxxxxxxx)",
    )
    enrich_orch_parser.add_argument(
        "--run-dir",
        default="",
        metavar="PATH",
        help="Run output root (default: _artifacts/_enrichment-runs/{run_id}/)",
    )
    enrich_orch_parser.add_argument(
        "--provider",
        default="gemini",
        choices=["ollama", "gemini"],
        help="LLM provider for enrichment steps",
    )
    enrich_orch_parser.add_argument(
        "--model",
        default="",
        help=f"Model for enrich-cards workflows (default: {DEFAULT_ENRICH_CARD_GEMINI_MODEL} for gemini)",
    )
    enrich_orch_parser.add_argument(
        "--enrich-emails-model",
        default="",
        help="Model for enrich-emails classify+extract (default: gemini-2.5-flash-lite for gemini)",
    )
    enrich_orch_parser.add_argument(
        "--base-url",
        default="http://localhost:11434",
        help="Ollama base URL (ignored for gemini)",
    )
    enrich_orch_parser.add_argument("--workers", type=int, default=24, help="Parallel workers for enrich-cards")
    enrich_orch_parser.add_argument(
        "--enrich-emails-workers",
        type=int,
        default=8,
        help="Concurrent threads for enrich-emails",
    )
    enrich_orch_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pass dry-run through all steps (no vault writes; enrich-cards skips cache writes)",
    )
    enrich_orch_parser.add_argument(
        "--steps",
        default="",
        help="Comma-separated step keys (default: all). Example: enrich_email_thread,enrich_document",
    )
    enrich_orch_parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=500,
        metavar="N",
        help="Write _metrics_checkpoint.json every N processed eligible cards per enrich-cards step (0=off)",
    )
    enrich_orch_parser.add_argument(
        "--cache-db",
        default="",
        metavar="PATH",
        help="Inference cache SQLite path (default: {run_dir}/cache.db)",
    )
    enrich_orch_parser.add_argument(
        "--resume",
        action="store_true",
        help="Reserved for manifest resume (same effect as reusing --run-id with existing run-dir)",
    )
    enrich_orch_parser.add_argument(
        "--skip-populated",
        action="store_true",
        default=False,
        help=(
            "For enrich-cards steps: skip cards that already have thread_summary / tags / descriptions "
            "(saves API spend). Default is off — live runs re-enrich populated cards."
        ),
    )

    geocode_parser = subparsers.add_parser(
        "geocode-places",
        help="Geocode PlaceCards via Nominatim (Phase 4 post-rebuild)",
    )
    geocode_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show candidates without modifying anything",
    )
    geocode_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max PlaceCards to geocode (0 = all)",
    )

    subparsers.add_parser("quality-report", help="Quality score distribution by card type")

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
    resolve_parser.add_argument(
        "--vault",
        default="",
        help="Vault path (default: PPA_PATH)",
    )
    resolve_parser.add_argument(
        "--staging-root",
        default="",
        dest="entity_staging_root",
        metavar="PATH",
        help="Enrichment run staging directory (e.g. …/enrich-…/staging) — loads */entity_mentions.jsonl",
    )
    resolve_parser.add_argument(
        "--person-mentions-out",
        default="",
        metavar="PATH",
        help="Write person rows from JSONL here (default: _artifacts/_staging-enrichment/person_mentions.jsonl)",
    )

    link_persons_parser = subparsers.add_parser(
        "link-persons",
        help="Resolve person-like strings on derived cards; optional wikilink writes (Phase 2.9)",
    )
    link_persons_parser.add_argument("--dry-run", action="store_true", help="Resolve only; do not write vault")
    link_persons_parser.add_argument(
        "--report-dir",
        default="",
        help="Write person-linking-report.json here",
    )
    link_persons_parser.add_argument("--run-id", default="link-persons", help="Provenance run id for applied writes")
    link_persons_parser.add_argument(
        "--type",
        default="",
        help="Comma-separated card types to resolve (default: all derived entity types)",
    )
    link_persons_parser.add_argument(
        "--provider",
        default="",
        help="LLM provider for conflict disambiguation (ollama or gemini; omit to skip)",
    )
    link_persons_parser.add_argument(
        "--conflict-model",
        default="",
        help="Model for conflict disambiguation (default: gemma4:e4b for ollama, gemini-2.5-flash-lite for gemini)",
    )
    link_persons_parser.add_argument(
        "--vault",
        default="",
        help="Vault path (default: PPA_PATH)",
    )

    resolve_matches_parser = subparsers.add_parser(
        "resolve-matches",
        help="Phase 3 — resolve match_candidates.jsonl into vault wikilinks (thread↔calendar, finance→email)",
    )
    resolve_matches_parser.add_argument(
        "--vault",
        default="",
        help="Vault path (default: PPA_PATH)",
    )
    resolve_matches_parser.add_argument(
        "--staging-root",
        required=True,
        metavar="PATH",
        help="Enrichment staging root containing enrich_*/match_candidates.jsonl",
    )
    resolve_matches_parser.add_argument("--dry-run", action="store_true")
    resolve_matches_parser.add_argument(
        "--cache-db",
        default="",
        metavar="PATH",
        help="SQLite inference cache for LLM disambiguation (recommended: enrichment run cache.db)",
    )
    resolve_matches_parser.add_argument("--run-id", default="phase3-match", help="Provenance / cache run id")
    resolve_matches_parser.add_argument(
        "--provider",
        default="gemini",
        choices=["gemini", "ollama"],
        help="LLM provider for ambiguous matches",
    )
    resolve_matches_parser.add_argument(
        "--model",
        default="gemini-2.5-flash-lite",
        help="Model id (gemini or ollama)",
    )
    resolve_matches_parser.add_argument(
        "--base-url",
        default="http://localhost:11434",
        help="Ollama base URL (ignored for gemini)",
    )
    resolve_matches_parser.add_argument(
        "--progress-every",
        type=int,
        default=50,
        metavar="N",
        help="Log match-candidate progress every N rows (0 = only start/end lines)",
    )
    resolve_matches_parser.add_argument(
        "--vault-cache-progress-every",
        type=int,
        default=5000,
        metavar="N",
        help="During vault-cache rebuild on miss, log every N notes (0 = no mid-build lines)",
    )

    staging_report_parser = subparsers.add_parser("staging-report", help="Inspect extraction staging output")
    staging_report_parser.add_argument("--staging-dir", required=True, help="Path to staging directory")
    staging_report_parser.add_argument("--json", action="store_true", help="Output JSON on stdout")

    promote_parser = subparsers.add_parser("promote-staging", help="Move staged derived cards into the vault")
    promote_parser.add_argument("--staging-dir", required=True, help="Path to staging directory")
    promote_parser.add_argument("--dry-run", action="store_true")

    # Phase 6.5 `ppa linker` subcommand family.
    from archive_cli import \
        linker_cli as _linker_cli  # lazy to avoid import cycles
    _linker_cli.add_parser(subparsers)

    parser.set_defaults(command="serve")
    args = parser.parse_args()
    if args.command == "serve" and not hasattr(args, "tunnel"):
        args.tunnel = ""
    # Stderr-only logging for all subcommands; keep stdout for MCP JSON-RPC / CLI JSON. See archive_cli/log.py.
    configure_logging(verbose=args.verbose)
    log_file = str(getattr(args, "log_file", "") or "").strip()
    if log_file:
        from .log import attach_file_log

        attach_file_log(Path(log_file))
    if args.command == "mcp-config":
        _emit_mcp_config()
        return
    if args.command == "linker":
        rc = _linker_cli.dispatch(args)
        raise SystemExit(rc)
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
                staging_dir=str(getattr(args, "staging_dir", "_artifacts/_staging-llm") or "_artifacts/_staging-llm"),
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
    if args.command == "enrich-cards":
        try:
            from archive_sync.llm_enrichment.card_enrichment_runner import \
                CardEnrichmentRunner

            from .commands._resolve import resolve_vault

            vault_arg = str(getattr(args, "vault", "") or "").strip()
            vault = Path(vault_arg) if vault_arg else resolve_vault()
            cache_raw = str(getattr(args, "cache_db", "") or "").strip()
            cache_db = Path(cache_raw) if cache_raw else None
            prov = str(getattr(args, "provider", "gemini") or "gemini").strip()
            model = str(getattr(args, "model", "") or "").strip()
            if not model:
                model = (
                    DEFAULT_ENRICH_CARD_GEMINI_MODEL
                    if prov == "gemini"
                    else DEFAULT_ENRICH_EXTRACT_MODEL
                )
            vp = float(getattr(args, "vault_percent", 0.0) or 0.0)
            lim = int(getattr(args, "limit", 0) or 0)
            uid_filter_path = str(getattr(args, "uid_filter_file", "") or "").strip()
            workers = max(1, int(getattr(args, "workers", 24)))
            classify_raw = str(getattr(args, "classify_index_db", "") or "").strip()
            classify_index_db = Path(classify_raw) if classify_raw else None
            runner = CardEnrichmentRunner(
                vault_path=vault,
                workflow=str(getattr(args, "workflow", "email_thread") or "email_thread"),
                provider_kind=prov,
                model=model,
                base_url=str(getattr(args, "base_url", "http://localhost:11434") or "http://localhost:11434"),
                cache_db=cache_db,
                run_id=str(getattr(args, "run_id", "") or ""),
                staging_dir=Path(
                    str(getattr(args, "staging_dir", "_artifacts/_staging-enrichment") or "_artifacts/_staging-enrichment")
                ),
                dry_run=bool(getattr(args, "dry_run", False)),
                progress_every=max(1, int(getattr(args, "progress_every", 1) or 1)),
                vault_percent=(vp if vp > 0 else None),
                limit=(lim if lim > 0 else None),
                skip_populated=not bool(getattr(args, "no_skip_populated", False)),
                workers=workers,
                uid_filter_file=Path(uid_filter_path) if uid_filter_path else None,
                classify_index_db=classify_index_db,
            )
            metrics = runner.run()
            _print_json(metrics.to_dict())
        except RuntimeError as exc:
            print(str(exc), file=sys.stderr)
            raise SystemExit(1) from exc
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "extract-document-text":
        try:
            from archive_sync.llm_enrichment.document_text_extractor import \
                run_document_text_extraction

            from .commands._resolve import resolve_vault

            vault_arg = str(getattr(args, "vault", "") or "").strip()
            vault = Path(vault_arg) if vault_arg else resolve_vault()
            lim = int(getattr(args, "limit", 0) or 0)
            out = run_document_text_extraction(
                vault,
                dry_run=bool(getattr(args, "dry_run", False)),
                limit=(lim if lim > 0 else None),
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "enrich":
        try:
            from archive_sync.llm_enrichment.enrichment_orchestrator import (
                EnrichmentOrchestrator, default_run_id)

            vault = Path(str(getattr(args, "vault", "") or "").strip())
            run_id = str(getattr(args, "run_id", "") or "").strip() or default_run_id()
            run_dir_raw = str(getattr(args, "run_dir", "") or "").strip()
            run_dir = Path(run_dir_raw) if run_dir_raw else Path("_artifacts") / "_enrichment-runs" / run_id
            cache_raw = str(getattr(args, "cache_db", "") or "").strip()
            cache_db = Path(cache_raw) if cache_raw else run_dir / "cache.db"
            steps_raw = str(getattr(args, "steps", "") or "").strip()
            enabled = frozenset(s.strip() for s in steps_raw.split(",")) if steps_raw else None
            prov = str(getattr(args, "provider", "gemini") or "gemini").strip()
            model = str(getattr(args, "model", "") or "").strip()
            if not model:
                model = DEFAULT_ENRICH_CARD_GEMINI_MODEL if prov == "gemini" else DEFAULT_ENRICH_EXTRACT_MODEL
            enrich_emails_model = str(getattr(args, "enrich_emails_model", "") or "").strip()
            if not enrich_emails_model:
                enrich_emails_model = "gemini-2.5-flash-lite" if prov == "gemini" else DEFAULT_ENRICH_EXTRACT_MODEL
            orch = EnrichmentOrchestrator(
                vault_path=vault,
                run_id=run_id,
                run_dir=run_dir,
                provider=prov,
                model=model,
                enrich_emails_model=enrich_emails_model,
                base_url=str(getattr(args, "base_url", "http://localhost:11434") or "http://localhost:11434"),
                dry_run=bool(getattr(args, "dry_run", False)),
                workers=int(getattr(args, "workers", 24) or 24),
                enrich_emails_workers=int(getattr(args, "enrich_emails_workers", 8) or 8),
                checkpoint_every=int(getattr(args, "checkpoint_every", 500)),
                cache_db=cache_db,
                enabled_steps=enabled,
                skip_populated=bool(getattr(args, "skip_populated", False)),
            )
            manifest = orch.run()
            _print_json(manifest.to_jsonable())
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "geocode-places":
        try:
            from .commands.geocode import geocode_places

            store = resolve_store()
            result = geocode_places(
                store=store,
                dry_run=bool(getattr(args, "dry_run", False)),
                limit=int(getattr(args, "limit", 0) or 0),
            )
            _print_cli_result(result)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "quality-report":
        try:
            from .commands.quality_report import quality_report
            from .index_config import get_index_dsn, get_index_schema

            result = quality_report(dsn=get_index_dsn(), schema=get_index_schema())
            _print_cli_result(result)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "resolve-entities":
        try:
            from archive_sync.extractors.entity_resolution import \
                run_entity_resolution

            from .commands._resolve import resolve_vault

            vault_arg = str(getattr(args, "vault", "") or "").strip()
            if vault_arg:
                vp = Path(vault_arg).expanduser().resolve()
                if not vp.is_dir():
                    raise VaultNotFoundError(f"Vault not found: {vp}")
                vault = str(vp)
            else:
                vault = str(resolve_vault())
            staging_raw = str(getattr(args, "entity_staging_root", "") or "").strip()
            entity_root = Path(staging_raw) if staging_raw else None
            po_raw = str(getattr(args, "person_mentions_out", "") or "").strip()
            person_out = Path(po_raw) if po_raw else None
            out = run_entity_resolution(
                vault,
                entity_filter=str(args.entity_type),
                dry_run=bool(args.dry_run),
                report_dir=str(getattr(args, "report_dir", "") or "").strip(),
                entity_mentions_staging_root=entity_root,
                person_mentions_out=person_out,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "link-persons":
        try:
            from archive_sync.extractors.entity_resolution import \
                run_person_linking

            from .commands._resolve import resolve_vault

            vault_arg = str(getattr(args, "vault", "") or "").strip()
            if vault_arg:
                vp = Path(vault_arg).expanduser().resolve()
                if not vp.is_dir():
                    raise VaultNotFoundError(f"Vault not found: {vp}")
                vault = str(vp)
            else:
                vault = str(resolve_vault())
            type_raw = str(getattr(args, "type", "") or "").strip()
            card_types = frozenset(t.strip() for t in type_raw.split(",") if t.strip()) if type_raw else None
            out = run_person_linking(
                vault,
                dry_run=bool(getattr(args, "dry_run", False)),
                report_dir=str(getattr(args, "report_dir", "") or "").strip(),
                run_id=str(getattr(args, "run_id", "") or "link-persons"),
                card_types=card_types,
                conflict_provider=str(getattr(args, "provider", "") or "").strip(),
                conflict_model=str(getattr(args, "conflict_model", "") or "").strip(),
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "resolve-matches":
        try:
            from archive_sync.llm_enrichment.match_resolver import \
                run_match_resolution

            from .commands._resolve import resolve_vault

            vault_arg = str(getattr(args, "vault", "") or "").strip()
            if vault_arg:
                vp = Path(vault_arg).expanduser().resolve()
                if not vp.is_dir():
                    raise VaultNotFoundError(f"Vault not found: {vp}")
                vault = str(vp)
            else:
                vault = str(resolve_vault())
            staging = Path(str(getattr(args, "staging_root", "") or ""))
            cache_raw = str(getattr(args, "cache_db", "") or "").strip()
            cache_db = Path(cache_raw) if cache_raw else None
            out = run_match_resolution(
                vault,
                staging,
                provider_kind=str(getattr(args, "provider", "gemini") or "gemini"),
                model=str(getattr(args, "model", "") or "gemini-2.5-flash-lite"),
                base_url=str(getattr(args, "base_url", "http://localhost:11434") or "http://localhost:11434"),
                cache_db=cache_db,
                dry_run=bool(getattr(args, "dry_run", False)),
                run_id=str(getattr(args, "run_id", "") or "phase3-match"),
                progress_every=int(getattr(args, "progress_every", 50) or 0),
                vault_cache_progress_every=int(
                    getattr(args, "vault_cache_progress_every", 5000) or 0
                ),
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
    if args.command == "embed-estimate":
        try:
            store = resolve_store()
            out = status_cmd.embedding_estimate(
                store=store,
                logger=_cli_log,
                embedding_model=args.embedding_model,
                embedding_version=args.embedding_version,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "embed-batch-submit":
        try:
            store = resolve_store()
            out = batch_embed_cmd.embed_batch_submit(
                store=store,
                logger=_cli_log,
                embedding_model=args.embedding_model,
                embedding_version=args.embedding_version,
                max_batches=args.max_batches,
                requests_per_batch=args.requests_per_batch,
                include_context_prefix=(False if args.no_context_prefix else None),
                artifact_dir=args.artifact_dir,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "embed-batch-poll":
        try:
            store = resolve_store()
            out = batch_embed_cmd.embed_batch_poll(store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "embed-batch-ingest":
        try:
            store = resolve_store()
            out = batch_embed_cmd.embed_batch_ingest(
                store=store,
                logger=_cli_log,
                artifact_dir=args.artifact_dir,
                write_batch_size=args.write_batch_size,
                workers=args.workers,
            )
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "embed-batch-status":
        try:
            store = resolve_store()
            out = batch_embed_cmd.embed_batch_status(store=store, logger=_cli_log)
            _print_json(out)
        except PpaError as exc:
            _cli_fail(exc)
        return
    if args.command == "embed-cache-rotate":
        try:
            out = batch_embed_cmd.embed_cache_rotate(
                logger=_cli_log,
                artifact_dir=str(getattr(args, "artifact_dir", "") or ""),
                cache_dir=str(getattr(args, "cache_dir", "") or ""),
                keep=int(getattr(args, "keep", 1) or 1),
                dry_run=bool(getattr(args, "dry_run", False)),
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
        copy_src = str(getattr(args, "copy_from_schema", "") or "").strip()
        if copy_src:
            kwargs["copy_from_schema"] = copy_src
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
            result = admin_cmd.bootstrap_postgres(
                vault=vault,
                logger=_cli_log,
                force=bool(getattr(args, "force", False)),
            )
            _print_cli_result(result)
        except PpaError as exc:
            print(str(exc))
        return
    if args.command == "embed-gc":
        try:
            store = resolve_store()
            result = admin_cmd.embed_gc(
                store=store,
                logger=_cli_log,
                dry_run=not bool(getattr(args, "apply", False)),
            )
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
    if args.command == "slice-bootstrap":
        try:
            from typing import Any as _Any

            from .commands import admin as _admin
            store = resolve_store()
            steps: list[dict[str, _Any]] = []
            t0 = time.monotonic()

            # 1. bootstrap-postgres (idempotent — creates schema if missing).
            _cli_log.info("slice-bootstrap step=1/5 bootstrap-postgres")
            r1 = _admin.bootstrap_postgres(vault=store.vault, logger=_cli_log)
            steps.append({"step": "bootstrap_postgres", **{k: r1.get(k) for k in ("schema", "vector_dimension")}})

            # 2. rebuild-indexes (unless skipped — useful when re-running on an existing schema).
            if not bool(getattr(args, "skip_rebuild", False)):
                _cli_log.info("slice-bootstrap step=2/5 rebuild-indexes")
                r2 = _admin.rebuild_indexes(store=store, logger=_cli_log, workers=args.workers)
                steps.append({"step": "rebuild_indexes", "row_counts": r2})
            else:
                steps.append({"step": "rebuild_indexes", "skipped": True})

            copy_src = str(getattr(args, "copy_from_schema", "") or "").strip()
            if copy_src:
                # 3. copy embeddings (free, fast).
                _cli_log.info("slice-bootstrap step=3/5 copy-embeddings from=%s", copy_src)
                from .index_config import (get_default_embedding_model,
                                           get_default_embedding_version)
                model = get_default_embedding_model()
                version = get_default_embedding_version()
                r3 = store.index.copy_embeddings_from_schema(
                    source_schema=copy_src,
                    embedding_model=model,
                    embedding_version=version,
                )
                steps.append({"step": "copy_embeddings", **dict(r3)})

                # 4. copy classifications (free, fast).
                _cli_log.info("slice-bootstrap step=4/5 copy-classifications from=%s", copy_src)
                try:
                    r4 = store.index.copy_classifications_from_schema(source_schema=copy_src)
                    steps.append({"step": "copy_classifications", **dict(r4)})
                except RuntimeError as exc:
                    steps.append({"step": "copy_classifications", "skipped": True, "reason": str(exc)})
            else:
                steps.append({"step": "copy_embeddings", "skipped": True, "reason": "no --copy-from-schema"})
                steps.append({"step": "copy_classifications", "skipped": True, "reason": "no --copy-from-schema"})

            # 5. Build IVFFlat (or rebuild if data changed).
            _cli_log.info("slice-bootstrap step=5/5 build-vector-index")
            r5 = store.index.build_vector_index()
            steps.append({"step": "build_vector_index", **dict(r5)})

            elapsed = round(time.monotonic() - t0, 2)
            _print_json({"slice_bootstrap": "ok", "elapsed_seconds": elapsed, "steps": steps})
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
        schema = os.environ.get("PPA_INDEX_SCHEMA", "ppa")
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

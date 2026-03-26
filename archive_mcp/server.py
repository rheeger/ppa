"""Archive MCP server backed by the shared HFA library."""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:  # pragma: no cover

    class FastMCP:  # type: ignore[override]
        def __init__(self, *_args, **_kwargs):
            pass

        def tool(self):
            def decorator(func):
                return func

            return decorator

        def run(self):
            raise RuntimeError("mcp package is required to run ppa")


from .commands import admin, explain
from .commands import graph as graph_cmd
from .commands import query as query_cmd
from .commands import read as read_cmd
from .commands import search as search_cmd
from .commands import seed_links as seed_cmd
from .commands import status as status_cmd
from .commands._resolve import resolve_index, resolve_store
from .embedding_provider import get_embedding_provider
from .errors import InvalidInputError, PpaError, SeedLinksDisabledError
from .index_config import get_seed_links_enabled
from .index_store import (
    BaseArchiveIndex,
    get_archive_index,
    get_default_embedding_model,
    get_default_embedding_version,
)
from .store import get_archive_store

_SEED_LINKS_DISABLED_MSG = "Seed links are not enabled. Set PPA_SEED_LINKS_ENABLED=1 to enable."

_log = logging.getLogger("ppa.server")


def _import_seed_links():
    from .commands.seed_links import default_seed_link_imports

    return default_seed_link_imports()


def _log_tool_call(tool_name: str, **params: object) -> float:
    """Log tool invocation at INFO. Returns monotonic start time for elapsed calculation."""
    parts = " ".join(f"{k}={v!r}" for k, v in params.items())
    _log.info("tool_start tool=%s %s", tool_name, parts)
    return time.monotonic()


def _log_tool_done(tool_name: str, t0: float, **extra: object) -> None:
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    extras = " ".join(f"{k}={v!r}" for k, v in extra.items())
    if extras:
        _log.info("tool_done tool=%s elapsed_ms=%d %s", tool_name, elapsed_ms, extras)
    else:
        _log.info("tool_done tool=%s elapsed_ms=%d", tool_name, elapsed_ms)


def _log_tool_return_error(tool_name: str, message: str) -> str:
    _log.error("tool=%s error=%s", tool_name, message)
    return message


_instance_name = os.environ.get("PPA_INSTANCE_NAME", "Personal Private Archives").strip()
_server_instructions = (
    f"{_instance_name}\n\n"
    "Retrieval order: (1) archive_read for known UIDs/paths (2) archive_query for structured filters "
    "(3) archive_search_json for keywords (4) archive_hybrid_search_json for semantic queries "
    "(5) archive_graph for relationships. Always read canonical cards before factual claims. "
    "Prefer _json tool variants when available."
)
mcp = FastMCP("ppa", _server_instructions)

_TOOL_PROFILES: dict[str, set[str] | None] = {
    "full": None,
    "read-only": {
        "archive_search",
        "archive_read",
        "archive_query",
        "archive_graph",
        "archive_person",
        "archive_timeline",
        "archive_stats",
        "archive_vector_search",
        "archive_hybrid_search",
        "archive_search_json",
        "archive_hybrid_search_json",
        "archive_read_many",
        "archive_status_json",
        "archive_retrieval_explain_json",
    },
    "remote-read": {
        "archive_search",
        "archive_query",
        "archive_timeline",
        "archive_stats",
        "archive_search_json",
    },
    "admin-only": {
        "archive_validate",
        "archive_duplicates",
        "archive_duplicate_uids",
        "archive_rebuild_indexes",
        "archive_bootstrap_postgres",
        "archive_index_status",
        "archive_projection_inventory",
        "archive_projection_status",
        "archive_projection_explain",
        "archive_retrieval_explain",
        "archive_embedding_status",
        "archive_embedding_backlog",
        "archive_embed_pending",
        "archive_seed_link_surface",
        "archive_seed_link_enqueue",
        "archive_seed_link_backfill",
        "archive_seed_link_refresh",
        "archive_seed_link_worker",
        "archive_seed_link_promote",
        "archive_seed_link_report",
        "archive_link_candidates",
        "archive_link_candidate",
        "archive_review_link_candidate",
        "archive_link_quality_gate",
        "archive_status_json",
    },
}


def get_vault() -> Path:
    return Path(os.environ.get("PPA_PATH", Path.home() / "Archive" / "vault"))


def get_index(vault: Path | None = None) -> BaseArchiveIndex:
    return get_archive_index(vault or get_vault())


def get_store(vault: Path | None = None):
    resolved_vault = vault or get_vault()
    return get_archive_store(
        vault=resolved_vault,
        index=get_index(resolved_vault),
        provider_factory=get_embedding_provider,
    )


def _load_store(vault: Path):
    try:
        return get_store(vault), None
    except RuntimeError as exc:
        return None, str(exc)


def _tool_profile_error(tool_name: str) -> str | None:
    from .index_config import _ppa_env

    profile = _ppa_env("PPA_MCP_TOOL_PROFILE", default="full").lower() or "full"
    allowed = _TOOL_PROFILES.get(profile)
    if allowed is None:
        return None
    if tool_name in allowed:
        return None
    return f"Tool disabled by PPA_MCP_TOOL_PROFILE={profile}"


def _format_search_line(row: dict) -> str:
    """Render a search/query result row with type, date, and fuller summary."""
    rel_path = row.get("rel_path", "")
    card_type = row.get("type", "")
    date = str(row.get("activity_at", ""))[:10]
    summary = str(row.get("summary", ""))[:200]
    meta = ", ".join(part for part in [card_type, date] if part)
    return f"- {rel_path} [{meta}]: {summary}"


def _ppa_err(tool: str, exc: BaseException) -> str:
    _log.error("tool=%s ppa_error=%s", tool, str(exc))
    return str(exc)


def _format_seed_surface(payload: dict) -> str:
    scope_rows = payload["scope"]
    policy_rows = payload["policies"]
    lines = ["Archive seed link surface:", "", "Scope:"]
    for row in scope_rows:
        modules = ",".join(row["modules"])
        lines.append(f"- priority={row['priority']} type={row['card_type']} modules={modules}")
    lines.extend(["", "Policies:"])
    for row in policy_rows:
        target_field = f" field={row['canonical_field_name']}" if row["canonical_field_name"] else ""
        lines.append(
            f"- {row['link_type']} module={row['module_name']} surface={row['surface']} "
            f"promotion={row['promotion_target']}{target_field} auto={row['auto_promote_floor']:.2f} "
            f"canonical={row['canonical_floor']:.2f}"
        )
    return "\n".join(lines)


@mcp.tool()
def archive_search(query: str, limit: int = 20) -> str:
    """Full-text search across all notes."""

    t0 = _log_tool_call("archive_search", query=query, limit=limit)
    try:
        profile_error = _tool_profile_error("archive_search")
        if profile_error:
            return _log_tool_return_error("archive_search", profile_error)
        store = resolve_store()
        result = search_cmd.search(query, limit=limit, store=store, logger=_log)
        rows = result["rows"]
        out = "\n".join(_format_search_line(r) for r in rows) if rows else "No matches"
        _log_tool_done("archive_search", t0, result_count=len(rows))
        return out
    except PpaError as exc:
        return _ppa_err("archive_search", exc)
    except Exception as exc:
        _log.error("tool=archive_search error=%s", str(exc))
        raise


@mcp.tool()
def archive_read(path_or_uid: str) -> str:
    """Read note by relative path or UID."""

    t0 = _log_tool_call("archive_read", path_or_uid=path_or_uid)
    try:
        profile_error = _tool_profile_error("archive_read")
        if profile_error:
            return _log_tool_return_error("archive_read", profile_error)
        store = resolve_store()
        payload = read_cmd.read(path_or_uid, store=store, logger=_log)
        if not payload.get("found"):
            _log_tool_done("archive_read", t0, found=False)
            return "Not found"
        _log_tool_done("archive_read", t0, found=True)
        return str(payload.get("content", ""))
    except PpaError as exc:
        return _ppa_err("archive_read", exc)
    except Exception as exc:
        _log.error("tool=archive_read error=%s", str(exc))
        raise


@mcp.tool()
def archive_query(
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    org_filter: str = "",
    limit: int = 20,
) -> str:
    """Structured query by frontmatter fields."""

    t0 = _log_tool_call(
        "archive_query",
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        limit=limit,
    )
    try:
        profile_error = _tool_profile_error("archive_query")
        if profile_error:
            return _log_tool_return_error("archive_query", profile_error)
        store = resolve_store()
        result = query_cmd.query(
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            org_filter=org_filter,
            limit=limit,
            store=store,
            logger=_log,
        )
        rows = result["rows"]
        out = "\n".join(_format_search_line(r) for r in rows) if rows else "No matches"
        _log_tool_done("archive_query", t0, result_count=len(rows))
        return out
    except PpaError as exc:
        return _ppa_err("archive_query", exc)
    except Exception as exc:
        _log.error("tool=archive_query error=%s", str(exc))
        raise


@mcp.tool()
def archive_graph(note_path: str, hops: int = 2) -> str:
    """Get notes linked from the given note via wikilinks."""

    profile_error = _tool_profile_error("archive_graph")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        payload = graph_cmd.graph(note_path, hops=hops, store=store, logger=_log)
        rel_path = str(payload.get("rel_path", note_path))
        graph = payload.get("graph")
        if graph is None:
            return "Note not found"

        lines = [f"Graph from {rel_path}:"]
        for source, targets in graph.items():
            lines.append(f"- {source}")
            for target in targets:
                lines.append(f"  -> {target}")
        return "\n".join(lines) if len(lines) > 1 else "No linked notes"
    except PpaError as exc:
        return _ppa_err("archive_graph", exc)


@mcp.tool()
def archive_person(name: str) -> str:
    """Get person profile by slug."""

    profile_error = _tool_profile_error("archive_person")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        result = graph_cmd.person(name, store=store, logger=_log)
        if not result.get("found"):
            return "Person not found"
        return str(result.get("content", ""))
    except PpaError as exc:
        return _ppa_err("archive_person", exc)


@mcp.tool()
def archive_timeline(start_date: str = "", end_date: str = "", limit: int = 20) -> str:
    """Notes in date range."""

    profile_error = _tool_profile_error("archive_timeline")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        result = graph_cmd.timeline(
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            store=store,
            logger=_log,
        )
        rows = result["rows"]
        results = [f"- {str(row['created'])[:10]} {row['rel_path']}: {str(row['summary'])[:160]}" for row in rows]
        return "\n".join(results) if results else "No matches"
    except PpaError as exc:
        return _ppa_err("archive_timeline", exc)


@mcp.tool()
def archive_stats() -> str:
    """Vault health metrics."""

    profile_error = _tool_profile_error("archive_stats")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = status_cmd.stats(index=index, logger=_log)
        total = result["total"]
        by_type = result["by_type"]
        by_source = result["by_source"]
        lines = [f"Total: {total} notes", "", "By type:"]
        for row in by_type:
            lines.append(f"  {row['type']}: {row['count']}")
        lines.extend(["", "By source:"])
        for row in by_source:
            lines.append(f"  {row['source']}: {row['count']}")
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_stats", exc)


@mcp.tool()
def archive_validate() -> str:
    """Run schema validation on all cards, return summary."""

    profile_error = _tool_profile_error("archive_validate")
    if profile_error:
        return profile_error
    try:
        vault = resolve_store().vault
        result = status_cmd.validate(vault=vault, logger=_log)
        valid = result["valid"]
        total = result["total"]
        errors = result["errors"]
        lines = [f"Validated {valid}/{total} notes"]
        if errors:
            lines.append("Errors:")
            lines.extend(errors[:20])
            if len(errors) > 20:
                lines.append(f"... and {len(errors) - 20} more")
        else:
            lines.append("0 errors")
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_validate", exc)


@mcp.tool()
def archive_duplicates() -> str:
    """Return pending dedup candidates from _meta/dedup-candidates.json."""

    profile_error = _tool_profile_error("archive_duplicates")
    if profile_error:
        return profile_error
    try:
        vault = resolve_store().vault
        result = status_cmd.duplicates(vault=vault, logger=_log)
        st = result["status"]
        if st == "missing":
            return "No pending duplicate candidates"
        if st == "parse_error":
            return "Could not parse dedup candidates"
        if st == "empty":
            return "No pending duplicate candidates"
        payload = result["candidates"]
        lines: list[str] = []
        for candidate in payload[:20]:
            if not isinstance(candidate, dict):
                continue
            existing = str(candidate.get("existing", ""))
            confidence = candidate.get("confidence", "")
            incoming = candidate.get("incoming", {})
            incoming_summary = incoming.get("summary", "") if isinstance(incoming, dict) else ""
            lines.append(f"- {incoming_summary} -> {existing} ({confidence})")
        return "\n".join(lines) if lines else "No pending duplicate candidates"
    except PpaError as exc:
        return _ppa_err("archive_duplicates", exc)


@mcp.tool()
def archive_duplicate_uids(limit: int = 20) -> str:
    """Return duplicate UID collisions from the derived index."""

    profile_error = _tool_profile_error("archive_duplicate_uids")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = status_cmd.duplicate_uids(limit=limit, index=index, logger=_log)
        rows = result["rows"]
        if not rows:
            return "No duplicate UID rows"
        lines = ["Archive duplicate UID rows:"]
        for row in rows:
            lines.append(
                f"- uid={row['uid']} group_size={row['duplicate_group_size']} preferred={row['preferred_rel_path']} "
                f"duplicate={row['duplicate_rel_path']} preferred_type={row['preferred_type']} duplicate_type={row['duplicate_type']}"
            )
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_duplicate_uids", exc)


@mcp.tool()
def archive_rebuild_indexes() -> str:
    """Rebuild the derived archive index from canonical markdown cards."""

    profile_error = _tool_profile_error("archive_rebuild_indexes")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        counts = index.rebuild()
        return (
            f"Rebuilt archive index at {index.location}\n"
            f"- cards: {counts['cards']}\n"
            f"- external_ids: {counts['external_ids']}\n"
            f"- edges: {counts['edges']}\n"
            f"- chunks: {counts['chunks']}\n"
            f"- duplicate_uids: {counts['duplicate_uids']}"
        )
    except PpaError as exc:
        return _ppa_err("archive_rebuild_indexes", exc)


@mcp.tool()
def archive_bootstrap_postgres() -> str:
    """Bootstrap the Postgres archive index schema and pgvector-ready tables."""

    profile_error = _tool_profile_error("archive_bootstrap_postgres")
    if profile_error:
        return profile_error
    try:
        vault = resolve_store().vault
        result = admin.bootstrap_postgres(vault=vault, logger=_log)
        lines = ["Bootstrapped Postgres archive index:"]
        for key in sorted(result):
            lines.append(f"- {key}: {result[key]}")
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_bootstrap_postgres", exc)


@mcp.tool()
def archive_index_status() -> str:
    """Report the current derived index status."""

    profile_error = _tool_profile_error("archive_index_status")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        status = status_cmd.index_status(store=store, logger=_log)
        if not status:
            return "No index metadata found"
        lines = ["Archive index status:"]
        for key in sorted(status):
            lines.append(f"- {key}: {status[key]}")
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_index_status", exc)


@mcp.tool()
def archive_projection_inventory() -> str:
    """List registered archive projections."""

    profile_error = _tool_profile_error("archive_projection_inventory")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        payload = admin.projection_inventory(store=store, logger=_log)
        lines = ["Archive projection inventory:"]
        for projection in payload.get("projections", []):
            lines.append(
                f"- {projection['name']} table={projection['table_name']} kind={projection['kind']} "
                f"types={','.join(projection['applies_to_types'])}"
            )
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_projection_inventory", exc)


@mcp.tool()
def archive_projection_status() -> str:
    """Report typed projection coverage and readiness."""

    profile_error = _tool_profile_error("archive_projection_status")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        payload = admin.projection_status(store=store, logger=_log)
        lines = ["Archive projection status:"]
        for row in payload.get("projection_coverage", []):
            blockers = ",".join(row.get("migration_blockers", []))
            lines.append(
                f"- {row['card_type']} projection={row['typed_projection']} rows={row['materialized_row_count']} "
                f"ready_ratio={float(row['canonical_ready_ratio']):.2f} blockers={blockers}"
            )
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_projection_status", exc)


@mcp.tool()
def archive_projection_explain(card_uid: str) -> str:
    """Explain how a typed projection row is populated."""

    profile_error = _tool_profile_error("archive_projection_explain")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        payload = admin.projection_explain(card_uid, store=store, logger=_log)
        lines = [
            f"Archive projection explain for {card_uid}:",
            f"- card_type: {payload.get('card_type', '')}",
            f"- typed_projection: {payload.get('typed_projection', '')}",
            f"- canonical_ready: {payload.get('canonical_ready', False)}",
        ]
        for mapping in payload.get("field_mappings", [])[:20]:
            fields = ",".join(mapping.get("canonical_fields", []))
            lines.append(f"- {mapping['typed_column']} <- {fields} ({mapping['status']})")
        if payload.get("migration_notes"):
            lines.append(f"- migration_notes: {'; '.join(payload['migration_notes'])}")
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_projection_explain", exc)


@mcp.tool()
def archive_embedding_status(embedding_model: str = "", embedding_version: int = 0) -> str:
    """Report embedding backlog status for a given model/version."""

    profile_error = _tool_profile_error("archive_embedding_status")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        status = status_cmd.embedding_status(
            store=store,
            logger=_log,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
        )
        lines = ["Archive embedding status:"]
        for key in (
            "embedding_model",
            "embedding_version",
            "chunk_schema_version",
            "chunk_count",
            "embedded_chunk_count",
            "pending_chunk_count",
        ):
            lines.append(f"- {key}: {status[key]}")
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_embedding_status", exc)


@mcp.tool()
def archive_embedding_backlog(limit: int = 20, embedding_model: str = "", embedding_version: int = 0) -> str:
    """List chunks that are still pending embeddings for a given model/version."""

    profile_error = _tool_profile_error("archive_embedding_backlog")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        payload = status_cmd.embedding_backlog(
            store=store,
            logger=_log,
            limit=limit,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
        )
        model = str(payload["embedding_model"])
        version = int(payload["embedding_version"])
        rows = payload["rows"]
        if not rows:
            return f"No pending chunks for {model} v{version}"
        lines = [f"Embedding backlog for {model} v{version}:"]
        for row in rows:
            preview = str(row["content"]).replace("\n", " ")[:80]
            lines.append(
                f"- {row['rel_path']} [{row['chunk_type']}#{row['chunk_index']}] ({row['token_count']} tokens): {preview}"
            )
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_embedding_backlog", exc)


@mcp.tool()
def archive_embed_pending(limit: int = 20, embedding_model: str = "", embedding_version: int = 0) -> str:
    """Generate embeddings for pending chunks using the configured embedding provider."""

    profile_error = _tool_profile_error("archive_embed_pending")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        result = admin.embed_pending(
            store=store,
            logger=_log,
            limit=limit,
            embedding_model=embedding_model.strip(),
            embedding_version=embedding_version,
        )
        lines = [
            f"Embedded chunks for {result['embedding_model']} v{result['embedding_version']}",
            f"- provider: {result['provider']}",
            f"- chunk_schema_version: {result['chunk_schema_version']}",
            f"- batch_size: {result['batch_size']}",
            f"- embedded: {result['embedded']}",
            f"- failed: {result['failed']}",
        ]
        if result.get("write_batch_size") is not None:
            lines.insert(4, f"- write_batch_size: {result['write_batch_size']}")
        if result.get("concurrency") is not None:
            lines.insert(5, f"- concurrency: {result['concurrency']}")
        if result.get("last_error"):
            lines.append(f"- last_error: {result['last_error']}")
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_embed_pending", exc)


@mcp.tool()
def archive_seed_link_surface() -> str:
    """Describe the seed link review scope and promotion surface."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_surface")
    if profile_error:
        return profile_error
    try:
        payload = seed_cmd.seed_link_surface(logger=_log)
        return _format_seed_surface(payload)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_seed_link_surface", exc)


@mcp.tool()
def archive_seed_link_enqueue(
    modules: str = "",
    source_uids: str = "",
    job_type: str = "seed_backfill",
    reset_existing: bool = False,
) -> str:
    """Enqueue seed link jobs without processing them."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_enqueue")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = seed_cmd.seed_link_enqueue(
            index=index,
            logger=_log,
            modules=modules,
            source_uids=source_uids,
            job_type=job_type,
            reset_existing=reset_existing,
        )
        return (
            "Archive seed link enqueue:\n"
            f"- job_type: {job_type}\n"
            f"- prepared: {result['prepared']}\n"
            f"- enqueued: {result['enqueued']}\n"
            f"- existing: {result['existing']}"
        )
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_seed_link_enqueue", exc)


@mcp.tool()
def archive_seed_link_backfill(
    limit: int = 0,
    modules: str = "",
    workers: int = 0,
    include_llm: bool = True,
    apply_promotions: bool = True,
) -> str:
    """Run the seed link review backfill before embeddings."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_backfill")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = seed_cmd.seed_link_backfill(
            index=index,
            logger=_log,
            limit=limit,
            modules=modules,
            workers=workers,
            include_llm=include_llm,
            apply_promotions=apply_promotions,
        )
        lines = ["Archive seed link backfill:"]
        for key in (
            "workers",
            "jobs_enqueued",
            "jobs_completed",
            "jobs_failed",
            "candidates",
            "needs_review",
            "auto_promoted",
            "canonical_safe",
            "derived_promotions_applied",
            "canonical_applied",
            "llm_judged",
            "promotion_blocked",
            "orphaned_links_before",
            "orphaned_links_after",
        ):
            lines.append(f"- {key}: {result[key]}")
        return "\n".join(lines)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_seed_link_backfill", exc)


@mcp.tool()
def archive_seed_link_refresh(
    source_uids: str,
    modules: str = "",
    workers: int = 0,
    include_llm: bool = True,
    apply_promotions: bool = True,
) -> str:
    """Re-run seed link review for specific changed cards."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_refresh")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = seed_cmd.seed_link_refresh(
            index=index,
            logger=_log,
            source_uids=source_uids,
            modules=modules,
            workers=workers,
            include_llm=include_llm,
            apply_promotions=apply_promotions,
        )
        lines = ["Archive seed link refresh:"]
        for key in (
            "job_type",
            "jobs_enqueued",
            "jobs_completed",
            "jobs_failed",
            "candidates",
            "needs_review",
            "auto_promoted",
            "canonical_safe",
            "derived_promotions_applied",
            "canonical_applied",
        ):
            lines.append(f"- {key}: {result[key]}")
        return "\n".join(lines)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_seed_link_refresh", exc)


@mcp.tool()
def archive_seed_link_worker(limit: int = 0, modules: str = "", workers: int = 0, include_llm: bool = True) -> str:
    """Process pending seed link jobs from the shared queue."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_worker")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = seed_cmd.seed_link_worker(
            index=index,
            logger=_log,
            limit=limit,
            modules=modules,
            workers=workers,
            include_llm=include_llm,
        )
        lines = ["Archive seed link worker:"]
        for key in (
            "workers",
            "jobs_completed",
            "jobs_failed",
            "candidates",
            "needs_review",
            "auto_promoted",
            "canonical_safe",
            "llm_judged",
        ):
            lines.append(f"- {key}: {result[key]}")
        return "\n".join(lines)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_seed_link_worker", exc)


@mcp.tool()
def archive_seed_link_promote(limit: int = 0, workers: int = 1) -> str:
    """Process queued seed link promotions from the shared queue."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_promote")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = seed_cmd.seed_link_promote(index=index, logger=_log, limit=limit, workers=workers)
        return (
            "Archive seed link promote:\n"
            f"- derived_edge: {result['derived_edge']}\n"
            f"- canonical_field: {result['canonical_field']}\n"
            f"- blocked: {result['blocked']}"
        )
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_seed_link_promote", exc)


@mcp.tool()
def archive_seed_link_report(rebuild_if_dirty: bool = True) -> str:
    """Finalize and report the current shared seed link queue state."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_report")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        payload = seed_cmd.seed_link_report(index=index, logger=_log, rebuild_if_dirty=bool(rebuild_if_dirty))
        lines = ["Archive seed link report:"]
        for key in (
            "rebuilt",
            "passes",
            "seed_card_count",
            "reviewable_seed_card_count",
            "total_cards_reviewed",
            "scan_coverage",
            "orphaned_links_after",
            "duplicate_uid_count",
            "high_priority_review_backlog",
            "high_risk_precision",
        ):
            lines.append(f"- {key}: {payload[key]}")
        return "\n".join(lines)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_seed_link_report", exc)


@mcp.tool()
def archive_link_candidates(
    status: str = "",
    module_name: str = "",
    min_confidence: float = 0.0,
    limit: int = 20,
) -> str:
    """List seed link candidates and review queue items."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_link_candidates")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        result = seed_cmd.link_candidates(
            index=index,
            logger=_log,
            status=status,
            module_name=module_name,
            min_confidence=min_confidence,
            limit=limit,
        )
        rows = result["rows"]
        if not rows:
            return "No link candidates"
        lines = ["Archive link candidates:"]
        for row in rows:
            promotion_status = f" promotion={row['promotion_status']}" if row.get("promotion_status") else ""
            lines.append(
                f"- id={row['candidate_id']} module={row['module_name']} score={float(row['final_confidence']):.4f} "
                f"status={row['status']} decision={row['decision']} type={row['proposed_link_type']}{promotion_status}: "
                f"{row['source_rel_path']} -> {row['target_rel_path']}"
            )
        return "\n".join(lines)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_link_candidates", exc)


@mcp.tool()
def archive_link_candidate(candidate_id: int) -> str:
    """Show a single seed link candidate with evidence."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_link_candidate")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        payload = seed_cmd.link_candidate(candidate_id, index=index, logger=_log)
        if payload is None:
            return "Candidate not found"
        lines = [
            f"Archive link candidate {candidate_id}:",
            f"- module: {payload['module_name']}",
            f"- type: {payload['proposed_link_type']}",
            f"- source: {payload['source_rel_path']}",
            f"- target: {payload['target_rel_path']}",
            f"- status: {payload['status']}",
            f"- confidence: {float(payload['final_confidence']):.4f}",
            f"- decision: {payload['decision']}",
            f"- reason: {payload['decision_reason']}",
            f"- scores: deterministic={float(payload['deterministic_score']):.4f} lexical={float(payload['lexical_score']):.4f} "
            f"graph={float(payload['graph_score']):.4f} llm={float(payload['llm_score']):.4f} risk={float(payload['risk_penalty']):.4f}",
        ]
        if payload.get("promotion_target"):
            lines.append(
                f"- promotion: target={payload['promotion_target']} status={payload.get('promotion_status', '')} "
                f"field={payload.get('target_field_name', '')} blocked_reason={payload.get('blocked_reason', '')}"
            )
        if payload.get("llm_model"):
            lines.append(f"- llm_model: {payload['llm_model']}")
        if payload.get("evidence"):
            lines.append("Evidence:")
            for evidence in payload["evidence"][:20]:
                lines.append(
                    f"  - {evidence['evidence_type']} source={evidence['evidence_source']} "
                    f"{evidence['feature_name']}={evidence['feature_value']} weight={float(evidence['feature_weight']):.2f}"
                )
        if payload.get("reviews"):
            lines.append("Reviews:")
            for review in payload["reviews"][:10]:
                lines.append(
                    f"  - {review['created_at']} reviewer={review['reviewer']} action={review['action']} "
                    f"score={float(review['score_at_review']):.4f} decision={review['decision_at_review']}"
                )
        return "\n".join(lines)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_link_candidate", exc)


@mcp.tool()
def archive_review_link_candidate(candidate_id: int, reviewer: str, action: str, notes: str = "") -> str:
    """Approve, reject, or override a seed link candidate."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_review_link_candidate")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        payload = seed_cmd.review_link_candidate(
            index=index,
            logger=_log,
            candidate_id=candidate_id,
            reviewer=reviewer,
            action=action,
            notes=notes,
        )
        return (
            f"Reviewed candidate {candidate_id}\n"
            f"- action: {action}\n"
            f"- status: {payload.get('status', '')}\n"
            f"- decision: {payload.get('decision', '')}\n"
            f"- confidence: {float(payload.get('final_confidence', 0.0)):.4f}"
        )
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_review_link_candidate", exc)


@mcp.tool()
def archive_link_quality_gate() -> str:
    """Report whether the seed link review has reached the quality gate."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_link_quality_gate")
    if profile_error:
        return profile_error
    try:
        index = resolve_index()
        gate = seed_cmd.link_quality_gate(index=index, logger=_log)
        lines = ["Archive link quality gate:"]
        for key in (
            "passes",
            "seed_card_count",
            "total_cards_reviewed",
            "scan_coverage",
            "required_scan_coverage",
            "orphaned_links_after",
            "duplicate_uid_count",
            "dead_end_count",
            "high_priority_review_backlog",
            "max_high_priority_review_backlog",
            "high_risk_precision",
            "required_high_risk_precision",
        ):
            lines.append(f"- {key}: {gate[key]}")
        if gate.get("candidate_counts"):
            lines.append("Candidate counts:")
            for row in gate["candidate_counts"][:20]:
                lines.append(f"  - {row['module_name']} {row['proposed_link_type']}: {row['count']}")
        if gate.get("auto_promoted_counts"):
            lines.append("Auto promoted counts:")
            for row in gate["auto_promoted_counts"][:20]:
                lines.append(f"  - {row['module_name']} {row['proposed_link_type']}: {row['count']}")
        return "\n".join(lines)
    except SeedLinksDisabledError:
        return _SEED_LINKS_DISABLED_MSG
    except PpaError as exc:
        return _ppa_err("archive_link_quality_gate", exc)


@mcp.tool()
def archive_vector_search(
    query: str,
    limit: int = 20,
    embedding_model: str = "",
    embedding_version: int = 0,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Run semantic search over embedded chunks."""

    t0 = _log_tool_call(
        "archive_vector_search",
        query=query,
        limit=limit,
        embedding_model=embedding_model,
        embedding_version=embedding_version,
    )
    try:
        profile_error = _tool_profile_error("archive_vector_search")
        if profile_error:
            return _log_tool_return_error("archive_vector_search", profile_error)
        store = resolve_store()
        model = embedding_model.strip() or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        result = search_cmd.vector_search(
            query,
            store=store,
            logger=_log,
            limit=limit,
            embedding_model=model,
            embedding_version=version,
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            start_date=start_date,
            end_date=end_date,
        )
        rows = result["rows"]
        if not rows:
            _log_tool_done("archive_vector_search", t0, result_count=0)
            return f"No vector matches for {model} v{version}"
        lines = [f"Vector matches for {model} v{version}:"]
        for row in rows:
            card_type = str(row.get("type", ""))
            date = str(row.get("activity_at", ""))[:10]
            summary = str(row.get("summary", ""))[:200]
            lines.append(
                f"- {row['rel_path']} [{card_type}, {date}] matched_by={row['matched_by']} score={float(row['score']):.4f} "
                f"sim={float(row['similarity']):.4f} chunk={row['chunk_type']}#{row['chunk_index']} "
                f"provenance_bias={row['provenance_bias']} matched_chunks={row['matched_chunk_count']}\n"
                f"  summary: {summary}\n"
                f"  preview: {row['preview']}"
            )
        _log_tool_done("archive_vector_search", t0, result_count=len(rows))
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_vector_search", exc)
    except Exception as exc:
        _log.error("tool=archive_vector_search error=%s", str(exc))
        raise


@mcp.tool()
def archive_retrieval_explain(
    query: str,
    mode: str = "hybrid",
    limit: int = 10,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> str:
    """Explain retrieval scoring and context for a query."""

    profile_error = _tool_profile_error("archive_retrieval_explain")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        payload = explain.retrieval_explain(
            query,
            store=store,
            logger=_log,
            mode=mode,
            limit=limit,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
        )
        return json.dumps(payload, indent=2)
    except PpaError as exc:
        return _ppa_err("archive_retrieval_explain", exc)


@mcp.tool()
def archive_hybrid_search(
    query: str,
    limit: int = 20,
    embedding_model: str = "",
    embedding_version: int = 0,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Combine lexical and semantic retrieval into a ranked card-level result set."""

    t0 = _log_tool_call(
        "archive_hybrid_search",
        query=query,
        limit=limit,
        embedding_model=embedding_model,
        embedding_version=embedding_version,
    )
    try:
        profile_error = _tool_profile_error("archive_hybrid_search")
        if profile_error:
            return _log_tool_return_error("archive_hybrid_search", profile_error)
        store = resolve_store()
        model = embedding_model.strip() or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        payload = search_cmd.hybrid_search(
            query,
            store=store,
            logger=_log,
            limit=limit,
            embedding_model=model,
            embedding_version=version,
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            start_date=start_date,
            end_date=end_date,
        )
        rows = payload["rows"]
        if not rows:
            _log_tool_done("archive_hybrid_search", t0, result_count=0)
            return f"No hybrid matches for '{query}'"
        lines = [f"Hybrid matches for '{query}':"]
        for row in rows:
            card_type = str(row.get("type", ""))
            date = str(row.get("activity_at", ""))[:10]
            graph_hops = f" graph_hops={row['graph_hops']}" if row.get("graph_hops") else ""
            chunk = ""
            if int(row.get("chunk_index", -1)) >= 0 and str(row.get("chunk_type", "")):
                chunk = f" chunk={row['chunk_type']}#{row['chunk_index']}"
            summary = str(row.get("summary", ""))[:200]
            preview = str(row.get("preview", ""))
            lines.append(
                f"- {row['rel_path']} [{card_type}, {date}] matched_by={row['matched_by']} score={float(row['score']):.4f} "
                f"lexical={float(row['lexical_score']):.4f} vector={float(row['vector_similarity']):.4f} "
                f"exact_match={str(bool(row['exact_match'])).lower()}{graph_hops}{chunk} "
                f"provenance_bias={row['provenance_bias']}\n"
                f"  summary: {summary}\n"
                f"  preview: {preview}"
            )
        _log_tool_done("archive_hybrid_search", t0, result_count=len(rows))
        return "\n".join(lines)
    except PpaError as exc:
        return _ppa_err("archive_hybrid_search", exc)
    except Exception as exc:
        _log.error("tool=archive_hybrid_search error=%s", str(exc))
        raise


@mcp.tool()
def archive_search_json(query: str, limit: int = 20) -> str:
    """Lexical search results as JSON (paths + summaries, no embedding)."""

    profile_error = _tool_profile_error("archive_search_json")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        result = search_cmd.search(query, limit=limit, store=store, logger=_log)
        return json.dumps(result, indent=2)
    except PpaError as exc:
        return str(exc)


@mcp.tool()
def archive_hybrid_search_json(
    query: str,
    limit: int = 20,
    embedding_model: str = "",
    embedding_version: int = 0,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Hybrid retrieval as structured JSON (rows + embedding model metadata)."""

    profile_error = _tool_profile_error("archive_hybrid_search_json")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        model = embedding_model.strip() or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        payload = search_cmd.hybrid_search(
            query,
            store=store,
            logger=_log,
            limit=limit,
            embedding_model=model,
            embedding_version=version,
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            start_date=start_date,
            end_date=end_date,
        )
        return json.dumps(payload, indent=2)
    except PpaError as exc:
        return str(exc)


@mcp.tool()
def archive_read_many(paths_json: str) -> str:
    """Read multiple notes by rel path or card uid. `paths_json` is a JSON array of strings."""

    profile_error = _tool_profile_error("archive_read_many")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        paths = read_cmd.parse_paths_json(paths_json)
        result = read_cmd.read_many(paths, store=store, logger=_log)
        return json.dumps(result, indent=2)
    except InvalidInputError as exc:
        return str(exc)
    except PpaError as exc:
        return str(exc)


@mcp.tool()
def archive_status_json() -> str:
    """Index + runtime status as JSON."""

    profile_error = _tool_profile_error("archive_status_json")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        result = status_cmd.status_json(store=store, logger=_log)
        return json.dumps(result, indent=2)
    except PpaError as exc:
        return str(exc)


@mcp.tool()
def archive_retrieval_explain_json(
    query: str,
    mode: str = "hybrid",
    limit: int = 10,
    embedding_model: str = "",
    embedding_version: int = 0,
) -> str:
    """Retrieval explain payload (v2 schema) as JSON."""

    profile_error = _tool_profile_error("archive_retrieval_explain_json")
    if profile_error:
        return profile_error
    try:
        store = resolve_store()
        payload = explain.retrieval_explain(
            query,
            store=store,
            logger=_log,
            mode=mode,
            limit=limit,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
        )
        return json.dumps(payload, indent=2)
    except PpaError as exc:
        return str(exc)


if __name__ == "__main__":
    mcp.run()

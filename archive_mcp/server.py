"""Archive MCP server backed by the shared HFA library."""

from __future__ import annotations

import json
import os
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


from hfa.provenance import validate_provenance
from hfa.schema import BaseCard, validate_card_permissive, validate_card_strict
from hfa.vault import (find_note_by_slug, iter_notes, iter_parsed_notes,
                       read_note)

from .embedding_provider import get_embedding_provider
from .index_config import get_seed_links_enabled
from .index_store import (BaseArchiveIndex, PostgresArchiveIndex,
                          get_archive_index, get_default_embedding_model,
                          get_default_embedding_version, get_index_dsn)
from .store import get_archive_store

_SEED_LINKS_DISABLED_MSG = "Seed links are not enabled. Set PPA_SEED_LINKS_ENABLED=1 to enable."


def _import_seed_links():
    from .seed_links import (compute_link_quality_gate,
                             get_link_candidate_details, get_seed_scope_rows,
                             get_surface_policy_rows, list_link_candidates,
                             review_link_candidate,
                             run_incremental_link_refresh,
                             run_seed_link_backfill, run_seed_link_enqueue,
                             run_seed_link_promotion_workers,
                             run_seed_link_report, run_seed_link_workers)
    return {
        "compute_link_quality_gate": compute_link_quality_gate,
        "get_link_candidate_details": get_link_candidate_details,
        "get_seed_scope_rows": get_seed_scope_rows,
        "get_surface_policy_rows": get_surface_policy_rows,
        "list_link_candidates": list_link_candidates,
        "review_link_candidate": review_link_candidate,
        "run_incremental_link_refresh": run_incremental_link_refresh,
        "run_seed_link_backfill": run_seed_link_backfill,
        "run_seed_link_enqueue": run_seed_link_enqueue,
        "run_seed_link_promotion_workers": run_seed_link_promotion_workers,
        "run_seed_link_report": run_seed_link_report,
        "run_seed_link_workers": run_seed_link_workers,
    }

_instance_name = os.environ.get("PPA_INSTANCE_NAME", "Personal Private Archives").strip()
mcp = FastMCP("ppa", _instance_name)

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


def _resolve_vault_note_path(vault: Path, rel_or_note_path: str) -> Path | None:
    try:
        candidate = (vault / rel_or_note_path).resolve()
        candidate.relative_to(vault.resolve())
    except Exception:
        return None
    if not candidate.exists() or candidate.suffix != ".md":
        return None
    return candidate


def _load_index(vault: Path) -> tuple[BaseArchiveIndex | None, str | None]:
    try:
        return get_index(vault), None
    except RuntimeError as exc:
        return None, str(exc)


def _card_summary(card: BaseCard, rel_path: Path) -> str:
    return f"- {rel_path}: {card.summary[:80]}"


def _format_search_line(row: dict) -> str:
    """Render a search/query result row with type, date, and fuller summary."""
    rel_path = row.get("rel_path", "")
    card_type = row.get("type", "")
    date = str(row.get("activity_at", ""))[:10]
    summary = str(row.get("summary", ""))[:200]
    meta = ", ".join(part for part in [card_type, date] if part)
    return f"- {rel_path} [{meta}]: {summary}"


def _all_cards(vault: Path) -> list[tuple[Path, BaseCard, str, dict]]:
    rows: list[tuple[Path, BaseCard, str, dict]] = []
    for note in iter_parsed_notes(vault):
        card = validate_card_permissive(note.frontmatter)
        rows.append((note.rel_path, card, note.body, note.provenance))
    return rows


@mcp.tool()
def archive_search(query: str, limit: int = 20) -> str:
    """Full-text search across all notes."""

    profile_error = _tool_profile_error("archive_search")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    rows = store.search(query, limit=limit)["rows"]
    results = [_format_search_line(row) for row in rows]
    return "\n".join(results) if results else "No matches"


@mcp.tool()
def archive_read(path_or_uid: str) -> str:
    """Read note by relative path or UID."""

    profile_error = _tool_profile_error("archive_read")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.read(path_or_uid)
    if not payload.get("found"):
        return "Not found"
    return str(payload.get("content", ""))


@mcp.tool()
def archive_query(
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    org_filter: str = "",
    limit: int = 20,
) -> str:
    """Structured query by frontmatter fields."""

    profile_error = _tool_profile_error("archive_query")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    rows = store.query(
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        limit=limit,
    )["rows"]
    results = [_format_search_line(row) for row in rows]
    return "\n".join(results) if results else "No matches"


@mcp.tool()
def archive_graph(note_path: str, hops: int = 2) -> str:
    """Get notes linked from the given note via wikilinks."""

    profile_error = _tool_profile_error("archive_graph")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.graph(note_path, hops=hops)
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


@mcp.tool()
def archive_person(name: str) -> str:
    """Get person profile by slug."""

    profile_error = _tool_profile_error("archive_person")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    rel_path = index.person_path(name)
    if rel_path is None:
        match = find_note_by_slug(vault, name.replace(" ", "-").lower())
        if match is None:
            return "Person not found"
        return match.read_text(encoding="utf-8")
    path = vault / rel_path
    if not path.exists():
        return "Person not found"
    return path.read_text(encoding="utf-8")


@mcp.tool()
def archive_timeline(start_date: str = "", end_date: str = "", limit: int = 20) -> str:
    """Notes in date range."""

    profile_error = _tool_profile_error("archive_timeline")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    rows = store.timeline(start_date=start_date, end_date=end_date, limit=limit)["rows"]
    results = [f"- {str(row['created'])[:10]} {row['rel_path']}: {str(row['summary'])[:160]}" for row in rows]
    return "\n".join(results) if results else "No matches"


@mcp.tool()
def archive_stats() -> str:
    """Vault health metrics."""

    profile_error = _tool_profile_error("archive_stats")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    total, by_type, by_source = index.stats()
    lines = [f"Total: {total} notes", "", "By type:"]
    for row in by_type:
        lines.append(f"  {row['type']}: {row['count']}")
    lines.extend(["", "By source:"])
    for row in by_source:
        lines.append(f"  {row['source']}: {row['count']}")
    return "\n".join(lines)


@mcp.tool()
def archive_validate() -> str:
    """Run schema validation on all cards, return summary."""

    profile_error = _tool_profile_error("archive_validate")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    total = 0
    valid = 0
    errors: list[str] = []
    for rel_path, _ in iter_notes(vault):
        total += 1
        try:
            frontmatter, _, provenance = read_note(vault, str(rel_path))
            card = validate_card_strict(frontmatter)
            provenance_errors = validate_provenance(card.model_dump(mode="python"), provenance)
            if provenance_errors:
                raise ValueError("; ".join(provenance_errors))
            valid += 1
        except Exception as exc:
            errors.append(f"- {rel_path}: {exc}")
    lines = [f"Validated {valid}/{total} notes"]
    if errors:
        lines.append("Errors:")
        lines.extend(errors[:20])
        if len(errors) > 20:
            lines.append(f"... and {len(errors) - 20} more")
    else:
        lines.append("0 errors")
    return "\n".join(lines)


@mcp.tool()
def archive_duplicates() -> str:
    """Return pending dedup candidates from _meta/dedup-candidates.json."""

    profile_error = _tool_profile_error("archive_duplicates")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    path = vault / "_meta" / "dedup-candidates.json"
    if not path.exists():
        return "No pending duplicate candidates"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return "Could not parse dedup candidates"
    if not isinstance(payload, list) or not payload:
        return "No pending duplicate candidates"
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


@mcp.tool()
def archive_duplicate_uids(limit: int = 20) -> str:
    """Return duplicate UID collisions from the derived index."""

    profile_error = _tool_profile_error("archive_duplicate_uids")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    rows = index.duplicate_uid_rows(limit=limit)
    if not rows:
        return "No duplicate UID rows"
    lines = ["Archive duplicate UID rows:"]
    for row in rows:
        lines.append(
            f"- uid={row['uid']} group_size={row['duplicate_group_size']} preferred={row['preferred_rel_path']} "
            f"duplicate={row['duplicate_rel_path']} preferred_type={row['preferred_type']} duplicate_type={row['duplicate_type']}"
        )
    return "\n".join(lines)


@mcp.tool()
def archive_rebuild_indexes() -> str:
    """Rebuild the derived archive index from canonical markdown cards."""

    profile_error = _tool_profile_error("archive_rebuild_indexes")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    counts = index.rebuild()
    return (
        f"Rebuilt archive index at {index.location}\n"
        f"- cards: {counts['cards']}\n"
        f"- external_ids: {counts['external_ids']}\n"
        f"- edges: {counts['edges']}\n"
        f"- chunks: {counts['chunks']}\n"
        f"- duplicate_uids: {counts['duplicate_uids']}"
    )


@mcp.tool()
def archive_bootstrap_postgres() -> str:
    """Bootstrap the Postgres archive index schema and pgvector-ready tables."""

    profile_error = _tool_profile_error("archive_bootstrap_postgres")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    dsn = get_index_dsn()
    if not dsn:
        return "PPA_INDEX_DSN is required"
    result = PostgresArchiveIndex(vault, dsn=dsn).bootstrap()
    lines = ["Bootstrapped Postgres archive index:"]
    for key in sorted(result):
        lines.append(f"- {key}: {result[key]}")
    return "\n".join(lines)


@mcp.tool()
def archive_index_status() -> str:
    """Report the current derived index status."""

    profile_error = _tool_profile_error("archive_index_status")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    status = store.status()
    if not status:
        return "No index metadata found"
    lines = ["Archive index status:"]
    for key in sorted(status):
        lines.append(f"- {key}: {status[key]}")
    return "\n".join(lines)


@mcp.tool()
def archive_projection_inventory() -> str:
    """List registered archive projections."""

    profile_error = _tool_profile_error("archive_projection_inventory")
    if profile_error:
        return profile_error
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.projection_inventory()
    lines = ["Archive projection inventory:"]
    for projection in payload.get("projections", []):
        lines.append(
            f"- {projection['name']} table={projection['table_name']} kind={projection['kind']} "
            f"types={','.join(projection['applies_to_types'])}"
        )
    return "\n".join(lines)


@mcp.tool()
def archive_projection_status() -> str:
    """Report typed projection coverage and readiness."""

    profile_error = _tool_profile_error("archive_projection_status")
    if profile_error:
        return profile_error
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.projection_status()
    lines = ["Archive projection status:"]
    for row in payload.get("projection_coverage", []):
        blockers = ",".join(row.get("migration_blockers", []))
        lines.append(
            f"- {row['card_type']} projection={row['typed_projection']} rows={row['materialized_row_count']} "
            f"ready_ratio={float(row['canonical_ready_ratio']):.2f} blockers={blockers}"
        )
    return "\n".join(lines)


@mcp.tool()
def archive_projection_explain(card_uid: str) -> str:
    """Explain how a typed projection row is populated."""

    profile_error = _tool_profile_error("archive_projection_explain")
    if profile_error:
        return profile_error
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.projection_explain(card_uid)
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


@mcp.tool()
def archive_embedding_status(embedding_model: str = "", embedding_version: int = 0) -> str:
    """Report embedding backlog status for a given model/version."""

    profile_error = _tool_profile_error("archive_embedding_status")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    status = store.embedding_status(
        embedding_model=embedding_model.strip(),
        embedding_version=embedding_version,
    )
    lines = ["Archive embedding status:"]
    for key in ("embedding_model", "embedding_version", "chunk_schema_version", "chunk_count", "embedded_chunk_count", "pending_chunk_count"):
        lines.append(f"- {key}: {status[key]}")
    return "\n".join(lines)


@mcp.tool()
def archive_embedding_backlog(limit: int = 20, embedding_model: str = "", embedding_version: int = 0) -> str:
    """List chunks that are still pending embeddings for a given model/version."""

    profile_error = _tool_profile_error("archive_embedding_backlog")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.embedding_backlog(
        limit=limit,
        embedding_model=embedding_model.strip(),
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


@mcp.tool()
def archive_embed_pending(limit: int = 20, embedding_model: str = "", embedding_version: int = 0) -> str:
    """Generate embeddings for pending chunks using the configured embedding provider."""

    profile_error = _tool_profile_error("archive_embed_pending")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    result = store.embed_pending(
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


@mcp.tool()
def archive_seed_link_surface() -> str:
    """Describe the seed link review scope and promotion surface."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_surface")
    if profile_error:
        return profile_error
    sl = _import_seed_links()
    scope_rows = sl["get_seed_scope_rows"]()
    policy_rows = sl["get_surface_policy_rows"]()
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
def archive_seed_link_enqueue(modules: str = "", source_uids: str = "", job_type: str = "seed_backfill", reset_existing: bool = False) -> str:
    """Enqueue seed link jobs without processing them."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_enqueue")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    selected_uids = {item.strip() for item in source_uids.split(",") if item.strip()}
    result = sl["run_seed_link_enqueue"](
        index,
        modules=selected_modules or None,
        source_uids=selected_uids or None,
        job_type=job_type.strip() or "seed_backfill",
        reset_existing=bool(reset_existing),
    )
    return (
        "Archive seed link enqueue:\n"
        f"- job_type: {job_type}\n"
        f"- prepared: {result['prepared']}\n"
        f"- enqueued: {result['enqueued']}\n"
        f"- existing: {result['existing']}"
    )


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
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    result = sl["run_seed_link_backfill"](
        index,
        modules=selected_modules or None,
        limit=limit,
        max_workers=workers,
        include_llm=bool(include_llm),
        apply_promotions=bool(apply_promotions),
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
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    selected_uids = [item.strip() for item in source_uids.split(",") if item.strip()]
    if not selected_uids:
        return "source_uids is required"
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    result = sl["run_incremental_link_refresh"](
        index,
        source_uids=selected_uids,
        modules=selected_modules or None,
        max_workers=workers,
        include_llm=bool(include_llm),
        apply_promotions=bool(apply_promotions),
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


@mcp.tool()
def archive_seed_link_worker(limit: int = 0, modules: str = "", workers: int = 0, include_llm: bool = True) -> str:
    """Process pending seed link jobs from the shared queue."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_worker")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    selected_modules = [item.strip() for item in modules.split(",") if item.strip()]
    result = sl["run_seed_link_workers"](
        index,
        modules=selected_modules or None,
        limit=limit,
        max_workers=workers,
        include_llm=bool(include_llm),
    )
    lines = ["Archive seed link worker:"]
    for key in ("workers", "jobs_completed", "jobs_failed", "candidates", "needs_review", "auto_promoted", "canonical_safe", "llm_judged"):
        lines.append(f"- {key}: {result[key]}")
    return "\n".join(lines)


@mcp.tool()
def archive_seed_link_promote(limit: int = 0, workers: int = 1) -> str:
    """Process queued seed link promotions from the shared queue."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_promote")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    result = sl["run_seed_link_promotion_workers"](index, limit=limit, max_workers=max(1, workers))
    return (
        "Archive seed link promote:\n"
        f"- derived_edge: {result['derived_edge']}\n"
        f"- canonical_field: {result['canonical_field']}\n"
        f"- blocked: {result['blocked']}"
    )


@mcp.tool()
def archive_seed_link_report(rebuild_if_dirty: bool = True) -> str:
    """Finalize and report the current shared seed link queue state."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_seed_link_report")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    payload = sl["run_seed_link_report"](index, rebuild_if_dirty=bool(rebuild_if_dirty))
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


@mcp.tool()
def archive_link_candidates(status: str = "", module_name: str = "", min_confidence: float = 0.0, limit: int = 20) -> str:
    """List seed link candidates and review queue items."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_link_candidates")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    rows = sl["list_link_candidates"](
        index,
        status=status.strip(),
        module_name=module_name.strip(),
        min_confidence=min_confidence,
        limit=limit,
    )
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


@mcp.tool()
def archive_link_candidate(candidate_id: int) -> str:
    """Show a single seed link candidate with evidence."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_link_candidate")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    payload = sl["get_link_candidate_details"](index, candidate_id)
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


@mcp.tool()
def archive_review_link_candidate(candidate_id: int, reviewer: str, action: str, notes: str = "") -> str:
    """Approve, reject, or override a seed link candidate."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_review_link_candidate")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    payload = sl["review_link_candidate"](index, candidate_id=candidate_id, reviewer=reviewer, action=action, notes=notes)
    return (
        f"Reviewed candidate {candidate_id}\n"
        f"- action: {action}\n"
        f"- status: {payload.get('status', '')}\n"
        f"- decision: {payload.get('decision', '')}\n"
        f"- confidence: {float(payload.get('final_confidence', 0.0)):.4f}"
    )


@mcp.tool()
def archive_link_quality_gate() -> str:
    """Report whether the seed link review has reached the quality gate."""

    if not get_seed_links_enabled():
        return _SEED_LINKS_DISABLED_MSG
    profile_error = _tool_profile_error("archive_link_quality_gate")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    sl = _import_seed_links()
    gate = sl["compute_link_quality_gate"](index)
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

    profile_error = _tool_profile_error("archive_vector_search")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    provider = get_embedding_provider(model=model)
    query_vector = provider.embed_texts([query.strip() or ""])[0]
    index, error = _load_index(vault)
    if error:
        return error
    assert index is not None
    rows = index.vector_search(
        query_vector=query_vector,
        embedding_model=model,
        embedding_version=version,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if not rows:
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
    return "\n".join(lines)


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
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.retrieval_explain(
        query,
        mode=mode,
        limit=limit,
        embedding_model=embedding_model,
        embedding_version=embedding_version,
    )
    return json.dumps(payload, indent=2)


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

    profile_error = _tool_profile_error("archive_hybrid_search")
    if profile_error:
        return profile_error
    vault = get_vault()
    if not vault.is_dir():
        return "Vault not found"
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    payload = store.hybrid_search(
        query,
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
    return "\n".join(lines)


@mcp.tool()
def archive_search_json(query: str, limit: int = 20) -> str:
    """Lexical search results as JSON (paths + summaries, no embedding)."""

    profile_error = _tool_profile_error("archive_search_json")
    if profile_error:
        return profile_error
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    return json.dumps(store.search(query, limit=limit), indent=2)


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
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    model = embedding_model.strip() or get_default_embedding_model()
    version = embedding_version or get_default_embedding_version()
    payload = store.hybrid_search(
        query,
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


@mcp.tool()
def archive_read_many(paths_json: str) -> str:
    """Read multiple notes by rel path or card uid. `paths_json` is a JSON array of strings."""

    profile_error = _tool_profile_error("archive_read_many")
    if profile_error:
        return profile_error
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    try:
        paths = json.loads(paths_json)
    except json.JSONDecodeError as exc:
        return f"Invalid JSON: {exc}"
    if not isinstance(paths, list):
        return "paths_json must be a JSON array"
    return json.dumps(store.read_many([str(p) for p in paths]), indent=2)


@mcp.tool()
def archive_status_json() -> str:
    """Index + runtime status as JSON."""

    profile_error = _tool_profile_error("archive_status_json")
    if profile_error:
        return profile_error
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    return json.dumps(store.status(), indent=2)


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
    vault = get_vault()
    store, error = _load_store(vault)
    if error:
        return error
    assert store is not None
    payload = store.retrieval_explain(
        query,
        mode=mode,
        limit=limit,
        embedding_model=embedding_model,
        embedding_version=embedding_version,
    )
    return json.dumps(payload, indent=2)


if __name__ == "__main__":
    mcp.run()

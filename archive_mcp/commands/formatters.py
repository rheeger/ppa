"""MCP tool output formatters.

These functions convert command result dicts into the human-readable strings
that MCP tools return. They exist separately from the commands (which return
dicts) because MCP tool return types are frozen as strings in the protocol,
while the CLI and tests consume the dicts directly as JSON.

Every function takes structured inputs (often a command result dict) and returns a str.
Naming convention: format_<tool_name_without_archive_prefix>().
"""

from __future__ import annotations

from typing import Any

from archive_mcp.index_config import _activity_date


def format_search_line(row: dict) -> str:
    """Render a search/query result row with type, date, and fuller summary."""
    rel_path = row.get("rel_path", "")
    card_type = row.get("type", "")
    date = _activity_date(row.get("activity_at"))
    summary = str(row.get("summary", ""))[:200]
    meta = ", ".join(part for part in [card_type, date] if part)
    return f"- {rel_path} [{meta}]: {summary}"


def format_search(result: dict) -> str:
    """Format archive_search / archive_query result as text lines."""
    rows = result.get("rows", [])
    if not rows:
        return "No matches"
    return "\n".join(format_search_line(r) for r in rows)


def format_graph(rel_path: str, graph: dict[str, Any]) -> str:
    """Format archive_graph result when the note was found."""
    lines = [f"Graph from {rel_path}:"]
    for source, targets in graph.items():
        lines.append(f"- {source}")
        for target in targets:
            lines.append(f"  -> {target}")
    return "\n".join(lines) if len(lines) > 1 else "No linked notes"


def format_timeline(result: dict) -> str:
    """Format archive_timeline result."""
    rows = result["rows"]
    results = [f"- {str(row['created'])[:10]} {row['rel_path']}: {str(row['summary'])[:160]}" for row in rows]
    return "\n".join(results) if results else "No matches"


def format_stats(result: dict) -> str:
    """Format archive_stats result as text."""
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


def format_validate(result: dict) -> str:
    """Format archive_validate result."""
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


def format_duplicates(result: dict) -> str:
    """Format archive_duplicates result."""
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


def format_duplicate_uids(result: dict) -> str:
    """Format archive_duplicate_uids result."""
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


def format_rebuild_indexes(index_location: str, counts: dict[str, Any]) -> str:
    """Format archive_rebuild_indexes output."""
    return (
        f"Rebuilt archive index at {index_location}\n"
        f"- cards: {counts['cards']}\n"
        f"- external_ids: {counts['external_ids']}\n"
        f"- edges: {counts['edges']}\n"
        f"- chunks: {counts['chunks']}\n"
        f"- duplicate_uids: {counts['duplicate_uids']}"
    )


def format_bootstrap_postgres(result: dict[str, Any]) -> str:
    """Format archive_bootstrap_postgres result."""
    lines = ["Bootstrapped Postgres archive index:"]
    for key in sorted(result):
        lines.append(f"- {key}: {result[key]}")
    return "\n".join(lines)


def format_index_status(status: dict[str, Any] | None) -> str:
    """Format archive_index_status result."""
    if not status:
        return "No index metadata found"
    lines = ["Archive index status:"]
    for key in sorted(status):
        lines.append(f"- {key}: {status[key]}")
    return "\n".join(lines)


def format_projection_inventory(payload: dict) -> str:
    """Format archive_projection_inventory result."""
    projections = payload.get("projections", [])
    lines = ["Archive projection inventory:"]
    for projection in projections:
        lines.append(
            f"- {projection['name']} table={projection['table_name']} kind={projection['kind']} "
            f"types={','.join(projection['applies_to_types'])}"
        )
    return "\n".join(lines)


def format_projection_status(payload: dict) -> str:
    """Format archive_projection_status result."""
    cov = payload.get("projection_coverage", [])
    lines = ["Archive projection status:"]
    for row in cov:
        blockers = ",".join(row.get("migration_blockers", []))
        lines.append(
            f"- {row['card_type']} projection={row['typed_projection']} rows={row['materialized_row_count']} "
            f"ready_ratio={float(row['canonical_ready_ratio']):.2f} blockers={blockers}"
        )
    return "\n".join(lines)


def format_projection_explain(card_uid: str, payload: dict) -> str:
    """Format archive_projection_explain result."""
    mappings = payload.get("field_mappings", [])[:20]
    lines = [
        f"Archive projection explain for {card_uid}:",
        f"- card_type: {payload.get('card_type', '')}",
        f"- typed_projection: {payload.get('typed_projection', '')}",
        f"- canonical_ready: {payload.get('canonical_ready', False)}",
    ]
    for mapping in mappings:
        fields = ",".join(mapping.get("canonical_fields", []))
        lines.append(f"- {mapping['typed_column']} <- {fields} ({mapping['status']})")
    if payload.get("migration_notes"):
        lines.append(f"- migration_notes: {'; '.join(payload['migration_notes'])}")
    return "\n".join(lines)


def format_embedding_status(status: dict) -> str:
    """Format archive_embedding_status result."""
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


def format_embedding_backlog(payload: dict) -> str:
    """Format archive_embedding_backlog result."""
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


def format_embed_pending(result: dict[str, Any]) -> str:
    """Format archive_embed_pending result."""
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


def format_seed_link_surface(payload: dict) -> str:
    """Format archive_seed_link_surface result as text."""
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


def format_seed_link_enqueue(job_type: str, result: dict) -> str:
    """Format archive_seed_link_enqueue result."""
    return (
        "Archive seed link enqueue:\n"
        f"- job_type: {job_type}\n"
        f"- prepared: {result['prepared']}\n"
        f"- enqueued: {result['enqueued']}\n"
        f"- existing: {result['existing']}"
    )


def format_seed_link_backfill(result: dict) -> str:
    """Format archive_seed_link_backfill result."""
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


def format_seed_link_refresh(result: dict) -> str:
    """Format archive_seed_link_refresh result."""
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


def format_seed_link_worker(result: dict) -> str:
    """Format archive_seed_link_worker result."""
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


def format_seed_link_promote(result: dict) -> str:
    """Format archive_seed_link_promote result."""
    return (
        "Archive seed link promote:\n"
        f"- derived_edge: {result['derived_edge']}\n"
        f"- canonical_field: {result['canonical_field']}\n"
        f"- blocked: {result['blocked']}"
    )


def format_seed_link_report(payload: dict) -> str:
    """Format archive_seed_link_report result."""
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


def format_link_candidates(result: dict) -> str:
    """Format archive_link_candidates result."""
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


def format_link_candidate(candidate_id: int, payload: dict) -> str:
    """Format archive_link_candidate result when candidate exists."""
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


def format_review_link_candidate(candidate_id: int, action: str, payload: dict) -> str:
    """Format archive_review_link_candidate result."""
    return (
        f"Reviewed candidate {candidate_id}\n"
        f"- action: {action}\n"
        f"- status: {payload.get('status', '')}\n"
        f"- decision: {payload.get('decision', '')}\n"
        f"- confidence: {float(payload.get('final_confidence', 0.0)):.4f}"
    )


def format_link_quality_gate(gate: dict) -> str:
    """Format archive_link_quality_gate result."""
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


def format_vector_search(model: str, version: int, rows: list[dict]) -> str:
    """Format archive_vector_search result."""
    if not rows:
        return f"No vector matches for {model} v{version}"
    lines = [f"Vector matches for {model} v{version}:"]
    for row in rows:
        card_type = str(row.get("type", ""))
        date = _activity_date(row.get("activity_at"))
        summary = str(row.get("summary", ""))[:200]
        lines.append(
            f"- {row['rel_path']} [{card_type}, {date}] matched_by={row['matched_by']} score={float(row['score']):.4f} "
            f"sim={float(row['similarity']):.4f} chunk={row['chunk_type']}#{row['chunk_index']} "
            f"provenance_bias={row['provenance_bias']} matched_chunks={row['matched_chunk_count']}\n"
            f"  summary: {summary}\n"
            f"  preview: {row['preview']}"
        )
    return "\n".join(lines)


def format_hybrid_search(query: str, rows: list[dict]) -> str:
    """Format archive_hybrid_search result."""
    if not rows:
        return f"No hybrid matches for '{query}'"
    lines = [f"Hybrid matches for '{query}':"]
    for row in rows:
        card_type = str(row.get("type", ""))
        date = _activity_date(row.get("activity_at"))
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

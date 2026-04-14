"""Helpers for projection and retrieval explainability payloads."""

from __future__ import annotations

from typing import Any

from .contracts import ArchiveContext, ProjectionSpec


def context_payload(context: ArchiveContext) -> dict[str, Any]:
    return {
        "card_type": context.card_type,
        "source_labels": list(context.source_labels),
        "people": list(context.people),
        "orgs": list(context.orgs),
        "time_span": list(context.time_span),
        "provenance_bias": context.provenance_bias,
        "graph_neighbor_types": list(context.graph_neighbor_types),
        "typed_projection_names": list(context.typed_projection_names),
    }


def projection_inventory_payload(projections: list[ProjectionSpec]) -> dict[str, Any]:
    return {
        "projections": [
            {
                "name": projection.name,
                "table_name": projection.table_name,
                "kind": projection.kind,
                "applies_to_types": list(projection.applies_to_types),
                "columns": [column.name for column in projection.columns],
                "builder_name": projection.builder_name,
            }
            for projection in projections
        ]
    }


def projection_status_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"projection_coverage": rows}


def projection_explain_payload(
    *,
    card_uid: str,
    card_type: str,
    typed_projection: str,
    canonical_ready: bool,
    field_mappings: list[dict[str, Any]],
    migration_notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "card_uid": card_uid,
        "card_type": card_type,
        "typed_projection": typed_projection,
        "canonical_ready": canonical_ready,
        "field_mappings": field_mappings,
        "migration_notes": migration_notes or [],
    }


def retrieval_explain_payload(query: str, mode: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "query": query,
        "mode": mode,
        "results": rows,
    }


def retrieval_explain_payload_v2(
    *,
    pipeline_version: str,
    query: str,
    mode: str,
    query_plan: dict[str, Any],
    candidate_generation: dict[str, Any],
    fusion_strategy: str,
    results: list[dict[str, Any]],
    reranker: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Structured explain surface; see docs/RETRIEVAL_EXPLAIN_SCHEMA.md."""
    payload: dict[str, Any] = {
        "schema": "archive_retrieval_explain_v2",
        "pipeline_version": pipeline_version,
        "query": query,
        "mode": mode,
        "query_plan": query_plan,
        "candidate_generation": candidate_generation,
        "fusion_strategy": fusion_strategy,
        "results": results,
    }
    if reranker is not None:
        payload["reranker"] = reranker
    return payload

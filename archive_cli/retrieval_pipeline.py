"""Staged hybrid retrieval: fuse lexical + vector candidates and rank (no SQL)."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

# Shared with index_store ranking; keep in sync with CARD_TYPE_PRIORS there.
from archive_cli.corpus_hygiene.state_store import (
    annotate_retrieval_corpus_fields,
    retrieval_weight_for_corpus_state,
)

from .index_config import _format_activity_at
from .index_store import CARD_TYPE_PRIORS
from .rank_fusion import (
    FUSION_STRATEGY,
    FusionOptions,
    apply_rrf_scores,
    apply_thread_diversity,
    query_has_historical_dates,
    query_requests_current_ops,
    sort_fused_rows,
)

PIPELINE_VERSION = "2026.09.06.p01b2"


@dataclass(frozen=True)
class PlannedQuery:
    """One retrieval subquery string and metadata."""

    text: str
    role: str = "primary"
    weight: float = 1.0


@dataclass(frozen=True)
class FilterInference:
    """Planner-inferred filters (explicit caller filters still win at merge time)."""

    type_hints: tuple[str, ...] = ()
    source_hints: tuple[str, ...] = ()
    start_date_hint: str = ""
    end_date_hint: str = ""
    phrases: tuple[str, ...] = ()
    emails: tuple[str, ...] = ()
    external_ids: tuple[str, ...] = ()
    historical: bool = False
    current_ops: bool = False
    has_exact_identifier: bool = False


@dataclass(frozen=True)
class QueryPlan:
    """Full plan: ordered subqueries plus inferred filters."""

    queries: tuple[PlannedQuery, ...]
    inferred: FilterInference
    planner_provider: str = "deterministic"
    model_notes: tuple[str, ...] = ()


@dataclass
class HybridFetchInputs:
    """Raw candidate lists before fusion (Postgres index produces these)."""

    lexical_rows: list[dict[str, Any]]
    vector_rows: list[dict[str, Any]]
    neighbor_trust: dict[str, float]
    query_cleaned: str = ""
    subqueries_used: tuple[str, ...] = ()


def _card_type_prior(card_type: str) -> float:
    return CARD_TYPE_PRIORS.get(card_type, 0.02)


def _apply_recency_boost(rows: list[dict[str, Any]], *, key_name: str) -> None:
    dated = [row for row in rows if _format_activity_at(row.get("activity_at")).strip()]
    if not dated:
        return
    ordered = sorted(
        dated,
        key=lambda row: (_format_activity_at(row.get("activity_at")), str(row.get("rel_path", ""))),
        reverse=True,
    )
    total = max(len(ordered) - 1, 1)
    for index, row in enumerate(ordered):
        row[key_name] = round((1.0 - (index / total)) * 0.06, 6)


def merge_lexical_rows(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep best lexical_score per card_uid."""
    by_uid: dict[str, dict[str, Any]] = {str(r["card_uid"]): dict(r) for r in existing}
    for row in incoming:
        uid = str(row["card_uid"])
        prev = by_uid.get(uid)
        if prev is None or float(row.get("lexical_score", 0.0)) > float(prev.get("lexical_score", 0.0)):
            by_uid[uid] = dict(row)
    return list(by_uid.values())


def merge_vector_rows(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep best vector similarity per card_uid (expects card-level aggregated vector rows)."""
    by_uid: dict[str, dict[str, Any]] = {str(r["card_uid"]): dict(r) for r in existing}
    for row in incoming:
        uid = str(row["card_uid"])
        sim = float(row.get("similarity", 0.0))
        prev = by_uid.get(uid)
        if prev is None or sim > float(prev.get("similarity", 0.0)):
            by_uid[uid] = dict(row)
    return list(by_uid.values())


def anchor_uids_from_lexical(lexical_rows: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for row in lexical_rows:
        if (
            int(row.get("slug_exact", 0))
            or int(row.get("summary_exact", 0))
            or int(row.get("external_id_exact", 0))
            or int(row.get("person_exact", 0))
        ):
            out.append(str(row["card_uid"]))
    return out


def fuse_lexical_vector_rows(
    lexical_rows: list[dict[str, Any]],
    vector_rows: list[dict[str, Any]],
    neighbor_trust: dict[str, float],
) -> list[dict[str, Any]]:
    """Merge lexical and vector hits into one row per card (same shape as legacy hybrid_search)."""
    anchor_uids = set(anchor_uids_from_lexical(lexical_rows))
    merged: dict[str, dict[str, Any]] = {}
    for row in lexical_rows:
        card_uid = str(row["card_uid"])
        exact_match = bool(
            int(row["slug_exact"])
            or int(row["summary_exact"])
            or int(row["external_id_exact"])
            or int(row["person_exact"])
        )
        merged[card_uid] = {
            "card_uid": card_uid,
            "rel_path": str(row["rel_path"]),
            "summary": str(row["summary"]),
            "type": str(row["type"]),
            "activity_at": _format_activity_at(row.get("activity_at")),
            "preview": str(row["summary"])[:160],
            "matched_by": "lexical",
            "lexical_score": float(row["lexical_score"]),
            "vector_similarity": 0.0,
            "exact_match": exact_match,
            "chunk_type": "",
            "chunk_index": -1,
            "matched_chunk_count": 0,
            "provenance_bias": "deterministic" if exact_match else "mixed",
            "provenance_score": 0.08 if exact_match else 0.04,
            "graph_hops": "0" if card_uid in anchor_uids else "",
            "corpus_state": row.get("corpus_state"),
            "parent_thread": str(row.get("parent_thread") or row.get("thread") or ""),
            "thread": str(row.get("thread") or ""),
            "score": 0.0,
        }
    for row in vector_rows:
        card_uid = str(row["card_uid"])
        entry = merged.setdefault(
            card_uid,
            {
                "card_uid": card_uid,
                "rel_path": str(row["rel_path"]),
                "summary": str(row["summary"]),
                "type": str(row["type"]),
                "activity_at": _format_activity_at(row.get("activity_at")),
                "preview": str(row["preview"]),
                "matched_by": "vector",
                "lexical_score": 0.0,
                "vector_similarity": float(row["similarity"]),
                "exact_match": False,
                "chunk_type": str(row["chunk_type"]),
                "chunk_index": int(row["chunk_index"]),
                "matched_chunk_count": int(row["matched_chunk_count"]),
                "provenance_bias": str(row["provenance_bias"]),
                "provenance_score": float(row["provenance_score"]),
                "graph_hops": "",
                "corpus_state": row.get("corpus_state"),
                "parent_thread": str(row.get("parent_thread") or row.get("thread") or ""),
                "thread": str(row.get("thread") or ""),
                "score": 0.0,
            },
        )
        if entry["matched_by"] == "lexical":
            entry["matched_by"] = "hybrid"
        entry["vector_similarity"] = max(float(entry["vector_similarity"]), float(row["similarity"]))
        if float(row["similarity"]) >= float(entry["vector_similarity"]):
            entry["preview"] = str(row["preview"])
            entry["chunk_type"] = str(row["chunk_type"])
            entry["chunk_index"] = int(row["chunk_index"])
            entry["matched_chunk_count"] = int(row["matched_chunk_count"])
            entry["provenance_bias"] = str(row["provenance_bias"])
            entry["provenance_score"] = float(row["provenance_score"])
        if not entry.get("corpus_state") and row.get("corpus_state"):
            entry["corpus_state"] = row.get("corpus_state")
    ranked = list(merged.values())
    _apply_recency_boost(ranked, key_name="recency_score")
    for entry in ranked:
        annotate_retrieval_corpus_fields(entry)
        card_uid = str(entry["card_uid"])
        trust = float(neighbor_trust.get(card_uid, 0.0))
        if not entry["graph_hops"] and trust > 0.0:
            entry["graph_hops"] = "1"
            entry["graph_neighbor_trust"] = trust
        if trust > 0.0:
            entry["graph_neighbor_trust"] = trust
        entry["type_prior"] = _card_type_prior(str(entry["type"]))
        entry["score"] = 0.0
    return ranked


def rank_fused_hybrid_rows(
    fused: list[dict[str, Any]],
    *,
    final_limit: int,
    options: FusionOptions | None = None,
) -> list[dict[str, Any]]:
    opts = options or FusionOptions.from_env()
    apply_rrf_scores(fused, opts)
    sort_fused_rows(fused)
    diversified = apply_thread_diversity(fused, opts)
    return diversified[:final_limit]


def fuse_and_rank_hybrid(
    inputs: HybridFetchInputs,
    *,
    final_limit: int,
    pipeline_meta: dict[str, Any] | None = None,
    fusion: FusionOptions | None = None,
) -> list[dict[str, Any]]:
    """Fuse lexical/semantic ranks with RRF, then apply diversity and freshness."""
    query = inputs.query_cleaned
    opts = fusion or FusionOptions.from_env(
        historical_query=query_has_historical_dates(query),
        exact_ids_present=False,
        ranking_profile=(
            "current_ops" if query_requests_current_ops(query) and not query_has_historical_dates(query) else None
        ),
    )
    if query_has_historical_dates(query):
        opts = replace(opts, ranking_profile="default", historical_query=True)
    meta = pipeline_meta if pipeline_meta is not None else {}
    meta["pipeline_version"] = PIPELINE_VERSION
    meta["fusion_strategy"] = FUSION_STRATEGY
    meta["ranking_version"] = "p01b2-rrf-1"
    meta["subqueries_used"] = list(inputs.subqueries_used) or ([query] if query else [])
    meta["lexical_candidate_count"] = len(inputs.lexical_rows)
    meta["vector_candidate_count"] = len(inputs.vector_rows)
    meta["graph_neighbor_count"] = len(inputs.neighbor_trust)
    meta["rare_token_weight"] = opts.rare_token_weight
    meta["ranking_profile"] = opts.ranking_profile
    fused = fuse_lexical_vector_rows(inputs.lexical_rows, inputs.vector_rows, inputs.neighbor_trust)
    if any(bool(row.get("exact_match")) for row in fused):
        opts = replace(opts, exact_ids_present=True)
    return rank_fused_hybrid_rows(fused, final_limit=final_limit, options=opts)


def score_breakdown_for_row(row: dict[str, Any]) -> dict[str, float]:
    """Decompose final score into named components (for explain)."""
    trust = float(row.get("graph_neighbor_trust", 0.0))
    if "rrf_score" in row or "lexical_rank" in row:
        return {
            "exact_boost": float(row.get("rrf_exact", 0.0) or 0.0),
            "lexical_component": float(row.get("rrf_lexical", 0.0) or 0.0),
            "vector_component": float(row.get("rrf_vector", 0.0) or 0.0),
            "multi_signal_boost": 0.2 if str(row.get("matched_by", "")) == "hybrid" else 0.0,
            "graph_boost": float(row.get("rrf_graph", 0.0) or 0.0),
            "graph_neighbor_trust": trust,
            "type_prior": float(row.get("type_prior", _card_type_prior(str(row.get("type", ""))))),
            "recency": float(row.get("recency_score", 0.0) or 0.0),
            "provenance": float(row.get("provenance_score", 0.0) or 0.0),
            "rerank_contribution": float(row.get("rerank_contribution", 0.0) or 0.0),
            "corpus_weight": retrieval_weight_for_corpus_state(row.get("corpus_state")),
            "rrf_score": float(row.get("rrf_score", 0.0) or 0.0),
            "rare_token_boost": float(row.get("rare_token_boost", 0.0) or 0.0),
            "age_decay": float(row.get("age_decay", 1.0) or 1.0),
            "lexical_rarity": float(row.get("lexical_rarity", row.get("lexical_score", 0.0)) or 0.0),
        }
    exact_boost = 3.0 if bool(row.get("exact_match")) else 0.0
    lexical_component = min(float(row.get("lexical_score", 0.0)), 1.5) * (
        1.2 if not bool(row.get("exact_match")) else 1.4
    )
    vector_component = float(row.get("vector_similarity", 0.0)) * 1.2
    multi_signal_boost = 0.2 if str(row.get("matched_by", "")) == "hybrid" else 0.0
    graph_boost = 0.22 * trust if str(row.get("graph_hops", "")) == "1" else 0.0
    return {
        "exact_boost": exact_boost,
        "lexical_component": round(lexical_component, 6),
        "vector_component": round(vector_component, 6),
        "multi_signal_boost": multi_signal_boost,
        "graph_boost": graph_boost,
        "graph_neighbor_trust": trust,
        "type_prior": _card_type_prior(str(row.get("type", ""))),
        "recency": float(row.get("recency_score", 0.0)),
        "provenance": float(row.get("provenance_score", 0.0)),
        "rerank_contribution": float(row.get("rerank_contribution", 0.0)),
        "corpus_weight": retrieval_weight_for_corpus_state(row.get("corpus_state")),
    }


@dataclass
class PipelineResult:
    """Structured pipeline output for tests and explain."""

    rows: list[dict[str, Any]]
    query_plan: QueryPlan | None = None
    fetch: HybridFetchInputs | None = None
    fusion_strategy: str = FUSION_STRATEGY
    pipeline_version: str = PIPELINE_VERSION
    extra: dict[str, Any] = field(default_factory=dict)

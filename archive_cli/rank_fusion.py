"""Rank-based reciprocal-rank fusion, thread diversity, and query-aware freshness."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .index_config import (
    DEFAULT_CURRENT_OPS_HALF_LIFE_DAYS,
    DEFAULT_DIVERSITY_CAP,
    DEFAULT_DIVERSITY_WINDOW,
    DEFAULT_RRF_K,
    MULTI_EVENT_CARD_TYPES,
    get_current_ops_half_life_days,
    get_diversity_cap,
    get_diversity_window,
    get_ranking_profile,
    get_rare_token_weight,
    get_rrf_channel_weight,
    get_rrf_k,
)

_YEAR = re.compile(r"\b(19|20)\d{2}\b")
_ISO_DATE = re.compile(r"\b(\d{4})-(\d{2})-(\d{2})\b")

FUSION_STRATEGY = "rrf_k60"
RANKING_VERSION = "p01b2-rrf-1"

_CURRENT_OPS_MARKERS = (
    "latest",
    "recent",
    "current",
    "today",
    "tonight",
    "now",
    "this week",
    "this month",
)


@dataclass(frozen=True)
class FusionOptions:
    rrf_k: int = DEFAULT_RRF_K
    exact_weight: float = 2.0
    lexical_weight: float = 1.0
    vector_weight: float = 1.0
    graph_weight: float = 0.25
    diversity_cap: int = DEFAULT_DIVERSITY_CAP
    diversity_window: int = DEFAULT_DIVERSITY_WINDOW
    rare_token_weight: float = 0.0
    ranking_profile: str = "default"
    half_life_days: float = DEFAULT_CURRENT_OPS_HALF_LIFE_DAYS
    now: datetime | None = None
    historical_query: bool = False
    exact_ids_present: bool = False
    multi_event: bool = False

    @classmethod
    def from_env(cls, **overrides: Any) -> FusionOptions:
        profile = str(overrides.get("ranking_profile") or get_ranking_profile())
        return cls(
            rrf_k=int(overrides.get("rrf_k") or get_rrf_k()),
            exact_weight=float(overrides.get("exact_weight") or get_rrf_channel_weight("exact")),
            lexical_weight=float(overrides.get("lexical_weight") or get_rrf_channel_weight("lexical")),
            vector_weight=float(overrides.get("vector_weight") or get_rrf_channel_weight("vector")),
            graph_weight=float(overrides.get("graph_weight") or get_rrf_channel_weight("graph")),
            diversity_cap=int(overrides.get("diversity_cap") or get_diversity_cap()),
            diversity_window=int(overrides.get("diversity_window") or get_diversity_window()),
            rare_token_weight=float(overrides.get("rare_token_weight", get_rare_token_weight())),
            ranking_profile=profile,
            half_life_days=float(overrides.get("half_life_days") or get_current_ops_half_life_days()),
            now=overrides.get("now"),
            historical_query=bool(overrides.get("historical_query", False)),
            exact_ids_present=bool(overrides.get("exact_ids_present", False)),
            multi_event=bool(overrides.get("multi_event", False)),
        )


def query_has_historical_dates(query: str) -> bool:
    text = query or ""
    return bool(_YEAR.search(text) or _ISO_DATE.search(text))


def query_requests_current_ops(query: str) -> bool:
    lowered = (query or "").casefold()
    if query_has_historical_dates(query):
        return False
    return any(marker in lowered for marker in _CURRENT_OPS_MARKERS)


def parent_thread_of(row: dict[str, Any]) -> str:
    for key in ("parent_thread", "thread", "gmail_thread_id", "imessage_chat_id", "beeper_room_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    card_type = str(row.get("type") or "")
    uid = str(row.get("card_uid") or "")
    if card_type.endswith("_thread"):
        return uid
    return uid


def is_multi_event_row(row: dict[str, Any]) -> bool:
    return str(row.get("type") or "") in MULTI_EVENT_CARD_TYPES


def _stable_channel_ranks(rows: Iterable[dict[str, Any]], score_key: str) -> dict[str, int]:
    eligible = [row for row in rows if float(row.get(score_key, 0.0) or 0.0) > 0.0]
    eligible.sort(
        key=lambda row: (
            -float(row.get(score_key, 0.0) or 0.0),
            str(row.get("card_uid") or ""),
        )
    )
    return {str(row["card_uid"]): index + 1 for index, row in enumerate(eligible)}


def rrf_contribution(weight: float, rank: int | None, *, k: int) -> float:
    if not rank:
        return 0.0
    return float(weight) / (float(k) + float(rank))


def age_decay_factor(
    activity_at: str,
    *,
    half_life_days: float,
    now: datetime | None,
) -> float:
    text = (activity_at or "").strip()
    if not text or half_life_days <= 0:
        return 1.0
    parsed: datetime | None = None
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return 1.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    clock = now or datetime.now(timezone.utc)
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=timezone.utc)
    days = max((clock - parsed).total_seconds() / 86400.0, 0.0)
    return float(0.5 ** (days / half_life_days))


def apply_rrf_scores(rows: list[dict[str, Any]], options: FusionOptions) -> list[dict[str, Any]]:
    lexical_ranks = _stable_channel_ranks(rows, "lexical_score")
    vector_ranks = _stable_channel_ranks(rows, "vector_similarity")
    graph_ranks = _stable_channel_ranks(rows, "graph_neighbor_trust")
    exact_rows = [row for row in rows if bool(row.get("exact_match"))]
    exact_ranks = {
        str(row["card_uid"]): index + 1
        for index, row in enumerate(
            sorted(exact_rows, key=lambda item: (-float(item.get("lexical_score", 0.0) or 0.0), str(item["card_uid"])))
        )
    }
    decay_enabled = (
        options.ranking_profile == "current_ops" and not options.historical_query and not options.exact_ids_present
    )
    for row in rows:
        uid = str(row["card_uid"])
        lex_rank = lexical_ranks.get(uid)
        vec_rank = vector_ranks.get(uid)
        graph_rank = graph_ranks.get(uid)
        exact_rank = exact_ranks.get(uid)
        lex = rrf_contribution(options.lexical_weight, lex_rank, k=options.rrf_k)
        vec = rrf_contribution(options.vector_weight, vec_rank, k=options.rrf_k)
        graph = rrf_contribution(options.graph_weight, graph_rank, k=options.rrf_k)
        exact = rrf_contribution(options.exact_weight, exact_rank, k=options.rrf_k)
        rarity = float(row.get("lexical_score", 0.0) or 0.0) * float(options.rare_token_weight)
        decay = (
            age_decay_factor(
                str(row.get("activity_at") or ""),
                half_life_days=options.half_life_days,
                now=options.now,
            )
            if decay_enabled
            else 1.0
        )
        rrf = exact + lex + vec + graph + rarity
        weight = float(row.get("retrieval_weight", 1.0) or 1.0)
        row["lexical_rank"] = lex_rank or 0
        row["vector_rank"] = vec_rank or 0
        row["graph_rank"] = graph_rank or 0
        row["exact_rank"] = exact_rank or 0
        row["rrf_lexical"] = round(lex, 8)
        row["rrf_vector"] = round(vec, 8)
        row["rrf_graph"] = round(graph, 8)
        row["rrf_exact"] = round(exact, 8)
        row["rare_token_boost"] = round(rarity, 8)
        row["age_decay"] = round(decay, 8)
        row["rrf_score"] = round(rrf, 8)
        row["score"] = round(rrf * weight * decay, 8)
        row["lexical_rarity"] = float(row.get("lexical_score", 0.0) or 0.0)
        channels = []
        if exact_rank:
            channels.append("exact")
        if lex_rank:
            channels.append("lexical")
        if vec_rank:
            channels.append("vector")
        if graph_rank:
            channels.append("graph")
        row["match_channel"] = "+".join(channels) if channels else str(row.get("matched_by") or "")
        if not row.get("parent_thread"):
            row["parent_thread"] = parent_thread_of(row)
    return rows


def apply_thread_diversity(rows: list[dict[str, Any]], options: FusionOptions) -> list[dict[str, Any]]:
    if options.multi_event or options.diversity_cap <= 0:
        return rows
    selected: list[dict[str, Any]] = []
    deferred: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    window = max(int(options.diversity_window), 1)
    cap = max(int(options.diversity_cap), 1)
    for row in rows:
        if len(selected) >= window:
            selected.append(row)
            continue
        if bool(row.get("exact_match")) or is_multi_event_row(row):
            selected.append(row)
            continue
        parent = parent_thread_of(row)
        if counts.get(parent, 0) >= cap:
            deferred.append(row)
            continue
        counts[parent] = counts.get(parent, 0) + 1
        selected.append(row)
    return selected + deferred


def sort_fused_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows.sort(
        key=lambda row: (
            -int(bool(row.get("exact_match"))),
            -float(row.get("score", 0.0) or 0.0),
            -float(row.get("vector_similarity", 0.0) or 0.0),
            -float(row.get("lexical_score", 0.0) or 0.0),
            str(row.get("card_uid") or ""),
            str(row.get("rel_path") or ""),
        )
    )
    return rows


def ndcg_at_k(ranked_ids: list[str], relevant: set[str], *, k: int) -> float:
    def gain(hit: bool, rank: int) -> float:
        if not hit:
            return 0.0
        return 1.0 / _log2(rank + 1)

    def _log2(value: int) -> float:
        import math

        return math.log(value, 2)

    scored = 0.0
    for index, uid in enumerate(ranked_ids[:k], start=1):
        scored += gain(uid in relevant, index)
    ideal = 0.0
    for index in range(1, min(k, len(relevant)) + 1):
        ideal += gain(True, index)
    return 0.0 if ideal == 0.0 else scored / ideal


def mrr(ranked_ids: list[str], relevant: set[str]) -> float:
    for index, uid in enumerate(ranked_ids, start=1):
        if uid in relevant:
            return 1.0 / float(index)
    return 0.0


def recall_at_k(ranked_ids: list[str], relevant: set[str], *, k: int) -> float:
    if not relevant:
        return 0.0
    return len(set(ranked_ids[:k]) & relevant) / float(len(relevant))

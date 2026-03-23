"""Pluggable reranking over fused hybrid candidates (optional model providers)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Protocol


def _tokenize(text: str) -> set[str]:
    return {t for t in re.split(r"[^\w]+", text.lower()) if len(t) > 1}


@dataclass
class RerankInput:
    query: str
    card_uid: str
    summary: str
    preview: str
    card_type: str
    context_text: str = ""


@dataclass
class RerankResult:
    card_uid: str
    score: float
    detail: str = ""


class Reranker(Protocol):
    def rerank(self, query: str, candidates: list[dict[str, Any]], **kwargs: Any) -> list[RerankResult]: ...


class NoopReranker:
    def rerank(self, query: str, candidates: list[dict[str, Any]], **kwargs: Any) -> list[RerankResult]:
        return [RerankResult(card_uid=str(c.get("card_uid", "")), score=0.0, detail="noop") for c in candidates]


class HeuristicReranker:
    """Lexical overlap between query and summary/preview/context (CPU-only)."""

    def rerank(self, query: str, candidates: list[dict[str, Any]], **kwargs: Any) -> list[RerankResult]:
        q_tokens = _tokenize(query)
        if not q_tokens:
            return [RerankResult(card_uid=str(c.get("card_uid", "")), score=0.0, detail="empty_query") for c in candidates]
        out: list[RerankResult] = []
        for c in candidates:
            blob = " ".join(
                [
                    str(c.get("summary", "")),
                    str(c.get("preview", "")),
                    str(c.get("context_text", "")),
                ]
            )
            ct = _tokenize(blob)
            overlap = len(q_tokens & ct)
            union = len(q_tokens | ct) or 1
            score = min(1.0, overlap / max(len(q_tokens), 1) * 0.5 + (overlap / union) * 0.5)
            out.append(RerankResult(card_uid=str(c.get("card_uid", "")), score=round(score, 6), detail=f"overlap={overlap}"))
        return out


class ModelReranker:
    """Reserved for OpenAI-compatible / local cross-encoder providers."""

    def __init__(self, *, model: str = "") -> None:
        self.model = model

    def rerank(self, query: str, candidates: list[dict[str, Any]], **kwargs: Any) -> list[RerankResult]:
        # Shipping default: behave like heuristic until wired to an API.
        return HeuristicReranker().rerank(query, candidates, **kwargs)


def blend_rerank_scores(
    rows: list[dict[str, Any]],
    rerank_by_uid: dict[str, RerankResult],
    *,
    top_1_3_retrieval_weight: float = 0.75,
    top_4_10_retrieval_weight: float = 0.60,
    rest_retrieval_weight: float = 0.40,
    preserve_exact_match_floor: bool = True,
) -> list[dict[str, Any]]:
    """Position-aware blend: strong retrieval scores keep weight at top ranks."""
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(rows):
        uid = str(row.get("card_uid", ""))
        base = float(row.get("score", 0.0))
        rr = rerank_by_uid.get(uid)
        rscore = float(rr.score) if rr else 0.0
        if idx < 3:
            w = top_1_3_retrieval_weight
        elif idx < 10:
            w = top_4_10_retrieval_weight
        else:
            w = rest_retrieval_weight
        blended = w * base + (1.0 - w) * (rscore * 4.0)
        if preserve_exact_match_floor and bool(row.get("exact_match")):
            blended = max(blended, base)
        new_row = dict(row)
        new_row["rerank_score"] = rscore
        new_row["rerank_contribution"] = round(blended - base, 6)
        new_row["score"] = round(blended, 6)
        new_row["pre_rerank_score"] = base
        out.append(new_row)
    out.sort(
        key=lambda r: (
            -float(r["score"]),
            -int(bool(r.get("exact_match"))),
            -float(r.get("vector_similarity", 0.0)),
            -float(r.get("lexical_score", 0.0)),
            str(r.get("rel_path", "")),
        )
    )
    return out


def reranker_for_config(cfg: dict[str, Any]) -> Reranker:
    block = cfg.get("reranker") or {}
    if not block.get("enabled", False):
        return NoopReranker()
    provider = str(block.get("provider") or "none").strip().lower()
    if provider in {"", "none", "noop"}:
        return NoopReranker()
    if provider == "heuristic":
        return HeuristicReranker()
    if provider == "model":
        return ModelReranker(model=str(block.get("model") or ""))
    return NoopReranker()

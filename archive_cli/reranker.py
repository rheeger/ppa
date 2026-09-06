"""Pluggable reranking over fused hybrid candidates.

Heuristic overlap is an explicit provider. Model/HTTP rerank never silently
falls back to that heuristic.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Protocol

from .index_config import get_rerank_timeout_ms

RERANK_SCHEMA_VERSION = "p01b2-rerank-1"


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
    model_revision: str = ""


class RerankError(RuntimeError):
    """Transport or contract failure. Callers keep RRF order."""


class RerankUnavailable(RerankError):
    """Configured model rerank is not actually callable."""


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
            return [
                RerankResult(card_uid=str(c.get("card_uid", "")), score=0.0, detail="empty_query") for c in candidates
            ]
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
            out.append(
                RerankResult(card_uid=str(c.get("card_uid", "")), score=round(score, 6), detail=f"overlap={overlap}")
            )
        return out


class HttpReranker:
    """Versioned local HTTP scorer. Transport proof, not model-quality proof."""

    def __init__(
        self,
        *,
        endpoint: str,
        model: str = "",
        timeout_ms: int | None = None,
        schema_version: str = RERANK_SCHEMA_VERSION,
    ) -> None:
        self.endpoint = (endpoint or "").strip()
        self.model = model
        self.timeout_ms = timeout_ms if timeout_ms is not None else get_rerank_timeout_ms()
        self.schema_version = schema_version
        if not self.endpoint:
            raise RerankUnavailable("http_rerank_endpoint_unconfigured")

    def rerank(self, query: str, candidates: list[dict[str, Any]], **kwargs: Any) -> list[RerankResult]:
        payload = {
            "schema_version": self.schema_version,
            "query": query,
            "model": self.model,
            "candidates": [
                {
                    "id": str(row.get("card_uid", "")),
                    "text": str(row.get("summary", "") or row.get("preview", "")),
                    "context": str(row.get("context_text", "")),
                }
                for row in candidates
            ],
        }
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        timeout = max(self.timeout_ms, 1) / 1000.0
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raise RerankError(f"http_rerank_status_{exc.code}") from exc
        except urllib.error.URLError as exc:
            raise RerankError(f"http_rerank_transport: {exc.reason}") from exc
        except TimeoutError as exc:
            raise RerankError("http_rerank_timeout") from exc
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RerankError("http_rerank_invalid_json") from exc
        if not isinstance(parsed, dict):
            raise RerankError("http_rerank_invalid_payload")
        if str(parsed.get("schema_version") or "") != self.schema_version:
            raise RerankError("http_rerank_schema_mismatch")
        results = parsed.get("results")
        if not isinstance(results, list):
            raise RerankError("http_rerank_missing_results")
        revision = str(parsed.get("model_revision") or self.model or "")
        seen: set[str] = set()
        out: list[RerankResult] = []
        for item in results:
            if not isinstance(item, dict):
                continue
            uid = str(item.get("id") or "")
            if not uid or uid in seen:
                continue
            try:
                score = float(item.get("score"))
            except (TypeError, ValueError) as exc:
                raise RerankError("http_rerank_invalid_score") from exc
            if score != score:  # NaN
                raise RerankError("http_rerank_invalid_score")
            seen.add(uid)
            out.append(RerankResult(card_uid=uid, score=score, detail="http", model_revision=revision))
        known = {str(row.get("card_uid", "")) for row in candidates}
        if any(uid not in known for uid in seen):
            raise RerankError("http_rerank_unknown_id")
        return out


class ModelReranker:
    """HTTP model adapter. Never masquerades as HeuristicReranker."""

    def __init__(self, *, model: str = "", endpoint: str = "", timeout_ms: int | None = None) -> None:
        self.model = model
        self.endpoint = (endpoint or "").strip()
        self.timeout_ms = timeout_ms
        self._http: HttpReranker | None = None
        if self.endpoint:
            self._http = HttpReranker(endpoint=self.endpoint, model=model, timeout_ms=timeout_ms)

    def rerank(self, query: str, candidates: list[dict[str, Any]], **kwargs: Any) -> list[RerankResult]:
        if self._http is None:
            raise RerankUnavailable("model_rerank_unconfigured")
        return self._http.rerank(query, candidates, **kwargs)


def blend_rerank_scores(
    rows: list[dict[str, Any]],
    rerank_by_uid: dict[str, RerankResult],
    *,
    top_1_3_retrieval_weight: float = 0.75,
    top_4_10_retrieval_weight: float = 0.60,
    rest_retrieval_weight: float = 0.40,
    preserve_exact_match_floor: bool = True,
) -> list[dict[str, Any]]:
    """Position-aware blend used only by the explicit heuristic provider."""
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
        if rr and rr.model_revision:
            new_row["rerank_model_revision"] = rr.model_revision
        out.append(new_row)
    out.sort(
        key=lambda r: (
            -int(bool(r.get("exact_match"))),
            -float(r["score"]),
            -float(r.get("vector_similarity", 0.0)),
            -float(r.get("lexical_score", 0.0)),
            str(r.get("rel_path", "")),
        )
    )
    return out


def apply_http_rerank_order(
    rows: list[dict[str, Any]],
    rerank_by_uid: dict[str, RerankResult],
    *,
    preserve_exact_match_floor: bool = True,
) -> list[dict[str, Any]]:
    """Reorder non-exact head rows by the HTTP scorer. Exact IDs stay first."""
    exact: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = []
    for row in rows:
        updated = dict(row)
        uid = str(updated.get("card_uid", ""))
        rr = rerank_by_uid.get(uid)
        base = float(updated.get("score", 0.0) or 0.0)
        updated["pre_rerank_score"] = base
        if rr is not None:
            updated["rerank_score"] = rr.score
            updated["rerank_model_revision"] = rr.model_revision
            if not (preserve_exact_match_floor and bool(updated.get("exact_match"))):
                updated["score"] = float(rr.score)
                updated["rerank_contribution"] = round(float(rr.score) - base, 6)
            else:
                updated["score"] = base
                updated["rerank_contribution"] = 0.0
        else:
            updated["rerank_score"] = 0.0
            updated["rerank_contribution"] = 0.0
        if preserve_exact_match_floor and bool(updated.get("exact_match")):
            exact.append(updated)
        else:
            rest.append(updated)
    rest.sort(
        key=lambda item: (
            -float(item.get("rerank_score", 0.0) or 0.0),
            -float(item.get("pre_rerank_score", 0.0) or 0.0),
            str(item.get("card_uid") or ""),
        )
    )
    return exact + rest


def reranker_for_config(cfg: dict[str, Any]) -> Reranker:
    block = cfg.get("reranker") or {}
    if not block.get("enabled", False):
        return NoopReranker()
    provider = str(block.get("provider") or "none").strip().lower()
    endpoint = str(block.get("endpoint") or block.get("base_url") or "").strip()
    timeout_ms = int(block.get("timeout_ms") or get_rerank_timeout_ms())
    if provider in {"", "none", "noop"}:
        return NoopReranker()
    if provider == "heuristic":
        return HeuristicReranker()
    if provider in {"http", "model"}:
        return ModelReranker(
            model=str(block.get("model") or ""),
            endpoint=endpoint,
            timeout_ms=timeout_ms,
        )
    return NoopReranker()

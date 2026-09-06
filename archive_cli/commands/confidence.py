"""Confidence signaling and gap detection for retrieval tools.

``high`` / ``medium`` / ``low`` is retrieval evidence quality — not the
probability that a user's implied proposition is true, and not a function of
hit count. Eleven weak hits stay low. One exact identifier match may be strong
without implying the source is complete.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal

from archive_engine.contracts import UNKNOWN, EvidenceEnvelope, EvidenceKind

log = logging.getLogger("ppa.confidence")

CONFIDENCE_MEANING = "retrieval_evidence_quality"
ASSESSMENT_VERSION = "p01c-1"
PIPELINE_VERSION_FALLBACK = "2026.09.06.p01b2"

REASON_NO_RESULTS = "no_results"
REASON_SPARSE = "sparse_results"
REASON_EXACT_IDENTIFIER = "exact_identifier"
REASON_VOLUME_WITHOUT_RELEVANCE = "volume_without_relevance"
REASON_CHANNEL_HITS = "lexical_or_semantic_hits"
_RELEVANCE_MARKERS = frozenset({"exact", "lexical", "vector", "hybrid", "graph", "seed-link", "seed_link"})


class ConfidenceLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True)
class GapEntry:
    query_text: str
    gap_type: str
    detail: str = ""
    card_uid: str = ""


@dataclass(frozen=True)
class ConfidenceAssessment:
    """Structured evidence-quality decision attached to CLI and MCP responses."""

    level: ConfidenceLevel
    reason_codes: tuple[str, ...]
    reason: str
    exact_match: bool
    result_count: int
    truncated: bool | None
    coverage: str
    freshness: str
    method: str
    corpus_state: str
    provenance: str
    evidence_kind: EvidenceKind
    errors: tuple[str, ...] = ()
    complete: bool = False


def _as_row_text(row: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value:
            return str(value).strip()
    return ""


def _channel_tokens(row: Mapping[str, Any]) -> set[str]:
    raw = " ".join(
        part
        for part in (
            _as_row_text(row, "match_channel"),
            _as_row_text(row, "matched_by"),
            "+".join(str(item) for item in row.get("matched_by")) if isinstance(row.get("matched_by"), list) else "",
        )
        if part
    )
    tokens = {token.strip().lower() for token in raw.replace("+", " ").replace(",", " ").split() if token.strip()}
    return tokens


def rows_have_relevance(rows: Sequence[Mapping[str, Any]]) -> bool:
    """True when any hit carries an exact/channel signal. Volume is not a signal."""

    for row in rows:
        if bool(row.get("exact_match")):
            return True
        if _channel_tokens(row) & _RELEVANCE_MARKERS:
            return True
        kind = str(row.get("evidence_kind") or "").strip()
        if kind in {"source_reported", "derived", "proposed_link"}:
            return True
    return False


def _shared_row_value(rows: Sequence[Mapping[str, Any]], *keys: str) -> str:
    values = {value for value in (_as_row_text(row, *keys) for row in rows) if value}
    if len(values) == 1:
        return next(iter(values))
    return ""


def _infer_method(rows: Sequence[Mapping[str, Any]], *, exact_match: bool, method: str) -> str:
    if method:
        return method
    if exact_match:
        return "exact"
    for row in rows:
        tokens = _channel_tokens(row)
        for marker in ("exact", "lexical", "vector", "hybrid", "graph", "seed-link"):
            if marker in tokens:
                return marker
    return UNKNOWN


def _infer_evidence_kind(
    rows: Sequence[Mapping[str, Any]], *, exact_match: bool, evidence_kind: str
) -> EvidenceKind:
    if evidence_kind in {"source_reported", "derived", "proposed_link", UNKNOWN}:
        return evidence_kind  # type: ignore[return-value]
    kinds = {str(row.get("evidence_kind") or "").strip() for row in rows}
    kinds.discard("")
    if kinds == {"source_reported"} or (exact_match and not kinds):
        return "source_reported"
    if kinds == {"derived"}:
        return "derived"
    if kinds == {"proposed_link"}:
        return "proposed_link"
    return UNKNOWN


def _infer_truncated(*, result_count: int, limit: int | None, truncated: bool | None) -> bool | None:
    if truncated is not None:
        return bool(truncated)
    if limit is None:
        return None
    return result_count >= limit and limit > 0


def assess_confidence(
    *,
    result_count: int,
    exact_match: bool = False,
    query_text: str = "",
    rows: Sequence[Mapping[str, Any]] | None = None,
    limit: int | None = None,
    truncated: bool | None = None,
    coverage: str | None = None,
    freshness: str | None = None,
    method: str = "",
    corpus_state: str = "",
    provenance: str = "",
    evidence_kind: str = "",
    errors: Sequence[str] = (),
) -> ConfidenceAssessment:
    """Score retrieval evidence quality. Hit count never produces ``high``."""

    del query_text  # reserved for future query-shape signals
    row_list = list(rows or ())
    known_truncated = _infer_truncated(result_count=result_count, limit=limit, truncated=truncated)
    has_relevance = exact_match or rows_have_relevance(row_list)
    coverage_value = coverage or UNKNOWN
    freshness_value = freshness or UNKNOWN
    method_value = _infer_method(row_list, exact_match=exact_match, method=method)
    corpus_value = corpus_state or _shared_row_value(row_list, "corpus_state") or UNKNOWN
    provenance_value = provenance or _shared_row_value(row_list, "provenance") or UNKNOWN
    kind_value = _infer_evidence_kind(row_list, exact_match=exact_match, evidence_kind=evidence_kind)
    error_tuple = tuple(str(item) for item in errors if str(item).strip())

    if exact_match:
        level = ConfidenceLevel.HIGH
        codes = (REASON_EXACT_IDENTIFIER,)
        reason = "exact_identifier|authoritative identifier match; completeness not implied"
    elif result_count == 0:
        level = ConfidenceLevel.LOW
        codes = (REASON_NO_RESULTS,)
        reason = "no_results|zero hits; coverage=unknown; freshness=unknown"
    elif result_count > 10 and not has_relevance:
        level = ConfidenceLevel.LOW
        codes = (REASON_VOLUME_WITHOUT_RELEVANCE,)
        reason = f"volume_without_relevance|{result_count} hits without exact or channel support"
    elif result_count < 3:
        level = ConfidenceLevel.LOW
        codes = (REASON_SPARSE,)
        reason = f"sparse_results|only {result_count} results"
    elif has_relevance:
        level = ConfidenceLevel.MEDIUM
        codes = (REASON_CHANNEL_HITS,)
        reason = f"lexical_or_semantic_hits|{result_count} channel-backed hits; not an exact identifier"
    else:
        level = ConfidenceLevel.MEDIUM
        codes = (REASON_CHANNEL_HITS,)
        reason = f"lexical_or_semantic_hits|{result_count} hits without exact identifier"

    return ConfidenceAssessment(
        level=level,
        reason_codes=codes,
        reason=reason,
        exact_match=exact_match,
        result_count=result_count,
        truncated=known_truncated,
        coverage=coverage_value,
        freshness=freshness_value,
        method=method_value,
        corpus_state=corpus_value,
        provenance=provenance_value,
        evidence_kind=kind_value,
        errors=error_tuple,
        complete=False,
    )


def compute_confidence(
    *,
    result_count: int,
    exact_match: bool = False,
    query_text: str = "",
    rows: Sequence[Mapping[str, Any]] | None = None,
    limit: int | None = None,
) -> ConfidenceLevel:
    """Compatibility wrapper. Volume alone cannot return high."""

    return assess_confidence(
        result_count=result_count,
        exact_match=exact_match,
        query_text=query_text,
        rows=rows,
        limit=limit,
    ).level


def build_evidence_envelope(
    assessment: ConfidenceAssessment,
    *,
    query: str = "",
    generation: str = "",
    watermark: int | None = None,
    pipeline_version: str = "",
    evidence_refs: Sequence[Any] = (),
) -> EvidenceEnvelope:
    """Build the shared P06-A envelope. Unknown fields stay unknown."""

    truncated_flag = bool(assessment.truncated) if assessment.truncated is not None else False
    return EvidenceEnvelope(
        generation=generation.strip() or UNKNOWN,
        watermark=int(watermark or 0),
        evidence_refs=tuple(evidence_refs),
        coverage=assessment.coverage or UNKNOWN,
        freshness=assessment.freshness or UNKNOWN,
        complete=False,
        truncated=truncated_flag,
        confidence_reason=assessment.reason,
        errors=assessment.errors,
        method=assessment.method or UNKNOWN,
        corpus_state=assessment.corpus_state or UNKNOWN,
        provenance=assessment.provenance or UNKNOWN,
        evidence_kind=assessment.evidence_kind,
        pipeline_version=pipeline_version or PIPELINE_VERSION_FALLBACK,
        query=query,
    )


def attach_retrieval_envelope(
    result: dict[str, Any],
    *,
    query: str,
    rows_key: str = "rows",
    result_count: int | None = None,
    exact_match: bool | None = None,
    limit: int | None = None,
    truncated: bool | None = None,
    coverage: str | None = None,
    freshness: str | None = None,
    method: str = "",
    corpus_state: str = "",
    provenance: str = "",
    evidence_kind: str = "",
    errors: Sequence[str] = (),
    pipeline_version: str = "",
    generation: str = "",
    watermark: int | None = None,
) -> dict[str, Any]:
    """Write legacy confidence plus ``EvidenceEnvelope`` onto a command/store payload."""

    rows_raw = result.get(rows_key)
    rows = list(rows_raw) if isinstance(rows_raw, list) else []
    count = len(rows) if result_count is None else result_count
    any_exact = bool(exact_match) if exact_match is not None else any(bool(row.get("exact_match")) for row in rows)
    known_truncated = truncated if truncated is not None else result.get("truncated")
    if isinstance(known_truncated, bool) or known_truncated is None:
        truncated_arg = known_truncated
    else:
        truncated_arg = None
    existing_errors = result.get("errors")
    error_list = list(errors)
    if isinstance(existing_errors, list):
        error_list.extend(str(item) for item in existing_errors)
    generation_value = generation or str(result.get("serving_index_generation") or "")
    if not generation_value:
        existing = result.get("evidence")
        if isinstance(existing, dict):
            generation_value = str(existing.get("generation") or "")
    assessment = assess_confidence(
        result_count=count,
        exact_match=any_exact,
        query_text=query,
        rows=rows,
        limit=limit,
        truncated=truncated_arg,
        coverage=coverage,
        freshness=freshness,
        method=method,
        corpus_state=corpus_state,
        provenance=provenance,
        evidence_kind=evidence_kind,
        errors=error_list,
    )
    envelope = build_evidence_envelope(
        assessment,
        query=query,
        generation=generation_value,
        watermark=watermark,
        pipeline_version=pipeline_version or str(result.get("pipeline_version") or ""),
    )
    result["ok"] = True
    result["confidence"] = assessment.level.value
    result["confidence_reason"] = assessment.reason
    result["confidence_reasons"] = list(assessment.reason_codes)
    result["confidence_meaning"] = CONFIDENCE_MEANING
    result["assessment_version"] = ASSESSMENT_VERSION
    result["evidence"] = envelope.to_payload()
    return result


def client_failure_payload(
    *,
    status: Literal["error", "denied"],
    error: str,
    message: str,
    tool: str = "",
    query: str = "",
) -> dict[str, Any]:
    """Denial/error payload that cannot be read as empty success."""

    envelope = EvidenceEnvelope(
        generation=UNKNOWN,
        watermark=0,
        coverage=UNKNOWN,
        freshness=UNKNOWN,
        complete=False,
        truncated=False,
        confidence_reason=f"{status}|{error}",
        errors=(error, message),
        method=UNKNOWN,
        corpus_state=UNKNOWN,
        provenance=UNKNOWN,
        evidence_kind=UNKNOWN,
        pipeline_version=PIPELINE_VERSION_FALLBACK,
        query=query,
    )
    payload: dict[str, Any] = {
        "ok": False,
        "status": status,
        "error": error,
        "message": message,
        "tool": tool,
        "confidence": None,
        "confidence_meaning": CONFIDENCE_MEANING,
        "evidence": envelope.to_payload(),
    }
    return payload


def is_client_failure(payload: Mapping[str, Any] | str) -> bool:
    if isinstance(payload, str):
        text = payload.lstrip()
        if not text.startswith("{"):
            return "Tool disabled" in payload or "Invalid PPA_MCP_TOOL_PROFILE" in payload
        try:
            import json

            payload = json.loads(payload)
        except ValueError:
            return False
    if not isinstance(payload, Mapping):
        return False
    if payload.get("ok") is False:
        return True
    status = str(payload.get("status") or "")
    return status in {"error", "denied"} or bool(payload.get("error"))


def detect_gaps(
    *,
    query_text: str,
    result_count: int,
    expected_types: list[str] | None = None,
    actual_types: list[str] | None = None,
    card_uid: str = "",
) -> list[GapEntry]:
    """Return gap entries for sparse/missing results."""
    gaps: list[GapEntry] = []
    if result_count == 0:
        gaps.append(GapEntry(query_text=query_text, gap_type="no_results"))
    elif result_count < 3:
        gaps.append(
            GapEntry(
                query_text=query_text,
                gap_type="sparse_results",
                detail=f"only {result_count} results",
            )
        )
    if expected_types and actual_types is not None:
        missing = set(expected_types) - set(actual_types)
        if missing:
            gaps.append(
                GapEntry(
                    query_text=query_text,
                    gap_type="type_mismatch",
                    detail=f"missing types: {sorted(missing)}",
                    card_uid=card_uid,
                )
            )
    return gaps


def log_gaps(
    gaps: list[GapEntry],
    *,
    index: Any,
    logger: logging.Logger,
) -> None:
    """Write gap entries to retrieval_gaps table. Skip silently if table missing."""
    del logger
    if not gaps:
        return
    index.log_retrieval_gaps(
        [
            {
                "query_text": g.query_text,
                "gap_type": g.gap_type,
                "detail": g.detail,
                "card_uid": g.card_uid,
            }
            for g in gaps
        ]
    )

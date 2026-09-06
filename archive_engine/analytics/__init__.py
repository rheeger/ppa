"""Bounded analytical adapters and deterministic workflows (P10-A / P10-C).

Warehouse SQL is allowlisted. Workflows read typed rows / journal records.
Clients cannot submit SQL or invent a current subscription from an old renewal.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from archive_engine.access import card_permitted
from archive_engine.analytics.warehouse import (
    ALLOWED_AGGREGATES,
    TypedAggregateSpec,
    compile_typed_aggregate,
    execute_typed_aggregate,
)
from archive_engine.contracts import UNKNOWN, AccessContext
from archive_engine.errors import QueryValidationError

WORKFLOW_CONTRACT_VERSION = "p10c.1"
EvidenceKind = Literal["source_reported", "derived", "proposed_link", "unknown"]
EVIDENCE_KINDS = frozenset({"source_reported", "derived", "proposed_link", "unknown"})
ADVICE_KEYS = frozenset(
    {
        "advice",
        "recommendation",
        "should_cancel",
        "should_buy",
        "authorized",
        "currently_subscribed",
        "current_subscription",
        "fx_rate",
        "converted_amount",
    }
)


@dataclass(frozen=True)
class WorkflowEvidence:
    uid: str
    evidence_kind: EvidenceKind
    role: str
    method: str = UNKNOWN
    reason: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "uid": self.uid,
            "evidence_kind": self.evidence_kind,
            "role": self.role,
            "method": self.method or UNKNOWN,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class WorkflowResult:
    case_id: str
    workflow: str
    rows: tuple[dict[str, Any], ...]
    evidence: tuple[WorkflowEvidence, ...]
    complete: bool
    truncated: bool
    coverage: str = "eligible_stored"
    freshness: str = UNKNOWN
    total_status: str = "exact"

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "contract_version": WORKFLOW_CONTRACT_VERSION,
            "case_id": self.case_id,
            "workflow": self.workflow,
            "rows": [dict(row) for row in self.rows],
            "evidence": [item.to_payload() for item in self.evidence],
            "complete": self.complete,
            "truncated": self.truncated,
            "coverage": self.coverage,
            "freshness": self.freshness,
            "total_status": self.total_status,
        }
        leaked = ADVICE_KEYS.intersection(payload)
        if leaked:
            raise QueryValidationError(f"workflow payload must not emit {sorted(leaked)}")
        return payload


def flatten_card(card: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize a P04 card dict or a typed query row into a flat workflow row."""

    fields = dict(card.get("fields") or {})
    row = dict(card)
    row.update(fields)
    uid = str(row.get("uid") or row.get("card_uid") or "").strip()
    row["uid"] = uid
    row["card_uid"] = uid
    row["type"] = str(row.get("type") or "").strip()
    row["corpus_state"] = str(row.get("corpus_state") or "active").strip() or "active"
    if "source" not in row and fields.get("source") is not None:
        row["source"] = fields.get("source")
    if "sources" not in row:
        source = row.get("source")
        row["sources"] = list(source) if isinstance(source, (list, tuple)) else ([source] if source else [])
    if "required_sources" not in row:
        row["required_sources"] = list(row.get("sources") or [])
    if "lineage_complete" not in row:
        row["lineage_complete"] = True
    return row


def eligible_rows(
    cards: Sequence[Mapping[str, Any]],
    access: AccessContext | None,
) -> list[dict[str, Any]]:
    rows = [flatten_card(card) for card in cards]
    if access is None:
        return rows
    return [row for row in rows if card_permitted(access, row)]


def decimal_amount(value: Any) -> Decimal:
    """Precise decimal from stored values. Floats use ``str`` so 482.0 stays 482.0."""

    if value is None or value == "":
        raise QueryValidationError("amount is required")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise QueryValidationError("amount must be numeric")
    try:
        if isinstance(value, int):
            return Decimal(value)
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise QueryValidationError(f"amount is not a decimal: {value!r}") from exc


def evidence_kind_of(row: Mapping[str, Any], *, default: EvidenceKind = "source_reported") -> EvidenceKind:
    raw = str(row.get("evidence_kind") or row.get("edge_kind") or "").strip()
    if raw == "inferred":
        return "proposed_link"
    if raw in EVIDENCE_KINDS:
        return raw  # type: ignore[return-value]
    tags = row.get("tags") or []
    if isinstance(tags, (list, tuple)) and any(str(tag) == "derived_duplicate" for tag in tags):
        return "derived"
    if str(row.get("type") or "") == "observation":
        return "proposed_link"
    return default


def reject_advice(payload: Mapping[str, Any]) -> None:
    leaked = ADVICE_KEYS.intersection(payload)
    if leaked:
        raise QueryValidationError(f"workflow payload must not emit {sorted(leaked)}")
    for value in payload.values():
        if isinstance(value, dict):
            reject_advice(value)


__all__ = [
    "ADVICE_KEYS",
    "ALLOWED_AGGREGATES",
    "EVIDENCE_KINDS",
    "TypedAggregateSpec",
    "WORKFLOW_CONTRACT_VERSION",
    "WorkflowEvidence",
    "WorkflowResult",
    "compile_typed_aggregate",
    "decimal_amount",
    "eligible_rows",
    "evidence_kind_of",
    "execute_typed_aggregate",
    "flatten_card",
    "reject_advice",
]

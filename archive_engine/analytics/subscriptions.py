"""Subscription lifecycle over the full eligible event set.

A renewal is last-observed, never proof of currently active billing.
Cancellations, pauses, and starts stay distinct. Simultaneous contradictions
are unknown/conflict. No financial advice is emitted.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any

from archive_engine.analytics import (
    WORKFLOW_CONTRACT_VERSION,
    WorkflowEvidence,
    WorkflowResult,
    eligible_rows,
    evidence_kind_of,
    reject_advice,
)
from archive_engine.contracts import UNKNOWN, AccessContext

CASE_RENEWAL_IS_NOT_CURRENT = "rel-p04b-renewal-is-not-current"
WORKFLOW = "subscription_lifecycle"
CURRENT_STATES = frozenset({"current", "active", "currently_subscribed", "current_subscription"})
EVENT_RENEW = frozenset({"renewal", "renew", "renewed"})
EVENT_CANCEL = frozenset({"cancel", "cancelled", "canceled", "cancellation"})
EVENT_START = frozenset({"start", "started", "signup", "subscribe"})
EVENT_PAUSE = frozenset({"pause", "paused"})
STALE_STATES = frozenset({"quarantine", "suppressed"})


def _norm(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def _event_type(row: Mapping[str, Any]) -> str:
    return _norm(row.get("event_type") or row.get("lifecycle_event"))


def _service_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        _norm(row.get("service_name") or row.get("service") or row.get("summary")),
        _norm(row.get("account") or row.get("account_email") or ""),
        _norm(row.get("plan_name") or row.get("plan") or ""),
    )


def _event_at(row: Mapping[str, Any]) -> str:
    return str(row.get("event_at") or row.get("activity_at") or row.get("created") or "")


def _state_for(event_type: str) -> str:
    if event_type in EVENT_CANCEL:
        return "last_observed_canceled"
    if event_type in EVENT_RENEW:
        return "last_observed_renewed"
    if event_type in EVENT_START:
        return "last_observed_started"
    if event_type in EVENT_PAUSE:
        return "last_observed_paused"
    return "unknown"


def subscription_lifecycle(
    cards: Sequence[Mapping[str, Any]],
    *,
    access: AccessContext | None = None,
    as_of: str = "",
    truncated: bool = False,
    case_id: str = CASE_RENEWAL_IS_NOT_CURRENT,
) -> WorkflowResult:
    """Group subscription events and report last-observed state, never current."""

    rows = [row for row in eligible_rows(cards, access) if row.get("type") == "subscription"]
    excluded: list[WorkflowEvidence] = []
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("corpus_state") in STALE_STATES:
            excluded.append(
                WorkflowEvidence(
                    uid=row["uid"],
                    evidence_kind=evidence_kind_of(row),
                    role="excluded",
                    reason="stale_source",
                )
            )
            continue
        if as_of and _event_at(row) > as_of:
            excluded.append(
                WorkflowEvidence(
                    uid=row["uid"],
                    evidence_kind=evidence_kind_of(row),
                    role="excluded",
                    reason="after_as_of",
                )
            )
            continue
        groups[_service_key(row)].append(row)

    result_rows: list[dict[str, Any]] = []
    evidence: list[WorkflowEvidence] = list(excluded)
    freshness = UNKNOWN
    for key, events in sorted(groups.items(), key=lambda item: item[0]):
        ordered = sorted(events, key=lambda row: (_event_at(row), row["uid"]))
        types = [_event_type(row) for row in ordered]
        latest = ordered[-1]
        latest_type = _event_type(latest)
        state = _state_for(latest_type)
        conflict_uids = [row["uid"] for row in ordered if _event_type(row) != latest_type]
        same_stamp = len({_event_at(row) for row in ordered}) == 1 and len(set(types)) > 1
        if same_stamp:
            state = "conflict"
        if state in CURRENT_STATES:
            state = "unknown"
        row = {
            "service_name": events[0].get("service_name") or events[0].get("service") or "",
            "account": key[1],
            "plan_name": events[0].get("plan_name") or "",
            "lifecycle_state": state,
            "last_observed_event_uid": latest["uid"],
            "supporting_event_uids": [item["uid"] for item in ordered],
            "conflict_uids": conflict_uids,
            "freshness": UNKNOWN,
            "as_of": as_of,
            "billing_period": events[0].get("plan_name") or "",
        }
        reject_advice(row)
        result_rows.append(row)
        for item in ordered:
            evidence.append(
                WorkflowEvidence(
                    uid=item["uid"],
                    evidence_kind=evidence_kind_of(item),
                    role="supporting",
                    method=str(item.get("method") or "source_reported"),
                    reason=_event_type(item) or "subscription_event",
                )
            )

    payload_rows = tuple(result_rows)
    result = WorkflowResult(
        case_id=case_id,
        workflow=WORKFLOW,
        rows=payload_rows,
        evidence=tuple(evidence),
        complete=not truncated,
        truncated=truncated,
        freshness=freshness,
        total_status="unknown" if truncated else "exact",
    )
    reject_advice(result.to_payload())
    return result


def pebble_mail_state(cards: Sequence[Mapping[str, Any]], **kwargs: Any) -> dict[str, Any]:
    """Convenience view of the P04 Pebble Mail fixture."""

    result = subscription_lifecycle(cards, **kwargs)
    row = result.rows[0] if result.rows else {}
    return {
        "contract_version": WORKFLOW_CONTRACT_VERSION,
        "case_id": result.case_id,
        "lifecycle_state": row.get("lifecycle_state") or "unknown",
        "last_observed_event_uid": row.get("last_observed_event_uid") or "",
        "conflict_uids": list(row.get("conflict_uids") or []),
        "freshness": row.get("freshness") or UNKNOWN,
        "supporting_event_uids": list(row.get("supporting_event_uids") or []),
        "evidence": [item.to_payload() for item in result.evidence],
        "complete": result.complete,
    }

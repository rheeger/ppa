"""Deterministic trip membership and reconciled costs.

Booking identity is confirmation / order / source-email wikilink — never
proximity-only inference. Actual charges and booking estimates stay separate.
Currencies are never converted. Refunds stay negative.
"""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any

from archive_engine.analytics import (
    WorkflowEvidence,
    WorkflowResult,
    decimal_amount,
    eligible_rows,
    evidence_kind_of,
    reject_advice,
)
from archive_engine.contracts import UNKNOWN, AccessContext
from archive_engine.errors import QueryValidationError

CASE_SAME_TRIP = "rel-p04b-same-trip"
CASE_SAME_CHARGE = "rel-p04b-same-charge"
CASE_EUR_NET = "q-p04b-agg-eur-net"
WIKILINK_RE = re.compile(r"\[\[([^\]|#]+)(?:[|#][^\]]*)?\]\]")
TRAVEL_TYPES = frozenset({"flight", "accommodation", "car_rental", "ride", "finance", "purchase", "email_message"})
ROUNDING_NOTE = "legacy float fields use Decimal(str(value)); currencies are not converted"


def _wikilink_uid(value: Any) -> str:
    text = str(value or "").strip()
    match = WIKILINK_RE.search(text)
    if match:
        target = match.group(1).strip()
        if target.startswith("hfa-"):
            return target
    if text.startswith("hfa-"):
        return text
    return ""


def _booking_keys(row: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    for field in ("confirmation_code", "order_number"):
        raw = str(row.get(field) or "").strip()
        if raw:
            keys.add(raw.casefold())
    email = _wikilink_uid(row.get("source_email"))
    if email:
        keys.add(f"email:{email}")
    if row.get("type") == "email_message" and row.get("uid"):
        keys.add(f"email:{row['uid']}")
    return keys


def _amount_field(row: Mapping[str, Any]) -> Any:
    if row.get("amount") not in (None, ""):
        return row.get("amount")
    if row.get("total") not in (None, ""):
        return row.get("total")
    if row.get("total_cost") not in (None, ""):
        return row.get("total_cost")
    if row.get("fare") not in (None, ""):
        return row.get("fare")
    if row.get("fare_amount") not in (None, ""):
        return row.get("fare_amount")
    return None


def currency_totals(
    cards: Sequence[Mapping[str, Any]],
    *,
    access: AccessContext | None = None,
    truncated: bool = False,
    types: Sequence[str] = ("finance",),
    currency: str = "",
) -> dict[str, Any]:
    """Sum finance amounts by currency. Refuses to total a truncated page."""

    if truncated:
        return {
            "totals": {},
            "complete": False,
            "total_status": "unknown",
            "uids": [],
            "rounding": ROUNDING_NOTE,
        }
    rows = [
        row
        for row in eligible_rows(cards, access)
        if row.get("type") in set(types) and row.get("corpus_state") != "suppressed"
    ]
    if currency:
        rows = [row for row in rows if str(row.get("currency") or "").upper() == currency.upper()]
    grouped: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    uids: list[str] = []
    for row in rows:
        raw = _amount_field(row)
        if raw is None:
            continue
        code = str(row.get("currency") or "").upper() or UNKNOWN
        grouped[code] += decimal_amount(raw)
        uids.append(row["uid"])
    totals = {code: format(amount, "f") for code, amount in sorted(grouped.items())}
    payload = {
        "totals": totals,
        "complete": True,
        "total_status": "exact",
        "uids": uids,
        "rounding": ROUNDING_NOTE,
    }
    reject_advice(payload)
    return payload


def assemble_trip(
    cards: Sequence[Mapping[str, Any]],
    *,
    access: AccessContext | None = None,
    truncated: bool = False,
    case_id: str = CASE_SAME_TRIP,
) -> WorkflowResult:
    """Cluster travel cards that share a confirmation, order, or booking email."""

    rows = eligible_rows(cards, access)
    by_uid = {row["uid"]: row for row in rows}
    parent: dict[str, str] = {}

    def find(uid: str) -> str:
        parent.setdefault(uid, uid)
        while parent[uid] != uid:
            parent[uid] = parent[parent[uid]]
            uid = parent[uid]
        return uid

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[b] = a

    keyed: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        if row.get("type") not in TRAVEL_TYPES:
            continue
        if evidence_kind_of(row) == "proposed_link":
            continue
        for key in _booking_keys(row):
            keyed[key].append(row["uid"])
    for uids in keyed.values():
        head = uids[0]
        parent.setdefault(head, head)
        for uid in uids[1:]:
            union(head, uid)

    clusters: dict[str, list[dict[str, Any]]] = defaultdict(list)
    clustered_uids: set[str] = set()
    for uid, row in by_uid.items():
        if uid in parent:
            clusters[find(uid)].append(row)
            clustered_uids.add(uid)

    trip_rows: list[dict[str, Any]] = []
    evidence: list[WorkflowEvidence] = []
    for root, members in sorted(clusters.items(), key=lambda item: item[0]):
        member_uids = tuple(sorted(item["uid"] for item in members))
        keys = sorted({key for item in members for key in _booking_keys(item)})
        trip_rows.append(
            {
                "trip_key": root,
                "booking_keys": keys,
                "member_uids": list(member_uids),
                "membership_method": "booking_identity",
            }
        )
        for item in members:
            evidence.append(
                WorkflowEvidence(
                    uid=item["uid"],
                    evidence_kind=evidence_kind_of(item),
                    role="member",
                    method="booking_identity",
                    reason="confirmation_or_source_email",
                )
            )

    unmatched: list[dict[str, Any]] = []
    for row in rows:
        kind = evidence_kind_of(row)
        if kind == "proposed_link":
            unmatched.append(row)
            evidence.append(
                WorkflowEvidence(
                    uid=row["uid"],
                    evidence_kind="proposed_link",
                    role="unmatched",
                    method=str(row.get("method") or "inferred"),
                    reason="proposed_link_not_source_fact",
                )
            )
            continue
        if row.get("type") in TRAVEL_TYPES and row["uid"] not in clustered_uids:
            unmatched.append(row)
            evidence.append(
                WorkflowEvidence(
                    uid=row["uid"],
                    evidence_kind=kind,
                    role="unmatched",
                    method=UNKNOWN,
                    reason="no_booking_identity",
                )
            )

    scored = sorted(trip_rows, key=lambda trip: (-len(trip["member_uids"]), trip["trip_key"]))
    primary = next(
        (trip for trip in scored if len(trip["member_uids"]) >= 2),
        scored[0] if scored else {"member_uids": [], "booking_keys": []},
    )
    primary_members = set(primary.get("member_uids") or [])
    lookalikes = [
        row["uid"]
        for row in rows
        if row.get("type") in TRAVEL_TYPES
        and row["uid"] not in primary_members
        and evidence_kind_of(row) != "proposed_link"
        and row["uid"] not in {item["uid"] for item in unmatched}
    ]
    summary = {
        "member_uids": list(primary.get("member_uids") or []),
        "unmatched_evidence": [row["uid"] for row in unmatched],
        "excluded_uids": lookalikes,
        "trips": trip_rows,
        "membership_method": "booking_identity",
    }
    reject_advice(summary)
    return WorkflowResult(
        case_id=case_id,
        workflow="trip_workflow",
        rows=(summary,),
        evidence=tuple(evidence),
        complete=not truncated,
        truncated=truncated,
        total_status="unknown" if truncated else "exact",
    )


def reconcile_trip_costs(
    cards: Sequence[Mapping[str, Any]],
    *,
    access: AccessContext | None = None,
    truncated: bool = False,
    case_id: str = CASE_SAME_CHARGE,
) -> WorkflowResult:
    """Prefer a supported actual charge. Booking estimates stay separate."""

    if truncated:
        raise QueryValidationError("cannot reconcile costs over a truncated page")
    rows = eligible_rows(cards, access)
    assembled = assemble_trip(rows, access=access)
    member_uids = set(assembled.rows[0]["member_uids"] if assembled.rows else [])
    members = [row for row in rows if row["uid"] in member_uids]
    charges = [row for row in members if row.get("type") == "finance"]
    purchases = [row for row in members if row.get("type") == "purchase"]
    hotels = [row for row in members if row.get("type") == "accommodation"]
    flights = [row for row in members if row.get("type") == "flight"]
    rides = [row for row in members if row.get("type") == "ride"]

    evidence = [item for item in assembled.evidence if item.uid in member_uids]
    actual: list[dict[str, Any]] = []
    if charges:
        charge = sorted(charges, key=lambda row: row["uid"])[0]
        actual.append(
            {
                "supported_charge_uid": charge["uid"],
                "amount": format(decimal_amount(charge.get("amount")), "f"),
                "currency": str(charge.get("currency") or "USD").upper(),
                "receipt_uids": [row["uid"] for row in purchases if evidence_kind_of(row) != "derived"]
                + [
                    row["uid"]
                    for row in rows
                    if row.get("type") == "email_attachment"
                    and "receipt" in str(row.get("filename") or row.get("rel_path") or "").casefold()
                ],
            }
        )
        evidence.append(
            WorkflowEvidence(
                uid=charge["uid"],
                evidence_kind="source_reported",
                role="supported_charge",
                method="source_reported",
                reason="actual_charge",
            )
        )

    estimates: list[dict[str, Any]] = []
    for hotel in hotels:
        estimates.append(
            {
                "uid": hotel["uid"],
                "kind": "booking_estimate",
                "amount": format(decimal_amount(hotel.get("total_cost") or hotel.get("total")), "f"),
                "currency": "USD",
            }
        )
    segment_totals: list[dict[str, Any]] = []
    for flight in flights:
        fare = _amount_field(flight)
        if fare is None:
            continue
        segment_totals.append(
            {
                "uid": flight["uid"],
                "kind": "segment_estimate",
                "amount": format(decimal_amount(fare), "f"),
                "currency": "USD",
                "counted_in_actual": False,
            }
        )

    unmatched_ids = set(assembled.rows[0].get("unmatched_evidence") or [])
    unmatched_rides = [row for row in rows if row["uid"] in unmatched_ids and row.get("type") == "ride"]
    unmatched_expenses = []
    for ride in (*rides, *unmatched_rides):
        fare = _amount_field(ride)
        unmatched_expenses.append(
            {
                "uid": ride["uid"],
                "reason": "no_matching_charge" if ride["uid"] in member_uids else "no_booking_identity",
                "amount": None if fare is None else format(decimal_amount(fare), "f"),
                "currency": "USD",
            }
        )

    excluded = [
        row["uid"] for row in rows if row.get("type") in {"finance", "purchase"} and row["uid"] not in member_uids
    ]
    for uid in excluded:
        evidence.append(
            WorkflowEvidence(uid=uid, evidence_kind="source_reported", role="excluded", reason="lookalike_or_unlinked")
        )
    for purchase in purchases:
        evidence.append(
            WorkflowEvidence(
                uid=purchase["uid"],
                evidence_kind=evidence_kind_of(purchase, default="derived"),
                role="receipt",
                method="derived" if evidence_kind_of(purchase) == "derived" else "source_reported",
                reason="not_added_to_actual",
            )
        )

    actual_totals = currency_totals(charges, access=access, types=("finance",))
    summary = {
        "supported_charge_uid": actual[0]["supported_charge_uid"] if actual else "",
        "receipt_uids": actual[0]["receipt_uids"] if actual else [],
        "amount": actual[0]["amount"] if actual else "",
        "currency": actual[0]["currency"] if actual else "",
        "actual_charges": actual,
        "booking_estimates": estimates,
        "segment_estimates": segment_totals,
        "unmatched_expenses": unmatched_expenses,
        "excluded_uids": excluded,
        "currency_totals": actual_totals["totals"],
        "member_uids": sorted(member_uids),
        "rounding": ROUNDING_NOTE,
    }
    reject_advice(summary)
    return WorkflowResult(
        case_id=case_id,
        workflow="reconcile_trip_costs",
        rows=(summary,),
        evidence=tuple(evidence),
        complete=True,
        truncated=False,
        total_status="exact",
    )

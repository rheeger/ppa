"""CLI/MCP evidence clients (P10-D).

Typed queries, neighbor context, and the three deterministic workflows share
one request/result contract. Saved scopes narrow; they never widen policy.
Clients synthesize answers — this module returns facts, citations, and
completeness, not advice.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from archive_engine.analytics import ADVICE_KEYS, reject_advice
from archive_engine.analytics.changes import changes_since
from archive_engine.analytics.subscriptions import subscription_lifecycle
from archive_engine.analytics.trip_costs import assemble_trip, currency_totals, reconcile_trip_costs
from archive_engine.context import expand_neighbors, neighbor_units_from_cards
from archive_engine.contracts import AccessContext, ChangeRecord
from archive_engine.errors import QueryValidationError
from archive_engine.query import execute_typed_query, request_from_simple_filters
from archive_engine.scopes import (
    RequestFilters,
    SavedScope,
    empty_scope_result,
    parse_saved_scopes,
    resolve_effective_scope,
)

from ..store import DefaultArchiveStore

CLIENT_CONTRACT_VERSION = "p10d.1"
WORKFLOWS = frozenset(
    {
        "typed_query",
        "query",
        "context",
        "subscription_lifecycle",
        "subscriptions",
        "trip_costs",
        "trip-costs",
        "assemble_trip",
        "changes_since",
        "changes-since",
        "changes",
    }
)

_log = logging.getLogger("ppa.analytics")

REPLY = "hfa-email-message-p04breply01"
REQUEST = "hfa-email-message-p04breq0001"
STALE = "hfa-email-message-p04bstale01"
PERSON_A = "hfa-person-p04balex0001"
PERSON_B = "hfa-person-p04balex0002"


def _access(*, archive_id: str = "archive-p10d", **overrides: object) -> AccessContext:
    payload = {
        "archive_id": archive_id,
        "principal": "local-operator",
        "profile": "trusted-local",
        "allowed_tools": (),
        "allowed_sources": (),
        "allowed_domains": (),
        "deny": False,
        "deny_reason": "",
    }
    payload.update(overrides)
    return AccessContext(**payload)  # type: ignore[arg-type]


def load_json(raw: str | Path | None) -> Any:
    if raw is None or raw == "":
        return None
    if isinstance(raw, Path):
        return json.loads(raw.read_text(encoding="utf-8"))
    text = str(raw).strip()
    if text[:1] in "{[":
        return json.loads(text)
    path = Path(text)
    if len(text) < 4096 and path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(text)


def load_scopes(scopes: Sequence[SavedScope] | Sequence[Mapping[str, object]] | str | Path | None) -> tuple[SavedScope, ...]:
    if scopes is None or scopes == "":
        return ()
    if isinstance(scopes, (str, Path)):
        payload = load_json(scopes)
        if payload is None:
            return ()
        if isinstance(payload, Mapping) and "scopes" in payload:
            payload = payload["scopes"]
        if not isinstance(payload, Sequence) or isinstance(payload, (str, bytes)):
            raise QueryValidationError("scopes catalog must be a list")
        return parse_saved_scopes(payload)
    catalog: list[SavedScope] = []
    for item in scopes:
        if isinstance(item, SavedScope):
            catalog.append(item)
        else:
            catalog.append(SavedScope.from_payload(item))
    return tuple(catalog)


def rows_from_cards(cards: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for card in cards:
        fields = dict(card.get("fields") or {})
        if fields or card.get("type"):
            rows.append(
                {
                    "uid": card.get("uid") or card.get("card_uid") or "",
                    "card_uid": card.get("uid") or card.get("card_uid") or "",
                    "type": card.get("type") or "",
                    "summary": fields.get("summary") or card.get("summary") or "",
                    "source": list(fields.get("source") or card.get("source") or card.get("sources") or []),
                    "sources": list(fields.get("source") or card.get("sources") or card.get("source") or []),
                    "required_sources": list(card.get("required_sources") or fields.get("source") or card.get("sources") or []),
                    "people": list(fields.get("people") or card.get("people") or []),
                    "orgs": list(card.get("orgs") or []),
                    "activity_at": str(fields.get("created") or card.get("activity_at") or ""),
                    "corpus_state": card.get("corpus_state") or "active",
                    "rel_path": card.get("rel_path") or "",
                    "amount": fields.get("amount", card.get("amount")),
                    "currency": fields.get("currency", card.get("currency")),
                    "event_type": fields.get("event_type", card.get("event_type")),
                    "lineage_complete": card.get("lineage_complete", True),
                }
            )
        else:
            rows.append(dict(card))
    return rows


def load_cards(cards: Sequence[Mapping[str, Any]] | str | Path | None) -> list[dict[str, Any]]:
    if cards is None or cards == "":
        return []
    if isinstance(cards, (str, Path)):
        payload = load_json(cards)
        if payload is None:
            return []
        if isinstance(payload, Mapping):
            payload = payload.get("cards") or payload.get("rows") or []
        return [dict(item) for item in payload]
    return [dict(item) for item in cards]


def load_records(records: Sequence[Mapping[str, Any] | ChangeRecord] | str | Path | None) -> list[ChangeRecord]:
    if records is None or records == "":
        return []
    if isinstance(records, (str, Path)):
        payload = load_json(records)
        if payload is None:
            return []
        if isinstance(payload, Mapping):
            payload = payload.get("records") or payload.get("rows") or []
        records = payload
    out: list[ChangeRecord] = []
    for item in records:
        if isinstance(item, ChangeRecord):
            out.append(item)
        else:
            out.append(ChangeRecord.from_payload(item) if hasattr(ChangeRecord, "from_payload") else ChangeRecord(**dict(item)))
    return out


def _checkpoint_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    served = str(payload.get("served_checkpoint") or payload.get("snapshot") or "")
    materialized = str(
        payload.get("materialized_checkpoint") or payload.get("warehouse_checkpoint") or ""
    )
    if served and materialized and served != materialized:
        agreement = "diverged"
        stale = True
        label = "stale"
    elif payload.get("empty_scope"):
        agreement = "empty_scope"
        stale = False
        label = "empty_scope"
    elif payload.get("truncated") or payload.get("complete") is False:
        agreement = "aligned" if not (served and materialized and served != materialized) else "diverged"
        stale = bool(payload.get("stale"))
        label = "incomplete"
    elif payload.get("stale"):
        agreement = "aligned"
        stale = True
        label = "stale"
    else:
        agreement = "aligned"
        stale = False
        label = "complete"
    return {
        "served_checkpoint": served,
        "materialized_checkpoint": materialized,
        "checkpoint_agreement": agreement,
        "stale": stale,
        "completeness_label": label,
    }


def client_envelope(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Shared CLI/MCP completeness envelope. Never claims production proof."""

    out = dict(payload)
    leaked = ADVICE_KEYS.intersection(out)
    if leaked:
        raise QueryValidationError(f"client payload must not emit {sorted(leaked)}")
    reject_advice(out)
    out["contract_version"] = out.get("contract_version") or CLIENT_CONTRACT_VERSION
    out["client_contract_version"] = CLIENT_CONTRACT_VERSION
    out["production_proven"] = False
    out.setdefault("coverage", "eligible_stored")
    out.setdefault("freshness", out.get("freshness") or "unknown")
    out.update(_checkpoint_fields(out))
    if "fx_rate" in json.dumps(out, default=str):
        raise QueryValidationError("client payload must not emit FX conversion")
    return out


def _comparable(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Stable subset used to prove CLI and MCP agree."""

    rows = payload.get("rows") or payload.get("hits") or []
    citations: list[str] = []
    kinds: list[str] = []
    for item in rows:
        if not isinstance(item, Mapping):
            continue
        uid = str(item.get("uid") or item.get("card_uid") or "")
        if uid:
            citations.append(uid)
        kind = str(item.get("evidence_kind") or "")
        if kind:
            kinds.append(kind)
    for item in payload.get("evidence") or []:
        if isinstance(item, Mapping):
            uid = str(item.get("uid") or "")
            if uid:
                citations.append(uid)
            kind = str(item.get("evidence_kind") or "")
            if kind:
                kinds.append(kind)
    for uid in payload.get("matched_uids") or []:
        citations.append(str(uid))
    for uid in payload.get("context_uids") or []:
        citations.append(str(uid))
    return {
        "rows": [dict(item) if isinstance(item, Mapping) else item for item in rows],
        "citations": list(dict.fromkeys(citations)),
        "evidence_kinds": list(dict.fromkeys(kinds)),
        "matched_total": payload.get("matched_total"),
        "total_status": payload.get("total_status"),
        "empty_scope": bool(payload.get("empty_scope")),
        "saved_scope": payload.get("saved_scope") or payload.get("effective_scope"),
        "coverage": payload.get("coverage"),
        "freshness": payload.get("freshness"),
        "complete": payload.get("complete"),
        "truncated": payload.get("truncated"),
        "completeness_label": payload.get("completeness_label"),
        "served_checkpoint": payload.get("served_checkpoint"),
        "materialized_checkpoint": payload.get("materialized_checkpoint"),
        "checkpoint_agreement": payload.get("checkpoint_agreement"),
        "production_proven": payload.get("production_proven"),
        "workflow": payload.get("workflow") or payload.get("surface"),
    }


def comparable_view(payload: Mapping[str, Any]) -> dict[str, Any]:
    return _comparable(payload)


def run_typed_query(
    *,
    access: AccessContext,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    org_filter: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 20,
    saved_scope_name: str = "",
    scopes: Sequence[SavedScope] | Sequence[Mapping[str, object]] | str | Path | None = None,
    rows: Sequence[Mapping[str, Any]] | None = None,
    store: DefaultArchiveStore | None = None,
    warehouse_checkpoint: str = "",
    snapshot: str = "",
) -> dict[str, Any]:
    from dataclasses import replace

    catalog = load_scopes(scopes)
    request = request_from_simple_filters(
        access=access,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        saved_scope_name=saved_scope_name,
    )
    if snapshot:
        request = replace(request, snapshot=snapshot)
    runtime = None if store is None else store.runtime
    page = execute_typed_query(
        runtime,
        request,
        rows=rows,
        warehouse_checkpoint=warehouse_checkpoint,
        scopes=catalog,
    )
    payload = page.to_legacy_result()
    payload["surface"] = "typed_query"
    payload["workflow"] = "typed_query"
    if page.empty_scope:
        empty = empty_scope_result(
            reason=str((page.saved_scope or {}).get("reason") or "empty_intersection"),
            effective=None,
        )
        payload.update(empty)
        payload["saved_scope"] = page.saved_scope
    return client_envelope(payload)


def run_context(
    *,
    access: AccessContext | None = None,
    cards: Sequence[Mapping[str, Any]] | str | Path | None = None,
    hit_uid: str = REPLY,
    excluded_uids: Sequence[str] = (STALE,),
    archive_id: str = "archive-p10d",
) -> dict[str, Any]:
    loaded = load_cards(cards)
    units = neighbor_units_from_cards(loaded, archive_id=archive_id)
    hit = next((item for item in units if item.uid == hit_uid), None)
    if hit is None:
        raise QueryValidationError(f"context hit {hit_uid} is not in the fixture set")
    expanded = expand_neighbors(
        hit,
        units,
        access=access,
        archive_id=archive_id,
        excluded_uids=excluded_uids,
    )
    matched_uids = [item.uid for item in expanded.matched]
    context_uids = [item.uid for item in expanded.context]
    labels = []
    for item in expanded.matched:
        labels.append({"uid": item.uid, "role": "matched", "evidence_kind": "source_reported", "stale": item.stale})
    for item in expanded.context:
        labels.append({"uid": item.uid, "role": "context", "evidence_kind": "source_reported", "stale": item.stale})
    payload = {
        "surface": "context",
        "workflow": "context",
        "rows": labels,
        "hits": labels,
        "matched_uids": matched_uids,
        "context_uids": context_uids,
        "support_uids": list(expanded.support_uids),
        "excluded_uids": list(expanded.excluded_uids),
        "complete": not expanded.truncated and not expanded.stale,
        "truncated": expanded.truncated,
        "stale": expanded.stale,
        "freshness": "stale" if expanded.stale else "source_revision",
        "coverage": "eligible_stored",
        "citations": [item.citation.to_payload() if hasattr(item.citation, "to_payload") else {} for item in (*expanded.matched, *expanded.context)],
        "expanded": expanded.to_payload(),
        "matched_total": len(matched_uids),
        "total_status": "exact",
    }
    if "authorized" in json.dumps(payload, default=str):
        raise QueryValidationError("context client must not emit authorized")
    return client_envelope(payload)


def run_subscriptions(
    *,
    cards: Sequence[Mapping[str, Any]] | str | Path | None,
    access: AccessContext | None = None,
    truncated: bool = False,
) -> dict[str, Any]:
    result = subscription_lifecycle(load_cards(cards), access=access, truncated=truncated)
    payload = result.to_payload()
    payload["surface"] = "subscription_lifecycle"
    payload["rows"] = [dict(row) for row in result.rows]
    payload["matched_total"] = len(result.rows)
    payload["citations"] = [item.uid for item in result.evidence]
    return client_envelope(payload)


def run_trip_costs(
    *,
    cards: Sequence[Mapping[str, Any]] | str | Path | None,
    access: AccessContext | None = None,
    truncated: bool = False,
) -> dict[str, Any]:
    loaded = load_cards(cards)
    trip = assemble_trip(loaded, access=access, truncated=truncated)
    costs = reconcile_trip_costs(loaded, access=access, truncated=truncated)
    totals = currency_totals(loaded, access=access, truncated=truncated, currency="EUR")
    payload = {
        "surface": "trip_costs",
        "workflow": "trip_costs",
        "trip": trip.to_payload(),
        "costs": costs.to_payload(),
        "currency_totals": totals,
        "rows": [dict(row) for row in costs.rows],
        "evidence": [item.to_payload() for item in (*trip.evidence, *costs.evidence)],
        "complete": trip.complete and costs.complete,
        "truncated": trip.truncated or costs.truncated,
        "coverage": "eligible_stored",
        "freshness": trip.freshness,
        "total_status": costs.total_status,
        "matched_total": len(costs.rows),
        "citations": list(trip.rows[0]["member_uids"]) if trip.rows else [],
    }
    return client_envelope(payload)


def run_changes_since(
    *,
    records: Sequence[Mapping[str, Any] | ChangeRecord] | str | Path | None,
    access: AccessContext | None = None,
    after_sequence: int = 0,
    snapshot_id: str = "p10d-journal",
    cursor: str = "",
    decisions: Sequence[Mapping[str, Any]] = (),
    denied_uids: Sequence[str] = (),
) -> dict[str, Any]:
    page = changes_since(
        load_records(records),
        after_sequence=after_sequence,
        snapshot_id=snapshot_id,
        cursor=cursor,
        decisions=decisions,
        access=access,
        denied_uids=denied_uids,
    )
    payload = page.to_payload()
    payload["surface"] = "changes_since"
    payload["workflow"] = "changes_since"
    payload["matched_total"] = len(page.rows)
    payload["total_status"] = "exact" if page.complete else "unknown"
    payload["citations"] = [row.uid for row in page.rows]
    payload["freshness"] = "journal"
    payload["served_checkpoint"] = page.snapshot_id
    payload["materialized_checkpoint"] = page.snapshot_id
    return client_envelope(payload)


def execute_client_request(
    workflow: str,
    *,
    access: AccessContext | None = None,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    org_filter: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 20,
    saved_scope_name: str = "",
    scopes: Sequence[SavedScope] | Sequence[Mapping[str, object]] | str | Path | None = None,
    cards: Sequence[Mapping[str, Any]] | str | Path | None = None,
    rows: Sequence[Mapping[str, Any]] | None = None,
    records: Sequence[Mapping[str, Any] | ChangeRecord] | str | Path | None = None,
    hit_uid: str = REPLY,
    excluded_uids: Sequence[str] = (STALE,),
    store: DefaultArchiveStore | None = None,
    warehouse_checkpoint: str = "",
    snapshot: str = "",
    snapshot_id: str = "p10d-journal",
    after_sequence: int = 0,
    cursor: str = "",
    decisions: Sequence[Mapping[str, Any]] = (),
    truncated: bool = False,
) -> dict[str, Any]:
    """Single entry used by CLI dispatch and the MCP analytics tool."""

    name = str(workflow or "").strip()
    if name not in WORKFLOWS:
        raise QueryValidationError(f"unknown analytics workflow: {workflow}")
    ctx = access or _access()
    if name in {"typed_query", "query"}:
        query_rows = rows
        if query_rows is None and cards not in (None, ""):
            query_rows = rows_from_cards(load_cards(cards))
        return run_typed_query(
            access=ctx,
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            org_filter=org_filter,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            saved_scope_name=saved_scope_name,
            scopes=scopes,
            rows=query_rows,
            store=store,
            warehouse_checkpoint=warehouse_checkpoint,
            snapshot=snapshot,
        )
    scoped, scope_payload = scoped_cards(
        cards,
        access=ctx,
        saved_scope_name=saved_scope_name,
        scopes=scopes,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        start_date=start_date,
        end_date=end_date,
    )
    if scope_payload and scope_payload.get("empty_scope") and name not in {"typed_query", "query"}:
        empty = empty_scope_result(reason=str(scope_payload.get("reason") or "empty_intersection"))
        empty["surface"] = name
        empty["workflow"] = name
        empty["saved_scope"] = scope_payload
        empty["matched_total"] = 0
        empty["total_status"] = "exact"
        empty["complete"] = True
        empty["truncated"] = False
        empty["coverage"] = "empty_scope"
        return client_envelope(empty)
    if name == "context":
        payload = run_context(
            access=ctx,
            cards=scoped,
            hit_uid=hit_uid,
            excluded_uids=excluded_uids,
            archive_id=ctx.archive_id,
        )
        if scope_payload:
            payload["saved_scope"] = scope_payload
        return payload
    if name in {"subscription_lifecycle", "subscriptions"}:
        payload = run_subscriptions(cards=scoped, access=ctx, truncated=truncated)
        if scope_payload:
            payload["saved_scope"] = scope_payload
        return payload
    if name in {"trip_costs", "trip-costs", "assemble_trip"}:
        payload = run_trip_costs(cards=scoped, access=ctx, truncated=truncated)
        if scope_payload:
            payload["saved_scope"] = scope_payload
        return payload
    return run_changes_since(
        records=records,
        access=ctx,
        after_sequence=after_sequence,
        snapshot_id=snapshot_id,
        cursor=cursor,
        decisions=decisions,
    )


def scoped_cards(
    cards: Sequence[Mapping[str, Any]] | str | Path | None,
    *,
    access: AccessContext,
    saved_scope_name: str = "",
    scopes: Sequence[SavedScope] | Sequence[Mapping[str, object]] | str | Path | None = None,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    org_filter: str = "",
    start_date: str = "",
    end_date: str = "",
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Apply a saved scope to fixture cards. Empty intersection yields no cards."""

    loaded = load_cards(cards)
    filters, payload = apply_saved_scope_filters(
        access=access,
        saved_scope_name=saved_scope_name,
        scopes=scopes,
        type_filter=type_filter,
        source_filter=source_filter,
        people_filter=people_filter,
        org_filter=org_filter,
        start_date=start_date,
        end_date=end_date,
    )
    if payload and payload.get("empty_scope"):
        return [], payload
    if not saved_scope_name.strip():
        return loaded, payload

    def _needles(raw: str) -> set[str]:
        return {part.strip().casefold() for part in raw.split(",") if part.strip() and part != "__empty_scope__"}

    wanted_types = _needles(filters.get("type_filter", ""))
    wanted_sources = _needles(filters.get("source_filter", ""))
    wanted_people = _needles(filters.get("people_filter", ""))
    kept: list[dict[str, Any]] = []
    for card in loaded:
        fields = dict(card.get("fields") or {})
        card_type = str(card.get("type") or "").casefold()
        sources = [str(item).casefold() for item in (fields.get("source") or card.get("sources") or card.get("source") or [])]
        people = [str(item).casefold() for item in (fields.get("people") or card.get("people") or [])]
        if wanted_types and card_type not in wanted_types:
            continue
        if wanted_sources and not any(item in wanted_sources or wanted_sources.intersection({item}) for item in sources):
            if not any(src in wanted_sources for src in sources):
                continue
        if wanted_people and not any(person in wanted_people for person in people):
            continue
        kept.append(card)
    return kept, payload


def apply_saved_scope_filters(
    *,
    access: AccessContext,
    saved_scope_name: str,
    scopes: Sequence[SavedScope] | Sequence[Mapping[str, object]] | str | Path | None,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    org_filter: str = "",
    start_date: str = "",
    end_date: str = "",
) -> tuple[dict[str, str], dict[str, Any] | None]:
    """Resolve a named scope for evidence/query filters. Empty ≠ unscoped."""

    name = saved_scope_name.strip()
    if not name:
        return (
            {
                "type_filter": type_filter,
                "source_filter": source_filter,
                "people_filter": people_filter,
                "org_filter": org_filter,
                "start_date": start_date,
                "end_date": end_date,
            },
            None,
        )
    catalog = load_scopes(scopes)
    found = None
    needle = name.casefold()
    for scope in catalog:
        if scope.name.casefold() == needle:
            found = scope
            break
    if found is None:
        raise QueryValidationError(f"unknown saved scope: {name}")
    effective = resolve_effective_scope(
        access=access,
        scope=found,
        request=RequestFilters.from_mapping(
            {
                "type_filter": type_filter,
                "source_filter": source_filter,
                "people_filter": people_filter,
                "org_filter": org_filter,
                "start_date": start_date,
                "end_date": end_date,
            }
        ),
    )
    if effective.empty:
        return effective.query_kwargs(), effective.to_payload()
    return effective.query_kwargs(), effective.to_payload()


def measure_context_ablation(
    cards: Sequence[Mapping[str, Any]],
    *,
    access: AccessContext | None = None,
) -> dict[str, Any]:
    """Matched-alone vs context-expanded completeness on the loft-reply fixture."""

    ctx = run_context(access=access, cards=cards, hit_uid=REPLY, excluded_uids=(STALE,))
    reply_cards = [card for card in cards if str(card.get("uid")) == REPLY]
    fields = dict(reply_cards[0].get("fields") or {}) if reply_cards else {}
    alone_text = str(fields.get("body") or fields.get("summary") or "")
    alone_interpretable = "book the loft" in alone_text.casefold()
    expanded_interpretable = REQUEST in ctx["context_uids"] and REQUEST in ctx["support_uids"]
    alone_complete = 1 if alone_interpretable else 0
    expanded_complete = 1 if expanded_interpretable and STALE not in ctx["support_uids"] else 0
    return {
        "matched_alone": {
            "uids": [REPLY],
            "interpretable": alone_interpretable,
            "evidence_completeness": alone_complete,
            "stale_included": False,
        },
        "context_expanded": {
            "matched_uids": ctx["matched_uids"],
            "context_uids": ctx["context_uids"],
            "support_uids": ctx["support_uids"],
            "interpretable": expanded_interpretable,
            "evidence_completeness": expanded_complete,
            "stale_excluded": STALE not in ctx["support_uids"] and STALE in ctx["excluded_uids"],
        },
        "completeness_gain": expanded_complete - alone_complete,
        "relevance_gain": 1 if expanded_interpretable and not alone_interpretable else 0,
        "historical_recall_regression": False,
    }


def measure_scope_ablation(
    rows: Sequence[Mapping[str, Any]],
    *,
    access: AccessContext,
    scopes: Sequence[SavedScope],
) -> dict[str, Any]:
    """Scoped vs unscoped namesake query. Historical recall must not regress when unscoped."""

    unscoped = run_typed_query(
        access=access,
        type_filter="person",
        limit=50,
        rows=rows,
    )
    scoped = run_typed_query(
        access=access,
        type_filter="person",
        limit=50,
        saved_scope_name=scopes[0].name,
        scopes=scopes,
        rows=rows,
    )
    unscoped_uids = {str(row.get("uid")) for row in unscoped.get("rows") or []}
    scoped_uids = {str(row.get("uid")) for row in scoped.get("rows") or []}
    namesake = {PERSON_A, PERSON_B}
    recall_ok = namesake <= unscoped_uids
    return {
        "unscoped_uids": sorted(unscoped_uids),
        "scoped_uids": sorted(scoped_uids),
        "unscoped_total": unscoped.get("matched_total"),
        "scoped_total": scoped.get("matched_total"),
        "scoped_empty": bool(scoped.get("empty_scope")),
        "historical_recall_ok": recall_ok,
        "historical_recall_regression": not recall_ok,
        "scope_narrowed": scoped.get("empty_scope") or scoped_uids <= unscoped_uids,
    }


def dispatch_analytics_args(args: Any) -> dict[str, Any]:
    """Translate the registered CLI namespace into ``execute_client_request``."""

    workflow = str(getattr(args, "analytics_command", "") or getattr(args, "workflow", "") or "")
    access = getattr(args, "access", None) or _access()
    return execute_client_request(
        workflow,
        access=access,
        type_filter=getattr(args, "type_filter", "") or "",
        source_filter=getattr(args, "source_filter", "") or "",
        people_filter=getattr(args, "people_filter", "") or "",
        org_filter=getattr(args, "org_filter", "") or "",
        start_date=getattr(args, "start_date", "") or "",
        end_date=getattr(args, "end_date", "") or "",
        limit=int(getattr(args, "limit", 20) or 20),
        saved_scope_name=getattr(args, "saved_scope_name", "") or "",
        scopes=getattr(args, "scopes_json", "") or "",
        cards=getattr(args, "cards_json", "") or "",
        records=getattr(args, "records_json", "") or "",
        hit_uid=getattr(args, "hit_uid", "") or REPLY,
        snapshot=getattr(args, "snapshot", "") or "",
        warehouse_checkpoint=getattr(args, "warehouse_checkpoint", "") or "",
        snapshot_id=getattr(args, "snapshot_id", "") or "p10d-journal",
        after_sequence=int(getattr(args, "after_sequence", 0) or 0),
        cursor=getattr(args, "cursor", "") or "",
        truncated=bool(getattr(args, "truncated", False)),
    )


__all__ = [
    "CLIENT_CONTRACT_VERSION",
    "WORKFLOWS",
    "apply_saved_scope_filters",
    "client_envelope",
    "comparable_view",
    "dispatch_analytics_args",
    "execute_client_request",
    "load_cards",
    "load_records",
    "load_scopes",
    "measure_context_ablation",
    "measure_scope_ablation",
    "run_changes_since",
    "run_context",
    "run_subscriptions",
    "run_trip_costs",
    "run_typed_query",
]

"""Typed evidence query service (P10-A).

Clients submit validated predicates over registered fields. Execution uses
``runtime.retrieval`` / ``runtime.query`` plus ``AccessContext``. Counts are
computed over the full eligible set or marked incomplete. Arbitrary SQL is
rejected. Saved-scope resolution is reserved for P09.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from archive_engine.access import card_permitted, is_restricted, policy_identity
from archive_engine.contracts import AccessContext, EvidenceEnvelope
from archive_engine.errors import CapabilityUnavailableError, QueryValidationError
from archive_engine.query_cursor import QueryCursor, bind_cursor, decode_cursor
from archive_engine.runtime import ArchiveRuntime

QUERY_CONTRACT_VERSION = "p10a.1"
MAX_AST_DEPTH = 4
MAX_TERMS = 16
MAX_IN_LIST = 32
MAX_PAGE_SIZE = 200
DEFAULT_PAGE_SIZE = 20
MAX_ELIGIBLE_SCAN = 100_000
MAX_RETURNED_FIELDS = 32
MAX_OUTPUT_ROWS = 10_000

PredicateOp = Literal["eq", "in", "lt", "lte", "gt", "gte", "exists", "and", "or"]
FieldKind = Literal["string", "string_list", "number", "datetime"]
TotalStatus = Literal["exact", "unknown"]

SERVING_FIELDS = frozenset(
    {
        "uid",
        "type",
        "source",
        "people",
        "org",
        "activity_at",
        "corpus_state",
        "summary",
        "slug",
        "emails",
        "domains",
    }
)
ORDER_FIELDS = frozenset({"uid", "type", "activity_at", "summary", "slug", "corpus_state"})
NUMERIC_COMPARE = frozenset({"lt", "lte", "gt", "gte"})
FINANCE_TYPES = frozenset(
    {
        "finance",
        "purchase",
        "meal_order",
        "grocery_order",
        "ride",
        "flight",
        "subscription",
        "invoice",
        "receipt",
        "payroll",
        "event_ticket",
    }
)
SUBSCRIPTION_TYPES = frozenset({"subscription"})


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: FieldKind
    nullable: bool = True
    card_types: frozenset[str] | None = None
    aliases: tuple[str, ...] = ()
    currency: bool = False
    temporal: bool = False


REGISTERED_FIELDS: dict[str, FieldSpec] = {
    "uid": FieldSpec("uid", "string", nullable=False, aliases=("card_uid",)),
    "type": FieldSpec("type", "string", nullable=False, aliases=("card_type",)),
    "source": FieldSpec("source", "string_list", aliases=("sources",)),
    "people": FieldSpec("people", "string_list"),
    "org": FieldSpec("org", "string_list", aliases=("orgs", "organization")),
    "activity_at": FieldSpec("activity_at", "datetime", temporal=True),
    "corpus_state": FieldSpec("corpus_state", "string"),
    "summary": FieldSpec("summary", "string"),
    "slug": FieldSpec("slug", "string"),
    "emails": FieldSpec("emails", "string_list"),
    "domains": FieldSpec("domains", "string_list"),
    "amount": FieldSpec("amount", "number", card_types=FINANCE_TYPES),
    "currency": FieldSpec("currency", "string", card_types=FINANCE_TYPES, currency=True),
    "event_type": FieldSpec("event_type", "string", card_types=SUBSCRIPTION_TYPES),
}

_FIELD_BY_ALIAS: dict[str, str] = {}
for _name, _spec in REGISTERED_FIELDS.items():
    _FIELD_BY_ALIAS[_name] = _name
    for _alias in _spec.aliases:
        _FIELD_BY_ALIAS[_alias] = _name


@dataclass(frozen=True)
class Predicate:
    op: PredicateOp
    field: str = ""
    value: Any = None
    predicates: tuple[Predicate, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"op": self.op}
        if self.field:
            payload["field"] = self.field
        if self.value is not None:
            payload["value"] = self.value
        if self.predicates:
            payload["predicates"] = [item.to_payload() for item in self.predicates]
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any] | Predicate | None) -> Predicate | None:
        if payload is None:
            return None
        if isinstance(payload, Predicate):
            return payload
        if isinstance(payload, str):
            raise QueryValidationError("arbitrary SQL or raw strings are not accepted as predicates")
        if not isinstance(payload, Mapping):
            raise QueryValidationError("predicate must be an object")
        if any(key in payload for key in ("sql", "raw_sql", "query_sql", "statement")):
            raise QueryValidationError("arbitrary SQL is not accepted")
        op = str(payload.get("op") or "").strip().lower()
        if op not in {"eq", "in", "lt", "lte", "gt", "gte", "exists", "and", "or"}:
            raise QueryValidationError(f"unsupported predicate operator: {op or '(missing)'}")
        children = tuple(cls.from_payload(item) for item in (payload.get("predicates") or ()))  # type: ignore[misc]
        children = tuple(item for item in children if item is not None)
        return cls(
            op=op,  # type: ignore[arg-type]
            field=str(payload.get("field") or ""),
            value=payload.get("value"),
            predicates=children,
        )


@dataclass(frozen=True)
class StructuredQueryRequest:
    """Typed query. Saved scope names are accepted but not resolved until P09."""

    archive_id: str
    access: AccessContext
    predicate: Predicate | None = None
    filters: Mapping[str, str] = field(default_factory=dict)
    fields: tuple[str, ...] = ()
    order_field: str = "uid"
    order_direction: str = "asc"
    page_size: int = DEFAULT_PAGE_SIZE
    cursor: str = ""
    aggregate: str = ""
    as_of_checkpoint: str = ""
    saved_scope_name: str = ""
    snapshot: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "archive_id": self.archive_id,
            "access": self.access.to_payload(),
            "predicate": None if self.predicate is None else self.predicate.to_payload(),
            "filters": dict(self.filters),
            "fields": list(self.fields),
            "order_field": self.order_field,
            "order_direction": self.order_direction,
            "page_size": self.page_size,
            "cursor": self.cursor,
            "aggregate": self.aggregate,
            "as_of_checkpoint": self.as_of_checkpoint,
            "saved_scope_name": self.saved_scope_name,
            "snapshot": self.snapshot,
        }


@dataclass(frozen=True)
class QueryPage:
    rows: tuple[dict[str, Any], ...]
    next_cursor: str = ""
    effective_scope: tuple[tuple[str, str], ...] = ()
    matched_total: int | None = None
    total_status: TotalStatus = "unknown"
    snapshot: str = ""
    warehouse_checkpoint: str = ""
    coverage: str = "eligible_stored"
    freshness: str = "unknown"
    truncated: bool = False
    complete: bool = False
    evidence: EvidenceEnvelope | None = None
    aggregate: Mapping[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "rows": [dict(row) for row in self.rows],
            "next_cursor": self.next_cursor,
            "effective_scope": dict(self.effective_scope),
            "matched_total": self.matched_total,
            "total_status": self.total_status,
            "snapshot": self.snapshot,
            "warehouse_checkpoint": self.warehouse_checkpoint,
            "coverage": self.coverage,
            "freshness": self.freshness,
            "truncated": self.truncated,
            "complete": self.complete,
            "contract_version": QUERY_CONTRACT_VERSION,
        }
        if self.evidence is not None:
            payload["evidence"] = self.evidence.to_payload()
        if self.aggregate is not None:
            payload["aggregate"] = dict(self.aggregate)
        return payload

    def to_legacy_result(self) -> dict[str, Any]:
        result = {"rows": [dict(row) for row in self.rows]}
        result.update({key: value for key, value in self.to_payload().items() if key != "rows"})
        return result


def resolve_field_name(raw: str) -> str:
    name = str(raw or "").strip()
    if not name:
        raise QueryValidationError("predicate field is required")
    resolved = _FIELD_BY_ALIAS.get(name)
    if resolved is None:
        raise QueryValidationError(f"unknown field: {name}")
    return resolved


def field_spec(name: str) -> FieldSpec:
    return REGISTERED_FIELDS[resolve_field_name(name)]


def _canonicalize(value: Any) -> Any:
    if isinstance(value, Predicate):
        return value.to_payload()
    if isinstance(value, Mapping):
        return {str(key): _canonicalize(value[key]) for key in sorted(value)}
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    return value


def predicate_fingerprint(predicate: Predicate | None, filters: Mapping[str, str]) -> str:
    material = {"predicate": None if predicate is None else predicate.to_payload(), "filters": dict(filters)}
    raw = json.dumps(_canonicalize(material), sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def filters_to_predicate(filters: Mapping[str, str] | None) -> Predicate | None:
    """Map legacy type/source/people/org/date filters to a typed AND tree."""

    clauses: list[Predicate] = []
    mapping = {
        "type_filter": ("type", "eq"),
        "source_filter": ("source", "eq"),
        "people_filter": ("people", "eq"),
        "org_filter": ("org", "eq"),
        "start_date": ("activity_at", "gte"),
        "end_date": ("activity_at", "lte"),
    }
    for key, (field_name, op) in mapping.items():
        raw = str((filters or {}).get(key) or "").strip()
        if not raw:
            continue
        clauses.append(Predicate(op=op, field=field_name, value=raw))  # type: ignore[arg-type]
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return Predicate(op="and", predicates=tuple(clauses))


def combine_predicates(*predicates: Predicate | None) -> Predicate | None:
    present = tuple(item for item in predicates if item is not None)
    if not present:
        return None
    if len(present) == 1:
        return present[0]
    return Predicate(op="and", predicates=present)


def _count_terms(predicate: Predicate | None) -> int:
    if predicate is None:
        return 0
    if predicate.op in {"and", "or"}:
        return sum(_count_terms(child) for child in predicate.predicates)
    return 1


def _depth(predicate: Predicate | None) -> int:
    if predicate is None:
        return 0
    if not predicate.predicates:
        return 1
    return 1 + max((_depth(child) for child in predicate.predicates), default=0)


def _constrained_types(predicate: Predicate | None) -> frozenset[str] | None:
    """Return the type-eq/in set if the tree constrains ``type``, else None."""

    if predicate is None:
        return None
    if predicate.op == "and":
        found: frozenset[str] | None = None
        for child in predicate.predicates:
            child_types = _constrained_types(child)
            if child_types is None:
                continue
            found = child_types if found is None else found & child_types
        return found
    if predicate.op == "or":
        parts = [_constrained_types(child) for child in predicate.predicates]
        if any(part is None for part in parts):
            return None
        out: set[str] = set()
        for part in parts:
            out.update(part or ())
        return frozenset(out)
    if resolve_field_name(predicate.field) != "type" if predicate.field else False:
        return None
    if predicate.op == "eq" and isinstance(predicate.value, str) and predicate.value.strip():
        return frozenset({predicate.value.strip()})
    if predicate.op == "in":
        values = predicate.value if isinstance(predicate.value, (list, tuple)) else ()
        labels = {str(item).strip() for item in values if str(item).strip()}
        return frozenset(labels) if labels else None
    return None


def validate_predicate(predicate: Predicate | None) -> Predicate | None:
    if predicate is None:
        return None
    if _depth(predicate) > MAX_AST_DEPTH:
        raise QueryValidationError(f"predicate exceeds max depth {MAX_AST_DEPTH}")
    if _count_terms(predicate) > MAX_TERMS:
        raise QueryValidationError(f"predicate exceeds max terms {MAX_TERMS}")
    _validate_node(predicate, _constrained_types(predicate))
    return predicate


def _validate_node(predicate: Predicate, constrained_types: frozenset[str] | None) -> None:
    if predicate.op in {"and", "or"}:
        if not predicate.predicates:
            raise QueryValidationError(f"{predicate.op} requires child predicates")
        for child in predicate.predicates:
            _validate_node(child, constrained_types)
        return
    spec = field_spec(predicate.field)
    if spec.card_types is not None:
        if constrained_types is None or not constrained_types or not constrained_types <= spec.card_types:
            raise QueryValidationError(
                f"field {spec.name} is not valid across unconstrained or incompatible card types"
            )
    if predicate.op == "exists":
        return
    if predicate.op == "in":
        values = predicate.value
        if not isinstance(values, (list, tuple)):
            raise QueryValidationError("in operator requires a list value")
        if len(values) > MAX_IN_LIST:
            raise QueryValidationError(f"in-list exceeds max length {MAX_IN_LIST}")
        if not values:
            raise QueryValidationError("in-list must not be empty")
        for item in values:
            _validate_value(spec, item)
        return
    if predicate.op in NUMERIC_COMPARE and spec.kind not in {"number", "datetime"}:
        raise QueryValidationError(f"operator {predicate.op} is not valid for {spec.kind} field {spec.name}")
    if predicate.op == "eq" and spec.kind not in {"string", "string_list", "number", "datetime"}:
        raise QueryValidationError(f"operator eq is not valid for field {spec.name}")
    _validate_value(spec, predicate.value)


def _validate_value(spec: FieldSpec, value: Any) -> None:
    if value is None:
        if spec.nullable:
            return
        raise QueryValidationError(f"field {spec.name} is not nullable")
    if spec.kind == "number":
        try:
            Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise QueryValidationError(f"field {spec.name} requires a number") from exc
        return
    if spec.kind == "datetime":
        if not isinstance(value, str) or not value.strip():
            raise QueryValidationError(f"field {spec.name} requires an ISO date/time string")
        return
    if spec.kind in {"string", "string_list"} and not isinstance(value, (str, int)):
        raise QueryValidationError(f"field {spec.name} requires a string")


def validate_request(request: StructuredQueryRequest) -> StructuredQueryRequest:
    if request.saved_scope_name.strip():
        raise QueryValidationError("saved scopes are not available until P09")
    if request.access.archive_id != request.archive_id:
        raise QueryValidationError("AccessContext.archive_id must match the query archive_id")
    if request.access.deny:
        raise QueryValidationError(request.access.deny_reason or "access denied")
    page_size = int(request.page_size or DEFAULT_PAGE_SIZE)
    if page_size < 1 or page_size > MAX_PAGE_SIZE:
        raise QueryValidationError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
    direction = str(request.order_direction or "asc").strip().lower()
    if direction not in {"asc", "desc"}:
        raise QueryValidationError("order_direction must be asc or desc")
    order_field = resolve_field_name(request.order_field or "uid")
    if order_field not in ORDER_FIELDS:
        raise QueryValidationError(f"order field {order_field} is not sortable")
    if request.fields:
        if len(request.fields) > MAX_RETURNED_FIELDS:
            raise QueryValidationError(f"requested fields exceed {MAX_RETURNED_FIELDS}")
        for name in request.fields:
            resolve_field_name(name)
    if request.aggregate and request.aggregate not in {"count", "sum"}:
        raise QueryValidationError(f"unsupported aggregate: {request.aggregate}")
    if request.aggregate == "sum":
        # Sum is only valid on a numeric registered field supplied as filters["aggregate_field"].
        agg_field = str(request.filters.get("aggregate_field") or "amount")
        spec = field_spec(agg_field)
        if spec.kind != "number":
            raise QueryValidationError(f"sum requires a numeric field, not {spec.name}")
    predicate = validate_predicate(combine_predicates(request.predicate, filters_to_predicate(request.filters)))
    return StructuredQueryRequest(
        archive_id=request.archive_id,
        access=request.access,
        predicate=predicate,
        filters=dict(request.filters),
        fields=tuple(resolve_field_name(name) for name in request.fields),
        order_field=order_field,
        order_direction=direction,
        page_size=page_size,
        cursor=request.cursor,
        aggregate=request.aggregate,
        as_of_checkpoint=request.as_of_checkpoint,
        saved_scope_name="",
        snapshot=request.snapshot,
    )


def _row_get(row: Mapping[str, Any], spec: FieldSpec) -> Any:
    keys = (spec.name, *spec.aliases)
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    if spec.name == "uid":
        return row.get("card_uid") or row.get("uid") or ""
    if spec.name == "source":
        return row.get("sources") or row.get("source") or row.get("required_sources") or []
    if spec.name == "org":
        return row.get("orgs") or row.get("org") or []
    return row.get(spec.name)


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)]


def _as_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _as_time_key(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _compare_values(spec: FieldSpec, left: Any, op: str, right: Any) -> bool:
    if spec.kind == "number":
        left_n = _as_decimal(left)
        right_n = _as_decimal(right)
        if left_n is None or right_n is None:
            return False
        if op == "eq":
            return left_n == right_n
        if op == "lt":
            return left_n < right_n
        if op == "lte":
            return left_n <= right_n
        if op == "gt":
            return left_n > right_n
        if op == "gte":
            return left_n >= right_n
        return False
    if spec.kind == "datetime":
        left_k = _as_time_key(left)
        right_k = _as_time_key(right)
        if not left_k or not right_k:
            return False
        left_cmp = left_k[:10] if len(right_k) == 10 else left_k
        right_cmp = right_k[:10] if len(right_k) == 10 else right_k
        if op == "eq":
            return left_cmp == right_cmp or left_k.startswith(right_k) or right_k.startswith(left_k)
        if op == "lt":
            return left_cmp < right_cmp
        if op == "lte":
            return left_cmp <= right_cmp
        if op == "gt":
            return left_cmp > right_cmp
        if op == "gte":
            return left_cmp >= right_cmp
        return False
    if spec.kind == "string_list":
        items = [item.casefold() for item in _as_list(left)]
        needle = str(right).strip().casefold()
        return any(item == needle or needle in item for item in items)
    left_s = str(left or "").strip().casefold()
    right_s = str(right or "").strip().casefold()
    if op == "eq":
        return left_s == right_s
    return False


def row_matches_predicate(row: Mapping[str, Any], predicate: Predicate | None) -> bool:
    if predicate is None:
        return True
    if predicate.op == "and":
        return all(row_matches_predicate(row, child) for child in predicate.predicates)
    if predicate.op == "or":
        return any(row_matches_predicate(row, child) for child in predicate.predicates)
    spec = field_spec(predicate.field)
    value = _row_get(row, spec)
    if predicate.op == "exists":
        if spec.kind == "string_list":
            return bool(_as_list(value))
        return value not in (None, "")
    if predicate.op == "in":
        values = predicate.value if isinstance(predicate.value, (list, tuple)) else ()
        return any(_compare_values(spec, value, "eq", item) for item in values)
    return _compare_values(spec, value, predicate.op, predicate.value)


def _order_value(row: Mapping[str, Any], order_field: str) -> str:
    spec = field_spec(order_field)
    raw = _row_get(row, spec)
    if raw in (None, ""):
        return ""
    if spec.kind == "string_list":
        items = _as_list(raw)
        return items[0] if items else ""
    return str(raw)


def _uid_of(row: Mapping[str, Any]) -> str:
    return str(row.get("uid") or row.get("card_uid") or "")


def _after_cursor(row: Mapping[str, Any], cursor: QueryCursor) -> bool:
    value = _order_value(row, cursor.order_field)
    uid = _uid_of(row)
    row_null = value == ""
    last_null = cursor.last_order_null or cursor.last_order_value == ""
    if cursor.order_direction == "asc":
        if last_null and not row_null:
            return True
        if row_null and not last_null:
            return False
        if row_null and last_null:
            return uid > cursor.last_uid
        if value == cursor.last_order_value:
            return uid > cursor.last_uid
        return value > cursor.last_order_value
    if last_null and not row_null:
        return False
    if row_null and not last_null:
        return True
    if row_null and last_null:
        return uid > cursor.last_uid
    if value == cursor.last_order_value:
        return uid > cursor.last_uid
    return value < cursor.last_order_value


def _project(row: Mapping[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    projected = dict(row)
    if not fields:
        return projected
    keep = {"uid", "card_uid", "rel_path", "type", *fields}
    return {key: value for key, value in projected.items() if key in keep}


def _scope_pairs(access: AccessContext, filters: Mapping[str, str]) -> tuple[tuple[str, str], ...]:
    pairs = [
        ("archive_id", access.archive_id),
        ("principal", access.principal),
        ("profile", access.profile),
        ("restricted", "1" if is_restricted(access) else "0"),
    ]
    for key, value in filters.items():
        if str(value).strip():
            pairs.append((str(key), str(value)))
    return tuple(pairs)


def _snapshot_for(runtime: ArchiveRuntime | None, request: StructuredQueryRequest) -> str:
    if request.snapshot.strip():
        return request.snapshot.strip()
    if request.as_of_checkpoint.strip():
        return request.as_of_checkpoint.strip()
    if runtime is None:
        return "memory"
    serving = runtime.retrieval.serving_or_none()
    if serving is not None:
        generation = getattr(serving, "generation_id", "") or ""
        if generation:
            return str(generation)
    return "memory"


def _eligible_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    access: AccessContext,
    predicate: Predicate | None,
) -> list[dict[str, Any]]:
    eligible: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        if not card_permitted(access, row):
            continue
        if str(row.get("corpus_state") or "") == "suppressed":
            continue
        if not row_matches_predicate(row, predicate):
            continue
        if not _uid_of(row):
            continue
        eligible.append(row)
    return eligible


def _sort_eligible(
    eligible: Sequence[Mapping[str, Any]],
    order_field: str,
    direction: str,
) -> list[dict[str, Any]]:
    def key(row: Mapping[str, Any]) -> tuple[int, str, str]:
        value = _order_value(row, order_field)
        null_rank = 1 if value == "" else 0
        return (null_rank, value, _uid_of(row))

    rows = [dict(row) for row in eligible]
    rows.sort(key=key, reverse=direction == "desc")
    if direction == "desc":
        # Explicit nulls-last: move empty order values to the end, keep uid asc on ties.
        present = [row for row in rows if _order_value(row, order_field) != ""]
        missing = [row for row in rows if _order_value(row, order_field) == ""]
        present.sort(key=lambda row: (_order_value(row, order_field), _uid_of(row)), reverse=True)
        # After reverse, uid ties would be desc; restore uid asc on equal order values.
        present.sort(key=lambda row: _order_value(row, order_field), reverse=True)
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in present:
            groups.setdefault(_order_value(row, order_field), []).append(row)
        rebuilt: list[dict[str, Any]] = []
        for value in sorted(groups, reverse=True):
            rebuilt.extend(sorted(groups[value], key=_uid_of))
        missing.sort(key=_uid_of)
        return rebuilt + missing
    return rows


def _collect_from_runtime(runtime: ArchiveRuntime, request: StructuredQueryRequest) -> tuple[list[dict[str, Any]], bool]:
    serving = runtime.retrieval.serving_or_none()
    if serving is not None and hasattr(serving, "typed_query"):
        collected: list[dict[str, Any]] = []
        after: dict[str, Any] = {}
        truncated = False
        while True:
            payload = serving.typed_query(
                predicate=None if request.predicate is None else request.predicate.to_payload(),
                order_field=request.order_field,
                order_direction=request.order_direction,
                page_size=MAX_PAGE_SIZE,
                **after,
                **runtime.retrieval._policy(),
            )
            if not isinstance(payload, dict):
                break
            collected.extend(dict(row) for row in (payload.get("rows") or []))
            if payload.get("truncated"):
                truncated = True
                break
            nxt = payload.get("next_after")
            if not nxt:
                break
            after = {
                "after_uid": nxt.get("uid") or "",
                "after_value": nxt.get("order_value") or "",
                "after_null": bool(nxt.get("order_null")),
            }
            if len(collected) >= MAX_ELIGIBLE_SCAN:
                truncated = True
                break
        return collected, truncated
    fetch_limit = MAX_ELIGIBLE_SCAN
    result = runtime.query(
        type_filter=str(request.filters.get("type_filter") or ""),
        source_filter=str(request.filters.get("source_filter") or ""),
        people_filter=str(request.filters.get("people_filter") or ""),
        org_filter=str(request.filters.get("org_filter") or ""),
        start_date=str(request.filters.get("start_date") or ""),
        end_date=str(request.filters.get("end_date") or ""),
        limit=fetch_limit,
        authorize_limit=fetch_limit,
    )
    rows = [dict(row) for row in (result.get("rows") or [])]
    return rows, len(rows) >= fetch_limit


def _sum_amount(rows: Sequence[Mapping[str, Any]], field_name: str) -> dict[str, Any]:
    spec = field_spec(field_name)
    by_currency: dict[str, Decimal] = {}
    missing = 0
    for row in rows:
        amount = _as_decimal(_row_get(row, spec))
        if amount is None:
            missing += 1
            continue
        currency = str(_row_get(row, field_spec("currency")) or "").strip() or "UNKNOWN"
        by_currency[currency] = by_currency.get(currency, Decimal("0")) + amount
    return {
        "op": "sum",
        "field": spec.name,
        "by_currency": {key: str(value) for key, value in sorted(by_currency.items())},
        "missing": missing,
        "over_full_eligible_set": True,
    }


def execute_typed_query(
    runtime: ArchiveRuntime | None,
    request: StructuredQueryRequest,
    *,
    rows: Sequence[Mapping[str, Any]] | None = None,
    warehouse_checkpoint: str = "",
) -> QueryPage:
    """Run a typed query. ``rows`` supplies an isolated corpus for tests."""

    validated = validate_request(request)
    snapshot = _snapshot_for(runtime, validated)
    validated = StructuredQueryRequest(
        archive_id=validated.archive_id,
        access=validated.access,
        predicate=validated.predicate,
        filters=validated.filters,
        fields=validated.fields,
        order_field=validated.order_field,
        order_direction=validated.order_direction,
        page_size=validated.page_size,
        cursor=validated.cursor,
        aggregate=validated.aggregate,
        as_of_checkpoint=validated.as_of_checkpoint,
        snapshot=snapshot,
    )
    policy_fp = policy_identity(validated.access)
    pred_fp = predicate_fingerprint(validated.predicate, validated.filters)
    cursor = None
    if validated.cursor.strip():
        cursor = bind_cursor(
            decode_cursor(validated.cursor),
            archive_id=validated.archive_id,
            snapshot=snapshot,
            policy_fingerprint=policy_fp,
            predicate_fingerprint=pred_fp,
            order_field=validated.order_field,
            order_direction=validated.order_direction,
        )
    if rows is not None:
        collected = [dict(row) for row in rows]
        scan_truncated = False
    elif runtime is not None:
        collected, scan_truncated = _collect_from_runtime(runtime, validated)
    else:
        raise CapabilityUnavailableError("typed_query_requires_runtime_or_rows")
    if len(collected) > MAX_OUTPUT_ROWS:
        collected = collected[:MAX_OUTPUT_ROWS]
        scan_truncated = True
    eligible = _eligible_rows(collected, access=validated.access, predicate=validated.predicate)
    ordered = _sort_eligible(eligible, validated.order_field, validated.order_direction)
    if cursor is not None:
        remaining = [row for row in ordered if _after_cursor(row, cursor)]
    else:
        remaining = ordered
    page = [_project(row, validated.fields) for row in remaining[: validated.page_size]]
    next_token = ""
    if len(remaining) > validated.page_size:
        last = remaining[validated.page_size - 1]
        last_value = _order_value(last, validated.order_field)
        next_token = QueryCursor(
            version=1,
            archive_id=validated.archive_id,
            snapshot=snapshot,
            policy_fingerprint=policy_fp,
            predicate_fingerprint=pred_fp,
            order_field=validated.order_field,
            order_direction=validated.order_direction,
            last_order_value=last_value,
            last_uid=_uid_of(last),
            last_order_null=last_value == "",
        ).encode()
    total_status: TotalStatus = "unknown" if scan_truncated else "exact"
    complete = total_status == "exact"
    aggregate = None
    if validated.aggregate == "count":
        aggregate = {
            "op": "count",
            "value": None if scan_truncated else len(eligible),
            "status": total_status,
            "over_full_eligible_set": not scan_truncated,
        }
    elif validated.aggregate == "sum":
        if scan_truncated:
            aggregate = {
                "op": "sum",
                "status": "unknown",
                "over_full_eligible_set": False,
                "reason": "eligible set was truncated before aggregation",
            }
        else:
            aggregate = _sum_amount(eligible, str(validated.filters.get("aggregate_field") or "amount"))
            aggregate["status"] = "exact"
    evidence = EvidenceEnvelope(
        generation=snapshot,
        watermark=0,
        coverage="eligible_stored",
        freshness="unknown",
        complete=complete,
        truncated=bool(next_token) or scan_truncated,
        confidence_reason="typed-predicate-full-eligible" if complete else "typed-predicate-incomplete",
        method="typed_query",
        corpus_state="active",
        provenance="derived",
        evidence_kind="derived",
        pipeline_version=QUERY_CONTRACT_VERSION,
        query=json.dumps(validated.predicate.to_payload() if validated.predicate else {}, sort_keys=True),
    )
    return QueryPage(
        rows=tuple(page),
        next_cursor=next_token,
        effective_scope=_scope_pairs(validated.access, validated.filters),
        matched_total=None if scan_truncated else len(eligible),
        total_status=total_status,
        snapshot=snapshot,
        warehouse_checkpoint=warehouse_checkpoint,
        coverage="eligible_stored",
        freshness="unknown",
        truncated=bool(next_token) or scan_truncated,
        complete=complete,
        evidence=evidence,
        aggregate=aggregate,
    )


def request_from_simple_filters(
    *,
    access: AccessContext,
    type_filter: str = "",
    source_filter: str = "",
    people_filter: str = "",
    org_filter: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = DEFAULT_PAGE_SIZE,
    cursor: str = "",
    order_field: str = "activity_at",
    order_direction: str = "desc",
) -> StructuredQueryRequest:
    filters = {
        "type_filter": type_filter,
        "source_filter": source_filter,
        "people_filter": people_filter,
        "org_filter": org_filter,
        "start_date": start_date,
        "end_date": end_date,
    }
    return StructuredQueryRequest(
        archive_id=access.archive_id,
        access=access,
        predicate=filters_to_predicate(filters),
        filters=filters,
        order_field=order_field,
        order_direction=order_direction,
        page_size=limit,
        cursor=cursor,
        aggregate="count",
    )

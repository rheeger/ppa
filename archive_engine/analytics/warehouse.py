"""Read-only warehouse analytical adapter (P10-A).

Compiles allowlisted typed aggregates to parameterized SQL. Arbitrary SQL,
unregistered columns, and mixed snapshot+warehouse results without an
explicit checkpoint are rejected. AccessContext is applied before the
aggregate, never after a truncated page.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from archive_engine.access import is_restricted
from archive_engine.contracts import AccessContext
from archive_engine.errors import CapabilityUnavailableError, QueryValidationError
from archive_engine.query import Predicate, field_spec, validate_predicate
from archive_engine.runtime import ArchiveRuntime

ALLOWED_AGGREGATES = frozenset({"count", "sum"})
ALLOWED_TABLES: dict[str, frozenset[str]] = {
    "cards": frozenset({"uid", "type", "activity_at", "summary", "corpus_state", "rel_path"}),
    "finance": frozenset({"uid", "amount", "currency", "type"}),
    "subscriptions": frozenset({"uid", "event_type", "service_name", "type"}),
}
TABLE_FOR_FIELD = {
    "amount": "finance",
    "currency": "finance",
    "event_type": "subscriptions",
    "service_name": "subscriptions",
}
MAX_GROUP_KEYS = 4
MAX_STATEMENT_ROWS = 10_000


@dataclass(frozen=True)
class TypedAggregateSpec:
    op: Literal["count", "sum"]
    field: str = ""
    group_by: tuple[str, ...] = ()
    predicate: Predicate | None = None
    checkpoint: str = ""
    serving_snapshot: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "op": self.op,
            "field": self.field,
            "group_by": list(self.group_by),
            "predicate": None if self.predicate is None else self.predicate.to_payload(),
            "checkpoint": self.checkpoint,
            "serving_snapshot": self.serving_snapshot,
        }


@dataclass(frozen=True)
class CompiledAggregate:
    sql: str
    params: tuple[Any, ...]
    table: str
    checkpoint: str


def _reject_sql_payload(payload: Mapping[str, Any] | None) -> None:
    if not payload:
        return
    for key in ("sql", "raw_sql", "query_sql", "statement", "select"):
        if key in payload and payload[key] not in (None, ""):
            raise QueryValidationError("arbitrary SQL is not accepted")


def _column(table: str, field_name: str) -> str:
    spec = field_spec(field_name)
    name = spec.name
    if name not in ALLOWED_TABLES[table] and name not in {"uid", "type"}:
        raise QueryValidationError(f"field {name} is not available on warehouse table {table}")
    return name


def _table_for(spec: TypedAggregateSpec) -> str:
    if spec.op == "sum":
        field_name = spec.field or "amount"
        table = TABLE_FOR_FIELD.get(field_spec(field_name).name, "cards")
        if table == "cards":
            raise QueryValidationError("sum requires a numeric warehouse projection field")
        return table
    if spec.field:
        return TABLE_FOR_FIELD.get(field_spec(spec.field).name, "cards")
    return "cards"


def compile_typed_aggregate(
    spec: TypedAggregateSpec,
    *,
    access: AccessContext,
    schema: str,
) -> CompiledAggregate:
    """Compile a typed aggregate. Never interpolates user strings into SQL."""

    if spec.op not in ALLOWED_AGGREGATES:
        raise QueryValidationError(f"unsupported aggregate: {spec.op}")
    if spec.serving_snapshot and spec.checkpoint and spec.serving_snapshot != spec.checkpoint:
        raise QueryValidationError(
            "serving snapshot and warehouse checkpoint differ; refuse to combine them without reconciliation"
        )
    predicate = validate_predicate(spec.predicate)
    table = _table_for(spec)
    if table not in ALLOWED_TABLES:
        raise QueryValidationError(f"warehouse table {table} is not allowlisted")
    if len(spec.group_by) > MAX_GROUP_KEYS:
        raise QueryValidationError(f"group_by exceeds {MAX_GROUP_KEYS} keys")
    group_cols = [_column(table, name) for name in spec.group_by]
    params: list[Any] = []
    clauses = ["corpus_state IS DISTINCT FROM %s"] if table == "cards" else ["TRUE"]
    if table == "cards":
        params.append("suppressed")
    if access.deny:
        raise QueryValidationError(access.deny_reason or "access denied")
    if is_restricted(access) and access.allowed_sources:
        # Source restriction is applied as an allow-list of exact source labels.
        # Mixed-source deny is enforced by the query service on rows; this SQL
        # is a conservative prefilter, not a policy replacement.
        placeholders = ", ".join(["%s"] * len(access.allowed_sources))
        clauses.append(f"source && ARRAY[{placeholders}]::text[]")
        params.extend(access.allowed_sources)
    if predicate is not None:
        sql_pred, pred_params = _predicate_sql(predicate, table)
        clauses.append(sql_pred)
        params.extend(pred_params)
    where_sql = " AND ".join(f"({clause})" for clause in clauses)
    if spec.op == "count":
        select = "COUNT(*)::bigint AS value"
    else:
        field_name = _column(table, spec.field or "amount")
        select = f"SUM({field_name}) AS value"
    group_sql = ""
    if group_cols:
        select = ", ".join(group_cols) + f", {select}"
        group_sql = " GROUP BY " + ", ".join(group_cols)
    sql = (
        f"SELECT {select} FROM {schema}.{table} WHERE {where_sql}{group_sql} LIMIT {MAX_STATEMENT_ROWS}"
    )
    return CompiledAggregate(
        sql=sql,
        params=tuple(params),
        table=table,
        checkpoint=spec.checkpoint or spec.serving_snapshot,
    )


def _predicate_sql(predicate: Predicate, table: str) -> tuple[str, list[Any]]:
    if predicate.op == "and":
        parts = [_predicate_sql(child, table) for child in predicate.predicates]
        return " AND ".join(f"({sql})" for sql, _ in parts), [item for _, params in parts for item in params]
    if predicate.op == "or":
        parts = [_predicate_sql(child, table) for child in predicate.predicates]
        return " OR ".join(f"({sql})" for sql, _ in parts), [item for _, params in parts for item in params]
    spec = field_spec(predicate.field)
    column = _column(table, spec.name) if spec.name in ALLOWED_TABLES[table] or spec.name in {"uid", "type"} else None
    if column is None:
        raise QueryValidationError(f"field {spec.name} cannot be pushed to warehouse table {table}")
    if predicate.op == "exists":
        return f"{column} IS NOT NULL", []
    if predicate.op == "in":
        values = list(predicate.value or [])
        placeholders = ", ".join(["%s"] * len(values))
        return f"{column} IN ({placeholders})", values
    op_sql = {"eq": "=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">="}[predicate.op]
    return f"{column} {op_sql} %s", [predicate.value]


def execute_typed_aggregate(
    runtime: ArchiveRuntime,
    spec: TypedAggregateSpec,
    *,
    access: AccessContext,
    schema: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a compiled aggregate. ``payload`` is inspected only to reject SQL."""

    _reject_sql_payload(payload)
    compiled = compile_typed_aggregate(spec, access=access, schema=schema)
    opener = getattr(getattr(runtime, "warehouse", None), "snapshot", None)
    if not callable(opener):
        raise CapabilityUnavailableError("warehouse_snapshot_unavailable")
    # The public warehouse snapshot does not expose the connection. Callers that
    # need a live aggregate go through an explicit executor supplied on the
    # runtime warehouse, never through a leaked _connect.
    executor = getattr(runtime.warehouse, "execute_readonly", None)
    if not callable(executor):
        raise CapabilityUnavailableError("warehouse_readonly_executor_unavailable")
    rows = list(executor(compiled.sql, compiled.params) or [])
    return {
        "op": spec.op,
        "table": compiled.table,
        "checkpoint": compiled.checkpoint,
        "rows": rows,
        "status": "exact" if len(rows) < MAX_STATEMENT_ROWS else "unknown",
        "over_full_eligible_set": len(rows) < MAX_STATEMENT_ROWS,
        "sql_allowlisted": True,
    }

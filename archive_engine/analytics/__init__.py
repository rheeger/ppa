"""Bounded analytical adapters (P10-A).

Workflows (subscriptions, trip costs, changes-since) land in P10-C.
This package owns the read-only warehouse adapter and typed aggregate
compilation. Clients cannot submit SQL.
"""

from archive_engine.analytics.warehouse import (
    ALLOWED_AGGREGATES,
    TypedAggregateSpec,
    compile_typed_aggregate,
    execute_typed_aggregate,
)

__all__ = [
    "ALLOWED_AGGREGATES",
    "TypedAggregateSpec",
    "compile_typed_aggregate",
    "execute_typed_aggregate",
]

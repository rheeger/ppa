"""P10-A typed predicates: validation, access-before-totals, no arbitrary SQL."""

from __future__ import annotations

import pytest

from archive_engine.analytics.warehouse import TypedAggregateSpec, compile_typed_aggregate
from archive_engine.contracts import AccessContext
from archive_engine.errors import QueryValidationError
from archive_engine.query import (
    Predicate,
    StructuredQueryRequest,
    execute_typed_query,
    filters_to_predicate,
    request_from_simple_filters,
)
from archive_tests.acceptance.corpus import build_cards, build_queries


def _access(archive_id: str = "archive-p10a", **kwargs) -> AccessContext:
    return AccessContext(
        archive_id=archive_id,
        principal="local-operator",
        profile="trusted-local",
        **kwargs,
    )


def _rows_from_corpus() -> list[dict]:
    rows = []
    for card in build_cards():
        fields = dict(card["fields"])
        rows.append(
            {
                "uid": card["uid"],
                "card_uid": card["uid"],
                "type": card["type"],
                "summary": fields.get("summary") or "",
                "source": list(fields.get("source") or []),
                "sources": list(fields.get("source") or []),
                "required_sources": list(fields.get("source") or []),
                "people": list(fields.get("people") or []),
                "orgs": [],
                "activity_at": str(fields.get("created") or ""),
                "corpus_state": card["corpus_state"],
                "rel_path": card["rel_path"],
                "amount": fields.get("amount"),
                "currency": fields.get("currency"),
                "event_type": fields.get("event_type"),
                "lineage_complete": True,
            }
        )
    return rows


def _query(access: AccessContext, predicate, **kwargs):
    request = StructuredQueryRequest(
        archive_id=access.archive_id,
        access=access,
        predicate=predicate if isinstance(predicate, Predicate) else Predicate.from_payload(predicate),
        **kwargs,
    )
    return execute_typed_query(None, request, rows=_rows_from_corpus())


def test_unknown_field_rejected_before_execution() -> None:
    with pytest.raises(QueryValidationError, match="unknown field"):
        _query(_access(), {"op": "eq", "field": "favorite_color", "value": "blue"})


def test_unknown_operator_rejected() -> None:
    with pytest.raises(QueryValidationError, match="unsupported predicate operator"):
        _query(_access(), {"op": "like", "field": "type", "value": "person%"})


def test_numeric_operator_on_string_field_rejected() -> None:
    with pytest.raises(QueryValidationError, match="not valid"):
        _query(_access(), {"op": "gt", "field": "summary", "value": "A"})


def test_cross_type_amount_without_type_constraint_rejected() -> None:
    with pytest.raises(QueryValidationError, match="not valid across"):
        _query(_access(), {"op": "gt", "field": "amount", "value": 10})


def test_arbitrary_sql_rejected() -> None:
    with pytest.raises(QueryValidationError, match="arbitrary SQL"):
        Predicate.from_payload({"op": "eq", "field": "type", "value": "person", "sql": "SELECT 1"})
    with pytest.raises(QueryValidationError, match="arbitrary SQL"):
        Predicate.from_payload("SELECT * FROM cards")
    from archive_engine.analytics.warehouse import _reject_sql_payload

    with pytest.raises(QueryValidationError, match="arbitrary SQL"):
        _reject_sql_payload({"sql": "SELECT uid FROM cards"})


def test_namesake_typed_query_returns_both_people() -> None:
    expected = next(item for item in build_queries() if item["query_id"] == "q-p04b-person-namesake")
    page = _query(
        _access(),
        {
            "op": "and",
            "predicates": [
                {"op": "eq", "field": "type", "value": "person"},
                {"op": "eq", "field": "summary", "value": "Alex Rivera"},
            ],
        },
        order_field="uid",
        order_direction="asc",
        page_size=20,
        aggregate="count",
    )
    uids = {row["uid"] for row in page.rows}
    assert uids == set(expected["expected_uids"])
    assert page.total_status == "exact"
    assert page.matched_total == 2
    assert page.complete is True
    assert page.aggregate == {
        "op": "count",
        "value": 2,
        "status": "exact",
        "over_full_eligible_set": True,
    }


def test_restricted_scope_applied_before_totals() -> None:
    access = _access(allowed_sources=("acceptance.p04b",), allowed_domains=("identity",))
    page = _query(
        access,
        {
            "op": "and",
            "predicates": [
                {"op": "eq", "field": "type", "value": "person"},
                {"op": "eq", "field": "summary", "value": "Alex Rivera"},
            ],
        },
        aggregate="count",
    )
    assert page.matched_total == 2
    denied = AccessContext(
        archive_id="archive-p10a",
        principal="local-operator",
        profile="restricted",
        allowed_sources=("other-source",),
        allowed_domains=("identity",),
    )
    empty = _query(
        denied,
        {
            "op": "and",
            "predicates": [
                {"op": "eq", "field": "type", "value": "person"},
                {"op": "eq", "field": "summary", "value": "Alex Rivera"},
            ],
        },
        aggregate="count",
    )
    assert empty.matched_total == 0
    assert empty.aggregate["value"] == 0
    assert empty.complete is True


def test_denied_access_rejects_before_query() -> None:
    access = _access(deny=True, deny_reason="explicit deny")
    with pytest.raises(QueryValidationError, match="explicit deny"):
        _query(access, {"op": "eq", "field": "type", "value": "person"})


def test_unknown_saved_scope_rejected() -> None:
    with pytest.raises(QueryValidationError, match="unknown saved scope"):
        execute_typed_query(
            None,
            StructuredQueryRequest(
                archive_id="archive-p10a",
                access=_access(),
                saved_scope_name="family",
            ),
            rows=_rows_from_corpus(),
            scopes=(),
        )


def test_saved_scope_applies_without_widening() -> None:
    from archive_engine.scopes import SavedScope

    page = execute_typed_query(
        None,
        StructuredQueryRequest(
            archive_id="archive-p10a",
            access=_access(allowed_sources=("acceptance.p04b",)),
            saved_scope_name="p04b",
            filters={"type_filter": "person"},
            aggregate="count",
            page_size=20,
        ),
        rows=_rows_from_corpus(),
        scopes=(SavedScope(name="p04b", sources=("acceptance.p04b",), card_types=("person",)),),
    )
    assert page.empty_scope is False
    assert {row["uid"] for row in page.rows} >= {"hfa-person-p04balex0001", "hfa-person-p04balex0002"}
    empty = execute_typed_query(
        None,
        StructuredQueryRequest(
            archive_id="archive-p10a",
            access=_access(allowed_sources=("gmail",)),
            saved_scope_name="slack-only",
        ),
        rows=_rows_from_corpus(),
        scopes=(SavedScope(name="slack-only", sources=("slack",)),),
    )
    assert empty.empty_scope is True
    assert empty.rows == ()
    assert empty.matched_total == 0


def test_simple_filter_mapping_matches_type_membership() -> None:
    access = _access()
    request = request_from_simple_filters(access=access, type_filter="finance", limit=50)
    page = execute_typed_query(None, request, rows=_rows_from_corpus())
    expected = {card["uid"] for card in build_cards() if card["type"] == "finance"}
    assert {row["uid"] for row in page.rows} == expected
    assert page.matched_total == len(expected)
    pred = filters_to_predicate({"type_filter": "finance"})
    assert pred is not None and pred.op == "eq" and pred.field == "type"


def test_sum_over_full_eligible_set_not_page() -> None:
    page = _query(
        _access(),
        {
            "op": "and",
            "predicates": [
                {"op": "eq", "field": "type", "value": "finance"},
                {"op": "eq", "field": "currency", "value": "EUR"},
            ],
        },
        page_size=1,
        aggregate="sum",
        filters={"aggregate_field": "amount"},
    )
    assert len(page.rows) == 1
    assert page.matched_total == 2
    assert page.aggregate["over_full_eligible_set"] is True
    assert page.aggregate["by_currency"]["EUR"] == "60.0"


def test_warehouse_compile_is_parameterized() -> None:
    compiled = compile_typed_aggregate(
        TypedAggregateSpec(
            op="count",
            predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "person"}),
            checkpoint="ckpt-1",
        ),
        access=_access(),
        schema="plan_p10",
    )
    assert "%s" in compiled.sql
    assert "person" in compiled.params
    assert "SELECT" in compiled.sql
    assert "plan_p10.cards" in compiled.sql

"""P10-A acceptance: typed filters page a frozen corpus without gaps or duplicates."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from archive_engine.contracts import AccessContext
from archive_engine.errors import CursorInvalidError, QueryValidationError
from archive_engine.query import Predicate, StructuredQueryRequest, execute_typed_query
from archive_tests.acceptance.corpus import build_cards, build_queries, materialize_corpus
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register

PERSON_A = "hfa-person-p04balex0001"
PERSON_B = "hfa-person-p04balex0002"


def _access(archive_id: str) -> AccessContext:
    return AccessContext(
        archive_id=archive_id,
        principal="local-operator",
        profile="trusted-local",
    )


def _rows_from_cards() -> list[dict[str, Any]]:
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
                "activity_at": str(fields.get("created") or ""),
                "corpus_state": card["corpus_state"],
                "rel_path": card["rel_path"],
                "amount": fields.get("amount"),
                "currency": fields.get("currency"),
                "lineage_complete": True,
            }
        )
    return rows


def run_p10_queries(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    materialized = materialize_corpus(runtime.vault, owned_root=runtime.root)
    rows = _rows_from_cards()
    access = _access("archive-p10a")
    namesake = next(item for item in build_queries() if item["query_id"] == "q-p04b-person-namesake")
    page = execute_typed_query(
        None,
        StructuredQueryRequest(
            archive_id=access.archive_id,
            access=access,
            predicate=Predicate.from_payload(
                {
                    "op": "and",
                    "predicates": [
                        {"op": "eq", "field": "type", "value": "person"},
                        {"op": "eq", "field": "summary", "value": "Alex Rivera"},
                    ],
                }
            ),
            order_field="uid",
            page_size=10,
            aggregate="count",
        ),
        rows=rows,
    )
    actual = sorted(row["uid"] for row in page.rows)
    expected = sorted(namesake["expected_uids"])
    if actual != expected:
        raise AssertionError(f"namesake mismatch expected={expected} actual={actual}")
    if page.matched_total != 2 or page.total_status != "exact":
        raise AssertionError(f"namesake totals not exact: {page.matched_total} {page.total_status}")

    finance_expected = sorted(
        card["uid"] for card in build_cards() if card["type"] == "finance" and card["corpus_state"] != "suppressed"
    )
    seen: list[str] = []
    cursor = ""
    pages = 0
    first_finance_cursor = ""
    while True:
        chunk = execute_typed_query(
            None,
            StructuredQueryRequest(
                archive_id=access.archive_id,
                access=access,
                predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
                order_field="uid",
                page_size=2,
                cursor=cursor,
                aggregate="count",
            ),
            rows=rows,
        )
        pages += 1
        if chunk.next_cursor and not first_finance_cursor:
            first_finance_cursor = chunk.next_cursor
        for row in chunk.rows:
            if row["uid"] in seen:
                raise AssertionError(f"duplicate finance uid {row['uid']}")
            seen.append(row["uid"])
        if chunk.matched_total != len(finance_expected):
            raise AssertionError("finance count was not over the full eligible set")
        if not chunk.next_cursor:
            break
        cursor = chunk.next_cursor
    if seen != finance_expected:
        raise AssertionError(f"finance pagination missed or reordered: {seen} != {finance_expected}")
    if not first_finance_cursor:
        raise AssertionError("finance fixture is too small to prove multi-page cursors")

    misuse = None
    try:
        execute_typed_query(
            None,
            StructuredQueryRequest(
                archive_id=access.archive_id,
                access=access,
                predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "person"}),
                order_field="uid",
                page_size=2,
                cursor=first_finance_cursor,
            ),
            rows=rows,
        )
    except CursorInvalidError as exc:
        misuse = str(exc)
    if not misuse:
        raise AssertionError("predicate-changed cursor must fail")

    try:
        execute_typed_query(
            None,
            StructuredQueryRequest(
                archive_id=access.archive_id,
                access=access,
                predicate=Predicate.from_payload({"op": "eq", "field": "not_a_field", "value": "x"}),
            ),
            rows=rows,
        )
        raise AssertionError("unknown field must reject")
    except QueryValidationError:
        pass

    payload = {
        "id": "p10.queries.typed_pagination",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "materialized_cards": materialized["card_count"],
        "namesake_expected": expected,
        "namesake_actual": actual,
        "finance_expected": finance_expected,
        "finance_actual": seen,
        "finance_pages": pages,
        "cursor_misuse": misuse,
        "matched_total": page.matched_total,
        "total_status": page.total_status,
    }
    artifact = Path(runtime.root.parent) / "p10-queries.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p10.queries.typed_pagination",
        suite="p10",
        product_guarantee="A typed query pages the full eligible fixture set exactly once and rejects invalid predicates/cursors",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p10-queries.json",),
        run=run_p10_queries,
    )
)

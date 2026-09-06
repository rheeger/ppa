"""P10-A stable pagination: full eligible set exactly once; cursor misuse fails closed."""

from __future__ import annotations

from archive_engine.contracts import AccessContext
from archive_engine.errors import CursorInvalidError
from archive_engine.query import (
    Predicate,
    StructuredQueryRequest,
    execute_typed_query,
    predicate_fingerprint,
)
from archive_engine.query_cursor import QueryCursor, decode_cursor
from archive_tests.acceptance.corpus import build_cards


def _access(archive_id: str = "archive-p10a", **kwargs) -> AccessContext:
    return AccessContext(
        archive_id=archive_id,
        principal="local-operator",
        profile="trusted-local",
        **kwargs,
    )


def _rows() -> list[dict]:
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
                "lineage_complete": True,
            }
        )
    return rows


def _page(access: AccessContext, **kwargs):
    request = StructuredQueryRequest(archive_id=access.archive_id, access=access, **kwargs)
    return execute_typed_query(None, request, rows=_rows())


def _finance_expected() -> list[str]:
    return sorted(
        card["uid"]
        for card in build_cards()
        if card["type"] == "finance" and card["corpus_state"] != "suppressed"
    )


def paginate_all(access: AccessContext, *, page_size: int = 2, **kwargs) -> tuple[list[str], list[dict]]:
    seen: list[str] = []
    pages: list[dict] = []
    cursor = ""
    while True:
        page = _page(
            access,
            predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
            order_field="uid",
            order_direction="asc",
            page_size=page_size,
            cursor=cursor,
            aggregate="count",
            **kwargs,
        )
        pages.append(page.to_payload())
        for row in page.rows:
            uid = row["uid"]
            assert uid not in seen, f"duplicate uid {uid} across pages"
            seen.append(uid)
        if not page.next_cursor:
            break
        cursor = page.next_cursor
    return seen, pages


def test_full_pagination_matches_independent_fixture_set() -> None:
    expected = _finance_expected()
    seen, pages = paginate_all(_access(), page_size=2)
    assert seen == expected
    assert pages[0]["matched_total"] == len(expected)
    assert pages[0]["total_status"] == "exact"
    assert pages[0]["aggregate"]["value"] == len(expected)
    assert pages[0]["aggregate"]["over_full_eligible_set"] is True
    assert all(page["matched_total"] == len(expected) for page in pages)
    assert sum(len(page["rows"]) for page in pages) == len(expected)
    assert pages[-1]["next_cursor"] == ""


def test_totals_never_come_from_top_k_page() -> None:
    page = _page(
        _access(),
        predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
        order_field="uid",
        page_size=1,
        aggregate="count",
    )
    assert len(page.rows) == 1
    assert page.matched_total == len(_finance_expected())
    assert page.matched_total != len(page.rows)


def test_tampered_cursor_rejected() -> None:
    page = _page(
        _access(),
        predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
        order_field="uid",
        page_size=2,
    )
    assert page.next_cursor
    broken = page.next_cursor[:-2] + ("AA" if not page.next_cursor.endswith("AA") else "BB")
    try:
        _page(
            _access(),
            predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
            order_field="uid",
            page_size=2,
            cursor=broken,
        )
        raise AssertionError("tampered cursor must fail")
    except CursorInvalidError as exc:
        assert "integrity" in str(exc) or "malformed" in str(exc)


def test_policy_change_invalidates_cursor() -> None:
    first = _page(
        _access(),
        predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
        order_field="uid",
        page_size=2,
    )
    changed = _access(allowed_sources=("acceptance.p04b",))
    try:
        _page(
            changed,
            predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
            order_field="uid",
            page_size=2,
            cursor=first.next_cursor,
        )
        raise AssertionError("policy change must invalidate cursor")
    except CursorInvalidError as exc:
        assert "policy" in str(exc)


def test_snapshot_change_invalidates_cursor() -> None:
    first = _page(
        _access(),
        predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
        order_field="uid",
        page_size=2,
        snapshot="gen-a",
    )
    try:
        _page(
            _access(),
            predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
            order_field="uid",
            page_size=2,
            cursor=first.next_cursor,
            snapshot="gen-b",
        )
        raise AssertionError("snapshot change must invalidate cursor")
    except CursorInvalidError as exc:
        assert "snapshot" in str(exc)


def test_predicate_change_invalidates_cursor() -> None:
    first = _page(
        _access(),
        predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"}),
        order_field="uid",
        page_size=2,
    )
    try:
        _page(
            _access(),
            predicate=Predicate.from_payload({"op": "eq", "field": "type", "value": "person"}),
            order_field="uid",
            page_size=2,
            cursor=first.next_cursor,
        )
        raise AssertionError("predicate change must invalidate cursor")
    except CursorInvalidError as exc:
        assert "predicate" in str(exc)


def test_cursor_round_trip_binding() -> None:
    access = _access()
    pred = Predicate.from_payload({"op": "eq", "field": "type", "value": "finance"})
    page = _page(access, predicate=pred, order_field="uid", page_size=2, snapshot="memory")
    cursor = decode_cursor(page.next_cursor)
    assert cursor.archive_id == access.archive_id
    assert cursor.snapshot == "memory"
    assert cursor.order_field == "uid"
    assert cursor.predicate_fingerprint == predicate_fingerprint(pred, {})
    assert isinstance(cursor, QueryCursor)

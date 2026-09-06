"""P10-C: subscription lifecycle, trip costs, and changes-since."""

from __future__ import annotations

from decimal import Decimal

import pytest

from archive_engine.analytics import decimal_amount
from archive_engine.analytics.changes import changes_since
from archive_engine.analytics.subscriptions import pebble_mail_state, subscription_lifecycle
from archive_engine.analytics.trip_costs import assemble_trip, currency_totals, reconcile_trip_costs
from archive_engine.contracts import AccessContext, ChangeRecord
from archive_engine.errors import CursorInvalidError, QueryValidationError
from archive_tests.acceptance.corpus import build_cards, build_queries, build_relations

RENEW = "hfa-subscription-p04brenw001"
CANCEL = "hfa-subscription-p04bcncl001"
CHARGE = "hfa-finance-p04bchg0001"
LOOK_FIN = "hfa-finance-p04blook001"
PURCHASE = "hfa-purchase-p04bhotl001"
PURCHASE_LOOK = "hfa-purchase-p04blook001"
HOTEL = "hfa-accommodation-p04bhotl001"
FLIGHT_OUT = "hfa-flight-p04bout0001"
FLIGHT_IN = "hfa-flight-p04bin00001"
FLIGHT_LOOK = "hfa-flight-p04blook001"
RIDE = "hfa-ride-p04bairp001"
BOOKING = "hfa-email-message-p04bbook001"
OBSERVE = "hfa-observation-p04btrip001"
EUR = "hfa-finance-p04beur0001"
REFUND = "hfa-finance-p04bref0001"
ATTACH = "hfa-email-attachment-p04brcpt001"


def _access(**kwargs) -> AccessContext:
    return AccessContext(
        archive_id="archive-p10c",
        principal="local-operator",
        profile="trusted-local",
        **kwargs,
    )


def _cards(*uids: str) -> list[dict]:
    wanted = set(uids)
    return [card for card in build_cards() if card["uid"] in wanted]


def _record(**kwargs) -> ChangeRecord:
    defaults = {
        "archive_id": "archive-p10c",
        "sequence": 1,
        "mutation_id": "mut-1",
        "uid": "hfa-finance-p04bchg0001",
        "operation": "create",
        "before_revision": "",
        "after_revision": "sha256:a",
        "source": "acceptance.p04b",
        "account": "alex@example.test",
        "run_id": "run-1",
        "committed": True,
    }
    defaults.update(kwargs)
    return ChangeRecord(**defaults)


def test_renewal_is_last_observed_not_current() -> None:
    relation = next(item for item in build_relations() if item["case_id"] == "rel-p04b-renewal-is-not-current")
    result = subscription_lifecycle(_cards(RENEW, CANCEL))
    row = result.rows[0]
    assert row["lifecycle_state"] not in {"current", "active", "currently_subscribed"}
    assert row["lifecycle_state"] == "last_observed_canceled"
    assert row["last_observed_event_uid"] == CANCEL
    assert RENEW in row["conflict_uids"]
    assert set(row["supporting_event_uids"]) == set(relation["positive_evidence_ids"])
    assert row["freshness"] == "unknown"
    compact = pebble_mail_state(_cards(RENEW, CANCEL))
    assert compact["lifecycle_state"] == "last_observed_canceled"
    dumped = str(result.to_payload())
    assert "current_subscription" not in dumped
    assert "advice" not in dumped


def test_renewal_only_is_last_observed_renewed() -> None:
    result = subscription_lifecycle(_cards(RENEW))
    assert result.rows[0]["lifecycle_state"] == "last_observed_renewed"
    assert result.rows[0]["last_observed_event_uid"] == RENEW
    assert result.rows[0]["conflict_uids"] == []


def test_simultaneous_events_are_conflict() -> None:
    renew = next(card for card in build_cards() if card["uid"] == RENEW)
    cancel = next(card for card in build_cards() if card["uid"] == CANCEL)
    cancel = dict(cancel)
    cancel["fields"] = dict(cancel["fields"], event_at=renew["fields"]["event_at"])
    result = subscription_lifecycle([renew, cancel])
    assert result.rows[0]["lifecycle_state"] in {"conflict", "unknown"}


def test_stale_subscription_source_excluded() -> None:
    renew = next(card for card in build_cards() if card["uid"] == RENEW)
    stale = dict(renew)
    stale["uid"] = "hfa-subscription-p04bstale01"
    stale["corpus_state"] = "suppressed"
    result = subscription_lifecycle([stale, next(card for card in build_cards() if card["uid"] == CANCEL)])
    assert "hfa-subscription-p04bstale01" not in result.rows[0]["supporting_event_uids"]
    assert any(item.uid == stale["uid"] and item.role == "excluded" for item in result.evidence)


def test_same_trip_booking_identity_excludes_lookalike() -> None:
    relation = next(item for item in build_relations() if item["case_id"] == "rel-p04b-same-trip")
    cards = build_cards()
    result = assemble_trip(cards)
    row = result.rows[0]
    members = set(row["member_uids"])
    assert FLIGHT_LOOK not in members
    assert FLIGHT_LOOK in row["excluded_uids"]
    assert {FLIGHT_OUT, FLIGHT_IN, HOTEL, CHARGE, BOOKING} <= members
    assert RIDE in row["unmatched_evidence"]
    assert OBSERVE in row["unmatched_evidence"]
    kinds = {item.uid: item.evidence_kind for item in result.evidence}
    assert kinds[OBSERVE] == "proposed_link"
    assert kinds[OBSERVE] != "source_reported"
    assert set(relation["negative_evidence_ids"]) <= set(row["excluded_uids"])


def test_same_charge_is_482_once() -> None:
    relation = next(item for item in build_relations() if item["case_id"] == "rel-p04b-same-charge")
    result = reconcile_trip_costs(build_cards())
    row = result.rows[0]
    assert row["supported_charge_uid"] == CHARGE
    assert row["amount"] == "482.0"
    assert row["currency"] == "USD"
    assert ATTACH in row["receipt_uids"]
    assert LOOK_FIN in row["excluded_uids"]
    assert PURCHASE_LOOK in row["excluded_uids"]
    assert Decimal(row["amount"]) == Decimal("482.0")
    estimates = {item["uid"]: Decimal(item["amount"]) for item in row["booking_estimates"]}
    assert estimates[HOTEL] == Decimal("482.0")
    actual = Decimal(row["currency_totals"]["USD"])
    assert actual == Decimal("482.0")
    assert actual != estimates[HOTEL] + actual
    segment_sum = sum(Decimal(item["amount"]) for item in row["segment_estimates"])
    assert segment_sum == Decimal("318.0") + Decimal("302.0")
    assert all(item["counted_in_actual"] is False for item in row["segment_estimates"])
    kinds = {item.uid: item.evidence_kind for item in result.evidence}
    assert kinds[PURCHASE] == "derived"
    assert set(relation["negative_evidence_ids"]) <= set(row["excluded_uids"])


def test_eur_net_preserves_refund_no_fx() -> None:
    query = next(item for item in build_queries() if item["query_id"] == "q-p04b-agg-eur-net")
    totals = currency_totals(_cards(EUR, REFUND), currency="EUR")
    assert set(totals["uids"]) == set(query["expected_uids"])
    assert Decimal(totals["totals"]["EUR"]) == Decimal("60.0")
    assert totals["total_status"] == "exact"
    assert "USD" not in totals["totals"]
    assert "fx_rate" not in totals
    assert "converted_amount" not in totals


def test_truncated_page_cannot_sum() -> None:
    totals = currency_totals(_cards(CHARGE), truncated=True)
    assert totals["total_status"] == "unknown"
    assert totals["totals"] == {}
    with pytest.raises(QueryValidationError, match="truncated"):
        reconcile_trip_costs(_cards(CHARGE), truncated=True)


def test_restricted_access_applied_before_totals() -> None:
    access = _access(allowed_sources=("not-a-source",))
    result = subscription_lifecycle(build_cards(), access=access)
    assert result.rows == ()
    totals = currency_totals(_cards(CHARGE, EUR), access=access)
    assert totals["totals"] == {}


def test_changes_create_update_delete_correction() -> None:
    records = [
        _record(sequence=1, uid="hfa-note-new", operation="create", mutation_id="m1"),
        _record(sequence=2, uid="hfa-note-new", operation="update", mutation_id="m2", before_revision="sha256:a"),
        _record(sequence=3, uid="hfa-doc-gone", operation="delete", mutation_id="m3", after_revision=""),
        _record(
            sequence=4,
            uid="hfa-finance-p04bcorr01",
            operation="update",
            mutation_id="m4",
            source="decision:dec-1",
            before_revision="sha256:425",
            after_revision="sha256:42.50",
        ),
        _record(sequence=5, uid="hfa-note-new", operation="embed", mutation_id="m5"),
        _record(sequence=6, uid="hfa-link-prop", operation="create", mutation_id="m6"),
    ]
    page = changes_since(
        records,
        after_sequence=0,
        snapshot_id="snap-1",
        decisions=[{"uid": "hfa-link-prop", "kind": "proposed_link", "method": "inferred"}],
    )
    roles = {row.uid: row.role for row in page.rows}
    ops = {row.uid: row.operation for row in page.rows}
    assert roles["hfa-note-new"] == "updated"
    assert ops["hfa-doc-gone"] == "delete"
    assert page.rows[2].tombstone is True
    assert "body" not in page.rows[2].to_payload()
    assert roles["hfa-finance-p04bcorr01"] == "corrected"
    assert ops["hfa-finance-p04bcorr01"] == "correct"
    assert page.rows[3].evidence_kind == "derived"
    assert roles["hfa-link-prop"] == "proposed"
    assert page.rows[4].evidence_kind == "proposed_link"
    assert "hfa-note-new" in page.excluded_uids
    assert all(row.role != "derived_refresh" for row in page.rows)


def test_changes_cursor_is_snapshot_bound() -> None:
    records = [_record(sequence=index, mutation_id=f"m{index}", uid=f"hfa-note-{index:04d}") for index in range(1, 6)]
    first = changes_since(records, snapshot_id="snap-a", page_size=2)
    assert first.truncated is True
    assert first.next_cursor
    second = changes_since(records, snapshot_id="snap-a", cursor=first.next_cursor, page_size=2)
    assert [row.sequence for row in second.rows] == [3, 4]
    with pytest.raises(CursorInvalidError, match="snapshot"):
        changes_since(records, snapshot_id="snap-b", cursor=first.next_cursor)


def test_denied_delete_is_bodyless_tombstone() -> None:
    page = changes_since(
        [_record(sequence=1, uid="hfa-secret", operation="delete", mutation_id="mdel")],
        denied_uids=("hfa-secret",),
    )
    assert page.rows[0].tombstone is True
    assert page.rows[0].after_revision == ""
    assert page.rows[0].source == ""


def test_decimal_helper_keeps_fixture_scale() -> None:
    assert decimal_amount(482.0) == Decimal("482.0")
    assert decimal_amount(-40.0) == Decimal("-40.0")
    assert decimal_amount("42.50") == Decimal("42.50")

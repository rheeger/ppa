"""Phase 6.5 Step 10 -- integration tests for the three new linkers.

Uses hand-built SeedLinkCatalog fixtures (no DB required) to exercise every
tier of every module with positive cases and negative controls.
"""

from __future__ import annotations

from typing import Any

import pytest
from archive_cli import linker_framework as lf
from archive_cli import seed_links as s
from archive_cli.linker_modules import finance_reconcile as fr_mod
from archive_cli.linker_modules import meeting_artifact as ma_mod
from archive_cli.linker_modules import trip_cluster as tc_mod

# --- Helpers --------------------------------------------------------------


def _make_sketch(
    uid: str,
    card_type: str,
    *,
    frontmatter: dict[str, Any] | None = None,
    summary: str = "",
    activity_at: str = "",
    participant_emails: set[str] | None = None,
    slug: str | None = None,
) -> s.SeedCardSketch:
    return s.SeedCardSketch(
        uid=uid,
        rel_path=f"{card_type}/{uid}.md",
        slug=slug or uid,
        card_type=card_type,
        summary=summary,
        frontmatter=frontmatter or {},
        body="",
        content_hash="",
        activity_at=activity_at,
        wikilinks=[],
        participant_emails=participant_emails or set(),
    )


def _empty_catalog() -> s.SeedLinkCatalog:
    return s.SeedLinkCatalog(
        cards_by_uid={},
        cards_by_exact_slug={},
        cards_by_slug={},
        cards_by_type={},
        person_by_email={},
        person_by_phone={},
        person_by_handle={},
        person_by_alias={},
        email_threads_by_thread_id={},
        email_messages_by_thread_id={},
        email_messages_by_message_id={},
        email_attachments_by_message_id={},
        email_attachments_by_thread_id={},
        imessage_threads_by_chat_id={},
        imessage_messages_by_chat_id={},
        calendar_events_by_event_id={},
        calendar_events_by_ical_uid={},
        media_by_day={},
        events_by_day={},
        path_buckets={},
    )


def _populate_catalog(cards: list[s.SeedCardSketch]) -> s.SeedLinkCatalog:
    cat = _empty_catalog()
    for c in cards:
        cat.cards_by_uid[c.uid] = c
        cat.cards_by_slug.setdefault(c.slug, c)
        cat.cards_by_exact_slug.setdefault(c.slug, c)
        cat.cards_by_type.setdefault(c.card_type, []).append(c)
        if c.card_type == "calendar_event":
            ical_uid = str(c.frontmatter.get("ical_uid") or "")
            if ical_uid:
                cat.calendar_events_by_ical_uid.setdefault(ical_uid, []).append(c)
            event_id = str(c.frontmatter.get("event_id") or "")
            if event_id:
                cat.calendar_events_by_event_id.setdefault(event_id, []).append(c)
            start_day = (c.frontmatter.get("start_at") or c.activity_at or "")[:10]
            if start_day:
                cat.events_by_day.setdefault(start_day, []).append(c)
        if c.card_type == "email_message":
            mid = str(c.frontmatter.get("gmail_message_id") or c.slug)
            cat.email_messages_by_message_id.setdefault(mid, []).append(c)
    return cat


def _run_hooks(cat: s.SeedLinkCatalog) -> None:
    lf.run_post_build_hooks(cat)


# =========================================================================
# MODULE_MEETING_ARTIFACT
# =========================================================================


class TestMeetingArtifact:

    def test_tier_ical_uid_exact_match(self):
        transcript = _make_sketch(
            "hfa-meeting-transcript-1", "meeting_transcript",
            frontmatter={"ical_uid": "abc-123", "start_at": "2024-06-01T10:00:00Z"},
            summary="Q3 planning", activity_at="2024-06-01T10:00:00Z",
        )
        event = _make_sketch(
            "hfa-calendar-event-1", "calendar_event",
            frontmatter={"ical_uid": "abc-123", "start_at": "2024-06-01T10:00:00Z"},
            summary="Q3 planning", activity_at="2024-06-01T10:00:00Z",
        )
        cat = _populate_catalog([transcript, event])
        _run_hooks(cat)
        out = ma_mod._generate_meeting_artifact_candidates(cat, transcript)
        assert len(out) == 1
        assert out[0].target_card_uid == event.uid
        assert out[0].features["tier"] == "MEETING_TIER_ICAL_UID"
        assert out[0].features["deterministic_score"] == 1.00

    def test_tier_title_time_within_15_min(self):
        transcript = _make_sketch(
            "hfa-meeting-transcript-2", "meeting_transcript",
            frontmatter={"start_at": "2024-06-01T10:00:00Z"},
            summary="Product Review",
        )
        event = _make_sketch(
            "hfa-calendar-event-2", "calendar_event",
            frontmatter={"start_at": "2024-06-01T10:14:00Z"},
            summary="Product Review",
        )
        cat = _populate_catalog([transcript, event])
        _run_hooks(cat)
        out = ma_mod._generate_meeting_artifact_candidates(cat, transcript)
        assert len(out) == 1
        assert out[0].features["tier"] == "MEETING_TIER_TITLE_TIME"

    def test_tier_title_time_outside_window_rejected(self):
        transcript = _make_sketch(
            "hfa-meeting-transcript-3", "meeting_transcript",
            frontmatter={"start_at": "2024-06-01T10:00:00Z"},
            summary="Product Review",
        )
        event = _make_sketch(
            "hfa-calendar-event-3", "calendar_event",
            frontmatter={"start_at": "2024-06-01T10:20:00Z"},
            summary="Product Review",
        )
        cat = _populate_catalog([transcript, event])
        _run_hooks(cat)
        out = ma_mod._generate_meeting_artifact_candidates(cat, transcript)
        assert out == []

    def test_generic_titles_fall_to_tier3(self):
        """A generic title like 'Meeting' should NOT match at Tier 2 even
        with a 2-minute drift; it should fall to Tier 3 (participant-based)."""
        transcript = _make_sketch(
            "hfa-meeting-transcript-4", "meeting_transcript",
            frontmatter={"start_at": "2024-06-01T10:00:00Z"},
            summary="Meeting",
            participant_emails={"alice@x.com", "bob@x.com", "carol@x.com"},
        )
        event = _make_sketch(
            "hfa-calendar-event-4", "calendar_event",
            frontmatter={"start_at": "2024-06-01T10:02:00Z"},
            summary="Meeting",
            participant_emails={"alice@x.com", "bob@x.com"},
        )
        cat = _populate_catalog([transcript, event])
        _run_hooks(cat)
        out = ma_mod._generate_meeting_artifact_candidates(cat, transcript)
        assert len(out) == 1
        # Generic title -> Tier 3 (participant overlap), not Tier 2.
        assert out[0].features["tier"] == "MEETING_TIER_PARTICIPANT_TIME"
        assert out[0].features["deterministic_score"] == 0.70

    def test_insufficient_participant_overlap(self):
        transcript = _make_sketch(
            "hfa-meeting-transcript-5", "meeting_transcript",
            frontmatter={"start_at": "2024-06-01T10:00:00Z"},
            summary="1:1",
            participant_emails={"alice@x.com"},
        )
        event = _make_sketch(
            "hfa-calendar-event-5", "calendar_event",
            frontmatter={"start_at": "2024-06-01T10:20:00Z"},
            summary="1:1",
            participant_emails={"alice@x.com"},
        )
        cat = _populate_catalog([transcript, event])
        _run_hooks(cat)
        assert ma_mod._generate_meeting_artifact_candidates(cat, transcript) == []

    def test_negative_control_unrelated_events_same_day(self):
        transcript = _make_sketch(
            "hfa-meeting-transcript-neg", "meeting_transcript",
            frontmatter={"ical_uid": "XYZ-1"},
            summary="Board meeting",
        )
        event_unrelated = _make_sketch(
            "hfa-calendar-event-neg", "calendar_event",
            frontmatter={"ical_uid": "DIFFERENT"},
            summary="Team lunch",
        )
        cat = _populate_catalog([transcript, event_unrelated])
        _run_hooks(cat)
        assert ma_mod._generate_meeting_artifact_candidates(cat, transcript) == []


# =========================================================================
# MODULE_TRIP_CLUSTER
# =========================================================================


class TestTripCluster:

    def test_accommodation_flight_same_city_date_window(self):
        accom = _make_sketch(
            "hfa-accommodation-1", "accommodation",
            frontmatter={
                "address": "123 Market St, San Francisco, CA 94105",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
            activity_at="2024-06-10T15:00:00Z",
        )
        flight = _make_sketch(
            "hfa-flight-1", "flight",
            frontmatter={
                "destination_airport": "SFO",
                "arrival_at": "2024-06-10T13:00:00Z",
            },
            activity_at="2024-06-10T13:00:00Z",
        )
        cat = _populate_catalog([accom, flight])
        _run_hooks(cat)
        out = tc_mod._generate_trip_cluster_candidates(cat, accom)
        assert len(out) == 1
        assert out[0].target_card_uid == flight.uid
        assert out[0].features["tier"] == "TRIP_TIER_ACCOM_FLIGHT"
        assert out[0].features["deterministic_score"] == 0.92

    def test_flight_wrong_city_rejected(self):
        accom = _make_sketch(
            "hfa-accommodation-2", "accommodation",
            frontmatter={
                "address": "1 Main, Seattle, WA",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
        )
        flight = _make_sketch(
            "hfa-flight-2", "flight",
            frontmatter={
                "destination_airport": "LAX",  # wrong city
                "arrival_at": "2024-06-10T13:00:00Z",
            },
        )
        cat = _populate_catalog([accom, flight])
        _run_hooks(cat)
        assert tc_mod._generate_trip_cluster_candidates(cat, accom) == []

    def test_flight_outside_date_window_rejected(self):
        accom = _make_sketch(
            "hfa-accommodation-3", "accommodation",
            frontmatter={
                "address": "1 Main, San Francisco, CA",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
        )
        flight = _make_sketch(
            "hfa-flight-3", "flight",
            frontmatter={
                "destination_airport": "SFO",
                "arrival_at": "2024-07-01T13:00:00Z",  # way outside window
            },
        )
        cat = _populate_catalog([accom, flight])
        _run_hooks(cat)
        assert tc_mod._generate_trip_cluster_candidates(cat, accom) == []

    def test_car_rental_match(self):
        accom = _make_sketch(
            "hfa-accommodation-4", "accommodation",
            frontmatter={
                "address": "1 Main, New York, NY",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
        )
        cr = _make_sketch(
            "hfa-car-rental-4", "car_rental",
            frontmatter={
                "pickup_location": "New York, NY",
                "pickup_at": "2024-06-10T12:00:00Z",
            },
        )
        cat = _populate_catalog([accom, cr])
        _run_hooks(cat)
        out = tc_mod._generate_trip_cluster_candidates(cat, accom)
        assert len(out) == 1
        assert out[0].target_card_uid == cr.uid
        assert out[0].features["tier"] == "TRIP_TIER_ACCOM_CARRENTAL"

    def test_unknown_iata_code_skips(self):
        accom = _make_sketch(
            "hfa-accommodation-5", "accommodation",
            frontmatter={
                "address": "1 Main, San Francisco, CA",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
        )
        flight = _make_sketch(
            "hfa-flight-5", "flight",
            frontmatter={
                "destination_airport": "ZZZ",  # not in bundled CSV
                "arrival_at": "2024-06-10T13:00:00Z",
            },
        )
        cat = _populate_catalog([accom, flight])
        _run_hooks(cat)
        assert tc_mod._generate_trip_cluster_candidates(cat, accom) == []


# =========================================================================
# MODULE_FINANCE_RECONCILE
# =========================================================================


class TestFinanceReconcile:

    def test_tier_source_email_primary_key_match(self):
        fin = _make_sketch(
            "hfa-finance-1", "finance",
            frontmatter={
                "amount": 42.17,
                "currency": "USD",
                "counterparty": "AMZN MKTPLACE",
                "source_email": "[[hfa-email-message-abc]]",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-1", "purchase",
            frontmatter={
                "total": 42.17,
                "currency": "USD",
                "vendor": "Amazon.com",
                "source_email": "hfa-email-message-abc",  # same target, no brackets
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        assert len(out) == 1
        assert out[0].target_card_uid == purchase.uid
        assert out[0].features["tier"] == "RECONCILE_TIER_SOURCE_EMAIL"
        assert out[0].features["deterministic_score"] == 0.98

    def test_tier_high_amount_date_merchant(self):
        fin = _make_sketch(
            "hfa-finance-2", "finance",
            frontmatter={
                "amount": 42.17,
                "currency": "USD",
                "counterparty": "AMZN MKTPLACE",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-2", "purchase",
            frontmatter={
                "total": 42.17,
                "currency": "USD",
                "vendor": "Amazon",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        assert len(out) == 1
        assert out[0].features["tier"] == "RECONCILE_TIER_HIGH"
        assert out[0].features["deterministic_score"] == 0.90

    def test_cross_currency_rejected(self):
        fin = _make_sketch(
            "hfa-finance-3", "finance",
            frontmatter={
                "amount": 100.00,
                "currency": "USD",
                "counterparty": "Spotify",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        sub = _make_sketch(
            "hfa-subscription-3", "subscription",
            frontmatter={
                "price": 100.00,
                "currency": "GBP",  # different
                "service_name": "Spotify",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        cat = _populate_catalog([fin, sub])
        _run_hooks(cat)
        assert fr_mod._generate_finance_reconcile_candidates(cat, fin) == []

    def test_merchant_mismatch_no_high_tier(self):
        """Mismatched merchants must NOT produce a HIGH-tier (auto-promote)
        candidate. A LOW-tier (review-only) candidate is acceptable because
        amount + date alone legitimately warrants human review."""
        fin = _make_sketch(
            "hfa-finance-4", "finance",
            frontmatter={
                "amount": 42.17,
                "currency": "USD",
                "counterparty": "DOORDASH*SOMETHING",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-4", "purchase",
            frontmatter={
                "total": 42.17,
                "currency": "USD",
                "vendor": "Amazon",  # totally different
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        # No HIGH-tier (0.90) promotion; LOW-tier (0.40 after risk) review ok.
        assert all(c.features["tier"] != "RECONCILE_TIER_HIGH" for c in out)
        for c in out:
            assert c.features["deterministic_score"] <= 0.80

    def test_refund_with_extended_window(self):
        fin = _make_sketch(
            "hfa-finance-5", "finance",
            frontmatter={
                "amount": -30.00,
                "currency": "USD",
                "counterparty": "Amazon",
                "transaction_type": "refund",
            },
            activity_at="2024-07-15T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-5", "purchase",
            frontmatter={
                "total": 30.00,
                "currency": "USD",
                "vendor": "Amazon",
            },
            activity_at="2024-06-01T00:00:00Z",  # 44 days earlier
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        assert len(out) == 1
        assert out[0].features["tier"] == "RECONCILE_TIER_HIGH"
        assert out[0].features["refund"] is True

    def test_tier_low_amount_date_only(self):
        fin = _make_sketch(
            "hfa-finance-6", "finance",
            frontmatter={
                "amount": 42.17,
                "currency": "USD",
                "counterparty": "UNKNOWN VENDOR X",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-6", "purchase",
            frontmatter={
                "total": 42.17,
                "currency": "USD",
                "vendor": "Mystery Store",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        assert len(out) == 1
        assert out[0].features["tier"] == "RECONCILE_TIER_LOW"

    def test_ride_uses_fare_not_fare_amount(self):
        """Regression guard: ride card's amount field is 'fare' (+ 'tip'),
        not the (non-existent) 'fare_amount'. Phase 6.5 plan review fixed this."""
        ride = _make_sketch(
            "hfa-ride-regression", "ride",
            frontmatter={"fare": 15.00, "tip": 3.00, "service": "Uber"},
        )
        assert fr_mod._amount_for(ride) == 18.00

    def test_payroll_uses_net_amount(self):
        paycheck = _make_sketch(
            "hfa-payroll-regression", "payroll",
            frontmatter={"gross_amount": 5000.0, "net_amount": 3200.0},
        )
        assert fr_mod._amount_for(paycheck) == 3200.0

    def test_subscription_zero_price_skipped(self):
        free_trial = _make_sketch(
            "hfa-subscription-trial", "subscription",
            frontmatter={"price": 0.0},
        )
        assert fr_mod._amount_for(free_trial) == 0.0

    def test_zero_amount_source_skipped(self):
        fin = _make_sketch(
            "hfa-finance-zero", "finance",
            frontmatter={"amount": 0.0, "counterparty": "ATM"},
        )
        cat = _populate_catalog([fin])
        _run_hooks(cat)
        assert fr_mod._generate_finance_reconcile_candidates(cat, fin) == []


# =========================================================================
# Precision-first quality gate (linker-quality-gates.md)
# =========================================================================


class TestFinanceSourceEmailCorroboration:
    """The TIER_SOURCE_EMAIL family enforces the precision-first standard:

    bare wikilink agreement is not enough; tight-bound corroboration is
    required for auto-promote; one signal becomes review-only; zero signals
    is rejected. See archive_docs/runbooks/linker-quality-gates.md.
    """

    def test_two_corroborating_signals_promotes(self):
        fin = _make_sketch(
            "hfa-finance-corr-2", "finance",
            frontmatter={
                "amount": 42.17,
                "currency": "USD",
                "counterparty": "AMZN MKTPLACE",
                "source_email": "[[hfa-email-message-corr]]",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-corr-2", "purchase",
            frontmatter={
                "total": 42.17,
                "currency": "USD",
                "vendor": "Mystery Store",
                "source_email": "hfa-email-message-corr",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        assert len(out) == 1
        assert out[0].features["tier"] == "RECONCILE_TIER_SOURCE_EMAIL"
        assert out[0].features["deterministic_score"] == 0.98
        assert out[0].features["corroborating_signal_count"] >= 2

    def test_one_corroborating_signal_review_only(self):
        """Same source_email, dates within 2 days, but amount mismatch and
        merchant mismatch -> only 1 corroborating signal (date) -> WEAK
        tier with score below auto_promote_floor."""
        fin = _make_sketch(
            "hfa-finance-corr-1", "finance",
            frontmatter={
                "amount": 42.17,
                "currency": "USD",
                "counterparty": "MYSTERY",
                "source_email": "[[hfa-email-message-weak]]",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-corr-1", "purchase",
            frontmatter={
                "total": 99.99,  # amount mismatch
                "currency": "USD",
                "vendor": "Different Store",  # merchant mismatch
                "source_email": "hfa-email-message-weak",
            },
            activity_at="2024-06-11T00:00:00Z",  # 1 day -> date matches
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        assert len(out) == 1
        assert out[0].features["tier"] == "RECONCILE_TIER_SOURCE_EMAIL_WEAK"
        assert out[0].features["deterministic_score"] == 0.72
        assert out[0].features["corroborating_signal_count"] == 1
        # Score - risk = 0.72 - 0.06 = 0.66, below 0.80 auto-promote floor.
        det = out[0].features["deterministic_score"]
        risk = out[0].features["risk_penalty"]
        assert det - risk < 0.80

    def test_zero_corroborating_signals_rejected(self):
        """A bare source_email wikilink with no corroborating amount,
        date, or merchant signal must NOT produce a candidate. The
        upstream resolver is LLM-driven and bare wikilinks are a known
        false-positive source."""
        fin = _make_sketch(
            "hfa-finance-bare", "finance",
            frontmatter={
                "amount": 42.17,
                "currency": "USD",
                "counterparty": "MYSTERY",
                "source_email": "[[hfa-email-message-bare]]",
            },
            activity_at="2024-06-10T00:00:00Z",
        )
        purchase = _make_sketch(
            "hfa-purchase-bare", "purchase",
            frontmatter={
                "total": 999.99,  # amount mismatch
                "currency": "USD",
                "vendor": "Different Store",  # merchant mismatch
                "source_email": "hfa-email-message-bare",
            },
            activity_at="2024-08-01T00:00:00Z",  # 52 days -> no date match
        )
        cat = _populate_catalog([fin, purchase])
        _run_hooks(cat)
        out = fr_mod._generate_finance_reconcile_candidates(cat, fin)
        assert out == []


class TestTripClusterCityMatchStrength:
    """Trip-cluster auto-promote requires exact city match. Substring
    matches drop to a review-only tier so the candidate is still surfaced
    but never becomes an edge automatically. See
    archive_docs/runbooks/linker-quality-gates.md."""

    def test_substring_city_match_demotes_to_review(self):
        """City "san francisco bay area" substring-matches "san francisco"
        but is not exact. Result: TRIP_TIER_ACCOM_FLIGHT_LOOSE with score
        below auto_promote_floor."""
        accom = _make_sketch(
            "hfa-accommodation-loose", "accommodation",
            frontmatter={
                "address": "1 Main, San Francisco Bay Area, CA",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
        )
        flight = _make_sketch(
            "hfa-flight-loose", "flight",
            frontmatter={
                "destination_airport": "SFO",  # iata_to_city -> "san francisco"
                "arrival_at": "2024-06-10T13:00:00Z",
            },
        )
        cat = _populate_catalog([accom, flight])
        _run_hooks(cat)
        out = tc_mod._generate_trip_cluster_candidates(cat, accom)
        assert len(out) == 1
        assert out[0].features["tier"] == "TRIP_TIER_ACCOM_FLIGHT_LOOSE"
        assert out[0].features["deterministic_score"] == 0.74
        assert out[0].features["city_match_strength"] == "substring"
        det = out[0].features["deterministic_score"]
        risk = out[0].features["risk_penalty"]
        assert det - risk < 0.80

    def test_exact_city_match_auto_promotes(self):
        accom = _make_sketch(
            "hfa-accommodation-exact", "accommodation",
            frontmatter={
                "address": "1 Main, San Francisco, CA",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
        )
        flight = _make_sketch(
            "hfa-flight-exact", "flight",
            frontmatter={
                "destination_airport": "SFO",
                "arrival_at": "2024-06-10T13:00:00Z",
            },
        )
        cat = _populate_catalog([accom, flight])
        _run_hooks(cat)
        out = tc_mod._generate_trip_cluster_candidates(cat, accom)
        assert len(out) == 1
        assert out[0].features["tier"] == "TRIP_TIER_ACCOM_FLIGHT"
        assert out[0].features["deterministic_score"] == 0.92
        assert out[0].features["city_match_strength"] == "exact"

    def test_carrental_substring_city_demotes_to_review(self):
        accom = _make_sketch(
            "hfa-accommodation-cr-loose", "accommodation",
            frontmatter={
                "address": "1 Main, New York Metro, NY",
                "check_in": "2024-06-10T15:00:00Z",
                "check_out": "2024-06-13T11:00:00Z",
            },
        )
        cr = _make_sketch(
            "hfa-car-rental-loose", "car_rental",
            frontmatter={
                "pickup_location": "New York, NY",
                "pickup_at": "2024-06-10T12:00:00Z",
            },
        )
        cat = _populate_catalog([accom, cr])
        _run_hooks(cat)
        out = tc_mod._generate_trip_cluster_candidates(cat, accom)
        assert len(out) == 1
        assert out[0].features["tier"] == "TRIP_TIER_ACCOM_CARRENTAL_LOOSE"
        assert out[0].features["deterministic_score"] == 0.74
        assert out[0].features["city_match_strength"] == "substring"

"""Uber rides extractor tests."""

from __future__ import annotations

from archive_sync.extractors.uber_rides import UberRidesExtractor

UBER_TRIP_BODY = """Thanks for riding, Robbie

2024-03-15T14:30:00-07:00

Pickup: 123 Market St, SF
Dropoff: SFO Terminal 2

Fare: $32.50
Tip: $5.00
12.4 miles
28 min

Driver: Alex
Vehicle: Toyota Camry · ABC1234
"""

# Early template: label "Location:" on its own line; addresses follow.
LEGACY_LOCATION_BODY = """Thanks for riding

Pickup:
Location:
355-365 11th Street, San Francisco, CA

Dropoff:
Location:
880 Bush Street, San Francisco, CA

Total: $14.07
8 min
"""

# Charge-summary template: "Total  $xx.xx" with spaces (no colon), not only line-anchored
UBER_TOTAL_SPACED_BODY = """2026-03-08T21:03:00-07:00
Pickup: 100 1st St, San Francisco, CA
Dropoff: 200 2nd St, Oakland, CA
This document acknowledges your trip completion. Total  $18.65
Trip fare  $18.52
"""


class TestUberRidesExtractor:
    def test_matches_trip_subject(self):
        ex = UberRidesExtractor()
        assert ex.matches("noreply@uber.com", "Your Thursday trip with Uber")

    def test_rejects_uber_eats_subject(self):
        ex = UberRidesExtractor()
        assert not ex.matches("noreply@uber.com", "Your Uber Eats order is here")

    def test_parse_trip(self, sample_email_card):
        ex = UberRidesExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ub1",
            "noreply@uber.com",
            "Your trip receipt",
            UBER_TRIP_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.service == "Uber"
        assert out[0].card.pickup_location
        assert out[0].card.source_email == "[[hfa-email-message-ub1]]"

    def test_legacy_pickup_dropoff_after_location_labels(self, sample_email_card):
        ex = UberRidesExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ub-legacy",
            "noreply@uber.com",
            "Your Thursday trip with Uber",
            LEGACY_LOCATION_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "11th Street" in out[0].card.pickup_location
        assert "Bush Street" in out[0].card.dropoff_location
        assert out[0].card.pickup_location.lower() != "location"

    def test_total_with_spaces_charge_summary(self, sample_email_card):
        ex = UberRidesExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ub-total",
            "noreply@uber.com",
            "Your trip with Uber",
            UBER_TOTAL_SPACED_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert abs(out[0].card.fare - 18.65) < 0.01

"""Airbnb extractor tests."""

from __future__ import annotations

from archive_sync.extractors.airbnb import AirbnbExtractor

AIRBNB_BODY = """Reservation confirmed — confirmation HMXYZ789AB

Trip: Cozy loft downtown

Check-in: 2024-05-01
Check-out: 2024-05-05

Address: 500 Pine St, Seattle, WA

Total: $890.00
"""

AIRBNB_REMINDER_BODY = """Pack your bags!
It's almost time for your trip to Stockbridge.

Charming Stockbridge Cabin
Entire home/apt hosted by Tim

Reservation code
HM9TP2EZNM

Amount
$3634.68
"""


class TestAirbnbExtractor:
    def test_parse(self, sample_email_card):
        ex = AirbnbExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ab1",
            "automated@airbnb.com",
            "Your trip is confirmed",
            AIRBNB_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.confirmation_code
        assert "Cozy" in out[0].card.property_name or "loft" in out[0].card.property_name.lower()

    def test_reservation_code_block_and_amount_label(self, sample_email_card):
        ex = AirbnbExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ab2",
            "automated@airbnb.com",
            "Your trip is coming up",
            AIRBNB_REMINDER_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.confirmation_code == "HM9TP2EZNM"
        assert abs(out[0].card.total_cost - 3634.68) < 0.01
        assert "Stockbridge" in out[0].card.property_name or "Cabin" in out[0].card.property_name

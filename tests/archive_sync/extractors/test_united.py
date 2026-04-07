"""United Airlines extractor tests."""

from __future__ import annotations

from archive_sync.extractors.united import UnitedExtractor

UNITED_BODY = """Your trip is confirmed

Confirmation: ABCXYZ

SFO to LAX

Departure: March 20, 2024 6:00am
Arrival: March 20, 2024 8:30am

Total: $189.00
"""


NUMBER_BODY = """Itinerary

Confirmation number: NUMBER

SFO to JFK

Departure: March 20, 2024 6:00am
"""

NOISE_THEN_ROUTE_BODY = """Itinerary

Confirmation: ABCXYZ

SUB to TAL

Departure City and Time DUE to FAA

SFO to LAX

Total: $189.00
"""

# United purchase receipt style: cities with (EWR) (LAX), no "SFO to LAX" substring before seats
UNITED_PAREN_ONLY_BODY = """Confirmation Number:    PZS96Q       Flight 1 of 1 UA2238
Newark, NJ/New York, NY, US  (EWR)   Los Angeles, CA, US  (LAX)
Total:  $189.00 USD
"""

UNITED_DEPART_ARRIVE_BODY = """Confirmation number: M55YNQ
Flight: UA 448 Depart: EWR - New York/Newark on Thu, May 02 2024
Arrive: SFO - San Francisco on Thu, May 02 2024
"""


class TestUnitedExtractor:
    def test_parse(self, sample_email_card):
        ex = UnitedExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ua1",
            "notifications@united.com",
            "United confirmation",
            UNITED_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.confirmation_code == "ABCXYZ"
        assert out[0].card.origin_airport == "SFO"

    def test_rejects_number_placeholder(self, sample_email_card):
        ex = UnitedExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ua2",
            "notifications@united.com",
            "United confirmation",
            NUMBER_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 0

    def test_route_skips_non_iata_noise(self, sample_email_card):
        """Template text can contain SUB→TAL style tokens; use first real IATA pair."""
        ex = UnitedExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ua3",
            "notifications@united.com",
            "United confirmation",
            NOISE_THEN_ROUTE_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.origin_airport == "SFO"
        assert out[0].card.destination_airport == "LAX"

    def test_route_from_parenthetical_airport_codes(self, sample_email_card):
        ex = UnitedExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ua-paren",
            "notifications@united.com",
            "United receipt",
            UNITED_PAREN_ONLY_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.origin_airport == "EWR"
        assert out[0].card.destination_airport == "LAX"

    def test_route_from_depart_arrive_lines(self, sample_email_card):
        ex = UnitedExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ua-cal",
            "notifications@united.com",
            "United itinerary",
            UNITED_DEPART_ARRIVE_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.origin_airport == "EWR"
        assert out[0].card.destination_airport == "SFO"

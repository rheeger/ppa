"""Lyft extractor tests."""

from __future__ import annotations

from archive_sync.extractors.lyft import LyftExtractor

LYFT_BODY = """Your Lyft receipt

2024-04-01T18:00:00-07:00

Pickup: Mission St
Dropoff: Castro St

Total: $18.40
"""

LEGACY_LYFT_BODY = """Thanks

Pickup:
Location:
401 Main St, San Francisco, CA

Dropoff:
Location:
900 Market St, San Francisco, CA

Total: $12.00
"""

# Real template: time on Pickup/Drop-off row, address on next line; total before "You've already paid"
LYFT_STACKED_BODY = """Thanks for riding with minjie!
XL fare (26.28mi, 39m 11s)
$79.32
EWR Airport - Trip Fee
$2.50
Apple Pay (MasterCard)
$81.82
You've already paid for this ride.
This total may not match the charge on your account statement.
Pickup     10:17 PM
60 Earhart Dr, Newark, NJ
Drop-off   10:56 PM
186 Washington Park, Brooklyn, NY
"""


class TestLyftExtractor:
    def test_parse(self, sample_email_card):
        ex = LyftExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ly1",
            "receipts@lyft.com",
            "Your Friday evening Lyft ride",
            LYFT_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.service == "Lyft"
        assert out[0].card.pickup_location
        assert out[0].card.dropoff_location
        assert out[0].card.fare > 0

    def test_legacy_location_labels(self, sample_email_card):
        ex = LyftExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ly-legacy",
            "receipts@lyft.com",
            "Your Lyft ride receipt",
            LEGACY_LYFT_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Main St" in out[0].card.pickup_location
        assert "Market St" in out[0].card.dropoff_location

    def test_stacked_pickup_dropoff_and_fare_before_paid(self, sample_email_card):
        ex = LyftExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-ly-stack",
            "receipts@lyft.com",
            "Your Lyft ride receipt",
            LYFT_STACKED_BODY,
            sent_at="2024-01-15T22:00:00Z",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "Earhart" in out[0].card.pickup_location
        assert "Washington Park" in out[0].card.dropoff_location
        assert abs(out[0].card.fare - 81.82) < 0.01

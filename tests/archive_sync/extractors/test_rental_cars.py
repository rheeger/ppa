"""Rental car extractor tests."""

from __future__ import annotations

from archive_sync.extractors.rental_cars import RentalCarsExtractor

NATIONAL_BODY = """Your National reservation

Confirmation number: NATL998877

Pickup location: LAX Terminal 4
Return: LAX Terminal 4

Pickup date: April 12, 2024
Return date: April 15, 2024

Total: $210.00
"""

# National html2text: one line with "at LOS ANGELES … Your confirmation number is: 1591335071"
NATIONAL_INLINE_BODY = """Your Reservation is Confirmed.
You reserved a Full Size vehicle on December 25, 2025 at LOS ANGELES INTL ARPT . Your confirmation number is: 1591335071

Total: $99.00
"""


class TestRentalCarsExtractor:
    def test_parse_national(self, sample_email_card):
        ex = RentalCarsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-rc1",
            "email@nationalcar.com",
            "Reservation confirmed",
            NATIONAL_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.company == "National"
        assert out[0].card.confirmation_code

    def test_rejects_placeholder_confirmation_word(self, sample_email_card):
        ex = RentalCarsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-rc2",
            "email@nationalcar.com",
            "Reservation",
            """Confirmation number: NUMBER

Pickup location: LAX

Total: $99.00
""",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 0

    def test_trims_cancel_link_sentence_from_pickup(self, sample_email_card):
        ex = RentalCarsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-rc3",
            "email@nationalcar.com",
            "Reservation",
            """Confirmation: 1626267553

Pickup location: MIAMI INTL ARPT ( MIA ) please click on the link below to cancel your reservation.

Total: $100.00
""",
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert "click" not in (out[0].card.pickup_location or "").lower()
        assert "MIA" in out[0].card.pickup_location

    def test_confirmation_number_is_and_inline_at_location(self, sample_email_card):
        ex = RentalCarsExtractor()
        fm, body = sample_email_card(
            "hfa-email-message-rc4",
            "email@nationalcar.com",
            "Your reservation is confirmed",
            NATIONAL_INLINE_BODY,
        )
        out = ex.extract(fm, body, fm["uid"], "Email/x.md")
        assert len(out) == 1
        assert out[0].card.confirmation_code == "1591335071"
        assert "LOS ANGELES" in (out[0].card.pickup_location or "").upper()

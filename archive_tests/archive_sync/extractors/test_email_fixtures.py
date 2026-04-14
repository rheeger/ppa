"""Fixture-based quality tests for email extractors."""

from __future__ import annotations

from archive_sync.extractors.airbnb import AirbnbExtractor
from archive_sync.extractors.doordash import DoordashExtractor
from archive_sync.extractors.instacart import InstacartExtractor
from archive_sync.extractors.lyft import LyftExtractor
from archive_sync.extractors.rental_cars import RentalCarsExtractor
from archive_sync.extractors.shipping import ShippingExtractor
from archive_sync.extractors.uber_rides import UberRidesExtractor
from archive_sync.extractors.ubereats import UberEatsExtractor
from archive_sync.extractors.united import UnitedExtractor

from .conftest import load_email_fixture


def _assert_cards_match(out, expected: dict) -> None:
    exp_cards = expected["cards"]
    assert len(out) == len(exp_cards), (len(out), exp_cards)
    for er, exp in zip(out, exp_cards):
        d = er.card.model_dump()
        for k, v in exp.items():
            assert d.get(k) == v, f"field {k!r}: got {d.get(k)!r} expected {v!r}"


def test_doordash_receipt_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "doordash", "receipt_2023")
    out = DoordashExtractor().extract(fm, body, fm["uid"], "fixtures/doordash/receipt_2023.md")
    _assert_cards_match(out, exp)
    assert out[0].card.restaurant != "DoorDash order"


def test_doordash_compact_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "doordash", "compact_2026")
    out = DoordashExtractor().extract(fm, body, fm["uid"], "fixtures/doordash/compact_2026.md")
    _assert_cards_match(out, exp)
    assert "Panini" in out[0].card.restaurant


def test_doordash_promo_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "doordash", "promo_summer")
    out = DoordashExtractor().extract(fm, body, fm["uid"], "fixtures/doordash/promo_summer.md")
    _assert_cards_match(out, exp)


def test_uber_rides_trip_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "uber_rides", "trip_receipt_nyc")
    out = UberRidesExtractor().extract(fm, body, fm["uid"], "fixtures/uber_rides/trip_receipt_nyc.md")
    _assert_cards_match(out, exp)


def test_united_confirmation_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "united", "confirmation_sfo_jfk")
    out = UnitedExtractor().extract(fm, body, fm["uid"], "fixtures/united/confirmation_sfo_jfk.md")
    _assert_cards_match(out, exp)
    assert out[0].card.confirmation_code != "NUMBER"


def test_shipping_ups_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "shipping", "ups_tracking")
    out = ShippingExtractor().extract(fm, body, fm["uid"], "fixtures/shipping/ups_tracking.md")
    _assert_cards_match(out, exp)


def test_airbnb_booking_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "airbnb", "booking_loft")
    out = AirbnbExtractor().extract(fm, body, fm["uid"], "fixtures/airbnb/booking_loft.md")
    _assert_cards_match(out, exp)
    assert "Airbnb stay" not in (out[0].card.property_name or "")


def test_ubereats_order_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "ubereats", "order_receipt")
    out = UberEatsExtractor().extract(fm, body, fm["uid"], "fixtures/ubereats/order_receipt.md")
    _assert_cards_match(out, exp)


def test_instacart_costco_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "instacart", "costco_order")
    out = InstacartExtractor().extract(fm, body, fm["uid"], "fixtures/instacart/costco_order.md")
    _assert_cards_match(out, exp)
    assert out[0].card.store.lower() != "instacart"


def test_rental_national_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "rental_cars", "national_lax")
    out = RentalCarsExtractor().extract(fm, body, fm["uid"], "fixtures/rental_cars/national_lax.md")
    _assert_cards_match(out, exp)


def test_lyft_ride_fixture(email_fixture_dir):
    fm, body, exp = load_email_fixture(email_fixture_dir, "lyft", "ride_receipt")
    out = LyftExtractor().extract(fm, body, fm["uid"], "fixtures/lyft/ride_receipt.md")
    _assert_cards_match(out, exp)

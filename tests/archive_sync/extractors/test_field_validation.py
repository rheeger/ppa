"""Tests for archive_sync.extractors.field_validation."""

from __future__ import annotations

from archive_sync.extractors.field_validation import (
    validate_field,
    validate_provenance_round_trip,
)


def test_restaurant_rejects_url():
    assert validate_field("meal_order", "restaurant", "WARNING <https://www.p65warnings.ca.gov>") is None


def test_restaurant_rejects_footer_noise():
    assert validate_field("meal_order", "restaurant", "your bank statement shortly.") is None


def test_restaurant_rejects_long_string():
    assert validate_field("meal_order", "restaurant", "Mixt - Valencia " + "x" * 120) is None


def test_restaurant_accepts_normal_name():
    assert validate_field("meal_order", "restaurant", "Mixt - Valencia") == "Mixt - Valencia"


def test_airport_rejects_non_iata():
    assert validate_field("flight", "origin_airport", "SUB") is None
    assert validate_field("flight", "destination_airport", "FAA") is None


def test_airport_accepts_valid_iata():
    assert validate_field("flight", "origin_airport", "SFO") == "SFO"
    assert validate_field("flight", "destination_airport", "JFK") == "JFK"


def test_confirmation_rejects_english_word():
    assert validate_field("accommodation", "confirmation_code", "THAT") is None
    assert validate_field("car_rental", "confirmation_code", "VACATION") is None
    assert validate_field("car_rental", "confirmation_code", "CONFIRMATION") is None


def test_confirmation_accepts_valid_code():
    assert validate_field("accommodation", "confirmation_code", "765188312") == "765188312"
    assert validate_field("car_rental", "confirmation_code", "K5881551950") == "K5881551950"


def test_flight_confirmation_rejects_non_6char():
    assert validate_field("flight", "confirmation_code", "AB") is None
    assert validate_field("flight", "confirmation_code", "NUMBER") is None


def test_flight_confirmation_accepts_valid_pnr():
    assert validate_field("flight", "confirmation_code", "PK30W7") == "PK30W7"


def test_fare_rejects_miles():
    assert validate_field("flight", "fare_amount", 540000.0) is None


def test_fare_accepts_normal():
    assert validate_field("flight", "fare_amount", 568.37) == 568.37


def test_property_name_rejects_review_text():
    assert (
        validate_field(
            "accommodation",
            "property_name",
            "and communicated their plans incredibly well. We would absolutely host Robbie again.",
        )
        is None
    )


def test_property_name_rejects_long():
    assert validate_field("accommodation", "property_name", "x" * 201) is None


def test_property_name_accepts_normal():
    assert validate_field("accommodation", "property_name", "Sunny loft near the park") == "Sunny loft near the park"


def test_location_rejects_bare_label():
    assert validate_field("ride", "pickup_location", "Location:") is None


def test_location_rejects_too_short():
    assert validate_field("ride", "dropoff_location", "LA") is None


def test_location_rejects_paragraph():
    assert validate_field("car_rental", "pickup_location", "area. Go out the terminal " + "x" * 300) is None


def test_location_accepts_normal():
    assert (
        validate_field("ride", "pickup_location", "2919-2923 23rd St, San Francisco, CA")
        == "2919-2923 23rd St, San Francisco, CA"
    )


def test_check_in_rejects_non_date():
    assert validate_field("accommodation", "check_in", "as soon as possible!") is None
    assert validate_field("accommodation", "check_in", "Checkout") is None


def test_check_in_accepts_date():
    assert validate_field("accommodation", "check_in", "May 1, 2024") == "May 1, 2024"


def test_delivered_at_rejects_template_text():
    assert (
        validate_field(
            "shipment",
            "delivered_at",
            "by, based on the selected service, destination and ship date. Limitations and ex",
        )
        is None
    )


def test_flight_time_rejects_template_label():
    assert validate_field("flight", "departure_at", "City and Time") is None
    assert validate_field("flight", "arrival_at", "Cabin") is None


def test_ride_fare_rejects_implausible():
    assert validate_field("ride", "fare", 1200.0) is None


def test_unknown_field_passes_through():
    assert validate_field("meal_order", "mode", "delivery") == "delivery"


def test_round_trip_passes_when_value_in_source():
    card_data = {"type": "meal_order", "restaurant": "Mixt - Valencia", "total": 16.62}
    source = "Your order from Mixt - Valencia\nSubtotal: $15.31\nTotal: $16.62"
    warnings = validate_provenance_round_trip(card_data, source, "meal_order")
    assert len(warnings) == 0


def test_round_trip_warns_when_value_not_in_source():
    card_data = {"type": "meal_order", "restaurant": "PHANTOM RESTAURANT"}
    source = "Your order from Mixt - Valencia\nTotal: $16.62"
    warnings = validate_provenance_round_trip(card_data, source, "meal_order")
    assert any("restaurant" in w for w in warnings)


def test_round_trip_ignores_system_fields():
    card_data = {"uid": "hfa-meal-order-abc", "type": "meal_order", "source": ["email_extraction"]}
    warnings = validate_provenance_round_trip(card_data, "irrelevant body", "meal_order")
    assert len(warnings) == 0


def test_round_trip_handles_whitespace_normalization():
    card_data = {"type": "ride", "pickup_location": "2919-2923  23rd  St,  San Francisco"}
    source = "2919-2923 23rd St, San Francisco"
    warnings = validate_provenance_round_trip(card_data, source, "ride")
    assert len(warnings) == 0

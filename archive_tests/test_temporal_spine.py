"""Phase 1: temporal spine helpers and parsing."""

from __future__ import annotations

from archive_cli.features import card_activity_at, card_activity_end_at, parse_timestamp_to_utc


def test_parse_timestamp_to_utc_offset():
    dt = parse_timestamp_to_utc("2025-12-27T20:14:00-08:00")
    assert dt is not None
    assert dt.tzinfo is not None


def test_parse_date_only_uses_default_tz(monkeypatch):
    monkeypatch.setenv("PPA_DEFAULT_TIMEZONE", "America/Los_Angeles")
    from archive_cli.index_config import get_default_timezone

    assert get_default_timezone() == "America/Los_Angeles"
    dt = parse_timestamp_to_utc("2025-12-27")
    assert dt is not None


def test_card_activity_at_cascade():
    assert "2025-01-02" in card_activity_at({"departure_at": "2025-01-02T00:00:00Z"})


def test_card_activity_end_at_flight():
    assert card_activity_end_at("flight", {"arrival_at": "2025-01-03"}) == "2025-01-03"

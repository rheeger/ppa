"""Tests for ExtractorRegistry."""

from __future__ import annotations

from archive_sync.extractors.base import EmailExtractor
from archive_sync.extractors.registry import ExtractorRegistry, build_default_registry
from archive_sync.extractors.uber_rides import UberRidesExtractor
from archive_sync.extractors.ubereats import UberEatsExtractor


class _Stub(EmailExtractor):
    def __init__(self, sid: str, patterns: list[str]):
        self._sid = sid
        self.sender_patterns = patterns
        self.output_card_type = "meal_order"

    @property
    def extractor_id(self) -> str:
        return self._sid

    def template_versions(self):
        return []

    def summary_only_fallback(self, fm, body, suid, srp):
        return []


def test_register_and_match():
    r = ExtractorRegistry()
    r.register(_Stub("a", [r".*@foo\.com$"]))
    assert r.match("x@foo.com", "").extractor_id == "a"


def test_match_returns_none_for_unknown():
    r = ExtractorRegistry()
    r.register(_Stub("a", [r".*@foo\.com$"]))
    assert r.match("x@bar.com", "") is None


def test_domain_index_groups_by_domain():
    r = ExtractorRegistry()
    r.register(_Stub("dd", [r".*@doordash\.com$"]))
    idx = r.domain_index()
    assert "doordash.com" in idx
    assert idx["doordash.com"][0].extractor_id == "dd"


def test_disambiguation_uber_eats_vs_rides():
    r = ExtractorRegistry()
    r.register(UberEatsExtractor())
    r.register(UberRidesExtractor())
    assert r.match("ubereats@uber.com", "Anything").extractor_id == "uber_eats"
    assert r.match("noreply@uber.com", "Your Uber Eats order").extractor_id == "uber_eats"
    m = r.match("noreply@uber.com", "Your Thursday trip with Uber")
    assert m is not None
    assert m.extractor_id == "uber_rides"


def test_no_false_positive_personal_email():
    r = ExtractorRegistry()
    r.register(UberRidesExtractor())
    assert r.match("bob@uber.com", "Catch up soon?") is None


def test_build_default_registry_returns_all():
    r = build_default_registry()
    ids = sorted(e.extractor_id for e in r.all_extractors())
    assert "doordash" in ids
    assert "amazon" in ids
    assert "uber_rides" in ids
    assert "uber_eats" in ids
    assert len(ids) >= 10

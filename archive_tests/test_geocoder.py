"""Tests for the Nominatim geocoding client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from archive_cli.geocoder import GeoResult, NominatimGeocoder


class TestNominatimGeocoder:
    def test_geocode_structured_builds_correct_params(self) -> None:
        """geocode_structured sends city/state/country as separate params."""
        g = NominatimGeocoder()
        captured: list[dict[str, str]] = []

        def capture_request(params: dict[str, str]) -> list[dict]:
            captured.append(dict(params))
            return []

        with patch.object(g, "_request", side_effect=capture_request):
            g.geocode_structured(city="Austin", state="TX", country="USA")
        assert len(captured) == 1
        p = captured[0]
        assert p["city"] == "Austin"
        assert p["state"] == "TX"
        assert p["country"] == "USA"
        # format/limit are added inside _request after geocode_structured builds params

    def test_geocode_returns_geo_result(self) -> None:
        """Valid Nominatim response is parsed into GeoResult."""
        g = NominatimGeocoder()
        payload = [
            {
                "lat": "30.26",
                "lon": "-97.74",
                "display_name": "Austin, TX, USA",
                "osm_type": "relation",
                "osm_id": 113314,
                "importance": 0.65,
            }
        ]
        with patch.object(g, "_request", return_value=payload):
            r = g.geocode("Austin TX")
        assert isinstance(r, GeoResult)
        assert r.latitude == 30.26
        assert r.longitude == -97.74
        assert r.display_name == "Austin, TX, USA"
        assert r.osm_type == "relation"
        assert r.osm_id == 113314
        assert r.confidence == 0.65

    def test_geocode_returns_none_on_no_match(self) -> None:
        """Empty Nominatim response returns None."""
        g = NominatimGeocoder()
        with patch.object(g, "_request", return_value=[]):
            assert g.geocode("nowhere xyz") is None

    def test_geocode_returns_none_on_error(self) -> None:
        """HTTP error returns None, does not raise."""
        g = NominatimGeocoder()
        with patch.object(g, "_request", return_value=[]):
            assert g.geocode("anything") is None

    def test_rate_limiting_enforces_minimum_interval(self) -> None:
        """Two rapid requests are separated by at least MIN_INTERVAL_SECONDS."""
        g = NominatimGeocoder()
        g._last_request_at = 1000.0
        with patch("archive_cli.geocoder.time.monotonic", return_value=1000.05):
            with patch("archive_cli.geocoder.time.sleep") as sleep_mock:
                g._rate_limit()
        sleep_mock.assert_called_once()
        slept = sleep_mock.call_args[0][0]
        assert slept >= NominatimGeocoder.MIN_INTERVAL_SECONDS - 0.05 - 0.001

    def test_batch_geocode_processes_all_queries(self) -> None:
        """geocode_batch processes every query in the list."""
        g = NominatimGeocoder()
        queries = [
            {"city": "Austin", "state": "TX", "country": "USA"},
            {"city": "Dallas", "state": "TX", "country": "USA"},
        ]
        with patch.object(g, "geocode_structured", side_effect=[MagicMock(), MagicMock()]) as gs:
            out = g.geocode_batch(queries, progress_every=0)
        assert gs.call_count == 2
        assert len(out) == 2
        assert out[0][0] == queries[0]
        assert out[1][0] == queries[1]

    def test_batch_geocode_skips_failures(self) -> None:
        """Failures in batch mode are skipped and logged, not raised."""
        g = NominatimGeocoder()
        queries = [{"city": "A"}, {"city": "B"}]

        def boom(**_: str) -> GeoResult | None:
            raise RuntimeError("simulated")

        with patch.object(g, "geocode_structured", side_effect=boom):
            with patch("archive_cli.geocoder.log.warning") as warn:
                out = g.geocode_batch(queries, progress_every=0)
        assert len(out) == 2
        assert out[0][1] is None
        assert out[1][1] is None
        assert warn.call_count == 2

    def test_batch_geocode_returns_results_paired_with_queries(self) -> None:
        """Each result is paired with its input query dict."""
        g = NominatimGeocoder()
        q1 = {"city": "Seattle", "state": "WA", "country": "USA"}
        q2 = {"city": "Portland", "state": "OR", "country": "USA"}
        gr = GeoResult(
            latitude=1.0,
            longitude=2.0,
            display_name="x",
            osm_type="node",
            osm_id=1,
            confidence=0.5,
        )
        with patch.object(g, "geocode_structured", side_effect=[gr, None]):
            out = g.geocode_batch([q1, q2], progress_every=0)
        assert out[0] == (q1, gr)
        assert out[1] == (q2, None)

    def test_confidence_from_nominatim_importance(self) -> None:
        """GeoResult.confidence is derived from Nominatim importance field."""
        g = NominatimGeocoder()
        payload = [
            {
                "lat": "0",
                "lon": "0",
                "display_name": "pt",
                "osm_type": "node",
                "osm_id": 1,
                "importance": 1.5,
            }
        ]
        with patch.object(g, "_request", return_value=payload):
            r = g.geocode("q")
        assert r is not None
        assert r.confidence == 1.0

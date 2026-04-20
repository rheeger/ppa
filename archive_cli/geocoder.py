"""Geocoding client using OpenStreetMap Nominatim (free, no API key).

Usage policy: max 1 request/second, custom User-Agent required.
See https://operations.osmfoundation.org/policies/nominatim/
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

log = logging.getLogger("ppa.geocoder")


@dataclass(frozen=True)
class GeoResult:
    latitude: float
    longitude: float
    display_name: str
    osm_type: str
    osm_id: int
    confidence: float


class NominatimGeocoder:
    BASE_URL = "https://nominatim.openstreetmap.org/search"
    MIN_INTERVAL_SECONDS = 1.1

    def __init__(self, user_agent: str = "PPA-ArchiveGeocoder/1.0"):
        self._user_agent = user_agent
        self._last_request_at: float = 0.0

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.MIN_INTERVAL_SECONDS:
            time.sleep(self.MIN_INTERVAL_SECONDS - elapsed)

    def _request(self, params: dict[str, str]) -> list[dict[str, Any]]:
        self._rate_limit()
        params["format"] = "json"
        params["limit"] = "1"
        url = f"{self.BASE_URL}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": self._user_agent})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._last_request_at = time.monotonic()
                return json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            log.warning("Geocode request failed: %s", exc)
            self._last_request_at = time.monotonic()
            return []

    def _parse_result(self, data: list[dict[str, Any]]) -> GeoResult | None:
        if not data:
            return None
        item = data[0]
        raw_osm_id = item.get("osm_id", 0)
        try:
            osm_id = int(raw_osm_id) if raw_osm_id is not None else 0
        except (TypeError, ValueError):
            osm_id = 0
        return GeoResult(
            latitude=float(item.get("lat", 0)),
            longitude=float(item.get("lon", 0)),
            display_name=str(item.get("display_name", "")),
            osm_type=str(item.get("osm_type", "")),
            osm_id=osm_id,
            confidence=min(1.0, float(item.get("importance", 0.5))),
        )

    def geocode(self, query: str) -> GeoResult | None:
        return self._parse_result(self._request({"q": query}))

    def geocode_structured(
        self,
        *,
        name: str = "",
        street: str = "",
        city: str = "",
        state: str = "",
        country: str = "",
    ) -> GeoResult | None:
        params: dict[str, str] = {}
        if street:
            params["street"] = street
        if city:
            params["city"] = city
        if state:
            params["state"] = state
        if country:
            params["country"] = country
        if name and not street:
            params["q"] = f"{name}, {city}" if city else name
        if not params:
            return None
        return self._parse_result(self._request(params))

    def geocode_batch(
        self,
        queries: list[dict[str, str]],
        progress_every: int = 50,
    ) -> list[tuple[dict[str, str], GeoResult | None]]:
        results: list[tuple[dict[str, str], GeoResult | None]] = []
        for i, q in enumerate(queries, 1):
            try:
                result = self.geocode_structured(**q)
            except Exception as exc:
                log.warning("Batch geocode item %d failed: %s", i, exc)
                result = None
            results.append((q, result))
            if progress_every and i % progress_every == 0:
                log.info("Geocoded %d / %d", i, len(queries))
        return results

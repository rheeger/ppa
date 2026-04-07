"""Registry mapping sender patterns to extractors."""

from __future__ import annotations

import re
from typing import Optional

from archive_sync.extractors.base import EmailExtractor


def _domains_from_sender_patterns(patterns: list[str]) -> list[str]:
    """Best-effort domain literals from regex patterns like .*@doordash\\.com$."""
    found: list[str] = []
    seen: set[str] = set()
    for pattern in patterns:
        normalized = pattern.replace("\\.", ".")
        for m in re.finditer(r"@([\w.-]+\.[\w.-]+)", normalized):
            dom = m.group(1).lower().strip(".")
            if dom and dom not in seen:
                seen.add(dom)
                found.append(dom)
    return found


class ExtractorRegistry:
    """Maps sender email addresses to extractors."""

    def __init__(self) -> None:
        self._extractors: list[EmailExtractor] = []
        self._domain_cache: dict[str, list[EmailExtractor]] | None = None

    def register(self, extractor: EmailExtractor) -> None:
        """Add an extractor. Invalidates domain cache."""
        self._extractors.append(extractor)
        self._domain_cache = None

    def match(self, from_email: str, subject: str) -> Optional[EmailExtractor]:
        """Return the first matching extractor, or None."""
        for extractor in self._extractors:
            if extractor.matches(from_email, subject):
                return extractor
        return None

    def domain_index(self) -> dict[str, list[EmailExtractor]]:
        """Pre-computed domain -> extractors mapping for O(1) lookup in runner scan."""
        if self._domain_cache is not None:
            return self._domain_cache
        idx: dict[str, list[EmailExtractor]] = {}
        for extractor in self._extractors:
            domains = _domains_from_sender_patterns(extractor.sender_patterns)
            bucket_domains = domains if domains else ["*"]
            for dom in bucket_domains:
                idx.setdefault(dom, []).append(extractor)
        self._domain_cache = idx
        return self._domain_cache

    def all_extractors(self) -> list[EmailExtractor]:
        """Return all registered extractors."""
        return list(self._extractors)


def build_default_registry() -> ExtractorRegistry:
    """Construct registry with all known extractors.

    Registration order matters for overlapping sender domains (e.g. Uber Eats vs rides).
    """
    from archive_sync.extractors.airbnb import AirbnbExtractor
    from archive_sync.extractors.amazon import AmazonExtractor
    from archive_sync.extractors.doordash import DoordashExtractor
    from archive_sync.extractors.instacart import InstacartExtractor
    from archive_sync.extractors.lyft import LyftExtractor
    from archive_sync.extractors.rental_cars import RentalCarsExtractor
    from archive_sync.extractors.shipping import ShippingExtractor
    from archive_sync.extractors.uber_rides import UberRidesExtractor
    from archive_sync.extractors.ubereats import UberEatsExtractor
    from archive_sync.extractors.united import UnitedExtractor

    registry = ExtractorRegistry()
    registry.register(UberEatsExtractor())
    registry.register(UberRidesExtractor())
    registry.register(DoordashExtractor())
    registry.register(AmazonExtractor())
    registry.register(InstacartExtractor())
    registry.register(ShippingExtractor())
    registry.register(LyftExtractor())
    registry.register(UnitedExtractor())
    registry.register(AirbnbExtractor())
    registry.register(RentalCarsExtractor())
    return registry

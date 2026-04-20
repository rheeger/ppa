"""Bundled IATA code -> city lookup for Phase 6.5 MODULE_TRIP_CLUSTER.

Uses a static CSV at archive_cli/data/iata_cities.csv rather than Nominatim
(the existing archive_cli/geocoder.py would be 1 req/sec rate-limited and
unreliable for bare IATA codes).

The CSV is curated for the ~150 airports most represented in the seed plus
major international hubs. Expanding coverage: append rows; no format changes
needed.
"""

from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path

_DATA_PATH = Path(__file__).parent / "data" / "iata_cities.csv"


@lru_cache(maxsize=1)
def _iata_table() -> dict[str, str]:
    """Return a dict mapping uppercase IATA code -> city name."""
    table: dict[str, str] = {}
    if not _DATA_PATH.exists():
        return table
    with _DATA_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = (row.get("iata_code") or "").strip().upper()
            city = (row.get("city") or "").strip()
            if code and city and code not in table:
                table[code] = city
    return table


def iata_to_city(code: str | None) -> str | None:
    """Look up city name for an IATA airport code.

    Returns None for unknown codes, empty input, or missing CSV.
    """
    if not code:
        return None
    return _iata_table().get(code.strip().upper())


def iata_coverage_count() -> int:
    """Total IATA codes in the bundled table (for health / diagnostics)."""
    return len(_iata_table())

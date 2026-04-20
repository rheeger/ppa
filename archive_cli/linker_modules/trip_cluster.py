"""MODULE_TRIP_CLUSTER -- Phase 6.5 deterministic trip-cluster linker.

Clusters travel-related derived cards (accommodation, flight, car_rental) by
city + date-overlap. All tiers emit LINK_TYPE_PART_OF_TRIP.

  TRIP_TIER_ACCOM_FLIGHT (0.92, HIGH, auto-promote):
      iata_to_city(flight.destination_airport) == _city_of(accommodation.address)
      via *exact normalized equality*
      AND flight.arrival_at in [accommodation.check_in - 24h, .check_out + 24h]

  TRIP_TIER_ACCOM_FLIGHT_LOOSE (0.74, review-only):
      Same predicates but the city match is substring-only rather than exact.
      The substring fallback exists because some address strings parse oddly
      ("San Francisco Bay Area" vs "San Francisco"); we still surface those
      candidates but never auto-promote. See
      archive_docs/runbooks/linker-quality-gates.md for the rationale.

  TRIP_TIER_ACCOM_CARRENTAL (0.90, HIGH, auto-promote):
      Same pattern, substituting car_rental.pickup_location + .pickup_at;
      exact city match required.

  TRIP_TIER_ACCOM_CARRENTAL_LOOSE (0.74, review-only):
      Substring city match fallback for accommodation ↔ car_rental.

  TRIP_TIER_FLIGHT_CARRENTAL (0.85, MEDIUM, review) — currently unimplemented;
      reserved for the flight↔car_rental same-day predicate.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from archive_cli import linker_framework as lf
from archive_cli.iata import iata_to_city
from archive_cli.seed_links import (LINK_TYPE_PART_OF_TRIP, LinkEvidence,
                                    SeedCardSketch, SeedLinkCandidate,
                                    SeedLinkCatalog, _append_candidate,
                                    _clean_text, _day_key)

MODULE_TRIP_CLUSTER = "tripClusterLinker"


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = datetime.fromisoformat(s[:10])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


_PUNCT = re.compile(r"[^\w\s]")


def _city_of(address: str | None) -> str | None:
    """Extract a city name from a US-style ", "-delimited address.

    Heuristic: second-to-last comma-delimited segment (handles "1 Main St,
    San Francisco, CA 94107, USA" -> "San Francisco"). Falls back to the
    first non-empty segment for short addresses. Returns lowercase +
    stripped-punctuation form for matching.
    """
    if not address:
        return None
    parts = [p.strip() for p in str(address).split(",") if p.strip()]
    if not parts:
        return None
    # Prefer second-to-last if >=3 segments ("street, city, region[/country]").
    if len(parts) >= 3:
        candidate = parts[-2]
    elif len(parts) == 2:
        candidate = parts[0]
    else:
        candidate = parts[0]
    # Drop anything that looks like a zip or numeric-heavy token.
    clean = _PUNCT.sub(" ", candidate).strip().lower()
    # Collapse whitespace.
    clean = " ".join(clean.split())
    return clean or None


def _city_match_strength(a: str | None, b: str | None) -> str:
    """Classify the agreement between two city strings.

    Returns ``"exact"`` for normalized equality, ``"substring"`` for a
    legacy lenient match (min length 4 to avoid "LA" matching everything),
    or ``"none"``. Per archive_docs/runbooks/linker-quality-gates.md, only
    ``"exact"`` is allowed to auto-promote; ``"substring"`` falls through
    to a review-only tier so the candidate is still surfaced for human
    inspection without becoming an edge.
    """
    if not a or not b:
        return "none"
    a = a.lower().strip()
    b = b.lower().strip()
    if a == b:
        return "exact"
    if len(a) >= 4 and a in b:
        return "substring"
    if len(b) >= 4 and b in a:
        return "substring"
    return "none"


def _city_matches(a: str | None, b: str | None) -> bool:
    """Backwards-compatible wrapper: any non-"none" strength counts as a
    match for predicate filtering. Tier assignment uses
    :func:`_city_match_strength` to choose between auto-promote and
    review-only scores."""
    return _city_match_strength(a, b) != "none"


def _build_trip_date_buckets(catalog: SeedLinkCatalog) -> None:
    """post_build_hook: index flights/car_rentals by YYYY-MM-DD arrival/pickup day."""
    flights_by_day: dict[str, list[SeedCardSketch]] = {}
    carrentals_by_day: dict[str, list[SeedCardSketch]] = {}
    for flight in catalog.cards_by_type.get("flight", []):
        for field in ("arrival_at", "departure_at"):
            key = _day_key(_clean_text(flight.frontmatter.get(field, "")))
            if key:
                flights_by_day.setdefault(key, []).append(flight)
    for cr in catalog.cards_by_type.get("car_rental", []):
        key = _day_key(_clean_text(cr.frontmatter.get("pickup_at", "")))
        if key:
            carrentals_by_day.setdefault(key, []).append(cr)
    lf.set_private_index(catalog, "flights_by_date_bucket", flights_by_day)
    lf.set_private_index(catalog, "car_rentals_by_date_bucket", carrentals_by_day)


def _daterange(start: datetime, end: datetime) -> list[str]:
    """YYYY-MM-DD strings from start.date() to end.date() inclusive."""
    if end < start:
        return []
    out: list[str] = []
    cur = start.date()
    last = end.date()
    while cur <= last:
        out.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return out


def _generate_trip_cluster_candidates(
    catalog: SeedLinkCatalog, source: SeedCardSketch,
) -> list[SeedLinkCandidate]:
    if source.card_type != "accommodation":
        return []
    results: list[SeedLinkCandidate] = []
    seen: set[str] = set()

    address = source.frontmatter.get("address", "")
    city = _city_of(address)
    if not city:
        return results

    check_in = _parse_ts(source.frontmatter.get("check_in"))
    check_out = _parse_ts(source.frontmatter.get("check_out"))
    if check_in is None or check_out is None:
        return results

    window_start = check_in - timedelta(days=1)
    window_end = check_out + timedelta(days=1)
    window_days = _daterange(window_start, window_end)

    flights_by_day = lf.get_private_index(catalog, "flights_by_date_bucket")
    carrentals_by_day = lf.get_private_index(catalog, "car_rentals_by_date_bucket")

    # Tier 1 -- accommodation ↔ flight.
    for day in window_days:
        for flight in flights_by_day.get(day, []):
            if flight.uid == source.uid or flight.uid in seen:
                continue
            arrival = _parse_ts(flight.frontmatter.get("arrival_at"))
            if arrival is None or not (window_start <= arrival <= window_end):
                continue
            airport = (flight.frontmatter.get("destination_airport") or "").upper()
            flight_city = iata_to_city(airport) or ""
            strength = _city_match_strength(flight_city, city)
            if strength == "none":
                continue
            seen.add(flight.uid)
            if strength == "exact":
                tier = "TRIP_TIER_ACCOM_FLIGHT"
                det = 0.92
                risk = 0.0
            else:
                tier = "TRIP_TIER_ACCOM_FLIGHT_LOOSE"
                det = 0.74
                risk = 0.04
            _append_trip_cluster(
                results, source, flight,
                tier=tier,
                deterministic_score=det,
                risk_penalty=risk,
                extra_features={
                    "matched_city": city,
                    "city_match_strength": strength,
                    "airport": airport,
                    "flight_city": flight_city,
                    "arrival_offset_h": round(
                        (arrival - check_in).total_seconds() / 3600, 1
                    ),
                },
            )

    # Tier 2 -- accommodation ↔ car_rental.
    for day in window_days:
        for cr in carrentals_by_day.get(day, []):
            if cr.uid == source.uid or cr.uid in seen:
                continue
            pickup = _parse_ts(cr.frontmatter.get("pickup_at"))
            if pickup is None or not (window_start <= pickup <= window_end):
                continue
            cr_city = _city_of(cr.frontmatter.get("pickup_location", ""))
            strength = _city_match_strength(cr_city, city)
            if strength == "none":
                continue
            seen.add(cr.uid)
            if strength == "exact":
                tier = "TRIP_TIER_ACCOM_CARRENTAL"
                det = 0.90
                risk = 0.0
            else:
                tier = "TRIP_TIER_ACCOM_CARRENTAL_LOOSE"
                det = 0.74
                risk = 0.04
            _append_trip_cluster(
                results, source, cr,
                tier=tier,
                deterministic_score=det,
                risk_penalty=risk,
                extra_features={
                    "matched_city": city,
                    "city_match_strength": strength,
                    "carrental_city": cr_city,
                    "pickup_offset_h": round(
                        (pickup - check_in).total_seconds() / 3600, 1
                    ),
                },
            )

    return results


def _append_trip_cluster(
    results: list[SeedLinkCandidate],
    source: SeedCardSketch,
    target: SeedCardSketch,
    *,
    tier: str,
    deterministic_score: float,
    risk_penalty: float,
    extra_features: dict[str, Any],
) -> None:
    features: dict[str, Any] = {
        "tier": tier,
        "deterministic_score": deterministic_score,
        "risk_penalty": risk_penalty,
    }
    features.update(extra_features)
    evidences = [
        LinkEvidence(
            evidence_type="predicate_match",
            evidence_source="trip_cluster",
            feature_name="tier",
            feature_value=tier,
            feature_weight=deterministic_score,
            raw_payload_json=dict(extra_features),
        ),
    ]
    _append_candidate(
        results,
        module_name=MODULE_TRIP_CLUSTER,
        source=source,
        target=target,
        proposed_link_type=LINK_TYPE_PART_OF_TRIP,
        candidate_group=f"trip_cluster:{tier}",
        features=features,
        evidences=evidences,
    )


def _score_trip_cluster_features(
    features: dict[str, Any],
) -> tuple[float, float, float, float, float]:
    det = float(features.get("deterministic_score", 0.0))
    risk = float(features.get("risk_penalty", 0.0))
    return det, 0.0, 0.0, 0.0, risk


lf.register_linker(lf.LinkerSpec(
    module_name=MODULE_TRIP_CLUSTER,
    source_card_types=("accommodation",),
    emits_link_types=(LINK_TYPE_PART_OF_TRIP,),
    generator=_generate_trip_cluster_candidates,
    scoring_fn=_score_trip_cluster_features,
    scoring_mode="deterministic",
    policies=(),  # LINK_TYPE_PART_OF_TRIP policy already registered in seed_links
    requires_llm_judge=False,
    lifecycle_state="active",
    phase_owner="phase_6.5",
    post_promotion_action="edges_only",
    description=(
        "Clusters accommodation/flight/car_rental cards into a trip via "
        "IATA-city + date-overlap predicates."
    ),
    post_build_hook=_build_trip_date_buckets,
))

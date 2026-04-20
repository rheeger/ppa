"""Geocode PlaceCards — populate latitude/longitude from Nominatim/OSM."""

from __future__ import annotations

import logging
import time
from typing import Any

log = logging.getLogger("ppa.geocode")


def geocode_places(
    *,
    store: Any,
    dry_run: bool = False,
    limit: int = 0,
) -> dict[str, Any]:
    """Geocode PlaceCards that have address data but no lat/lng.

    1. Query places projection for candidates
    2. Call Nominatim for each
    3. Update vault frontmatter with lat/lng
    4. Run incremental rebuild to update Postgres
    """
    from ..geocoder import NominatimGeocoder

    started = time.time()
    vault_root = store.vault
    candidates = _find_geocode_candidates(store)
    total = len(candidates)
    if limit > 0:
        candidates = candidates[:limit]
    log.info("Geocode candidates: %d total, processing %d", total, len(candidates))

    if dry_run:
        return {"total_candidates": total, "would_process": len(candidates), "dry_run": True}

    geocoder = NominatimGeocoder()
    geocoded = 0
    failed = 0
    skipped = 0

    for card in candidates:
        result = geocoder.geocode_structured(
            name=str(card.get("name") or ""),
            street=str(card.get("address") or ""),
            city=str(card.get("city") or ""),
            state=str(card.get("state") or ""),
            country=str(card.get("country") or ""),
        )
        if result is None:
            failed += 1
            log.warning("No geocode result for %s", card.get("card_uid", "?"))
            continue

        rel_path = str(card.get("rel_path") or "")
        if not rel_path:
            skipped += 1
            continue

        from archive_vault.vault import update_frontmatter_fields

        update_frontmatter_fields(
            vault_root,
            rel_path,
            {"latitude": result.latitude, "longitude": result.longitude},
        )
        geocoded += 1

    rebuild_time = 0.0
    if geocoded > 0:
        log.info("Running incremental rebuild for %d geocoded PlaceCards", geocoded)
        rebuild_started = time.time()
        store.rebuild(force_full=False)
        rebuild_time = round(time.time() - rebuild_started, 2)
        log.info("Incremental rebuild completed in %.1fs", rebuild_time)

    elapsed = round(time.time() - started, 2)
    return {
        "total_candidates": total,
        "geocoded": geocoded,
        "failed": failed,
        "skipped": skipped,
        "rebuild_time_seconds": rebuild_time,
        "elapsed_seconds": elapsed,
    }


def _find_geocode_candidates(store: Any) -> list[dict[str, Any]]:
    """Query Postgres for PlaceCards missing lat/lng but having address data."""
    schema = store.index.schema
    sql = f"""
            SELECT
                p.card_uid AS card_uid,
                p.name AS name,
                p.address AS address,
                p.city AS city,
                p.state AS state,
                p.country AS country,
                c.rel_path AS rel_path
            FROM {schema}.places p
            JOIN {schema}.cards c ON c.uid = p.card_uid
            WHERE (p.latitude IS NULL OR p.latitude = 0)
              AND (
                COALESCE(p.address, '') != ''
                OR (COALESCE(p.city, '') != '' AND COALESCE(p.state, '') != '')
              )
            ORDER BY p.card_uid
            """
    with store.index._connect() as conn:
        rows = conn.execute(sql).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "card_uid": r["card_uid"],
                "name": r["name"] or "",
                "address": r["address"] or "",
                "city": r["city"] or "",
                "state": r["state"] or "",
                "country": r["country"] or "",
                "rel_path": r["rel_path"] or "",
            }
        )
    return out

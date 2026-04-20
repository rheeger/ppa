"""Tests for ppa geocode-places command."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from archive_cli.commands.geocode import geocode_places
from archive_cli.geocoder import GeoResult


class TestGeocodePlaces:
    def test_finds_candidates_missing_lat_lng(self) -> None:
        """Candidates are PlaceCards where latitude IS NULL or latitude = 0."""
        store = MagicMock()
        store.index.schema = "ppa"
        cm = MagicMock()
        cm.__enter__.return_value.execute.return_value.fetchall.return_value = [
            {
                "card_uid": "u1",
                "name": "X",
                "address": "",
                "city": "Austin",
                "state": "TX",
                "country": "USA",
                "rel_path": "Places/x.md",
            }
        ]
        cm.__exit__.return_value = None
        store.index._connect.return_value = cm
        from archive_cli.commands import geocode as geocode_mod

        rows = geocode_mod._find_geocode_candidates(store)
        assert len(rows) == 1
        assert rows[0]["card_uid"] == "u1"

    def test_dry_run_reports_without_modifying(self) -> None:
        """--dry-run prints candidate count, writes nothing."""
        store = MagicMock()
        store.vault = Path("/tmp/vault")
        with patch(
            "archive_cli.commands.geocode._find_geocode_candidates",
            return_value=[{"card_uid": "a"}] * 3,
        ):
            r = geocode_places(store=store, dry_run=True, limit=2)
        assert r["dry_run"] is True
        assert r["total_candidates"] == 3
        assert r["would_process"] == 2
        store.rebuild.assert_not_called()

    def test_calls_nominatim_for_each_candidate(self) -> None:
        """Each candidate PlaceCard is passed to geocoder.geocode_structured."""
        store = MagicMock()
        store.vault = Path("/tmp/vault")
        cands = [
            {
                "card_uid": "1",
                "name": "N",
                "address": "1 Main",
                "city": "Austin",
                "state": "TX",
                "country": "USA",
                "rel_path": "Places/a.md",
            },
        ]
        gr = GeoResult(1.0, 2.0, "d", "n", 1, 0.5)
        with patch("archive_cli.commands.geocode._find_geocode_candidates", return_value=cands):
            with patch("archive_cli.geocoder.NominatimGeocoder") as G:
                G.return_value.geocode_structured.return_value = gr
                with patch("archive_vault.vault.update_frontmatter_fields"):
                    geocode_places(store=store, dry_run=False, limit=0)
        G.return_value.geocode_structured.assert_called_once()

    def test_writes_lat_lng_to_vault_frontmatter(self) -> None:
        """Successful geocode updates the vault .md file with latitude/longitude."""
        store = MagicMock()
        store.vault = Path("/tmp/vault")
        cands = [
            {
                "card_uid": "1",
                "name": "N",
                "address": "",
                "city": "Austin",
                "state": "TX",
                "country": "USA",
                "rel_path": "Places/a.md",
            },
        ]
        gr = GeoResult(30.0, -97.0, "d", "n", 1, 0.5)
        with patch("archive_cli.commands.geocode._find_geocode_candidates", return_value=cands):
            with patch("archive_cli.geocoder.NominatimGeocoder") as G:
                G.return_value.geocode_structured.return_value = gr
                with patch("archive_vault.vault.update_frontmatter_fields") as uf:
                    geocode_places(store=store, dry_run=False, limit=0)
        uf.assert_called_once()
        args = uf.call_args[0]
        assert args[2] == {"latitude": 30.0, "longitude": -97.0}

    def test_triggers_incremental_rebuild(self) -> None:
        """After geocoding, an incremental rebuild runs to update Postgres."""
        store = MagicMock()
        store.vault = Path("/tmp/vault")
        cands = [
            {
                "card_uid": "1",
                "name": "N",
                "address": "",
                "city": "Austin",
                "state": "TX",
                "country": "USA",
                "rel_path": "Places/a.md",
            },
        ]
        gr = GeoResult(1.0, 2.0, "d", "n", 1, 0.5)
        with patch("archive_cli.commands.geocode._find_geocode_candidates", return_value=cands):
            with patch("archive_cli.geocoder.NominatimGeocoder") as G:
                G.return_value.geocode_structured.return_value = gr
                with patch("archive_vault.vault.update_frontmatter_fields"):
                    geocode_places(store=store, dry_run=False, limit=0)
        store.rebuild.assert_called_once_with(force_full=False)

    def test_returns_metrics(self) -> None:
        """Return dict has total_candidates, geocoded, failed, skipped keys."""
        store = MagicMock()
        store.vault = Path("/tmp/vault")
        with patch("archive_cli.commands.geocode._find_geocode_candidates", return_value=[]):
            r = geocode_places(store=store, dry_run=False, limit=0)
        for k in ("total_candidates", "geocoded", "failed", "skipped", "elapsed_seconds"):
            assert k in r

    def test_handles_no_candidates_gracefully(self) -> None:
        """With no PlaceCards to geocode, returns zeros without error."""
        store = MagicMock()
        store.vault = Path("/tmp/vault")
        with patch("archive_cli.commands.geocode._find_geocode_candidates", return_value=[]):
            r = geocode_places(store=store, dry_run=False, limit=0)
        assert r["geocoded"] == 0
        assert r["failed"] == 0
        store.rebuild.assert_not_called()

    def test_limit_restricts_count(self) -> None:
        """--limit 5 processes at most 5 PlaceCards."""
        store = MagicMock()
        store.vault = Path("/tmp/vault")
        cands = [
            {
                "card_uid": str(i),
                "name": "N",
                "address": "",
                "city": "Austin",
                "state": "TX",
                "country": "USA",
                "rel_path": f"Places/{i}.md",
            }
            for i in range(10)
        ]
        gr = GeoResult(1.0, 2.0, "d", "n", 1, 0.5)
        with patch("archive_cli.commands.geocode._find_geocode_candidates", return_value=cands):
            with patch("archive_cli.geocoder.NominatimGeocoder") as G:
                G.return_value.geocode_structured.return_value = gr
                with patch("archive_vault.vault.update_frontmatter_fields"):
                    r = geocode_places(store=store, dry_run=False, limit=5)
        assert G.return_value.geocode_structured.call_count == 5
        assert r["geocoded"] == 5

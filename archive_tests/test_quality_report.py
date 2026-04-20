"""Tests for ppa quality-report command."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from archive_cli.commands.quality_report import quality_report


class TestQualityReport:
    def test_report_structure_has_per_type_entries(self) -> None:
        mock_conn = MagicMock()
        r1 = MagicMock()
        r1.fetchall.return_value = [
            ("document", 2, 0.5, 0.5, 0.4, 0.6),
        ]
        r2 = MagicMock()
        r2.fetchall.return_value = []
        mock_conn.execute.side_effect = [r1, r2]
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_conn
        mock_cm.__exit__.return_value = None
        with patch("psycopg.connect", return_value=mock_cm):
            out = quality_report(dsn="postgresql://x", schema="ppa")
        assert len(out["types"]) == 1
        assert out["types"][0]["type"] == "document"
        assert out["types"][0]["count"] == 2

    def test_report_includes_avg_median_p10_p90(self) -> None:
        mock_conn = MagicMock()
        r1 = MagicMock()
        r1.fetchall.return_value = [
            ("meal_order", 1, 0.9, 0.9, 0.9, 0.9),
        ]
        r2 = MagicMock()
        r2.fetchall.return_value = []
        mock_conn.execute.side_effect = [r1, r2]
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_conn
        mock_cm.__exit__.return_value = None
        with patch("psycopg.connect", return_value=mock_cm):
            out = quality_report(dsn="postgresql://x", schema="ppa")
        t0 = out["types"][0]
        assert t0["avg_score"] == 0.9
        assert t0["median"] == 0.9
        assert t0["p10"] == 0.9
        assert t0["p90"] == 0.9

    def test_report_includes_common_flags(self) -> None:
        mock_conn = MagicMock()
        r1 = MagicMock()
        r1.fetchall.return_value = [
            ("document", 3, 0.4, 0.4, 0.3, 0.5),
        ]
        r2 = MagicMock()
        r2.fetchall.return_value = [
            ("document", "missing:summary", 2),
        ]
        mock_conn.execute.side_effect = [r1, r2]
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_conn
        mock_cm.__exit__.return_value = None
        with patch("psycopg.connect", return_value=mock_cm):
            out = quality_report(dsn="postgresql://x", schema="ppa")
        flags = out["types"][0]["common_flags"]
        assert len(flags) >= 1
        assert flags[0]["flag"] == "missing:summary"

    def test_report_empty_index_returns_empty(self) -> None:
        mock_conn = MagicMock()
        r1 = MagicMock()
        r1.fetchall.return_value = []
        r2 = MagicMock()
        r2.fetchall.return_value = []
        mock_conn.execute.side_effect = [r1, r2]
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_conn
        mock_cm.__exit__.return_value = None
        with patch("psycopg.connect", return_value=mock_cm):
            out = quality_report(dsn="postgresql://x", schema="ppa")
        assert out["types"] == []
        assert out["summary"]["total_cards"] == 0

    @pytest.mark.integration
    def test_derived_cards_with_items_score_higher(self, pgvector_dsn: str, tmp_path, monkeypatch) -> None:
        """After a fixture rebuild, meal_order median is non-zero when meal_order cards exist."""
        import uuid

        from archive_cli.index_store import PostgresArchiveIndex
        from archive_tests.fixtures import load_fixture_vault

        vault = tmp_path / "v"
        load_fixture_vault(vault)
        schema = f"qr_{uuid.uuid4().hex[:10]}"
        monkeypatch.setenv("PPA_PATH", str(vault))
        monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
        monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
        monkeypatch.setenv("PPA_VECTOR_DIMENSION", "8")

        index = PostgresArchiveIndex(vault, dsn=pgvector_dsn)
        index.schema = schema
        index.bootstrap()
        index.rebuild_with_metrics(force_full=True, workers=2, progress_every=0)

        out = quality_report(dsn=pgvector_dsn, schema=schema)
        meal = next((t for t in out["types"] if t["type"] == "meal_order"), None)
        if meal is None:
            pytest.skip("no meal_order rows in fixture vault")
        assert meal["median"] > 0.0

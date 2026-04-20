"""Tests for Phase 6 Tier 4: card_classifications projection + semantic linker filter."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.seed_links import (
    DEFAULT_SEMANTIC_SKIP_CLASSIFICATIONS,
    _is_classification_skipped,
    _semantic_skip_classifications,
)


@pytest.fixture(autouse=True)
def _enable_seed_links(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_SEED_LINKS_ENABLED", "1")


def test_default_skip_set_matches_email_triage_classifications():
    """The skip set must cover both v1 (classify.py) and v2 (triage.py) category names."""
    s = DEFAULT_SEMANTIC_SKIP_CLASSIFICATIONS
    # v1 classify.py categories that mean junk
    assert "marketing" in s
    assert "automated" in s
    assert "noise" in s
    assert "personal" in s
    # v2 triage.py SKIP_CLASSIFICATIONS aliases
    assert "automated_notification" in s
    assert "person_to_person" in s
    # transactional / booking / etc. must NOT be skipped
    assert "transactional" not in s
    assert "transactional_receipt" not in s
    assert "booking_confirmation" not in s


def test_env_override_replaces_skip_set(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PPA_SEMANTIC_SKIP_CLASSIFICATIONS", "marketing,noise")
    assert _semantic_skip_classifications() == frozenset({"marketing", "noise"})


def test_env_override_empty_uses_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("PPA_SEMANTIC_SKIP_CLASSIFICATIONS", raising=False)
    assert _semantic_skip_classifications() == DEFAULT_SEMANTIC_SKIP_CLASSIFICATIONS


@pytest.mark.integration
class TestClassificationFilter:
    SCHEMA = "ppa_t4_filter"

    def _bootstrap(self, dsn: str, tmp_path: Path) -> PostgresArchiveIndex:
        idx = PostgresArchiveIndex(vault=tmp_path, dsn=dsn)
        idx.schema = self.SCHEMA
        with idx._connect() as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {self.SCHEMA} CASCADE")
            conn.execute(f"CREATE SCHEMA {self.SCHEMA}")
            idx._create_schema(conn)
            runner = MigrationRunner(conn, self.SCHEMA)
            runner.ensure_table()
            runner.run()
        return idx

    def _seed(self, idx, classifications: list[tuple[str, str]]) -> None:
        with idx._connect() as conn:
            for uid, classification in classifications:
                conn.execute(
                    f"""
                    INSERT INTO {idx.schema}.card_classifications
                        (card_uid, classification, confidence, classify_model)
                    VALUES (%s, %s, 0.9, 'mock')
                    """,
                    (uid, classification),
                )
            conn.commit()

    def test_skip_returns_true_for_marketing(self, pgvector_dsn: str, tmp_path: Path) -> None:
        idx = self._bootstrap(pgvector_dsn, tmp_path)
        self._seed(idx, [("u-marketing", "marketing"), ("u-trans", "transactional")])
        with idx._connect() as conn:
            assert _is_classification_skipped(conn, idx.schema, "u-marketing") is True
            assert _is_classification_skipped(conn, idx.schema, "u-trans") is False

    def test_skip_returns_false_for_unclassified(self, pgvector_dsn: str, tmp_path: Path) -> None:
        """Cards without a classification row are never skipped (safe-default-include)."""
        idx = self._bootstrap(pgvector_dsn, tmp_path)
        with idx._connect() as conn:
            assert _is_classification_skipped(conn, idx.schema, "u-unknown") is False

    def test_table_absent_returns_false(self, pgvector_dsn: str, tmp_path: Path) -> None:
        """If card_classifications doesn't exist (older schema), never skip."""
        import psycopg
        with psycopg.connect(pgvector_dsn) as conn:
            schema = "ppa_t4_no_table"
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.execute(f"CREATE SCHEMA {schema}")
            conn.commit()
            assert _is_classification_skipped(conn, schema, "u-anything") is False

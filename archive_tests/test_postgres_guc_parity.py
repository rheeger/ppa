"""Verify Postgres GUC parity between test and production configs."""

from __future__ import annotations

from pathlib import Path

import pytest

EXPECTED_GUCS = {
    "shared_buffers": "256MB",
    "work_mem": "64MB",
    "maintenance_work_mem": "256MB",
    "effective_cache_size": "512MB",
}

PPA_ROOT = Path(__file__).resolve().parents[1]
HEY_ARNOLD_ROOT = PPA_ROOT.parent / "hey-arnold"
TEST_CONF = PPA_ROOT / "archive_tests" / "docker" / "postgres-test.conf"
PROD_CONF = HEY_ARNOLD_ROOT / "config" / "postgres.conf"


def _parse_postgres_conf(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, val = line.partition("=")
        result[key.strip()] = val.strip().strip("'\"")
    return result


def test_test_conf_has_expected_gucs() -> None:
    if not TEST_CONF.exists():
        pytest.skip(f"{TEST_CONF} not found")
    parsed = _parse_postgres_conf(TEST_CONF)
    for key, expected in EXPECTED_GUCS.items():
        assert parsed.get(key) == expected, f"{key}: expected {expected}, got {parsed.get(key)}"


def test_production_conf_has_expected_gucs() -> None:
    if not PROD_CONF.exists():
        pytest.skip(f"{PROD_CONF} not found")
    parsed = _parse_postgres_conf(PROD_CONF)
    for key, expected in EXPECTED_GUCS.items():
        assert parsed.get(key) == expected, f"{key}: expected {expected}, got {parsed.get(key)}"


def test_production_conf_matches_test_conf() -> None:
    if not TEST_CONF.exists() or not PROD_CONF.exists():
        pytest.skip("Config files not found")
    test_gucs = _parse_postgres_conf(TEST_CONF)
    prod_gucs = _parse_postgres_conf(PROD_CONF)
    for key in EXPECTED_GUCS:
        assert test_gucs.get(key) == prod_gucs.get(key), (
            f"{key} mismatch: test={test_gucs.get(key)}, prod={prod_gucs.get(key)}"
        )


@pytest.mark.integration
def test_postgres_gucs_applied(pgvector_dsn: str) -> None:
    """Verify GUCs are applied in the running test Postgres instance."""
    import psycopg

    with psycopg.connect(pgvector_dsn) as conn:
        for guc, expected in EXPECTED_GUCS.items():
            row = conn.execute(f"SHOW {guc}").fetchone()
            actual = row[0]
            assert actual == expected, f"{guc}: expected {expected}, got {actual}"

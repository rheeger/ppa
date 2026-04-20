"""Root pytest configuration: shared markers and Postgres fixtures."""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid

import pytest
from psycopg import connect


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: requires a running Postgres instance")
    config.addinivalue_line("markers", "slow: long-running tests (>30 seconds)")
    config.addinivalue_line("markers", "openai: requires OpenAI API key (skipped in CI)")


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, text=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False
    return True


def _pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_postgres(dsn: str, *, timeout_seconds: float = 45.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with connect(dsn) as conn:
                conn.execute("SELECT 1")
            return
        except Exception:
            time.sleep(1.0)
    raise RuntimeError(f"Timed out waiting for Postgres at {dsn}")


PGVECTOR_IMAGE = "pgvector/pgvector:pg17"


@pytest.fixture(scope="session")
def pgvector_dsn() -> str:
    """Ephemeral Postgres+pgvector in Docker for integration tests."""
    # Tests reuse fixed schema names like "archive_resume_test"; bootstrap()
    # refuses to recreate populated schemas (added 2026-04-24 after the embedding
    # wipe incident). Tests get a session-scoped force so reruns against a
    # persistent local Postgres don't regress. Production callers must opt in
    # explicitly via --force / PPA_BOOTSTRAP_FORCE=1.
    os.environ["PPA_BOOTSTRAP_FORCE"] = "1"
    preferred = os.environ.get("PPA_TEST_PG_DSN", "").strip()
    if preferred:
        wait_for_postgres(preferred)
        yield preferred
        return
    if not _docker_available():
        pytest.skip("Docker is required for live Postgres tests")
    container_name = f"ppa-test-{uuid.uuid4().hex[:10]}"
    port = _pick_port()
    dsn = f"postgresql://archive:archive@127.0.0.1:{port}/archive"
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "-d",
            "--name",
            container_name,
            "-e",
            "POSTGRES_USER=archive",
            "-e",
            "POSTGRES_PASSWORD=archive",
            "-e",
            "POSTGRES_DB=archive",
            "-p",
            f"127.0.0.1:{port}:5432",
            PGVECTOR_IMAGE,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        wait_for_postgres(dsn)
        yield dsn
    finally:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            check=False,
            capture_output=True,
            text=True,
        )

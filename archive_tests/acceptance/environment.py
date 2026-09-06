"""Isolated vault / Postgres / Rust runtime for acceptance runs."""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import socket
import subprocess
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

from psycopg import connect
from psycopg.rows import dict_row

from archive_tests.conftest import (
    OWNED_ROOT_MARKER,
    PGVECTOR_IMAGE,
    assert_owned_test_root,
    wait_for_postgres,
)

logger = logging.getLogger("ppa.acceptance")

SEED_PATH_MARKERS = (
    "hf-archives-seed",
    "/Archive/seed/",
    "/Archive/vault",
    "/Users/rheeger/Archive/",
)

FORBIDDEN_INHERITED_KEYS = (
    "PPA_TEST_PG_DSN",
    "PPA_CONFIG_PATH",
    "PPA_SERVING_INDEX_PATH",
    "PPA_QUERY_EMBED_CACHE_PATH",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
)

PROVIDER_ENV_KEYS = (
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "GOOGLE_API_KEY",
)


class IsolationError(RuntimeError):
    """Inherited production / shared-test configuration is forbidden."""


class IntegrationRequiredError(RuntimeError):
    """Required Docker or native engine is missing under fail-closed mode."""


@dataclass
class IsolatedRuntime:
    """Owned vault, schema, and optional Docker container for one run."""

    root: Path
    vault: Path
    dsn: str
    schema: str
    serving_index_path: Path
    query_embed_cache_path: Path
    container_name: str | None
    engine: dict[str, Any]
    env: Mapping[str, str]
    owned_marker: Path
    previous_env: dict[str, str | None] = field(default_factory=dict)

    def apply(self) -> None:
        """Install isolated environment variables for the current process."""

        self.previous_env = {key: os.environ.get(key) for key in set(self.env) | set(FORBIDDEN_INHERITED_KEYS)}
        for key in FORBIDDEN_INHERITED_KEYS:
            os.environ.pop(key, None)
        os.environ.update(self.env)

    def restore(self) -> None:
        """Restore process environment captured by :meth:`apply`."""

        for key, value in self.previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def cleanup(self) -> None:
        """Destroy only resources this run created."""

        if self.container_name:
            subprocess.run(
                ["docker", "rm", "-f", self.container_name],
                check=False,
                capture_output=True,
                text=True,
            )
            logger.info("cleaned owned container name=%s", self.container_name)
        self.container_name = None


def docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], check=True, capture_output=True, text=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False
    return True


def _pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def imported_engine_identity() -> dict[str, Any]:
    """Return the imported native extension path and content hash."""

    try:
        import archive_crate
    except ImportError as exc:
        raise IntegrationRequiredError(f"native engine missing: archive_crate import failed: {exc}") from exc
    from importlib.machinery import EXTENSION_SUFFIXES

    module = archive_crate.walk_vault.__module__
    native = __import__(module, fromlist=["*"])
    path = Path(getattr(native, "__file__", "") or "")
    if not path.is_file() or not any(str(path).endswith(suffix) for suffix in EXTENSION_SUFFIXES):
        raise IntegrationRequiredError(f"archive_crate did not load a native extension: {path}")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "module": module,
        "path": str(path.resolve()),
        "sha256": digest,
        "has_serving_index_open": hasattr(archive_crate, "serving_index_open"),
        "has_serving_index_search": hasattr(archive_crate, "serving_index_search"),
    }


def require_rust_engine(*, require_integration: bool) -> dict[str, Any]:
    engine = os.environ.get("PPA_ENGINE", "rust").strip().lower() or "rust"
    if engine != "rust":
        message = f"PPA_ENGINE={engine!r}; rust serving is required for acceptance"
        if require_integration:
            raise IntegrationRequiredError(message)
        raise IsolationError(message)
    try:
        identity = imported_engine_identity()
    except IntegrationRequiredError:
        if require_integration:
            raise
        raise IsolationError("native engine missing") from None
    return identity


def validate_no_production_config(environ: Mapping[str, str] | None = None) -> None:
    """Refuse inherited production DSN / seed vault / shared test DSN."""

    env = os.environ if environ is None else environ
    inherited = env.get("PPA_TEST_PG_DSN", "").strip()
    if inherited:
        raise IsolationError("PPA_TEST_PG_DSN must be unset; acceptance owns an ephemeral database")
    vault = env.get("PPA_PATH", "")
    if vault and any(marker in vault for marker in SEED_PATH_MARKERS):
        raise IsolationError(f"refusing inherited seed/production vault PPA_PATH={vault}")
    dsn = env.get("PPA_INDEX_DSN", "")
    if dsn and "127.0.0.1" not in dsn and "localhost" not in dsn:
        raise IsolationError("refusing non-loopback PPA_INDEX_DSN")
    serving = env.get("PPA_SERVING_INDEX_PATH", "")
    if serving and any(marker in serving for marker in SEED_PATH_MARKERS):
        raise IsolationError(f"refusing inherited serving path {serving}")


def _write_owned_marker(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    marker = root / OWNED_ROOT_MARKER
    marker.write_text("ppa-acceptance-owned\n", encoding="utf-8")
    return marker


def provision_isolated_runtime(
    root: Path,
    *,
    require_integration: bool,
    seed: int = 0,
    suite: str = "baseline",
) -> IsolatedRuntime:
    """Create a unique owned vault + ephemeral Postgres (unique container)."""

    # CI integration sets a job-level shared DSN; this runtime still owns its own.
    os.environ.pop("PPA_TEST_PG_DSN", None)
    validate_no_production_config()
    engine = require_rust_engine(require_integration=require_integration)
    if not docker_available():
        message = "Docker is required for live Postgres tests"
        if require_integration:
            raise IntegrationRequiredError(message)
        raise IsolationError(message)

    root = Path(root).resolve()
    marker = _write_owned_marker(root)
    assert_owned_test_root(root)
    run_id = uuid.uuid4().hex[:12]
    vault = root / "vault"
    serving = root / "rust-search-index"
    embed_cache = root / "query-embed-cache.sqlite3"
    for leftover in (vault, serving, embed_cache):
        if leftover.is_dir():
            shutil.rmtree(leftover)
        elif leftover.is_file():
            leftover.unlink()
    schema = f"plan_p04_{run_id}"
    container_name = f"ppa-p04-{run_id}"
    port = _pick_port()
    dsn = f"postgresql://archive:archive@127.0.0.1:{port}/archive"

    logger.info("starting owned postgres container=%s port=%s schema=%s", container_name, port, schema)
    try:
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
        wait_for_postgres(dsn)
    except Exception:
        subprocess.run(["docker", "rm", "-f", container_name], check=False, capture_output=True, text=True)
        raise

    env = {
        "PPA_PLAN_ID": "p04",
        "PPA_PATH": str(vault),
        "PPA_INDEX_DSN": dsn,
        "PPA_INDEX_SCHEMA": schema,
        "PPA_EMBEDDING_PROVIDER": "hash",
        "PPA_EMBEDDING_MODEL": "archive-hash-dev",
        "PPA_EMBEDDING_VERSION": "1",
        "PPA_VECTOR_DIMENSION": "8",
        "PPA_ENGINE": "rust",
        "PPA_SERVING_INDEX_PATH": str(serving),
        "PPA_QUERY_EMBED_CACHE_PATH": str(embed_cache),
        "PPA_BOOTSTRAP_FORCE": "1",
        "PPA_ACCEPTANCE_SEED": str(int(seed)),
        "PPA_ACCEPTANCE_SUITE": suite,
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    return IsolatedRuntime(
        root=root,
        vault=vault,
        dsn=dsn,
        schema=schema,
        serving_index_path=serving,
        query_embed_cache_path=embed_cache,
        container_name=container_name,
        engine=engine,
        env=MappingProxyType(env),
        owned_marker=marker,
    )


def reset_serving_handle() -> None:
    from archive_cli.serving_index import close_serving_handles
    from archive_engine.publication import clear_publication_pins

    close_serving_handles()
    clear_publication_pins()


def inspect_warehouse_card(dsn: str, schema: str, uid: str) -> dict[str, Any] | None:
    """Independent Postgres read of one card row (not via serving)."""

    with connect(dsn, row_factory=dict_row) as conn:
        row = conn.execute(
            f"SELECT uid, rel_path, type, summary FROM {schema}.cards WHERE uid = %s",
            (uid,),
        ).fetchone()
    if row is None:
        return None
    return {str(key): row[key] for key in row}


def rmtree_owned(root: Path) -> None:
    """Delete an owned runtime root after the marker has been checked."""

    assert_owned_test_root(root)
    if root.exists():
        shutil.rmtree(root)

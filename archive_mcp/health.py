"""PPA health check subsystem.

Provides a single function that independently checks each component of the PPA
stack (vault, postgres, embeddings, migrations) and returns a structured report.
Each check is isolated — a failure in one does not prevent the others from running.

Designed for:
- CLI: `ppa health` with exit code 0/1
- CI: fast pre-test validation that the stack is wired correctly
- Arnold: remote diagnostics over SSH without needing MCP
- Agent UX: a tool that quickly answers "is the archive working?"
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from .embedding_provider import get_embedding_provider
from .index_config import _ppa_env, get_index_dsn, get_index_schema
from .migrate import MigrationRunner


def _default_vault_path() -> Path:
    return Path(
        _ppa_env("PPA_PATH", default=str(Path.home() / "Archive" / "vault"))
    ).expanduser()


def _check_vault(vault_path: Path) -> dict[str, Any]:
    """Count ``*.md`` one level under the vault (root ``*.md`` + each subdir)."""
    try:
        if not vault_path.is_dir():
            return {
                "ok": False,
                "error": f"vault path is not a directory: {vault_path}",
            }
        note_count = 0
        for md in vault_path.glob("*.md"):
            if md.is_file():
                note_count += 1
        for child in vault_path.iterdir():
            if child.is_dir():
                note_count += sum(1 for p in child.glob("*.md") if p.is_file())
        return {
            "ok": True,
            "path": str(vault_path.resolve()),
            "note_count": note_count,
        }
    except OSError as exc:
        return {"ok": False, "error": str(exc)}


def _check_postgres(vault_path: Path, dsn: str, schema: str) -> dict[str, Any]:
    """Connect with the same timeouts as :meth:`PostgresArchiveIndex._connect`."""
    try:
        from .index_store import PostgresArchiveIndex

        started = time.monotonic()
        index = PostgresArchiveIndex(vault_path, dsn=dsn)
        with index._connect() as conn:
            connect_ms = int((time.monotonic() - started) * 1000)
            row = conn.execute(f"SELECT COUNT(*) AS c FROM {schema}.cards").fetchone()
            card_count = int(row["c"] if isinstance(row, dict) else row[0])
        return {
            "ok": True,
            "schema": schema,
            "connect_ms": connect_ms,
            "card_count": card_count,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _check_embeddings() -> dict[str, Any]:
    """Instantiate the configured embedding provider (no remote probe)."""
    try:
        provider = get_embedding_provider()
        return {
            "ok": True,
            "provider": getattr(provider, "name", type(provider).__name__),
            "model": getattr(provider, "model", ""),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _check_migrations(dsn: str, schema: str) -> dict[str, Any]:
    try:
        from .index_store import PostgresArchiveIndex

        vault = Path(os.devnull)
        index = PostgresArchiveIndex(vault, dsn=dsn)
        with index._connect() as conn:
            runner = MigrationRunner(conn, schema)
            status = runner.status()
        pending = int(status.get("pending_count", 0))
        applied = int(status.get("applied_count", 0))
        return {
            "ok": True,
            "pending": pending,
            "applied": applied,
            "pending_versions": status.get("pending_versions", []),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def run_health_checks(
    vault_path: str | Path | None = None,
    dsn: str | None = None,
    schema: str | None = None,
) -> dict[str, Any]:
    """Run isolated health checks and return a structured report.

    When ``vault_path``, ``dsn``, or ``schema`` is omitted, values are taken from
    the same environment as the rest of PPA (``PPA_PATH``, ``PPA_INDEX_DSN``,
    ``PPA_INDEX_SCHEMA``).
    """
    t0 = time.monotonic()
    vault = Path(vault_path) if vault_path is not None else _default_vault_path()
    resolved_dsn = (dsn or get_index_dsn()).strip()
    resolved_schema = schema or get_index_schema()

    checks: dict[str, Any] = {
        "vault": _check_vault(vault),
    }

    if not resolved_dsn:
        checks["postgres"] = {
            "ok": False,
            "error": "PPA_INDEX_DSN is not set",
        }
        checks["migrations"] = {
            "ok": False,
            "error": "PPA_INDEX_DSN is not set",
        }
    else:
        checks["postgres"] = _check_postgres(vault, resolved_dsn, resolved_schema)
        checks["migrations"] = _check_migrations(resolved_dsn, resolved_schema)

    checks["embeddings"] = _check_embeddings()

    ok = all(isinstance(c, dict) and c.get("ok") is True for c in checks.values())
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    return {
        "ok": ok,
        "checks": checks,
        "elapsed_ms": elapsed_ms,
    }


def health_report_json(report: dict[str, Any]) -> str:
    """Serialize a health report for CLI output."""
    return json.dumps(report, indent=2)

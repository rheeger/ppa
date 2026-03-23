"""PPA schema migration runner.

Manages a `schema_migrations` table and applies numbered migrations
discovered from the `archive_mcp.migrations` package.

Usage from code:
    runner = MigrationRunner(conn, schema="archive_seed")
    result = runner.run()          # apply all pending
    status = runner.status()       # report current state

The runner is idempotent: re-running skips already-applied migrations.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .migrations import Migration, discover_migrations


@dataclass(slots=True)
class MigrationResult:
    applied: list[int] = field(default_factory=list)
    already_applied: list[int] = field(default_factory=list)
    failed: int | None = None
    error: str = ""
    elapsed_ms: float = 0.0


class MigrationRunner:
    """Discovers and applies pending schema migrations."""

    def __init__(self, conn: Any, schema: str):
        self.conn = conn
        self.schema = schema
        self._migrations: list[Migration] | None = None

    @property
    def migrations(self) -> list[Migration]:
        if self._migrations is None:
            self._migrations = discover_migrations()
        return self._migrations

    def ensure_table(self) -> None:
        """Create the schema_migrations table if it doesn't exist."""
        self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
        self.conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        self.conn.commit()

    def applied_versions(self) -> set[int]:
        """Return the set of migration versions already applied."""
        try:
            rows = self.conn.execute(
                f"SELECT version FROM {self.schema}.schema_migrations ORDER BY version"
            ).fetchall()
            return {int(row["version"]) if isinstance(row, dict) else int(row[0]) for row in rows}
        except Exception:
            return set()

    def pending(self) -> list[Migration]:
        """Return migrations that have not yet been applied, in order."""
        applied = self.applied_versions()
        return [m for m in self.migrations if m.version not in applied]

    def run(self, *, dry_run: bool = False) -> MigrationResult:
        """Apply all pending migrations in version order.

        Each migration runs in its own transaction (auto-commit after each).
        If a migration fails, the runner stops and reports the failure;
        previously applied migrations in this run are already committed.
        """
        started = time.monotonic()
        self.ensure_table()
        applied = self.applied_versions()
        result = MigrationResult()
        result.already_applied = sorted(applied)

        for migration in self.migrations:
            if migration.version in applied:
                continue
            if dry_run:
                result.applied.append(migration.version)
                continue
            try:
                migration.upgrade(self.conn, self.schema)
                self.conn.execute(
                    f"""
                    INSERT INTO {self.schema}.schema_migrations (version, name)
                    VALUES (%s, %s)
                    ON CONFLICT (version) DO NOTHING
                    """,
                    (migration.version, migration.name),
                )
                self.conn.commit()
                result.applied.append(migration.version)
            except Exception as exc:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                result.failed = migration.version
                result.error = str(exc)
                break

        result.elapsed_ms = round((time.monotonic() - started) * 1000, 2)
        return result

    def mark_applied(self, version: int, name: str) -> None:
        """Mark a migration as applied without running it.

        Used for baselining existing databases whose schema already matches.
        """
        self.ensure_table()
        self.conn.execute(
            f"""
            INSERT INTO {self.schema}.schema_migrations (version, name)
            VALUES (%s, %s)
            ON CONFLICT (version) DO NOTHING
            """,
            (version, name),
        )
        self.conn.commit()

    def mark_all_applied(self) -> list[int]:
        """Mark all known migrations as applied. For fresh installs where
        _create_schema() already created the full current schema."""
        self.ensure_table()
        marked: list[int] = []
        applied = self.applied_versions()
        for migration in self.migrations:
            if migration.version not in applied:
                self.conn.execute(
                    f"""
                    INSERT INTO {self.schema}.schema_migrations (version, name)
                    VALUES (%s, %s)
                    ON CONFLICT (version) DO NOTHING
                    """,
                    (migration.version, migration.name),
                )
                marked.append(migration.version)
        if marked:
            self.conn.commit()
        return marked

    def status(self) -> dict[str, Any]:
        """Return migration status for diagnostics."""
        self.ensure_table()
        applied = self.applied_versions()
        all_versions = [m.version for m in self.migrations]
        pending = [m.version for m in self.migrations if m.version not in applied]
        latest_applied = max(applied) if applied else None

        rows = []
        try:
            fetched = self.conn.execute(
                f"SELECT version, name, applied_at FROM {self.schema}.schema_migrations ORDER BY version"
            ).fetchall()
            for row in fetched:
                if isinstance(row, dict):
                    rows.append({"version": row["version"], "name": row["name"], "applied_at": str(row["applied_at"])})
                else:
                    rows.append({"version": row[0], "name": row[1], "applied_at": str(row[2])})
        except Exception:
            pass

        return {
            "total_migrations": len(all_versions),
            "applied_count": len(applied),
            "pending_count": len(pending),
            "pending_versions": pending,
            "latest_applied": latest_applied,
            "applied": rows,
        }

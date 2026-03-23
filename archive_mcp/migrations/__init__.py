"""Numbered SQL migrations for PPA schema evolution.

Each migration is a Python module in this package with:
- VERSION: int — unique, ascending migration number
- NAME: str — short human-readable label
- upgrade(conn, schema: str) -> None — forward DDL/DML
- downgrade(conn, schema: str) -> None — optional rollback

Migrations are discovered by scanning this package for modules whose names
match the pattern NNN_*.py (e.g. 001_baseline.py, 002_add_foo_column.py).
"""

from __future__ import annotations

import importlib
import pkgutil
import re
from dataclasses import dataclass
from typing import Any, Callable

_MIGRATION_RE = re.compile(r"^(\d{3})_.+$")


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    upgrade: Callable[[Any, str], None]
    downgrade: Callable[[Any, str], None] | None


def discover_migrations() -> list[Migration]:
    """Return all migrations in this package, sorted by version ascending."""
    migrations: list[Migration] = []
    for importer, modname, ispkg in pkgutil.iter_modules(__path__):
        match = _MIGRATION_RE.match(modname)
        if not match:
            continue
        mod = importlib.import_module(f".{modname}", __package__)
        version = getattr(mod, "VERSION", None)
        name = getattr(mod, "NAME", modname)
        upgrade_fn = getattr(mod, "upgrade", None)
        if version is None or upgrade_fn is None:
            continue
        downgrade_fn = getattr(mod, "downgrade", None)
        migrations.append(Migration(version=int(version), name=name, upgrade=upgrade_fn, downgrade=downgrade_fn))
    migrations.sort(key=lambda m: m.version)
    _check_no_gaps(migrations)
    return migrations


def _check_no_gaps(migrations: list[Migration]) -> None:
    seen: set[int] = set()
    for m in migrations:
        if m.version in seen:
            raise ValueError(f"Duplicate migration version: {m.version}")
        seen.add(m.version)

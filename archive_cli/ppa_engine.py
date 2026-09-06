"""Compatibility facade for execution-mode selection.

The implementation lives in ``archive_engine.execution_mode``. Callers that
already imported ``archive_cli.ppa_engine`` keep working.
"""

from __future__ import annotations

from archive_engine.execution_mode import ppa_engine, use_rust_vault_cache_disk_build

__all__ = ["ppa_engine", "use_rust_vault_cache_disk_build"]

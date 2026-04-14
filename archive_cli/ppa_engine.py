"""Select native Rust vs Python code paths (Phase 2.9+).

Default is ``rust``. Set ``PPA_ENGINE=python`` to force legacy Python paths (e.g. when
``archive_crate`` is not built).
"""

from __future__ import annotations

import os


def ppa_engine() -> str:
    raw = os.environ.get("PPA_ENGINE", "rust")
    if not raw:
        return "rust"
    return str(raw).strip().lower()


def use_rust_vault_cache_disk_build() -> bool:
    """When True, ``VaultScanCache.build_or_load`` may write ``vault-scan-cache.sqlite3`` via Rust."""

    return ppa_engine() == "rust"

"""Native vs Python execution mode. Neutral of CLI/MCP transports.

``archive_cli.ppa_engine`` re-exports these symbols for compatibility.
``archive_vault`` imports this module instead of the CLI package.
"""

from __future__ import annotations

import os

ENGINE_RUST = "rust"
ENGINE_PYTHON = "python"


def ppa_engine() -> str:
    raw = os.environ.get("PPA_ENGINE", ENGINE_RUST)
    if not raw:
        return ENGINE_RUST
    return str(raw).strip().lower() or ENGINE_RUST


def use_rust_vault_cache_disk_build() -> bool:
    """When True, ``VaultScanCache.build_or_load`` may write the Rust scan cache."""

    return ppa_engine() == ENGINE_RUST

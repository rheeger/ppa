#!/usr/bin/env python3
"""Demonstrate ``PPA_ENGINE=rust`` vault cache disk build.

Requires a built extension: ``cd archive_crate && maturin develop`` (or ``make build-rust``).

Usage::

  PPA_ENGINE=rust python scripts/demo_ppa_rust_vault_cache.py /path/to/vault
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: demo_ppa_rust_vault_cache.py <vault_path>", file=sys.stderr)
        sys.exit(2)
    vault = Path(sys.argv[1]).resolve()
    if not vault.is_dir():
        print(f"not a directory: {vault}", file=sys.stderr)
        sys.exit(1)
    os.environ.setdefault("PPA_ENGINE", "rust")

    t0 = time.perf_counter()
    from archive_cli.vault_cache import VaultScanCache

    cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)
    elapsed = time.perf_counter() - t0
    print(
        f"engine={os.environ.get('PPA_ENGINE', 'python')} "
        f"notes={cache.note_count()} tier={cache.tier()} elapsed_s={elapsed:.3f}"
    )


if __name__ == "__main__":
    main()

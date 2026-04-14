#!/usr/bin/env python3
"""Phase 2.9 Step 15 — entity resolution benchmark (Python baseline).

Rust ``resolve_person_batch`` parity benchmark lands in test_archive_crate_fuzzy_resolver.

Environment:

- ``PPA_BENCHMARK_VAULT`` — vault path (required)

Output: JSON on stdout.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def main() -> None:
    vault_s = os.environ.get("PPA_BENCHMARK_VAULT", "").strip()
    if not vault_s:
        print(json.dumps({"error": "set PPA_BENCHMARK_VAULT"}, indent=2))
        sys.exit(1)
    vault = Path(vault_s).resolve()

    t0 = time.perf_counter()
    from archive_vault.identity_resolver import PersonIndex

    idx = PersonIndex(vault, preload=True, progress_every=0)
    load_s = time.perf_counter() - t0

    out = {
        "vault": str(vault),
        "python": {
            "person_index_load_seconds": round(load_s, 4),
            "person_records": len(idx.records),
        },
        "rust": {
            "resolve_person_batch": "partial — token_sort_ratio + index helpers in archive_crate",
            "note": "full batch parity: tests/test_archive_crate_fuzzy_resolver.py",
        },
    }
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

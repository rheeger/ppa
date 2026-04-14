#!/usr/bin/env python3
"""Phase 2.9 Step 12 — benchmark Tier 2 rebuild pipeline (Python vs Rust).

Measures:
- Vault cache tier-2 build (Python ``VaultScanCache`` vs ``archive_crate.build_vault_cache``)
- Materialize + chunk sample (Python vs Rust ``materialize_row_batch``)
- Cache iteration (Python ``iter_parsed_notes`` vs ``archive_crate.notes_from_cache``)
- Entity resolution batch (Python vs Rust ``resolve_person_batch``)

Environment:

- ``PPA_BENCHMARK_VAULT`` — vault path (default: ``.slices/1pct``)
- ``PPA_BENCHMARK_SAMPLE_NOTES`` — max notes to materialize+chunk (default 50, 0 = skip)

Output: JSON on stdout.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path


def main() -> None:
    vault_s = os.environ.get("PPA_BENCHMARK_VAULT", ".slices/1pct").strip()
    vault = Path(vault_s).resolve()
    if not vault.is_dir():
        print(json.dumps({"error": f"vault not found: {vault}"}, indent=2))
        sys.exit(1)

    sample = int(os.environ.get("PPA_BENCHMARK_SAMPLE_NOTES", "50"))
    out: dict = {"vault": str(vault), "python": {}, "rust": {}, "speedups": {}}

    cache_path = vault / "_meta" / "vault-scan-cache.sqlite3"

    # --- Python: vault cache tier-2 build ---
    t0 = time.perf_counter()
    from archive_cli.vault_cache import VaultScanCache
    cache = VaultScanCache.build_or_load(vault, tier=2, no_cache=True, progress_every=0)
    py_cache_s = time.perf_counter() - t0
    out["python"]["vault_cache_tier2_seconds"] = round(py_cache_s, 3)
    out["python"]["note_count"] = cache.note_count()

    # --- Rust: vault cache tier-2 build ---
    try:
        import archive_crate

        rust_cache = str(vault / "_meta" / "_bench_rust_cache.sqlite3")
        t0 = time.perf_counter()
        archive_crate.build_vault_cache(str(vault), rust_cache, 2)
        rs_cache_s = time.perf_counter() - t0
        out["rust"]["vault_cache_tier2_seconds"] = round(rs_cache_s, 3)
        out["speedups"]["cache_build"] = round(py_cache_s / rs_cache_s, 1) if rs_cache_s > 0 else None
        os.unlink(rust_cache)
    except ImportError:
        out["rust"]["vault_cache_tier2_seconds"] = "archive_crate not available"

    # --- Python: materialize + chunk sample ---
    if sample > 0:
        from archive_cli.chunking import render_chunks_for_card
        from archive_cli.materializer import _build_search_text

        rels = cache.all_rel_paths()[:sample]
        t1 = time.perf_counter()
        nchunks = 0
        for rel in rels:
            fm = cache.frontmatter_for_rel_path(rel)
            body = cache.body_for_rel_path(rel)
            _build_search_text(fm, body)
            chunks = render_chunks_for_card(fm, body)
            nchunks += len(chunks)
        py_mat_s = time.perf_counter() - t1
        out["python"]["materialize_chunk_sample_seconds"] = round(py_mat_s, 4)
        out["python"]["sample_notes"] = len(rels)
        out["python"]["chunk_rows"] = nchunks

    # --- Cache iteration comparison ---
    try:
        import archive_crate

        # Python iter
        os.environ["PPA_ENGINE"] = "python"
        from archive_vault.vault import _iter_parsed_notes_python_walk
        t0 = time.perf_counter()
        py_count = sum(1 for _ in _iter_parsed_notes_python_walk(vault))
        py_iter_s = time.perf_counter() - t0
        out["python"]["iter_parsed_notes_seconds"] = round(py_iter_s, 3)
        out["python"]["iter_parsed_notes_count"] = py_count

        # Rust cache read
        t0 = time.perf_counter()
        rs_rows = archive_crate.notes_from_cache(str(cache_path))
        rs_iter_s = time.perf_counter() - t0
        out["rust"]["notes_from_cache_seconds"] = round(rs_iter_s, 3)
        out["rust"]["notes_from_cache_count"] = len(rs_rows)
        out["speedups"]["iteration"] = round(py_iter_s / rs_iter_s, 1) if rs_iter_s > 0 else None

        # Frontmatter-only
        t0 = time.perf_counter()
        _ = archive_crate.frontmatter_dicts_from_cache(str(cache_path))
        rs_fm_s = time.perf_counter() - t0
        out["rust"]["frontmatter_dicts_all_seconds"] = round(rs_fm_s, 3)

    except ImportError:
        out["rust"]["iteration"] = "archive_crate not available"

    # --- Entity resolution comparison ---
    try:
        import archive_crate
        from archive_sync.extractors.entity_resolution import (
            PERSON_RESOLVABLE_CARD_TYPES,
            _person_names_from_derived_card,
        )
        from archive_vault.identity_resolver import resolve_person_batch as py_resolve

        derived = archive_crate.frontmatter_dicts_from_cache(
            str(cache_path), types=list(PERSON_RESOLVABLE_CARD_TYPES),
        )
        batch_ids = []
        for row in derived:
            for raw in _person_names_from_derived_card(row["frontmatter"]):
                batch_ids.append({"name": raw})

        if len(batch_ids) >= 10:
            t0 = time.perf_counter()
            py_resolve(str(vault), batch_ids)
            py_res_s = time.perf_counter() - t0
            out["python"]["resolve_batch_seconds"] = round(py_res_s, 3)
            out["python"]["resolve_batch_count"] = len(batch_ids)

            t0 = time.perf_counter()
            archive_crate.resolve_person_batch(str(vault), batch_ids)
            rs_res_s = time.perf_counter() - t0
            out["rust"]["resolve_batch_seconds"] = round(rs_res_s, 3)
            out["speedups"]["resolve_batch"] = round(py_res_s / rs_res_s, 1) if rs_res_s > 0 else None

    except ImportError:
        pass

    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

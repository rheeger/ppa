#!/usr/bin/env python3
"""Phase 2.9 Step 6 — benchmark Tier 1 (walk + vault fingerprint + cache build: Python vs Rust).

Requires a built extension: ``cd archive_crate && maturin develop``.

Environment:

- ``PPA_BENCHMARK_VAULT`` — vault path (required for non-trivial runs)
- ``PPA_BENCHMARK_SKIP_CACHE`` — if ``1``, skip cache timing (walk + fingerprint only)
- ``PPA_TIER1_ENFORCE_MIN_NOTES`` — minimum note count before ``--enforce`` applies (default ``500``)
- ``PPA_TIER1_MIN_WALK_SPEEDUP`` — for ``--enforce`` (default ``8``)
- ``PPA_TIER1_MIN_CACHE_SPEEDUP`` — for ``--enforce`` (default ``8``)
- ``PPA_TIER1_MIN_FINGERPRINT_SPEEDUP`` — only if ``--enforce-fingerprint`` (default ``2``)

Output: JSON on stdout.

Exit codes: ``0`` ok, ``1`` usage/config error, ``2`` enforce failure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
from pathlib import Path


def _bench_walk(vault: Path) -> dict:
    from archive_vault.vault import iter_note_paths

    t0 = time.perf_counter()
    py_paths = list(iter_note_paths(vault))
    py_elapsed = time.perf_counter() - t0

    rust_elapsed = None
    rust_count = None
    ratio = None
    try:
        import archive_crate

        t0 = time.perf_counter()
        rust_count = archive_crate.walk_vault_count(str(vault))
        rust_elapsed = time.perf_counter() - t0
        if rust_elapsed and rust_elapsed > 0 and py_elapsed > 0:
            ratio = round(py_elapsed / rust_elapsed, 2)
    except ImportError:
        pass

    return {
        "python_note_count": len(py_paths),
        "python_seconds": round(py_elapsed, 4),
        "rust_note_count": rust_count,
        "rust_seconds": round(rust_elapsed, 4) if rust_elapsed is not None else None,
        "python_over_rust_walltime": ratio,
    }


def _bench_fingerprint(vault: Path) -> dict:
    from archive_vault.vault import iter_note_paths

    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    from archive_cli.scanner import _vault_paths_and_fingerprint

    t0 = time.perf_counter()
    _stats_py, fp_py = _vault_paths_and_fingerprint(vault, rel_paths)
    py_elapsed = time.perf_counter() - t0

    rust_elapsed = None
    fp_rust = None
    ratio = None
    try:
        import archive_crate

        t0 = time.perf_counter()
        _, fp_rust = archive_crate.vault_fingerprint(str(vault))
        rust_elapsed = time.perf_counter() - t0
        if rust_elapsed and rust_elapsed > 0 and py_elapsed > 0:
            ratio = round(py_elapsed / rust_elapsed, 2)
    except ImportError:
        pass

    return {
        "python_fingerprint_seconds": round(py_elapsed, 4),
        "rust_fingerprint_seconds": round(rust_elapsed, 4) if rust_elapsed is not None else None,
        "python_over_rust_walltime": ratio,
        "fingerprint_match": fp_rust == fp_py if fp_rust is not None else None,
        "note_count": len(rel_paths),
    }


def _bench_cache_build(vault: Path) -> dict:
    try:
        import archive_crate
    except ImportError:
        return {"cache_rust_seconds": None, "cache_python_seconds": None, "skipped": "no archive_crate"}

    from archive_cli.vault_cache import VaultScanCache, _compute_vault_fingerprint
    from archive_vault.vault import iter_note_paths

    rel_paths = [p.as_posix() for p in iter_note_paths(vault)]
    stats, fp = _compute_vault_fingerprint(vault, rel_paths)

    tmp_py = Path(tempfile.mkdtemp(prefix="ppa-bench-py-cache-")) / "bench-cache.sqlite3"
    t0 = time.perf_counter()
    py_cache = VaultScanCache._build_fresh(
        vault,
        rel_paths,
        stats,
        fp,
        tier=2,
        persist_path=tmp_py,
        progress_every=0,
        cache_hit=False,
    )
    py_elapsed = time.perf_counter() - t0
    try:
        py_cache.close()
    except OSError:
        pass
    try:
        tmp_py.unlink(missing_ok=True)
    except OSError:
        pass

    out_r = Path(tempfile.mkdtemp(prefix="ppa-bench-rust-cache-")) / "bench-cache.sqlite3"
    t0 = time.perf_counter()
    archive_crate.build_vault_cache(str(vault), str(out_r), 2)
    rust_elapsed = time.perf_counter() - t0
    try:
        out_r.unlink(missing_ok=True)
    except OSError:
        pass

    ratio = None
    if rust_elapsed and rust_elapsed > 0 and py_elapsed > 0:
        ratio = round(py_elapsed / rust_elapsed, 2)

    return {
        "cache_python_tier2_seconds": round(py_elapsed, 4),
        "cache_rust_tier2_seconds": round(rust_elapsed, 4),
        "python_over_rust_cache_seconds": ratio,
        "note_count": len(rel_paths),
    }


def _enforce(
    result: dict,
    *,
    min_notes: int,
    min_walk: float,
    min_cache: float,
    min_fingerprint: float | None,
    enforce_fingerprint: bool,
) -> list[str]:
    """Return list of failure messages (empty if pass)."""
    failures: list[str] = []
    n = int(result.get("walk", {}).get("python_note_count") or 0)
    if n < min_notes:
        failures.append(
            f"enforce skipped: note_count={n} < PPA_TIER1_ENFORCE_MIN_NOTES={min_notes} "
            f"(set a larger vault or lower the threshold)"
        )
        return failures

    w = result.get("walk", {})
    wr = w.get("python_over_rust_walltime")
    if wr is None:
        failures.append("walk: python_over_rust_walltime missing (build archive_crate?)")
    elif wr < min_walk:
        failures.append(f"walk speedup {wr} < min {min_walk} (Python wall / Rust wall)")

    c = result.get("cache_build", {})
    cr = c.get("python_over_rust_cache_seconds")
    if cr is None:
        failures.append("cache_build.python_over_rust_cache_seconds missing (need archive_crate + cache not skipped)")
    elif cr < min_cache:
        failures.append(f"cache tier-2 speedup {cr} < min {min_cache} (Python / Rust)")

    if enforce_fingerprint and min_fingerprint is not None:
        f = result.get("fingerprint", {})
        fr = f.get("python_over_rust_walltime")
        if fr is not None and fr < min_fingerprint:
            failures.append(f"fingerprint speedup {fr} < min {min_fingerprint}")

    return failures


def main() -> None:
    ap = argparse.ArgumentParser(description="Tier 1 archive_crate benchmark (JSON stdout).")
    ap.add_argument(
        "--enforce",
        action="store_true",
        help="Exit 2 if speedups are below env thresholds (only when note_count >= PPA_TIER1_ENFORCE_MIN_NOTES).",
    )
    ap.add_argument(
        "--enforce-fingerprint",
        action="store_true",
        help="With --enforce, also require fingerprint speedup (see PPA_TIER1_MIN_FINGERPRINT_SPEEDUP).",
    )
    args = ap.parse_args()

    vault_s = os.environ.get("PPA_BENCHMARK_VAULT", "").strip()
    if not vault_s:
        print(
            json.dumps(
                {
                    "error": "set PPA_BENCHMARK_VAULT to a vault directory",
                    "hint": "PPA_BENCHMARK_VAULT=/path/to/vault python scripts/benchmark-archive_crate-tier1.py",
                },
                indent=2,
            )
        )
        sys.exit(1)

    vault = Path(vault_s).resolve()
    if not vault.is_dir():
        print(json.dumps({"error": f"not a directory: {vault}"}, indent=2))
        sys.exit(1)

    min_notes = int(os.environ.get("PPA_TIER1_ENFORCE_MIN_NOTES", "500"))
    min_walk = float(os.environ.get("PPA_TIER1_MIN_WALK_SPEEDUP", "8"))
    min_cache = float(os.environ.get("PPA_TIER1_MIN_CACHE_SPEEDUP", "8"))
    min_fp = float(os.environ.get("PPA_TIER1_MIN_FINGERPRINT_SPEEDUP", "2"))

    result: dict = {"vault": str(vault), "walk": _bench_walk(vault), "fingerprint": _bench_fingerprint(vault)}
    if os.environ.get("PPA_BENCHMARK_SKIP_CACHE", "").strip() != "1":
        try:
            import archive_crate  # noqa: F401

            result["cache_build"] = _bench_cache_build(vault)
        except ImportError:
            result["cache_build"] = {"skipped": "no archive_crate"}
    else:
        result["cache_build"] = {"skipped": "PPA_BENCHMARK_SKIP_CACHE=1"}

    result["enforce_config"] = {
        "min_notes": min_notes,
        "min_walk_speedup": min_walk,
        "min_cache_speedup": min_cache,
        "min_fingerprint_speedup": min_fp if args.enforce_fingerprint else None,
        "enforce": bool(args.enforce),
        "enforce_fingerprint": bool(args.enforce_fingerprint),
    }

    print(json.dumps(result, indent=2))

    if args.enforce:
        fails = _enforce(
            result,
            min_notes=min_notes,
            min_walk=min_walk,
            min_cache=min_cache,
            min_fingerprint=min_fp,
            enforce_fingerprint=args.enforce_fingerprint,
        )
        # If the only messages are "skipped" due to low note count, treat as pass with stderr notice
        skip_only = len(fails) == 1 and fails[0].startswith("enforce skipped:")
        if fails and not skip_only:
            print("\n".join(fails), file=sys.stderr)
            sys.exit(2)
        if skip_only:
            print(fails[0], file=sys.stderr)


if __name__ == "__main__":
    main()

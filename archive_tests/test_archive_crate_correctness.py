"""Step 11 — row-level materializer parity (Python vs Rust ``ProjectionRowBuffer``).

Full parity on the fixture vault runs in ``test_materialize_row_batch_rust_matches_python`` (shared helper).

Set ``PPA_CORRECTNESS_SLICE`` to an **on-disk vault root** (e.g. a 5% export) to run the same check on a larger slice — the integration gate for production-shaped vaults.

``test_correctness_rust_vs_baseline`` runs **Rust-only** against a saved Python
baseline (``<slice>_python_baseline.json``).  Use ``--run-python-baseline`` or set
``PPA_GENERATE_BASELINE=1`` to (re-)generate the baseline with the Python materializer.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

from archive_cli.materializer import _build_person_lookup, _materialize_row_batch
from archive_cli.projections.base import ProjectionRowBuffer
from archive_tests.archive_crate_projection_parity import (
    assert_projection_buffers_equal,
    collect_canonical_rows_from_vault,
    summarize_buffer,
)

log = logging.getLogger("ppa.step11")

BATCH_SIZE = 2_000


def _materialize_in_batches(
    rows, *, vault_root, slug_map, path_to_uid, person_lookup, batch_id, label,
):
    """Run ``_materialize_row_batch`` in chunks with progress logging."""
    total = len(rows)
    combined = ProjectionRowBuffer()
    t0 = time.monotonic()
    for start in range(0, total, BATCH_SIZE):
        chunk = rows[start : start + BATCH_SIZE]
        buf = _materialize_row_batch(
            chunk,
            vault_root=vault_root,
            slug_map=slug_map,
            path_to_uid=path_to_uid,
            person_lookup=person_lookup,
            batch_id=batch_id,
        )
        combined.extend(buf)
        done = min(start + BATCH_SIZE, total)
        elapsed = time.monotonic() - t0
        rate = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / rate if rate > 0 else 0
        log.info(
            "%s: %d/%d (%.0f/s, ETA %.0fs)", label, done, total, rate, eta,
        )
    elapsed = time.monotonic() - t0
    log.info("%s: done — %d rows in %.1fs (%.0f rows/s)", label, total, elapsed, total / elapsed if elapsed > 0 else 0)
    return combined


_ARTIFACTS = Path(__file__).resolve().parent.parent / "_artifacts"


def _baseline_path(vault: Path) -> Path:
    return _ARTIFACTS / f"_correctness_baseline_{vault.name}.json"


def _buffer_to_counts(buf: ProjectionRowBuffer) -> dict:
    tables = {}
    for table, rows in buf.rows_by_table.items():
        tables[table] = len(rows)
    return {
        "tables": tables,
        "ingestion_log_rows": len(buf.ingestion_log_rows),
        "total_rows": sum(tables.values()) + len(buf.ingestion_log_rows),
    }


@pytest.mark.integration
@pytest.mark.slow
def test_correctness_slice_materializer_matches_python(monkeypatch, caplog):
    """``PPA_CORRECTNESS_SLICE=/path/to/vault`` — run BOTH Python and Rust, compare buffers."""

    raw = os.environ.get("PPA_CORRECTNESS_SLICE", "").strip()
    if not raw:
        pytest.skip("Set PPA_CORRECTNESS_SLICE to a vault directory for Step 11 slice parity")

    vault = Path(raw).expanduser().resolve()
    if not vault.is_dir():
        pytest.fail(f"PPA_CORRECTNESS_SLICE is not a directory: {vault}")

    with caplog.at_level(logging.INFO, logger="ppa.step11"):
        log.info("collecting canonical rows from %s …", vault)
        t0 = time.monotonic()
        rows, slug_map, path_to_uid = collect_canonical_rows_from_vault(vault)
        if not rows:
            pytest.fail(f"No notes found under {vault}")
        log.info(
            "collected %d rows, %d slugs, %d uids in %.1fs",
            len(rows), len(slug_map), len(path_to_uid), time.monotonic() - t0,
        )

        person_lookup = _build_person_lookup(rows)
        log.info("person_lookup: %d entries", len(person_lookup))
        batch_id = "step11-correctness-slice"

        monkeypatch.setenv("PPA_ENGINE", "python")
        py_batch = _materialize_in_batches(
            rows,
            vault_root=str(vault),
            slug_map=slug_map,
            path_to_uid=path_to_uid,
            person_lookup=person_lookup,
            batch_id=batch_id,
            label="python",
        )
        log.info("python buffer: %s", summarize_buffer(py_batch))

        baseline = _buffer_to_counts(py_batch)
        bp = _baseline_path(vault)
        bp.write_text(json.dumps(baseline, indent=2, sort_keys=True))
        log.info("saved baseline → %s", bp)

        monkeypatch.setenv("PPA_ENGINE", "rust")
        rust_batch = _materialize_in_batches(
            rows,
            vault_root=str(vault),
            slug_map=slug_map,
            path_to_uid=path_to_uid,
            person_lookup=person_lookup,
            batch_id=batch_id,
            label="rust",
        )
        log.info("rust buffer: %s", summarize_buffer(rust_batch))

        log.info("comparing buffers …")
        assert_projection_buffers_equal(
            py_batch,
            rust_batch,
            context=f"PPA_CORRECTNESS_SLICE={vault}",
        )
        log.info("PASS — buffers identical")


def _collect_rows_via_rust_cache(vault: Path):
    """Build CanonicalRow list from Rust vault cache — cached on disk for reuse."""
    import sqlite3

    import archive_crate
    from archive_cli.scanner import CanonicalRow
    from archive_vault.schema import validate_card_permissive

    _ARTIFACTS.mkdir(parents=True, exist_ok=True)
    cache_path = str(_ARTIFACTS / f"_vault_scan_cache_{vault.name}.sqlite3")
    if Path(cache_path).exists():
        log.info("reusing existing Rust vault cache at %s", cache_path)
    else:
        t0 = time.monotonic()
        log.info("building Rust vault cache (tier 2) → %s …", cache_path)
        archive_crate.build_vault_cache(str(vault), cache_path, tier=2)
        log.info("cache built in %.1fs", time.monotonic() - t0)

    conn = sqlite3.connect(cache_path)
    cursor = conn.execute(
        "SELECT rel_path, uid, frontmatter_json FROM notes ORDER BY rel_path"
    )
    rows = []
    slug_map = {}
    path_to_uid = {}
    for rel_path, uid, fm_json in cursor:
        fm = json.loads(fm_json)
        stem = Path(rel_path).stem
        slug_map[stem] = rel_path
        path_to_uid[rel_path] = uid or str(fm.get("uid", ""))
        rows.append(
            CanonicalRow(
                rel_path=rel_path,
                frontmatter=fm,
                card=validate_card_permissive(fm),
            )
        )
    conn.close()
    log.info("loaded %d rows from cache", len(rows))
    return rows, slug_map, path_to_uid


@pytest.mark.integration
@pytest.mark.slow
def test_correctness_rust_vs_baseline(monkeypatch, caplog):
    """Rust-only: build cache in Rust, materialize in Rust, compare counts to Python baseline.

    Skips Python note parsing AND Python materializer.  Re-generate the baseline with
    ``test_correctness_slice_materializer_matches_python``.
    """

    raw = os.environ.get("PPA_CORRECTNESS_SLICE", "").strip()
    if not raw:
        pytest.skip("Set PPA_CORRECTNESS_SLICE to a vault directory")

    vault = Path(raw).expanduser().resolve()
    if not vault.is_dir():
        pytest.fail(f"PPA_CORRECTNESS_SLICE is not a directory: {vault}")

    bp = _baseline_path(vault)
    if not bp.exists():
        pytest.skip(f"No baseline at {bp} — run test_correctness_slice_materializer_matches_python first")

    baseline = json.loads(bp.read_text())

    with caplog.at_level(logging.INFO, logger="ppa.step11"):
        t0 = time.monotonic()
        rows, slug_map, path_to_uid = _collect_rows_via_rust_cache(vault)
        if not rows:
            pytest.fail(f"No notes found under {vault}")
        log.info(
            "collected %d rows via Rust cache in %.1fs",
            len(rows), time.monotonic() - t0,
        )

        person_lookup = _build_person_lookup(rows)
        batch_id = "step11-rust-baseline"

        monkeypatch.setenv("PPA_ENGINE", "rust")
        rust_batch = _materialize_in_batches(
            rows,
            vault_root=str(vault),
            slug_map=slug_map,
            path_to_uid=path_to_uid,
            person_lookup=person_lookup,
            batch_id=batch_id,
            label="rust",
        )
        elapsed = time.monotonic() - t0
        rust_counts = _buffer_to_counts(rust_batch)
        log.info("rust buffer: %s", rust_counts)
        log.info("total wall time: %.1fs", elapsed)

        diffs = []
        py_tables = baseline["tables"]
        rust_tables = rust_counts["tables"]
        all_tables = sorted(set(py_tables) | set(rust_tables))
        for table in all_tables:
            pc = py_tables.get(table, 0)
            rc = rust_tables.get(table, 0)
            if pc != rc:
                diffs.append(f"  {table}: python={pc} rust={rc} (delta={rc - pc:+d})")
        if baseline["ingestion_log_rows"] != rust_counts["ingestion_log_rows"]:
            diffs.append(
                f"  ingestion_log_rows: python={baseline['ingestion_log_rows']} "
                f"rust={rust_counts['ingestion_log_rows']}"
            )

        if diffs:
            detail = "\n".join(diffs)
            raise AssertionError(
                f"Rust vs Python baseline row-count mismatch:\n{detail}"
            )
        log.info("PASS — Rust row counts match Python baseline")

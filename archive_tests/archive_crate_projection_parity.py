"""Shared helpers for Step 8 / Step 11 — Python vs Rust ``ProjectionRowBuffer`` parity."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

from archive_cli.projections.base import ProjectionRowBuffer
from archive_cli.scanner import CanonicalRow
from archive_vault.schema import validate_card_permissive
from archive_vault.vault import iter_parsed_notes

log = logging.getLogger("ppa.step11")

COMPARE_TABLES_DOC = [
    "cards",
    "card_people",
    "card_orgs",
    "card_sources",
    "edges",
    "chunks",
    "note_manifest",
    "ingestion_log",
]

_COLLECT_LOG_INTERVAL = 5_000


def collect_canonical_rows_from_vault(vault: Path) -> tuple[list[CanonicalRow], dict[str, str], dict[str, str]]:
    """Build ``CanonicalRow`` list + slug/path maps — same shape as ``test_materialize_row_batch_rust_matches_python``."""
    rows: list[CanonicalRow] = []
    slug_map: dict[str, str] = {}
    path_to_uid: dict[str, str] = {}
    t0 = time.monotonic()
    for note in iter_parsed_notes(vault):
        rel = note.rel_path.as_posix()
        stem = Path(rel).stem
        slug_map[stem] = rel
        uid = str(note.frontmatter.get("uid", ""))
        path_to_uid[rel] = uid
        rows.append(
            CanonicalRow(
                rel_path=rel,
                frontmatter=dict(note.frontmatter),
                card=validate_card_permissive(note.frontmatter),
            )
        )
        if len(rows) % _COLLECT_LOG_INTERVAL == 0:
            elapsed = time.monotonic() - t0
            log.info("collect: %d notes parsed (%.1fs)", len(rows), elapsed)
    return rows, slug_map, path_to_uid


def format_projection_buffer_diff(
    py: ProjectionRowBuffer,
    rust: ProjectionRowBuffer,
    *,
    max_issues: int = 40,
) -> str:
    """Return a human-readable diff when buffers differ; empty string if equal."""
    lines: list[str] = []
    py_tables = set(py.rows_by_table.keys())
    rust_tables = set(rust.rows_by_table.keys())
    if py_tables != rust_tables:
        only_py = sorted(py_tables - rust_tables)
        only_rust = sorted(rust_tables - py_tables)
        if only_py:
            lines.append(f"tables only in python batch: {only_py}")
        if only_rust:
            lines.append(f"tables only in rust batch: {only_rust}")

    for table in sorted(py_tables & rust_tables):
        ra = list(py.rows_for(table))
        rb = list(rust.rows_for(table))
        if len(ra) != len(rb):
            lines.append(f"{table}: row count {len(ra)} (python) != {len(rb)} (rust)")
            if len(lines) >= max_issues:
                return "\n".join(lines)
            continue
        for i, (row_a, row_b) in enumerate(zip(ra, rb, strict=True)):
            if row_a == row_b:
                continue
            if len(row_a) != len(row_b):
                lines.append(
                    f"{table}[{i}]: tuple len {len(row_a)} (python) != {len(row_b)} (rust)"
                )
            else:
                for j, (ca, cb) in enumerate(zip(row_a, row_b, strict=True)):
                    if ca != cb:
                        lines.append(f"{table}[{i}].col[{j}]: python={ca!r} rust={cb!r}")
                        break
            if len(lines) >= max_issues:
                return "\n".join(lines)

    if py.ingestion_log_rows != rust.ingestion_log_rows:
        lines.append("ingestion_log_rows differ")
        pil = py.ingestion_log_rows
        ril = rust.ingestion_log_rows
        if len(pil) != len(ril):
            lines.append(f"  len {len(pil)} != {len(ril)}")
        else:
            for i, (a, b) in enumerate(zip(pil, ril, strict=True)):
                if a != b:
                    lines.append(f"  ingestion_log[{i}]: {a!r} != {b!r}")
                    break

    return "\n".join(lines)


def assert_projection_buffers_equal(
    py: ProjectionRowBuffer,
    rust: ProjectionRowBuffer,
    *,
    context: str = "",
) -> None:
    """Assert two buffers are identical; on failure raise AssertionError with a detailed diff."""
    msg = format_projection_buffer_diff(py, rust)
    if msg:
        prefix = f"{context}: " if context else ""
        raise AssertionError(f"{prefix}ProjectionRowBuffer mismatch:\n{msg}")


def summarize_buffer(py: ProjectionRowBuffer) -> dict[str, Any]:
    """Lightweight stats for logging (Step 11 reports)."""
    out: dict[str, Any] = {
        "tables": {k: len(v) for k, v in sorted(py.rows_by_table.items())},
        "ingestion_log_rows": len(py.ingestion_log_rows),
        "total_rows": sum(len(v) for v in py.rows_by_table.values()) + len(py.ingestion_log_rows),
    }
    return out

"""Deterministic Postgres index snapshots for integration tests (Phase 0)."""

from __future__ import annotations

from typing import Any

from archive_mcp.projections.registry import PROJECTION_REGISTRY

_NOTE_MANIFEST_COLS = (
    "rel_path, card_uid, slug, content_hash, frontmatter_hash, file_size, mtime_ns, "
    "card_type, typed_projection, people_json, orgs_json, scan_version, "
    "chunk_schema_version, projection_registry_version, index_schema_version"
)

_CHUNK_COLS = (
    "chunk_key, card_uid, rel_path, chunk_type, chunk_index, chunk_schema_version, "
    "source_fields, content, content_hash, token_count"
)


def _row_tuple(row: Any) -> tuple[Any, ...]:
    if hasattr(row, "keys"):
        keys = sorted(row.keys())
        return tuple(row[k] for k in keys)
    return tuple(row)


def _sorted_rows(conn: Any, schema: str, sql: str) -> tuple[tuple[Any, ...], ...]:
    rows = conn.execute(sql).fetchall()
    normalized = [_row_tuple(r) for r in rows]
    return tuple(sorted(normalized))


def snapshot_projection_state(conn: Any, schema: str) -> dict[str, tuple[tuple[Any, ...], ...]]:
    """Snapshot deterministic projection rows (excludes meta, checkpoints, embeddings, timestamps)."""
    out: dict[str, tuple[tuple[Any, ...], ...]] = {}
    for projection in PROJECTION_REGISTRY:
        table = projection.table_name
        cols = ", ".join(c.name for c in projection.columns)
        sql = f"SELECT {cols} FROM {schema}.{table}"
        out[table] = _sorted_rows(conn, schema, sql)

    out["note_manifest"] = _sorted_rows(
        conn,
        schema,
        f"SELECT {_NOTE_MANIFEST_COLS} FROM {schema}.note_manifest",
    )
    out["chunks"] = _sorted_rows(
        conn,
        schema,
        f"SELECT {_CHUNK_COLS} FROM {schema}.chunks",
    )
    return out

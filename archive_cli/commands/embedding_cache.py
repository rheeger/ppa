"""Direct embedding cache export/import commands for Phase 9.

These commands intentionally avoid OpenAI. They copy existing rows between
Postgres and a durable TSV cache.
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from ..errors import PpaError
from ..index_config import get_index_dsn, get_index_schema

EXPECTED_EMBEDDING_MODEL = "text-embedding-3-small"
EXPECTED_EMBEDDING_VERSION = 1


@dataclass(frozen=True)
class EmbeddingCacheManifest:
    cache_kind: str
    created_at_utc: str
    source: str
    source_schema: str
    source_dsn_redacted: str
    embedding_count: int
    chunk_count: int
    data_file: str = "embeddings.tsv"


def _dsn_redacted(dsn: str) -> str:
    if "@" not in dsn:
        return dsn
    prefix, suffix = dsn.rsplit("@", 1)
    if ":" not in prefix:
        return dsn
    return f"{prefix.split(':', 1)[0]}:***@{suffix}"


def _require_dsn() -> str:
    dsn = get_index_dsn()
    if not dsn:
        raise PpaError("PPA_INDEX_DSN is required")
    return dsn


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024 * 8)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def export_embedding_cache(
    *,
    output_dir: str,
    embedding_model: str = EXPECTED_EMBEDDING_MODEL,
    embedding_version: int = EXPECTED_EMBEDDING_VERSION,
    force: bool = False,
    allow_artifacts_dir: bool = False,
    allow_low_space: bool = False,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Export existing embeddings to a direct TSV cache.

    The output can be imported with ``import_embedding_cache`` and does not
    require OpenAI batch metadata.
    """
    dsn = _require_dsn()
    schema = get_index_schema()
    out = Path(output_dir).expanduser()
    if "_artifacts" in out.parts and not allow_artifacts_dir:
        raise PpaError("Refusing to write embedding cache under _artifacts without --allow-artifacts-dir")
    if out.exists() and any(out.iterdir()) and not force:
        raise PpaError(f"Output directory exists and is not empty: {out}")
    out.mkdir(parents=True, exist_ok=True)

    data = out / "embeddings.tsv"
    tmp = out / "embeddings.tsv.tmp"
    if data.exists() and not force:
        raise PpaError(f"Refusing to overwrite {data}")
    if tmp.exists():
        tmp.unlink()

    usage = shutil.disk_usage(out)
    if usage.free < 150 * 1024**3 and not allow_low_space:
        raise PpaError(f"Only {usage.free / 1024**3:.1f}GB free at {out}; pass --allow-low-space to override")

    started = time.time()
    with psycopg.connect(dsn) as conn:
        counts = conn.execute(
            f"""
            SELECT
              (SELECT COUNT(*) FROM {schema}.embeddings
               WHERE embedding_model = %s AND embedding_version = %s) AS embeddings,
              (SELECT COUNT(*) FROM {schema}.chunks) AS chunks
            """,
            (embedding_model, embedding_version),
        ).fetchone()
        embedding_count = int(counts[0])
        chunk_count = int(counts[1])
        logger.info(
            "export_embedding_cache_start output_dir=%s embeddings=%d chunks=%d",
            out,
            embedding_count,
            chunk_count,
        )
        with tmp.open("wb") as fh:
            with conn.cursor().copy(
                f"""
                COPY (
                  SELECT chunk_key, embedding_model, embedding_version, embedding::text
                  FROM {schema}.embeddings
                  WHERE embedding_model = %s AND embedding_version = %s
                  ORDER BY chunk_key
                ) TO STDOUT WITH (FORMAT csv, DELIMITER E'\\t')
                """,
                (embedding_model, embedding_version),
            ) as copy:
                for block in copy:
                    fh.write(block if isinstance(block, bytes) else str(block).encode("utf-8"))

    tmp.rename(data)
    sha = _sha256_file(data)
    (out / "embeddings.tsv.sha256").write_text(f"{sha}  {data}\n", encoding="utf-8")
    with data.open("rb") as fh:
        rows = sum(1 for _ in fh)
    (out / "embeddings.tsv.rows").write_text(f"{rows} {data}\n", encoding="utf-8")
    manifest = {
        "cache_kind": "direct-postgres-embedding-export",
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "local seed Postgres embeddings",
        "source_schema": schema,
        "source_dsn_redacted": _dsn_redacted(dsn),
        "embedding_count": embedding_count,
        "chunk_count": chunk_count,
        "models": [
            {
                "embedding_model": embedding_model,
                "embedding_version": embedding_version,
                "count": embedding_count,
            }
        ],
        "format": "TSV: chunk_key, embedding_model, embedding_version, embedding_vector_literal",
        "data_file": data.name,
        "sha256": sha,
        "rows": rows,
        "notes": "Built from local embeddings table only. No OpenAI downloads or embedding generation.",
    }
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (out / "MANIFEST.txt").write_text(
        "\n".join(
            [
                "# direct embedding cache export",
                f"# created_at_utc: {manifest['created_at_utc']}",
                f"# source_schema: {schema}",
                f"# embeddings: {embedding_count}",
                f"# chunks: {chunk_count}",
                f"# data_file: {data.name}",
                "# no_openai: true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    elapsed = round(time.time() - started, 2)
    logger.info("export_embedding_cache_done rows=%d sha256=%s elapsed=%.2fs", rows, sha, elapsed)
    return {
        "output_dir": str(out),
        "rows": rows,
        "sha256": sha,
        "elapsed_seconds": elapsed,
    }


def _read_expected_rows(input_dir: Path) -> int | None:
    p = input_dir / "embeddings.tsv.rows"
    if not p.exists():
        return None
    raw = p.read_text(encoding="utf-8").strip().split()
    if not raw:
        return None
    try:
        return int(raw[0])
    except ValueError:
        return None


def _verify_sha(input_dir: Path, data: Path) -> str:
    expected_path = input_dir / "embeddings.tsv.sha256"
    if not expected_path.exists():
        raise PpaError(f"Missing checksum file: {expected_path}")
    expected = expected_path.read_text(encoding="utf-8").strip().split()[0]
    actual = _sha256_file(data)
    if actual != expected:
        raise PpaError(f"Checksum mismatch for {data}: expected {expected}, got {actual}")
    return actual


def import_embedding_cache(
    *,
    input_dir: str,
    verify_sha: bool = True,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Import direct TSV embedding cache into the configured schema."""
    dsn = _require_dsn()
    schema = get_index_schema()
    inp = Path(input_dir).expanduser()
    data = inp / "embeddings.tsv"
    if not data.exists():
        raise PpaError(f"Missing embeddings.tsv in {inp}")
    expected_rows = _read_expected_rows(inp)
    sha = _verify_sha(inp, data) if verify_sha else ""
    started = time.time()
    logger.info("import_embedding_cache_start input_dir=%s expected_rows=%s", inp, expected_rows)
    with psycopg.connect(dsn) as conn:
        conn.execute(
            """
            CREATE TEMP TABLE embedding_cache_import (
                chunk_key TEXT NOT NULL,
                embedding_model TEXT NOT NULL,
                embedding_version INTEGER NOT NULL,
                embedding vector(1536) NOT NULL
            ) ON COMMIT DROP
            """
        )
        with data.open("rb") as fh:
            with conn.cursor().copy(
                "COPY embedding_cache_import FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t')"
            ) as copy:
                while True:
                    block = fh.read(1024 * 1024 * 8)
                    if not block:
                        break
                    copy.write(block)
        count = int(conn.execute("SELECT COUNT(*) FROM embedding_cache_import").fetchone()[0])
        if expected_rows is not None and count != expected_rows:
            raise PpaError(f"Imported row count {count} != expected {expected_rows}")
        conn.execute(
            f"""
            INSERT INTO {schema}.embeddings (chunk_key, embedding_model, embedding_version, embedding)
            SELECT chunk_key, embedding_model, embedding_version, embedding
            FROM embedding_cache_import
            ON CONFLICT (chunk_key, embedding_model, embedding_version)
            DO UPDATE SET embedding = EXCLUDED.embedding
            """
        )
        conn.commit()
    elapsed = round(time.time() - started, 2)
    logger.info("import_embedding_cache_done rows=%d elapsed=%.2fs", count, elapsed)
    return {"input_dir": str(inp), "rows": count, "sha256": sha, "elapsed_seconds": elapsed}

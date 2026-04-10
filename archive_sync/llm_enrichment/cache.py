"""SQLite inference cache for Phase 2.75 (email LLM) and Phase 6 enrichment."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hfa.provenance import compute_input_hash


def build_inference_cache_key(
    *,
    content_hash: str,
    model_id: str,
    prompt_version: str,
    schema_version: str,
    temperature: float,
    seed: int,
) -> str:
    """Deterministic cache key aligned with ``compute_input_hash`` strategy."""

    return compute_input_hash(
        {
            "content_hash": content_hash,
            "model_id": model_id,
            "prompt_version": prompt_version,
            "schema_version": schema_version,
            "temperature": temperature,
            "seed": seed,
        }
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_SCHEMA = """
CREATE TABLE IF NOT EXISTS inference_cache (
    cache_key TEXT PRIMARY KEY,
    stage TEXT NOT NULL,
    model_id TEXT NOT NULL,
    prompt_version TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    latency_ms REAL,
    run_id TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cache_stage ON inference_cache(stage);
CREATE INDEX IF NOT EXISTS idx_cache_model ON inference_cache(model_id);
CREATE INDEX IF NOT EXISTS idx_cache_run ON inference_cache(run_id);
"""


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    conn.commit()


class InferenceCache:
    """SHA-keyed SQLite cache for LLM responses."""

    def __init__(self, db_path: Path | str = Path("_enrichment_cache.db"), *, _skip_init: bool = False):
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), timeout=120.0, check_same_thread=False)
        if not _skip_init:
            _init_db(self._conn)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> InferenceCache:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get(self, key: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT response_json FROM inference_cache WHERE cache_key = ?", (key,)
        ).fetchone()
        if not row:
            return None
        try:
            out = json.loads(row[0])
        except json.JSONDecodeError:
            return None
        return out if isinstance(out, dict) else None

    def put(
        self,
        key: str,
        *,
        stage: str,
        model_id: str,
        prompt_version: str,
        content_hash: str,
        response: dict[str, Any],
        tokens: tuple[int, int],
        latency_ms: float,
        run_id: str = "",
    ) -> None:
        pt, ct = tokens
        self._conn.execute(
            """
            INSERT INTO inference_cache (
                cache_key, stage, model_id, prompt_version, content_hash,
                response_json, prompt_tokens, completion_tokens, latency_ms, run_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                stage = excluded.stage,
                model_id = excluded.model_id,
                prompt_version = excluded.prompt_version,
                content_hash = excluded.content_hash,
                response_json = excluded.response_json,
                prompt_tokens = excluded.prompt_tokens,
                completion_tokens = excluded.completion_tokens,
                latency_ms = excluded.latency_ms,
                run_id = excluded.run_id,
                created_at = excluded.created_at
            """,
            (
                key,
                stage,
                model_id,
                prompt_version,
                content_hash,
                json.dumps(response, sort_keys=True, ensure_ascii=False),
                int(pt),
                int(ct),
                float(latency_ms),
                run_id,
                _utc_now_iso(),
            ),
        )
        self._conn.commit()

    def stats(self) -> dict[str, Any]:
        total = int(self._conn.execute("SELECT COUNT(*) FROM inference_cache").fetchone()[0])
        by_stage: Counter[str] = Counter()
        by_model: Counter[str] = Counter()
        by_run: Counter[str] = Counter()
        for st, mid, rid in self._conn.execute(
            "SELECT stage, model_id, run_id FROM inference_cache"
        ).fetchall():
            by_stage[str(st)] += 1
            by_model[str(mid)] += 1
            r = str(rid or "")
            if r:
                by_run[r] += 1
        return {
            "total": total,
            "by_stage": dict(by_stage),
            "by_model": dict(by_model),
            "by_run": dict(by_run),
        }

    def purge_run(self, run_id: str) -> int:
        cur = self._conn.execute("DELETE FROM inference_cache WHERE run_id = ?", (run_id,))
        self._conn.commit()
        return int(cur.rowcount or 0)

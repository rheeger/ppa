"""SQLite inference cache for Phase 2.75 (email LLM) and Phase 6 enrichment."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from archive_vault.provenance import compute_input_hash


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

    def __init__(self, db_path: Path | str = Path("_artifacts/_enrichment_cache.db"), *, _skip_init: bool = False):
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), timeout=120.0, check_same_thread=False)
        self._lock = threading.RLock()
        if not _skip_init:
            _init_db(self._conn)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> InferenceCache:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def get(self, key: str) -> dict[str, Any] | None:
        with self._lock:
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
        with self._lock:
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
        with self._lock:
            total = int(self._conn.execute("SELECT COUNT(*) FROM inference_cache").fetchone()[0])
            stage_rows = self._conn.execute(
                "SELECT stage, model_id, run_id FROM inference_cache"
            ).fetchall()
        by_stage: Counter[str] = Counter()
        by_model: Counter[str] = Counter()
        by_run: Counter[str] = Counter()
        for st, mid, rid in stage_rows:
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
        with self._lock:
            cur = self._conn.execute("DELETE FROM inference_cache WHERE run_id = ?", (run_id,))
            self._conn.commit()
        return int(cur.rowcount or 0)

    def cost_summary(self, run_id: str) -> dict[str, Any]:
        """Aggregate tokens and a directional USD estimate for one ``run_id``."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT stage, model_id, prompt_tokens, completion_tokens FROM inference_cache WHERE run_id = ?",
                (run_id,),
            ).fetchall()
        by_stage: dict[str, dict[str, Any]] = {}
        total_pt = total_ct = 0
        total_usd = 0.0
        for stage, model_id, pt, ct in rows:
            pt_i = int(pt or 0)
            ct_i = int(ct or 0)
            total_pt += pt_i
            total_ct += ct_i
            usd = _estimate_cost_usd(pt_i, ct_i, str(model_id or ""))
            total_usd += usd
            key = str(stage or "")
            bucket = by_stage.setdefault(
                key,
                {"prompt_tokens": 0, "completion_tokens": 0, "estimated_cost_usd": 0.0},
            )
            bucket["prompt_tokens"] += pt_i
            bucket["completion_tokens"] += ct_i
            bucket["estimated_cost_usd"] = round(bucket["estimated_cost_usd"] + usd, 6)
        for b in by_stage.values():
            b["estimated_cost_usd"] = round(float(b["estimated_cost_usd"]), 4)
        return {
            "total_prompt_tokens": total_pt,
            "total_completion_tokens": total_ct,
            "estimated_cost_usd": round(total_usd, 4),
            "by_stage": by_stage,
        }


_BILLING_OVERHEAD = 1.3
"""Empirical multiplier: Google bills ~30% more than ``candidatesTokenCount``
reports (internal processing, thinking overhead, etc.).  Derived from two
calibration runs against the Google Cloud billing dashboard (1pct slice and
full seed, April 2026)."""


def _price_per_m_tokens(model_id: str) -> tuple[float, float]:
    """Return (input_usd_per_1m, output_usd_per_1m).

    Prices from https://ai.google.dev/gemini-api/docs/pricing (April 2026).
    """

    m = (model_id or "").strip().lower()
    if not m or m.startswith("gemma") or m.startswith("ollama") or ":latest" in m:
        return (0.0, 0.0)
    if "3.1" in m and "flash-lite" in m:
        return (0.25, 1.50)
    if "2.5-flash-lite" in m or ("flash-lite" in m and "3.1" not in m):
        return (0.10, 0.40)
    if "2.5-flash" in m and "lite" not in m:
        return (0.30, 2.50)
    if "2.0-flash" in m or "gemini-2.0" in m:
        return (0.10, 0.40)
    if "gemini" in m:
        return (0.10, 0.40)
    return (0.0, 0.0)


def _estimate_cost_usd(prompt_tokens: int, completion_tokens: int, model_id: str) -> float:
    pin, pout = _price_per_m_tokens(model_id)
    raw = (prompt_tokens / 1_000_000.0) * pin + (completion_tokens / 1_000_000.0) * pout
    return raw * _BILLING_OVERHEAD

"""Persistent thread classification index — stores classify results for reuse across runs.

Unlike the inference cache (keyed by prompt version + model), this stores the
**semantic classification** of each thread, independent of which prompt or model
produced it. This lets you:

1. Classify the vault once, then run different extractors against different categories
2. Come back later with a "conversation extractor" for personal threads
3. Track which threads have been classified and which are new
4. Re-classify only when the classify prompt changes significantly
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS thread_classifications (
    thread_id TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    confidence REAL NOT NULL DEFAULT 0.0,
    card_types TEXT NOT NULL DEFAULT '[]',
    message_count INTEGER NOT NULL DEFAULT 0,
    first_subject TEXT NOT NULL DEFAULT '',
    first_from_email TEXT NOT NULL DEFAULT '',
    classify_model TEXT NOT NULL DEFAULT '',
    classify_prompt_version TEXT NOT NULL DEFAULT '',
    run_id TEXT NOT NULL DEFAULT '',
    classified_at TEXT NOT NULL,
    extracted INTEGER NOT NULL DEFAULT 0,
    extraction_run_id TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_tc_category ON thread_classifications(category);
CREATE INDEX IF NOT EXISTS idx_tc_extracted ON thread_classifications(extracted);
"""


class ClassifyIndex:
    """SQLite-backed persistent thread classification index."""

    def __init__(self, db_path: Path | str):
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self._path), timeout=120.0, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()
        self._lock = threading.RLock()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> ClassifyIndex:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def put_classification(
        self,
        thread_id: str,
        category: str,
        confidence: float,
        card_types: list[str],
        *,
        message_count: int = 0,
        first_subject: str = "",
        first_from_email: str = "",
        classify_model: str = "",
        classify_prompt_version: str = "",
        run_id: str = "",
    ) -> None:
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO thread_classifications
               (thread_id, category, confidence, card_types, message_count,
                first_subject, first_from_email, classify_model,
                classify_prompt_version, run_id, classified_at, extracted, extraction_run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '')""",
                (
                    thread_id,
                    category,
                    confidence,
                    json.dumps(card_types),
                    message_count,
                    first_subject[:200],
                    first_from_email[:100],
                    classify_model,
                    classify_prompt_version,
                    run_id,
                    time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                ),
            )
            self._conn.commit()

    def mark_extracted(self, thread_id: str, extraction_run_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE thread_classifications SET extracted = 1, extraction_run_id = ? WHERE thread_id = ?",
                (extraction_run_id, thread_id),
            )
            self._conn.commit()

    def get(self, thread_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM thread_classifications WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            if not row:
                return None
            cols = [d[0] for d in self._conn.execute("SELECT * FROM thread_classifications LIMIT 0").description]
        d = dict(zip(cols, row))
        d["card_types"] = json.loads(d.get("card_types") or "[]")
        return d

    def get_by_category(self, category: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT thread_id, confidence, card_types, message_count, first_subject, first_from_email, extracted "
                "FROM thread_classifications WHERE category = ? ORDER BY confidence DESC",
                (category,),
            ).fetchall()
        return [
            {
                "thread_id": r[0],
                "confidence": r[1],
                "card_types": json.loads(r[2] or "[]"),
                "message_count": r[3],
                "first_subject": r[4],
                "first_from_email": r[5],
                "extracted": bool(r[6]),
            }
            for r in rows
        ]

    def stats(self) -> dict[str, Any]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT category, COUNT(*), SUM(extracted) FROM thread_classifications GROUP BY category"
            ).fetchall()
        categories = {}
        total = 0
        total_extracted = 0
        for cat, cnt, ext in rows:
            categories[cat] = {"count": cnt, "extracted": int(ext or 0)}
            total += cnt
            total_extracted += int(ext or 0)
        return {
            "total_classified": total,
            "total_extracted": total_extracted,
            "categories": categories,
        }

    def unextracted_transactional(self) -> list[str]:
        """Thread IDs classified as transactional but not yet extracted."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT thread_id FROM thread_classifications WHERE category = 'transactional' AND extracted = 0"
            ).fetchall()
        return [r[0] for r in rows]

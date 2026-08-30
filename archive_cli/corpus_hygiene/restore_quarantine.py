"""Restore vault-removed quarantine notes from a read-only source vault (the seed)."""

from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from archive_sync.gmail_promotion.ledger import default_ledger_path

from .apply import _format_mins_secs, safe_vault_file
from .decision_io import decisions_artifact_path, load_decision_records_jsonl
from .decisions import EmailCorpusDecisionRecord
from .state_store import (
    CORPUS_STATE_QUARANTINE,
    quarantine_uids_for_records,
)

logger = logging.getLogger("ppa.corpus_hygiene")

DEFAULT_COPY_PROGRESS_EVERY = 200
CANONICAL_SEED_MARKER = "hf-archives-seed"


@dataclass
class RestoreQuarantineCounts:
    quarantine_threads: int = 0
    uids_requested: int = 0
    paths_mapped: int = 0
    files_copied: int = 0
    files_already_present: int = 0
    files_missing_from_source: int = 0
    uids_unmapped: int = 0
    ledger_lines_dropped: int = 0
    rematerialized: bool = False
    rematerialize_counts: dict[str, Any] = field(default_factory=dict)


def scan_cache_path(vault: Path) -> Path:
    return Path(vault) / "_meta" / "vault-scan-cache.sqlite3"


def rel_paths_from_scan_cache(cache_db: Path, uids: list[str]) -> dict[str, str]:
    """Bulk uid -> rel_path from a VaultScanCache sqlite. Skips ``_artifacts/`` rows."""

    if not uids:
        return {}
    if not Path(cache_db).is_file():
        raise FileNotFoundError(f"vault scan cache missing: {cache_db}")
    mapped: dict[str, str] = {}
    con = sqlite3.connect(f"file:{cache_db}?mode=ro", uri=True)
    try:
        for i in range(0, len(uids), 800):
            batch = uids[i : i + 800]
            marks = ",".join("?" * len(batch))
            rows = con.execute(
                f"SELECT uid, rel_path FROM notes WHERE uid IN ({marks})",
                batch,
            ).fetchall()
            for uid, rel in rows:
                rel_s = str(rel or "").strip()
                if not uid or not rel_s or rel_s.startswith("_artifacts/"):
                    continue
                mapped[str(uid)] = rel_s
    finally:
        con.close()
    return mapped


def drop_quarantine_from_ledger(ledger_path: Path) -> int:
    """Rewrite the promotion ledger, dropping quarantine lines so Gmail can update them."""

    path = Path(ledger_path)
    if not path.is_file():
        return 0
    kept: list[str] = []
    dropped = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw:
            continue
        rec = json.loads(raw)
        if str(rec.get("corpus_decision") or "") == CORPUS_STATE_QUARANTINE:
            dropped += 1
            continue
        kept.append(json.dumps(rec, sort_keys=True))
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(("\n".join(kept) + ("\n" if kept else "")), encoding="utf-8")
    tmp.replace(path)
    logger.info("hygiene ledger dropped quarantine lines=%d path=%s", dropped, path)
    return dropped


def copy_notes_from_source(
    *,
    source_vault: Path,
    dest_vault: Path,
    rel_paths: list[str],
    progress_every: int = DEFAULT_COPY_PROGRESS_EVERY,
) -> tuple[int, int, int]:
    """Copy *rel_paths* from source to dest. Never writes the source vault."""

    source_root = Path(source_vault).resolve()
    dest_root = Path(dest_vault).resolve()
    if dest_root == source_root:
        raise ValueError("refusing to copy a vault onto itself")
    if CANONICAL_SEED_MARKER in dest_root.as_posix():
        raise ValueError("refusing to restore into the canonical seed")

    n = len(rel_paths)
    copied = 0
    already = 0
    missing = 0
    t0 = time.perf_counter()
    logger.info("hygiene quarantine restore copy start files=%d source=%s dest=%s", n, source_root, dest_root)
    for i, rel in enumerate(rel_paths, start=1):
        src = safe_vault_file(source_root, rel)
        dest = safe_vault_file(dest_root, rel)
        if dest is None or src is None:
            missing += 1
            continue
        if dest.is_file():
            already += 1
            continue
        if not src.is_file():
            missing += 1
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dest)
        copied += 1
        if progress_every > 0 and (i % progress_every == 0 or i == n):
            elapsed = time.perf_counter() - t0
            rate = i / elapsed if elapsed > 0 else 0.0
            remaining = (n - i) / rate if rate > 0 else 0.0
            logger.info(
                "hygiene quarantine restore files=%d/%d (%.1f%%) elapsed=%s eta_remaining=%s rate_files_per_s=%.1f",
                i,
                n,
                100.0 * i / n if n else 100.0,
                _format_mins_secs(elapsed),
                _format_mins_secs(remaining),
                rate,
            )
    return copied, already, missing


def restore_quarantine_notes(
    records: list[EmailCorpusDecisionRecord],
    *,
    source_vault: Path,
    dest_vault: Path,
    source_cache: Path | None = None,
    rematerialize: bool = False,
    store: Any | None = None,
    progress_every: int = DEFAULT_COPY_PROGRESS_EVERY,
) -> RestoreQuarantineCounts:
    """Copy quarantine notes back from *source_vault* and drop them from the Gmail ledger."""

    counts = RestoreQuarantineCounts()
    q_records = [rec for rec in records if rec.corpus_decision == CORPUS_STATE_QUARANTINE]
    counts.quarantine_threads = len(q_records)
    uids = quarantine_uids_for_records(records)
    counts.uids_requested = len(uids)
    cache = Path(source_cache) if source_cache is not None else scan_cache_path(source_vault)
    mapped = rel_paths_from_scan_cache(cache, uids)
    counts.paths_mapped = len(mapped)
    counts.uids_unmapped = max(0, len(uids) - len(mapped))
    if counts.uids_unmapped:
        logger.warning(
            "hygiene quarantine restore unmapped uids=%d sample=%s",
            counts.uids_unmapped,
            [uid for uid in uids if uid not in mapped][:8],
        )
    rel_paths = sorted(set(mapped.values()))
    copied, already, missing = copy_notes_from_source(
        source_vault=source_vault,
        dest_vault=dest_vault,
        rel_paths=rel_paths,
        progress_every=progress_every,
    )
    counts.files_copied = copied
    counts.files_already_present = already
    counts.files_missing_from_source = missing
    counts.ledger_lines_dropped = drop_quarantine_from_ledger(default_ledger_path(dest_vault))

    if rematerialize:
        if store is None:
            raise ValueError("rematerialize requires a store")
        logger.info("hygiene quarantine restore rematerialize uids=%d", len(uids))
        counts.rematerialize_counts = dict(
            store.rebuild(force_full=False, uid_allowlist=uids, progress_every=progress_every) or {}
        )
        counts.rematerialized = True
    return counts


def restore_quarantine_from_decision_run(
    *,
    decision_run_id: str,
    repo_root: Path,
    source_vault: Path,
    dest_vault: Path,
    rematerialize: bool = False,
    store: Any | None = None,
    progress_every: int = DEFAULT_COPY_PROGRESS_EVERY,
) -> RestoreQuarantineCounts:
    decisions_path = decisions_artifact_path(repo_root, decision_run_id)
    records = load_decision_records_jsonl(decisions_path)
    return restore_quarantine_notes(
        records,
        source_vault=source_vault,
        dest_vault=dest_vault,
        rematerialize=rematerialize,
        store=store,
        progress_every=progress_every,
    )

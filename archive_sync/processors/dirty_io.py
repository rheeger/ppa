"""Load dirty UIDs and resolve processor input snapshots (Section E Phase 2)."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any, Iterable

from archive_sync.cli_logging import log_ratio_progress

from .constants import CORPUS_ACTIVE
from .staleness import ProcessorInputSnapshot
from .state_store import ProcessorInputStateRecord, ProcessorStateStore

logger = logging.getLogger("ppa.processors")

_PRIOR_PROCESSOR_KEYS = (
    "materialization",
    "email_typed_extraction",
    "email_thread_enrichment",
    "embedding",
    "linkers",
    "entity_resolution",
    "email_promotion_policy",
)


def load_dirty_uids(path: Path) -> list[str]:
    """Load UIDs from dirty_uids.jsonl (one per line) or a JSON list/object."""

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    # JSON snapshot / list
    if text[0] in "[{":
        raw = json.loads(text)
        if isinstance(raw, list):
            if raw and isinstance(raw[0], dict):
                return [str(item.get("input_uid") or item.get("uid") or "").strip() for item in raw if item]
            return [str(uid).strip() for uid in raw if str(uid).strip()]
        if isinstance(raw, dict):
            if "inputs" in raw:
                return [
                    str(item.get("input_uid") or item.get("uid") or "").strip()
                    for item in (raw.get("inputs") or [])
                    if isinstance(item, dict)
                ]
            if "dirty_card_uids" in raw:
                return [str(uid).strip() for uid in (raw.get("dirty_card_uids") or []) if str(uid).strip()]
            if "uids" in raw:
                return [str(uid).strip() for uid in (raw.get("uids") or []) if str(uid).strip()]
        return []
    # jsonl: one UID per line (D Phase 2 artifact)
    uids: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line[0] == "{":
            try:
                obj = json.loads(line)
                uid = str(obj.get("input_uid") or obj.get("uid") or "").strip()
                if uid:
                    uids.append(uid)
                continue
            except json.JSONDecodeError:
                pass
        uids.append(line)
    return uids


def load_input_snapshots_from_file(path: Path) -> list[ProcessorInputSnapshot] | None:
    """If PATH is a Phase-1 JSON snapshot list, return snapshots; else None (use UID loader)."""

    text = path.read_text(encoding="utf-8").strip()
    if not text or text[0] not in "[{":
        return None
    raw = json.loads(text)
    items = raw if isinstance(raw, list) else raw.get("inputs", [])
    if not items or not isinstance(items[0], dict):
        return None
    if "card_type" not in items[0] and "input_uid" not in items[0]:
        return None
    # Only treat as full snapshots when card_type (or rich fields) present
    if not any(isinstance(i, dict) and i.get("card_type") for i in items):
        return None
    snapshots: list[ProcessorInputSnapshot] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        snapshots.append(
            ProcessorInputSnapshot(
                input_uid=str(item.get("input_uid") or ""),
                card_type=str(item.get("card_type") or ""),
                corpus_state=str(item.get("corpus_state") or CORPUS_ACTIVE),
                processor_decision=str(item.get("processor_decision") or ""),
                field_values=dict(item.get("field_values") or {}),
                source_dirty=bool(item.get("source_dirty", True)),
                upstream_complete=bool(item.get("upstream_complete", True)),
                recorded_input_hash=str(item.get("recorded_input_hash") or ""),
                recorded_processor_version=str(item.get("recorded_processor_version") or ""),
                recorded_corpus_state=str(item.get("recorded_corpus_state") or ""),
                output_exists=bool(item.get("output_exists", False)),
                output_failed=bool(item.get("output_failed", False)),
                upstream_output_hash=str(item.get("upstream_output_hash") or ""),
                recorded_upstream_output_hash=str(item.get("recorded_upstream_output_hash") or ""),
            )
        )
    return snapshots


def _note_meta_from_frontmatter(uid: str, fm: dict[str, Any], *, body: str = "") -> dict[str, Any]:
    card_type = str(fm.get("type") or "")
    body_sha = str(fm.get("body_sha") or fm.get("content_hash") or "")
    if not body_sha and body:
        import hashlib

        body_sha = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    return {
        "card_type": card_type,
        "body_sha": body_sha,
        "thread_uid": str(fm.get("thread_uid") or (uid if card_type == "email_thread" else "")),
        "frontmatter_hash": str(fm.get("frontmatter_hash") or ""),
        "chunk_hash": str(fm.get("chunk_hash") or body_sha),
        "processor_decision": str(fm.get("processor_decision") or ""),
        "corpus_state": str(fm.get("corpus_state") or fm.get("corpus_decision") or ""),
    }


def _resolve_notes_bulk(vault_path: str | Path | None, uids: list[str]) -> dict[str, dict[str, Any]]:
    """Resolve frontmatter for dirty UIDs via Rust cache batch, then Python IN-query."""

    if not vault_path or not uids:
        return {}
    vault = Path(vault_path)
    cache_path = vault / "_meta" / "vault-scan-cache.sqlite3"
    rows: list[dict[str, Any]] = []
    try:
        from archive_cli.ppa_engine import ppa_engine

        if ppa_engine() == "rust" and cache_path.exists():
            import archive_crate

            if hasattr(archive_crate, "frontmatter_for_uids"):
                raw = archive_crate.frontmatter_for_uids(str(cache_path), uids)
                rows = [dict(item) for item in (raw or [])]
    except Exception:
        logger.debug("processor plan rust frontmatter_for_uids failed; using vault cache", exc_info=True)
        rows = []
    if not rows:
        try:
            from archive_cli.vault_cache import VaultScanCache

            cache = VaultScanCache.build_or_load(vault, tier=1, progress_every=5000)
            rows = cache.frontmatter_rows_for_uids(uids)
        except Exception:
            logger.warning("processor plan vault-cache bulk resolve failed", exc_info=True)
            rows = []
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        uid = str(row.get("uid") or "").strip()
        fm = row.get("frontmatter") or {}
        if uid and isinstance(fm, dict):
            out[uid] = _note_meta_from_frontmatter(uid, fm)
    return out


def _resolve_note_meta(vault_path: str | Path | None, uid: str) -> dict[str, Any]:
    """Fallback for a single UID missing from the cache (just-written notes)."""

    if not vault_path:
        return {}
    try:
        from archive_vault.vault import read_note_by_uid

        found = read_note_by_uid(vault_path, uid)
    except Exception:
        return {}
    if not found:
        return {}
    _rel, fm, body, _prov = found
    return _note_meta_from_frontmatter(uid, fm, body=body)


def _resolve_corpus_state(
    uid: str,
    *,
    store: Any | None,
    default: str = CORPUS_ACTIVE,
) -> str:
    if store is None:
        return default
    index = getattr(store, "index", None)
    if index is None:
        return default
    schema = str(getattr(index, "schema", "ppa"))
    try:
        from archive_cli.corpus_hygiene.state_store import (
            corpus_state_table_exists,
            get_card_corpus_state,
        )

        with index._connect() as conn:
            if not corpus_state_table_exists(conn, schema):
                return default
            return get_card_corpus_state(conn, schema, uid) or default
    except Exception:
        return default


def _resolve_processor_decision(
    uid: str,
    *,
    store: Any | None,
    fallback: str = "",
) -> str:
    if store is None:
        return fallback
    index = getattr(store, "index", None)
    if index is None:
        return fallback
    schema = str(getattr(index, "schema", "ppa"))
    try:
        with index._connect() as conn:
            row = conn.execute(
                f"""
                SELECT processor_decision FROM {schema}.email_corpus_decisions
                WHERE thread_uid = %s
                ORDER BY applied_at DESC NULLS LAST
                LIMIT 1
                """,
                (uid,),
            ).fetchone()
            if row is None:
                return fallback
            return str(row["processor_decision"] if isinstance(row, dict) else row[0] or fallback)
    except Exception:
        return fallback


def _resolve_corpus_state_bulk(uids: list[str], *, store: Any | None, default: str = CORPUS_ACTIVE) -> dict[str, str]:
    out = {uid: default for uid in uids}
    if store is None or not uids:
        return out
    index = getattr(store, "index", None)
    if index is None:
        return out
    schema = str(getattr(index, "schema", "ppa"))
    try:
        from archive_cli.corpus_hygiene.state_store import corpus_state_table_exists

        with index._connect() as conn:
            if not corpus_state_table_exists(conn, schema):
                return out
            rows = conn.execute(
                f"SELECT card_uid, corpus_state FROM {schema}.card_corpus_state WHERE card_uid = ANY(%s)",
                (uids,),
            ).fetchall()
        for row in rows:
            uid = str(row["card_uid"] if isinstance(row, dict) else row[0])
            state = str(row["corpus_state"] if isinstance(row, dict) else row[1] or default)
            if uid:
                out[uid] = state or default
    except Exception:
        logger.debug("processor plan bulk corpus_state failed", exc_info=True)
    return out


def _resolve_processor_decision_bulk(
    uids: list[str],
    *,
    store: Any | None,
    fallback: str = "",
) -> dict[str, str]:
    out = {uid: fallback for uid in uids}
    if store is None or not uids:
        return out
    index = getattr(store, "index", None)
    if index is None:
        return out
    schema = str(getattr(index, "schema", "ppa"))
    try:
        with index._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT DISTINCT ON (thread_uid) thread_uid, processor_decision
                FROM {schema}.email_corpus_decisions
                WHERE thread_uid = ANY(%s)
                ORDER BY thread_uid, applied_at DESC NULLS LAST
                """,
                (uids,),
            ).fetchall()
        for row in rows:
            uid = str(row["thread_uid"] if isinstance(row, dict) else row[0])
            decision = str(row["processor_decision"] if isinstance(row, dict) else row[1] or fallback)
            if uid:
                out[uid] = decision or fallback
    except Exception:
        logger.debug("processor plan bulk processor_decision failed", exc_info=True)
    return out


def _prior_from_records(records: dict[str, ProcessorInputStateRecord] | None) -> tuple[str, str, str, bool, bool]:
    recorded_hash = ""
    recorded_version = ""
    recorded_corpus = ""
    output_exists = False
    output_failed = False
    if not records:
        return recorded_hash, recorded_version, recorded_corpus, output_exists, output_failed
    for key in _PRIOR_PROCESSOR_KEYS:
        prior = records.get(key)
        if prior is None:
            continue
        if prior.input_hash and not recorded_hash:
            recorded_hash = prior.input_hash
        if prior.processor_version and not recorded_version:
            recorded_version = prior.processor_version
        if prior.input_corpus_state and not recorded_corpus:
            recorded_corpus = prior.input_corpus_state
        if prior.status == "complete":
            output_exists = True
        if prior.status == "failed":
            output_failed = True
        break
    return recorded_hash, recorded_version, recorded_corpus, output_exists, output_failed


def resolve_snapshots_for_uids(
    uids: Iterable[str],
    *,
    vault_path: str | Path | None = None,
    store: Any | None = None,
    state_store: ProcessorStateStore | None = None,
    default_card_type: str = "email_thread",
    default_processor_decision: str = "",
    source_dirty: bool = True,
    progress_every: int = 500,
) -> list[ProcessorInputSnapshot]:
    """Resolve card_type / corpus_state / hash fields for dirty UIDs (bulk SQL / cache)."""

    wanted = [str(uid).strip() for uid in uids if str(uid).strip()]
    if not wanted:
        return []
    started = time.monotonic()
    logger.info("processor plan resolve start uids=%s", len(wanted))
    notes_by_uid = _resolve_notes_bulk(vault_path, wanted)
    missing = [uid for uid in wanted if uid not in notes_by_uid]
    if missing:
        logger.info("processor plan cache miss uids=%s falling back to per-uid read", len(missing))
        for i, uid in enumerate(missing, start=1):
            notes_by_uid[uid] = _resolve_note_meta(vault_path, uid)
            log_ratio_progress(
                logger,
                "processor plan note fallback",
                i,
                len(missing),
                started,
                every=max(1, progress_every),
            )
    corpus_by_uid = _resolve_corpus_state_bulk(wanted, store=store)
    decision_by_uid = _resolve_processor_decision_bulk(wanted, store=store, fallback=default_processor_decision)
    priors: dict[str, dict[str, ProcessorInputStateRecord]] = {}
    if state_store is not None:
        priors = state_store.get_input_states_for_uids(wanted)

    snapshots: list[ProcessorInputSnapshot] = []
    build_started = time.monotonic()
    for i, uid in enumerate(wanted, start=1):
        note = notes_by_uid.get(uid) or {}
        card_type = note.get("card_type") or default_card_type
        corpus_state = note.get("corpus_state") or corpus_by_uid.get(uid) or CORPUS_ACTIVE
        processor_decision = note.get("processor_decision") or decision_by_uid.get(uid) or default_processor_decision
        field_values: dict[str, Any] = {
            "body_sha": note.get("body_sha") or uid,
            "thread_uid": note.get("thread_uid") or uid,
            "frontmatter_hash": note.get("frontmatter_hash") or "",
            "chunk_hash": note.get("chunk_hash") or note.get("body_sha") or uid,
            "corpus_state": corpus_state,
            "processor_decision": processor_decision,
        }
        recorded_hash, recorded_version, recorded_corpus, output_exists, output_failed = _prior_from_records(
            priors.get(uid)
        )
        snapshots.append(
            ProcessorInputSnapshot(
                input_uid=uid,
                card_type=card_type,
                corpus_state=corpus_state or CORPUS_ACTIVE,
                processor_decision=processor_decision,
                field_values=field_values,
                source_dirty=source_dirty,
                upstream_complete=True,
                recorded_input_hash=recorded_hash,
                recorded_processor_version=recorded_version,
                recorded_corpus_state=recorded_corpus,
                output_exists=output_exists,
                output_failed=output_failed,
            )
        )
        log_ratio_progress(
            logger,
            "processor plan snapshots",
            i,
            len(wanted),
            build_started,
            every=max(1, progress_every),
        )
    logger.info(
        "processor plan resolve done snapshots=%s elapsed=%.1fs",
        len(snapshots),
        time.monotonic() - started,
    )
    return snapshots


def load_dirty_inputs(
    path: Path | None = None,
    *,
    dirty_uids: Iterable[str] | None = None,
    vault_path: str | Path | None = None,
    store: Any | None = None,
    state_store: ProcessorStateStore | None = None,
    default_card_type: str = "email_thread",
    default_processor_decision: str = "",
) -> list[ProcessorInputSnapshot]:
    """Unified loader: PATH (jsonl/JSON) and/or explicit UID list → snapshots."""

    if path is not None:
        snaps = load_input_snapshots_from_file(path)
        if snaps is not None:
            return snaps
        uids = load_dirty_uids(path)
        return resolve_snapshots_for_uids(
            uids,
            vault_path=vault_path,
            store=store,
            state_store=state_store,
            default_card_type=default_card_type,
            default_processor_decision=default_processor_decision,
            source_dirty=True,
        )
    return resolve_snapshots_for_uids(
        list(dirty_uids or []),
        vault_path=vault_path,
        store=store,
        state_store=state_store,
        default_card_type=default_card_type,
        default_processor_decision=default_processor_decision,
        source_dirty=True,
    )


def dirty_uids_from_source_reports(reports: list[dict[str, Any]]) -> list[str]:
    """Collect dirty UIDs from Section D run report dicts (in-process maintain handoff)."""

    seen: set[str] = set()
    out: list[str] = []
    for report in reports:
        uids = report.get("dirty_card_uids") or []
        if not uids and isinstance(report.get("batch"), dict):
            uids = report["batch"].get("dirty_card_uids") or []
        for uid in uids:
            uid = str(uid).strip()
            if uid and uid not in seen:
                seen.add(uid)
                out.append(uid)
        paths = report.get("artifact_paths") or {}
        dirty_path = paths.get("dirty_uids")
        if dirty_path:
            for uid in load_dirty_uids(Path(dirty_path)):
                if uid not in seen:
                    seen.add(uid)
                    out.append(uid)
    return out

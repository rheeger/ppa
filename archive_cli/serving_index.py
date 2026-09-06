"""Process-level handle for the Rust serving index.

The store is cheap and constructed per MCP call. This module caches the native
mmap handle keyed by (vault, index_root, ACTIVE generation).
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from pathlib import Path
from typing import Any

from archive_engine.contracts import (
    CHUNK_EVIDENCE_REF_VERSION,
    UNKNOWN,
    ChunkEvidenceRef,
    EmbeddingSpec,
    ServingEdge,
)
from archive_engine.publication import (
    ServingSnapshot,
    pin_generation,
    publish_snapshot,
    referenced_generations,
    should_compact,
    unpin_generation,
    walk_generation_chain,
)

from .corpus_hygiene.state_store import QUARANTINE_RETRIEVAL_WEIGHT
from .errors import ServingIndexUnavailableError
from .index_config import (
    CHUNK_SCHEMA_VERSION,
    get_default_embedding_model,
    get_default_embedding_version,
    get_query_embed_cache_max_age_days,
    get_query_embed_cache_max_rows,
    get_query_embed_cache_path,
    get_query_embed_cache_ram_entries,
    get_serving_candidate_budget,
    get_serving_index_max_rss_mb,
    get_serving_index_path,
    get_serving_nlist,
    get_serving_nprobe,
    get_serving_train_iters,
    get_serving_train_memory_mb,
    get_serving_train_sample,
    get_serving_train_seed,
    get_vector_dimension,
)
from .query_embed_cache import QueryEmbedCache, validate_embedding_spec

logger = logging.getLogger("ppa.serving_index")

KNOWN_CORPUS_STATES = frozenset({"active", "quarantine", "suppressed"})
WAREHOUSE_EDGE_CONFIDENCE = 1.0
INFERRED_EDGE_METHOD = "inferred"
SERVING_EMBEDDING_METRIC = "cosine"
SERVING_EMBEDDING_NORMALIZATION = "l2"


class ServingFidelityError(ValueError):
    """Trust-critical serving export would invent active/trusted policy."""


def serving_embedding_spec() -> EmbeddingSpec:
    """Build the live serving EmbeddingSpec from config. Does not invent model identity."""

    provider = os.environ.get("PPA_EMBEDDING_PROVIDER", "").strip() or "unspecified"
    spec = EmbeddingSpec(
        provider_namespace=provider,
        model=get_default_embedding_model(),
        model_revision=str(get_default_embedding_version()),
        dimension=get_vector_dimension(),
        metric=SERVING_EMBEDDING_METRIC,
        normalization=SERVING_EMBEDDING_NORMALIZATION,
        chunk_schema=str(CHUNK_SCHEMA_VERSION),
    )
    return validate_embedding_spec(spec)


def serving_train_config(spec: EmbeddingSpec | None = None) -> dict[str, Any]:
    """Frozen ANN training knobs for serving_index_build."""

    return {
        "nlist": get_serving_nlist(),
        "nprobe": get_serving_nprobe(),
        "train_sample": get_serving_train_sample(),
        "train_iters": get_serving_train_iters(),
        "seed": get_serving_train_seed(),
        "candidate_budget": get_serving_candidate_budget(),
        "memory_mb": get_serving_train_memory_mb(),
        "embedding_spec": (spec or serving_embedding_spec()).to_payload(),
    }


def serving_chunk_evidence_ref(card_uid: str, chunk_key: str) -> ChunkEvidenceRef:
    """Legacy-unavailable evidence for warehouse chunks. Does not invent spans."""

    return ChunkEvidenceRef(
        version=CHUNK_EVIDENCE_REF_VERSION,
        archive_id="local",
        card_uid=card_uid,
        chunk_id=chunk_key,
        chunk_schema_version=str(CHUNK_SCHEMA_VERSION),
        algorithm_version="p01b-freeze-1",
        evidence_kind=UNKNOWN,
        lineage_complete=False,
        span_unavailable=True,
        message_refs_available=False,
    )


def build_serving_chunk(row: Any) -> dict[str, Any]:
    uid = str(row["card_uid"])
    key = str(row["chunk_key"])
    return {
        "chunk_key": key,
        "card_uid": uid,
        "chunk_type": str(row.get("chunk_type") or ""),
        "chunk_index": int(row.get("chunk_index") or 0),
        "evidence": serving_chunk_evidence_ref(uid, key).to_payload(),
    }


def serving_corpus_state(raw: Any) -> str:
    cleaned = str(raw or "").strip()
    if cleaned in KNOWN_CORPUS_STATES:
        return cleaned
    return UNKNOWN


def serving_retrieval_weight(state: str) -> float | None:
    if state == "quarantine":
        return float(QUARANTINE_RETRIEVAL_WEIGHT)
    if state == "active":
        return 1.0
    if state == "suppressed":
        return 0.0
    return None


def _as_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    raw: Any = value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text[:1] in "[{":
            try:
                raw = json.loads(text)
            except json.JSONDecodeError:
                return [text]
        else:
            return [text]
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [str(raw).strip()] if str(raw).strip() else []


def _optional_rows(conn: Any, sql: str, params: tuple[Any, ...] | None = None) -> list[Any]:
    try:
        conn.execute("SAVEPOINT ppa_optional_export")
        rows = list(conn.execute(sql) if params is None else conn.execute(sql, params))
        conn.execute("RELEASE SAVEPOINT ppa_optional_export")
        return rows
    except Exception:
        try:
            conn.execute("ROLLBACK TO SAVEPOINT ppa_optional_export")
        except Exception:
            pass
        return []


def _uid_clause(column: str, uids: list[str] | None) -> tuple[str, tuple[Any, ...]]:
    if uids is None:
        return "", ()
    return f" AND {column} = ANY(%s)", (uids,)


def load_serving_export_maps(conn: Any, schema: str, uids: list[str] | None = None) -> dict[str, Any]:
    """Bulk-load relation / policy maps once per export."""

    people: dict[str, list[str]] = {}
    sources: dict[str, list[str]] = {}
    orgs: dict[str, list[str]] = {}
    aliases: dict[str, list[str]] = {}
    emails: dict[str, list[str]] = {}
    external_ids: dict[str, list[str]] = {}
    corpus_states: dict[str, str] = {}

    people_clause, people_params = _uid_clause("card_uid", uids)
    map_params = people_params or None
    for row in _optional_rows(conn, f"SELECT card_uid, person FROM {schema}.card_people WHERE TRUE{people_clause}", map_params):
        people.setdefault(str(row["card_uid"]), []).append(str(row["person"]))
    for row in _optional_rows(conn, f"SELECT card_uid, source FROM {schema}.card_sources WHERE TRUE{people_clause}", map_params):
        sources.setdefault(str(row["card_uid"]), []).append(str(row["source"]))
    for row in _optional_rows(conn, f"SELECT card_uid, org FROM {schema}.card_orgs WHERE TRUE{people_clause}", map_params):
        orgs.setdefault(str(row["card_uid"]), []).append(str(row["org"]))
    for row in _optional_rows(
        conn,
        f"SELECT card_uid, aliases_json, emails_json FROM {schema}.people WHERE TRUE{people_clause}",
        map_params,
    ):
        uid = str(row["card_uid"])
        aliases[uid] = _as_str_list(row.get("aliases_json"))
        emails[uid] = _as_str_list(row.get("emails_json"))
    for row in _optional_rows(
        conn,
        f"SELECT card_uid, external_id FROM {schema}.external_ids WHERE TRUE{people_clause}",
        map_params,
    ):
        ext = str(row.get("external_id") or "").strip()
        if ext:
            external_ids.setdefault(str(row["card_uid"]), []).append(ext)
    state_rows = _optional_rows(
        conn,
        f"SELECT card_uid, corpus_state FROM {schema}.card_corpus_state WHERE TRUE{people_clause}",
        map_params,
    )
    corpus_state_table = False
    try:
        conn.execute("SAVEPOINT ppa_corpus_state_probe")
        probe = list(conn.execute(f"SELECT 1 FROM {schema}.card_corpus_state LIMIT 1"))
        conn.execute("RELEASE SAVEPOINT ppa_corpus_state_probe")
        corpus_state_table = True
        _ = probe
    except Exception:
        try:
            conn.execute("ROLLBACK TO SAVEPOINT ppa_corpus_state_probe")
        except Exception:
            pass
        corpus_state_table = False
    if corpus_state_table:
        for row in state_rows:
            corpus_states[str(row["card_uid"])] = serving_corpus_state(row.get("corpus_state"))
    return {
        "people": people,
        "sources": sources,
        "orgs": orgs,
        "aliases": aliases,
        "emails": emails,
        "external_ids": external_ids,
        "corpus_states": corpus_states,
        "corpus_state_table": corpus_state_table,
    }


def build_serving_card(row: Any, maps: dict[str, Any]) -> dict[str, Any]:
    uid = str(row["uid"])
    table_present = bool(maps.get("corpus_state_table"))
    claimed = maps.get("corpus_states", {}).get(uid)
    policy_present = table_present and uid in maps.get("corpus_states", {})
    if not table_present and serving_corpus_state(claimed) == "active":
        raise ServingFidelityError(f"missing corpus_state must not upgrade to active uid={uid}")
    if policy_present:
        state = serving_corpus_state(claimed)
    else:
        state = UNKNOWN
    if state == "active" and not policy_present:
        raise ServingFidelityError(f"missing corpus_state must not upgrade to active uid={uid}")
    rec = {
        "card_uid": uid,
        "rel_path": str(row["rel_path"] or ""),
        "summary": str(row["summary"] or ""),
        "type": str(row["type"] or ""),
        "slug": str(row["slug"] or ""),
        "activity_at": str(row["activity_at"] or ""),
        "activity_end_at": str(row["activity_end_at"] or ""),
        "search_text": str(row["search_text"] or ""),
        "people": list(maps["people"].get(uid, [])),
        "sources": list(maps["sources"].get(uid, [])),
        "orgs": list(maps["orgs"].get(uid, [])),
        "aliases": list(maps["aliases"].get(uid, [])),
        "emails": list(maps["emails"].get(uid, [])),
        "external_ids": list(dict.fromkeys(maps["external_ids"].get(uid, []))),
        "corpus_state": state,
        "retrieval_weight": serving_retrieval_weight(state),
        "source_revision": str(row.get("content_hash") or ""),
        "provenance_summary": UNKNOWN,
    }
    return rec


def build_warehouse_edge(row: Any) -> dict[str, Any]:
    edge = ServingEdge(method=UNKNOWN, confidence=WAREHOUSE_EDGE_CONFIDENCE, evidence_uids=())
    payload = edge.to_payload()
    return {
        "source_uid": str(row.get("source_uid") or ""),
        "target_uid": str(row.get("target_uid") or ""),
        "edge_type": str(row.get("edge_type") or ""),
        "field_name": str(row.get("field_name") or ""),
        "direction": "forward",
        "method": payload["method"],
        "confidence": payload["confidence"],
        "evidence_uids": list(payload["evidence_uids"]),
        "trust": payload["confidence"],
    }


def build_inferred_edge(row: Any) -> dict[str, Any]:
    raw_conf = row.get("confidence")
    confidence = None if raw_conf is None else float(raw_conf)
    evidence = _as_str_list(row.get("evidence_uids"))
    edge = ServingEdge(method=INFERRED_EDGE_METHOD, confidence=confidence, evidence_uids=tuple(evidence))
    payload = edge.to_payload()
    rec = {
        "source_uid": str(row.get("source_uid") or row.get("source_card_uid") or ""),
        "target_uid": str(row.get("target_uid") or row.get("target_card_uid") or ""),
        "edge_type": str(row.get("edge_type") or row.get("proposed_link_type") or ""),
        "field_name": str(row.get("field_name") or row.get("target_field_name") or ""),
        "direction": "forward",
        "method": payload["method"],
        "confidence": payload["confidence"],
        "evidence_uids": list(payload["evidence_uids"]),
    }
    if payload["confidence"] is None:
        return rec
    rec["trust"] = payload["confidence"]
    return rec


def _card_export_sql(schema: str, *, incremental: bool) -> str:
    where = "WHERE c.uid = ANY(%s)" if incremental else ""
    return f"""
                SELECT c.uid, c.rel_path, c.summary, c.type, c.slug, c.activity_at,
                       c.activity_end_at, COALESCE(c.search_text, '') AS search_text,
                       COALESCE(c.content_hash, '') AS content_hash
                FROM {schema}.cards c
                {where}
                """


def load_serving_edges(conn: Any, schema: str, uids: list[str] | None = None) -> list[dict[str, Any]]:
    edges: list[dict[str, Any]] = []
    if uids is None:
        warehouse_sql = f"""
                        SELECT source_uid, target_uid, edge_type, field_name
                        FROM {schema}.edges
                        WHERE target_kind = 'card' AND target_uid <> ''
                        """
        warehouse_params: tuple[Any, ...] | None = None
        inferred_sql = f"""
                        SELECT lc.source_card_uid AS source_uid, lc.target_card_uid AS target_uid,
                               lc.proposed_link_type AS edge_type, pq.target_field_name AS field_name,
                               ld.final_confidence AS confidence
                        FROM {schema}.link_candidates lc
                        JOIN {schema}.promotion_queue pq
                          ON pq.candidate_id = lc.candidate_id
                         AND pq.promotion_target = 'derived_edge'
                         AND pq.promotion_status = 'applied'
                        LEFT JOIN {schema}.link_decisions ld ON ld.candidate_id = lc.candidate_id
                        WHERE lc.target_kind = 'card' AND lc.target_card_uid <> ''
                        """
        inferred_params: tuple[Any, ...] | None = None
    else:
        warehouse_sql = f"""
                        SELECT source_uid, target_uid, edge_type, field_name
                        FROM {schema}.edges
                        WHERE target_kind = 'card' AND target_uid <> ''
                          AND (source_uid = ANY(%s) OR target_uid = ANY(%s))
                        """
        warehouse_params = (uids, uids)
        inferred_sql = f"""
                        SELECT lc.source_card_uid AS source_uid, lc.target_card_uid AS target_uid,
                               lc.proposed_link_type AS edge_type, pq.target_field_name AS field_name,
                               ld.final_confidence AS confidence
                        FROM {schema}.link_candidates lc
                        JOIN {schema}.promotion_queue pq
                          ON pq.candidate_id = lc.candidate_id
                         AND pq.promotion_target = 'derived_edge'
                         AND pq.promotion_status = 'applied'
                        LEFT JOIN {schema}.link_decisions ld ON ld.candidate_id = lc.candidate_id
                        WHERE lc.target_kind = 'card' AND lc.target_card_uid <> ''
                          AND (lc.source_card_uid = ANY(%s) OR lc.target_card_uid = ANY(%s))
                        """
        inferred_params = (uids, uids)
    try:
        for row in conn.execute(warehouse_sql, warehouse_params) if warehouse_params else conn.execute(warehouse_sql):
            rec = build_warehouse_edge(row)
            if rec["source_uid"] and rec["target_uid"]:
                edges.append(rec)
    except Exception:
        logger.exception("serving_index warehouse edge export failed")
    for row in _optional_rows(conn, inferred_sql, inferred_params):
        rec = build_inferred_edge(row)
        if rec["source_uid"] and rec["target_uid"]:
            edges.append(rec)
    return edges

_LOCK = threading.RLock()
_HANDLES: dict[str, ServingIndexHandle] = {}
_HANDLE: ServingIndexHandle | None = None


def _vault_handle_key(vault: Path) -> str:
    return str(Path(vault).resolve())


def _crate():
    try:
        import archive_crate
    except ImportError as exc:
        raise ServingIndexUnavailableError("serving_index_unavailable") from exc
    return archive_crate


def _access_req(kwargs: dict[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key in (
        "access_deny",
        "access_restricted",
        "access_policy_identity",
        "access_sources",
        "access_domains",
    ):
        if key in kwargs:
            fields[key] = kwargs[key]
    for key in (
        "max_nodes",
        "max_edges",
        "max_depth",
        "max_elapsed_ms",
        "allowed_relation_types",
    ):
        if key in kwargs:
            fields[key] = kwargs[key]
    return fields


class ServingIndexHandle:
    def __init__(self, vault: Path, index_root: Path, generation_id: str, native: Any):
        self.vault = Path(vault)
        self.index_root = Path(index_root)
        self.generation_id = generation_id
        self._native = native
        self._closed = False
        pin_generation(self.index_root, self.generation_id)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        unpin_generation(self.index_root, self.generation_id)

    def search(self, query: str, **kwargs: Any) -> list[dict[str, Any]]:
        req = {
            "query": query,
            "limit": int(kwargs.get("limit", 20) or 20),
            "type_filter": str(kwargs.get("type_filter", "") or ""),
            "source_filter": str(kwargs.get("source_filter", "") or ""),
            "people_filter": str(kwargs.get("people_filter", "") or ""),
            "start_date": str(kwargs.get("start_date", "") or ""),
            "end_date": str(kwargs.get("end_date", "") or ""),
        }
        req.update(_access_req(kwargs))
        return list(_crate().serving_index_search(self._native, req) or [])

    def query(self, **kwargs: Any) -> list[dict[str, Any]]:
        req = dict(kwargs)
        req.update(_access_req(kwargs))
        return list(_crate().serving_index_query(self._native, req) or [])

    def typed_query(self, **kwargs: Any) -> dict[str, Any]:
        req = dict(kwargs)
        req.update(_access_req(kwargs))
        payload = _crate().serving_index_typed_query(self._native, req)
        return dict(payload or {})

    def vector(self, query_vector: list[float], **kwargs: Any) -> list[dict[str, Any]]:
        req = {
            "limit": int(kwargs.get("limit", 20) or 20),
            "type_filter": str(kwargs.get("type_filter", "") or ""),
            "source_filter": str(kwargs.get("source_filter", "") or ""),
            "people_filter": str(kwargs.get("people_filter", "") or ""),
            "start_date": str(kwargs.get("start_date", "") or ""),
            "end_date": str(kwargs.get("end_date", "") or ""),
            "nprobe": int(kwargs.get("nprobe", 0) or 0),
            "candidate_budget": int(kwargs.get("candidate_budget", 0) or 0),
        }
        if req["nprobe"] <= 0:
            req.pop("nprobe")
        if req["candidate_budget"] <= 0:
            req.pop("candidate_budget")
        req.update(_access_req(kwargs))
        rows = list(_crate().serving_index_vector(self._native, query_vector, req) or [])
        for row in rows:
            if row.get("score") is None:
                row["score"] = row.get("vector_similarity") or row.get("similarity") or 0.0
        return rows

    def hybrid(self, query: str, query_vector: list[float], **kwargs: Any) -> list[dict[str, Any]]:
        req = {
            "limit": int(kwargs.get("limit", 20) or 20),
            "type_filter": str(kwargs.get("type_filter", "") or ""),
            "source_filter": str(kwargs.get("source_filter", "") or ""),
            "people_filter": str(kwargs.get("people_filter", "") or ""),
            "start_date": str(kwargs.get("start_date", "") or ""),
            "end_date": str(kwargs.get("end_date", "") or ""),
        }
        req.update(_access_req(kwargs))
        return list(_crate().serving_index_hybrid(self._native, query, query_vector, req) or [])

    def graph(self, note_path: str, hops: int = 2, **kwargs: Any) -> dict[str, Any]:
        return dict(_crate().serving_index_graph(self._native, note_path, int(hops) or 1, _access_req(kwargs)) or {})

    def graph_bounded(self, note_path: str, hops: int = 1, **kwargs: Any) -> dict[str, Any]:
        return dict(
            _crate().serving_index_graph_bounded(self._native, note_path, int(hops) or 1, _access_req(kwargs)) or {}
        )

    def adjacent_chunks(self, chunk_key: str) -> dict[str, Any] | None:
        payload = _crate().serving_index_adjacent_chunks(self._native, chunk_key)
        return dict(payload) if payload else None

    def person(self, name: str, **kwargs: Any) -> dict[str, Any]:
        return dict(_crate().serving_index_person(self._native, name, _access_req(kwargs)) or {})

    def pointers(self, uids: list[str], **kwargs: Any) -> dict[str, dict[str, Any]]:
        return dict(_crate().serving_index_pointers(self._native, list(uids), _access_req(kwargs)) or {})

    def neighbor_uids(self, uids: list[str], hops: int = 1, **kwargs: Any) -> list[str]:
        return list(
            _crate().serving_index_neighbor_uids(self._native, list(uids), int(hops) or 1, _access_req(kwargs)) or []
        )

    def timeline(self, **kwargs: Any) -> list[dict[str, Any]]:
        req = dict(kwargs)
        req.update(_access_req(kwargs))
        return list(_crate().serving_index_timeline(self._native, req) or [])

    def temporal_neighbors(self, timestamp: str, **kwargs: Any) -> dict[str, Any]:
        req = dict(kwargs)
        req.update(_access_req(kwargs))
        return dict(_crate().serving_index_temporal_neighbors(self._native, timestamp, req) or {})

    def read_path(self, uid: str) -> str | None:
        return _crate().serving_index_read_path(self._native, uid)


def serving_index_status(vault: Path | None = None) -> dict[str, Any]:
    root = get_serving_index_path(vault)
    try:
        return dict(_crate().serving_index_status(str(root)) or {})
    except ServingIndexUnavailableError:
        return {
            "serving_index_generation": "",
            "serving_index_format": 0,
            "serving_index_dirty_records": 0,
            "serving_index_ready": False,
        }
    except Exception:
        return {
            "serving_index_generation": "",
            "serving_index_format": 0,
            "serving_index_dirty_records": 0,
            "serving_index_ready": False,
        }


def read_dirty_uids(vault: Path | None = None) -> list[str]:
    """Concrete UIDs from DIRTY records. Empty-uid ``vault_written`` lines are ignored."""

    root = get_serving_index_path(vault)
    path = root / "DIRTY"
    if not path.exists():
        return []
    uids: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        for uid in rec.get("uids") or []:
            text = str(uid).strip()
            if text:
                uids.add(text)
    return sorted(uids)


def prune_retired_serving_generations(
    vault: Path | None = None,
    *,
    keep: str | None = None,
    index_root: Path | None = None,
    logger: logging.Logger | None = None,
) -> list[str]:
    """Delete generations that are not ACTIVE, not in the parent chain, and not pinned."""

    log = logger or logging.getLogger("ppa.serving_index")
    root = Path(index_root) if index_root is not None else get_serving_index_path(vault)
    gens = root / "generations"
    if keep is None:
        active_path = root / "ACTIVE"
        try:
            keep = active_path.read_text(encoding="utf-8").strip()
        except OSError:
            keep = ""
    keep = str(keep or "").strip()
    retain = referenced_generations(root, keep)
    removed: list[str] = []
    if not gens.is_dir():
        return removed
    for child in sorted(gens.iterdir()):
        if not child.is_dir():
            continue
        if child.name in retain:
            continue
        try:
            shutil.rmtree(child)
            removed.append(child.name)
            log.info("serving_index_prune_generation generation=%s", child.name)
        except OSError:
            log.exception("serving_index_prune_generation_failed generation=%s", child.name)
    return removed


def merge_jsonl_by_key(src: Path, dest: Path, *, key: str, replacements: list[dict[str, Any]]) -> int:
    """Rewrite dest from src, replacing objects that share ``key`` with ``replacements``."""

    incoming = {str(row.get(key) or ""): row for row in replacements if str(row.get(key) or "")}
    seen: set[str] = set()
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8") as out:
        if src.exists():
            for raw in src.read_text(encoding="utf-8").splitlines():
                if not raw.strip():
                    continue
                try:
                    row = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                kid = str(row.get(key) or "")
                if kid and kid in incoming:
                    out.write(json.dumps(incoming[kid], ensure_ascii=False) + "\n")
                    seen.add(kid)
                else:
                    out.write(raw + "\n")
        for kid, row in incoming.items():
            if kid not in seen:
                out.write(json.dumps(row, ensure_ascii=False) + "\n")
    return len(incoming)


def mark_serving_index_dirty(vault: Path | str, reason: str, uids: list[str] | None = None) -> None:
    root = get_serving_index_path(Path(vault))
    uid_list = list(uids or [])
    try:
        _crate().serving_index_mark_dirty(str(root), reason, uid_list)
    except ServingIndexUnavailableError:
        root.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"ts": str(int(time.time())), "reason": reason, "uids": uid_list})
        with (root / "DIRTY").open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    _import_legacy_dirty(Path(vault), root, uid_list, reason)


def _import_legacy_dirty(vault: Path, index_root: Path, uids: list[str], reason: str) -> None:
    """Retain DIRTY writes and convert them into journal records when possible."""

    try:
        from archive_vault.change_journal import ChangeJournal

        if not Path(vault).is_dir():
            return
        with ChangeJournal(vault) as journal:
            journal.import_legacy_dirty(uids, reason=reason or "legacy_dirty")
            dirty = index_root / "DIRTY"
            if dirty.is_file() and not uids:
                journal.import_dirty_file(dirty)
    except Exception:
        logger.debug("journal legacy DIRTY import failed reason=%s", reason, exc_info=True)


def close_serving_handles(*, vault: Path | None = None) -> None:
    """Close pinned native handles. ``vault=None`` closes every instance."""

    global _HANDLE
    with _LOCK:
        if vault is None:
            for handle in list(_HANDLES.values()):
                handle.close()
            _HANDLES.clear()
            _HANDLE = None
            return
        key = _vault_handle_key(vault)
        handle = _HANDLES.pop(key, None)
        if handle is not None:
            handle.close()
        if _HANDLE is handle:
            _HANDLE = None


def get_serving_handle(vault: Path) -> ServingIndexHandle:
    global _HANDLE
    root = get_serving_index_path(vault)
    crate = _crate()
    status = dict(crate.serving_index_status(str(root)) or {})
    gid = str(status.get("serving_index_generation") or "")
    if not gid or not status.get("serving_index_ready"):
        raise ServingIndexUnavailableError("serving_index_unavailable")
    key = _vault_handle_key(vault)
    with _LOCK:
        if _HANDLE is None:
            stale = _HANDLES.pop(key, None)
            if stale is not None:
                stale.close()
            existing = None
        else:
            existing = _HANDLES.get(key)
        if (
            existing is not None
            and existing.index_root.resolve() == root.resolve()
            and existing.generation_id == gid
        ):
            _HANDLE = existing
            return existing
        if existing is not None:
            existing.close()
            _HANDLES.pop(key, None)
        native = crate.serving_index_open(str(root))
        handle = ServingIndexHandle(Path(vault), root, gid, native)
        _HANDLES[key] = handle
        _HANDLE = handle
        return handle


def _snapshot_binding(vault: Path, gid: str) -> tuple[str, int]:
    try:
        from archive_vault.change_journal import ChangeJournal

        with ChangeJournal(vault) as journal:
            watermark = int(journal.consumer_cursor("warehouse").high_watermark or 0)
            archive_id = journal.archive_id
        return f"{archive_id}:warehouse:{watermark}:{gid}", watermark
    except Exception:
        return f"export:{gid}", 0


def _decode_embedding(raw: Any, dim: int) -> list[float] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        nums = [float(x) for x in raw.strip("[]").split(",") if x.strip()]
    else:
        nums = [float(x) for x in raw]
    if len(nums) != dim:
        return None
    return nums


def _export_embeddings(
    conn: Any,
    schema: str,
    *,
    dim: int,
    uids: list[str] | None,
) -> list[tuple[str, tuple[float, ...]]]:
    model = get_default_embedding_model()
    version = get_default_embedding_version()
    out: list[tuple[str, tuple[float, ...]]] = []
    if uids is None:
        sql = f"""
            SELECT chunk_key, embedding
            FROM {schema}.embeddings
            WHERE embedding_model = %s AND embedding_version = %s
            """
        params: tuple[Any, ...] = (model, version)
    else:
        sql = f"""
            SELECT e.chunk_key, e.embedding
            FROM {schema}.embeddings e
            JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
            WHERE e.embedding_model = %s AND e.embedding_version = %s
              AND c.card_uid = ANY(%s)
            """
        params = (model, version, uids)
    try:
        rows = conn.execute(sql, params)
    except Exception:
        logger.exception("serving_index embed export failed")
        return out
    for row in rows:
        nums = _decode_embedding(row["embedding"] if not isinstance(row, tuple) else row[1], dim)
        if nums is None:
            continue
        key = str(row["chunk_key"] if not isinstance(row, tuple) else row[0])
        out.append((key, tuple(nums)))
    return out


def _export_warehouse_snapshot(
    store: Any,
    *,
    incremental: bool,
    dirty_uids: list[str],
    skip_embeddings: bool,
    log: logging.Logger,
    gid: str,
) -> ServingSnapshot:
    schema = str(getattr(store.index, "schema", "ppa"))
    dim = get_vector_dimension()
    spec = serving_embedding_spec()
    index = store.index
    cards: list[dict[str, Any]] = []
    chunks: list[dict[str, Any]] = []
    embeddings: list[tuple[str, tuple[float, ...]]] = []
    edges: list[dict[str, Any]] = []
    with index._connect() as conn:
        conn.execute("SET statement_timeout = 0")
        maps = load_serving_export_maps(conn, schema, dirty_uids if incremental else None)
        card_sql = _card_export_sql(schema, incremental=incremental)
        card_rows = conn.execute(card_sql, (dirty_uids,) if incremental else None) if incremental else conn.execute(card_sql)
        for row in card_rows:
            cards.append(build_serving_card(row, maps))
        if incremental:
            chunk_rows = conn.execute(
                f"SELECT chunk_key, card_uid, chunk_type, chunk_index FROM {schema}.chunks WHERE card_uid = ANY(%s)",
                (dirty_uids,),
            )
        else:
            chunk_rows = conn.execute(f"SELECT chunk_key, card_uid, chunk_type, chunk_index FROM {schema}.chunks")
        for row in chunk_rows:
            chunks.append(build_serving_chunk(row))
        if not skip_embeddings:
            embeddings = _export_embeddings(conn, schema, dim=dim, uids=dirty_uids if incremental else None)
        edges = load_serving_edges(conn, schema, dirty_uids if incremental else None)
    present = {str(row["card_uid"]) for row in cards}
    deleted = tuple(uid for uid in dirty_uids if uid not in present) if incremental else ()
    snapshot_id, watermark = _snapshot_binding(Path(store.vault), gid)
    log.info(
        "serving_index_export done mode=%s cards=%s chunks=%s embeddings=%s edges=%s deleted=%s snapshot=%s",
        "incremental" if incremental else "full",
        len(cards),
        len(chunks),
        len(embeddings),
        len(edges),
        len(deleted),
        snapshot_id,
    )
    return ServingSnapshot(
        snapshot_id=snapshot_id,
        source_watermark=watermark,
        cards=tuple(cards),
        chunks=tuple(chunks),
        edges=tuple(edges),
        embeddings=tuple(embeddings),
        embedding_spec=spec,
        deleted_uids=deleted,
        dirty_uids=tuple(dirty_uids) if incremental else (),
    )


def publish_serving_index(
    store: Any,
    *,
    logger: logging.Logger | None = None,
    dest_generation: str | None = None,
    skip_embeddings: bool = False,
    dirty_uids: list[str] | None = None,
) -> dict[str, Any]:
    """Build a new generation from the Postgres warehouse and atomically publish it.

    Incremental writes only dirty UIDs plus tombstones. Parent artifacts stay
    immutable. Compaction is an explicit full rebuild, never a silent copy.
    """
    log = logger or logging.getLogger("ppa.serving_index")
    vault = Path(store.vault)
    root = get_serving_index_path(vault)
    status = serving_index_status(vault)
    active_gid = str(status.get("serving_index_generation") or "")
    if dirty_uids is None:
        concrete: list[str] = []
        incremental = False
    else:
        concrete = [str(uid).strip() for uid in dirty_uids if str(uid).strip()]
        if status.get("serving_index_ready") and active_gid and not concrete:
            log.info("serving_index_publish skip incremental_without_uids keep_generation=%s", active_gid)
            return {"ok": True, "skipped": "dirty_without_uids", "generation": active_gid, **status}
        incremental = bool(concrete and status.get("serving_index_ready") and active_gid)
    gid = dest_generation or str(int(time.time() * 1000))
    mode = "delta" if incremental else "full"
    force_compact = False
    if incremental and active_gid:
        try:
            depth = len(walk_generation_chain(root, active_gid)) + 1
        except Exception:
            depth = 2
        if should_compact(chain_depth=depth, delta_vectors=0, live_vectors=0):
            log.info("serving_index_publish mode=compact reason=max_chain_depth parent=%s", active_gid)
            incremental = False
            mode = "compact"
            force_compact = True
    if incremental:
        log.info("serving_index_publish incremental uids=%s generation=%s", len(concrete), gid)
    spec = serving_embedding_spec()
    dim = spec.dimension
    snapshot = _export_warehouse_snapshot(
        store,
        incremental=incremental,
        dirty_uids=concrete,
        skip_embeddings=skip_embeddings,
        log=log,
        gid=gid,
    )
    rss_cap = get_serving_index_max_rss_mb()
    est_mb = (len(snapshot.embeddings) * dim * 4) / (1024 * 1024)
    if est_mb > rss_cap:
        if incremental:
            log.warning(
                "serving_index_publish incremental rss_over_cap estimated_mb=%.1f cap=%s segment_embeddings=%s",
                est_mb,
                rss_cap,
                len(snapshot.embeddings),
            )
        else:
            log.error("serving_index_refresh_failed reason=rss_cap estimated_mb=%.1f cap=%s", est_mb, rss_cap)
            return {"ok": False, "error": "serving_index_refresh_failed", "estimated_mb": est_mb}
    crate = _crate()
    captured = None
    try:
        from archive_engine.changes import CONSUMER_PUBLICATION, consume_batch
        from archive_vault.change_journal import ChangeJournal

        if Path(store.vault).is_dir():
            with ChangeJournal(store.vault) as journal:
                captured = consume_batch(journal, CONSUMER_PUBLICATION)
    except Exception:
        logger.debug("publication captured batch unavailable", exc_info=True)
        captured = None
    receipt = publish_snapshot(
        root,
        snapshot,
        generation_id=gid,
        parent_generation=active_gid if incremental else "",
        mode=mode,
        force_compact=force_compact,
        crate=crate,
        vault=vault,
        captured_batch=captured,
    )
    crate.serving_index_truncate_dirty(str(root))
    pruned = prune_retired_serving_generations(vault, keep=receipt.generation_id, logger=log)
    cache = QueryEmbedCache(get_query_embed_cache_path(vault), ram_entries=get_query_embed_cache_ram_entries())
    cache.evict(max_rows=get_query_embed_cache_max_rows(), max_age_days=get_query_embed_cache_max_age_days())
    cache.close()
    close_serving_handles(vault=vault)
    return {
        "ok": True,
        "generation": receipt.generation_id,
        "report": receipt.to_payload(),
        "cards": receipt.cards,
        "chunks": receipt.chunks,
        "embeddings": receipt.embeddings,
        "pruned_generations": pruned,
        "mode": receipt.mode,
        "parent_generation": receipt.parent_generation,
        "snapshot_id": receipt.snapshot_id,
    }


def verify_serving_index(vault: Path) -> dict[str, Any]:
    status = serving_index_status(vault)
    if not status.get("serving_index_ready"):
        raise ServingIndexUnavailableError("serving_index_unavailable")
    handle = get_serving_handle(vault)
    rows = handle.search("test", limit=1)
    return {"ok": True, "status": status, "sample_search_rows": len(rows), "generation": handle.generation_id}

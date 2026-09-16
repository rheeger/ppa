"""Process-level handle for the Rust serving index.

The store is cheap and constructed per MCP call. This module caches the native
mmap handle keyed by (vault, index_root). When ACTIVE flips, queries keep the
current handle and a background thread opens the new generation, then swaps.
"""

from __future__ import annotations

import array
import fcntl
import json
import logging
import math
import os
import resource
import shutil
import sys
import threading
import time
from dataclasses import dataclass
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
    choose_publication_mode,
    coverage_regression_reason,
    pin_generation,
    plan_publication_resources,
    publish_snapshot,
    read_active_generation,
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
    get_rebuild_progress_every,
    get_serving_candidate_budget,
    get_serving_export_batch_size,
    get_serving_follow_http_owner,
    get_serving_index_max_rss_mb,
    get_serving_prepare_on_start,
    get_serving_index_path,
    get_serving_prefault_enabled,
    get_serving_warm_poll_seconds,
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
REQUIRED_SERVING_INDEX_FORMAT = 2
REQUIRED_VECTOR_IMPL = "ivf_centroids_v2"


class ServingFidelityError(ValueError):
    """Trust-critical serving export would invent active/trusted policy."""


def serving_embedding_spec() -> EmbeddingSpec:
    """Build the live serving EmbeddingSpec from config. Does not invent model identity."""

    provider = os.environ.get("PPA_EMBEDDING_PROVIDER", "").strip()
    if not provider:
        from archive_cli.index_config import _active_serving_embedding_spec

        provider = str((_active_serving_embedding_spec() or {}).get("provider_namespace") or "").strip()
    if not provider:
        provider = "unspecified"
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
    phones: dict[str, list[str]] = {}
    external_ids: dict[str, list[str]] = {}
    corpus_states: dict[str, str] = {}

    people_clause, people_params = _uid_clause("card_uid", uids)
    map_params = people_params or None
    for row in _optional_rows(
        conn, f"SELECT card_uid, person FROM {schema}.card_people WHERE TRUE{people_clause}", map_params
    ):
        people.setdefault(str(row["card_uid"]), []).append(str(row["person"]))
    for row in _optional_rows(
        conn, f"SELECT card_uid, source FROM {schema}.card_sources WHERE TRUE{people_clause}", map_params
    ):
        sources.setdefault(str(row["card_uid"]), []).append(str(row["source"]))
    for row in _optional_rows(
        conn, f"SELECT card_uid, org FROM {schema}.card_orgs WHERE TRUE{people_clause}", map_params
    ):
        orgs.setdefault(str(row["card_uid"]), []).append(str(row["org"]))
    for row in _optional_rows(
        conn,
        f"SELECT card_uid, aliases_json, emails_json, phones_json FROM {schema}.people WHERE TRUE{people_clause}",
        map_params,
    ):
        uid = str(row["card_uid"])
        aliases[uid] = _as_str_list(row.get("aliases_json"))
        emails[uid] = _as_str_list(row.get("emails_json"))
        phones[uid] = _as_str_list(row.get("phones_json"))
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
        "phones": phones,
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
        "phones": list(maps.get("phones", {}).get(uid, [])),
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
_WARMING: dict[str, str] = {}
_WARM_THREADS: dict[str, threading.Thread] = {}
_OPEN_EVENTS: dict[str, threading.Event] = {}
_OPEN_ERRORS: dict[str, BaseException] = {}
_PREPARE_THREADS: dict[str, threading.Thread] = {}
_LOCK_FDS: dict[str, int] = {}
_WATCH_STOP = threading.Event()
_WATCH_THREAD: threading.Thread | None = None
_WATCH_VAULT: Path | None = None


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
    def __init__(
        self,
        vault: Path,
        index_root: Path,
        generation_id: str,
        native: Any,
        *,
        lock_fd: int | None = None,
    ):
        self.vault = Path(vault)
        self.index_root = Path(index_root)
        self.generation_id = generation_id
        self._native = native
        self._lock_fd = lock_fd
        self._closed = False
        self.vectors_resident = False
        pin_generation(self.index_root, self.generation_id)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        unpin_generation(self.index_root, self.generation_id)
        self._lock_fd = None

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


def serving_index_format_version(status: dict[str, Any] | None) -> int:
    """Read ACTIVE format from status/manifest. 0 means unknown or missing."""

    payload = status or {}
    raw = payload.get("serving_index_format")
    if raw is None:
        manifest = payload.get("manifest") or {}
        if isinstance(manifest, dict):
            raw = manifest.get("serving_index_format_version")
    try:
        return int(raw or 0)
    except (TypeError, ValueError):
        return 0


def serving_index_format_supported(status: dict[str, Any] | None) -> bool:
    return serving_index_format_version(status) == REQUIRED_SERVING_INDEX_FORMAT


def serving_index_status(vault: Path | None = None) -> dict[str, Any]:
    root = get_serving_index_path(vault)
    try:
        payload = dict(_crate().serving_index_status(str(root)) or {})
    except ServingIndexUnavailableError:
        payload = {
            "serving_index_generation": "",
            "serving_index_format": 0,
            "serving_index_dirty_records": 0,
            "serving_index_ready": False,
        }
    except Exception:
        payload = {
            "serving_index_generation": "",
            "serving_index_format": 0,
            "serving_index_dirty_records": 0,
            "serving_index_ready": False,
        }
    key = _vault_handle_key(vault) if vault is not None else ""
    with _LOCK:
        handle = _HANDLES.get(key) if key else _HANDLE
        warming = _WARMING.get(key, "") if key else ""
        if not warming and handle is not None:
            warming = next((gid for vault_key, gid in _WARMING.items() if vault_key == key), "")
    payload["serving_index_open_generation"] = str(getattr(handle, "generation_id", "") or "")
    payload["serving_index_warming_generation"] = warming
    payload["serving_index_vectors_resident"] = bool(getattr(handle, "vectors_resident", False))
    payload["serving_index_http_owner"] = _http_owner_label()
    return payload


def _http_owner_label() -> str:
    if not get_serving_follow_http_owner():
        return ""
    try:
        from archive_cli.http_serve import http_serving_owner_reachable

        return http_serving_owner_reachable()
    except Exception:
        return ""


def _open_lock_path(root: Path) -> Path:
    return Path(root) / "OPEN.lock"


def _lock_key(root: Path) -> str:
    return str(Path(root).resolve())


def _acquire_open_lock(root: Path) -> int:
    """Exclusive flock so only one process mmaps the serving index.

    Same-process generation swaps reuse the fd. A second open() plus
    ``LOCK_NB`` on macOS/Linux fails even in the same process.
    """

    key = _lock_key(root)
    with _LOCK:
        existing = _LOCK_FDS.get(key)
        if existing is not None:
            return existing
        path = _open_lock_path(root)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            other = _read_lock_pid(path)
            os.close(fd)
            if other > 0:
                raise ServingIndexUnavailableError(f"serving_index_already_open pid={other}") from exc
            raise ServingIndexUnavailableError("serving_index_already_open") from exc
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"{os.getpid()}\n".encode("ascii"))
            os.fsync(fd)
        except OSError:
            logger.debug("serving_index_lock write failed path=%s", path, exc_info=True)
        _LOCK_FDS[key] = fd
        return fd


def _release_open_lock(root: Path | None = None) -> None:
    with _LOCK:
        keys = [_lock_key(root)] if root is not None else list(_LOCK_FDS)
        fds = [(key, _LOCK_FDS.pop(key, None)) for key in keys]
    for key, fd in fds:
        del key
        if fd is None:
            continue
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            logger.debug("serving_index_lock unlock failed", exc_info=True)
        try:
            os.close(fd)
        except OSError:
            logger.debug("serving_index_lock close failed", exc_info=True)


def _read_lock_pid(path: Path) -> int:
    try:
        raw = path.read_text(encoding="utf-8").strip().splitlines()
    except OSError:
        return 0
    if not raw:
        return 0
    try:
        return int(raw[0])
    except ValueError:
        return 0


def _refuse_stdio_open() -> None:
    """Stdio MCP stays thin when the dedicated HTTP process already owns retrieval."""

    if not get_serving_follow_http_owner():
        return
    if os.environ.get("PPA_MCP_HTTP", "").strip().lower() in {"1", "true", "yes"}:
        return
    owner = _http_owner_label()
    if owner:
        raise ServingIndexUnavailableError(f"serving_index_owned_by_http {owner}")


def ack_dirty_uids(vault: Path | str | None, uids: list[str] | None) -> int:
    """Drop published UIDs from DIRTY. Does not wipe the file or other records."""

    acked = {str(uid).strip() for uid in (uids or []) if str(uid).strip()}
    if not acked:
        return 0
    path = get_serving_index_path(Path(vault) if vault is not None else None) / "DIRTY"
    if not path.is_file():
        return 0
    kept: list[str] = []
    removed = 0
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            kept.append(raw)
            continue
        if not isinstance(rec, dict):
            kept.append(raw)
            continue
        old = [str(uid).strip() for uid in (rec.get("uids") or []) if str(uid).strip()]
        new = [uid for uid in old if uid not in acked]
        removed += len(old) - len(new)
        if new:
            rec = {**rec, "uids": new}
            kept.append(json.dumps(rec, ensure_ascii=False))
        elif not old:
            kept.append(raw)
    path.write_text(("\n".join(kept) + "\n") if kept else "", encoding="utf-8")
    return removed


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


def estimate_live_vectors(index_root: Path, generation_id: str) -> int:
    """Sum embedding counts along the live chain. Overcounts replacements on purpose."""

    gid = str(generation_id or "").strip()
    root = Path(index_root)
    if not gid:
        return 0
    try:
        chain = walk_generation_chain(root, gid)
    except Exception:
        dest = root / "generations" / gid
        return _generation_embedding_count(dest) if dest.is_dir() else 0
    return sum(_generation_embedding_count(dest) for dest in chain)


def chain_depth_after_publish(index_root: Path, generation_id: str) -> int:
    gid = str(generation_id or "").strip()
    if not gid:
        return 1
    try:
        return len(walk_generation_chain(Path(index_root), gid)) + 1
    except Exception:
        return 2


def preflight_serving_publish(
    vault: Path | None = None,
    *,
    index_root: Path | None = None,
    dirty_uids: list[str] | None = None,
    uid_floor: int = 0,
    keep_export_tmp: str = "",
    reclaim: bool = True,
) -> Any:
    """Decide clip vs reprint from the files on disk, then size RAM and new disk.

    Does not open the search mmap. ``uid_floor`` covers nightly work that is not
    dirty yet (source pull still ahead).
    """

    from archive_cli.index_config import get_serving_index_path

    root = Path(index_root) if index_root is not None else get_serving_index_path(vault)
    status = serving_index_status(vault) if vault is not None else {}
    ready = bool(status.get("serving_index_ready"))
    active_gid = str(status.get("serving_index_generation") or read_active_generation(root) or "")
    if vault is not None and dirty_uids is None:
        concrete = read_dirty_uids(vault)
    else:
        concrete = [str(uid).strip() for uid in (dirty_uids or []) if str(uid).strip()]
    dirty_count = max(len(concrete), int(uid_floor or 0))
    depth = chain_depth_after_publish(root, active_gid) if active_gid else 1
    live_vectors = estimate_live_vectors(root, active_gid)
    mode = choose_publication_mode(
        ready=ready or bool(active_gid),
        parent_generation=active_gid,
        dirty_count=dirty_count,
        chain_depth=depth,
        delta_vectors=0,
        live_vectors=live_vectors,
    )
    return plan_publication_resources(
        root,
        mode=mode,
        live_vectors=live_vectors,
        dirty_count=dirty_count,
        chain_depth=depth,
        keep_export_tmp=keep_export_tmp,
        reclaim=reclaim,
    )


def _generation_embedding_count(generation_dir: Path) -> int:
    manifest = Path(generation_dir) / "manifest.json"
    if manifest.is_file():
        try:
            data = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = {}
        if isinstance(data, dict) and data.get("embedding_count") is not None:
            try:
                return int(data.get("embedding_count") or 0)
            except (TypeError, ValueError):
                pass
    keys = Path(generation_dir) / "embedding_keys.txt"
    if not keys.is_file():
        return 0
    count = 0
    with keys.open(encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                count += 1
    return count


def prune_retired_serving_generations(
    vault: Path | None = None,
    *,
    keep: str | None = None,
    index_root: Path | None = None,
    logger: logging.Logger | None = None,
) -> list[str]:
    """Delete generations that are not ACTIVE, not in the parent chain, and not pinned.

    Never delete a generation whose embedding_count is higher than ACTIVE. That
    is the paid ANN corpus; a thin publish must not prune it.
    """

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
    active_embeddings = _generation_embedding_count(gens / keep) if keep else 0
    removed: list[str] = []
    if not gens.is_dir():
        return removed
    for child in sorted(gens.iterdir()):
        if not child.is_dir():
            continue
        if child.name in retain:
            continue
        child_embeddings = _generation_embedding_count(child)
        if child_embeddings > active_embeddings:
            log.warning(
                "serving_index_prune_kept_higher_coverage generation=%s embeddings=%s active=%s active_embeddings=%s",
                child.name,
                child_embeddings,
                keep,
                active_embeddings,
            )
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
            roots = [handle.index_root for handle in _HANDLES.values()]
            for handle in list(_HANDLES.values()):
                handle.close()
            _HANDLES.clear()
            _HANDLE = None
            _WARMING.clear()
            for root in roots:
                _release_open_lock(root)
            if not roots:
                _release_open_lock()
            return
        key = _vault_handle_key(vault)
        handle = _HANDLES.pop(key, None)
        _WARMING.pop(key, None)
        root = handle.index_root if handle is not None else None
        if handle is not None:
            handle.close()
        if _HANDLE is handle:
            _HANDLE = None
        if root is not None:
            _release_open_lock(root)


def _active_generation(vault: Path) -> tuple[str, Path, dict[str, Any]]:
    root = get_serving_index_path(vault)
    crate = _crate()
    status = dict(crate.serving_index_status(str(root)) or {})
    gid = str(status.get("serving_index_generation") or "")
    if not gid or not status.get("serving_index_ready"):
        raise ServingIndexUnavailableError("serving_index_unavailable")
    found = serving_index_format_version(status)
    if found != REQUIRED_SERVING_INDEX_FORMAT:
        raise ServingIndexUnavailableError(
            f"serving_index_format_unsupported: found {found}, need {REQUIRED_SERVING_INDEX_FORMAT} ({REQUIRED_VECTOR_IMPL})"
        )
    return gid, root, status


def _format_bytes(n: int) -> str:
    value = float(max(n, 0))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(value)}{unit}"
            return f"{value:.1f}{unit}"
        value /= 1024.0
    return f"{n}B"


def _prefault_vectors(native: Any, generation_id: str, *, handle: ServingIndexHandle | None = None) -> int:
    """Fault cold embedding pages once per process per generation.

    Off unless ``PPA_SERVING_PREFAULT=1``. Cursor stdio must never pin the
    27GB embedding file just because a window spawned another MCP.
    """

    if not get_serving_prefault_enabled():
        logger.info("serving_index_prefault skip generation=%s reason=disabled", generation_id)
        return 0
    if handle is not None and handle.vectors_resident:
        logger.info("serving_index_prefault skip generation=%s reason=already_resident", generation_id)
        return 0
    crate = _crate()
    prefault = getattr(crate, "serving_index_prefault", None)
    if not callable(prefault):
        logger.warning("serving_index_prefault missing crate generation=%s", generation_id)
        return 0
    started = time.monotonic()
    last_log = [0]
    last_faulted = [0]
    logger.info("serving_index_prefault start generation=%s", generation_id)

    def _progress(done: int, total: int, faulted: int = 0) -> None:
        last_faulted[0] = int(faulted)
        step = 1 << 30
        if done < total and done - last_log[0] < step:
            return
        last_log[0] = int(done)
        elapsed = time.monotonic() - started
        pct = (100.0 * done / total) if total else 100.0
        logger.info(
            "serving_index_prefault generation=%s bytes=%s/%s (%.0f%%) faulted=%s elapsed_s=%.1f rss_mb=%.0f",
            generation_id,
            _format_bytes(int(done)),
            _format_bytes(int(total)),
            pct,
            _format_bytes(int(faulted)),
            elapsed,
            _rss_mb(),
        )

    touched = int(prefault(native, _progress) or 0)
    if handle is not None:
        handle.vectors_resident = True
    logger.info(
        "serving_index_prefault done generation=%s bytes=%s faulted=%s elapsed_s=%.1f rss_mb=%.0f",
        generation_id,
        _format_bytes(touched),
        _format_bytes(last_faulted[0]),
        time.monotonic() - started,
        _rss_mb(),
    )
    return touched


def _open_native_handle(vault: Path, root: Path, gid: str, *, prefault: bool = False) -> ServingIndexHandle:
    _refuse_stdio_open()
    lock_fd = _acquire_open_lock(root)
    started = time.monotonic()
    logger.info("serving_index_open start generation=%s", gid)
    try:
        native = _crate().serving_index_open(str(root))
    except Exception:
        with _LOCK:
            unused = all(Path(h.index_root).resolve() != Path(root).resolve() for h in _HANDLES.values())
        if unused:
            _release_open_lock(root)
        raise
    logger.info(
        "serving_index_open mmap done generation=%s elapsed_s=%.1f rss_mb=%.0f",
        gid,
        time.monotonic() - started,
        _rss_mb(),
    )
    handle = ServingIndexHandle(Path(vault), root, gid, native, lock_fd=lock_fd)
    if prefault:
        _prefault_vectors(native, gid, handle=handle)
    return handle


def _install_handle(key: str, handle: ServingIndexHandle, *, previous: ServingIndexHandle | None) -> None:
    global _HANDLE
    _HANDLES[key] = handle
    _HANDLE = handle
    if previous is not None and previous is not handle:
        previous.close()


def schedule_serving_handle_warm(vault: Path) -> None:
    """Open ACTIVE in a background thread so the first neighbor hop is not a cold mmap."""

    try:
        gid, root, _status = _active_generation(Path(vault))
    except ServingIndexUnavailableError:
        logger.info("serving_index_warm skip reason=unavailable")
        return
    _schedule_warm(Path(vault), gid, root)


def _schedule_warm(vault: Path, gid: str, root: Path) -> None:
    key = _vault_handle_key(vault)
    with _LOCK:
        if _WARMING.get(key) == gid:
            return
        current = _HANDLES.get(key)
        if current is not None and current.generation_id == gid:
            return
        _WARMING[key] = gid

    def _run() -> None:
        logger.info("serving_index_warm start generation=%s vault=%s", gid, vault)
        opened: ServingIndexHandle | None = None
        try:
            opened = _open_native_handle(vault, root, gid, prefault=get_serving_prefault_enabled())
            with _LOCK:
                try:
                    live_gid, _, _ = _active_generation(vault)
                except ServingIndexUnavailableError:
                    live_gid = ""
                previous = _HANDLES.get(key)
                if live_gid != gid:
                    logger.info(
                        "serving_index_warm discard generation=%s active=%s",
                        gid,
                        live_gid,
                    )
                    opened.close()
                    if _WARMING.get(key) == gid:
                        _WARMING.pop(key, None)
                    if live_gid:
                        _schedule_warm(vault, live_gid, get_serving_index_path(vault))
                    return
                _install_handle(key, opened, previous=previous)
                if _WARMING.get(key) == gid:
                    _WARMING.pop(key, None)
            logger.info("serving_index_warm done generation=%s", gid)
        except Exception:
            logger.exception("serving_index_warm failed generation=%s", gid)
            if opened is not None:
                try:
                    opened.close()
                except Exception:
                    logger.debug("serving_index_warm close failed generation=%s", gid, exc_info=True)
            with _LOCK:
                if _WARMING.get(key) == gid:
                    _WARMING.pop(key, None)

    thread = threading.Thread(target=_run, name=f"ppa-serving-warm-{gid}", daemon=True)
    _WARM_THREADS[key] = thread
    thread.start()


def wait_serving_handle(vault: Path, *, generation_id: str = "", timeout: float = 30.0) -> ServingIndexHandle:
    """Block until this process has opened ``generation_id`` or current ACTIVE."""

    deadline = time.monotonic() + max(timeout, 0.1)
    wanted = str(generation_id or "").strip()
    if not wanted:
        wanted, _, _ = _active_generation(vault)
    while time.monotonic() < deadline:
        handle = get_serving_handle(vault)
        if handle.generation_id == wanted:
            return handle
        time.sleep(0.05)
    raise ServingIndexUnavailableError(f"serving_index_warm_timeout generation={wanted}")


def start_mcp_serving_prepare(vault: Path, *, allow_stdio: bool = False) -> None:
    """Background-open the serving index for the HTTP owner only.

    Stdio MCP must stay thin. Cursor windows and agent workers each spawn
    their own ``archive_cli``. Prefaulting 27GB in every copy wedged the
    machine. HTTP serve is the one process allowed to mmap on startup.
    """

    target = Path(vault)
    http_owner = os.environ.get("PPA_MCP_HTTP", "").strip().lower() in {"1", "true", "yes"}
    if not http_owner and not allow_stdio:
        logger.info("mcp_serving_prepare skip vault=%s reason=stdio", target)
        return
    if http_owner and not get_serving_prepare_on_start():
        logger.info("mcp_serving_prepare skip vault=%s reason=prepare_on_start_off", target)
        return
    key = _vault_handle_key(target)
    thread: threading.Thread | None = None
    already_open = False
    with _LOCK:
        current = _HANDLES.get(key)
        if current is not None:
            already_open = True
        else:
            living = _PREPARE_THREADS.get(key)
            if living is not None and living.is_alive():
                return

            def _run() -> None:
                try:
                    prepare_mcp_serving(target)
                except Exception:
                    logger.exception("mcp_serving_prepare failed")
                finally:
                    with _LOCK:
                        if _PREPARE_THREADS.get(key) is threading.current_thread():
                            _PREPARE_THREADS.pop(key, None)

            thread = threading.Thread(target=_run, name="ppa-mcp-serving-prepare", daemon=True)
            _PREPARE_THREADS[key] = thread
    if already_open:
        start_serving_generation_watcher(target)
        logger.info("mcp_serving_prepare skip vault=%s reason=already_open", target)
        return
    if thread is not None:
        thread.start()
        logger.info("mcp_serving_prepare scheduled vault=%s", target)


def prepare_mcp_serving(vault: Path) -> ServingIndexHandle:
    """Open ACTIVE once. Prefault only when ``PPA_SERVING_PREFAULT=1``.

    Do not call this on the MCP handshake thread. Stdio serve should not
    call it at all when HTTP already owns the index.
    """

    target = Path(vault)
    started = time.monotonic()
    logger.info("mcp_serving_prepare start vault=%s", target)
    handle = get_serving_handle(target)
    if get_serving_prefault_enabled():
        _prefault_vectors(handle._native, handle.generation_id, handle=handle)
    start_serving_generation_watcher(target)
    logger.info(
        "mcp_serving_prepare ready generation=%s elapsed_s=%.1f rss_mb=%.0f",
        handle.generation_id,
        time.monotonic() - started,
        _rss_mb(),
    )
    return handle


def start_serving_generation_watcher(vault: Path) -> None:
    """Poll ACTIVE and open a new generation before the next MCP query."""

    global _WATCH_THREAD, _WATCH_VAULT
    stop_serving_generation_watcher()
    _WATCH_STOP.clear()
    _WATCH_VAULT = Path(vault)

    def _loop() -> None:
        while not _WATCH_STOP.wait(get_serving_warm_poll_seconds()):
            target = _WATCH_VAULT
            if target is None:
                continue
            try:
                gid, root, _ = _active_generation(target)
            except ServingIndexUnavailableError:
                continue
            except Exception:
                logger.debug("serving_index_watch status failed", exc_info=True)
                continue
            key = _vault_handle_key(target)
            with _LOCK:
                current = _HANDLES.get(key)
            if current is None:
                try:
                    get_serving_handle(target)
                except ServingIndexUnavailableError:
                    continue
                continue
            if current.generation_id != gid:
                _schedule_warm(target, gid, root)

    _WATCH_THREAD = threading.Thread(target=_loop, name="ppa-serving-watch", daemon=True)
    _WATCH_THREAD.start()
    logger.info("serving_index_watch start vault=%s poll_s=%s", vault, get_serving_warm_poll_seconds())


def stop_serving_generation_watcher() -> None:
    global _WATCH_THREAD, _WATCH_VAULT
    _WATCH_STOP.set()
    thread = _WATCH_THREAD
    _WATCH_THREAD = None
    _WATCH_VAULT = None
    if thread is not None and thread.is_alive() and thread is not threading.current_thread():
        thread.join(timeout=2.0)
    _WATCH_STOP.clear()


def get_serving_handle(vault: Path) -> ServingIndexHandle:
    global _HANDLE
    gid, root, _status = _active_generation(vault)
    key = _vault_handle_key(vault)
    while True:
        with _LOCK:
            existing = _HANDLES.get(key)
            if existing is not None and existing.index_root.resolve() == root.resolve() and existing.generation_id == gid:
                _HANDLE = existing
                return existing
            if existing is not None:
                _schedule_warm(vault, gid, root)
                return existing
            waiter = _OPEN_EVENTS.get(key)
            if waiter is None:
                waiter = threading.Event()
                _OPEN_EVENTS[key] = waiter
                owner = True
            else:
                owner = False
        if not owner:
            if not waiter.wait(timeout=600):
                raise ServingIndexUnavailableError("serving_index_open_timeout")
            with _LOCK:
                err = _OPEN_ERRORS.pop(key, None)
                opened = _HANDLES.get(key)
            if err is not None and opened is None:
                raise err
            continue
        try:
            native_handle = _open_native_handle(vault, root, gid)
            with _LOCK:
                current = _HANDLES.get(key)
                if current is not None and current.generation_id == gid:
                    native_handle.close()
                    _HANDLE = current
                    return current
                _install_handle(key, native_handle, previous=current)
                return native_handle
        except BaseException as exc:
            with _LOCK:
                _OPEN_ERRORS[key] = exc
            raise
        finally:
            with _LOCK:
                event = _OPEN_EVENTS.pop(key, None)
            if event is not None:
                event.set()


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
    if isinstance(raw, (bytes, bytearray, memoryview)) and len(raw) == dim * 4:
        return array.array("f", raw).tolist()
    if isinstance(raw, str):
        nums = [float(x) for x in raw.strip("[]").split(",") if x.strip()]
    else:
        nums = [float(x) for x in raw]
    if len(nums) != dim:
        return None
    return nums


def _pack_embedding(raw: Any, dim: int) -> bytes | None:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray, memoryview)) and len(raw) == dim * 4:
        return bytes(raw)
    nums = _decode_embedding(raw, dim)
    if nums is None:
        return None
    return array.array("f", nums).tobytes()


def _format_mins_secs(seconds: float) -> str:
    if not math.isfinite(seconds) or seconds < 0:
        return "?"
    total = int(round(seconds))
    m, s = divmod(total, 60)
    return f"{m}:{s:02d}"


def _rss_mb() -> float:
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return 0.0
    if sys.platform == "darwin":
        return usage / (1024 * 1024)
    return usage / 1024.0


def _first_row(result: Any) -> Any:
    fetchone = getattr(result, "fetchone", None)
    if fetchone is not None:
        return fetchone()
    try:
        return result[0]
    except (TypeError, IndexError, KeyError):
        return None


def _row_int(row: Any) -> int:
    if row is None:
        return 0
    if isinstance(row, dict):
        return int(next(iter(row.values())))
    if isinstance(row, (list, tuple)):
        return int(row[0])
    return int(row)


def _query_count(conn: Any, sql: str, params: tuple[Any, ...] | None = None) -> int:
    result = conn.execute(sql) if params is None else conn.execute(sql, params)
    return _row_int(_first_row(result))


def _iter_server_rows(
    conn: Any,
    sql: str,
    params: tuple[Any, ...] | None,
    *,
    name: str,
    batch: int,
) -> Any:
    """Prefer a named server-side cursor so the client never fetchall's millions of rows."""

    cursor_factory = getattr(conn, "cursor", None)
    if cursor_factory is None:
        yield from (conn.execute(sql) if params is None else conn.execute(sql, params))
        return
    try:
        cur = cursor_factory(name=name)
    except TypeError:
        yield from (conn.execute(sql) if params is None else conn.execute(sql, params))
        return
    if hasattr(cur, "__enter__"):
        with cur:
            if hasattr(cur, "itersize"):
                cur.itersize = batch
            if params is None:
                cur.execute(sql)
            else:
                cur.execute(sql, params)
            yield from cur
        return
    try:
        if hasattr(cur, "itersize"):
            cur.itersize = batch
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(sql, params)
        yield from cur
    finally:
        closer = getattr(cur, "close", None)
        if closer is not None:
            closer()


def _log_export_progress(
    log: logging.Logger,
    label: str,
    done: int,
    total: int,
    started: float,
    *,
    every: int,
    force: bool = False,
) -> None:
    if not force and every <= 0:
        return
    if not force and done != total and (every <= 0 or done % every != 0):
        return
    elapsed = time.monotonic() - started
    rate = done / elapsed if elapsed > 0 else 0.0
    remain = (total - done) / rate if rate > 0 and total > done else (0.0 if done >= total > 0 else float("nan"))
    pct = (100.0 * done / total) if total else 0.0
    log.info(
        "serving_index_export %s %s/%s (%.1f%%) elapsed=%s eta_remaining=%s rate_rows_per_s=%.1f rss_mb=%.0f",
        label,
        done,
        total if total else "?",
        pct,
        _format_mins_secs(elapsed),
        _format_mins_secs(remain),
        rate,
        _rss_mb(),
    )


@dataclass
class _EmbeddingExport:
    items: list[tuple[str, tuple[float, ...]]]
    count: int
    keys_path: str = ""
    bin_path: str = ""


def _export_embeddings(
    conn: Any,
    schema: str,
    *,
    dim: int,
    uids: list[str] | None,
    dest_dir: Path | None = None,
    log: logging.Logger | None = None,
    progress_every: int | None = None,
) -> _EmbeddingExport:
    """Stream warehouse vectors in bounded batches. Never hold 4M×dim Python floats."""

    log = log or logger
    model = get_default_embedding_model()
    version = get_default_embedding_version()
    batch = get_serving_export_batch_size()
    every = get_rebuild_progress_every() if progress_every is None else progress_every
    out: list[tuple[str, tuple[float, ...]]] = []
    if uids is None:
        count_sql = f"""
            SELECT COUNT(*) AS n
            FROM {schema}.embeddings e
            JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
            WHERE e.embedding_model = %s AND e.embedding_version = %s
            """
        sql = f"""
            SELECT e.chunk_key, e.embedding
            FROM {schema}.embeddings e
            JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
            WHERE e.embedding_model = %s AND e.embedding_version = %s
            """
        params: tuple[Any, ...] = (model, version)
    else:
        count_sql = f"""
            SELECT COUNT(*) AS n
            FROM {schema}.embeddings e
            JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
            WHERE e.embedding_model = %s AND e.embedding_version = %s
              AND c.card_uid = ANY(%s)
            """
        sql = f"""
            SELECT e.chunk_key, e.embedding
            FROM {schema}.embeddings e
            JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
            WHERE e.embedding_model = %s AND e.embedding_version = %s
              AND c.card_uid = ANY(%s)
            """
        params = (model, version, uids)
    started = time.monotonic()
    log.info("serving_index_export embeddings start batch=%s dest=%s", batch, dest_dir or "memory")
    try:
        total = _query_count(conn, count_sql, params)
    except Exception:
        logger.exception("serving_index embed count failed")
        total = 0
    log.info("serving_index_export embeddings counted total=%s rss_mb=%.0f", total, _rss_mb())
    keys_path = ""
    bin_path = ""
    count = 0
    vf = None
    kf = None
    try:
        if dest_dir is not None:
            dest_dir.mkdir(parents=True, exist_ok=True)
            keys_dest = dest_dir / "embedding_keys.txt"
            bin_dest = dest_dir / "embeddings.bin"
            vf = bin_dest.open("wb")
            kf = keys_dest.open("w", encoding="utf-8")
            keys_path = str(keys_dest)
            bin_path = str(bin_dest)
        rows = _iter_server_rows(conn, sql, params, name="ppa_serving_emb_export", batch=batch)
        for row in rows:
            packed = _pack_embedding(row["embedding"] if not isinstance(row, tuple) else row[1], dim)
            if packed is None:
                continue
            key = str(row["chunk_key"] if not isinstance(row, tuple) else row[0]).strip()
            if not key:
                continue
            if vf is not None and kf is not None:
                vf.write(packed)
                kf.write(key + "\n")
            else:
                nums = array.array("f")
                nums.frombytes(packed)
                out.append((key, tuple(nums)))
            count += 1
            _log_export_progress(log, "embeddings", count, total, started, every=every)
    except Exception as exc:
        logger.exception("serving_index embed export failed")
        if vf is not None:
            vf.close()
            vf = None
        if kf is not None:
            kf.close()
            kf = None
        if keys_path:
            Path(keys_path).unlink(missing_ok=True)
        if bin_path:
            Path(bin_path).unlink(missing_ok=True)
        from archive_engine.adapters.serving_export import ExportFailed

        raise ExportFailed(f"export_interrupted:{exc}") from exc
    finally:
        if vf is not None:
            vf.close()
        if kf is not None:
            kf.close()
    _log_export_progress(log, "embeddings", count, total or count, started, every=every, force=True)
    log.info(
        "serving_index_export embeddings done count=%s elapsed=%s rss_mb=%.0f",
        count,
        _format_mins_secs(time.monotonic() - started),
        _rss_mb(),
    )
    return _EmbeddingExport(items=out, count=count, keys_path=keys_path, bin_path=bin_path)


EXPORT_TMP_DIR_NAME = ".export-tmp"


def discard_export_tmp(root: Path, *, keep: str = "", log: logging.Logger | None = None) -> list[str]:
    """Delete export scratch under ``root/.export-tmp`` except ``keep``.

    A full export writes ``embeddings.bin`` here (about 27 GB for the seed) and
    ``publish_snapshot`` copies it into the generation, so nothing points back at
    the scratch after publish. Before this sweep, every full export left its copy
    behind and the disk filled until ``publication_disk_budget`` refused to publish.
    """

    tmp = Path(root) / EXPORT_TMP_DIR_NAME
    if not tmp.is_dir():
        return []
    removed: list[str] = []
    for child in sorted(tmp.iterdir()):
        if keep and child.name == keep:
            continue
        try:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            removed.append(child.name)
        except OSError:
            (log or logger).warning("serving_index_export_tmp_discard_failed path=%s", child, exc_info=True)
    if removed and log is not None:
        log.info("serving_index_export_tmp discarded=%s keep=%s", len(removed), keep or "-")
    return removed


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
    emb_export = _EmbeddingExport(items=[], count=0)
    every = get_rebuild_progress_every()
    batch = get_serving_export_batch_size()
    mode_name = "incremental" if incremental else "full"
    export_dir = get_serving_index_path(Path(store.vault)) / ".export-tmp" / gid
    log.info(
        "serving_index_export start mode=%s generation=%s dim=%s batch=%s progress_every=%s",
        mode_name,
        gid,
        dim,
        batch,
        every,
    )
    with index._connect() as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        conn.execute("SET statement_timeout = 0")
        t_maps = time.monotonic()
        log.info("serving_index_export maps start rss_mb=%.0f", _rss_mb())
        maps = load_serving_export_maps(conn, schema, dirty_uids if incremental else None)
        log.info(
            "serving_index_export maps done people=%s sources=%s elapsed=%s rss_mb=%.0f",
            len(maps.get("people") or {}),
            len(maps.get("sources") or {}),
            _format_mins_secs(time.monotonic() - t_maps),
            _rss_mb(),
        )
        card_sql = _card_export_sql(schema, incremental=incremental)
        if incremental:
            card_total = _query_count(
                conn,
                f"SELECT COUNT(*) AS n FROM {schema}.cards c WHERE c.uid = ANY(%s)",
                (dirty_uids,),
            )
            card_params: tuple[Any, ...] | None = (dirty_uids,)
        else:
            card_total = _query_count(conn, f"SELECT COUNT(*) AS n FROM {schema}.cards")
            card_params = None
        t_cards = time.monotonic()
        log.info("serving_index_export cards start total=%s rss_mb=%.0f", card_total, _rss_mb())
        for row in _iter_server_rows(conn, card_sql, card_params, name="ppa_serving_card_export", batch=batch):
            cards.append(build_serving_card(row, maps))
            _log_export_progress(log, "cards", len(cards), card_total, t_cards, every=every)
        _log_export_progress(log, "cards", len(cards), card_total or len(cards), t_cards, every=every, force=True)
        if incremental:
            chunk_sql = (
                f"SELECT chunk_key, card_uid, chunk_type, chunk_index FROM {schema}.chunks WHERE card_uid = ANY(%s)"
            )
            chunk_params: tuple[Any, ...] | None = (dirty_uids,)
            chunk_total = _query_count(
                conn,
                f"SELECT COUNT(*) AS n FROM {schema}.chunks WHERE card_uid = ANY(%s)",
                (dirty_uids,),
            )
        else:
            chunk_sql = f"SELECT chunk_key, card_uid, chunk_type, chunk_index FROM {schema}.chunks"
            chunk_params = None
            chunk_total = _query_count(conn, f"SELECT COUNT(*) AS n FROM {schema}.chunks")
        t_chunks = time.monotonic()
        log.info("serving_index_export chunks start total=%s rss_mb=%.0f", chunk_total, _rss_mb())
        for row in _iter_server_rows(conn, chunk_sql, chunk_params, name="ppa_serving_chunk_export", batch=batch):
            chunks.append(build_serving_chunk(row))
            _log_export_progress(log, "chunks", len(chunks), chunk_total, t_chunks, every=every)
        _log_export_progress(log, "chunks", len(chunks), chunk_total or len(chunks), t_chunks, every=every, force=True)
        if not skip_embeddings:
            emb_export = _export_embeddings(
                conn,
                schema,
                dim=dim,
                uids=dirty_uids if incremental else None,
                dest_dir=export_dir,
                log=log,
                progress_every=every,
            )
            embeddings = emb_export.items
        t_edges = time.monotonic()
        log.info("serving_index_export edges start rss_mb=%.0f", _rss_mb())
        edges = load_serving_edges(conn, schema, dirty_uids if incremental else None)
        log.info(
            "serving_index_export edges done count=%s elapsed=%s rss_mb=%.0f",
            len(edges),
            _format_mins_secs(time.monotonic() - t_edges),
            _rss_mb(),
        )
    present = {str(row["card_uid"]) for row in cards}
    deleted = tuple(uid for uid in dirty_uids if uid not in present) if incremental else ()
    snapshot_id, watermark = _snapshot_binding(Path(store.vault), gid)
    log.info(
        "serving_index_export done mode=%s cards=%s chunks=%s embeddings=%s edges=%s deleted=%s snapshot=%s rss_mb=%.0f",
        mode_name,
        len(cards),
        len(chunks),
        emb_export.count or len(embeddings),
        len(edges),
        len(deleted),
        snapshot_id,
        _rss_mb(),
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
        embedding_keys_path=emb_export.keys_path,
        embeddings_bin_path=emb_export.bin_path,
        embedding_count=emb_export.count,
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
    from archive_engine.publication import PublisherLease

    lease = PublisherLease(root)
    lease.acquire()
    try:
        return _publish_serving_index_locked(
            store,
            logger=log,
            dest_generation=dest_generation,
            skip_embeddings=skip_embeddings,
            dirty_uids=dirty_uids,
            vault=vault,
            root=root,
        )
    finally:
        lease.release()


def _publish_serving_index_locked(
    store: Any,
    *,
    logger: logging.Logger,
    dest_generation: str | None,
    skip_embeddings: bool,
    dirty_uids: list[str] | None,
    vault: Path,
    root: Path,
) -> dict[str, Any]:
    log = logger
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
    log.info(
        "serving_index_publish start mode=%s generation=%s keep_active=%s incremental=%s dirty_uids=%s",
        mode,
        gid,
        active_gid,
        incremental,
        len(concrete),
    )
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
    if incremental and active_gid:
        reason = coverage_regression_reason(
            root,
            candidate_cards=len(concrete),
            candidate_embeddings=len(concrete),
            active_gid=active_gid,
        )
        if reason:
            log.warning("serving_index_publish skip %s keep_generation=%s", reason, active_gid)
            return {
                "ok": True,
                "skipped": "coverage_regression",
                "generation": active_gid,
                "error": reason,
                "cards": 0,
                "chunks": 0,
                "embeddings": 0,
            }
    if incremental:
        log.info("serving_index_publish incremental uids=%s generation=%s", len(concrete), gid)
    spec = serving_embedding_spec()
    dim = spec.dimension
    plan = plan_publication_resources(
        root,
        mode=mode,
        live_vectors=estimate_live_vectors(root, active_gid),
        dirty_count=len(concrete),
        chain_depth=chain_depth_after_publish(root, active_gid) if active_gid else 1,
        dimension=dim,
        keep_export_tmp=gid,
    )
    if not plan.ok:
        log.error(
            "serving_index_refresh_failed reason=publication_resources mode=%s reasons=%s",
            mode,
            ",".join(plan.reasons),
        )
        return {
            "ok": False,
            "error": "publication_resources",
            "reasons": list(plan.reasons),
            **plan.to_payload(),
        }
    discard_export_tmp(root, keep=gid, log=log)
    try:
        return _export_and_publish(
            store,
            root=root,
            vault=vault,
            gid=gid,
            dim=dim,
            mode=mode,
            incremental=incremental,
            concrete=concrete,
            active_gid=active_gid,
            force_compact=force_compact,
            skip_embeddings=skip_embeddings,
            log=log,
        )
    finally:
        discard_export_tmp(root, log=log)


def _export_and_publish(
    store: Any,
    *,
    root: Path,
    vault: Path,
    gid: str,
    dim: int,
    mode: str,
    incremental: bool,
    concrete: list[str],
    active_gid: str,
    force_compact: bool,
    skip_embeddings: bool,
    log: logging.Logger,
) -> dict[str, Any]:
    snapshot = _export_warehouse_snapshot(
        store,
        incremental=incremental,
        dirty_uids=concrete,
        skip_embeddings=skip_embeddings,
        log=log,
        gid=gid,
    )
    rss_cap = get_serving_index_max_rss_mb()
    embed_n = snapshot.embedding_count or len(snapshot.embeddings)
    reason = coverage_regression_reason(
        root,
        candidate_cards=len(snapshot.cards),
        candidate_embeddings=int(embed_n or 0),
        active_gid=active_gid,
    )
    if reason:
        if incremental:
            log.warning("serving_index_publish skip %s keep_generation=%s", reason, active_gid)
            return {
                "ok": True,
                "skipped": "coverage_regression",
                "generation": active_gid,
                "error": reason,
                "cards": len(snapshot.cards),
                "chunks": len(snapshot.chunks),
                "embeddings": int(embed_n or 0),
            }
        log.error("serving_index_publish refused %s generation=%s", reason, gid)
        return {"ok": False, "error": reason, "generation": active_gid}
    est_mb = (embed_n * dim * 4) / (1024 * 1024)
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
            if captured is not None:
                from archive_engine.journaled_state import (
                    PUBLICATION_CAPTURE_REL,
                    PUBLICATION_CAPTURE_UID,
                    persist_json_state,
                )

                persist_json_state(
                    Path(store.vault),
                    uid=PUBLICATION_CAPTURE_UID,
                    rel=PUBLICATION_CAPTURE_REL,
                    payload=captured.to_payload(),
                    source="publication",
                )
    except Exception:
        logger.debug("publication captured batch unavailable", exc_info=True)
        captured = None
    discard_export_tmp(root, keep=gid, log=log)
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
        acquire_lease=False,
    )
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

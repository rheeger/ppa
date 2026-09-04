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

from .errors import ServingIndexUnavailableError
from .index_config import (
    get_default_embedding_model,
    get_default_embedding_version,
    get_query_embed_cache_max_age_days,
    get_query_embed_cache_max_rows,
    get_query_embed_cache_path,
    get_query_embed_cache_ram_entries,
    get_serving_index_max_rss_mb,
    get_serving_index_path,
    get_vector_dimension,
)
from .query_embed_cache import QueryEmbedCache

logger = logging.getLogger("ppa.serving_index")

_LOCK = threading.RLock()
_HANDLE: ServingIndexHandle | None = None


def _crate():
    try:
        import archive_crate
    except ImportError as exc:
        raise ServingIndexUnavailableError("serving_index_unavailable") from exc
    return archive_crate


class ServingIndexHandle:
    def __init__(self, vault: Path, index_root: Path, generation_id: str, native: Any):
        self.vault = Path(vault)
        self.index_root = Path(index_root)
        self.generation_id = generation_id
        self._native = native

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
        return list(_crate().serving_index_search(self._native, req) or [])

    def query(self, **kwargs: Any) -> list[dict[str, Any]]:
        return list(_crate().serving_index_query(self._native, dict(kwargs)) or [])

    def vector(self, query_vector: list[float], **kwargs: Any) -> list[dict[str, Any]]:
        req = {
            "limit": int(kwargs.get("limit", 20) or 20),
            "type_filter": str(kwargs.get("type_filter", "") or ""),
            "source_filter": str(kwargs.get("source_filter", "") or ""),
            "people_filter": str(kwargs.get("people_filter", "") or ""),
            "start_date": str(kwargs.get("start_date", "") or ""),
            "end_date": str(kwargs.get("end_date", "") or ""),
        }
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
        return list(_crate().serving_index_hybrid(self._native, query, query_vector, req) or [])

    def graph(self, note_path: str, hops: int = 2) -> dict[str, Any]:
        return dict(_crate().serving_index_graph(self._native, note_path, int(hops) or 1) or {})

    def person(self, name: str) -> dict[str, Any]:
        return dict(_crate().serving_index_person(self._native, name) or {})

    def pointers(self, uids: list[str]) -> dict[str, dict[str, Any]]:
        return dict(_crate().serving_index_pointers(self._native, list(uids)) or {})

    def neighbor_uids(self, uids: list[str], hops: int = 1) -> list[str]:
        return list(_crate().serving_index_neighbor_uids(self._native, list(uids), int(hops) or 1) or [])

    def timeline(self, **kwargs: Any) -> list[dict[str, Any]]:
        return list(_crate().serving_index_timeline(self._native, dict(kwargs)) or [])

    def temporal_neighbors(self, timestamp: str, **kwargs: Any) -> dict[str, Any]:
        return dict(_crate().serving_index_temporal_neighbors(self._native, timestamp, dict(kwargs)) or {})

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
    try:
        _crate().serving_index_mark_dirty(str(root), reason, list(uids or []))
    except ServingIndexUnavailableError:
        root.mkdir(parents=True, exist_ok=True)
        line = json.dumps({"ts": str(int(time.time())), "reason": reason, "uids": list(uids or [])})
        with (root / "DIRTY").open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")


def get_serving_handle(vault: Path) -> ServingIndexHandle:
    global _HANDLE
    root = get_serving_index_path(vault)
    crate = _crate()
    status = dict(crate.serving_index_status(str(root)) or {})
    gid = str(status.get("serving_index_generation") or "")
    if not gid or not status.get("serving_index_ready"):
        raise ServingIndexUnavailableError("serving_index_unavailable")
    with _LOCK:
        if (
            _HANDLE is not None
            and _HANDLE.vault.resolve() == Path(vault).resolve()
            and _HANDLE.index_root.resolve() == root.resolve()
            and _HANDLE.generation_id == gid
        ):
            return _HANDLE
        native = crate.serving_index_open(str(root))
        _HANDLE = ServingIndexHandle(Path(vault), root, gid, native)
        return _HANDLE


def publish_serving_index(
    store: Any,
    *,
    logger: logging.Logger | None = None,
    dest_generation: str | None = None,
    skip_embeddings: bool = False,
    dirty_uids: list[str] | None = None,
) -> dict[str, Any]:
    """Build a new generation from the Postgres warehouse and atomically publish it.

    Called only from maintain / rebuild. Never from the MCP query path.
    Incremental when ACTIVE exists and ``dirty_uids`` is a non-empty concrete set:
    copy prior jsonl, patch those UIDs, hardlink embeddings.bin.
    ``dirty_uids=None`` is a full rebuild publish. Maintain passes
    ``read_dirty_uids()`` so an empty DIRTY skips the 25GB export.
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
    dest = root / "generations" / gid
    dest.mkdir(parents=True, exist_ok=True)
    prev = root / "generations" / active_gid if incremental else None
    if incremental:
        log.info("serving_index_export mode=incremental dirty_uids=%s from=%s", len(concrete), active_gid)
    cards_path = dest / "cards.jsonl"
    chunks_path = dest / "chunks.jsonl"
    edges_path = dest / "edges.jsonl"
    keys_path = dest / "embedding_keys.txt"
    vec_path = dest / "embeddings.bin"
    schema = str(getattr(store.index, "schema", "ppa"))
    dim = get_vector_dimension()
    index = store.index
    card_count = 0
    chunk_count = 0
    embed_count = 0

    def _eta(started: float, done: int, total: int) -> str:
        elapsed = max(time.monotonic() - started, 0.001)
        rate = done / elapsed
        remain = max(total - done, 0) / rate if rate else 0
        em, es = divmod(int(elapsed), 60)
        rm, rs = divmod(int(remain), 60)
        pct = (100.0 * done / total) if total else 0.0
        return (
            f"rows={done}/{total} ({pct:.0f}%) elapsed={em}:{es:02d} "
            f"eta_remaining={rm}:{rs:02d} rate_rows_per_s={rate:.1f}"
        )

    if incremental and prev is not None:
        for name in ("embeddings.bin", "embedding_keys.txt"):
            src = prev / name
            dst = dest / name
            if src.exists():
                if dst.exists():
                    dst.unlink()
                try:
                    os.link(src, dst)
                except OSError:
                    shutil.copy2(src, dst)
        patched_cards: list[dict[str, Any]] = []
        patched_chunks: list[dict[str, Any]] = []
        patched_edges: list[dict[str, Any]] = []
        with index._connect() as conn:
            conn.execute("SET statement_timeout = 0")
            people_map: dict[str, list[str]] = {}
            src_map: dict[str, list[str]] = {}
            org_map: dict[str, list[str]] = {}
            for row in conn.execute(
                f"SELECT card_uid, person FROM {schema}.card_people WHERE card_uid = ANY(%s)",
                (concrete,),
            ):
                people_map.setdefault(str(row["card_uid"]), []).append(str(row["person"]))
            for row in conn.execute(
                f"SELECT card_uid, source FROM {schema}.card_sources WHERE card_uid = ANY(%s)",
                (concrete,),
            ):
                src_map.setdefault(str(row["card_uid"]), []).append(str(row["source"]))
            try:
                for row in conn.execute(
                    f"SELECT card_uid, org FROM {schema}.card_orgs WHERE card_uid = ANY(%s)",
                    (concrete,),
                ):
                    org_map.setdefault(str(row["card_uid"]), []).append(str(row["org"]))
            except Exception:
                pass
            for row in conn.execute(
                f"""
                SELECT c.uid, c.rel_path, c.summary, c.type, c.slug, c.activity_at,
                       c.activity_end_at, COALESCE(c.search_text, '') AS search_text
                FROM {schema}.cards c
                WHERE c.uid = ANY(%s)
                """,
                (concrete,),
            ):
                uid = str(row["uid"])
                patched_cards.append(
                    {
                        "card_uid": uid,
                        "rel_path": str(row["rel_path"] or ""),
                        "summary": str(row["summary"] or ""),
                        "type": str(row["type"] or ""),
                        "slug": str(row["slug"] or ""),
                        "activity_at": str(row["activity_at"] or ""),
                        "activity_end_at": str(row["activity_end_at"] or ""),
                        "search_text": str(row["search_text"] or ""),
                        "people": people_map.get(uid, []),
                        "sources": src_map.get(uid, []),
                        "orgs": org_map.get(uid, []),
                        "corpus_state": "active",
                        "aliases": [],
                        "emails": [],
                    }
                )
            for row in conn.execute(
                f"SELECT chunk_key, card_uid, chunk_type, chunk_index FROM {schema}.chunks WHERE card_uid = ANY(%s)",
                (concrete,),
            ):
                patched_chunks.append(
                    {
                        "chunk_key": str(row["chunk_key"]),
                        "card_uid": str(row["card_uid"]),
                        "chunk_type": str(row["chunk_type"] or ""),
                        "chunk_index": int(row["chunk_index"] or 0),
                    }
                )
            try:
                for row in conn.execute(
                    f"""
                    SELECT source_uid, target_uid, edge_type, field_name
                    FROM {schema}.edges
                    WHERE target_kind = 'card' AND target_uid <> ''
                      AND (source_uid = ANY(%s) OR target_uid = ANY(%s))
                    """,
                    (concrete, concrete),
                ):
                    patched_edges.append(
                        {
                            "source_uid": str(row["source_uid"] or ""),
                            "target_uid": str(row["target_uid"] or ""),
                            "edge_type": str(row["edge_type"] or ""),
                            "field_name": str(row["field_name"] or ""),
                            "trust": 1.0,
                        }
                    )
            except Exception:
                log.exception("serving_index incremental edge export failed")
        card_count = merge_jsonl_by_key(prev / "cards.jsonl", cards_path, key="card_uid", replacements=patched_cards)
        chunk_count = merge_jsonl_by_key(
            prev / "chunks.jsonl", chunks_path, key="chunk_key", replacements=patched_chunks
        )
        edge_count = 0
        if (prev / "edges.jsonl").exists():
            with edges_path.open("w", encoding="utf-8") as fh:
                for raw in (prev / "edges.jsonl").read_text(encoding="utf-8").splitlines():
                    if not raw.strip():
                        continue
                    try:
                        row = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    pair = (
                        str(row.get("source_uid") or ""),
                        str(row.get("target_uid") or ""),
                        str(row.get("edge_type") or ""),
                    )
                    if pair[0] in concrete or pair[1] in concrete:
                        continue
                    fh.write(raw + "\n")
                    edge_count += 1
                for row in patched_edges:
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                    edge_count += 1
        else:
            with edges_path.open("w", encoding="utf-8") as fh:
                for row in patched_edges:
                    fh.write(json.dumps(row, ensure_ascii=False) + "\n")
                    edge_count += 1
        embed_count = sum(1 for line in keys_path.open(encoding="utf-8") if line.strip()) if keys_path.exists() else 0
        log.info(
            "serving_index_export done mode=incremental patched_cards=%s patched_chunks=%s edges=%s embeddings_hardlinked=%s",
            len(patched_cards),
            len(patched_chunks),
            edge_count,
            embed_count,
        )
    if not (incremental and prev is not None):
        with index._connect() as conn:
            conn.execute("SET statement_timeout = 0")
            log.info("serving_index_export start schema=%s dest=%s dim=%s", schema, dest, dim)
            people_map: dict[str, list[str]] = {}
            src_map: dict[str, list[str]] = {}
            org_map: dict[str, list[str]] = {}
            for row in conn.execute(f"SELECT card_uid, person FROM {schema}.card_people"):
                people_map.setdefault(str(row["card_uid"]), []).append(str(row["person"]))
            for row in conn.execute(f"SELECT card_uid, source FROM {schema}.card_sources"):
                src_map.setdefault(str(row["card_uid"]), []).append(str(row["source"]))
            try:
                for row in conn.execute(f"SELECT card_uid, org FROM {schema}.card_orgs"):
                    org_map.setdefault(str(row["card_uid"]), []).append(str(row["org"]))
            except Exception:
                pass
            card_total = int(conn.execute(f"SELECT COUNT(*) AS c FROM {schema}.cards").fetchone()["c"] or 0)
            t_cards = time.monotonic()
            with cards_path.open("w", encoding="utf-8") as fh:
                rows = conn.execute(
                    f"""
                    SELECT c.uid, c.rel_path, c.summary, c.type, c.slug, c.activity_at,
                           c.activity_end_at, COALESCE(c.search_text, '') AS search_text
                    FROM {schema}.cards c
                    """
                )
                for row in rows:
                    uid = str(row["uid"])
                    rec = {
                        "card_uid": uid,
                        "rel_path": str(row["rel_path"] or ""),
                        "summary": str(row["summary"] or ""),
                        "type": str(row["type"] or ""),
                        "slug": str(row["slug"] or ""),
                        "activity_at": str(row["activity_at"] or ""),
                        "activity_end_at": str(row["activity_end_at"] or ""),
                        "search_text": str(row["search_text"] or ""),
                        "people": people_map.get(uid, []),
                        "sources": src_map.get(uid, []),
                        "orgs": org_map.get(uid, []),
                        "corpus_state": "active",
                        "aliases": [],
                        "emails": [],
                    }
                    fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    card_count += 1
                    if card_count % 25000 == 0 or card_count == card_total:
                        log.info("serving_index_export stage=cards %s", _eta(t_cards, card_count, card_total))
            chunk_total = int(conn.execute(f"SELECT COUNT(*) AS c FROM {schema}.chunks").fetchone()["c"] or 0)
            t_chunks = time.monotonic()
            with chunks_path.open("w", encoding="utf-8") as fh:
                for row in conn.execute(f"SELECT chunk_key, card_uid, chunk_type, chunk_index FROM {schema}.chunks"):
                    fh.write(
                        json.dumps(
                            {
                                "chunk_key": str(row["chunk_key"]),
                                "card_uid": str(row["card_uid"]),
                                "chunk_type": str(row["chunk_type"] or ""),
                                "chunk_index": int(row["chunk_index"] or 0),
                            }
                        )
                        + "\n"
                    )
                    chunk_count += 1
                    if chunk_count % 50000 == 0 or chunk_count == chunk_total:
                        log.info("serving_index_export stage=chunks %s", _eta(t_chunks, chunk_count, chunk_total))
            model = get_default_embedding_model()
            version = get_default_embedding_version()
            if skip_embeddings and vec_path.exists() and keys_path.exists():
                embed_count = sum(1 for line in keys_path.open(encoding="utf-8") if line.strip())
                log.info("serving_index_export skip embeddings existing=%s", embed_count)
            else:
                embed_total = int(
                    conn.execute(
                        f"""
                        SELECT COUNT(*) AS c FROM {schema}.embeddings
                        WHERE embedding_model = %s AND embedding_version = %s
                        """,
                        (model, version),
                    ).fetchone()["c"]
                    or 0
                )
                t_emb = time.monotonic()
                import array

                with vec_path.open("wb") as vf, keys_path.open("w", encoding="utf-8") as kf:
                    try:
                        with conn.cursor(name="serving_emb_export") as cur:
                            cur.itersize = 2000
                            cur.execute(
                                f"""
                                SELECT chunk_key, embedding
                                FROM {schema}.embeddings
                                WHERE embedding_model = %s AND embedding_version = %s
                                """,
                                (model, version),
                            )
                            for row in cur:
                                key = str(row["chunk_key"])
                                emb = row["embedding"]
                                if emb is None:
                                    continue
                                if isinstance(emb, str):
                                    nums = [float(x) for x in emb.strip("[]").split(",") if x.strip()]
                                else:
                                    nums = list(emb)
                                if len(nums) != dim:
                                    continue
                                vf.write(array.array("f", nums).tobytes())
                                kf.write(key + "\n")
                                embed_count += 1
                                if embed_count % 25000 == 0 or embed_count == embed_total:
                                    log.info(
                                        "serving_index_export stage=embeddings %s",
                                        _eta(t_emb, embed_count, embed_total),
                                    )
                    except Exception:
                        log.exception("serving_index embed export failed")
            edge_count = 0
            t_edges = time.monotonic()
            with edges_path.open("w", encoding="utf-8") as fh:
                try:
                    for row in conn.execute(
                        f"""
                        SELECT source_uid, target_uid, edge_type, field_name
                        FROM {schema}.edges
                        WHERE target_kind = 'card' AND target_uid <> ''
                        """
                    ):
                        fh.write(
                            json.dumps(
                                {
                                    "source_uid": str(row["source_uid"] or ""),
                                    "target_uid": str(row["target_uid"] or ""),
                                    "edge_type": str(row["edge_type"] or ""),
                                    "field_name": str(row["field_name"] or ""),
                                    "trust": 1.0,
                                }
                            )
                            + "\n"
                        )
                        edge_count += 1
                        if edge_count % 100000 == 0:
                            log.info(
                                "serving_index_export stage=edges rows=%s elapsed=%.0fs",
                                edge_count,
                                time.monotonic() - t_edges,
                            )
                except Exception:
                    log.exception("serving_index edge export failed")
            log.info(
                "serving_index_export done cards=%s chunks=%s embeddings=%s edges=%s",
                card_count,
                chunk_count,
                embed_count,
                edge_count,
            )
    crate = _crate()
    log.info("serving_index_build start dest=%s", dest)
    report = crate.serving_index_build(
        str(dest),
        str(cards_path),
        str(chunks_path),
        str(keys_path),
        str(vec_path),
        dim,
        str(edges_path),
    )
    rss_cap = get_serving_index_max_rss_mb()
    est_mb = (embed_count * dim * 4) / (1024 * 1024)
    if est_mb > rss_cap:
        log.error("serving_index_refresh_failed reason=rss_cap estimated_mb=%.1f cap=%s", est_mb, rss_cap)
        return {"ok": False, "error": "serving_index_refresh_failed", "estimated_mb": est_mb}
    crate.serving_index_publish(str(root), gid)
    crate.serving_index_truncate_dirty(str(root))
    cache = QueryEmbedCache(get_query_embed_cache_path(vault), ram_entries=get_query_embed_cache_ram_entries())
    cache.evict(max_rows=get_query_embed_cache_max_rows(), max_age_days=get_query_embed_cache_max_age_days())
    cache.close()
    global _HANDLE
    with _LOCK:
        _HANDLE = None
    log.info(
        "serving_index_published generation=%s cards=%s chunks=%s embeddings=%s",
        gid,
        card_count,
        chunk_count,
        embed_count,
    )
    return {
        "ok": True,
        "generation": gid,
        "report": report,
        "cards": card_count,
        "chunks": chunk_count,
        "embeddings": embed_count,
    }


def verify_serving_index(vault: Path) -> dict[str, Any]:
    status = serving_index_status(vault)
    if not status.get("serving_index_ready"):
        raise ServingIndexUnavailableError("serving_index_unavailable")
    handle = get_serving_handle(vault)
    rows = handle.search("test", limit=1)
    return {"ok": True, "status": status, "sample_search_rows": len(rows), "generation": handle.generation_id}

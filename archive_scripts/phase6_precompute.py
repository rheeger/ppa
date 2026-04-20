"""Precompute kNN candidates + LLM verdicts ONCE for the 1020-card sample.

Writes _artifacts/_phase6-iterations/cache-1020.json with one entry per
(source_uid, target_uid) pair containing:
- embedding_similarity
- target_type
- llm_verdict ('YES'|'UNSURE'|'NO'|'')
- llm_score (0..1)
- llm_model

Subsequent turns load this cache and only re-evaluate the formula — zero LLM
cost, sub-second per turn. Uses the LIBERAL config (k=30, threshold=0.5) so we
can re-filter to tighter configs offline without losing data.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from archive_cli.index_config import get_default_embedding_model, get_default_embedding_version
from archive_cli.seed_links import (
    LINK_TYPE_SEMANTICALLY_RELATED,
    MODULE_SEMANTIC,
    SEED_LINKER_VERSION,
    LinkEvidence,
    SeedCardSketch,
    SeedLinkCandidate,
    SeedLinkCatalog,
    _candidate_exists,
    _edge_exists,
    _is_classification_skipped,
    _semantic_allow_same_type,
    is_semantic_eligible,
    llm_judge_candidate,
)
from archive_vault.provenance import compute_input_hash

REPO_ROOT = Path(__file__).resolve().parents[1]
ITER_DIR = REPO_ROOT / "_artifacts" / "_phase6-iterations"
DEFAULT_CACHE_PATH = ITER_DIR / "cache-1020.json"
DEFAULT_SAMPLE_PATH = Path("/tmp/phase6-sample-uids-1020.json")

# Configurable via env vars to support multiple sweeps.
CACHE_PATH = Path(os.environ.get("PHASE6_CACHE_PATH", str(DEFAULT_CACHE_PATH)))
SAMPLE_PATH = Path(os.environ.get("PHASE6_SAMPLE_PATH", str(DEFAULT_SAMPLE_PATH)))

K_LIBERAL = int(os.environ.get("PHASE6_K_LIBERAL", "10"))
THRESHOLD_LIBERAL = float(os.environ.get("PHASE6_THRESHOLD_LIBERAL", "0.5"))
LLM_WORKERS = int(os.environ.get("PHASE6_LLM_WORKERS", "12"))


def _sketch_from_row(r: dict[str, Any]) -> SeedCardSketch:
    return SeedCardSketch(
        uid=r["uid"], rel_path=r["rel_path"],
        slug=r.get("slug") or r["rel_path"].split("/")[-1].removesuffix(".md"),
        card_type=r["type"], summary=r.get("summary") or "",
        frontmatter={}, body="", content_hash=r.get("content_hash") or "",
        activity_at="", wikilinks=[],
    )


def _build_catalog(conn, schema: str, source_uids: list[str]) -> SeedLinkCatalog:
    rows = conn.execute(
        f"SELECT uid, rel_path, slug, type, summary, content_hash FROM {schema}.cards WHERE uid = ANY(%s)",
        (source_uids,),
    ).fetchall()
    by_uid = {r["uid"]: _sketch_from_row(r) for r in rows}
    by_type: dict[str, list[SeedCardSketch]] = {}
    for sk in by_uid.values():
        by_type.setdefault(sk.card_type, []).append(sk)
    return SeedLinkCatalog(
        cards_by_uid=by_uid, cards_by_exact_slug={}, cards_by_slug={},
        cards_by_type=by_type,
        person_by_email={}, person_by_phone={}, person_by_handle={}, person_by_alias={},
        email_threads_by_thread_id={}, email_messages_by_thread_id={},
        email_messages_by_message_id={}, email_attachments_by_message_id={},
        email_attachments_by_thread_id={}, imessage_threads_by_chat_id={},
        imessage_messages_by_chat_id={}, calendar_events_by_event_id={},
        calendar_events_by_ical_uid={}, media_by_day={}, events_by_day={}, path_buckets={},
    )


def _hydrate(conn, schema: str, catalog: SeedLinkCatalog, target_uids: list[str]) -> None:
    missing = [u for u in target_uids if u not in catalog.cards_by_uid]
    if not missing:
        return
    for chunk in [missing[i:i+500] for i in range(0, len(missing), 500)]:
        rows = conn.execute(
            f"SELECT uid, rel_path, slug, type, summary, content_hash FROM {schema}.cards WHERE uid = ANY(%s)",
            (chunk,),
        ).fetchall()
        for r in rows:
            sk = _sketch_from_row(r)
            catalog.cards_by_uid[sk.uid] = sk


def _knn_for_source(conn, schema: str, source_uid: str, model: str, version: int) -> list[dict[str, Any]]:
    src = conn.execute(
        f"""
        SELECT AVG(e.embedding)::vector AS v FROM {schema}.chunks c
        JOIN {schema}.embeddings e ON e.chunk_key = c.chunk_key
        WHERE c.card_uid = %s AND e.embedding_model = %s AND e.embedding_version = %s
        """,
        (source_uid, model, version),
    ).fetchone()
    if src is None or src["v"] is None:
        return []
    sv = src["v"]
    overfetch = K_LIBERAL * 30
    rows = conn.execute(
        f"""
        WITH nearest AS (
            SELECT c.card_uid, e.embedding <=> %s::vector AS dist
            FROM {schema}.embeddings e
            JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
            WHERE c.card_uid != %s
              AND e.embedding_model = %s AND e.embedding_version = %s
            ORDER BY e.embedding <=> %s::vector
            LIMIT %s
        ),
        per_card AS (SELECT card_uid, MIN(dist) AS d FROM nearest GROUP BY card_uid)
        SELECT pc.card_uid AS target_uid, cards.rel_path AS target_rel_path,
               cards.type AS target_type, 1 - pc.d AS similarity
        FROM per_card pc JOIN {schema}.cards cards ON cards.uid = pc.card_uid
        WHERE 1 - pc.d >= %s
        ORDER BY pc.d
        LIMIT %s
        """,
        (sv, source_uid, model, version, sv, overfetch, THRESHOLD_LIBERAL, K_LIBERAL),
    ).fetchall()
    return [dict(r) for r in rows]


def _log(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY required")
    schema = os.environ.get("PPA_INDEX_SCHEMA", "ppa")
    dsn = os.environ["PPA_INDEX_DSN"]
    vault = Path(os.environ["PPA_PATH"])
    uids = json.loads(SAMPLE_PATH.read_text())
    _log(f"[precompute] schema={schema} sources={len(uids)} k={K_LIBERAL} t={THRESHOLD_LIBERAL}")

    ITER_DIR.mkdir(parents=True, exist_ok=True)
    cache: dict[str, Any] = {}
    if CACHE_PATH.exists():
        cache = json.loads(CACHE_PATH.read_text())
        _log(f"[precompute] loaded cache with {len(cache)} entries; resuming")

    model = get_default_embedding_model()
    if os.environ.get("PPA_EMBEDDING_MODEL"):
        model = os.environ["PPA_EMBEDDING_MODEL"]
    version = int(os.environ.get("PPA_EMBEDDING_VERSION", get_default_embedding_version()))

    t0 = time.time()
    llm_calls_session = 0
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        _log(f"[precompute] building catalog for {len(uids)} source uids…")
        catalog = _build_catalog(conn, schema, uids)
        _log(f"[precompute] catalog ready ({len(catalog.cards_by_uid)} sketches) in {int(time.time()-t0)}s")
        # Phase A: kNN sweep — collect every (source, target) pair we need to judge.
        # Apply the production-grade filters here so the cache reflects what
        # _generate_semantic_candidates would actually surface.
        same_type_allowed = _semantic_allow_same_type()
        pending: list[tuple[str, dict[str, Any]]] = []
        skipped_src_type = 0
        skipped_src_class = 0
        skipped_src_eligible = 0
        skipped_tgt_eligible = 0
        skipped_tgt_class = 0
        skipped_same_type = 0
        skipped_existing_edge = 0
        skipped_existing_cand = 0
        for i, uid in enumerate(uids):
            src = catalog.cards_by_uid.get(uid)
            if src is None:
                continue
            if not is_semantic_eligible(src.card_type, src.summary):
                skipped_src_type += 1
                continue
            if _is_classification_skipped(conn, schema, uid):
                skipped_src_class += 1
                continue
            knn = _knn_for_source(conn, schema, uid, model, version)
            _hydrate(conn, schema, catalog, [r["target_uid"] for r in knn])
            for r in knn:
                tgt_uid = r["target_uid"]
                tgt = catalog.cards_by_uid.get(tgt_uid)
                if tgt is None:
                    continue
                if not is_semantic_eligible(tgt.card_type, tgt.summary):
                    skipped_tgt_eligible += 1
                    continue
                if src.card_type == tgt.card_type and src.card_type not in same_type_allowed:
                    skipped_same_type += 1
                    continue
                if _is_classification_skipped(conn, schema, tgt_uid):
                    skipped_tgt_class += 1
                    continue
                if _edge_exists(conn, schema, uid, tgt_uid):
                    skipped_existing_edge += 1
                    continue
                if _candidate_exists(conn, schema, uid, tgt_uid):
                    skipped_existing_cand += 1
                    continue
                key = f"{uid}|{tgt_uid}"
                if key in cache:
                    continue
                pending.append((key, {"src_uid": uid, "knn": r}))
            if (i + 1) % 100 == 0:
                _log(f"[precompute] kNN swept {i+1}/{len(uids)} | pending={len(pending)} "
                     f"skipped src(type/class/elig)={skipped_src_type}/{skipped_src_class}/{skipped_src_eligible} "
                     f"tgt(elig/class)={skipped_tgt_eligible}/{skipped_tgt_class} "
                     f"same_type={skipped_same_type} edge={skipped_existing_edge} cand={skipped_existing_cand}")
        _log(f"[precompute] kNN complete in {int(time.time()-t0)}s | {len(pending)} pairs queued for LLM judge")

        # Phase B: parallel LLM judge with ThreadPoolExecutor.
        def _judge_pair(payload: dict[str, Any]) -> dict[str, Any] | None:
            uid = payload["src_uid"]
            r = payload["knn"]
            src = catalog.cards_by_uid.get(uid)
            tgt = catalog.cards_by_uid.get(r["target_uid"])
            if src is None or tgt is None:
                return None
            cand = SeedLinkCandidate(
                module_name=MODULE_SEMANTIC,
                source_card_uid=uid, source_rel_path=src.rel_path,
                target_card_uid=r["target_uid"], target_rel_path=r["target_rel_path"],
                target_kind="card", proposed_link_type=LINK_TYPE_SEMANTICALLY_RELATED,
                candidate_group="",
                input_hash=compute_input_hash({"s": uid, "t": r["target_uid"], "m": MODULE_SEMANTIC, "v": SEED_LINKER_VERSION}),
                evidence_hash="cache",
                features={
                    "embedding_similarity": round(float(r["similarity"]), 6),
                    "deterministic_hits": [],
                    "ambiguous_target_count": 0,
                },
                evidences=[LinkEvidence("embedding_similarity", "pgvector_knn", "cosine_similarity",
                                         f"{r['similarity']:.6f}", float(r["similarity"]), {})],
                surface="derived_only", promotion_target="derived_edge",
            )
            llm_score, llm_model, llm_payload = llm_judge_candidate(vault, cand, src, tgt)
            verdict = str((llm_payload or {}).get("link", "")).upper()
            return {
                "source_uid": uid, "source_rel_path": src.rel_path, "source_type": src.card_type,
                "target_uid": r["target_uid"], "target_rel_path": r["target_rel_path"], "target_type": tgt.card_type,
                "embedding_similarity": float(r["similarity"]),
                "llm_verdict": verdict, "llm_score": float(llm_score), "llm_model": llm_model,
            }

        with ThreadPoolExecutor(max_workers=LLM_WORKERS) as pool:
            futures = {pool.submit(_judge_pair, payload): key for key, payload in pending}
            for j, fut in enumerate(as_completed(futures)):
                key = futures[fut]
                try:
                    result = fut.result()
                except Exception as exc:
                    _log(f"[precompute] judge error key={key}: {exc}")
                    continue
                if result is None:
                    continue
                cache[key] = result
                llm_calls_session += 1
                if (j + 1) % 50 == 0:
                    elapsed = int(time.time() - t0)
                    eta = (elapsed / max(j + 1, 1)) * (len(pending) - j - 1)
                    _log(f"[precompute] judged {j+1}/{len(pending)} pairs | {len(cache)} cache | "
                         f"{elapsed}s | ~{int(eta)}s ETA")
                    CACHE_PATH.write_text(json.dumps(cache, indent=2))

    CACHE_PATH.write_text(json.dumps(cache, indent=2))
    elapsed = int(time.time() - t0)
    print(f"[precompute] done. {len(cache)} cache entries, {llm_calls_session} LLM calls, {elapsed}s")
    print(f"[precompute] estimated cost (gpt-4o-mini): ${llm_calls_session * 0.0001:.3f}")


if __name__ == "__main__":
    main()

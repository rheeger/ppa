"""Query, search, and read methods mixin for PostgresArchiveIndex."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from .explain import (
    projection_explain_payload,
    projection_inventory_payload,
    projection_status_payload,
)
from .index_config import (
    VECTOR_CANDIDATE_MULTIPLIER,
    _apply_recency_boost,
    _card_type_prior,
    _coerce_source_fields,
    _field_provenance_bonus,
    _field_provenance_label,
    _vector_literal,
    get_seed_links_enabled,
)
from .materializer import _normalize_exact_text, _normalize_slug
from .projections.registry import (
    PROJECTION_REGISTRY,
    TYPED_PROJECTIONS,
    projection_for_card_type,
)

logger = logging.getLogger("ppa.index_query")


class QueryMixin:
    def status(self) -> dict[str, str]:
        self.ensure_ready()
        with self._connect() as conn:
            rows = conn.execute(f"SELECT key, value FROM {self.schema}.meta").fetchall()
            payload = {str(row["key"]): str(row["value"]) for row in rows}
            try:
                mc = conn.execute(f"SELECT COUNT(*) AS c FROM {self.schema}.note_manifest").fetchone()
                payload["manifest_row_count"] = str(int(mc["c"] or 0))
            except Exception:
                payload["manifest_row_count"] = "0"
            try:
                chk = conn.execute(
                    f"SELECT status, mode, loaded_card_count, vault_manifest_hash, updated_at "
                    f"FROM {self.schema}.rebuild_checkpoint WHERE id = 1"
                ).fetchone()
                if chk:
                    payload["rebuild_checkpoint_status"] = str(chk.get("status") or "")
                    payload["rebuild_checkpoint_mode"] = str(chk.get("mode") or "")
                    payload["rebuild_checkpoint_loaded_cards"] = str(int(chk.get("loaded_card_count") or 0))
                    payload["rebuild_checkpoint_vault_fp"] = str(chk.get("vault_manifest_hash") or "")
                    payload["rebuild_checkpoint_updated_at"] = str(chk.get("updated_at") or "")
            except Exception:
                pass
            try:
                mig_status = self._migration_status(conn)
                payload["migration_applied_count"] = str(mig_status.get("applied_count", 0))
                payload["migration_pending_count"] = str(mig_status.get("pending_count", 0))
                payload["migration_latest_applied"] = str(mig_status.get("latest_applied", "none"))
            except Exception:
                pass
        return payload

    def projection_inventory(self) -> dict[str, Any]:
        return projection_inventory_payload(list(PROJECTION_REGISTRY))

    def projection_status(self) -> dict[str, Any]:
        self.ensure_ready()
        rows: list[dict[str, Any]] = []
        with self._connect() as conn:
            for projection in TYPED_PROJECTIONS:
                card_type = projection.applies_to_types[0]
                stats = conn.execute(
                    f"""
                    SELECT COUNT(*) AS row_count,
                           AVG(CASE WHEN canonical_ready THEN 1.0 ELSE 0.0 END) AS canonical_ready_ratio,
                           COALESCE(
                               ARRAY_REMOVE(ARRAY_AGG(DISTINCT NULLIF(migration_notes, '')), NULL),
                               ARRAY[]::text[]
                           ) AS migration_notes
                    FROM {self.schema}.{projection.table_name}
                    """
                ).fetchone()
                rows.append(
                    {
                        "card_type": card_type,
                        "typed_projection": projection.table_name,
                        "registered": True,
                        "materialized_row_count": int(stats["row_count"] or 0),
                        "canonical_ready_ratio": float(stats["canonical_ready_ratio"] or 0.0),
                        "migration_blockers": [note for note in stats["migration_notes"] or [] if note],
                    }
                )
        return projection_status_payload(rows)

    def projection_explain(self, card_uid: str) -> dict[str, Any]:
        self.ensure_ready()
        with self._connect() as conn:
            card_row = conn.execute(
                f"SELECT uid, type FROM {self.schema}.cards WHERE uid = %s",
                (card_uid,),
            ).fetchone()
            if card_row is None:
                return projection_explain_payload(
                    card_uid=card_uid,
                    card_type="",
                    typed_projection="",
                    canonical_ready=False,
                    field_mappings=[],
                    migration_notes=["card not found"],
                )
            card_type = str(card_row["type"])
            projection = projection_for_card_type(card_type)
            if projection is None:
                return projection_explain_payload(
                    card_uid=card_uid,
                    card_type=card_type,
                    typed_projection="",
                    canonical_ready=False,
                    field_mappings=[],
                    migration_notes=["typed projection not registered"],
                )
            row = conn.execute(
                f"""
                SELECT canonical_ready, migration_notes
                FROM {self.schema}.{projection.table_name}
                WHERE card_uid = %s
                """,
                (card_uid,),
            ).fetchone()
            migration_notes = [str(row["migration_notes"])] if row and str(row["migration_notes"] or "").strip() else []
            return projection_explain_payload(
                card_uid=card_uid,
                card_type=card_type,
                typed_projection=projection.table_name,
                canonical_ready=(bool(row["canonical_ready"]) if row is not None else False),
                field_mappings=[
                    {
                        "typed_column": column.name,
                        "canonical_fields": ([column.source_field] if column.source_field else []),
                        "status": "materialized" if row is not None else "missing",
                    }
                    for column in projection.columns
                ],
                migration_notes=migration_notes,
            )

    def _filter_clauses(
        self,
        *,
        alias: str,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        org_filter: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> tuple[list[str], list[Any]]:
        clauses = ["1 = 1"]
        params: list[Any] = []
        if type_filter:
            clauses.append(f"{alias}.type = %s")
            params.append(type_filter)
        if source_filter:
            clauses.append(
                f"EXISTS (SELECT 1 FROM {self.schema}.card_sources cs WHERE cs.card_uid = {alias}.uid AND cs.source = %s)"
            )
            params.append(source_filter)
        if people_filter:
            clauses.append(
                f"EXISTS (SELECT 1 FROM {self.schema}.card_people cp WHERE cp.card_uid = {alias}.uid AND cp.person = %s)"
            )
            params.append(people_filter)
        if org_filter:
            clauses.append(
                f"EXISTS (SELECT 1 FROM {self.schema}.card_orgs co WHERE co.card_uid = {alias}.uid AND co.org = %s)"
            )
            params.append(org_filter)
        if start_date:
            clauses.append(f"LEFT({alias}.activity_at, 10) >= %s")
            params.append(start_date)
        if end_date:
            clauses.append(f"LEFT({alias}.activity_at, 10) <= %s")
            params.append(end_date)
        return clauses, params

    @staticmethod
    def _merge_lexical_uid_rows(a: list[dict[str, Any]], b: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Keep best lexical_score per card_uid (same rule as retrieval_pipeline.merge_lexical_rows)."""
        by_uid: dict[str, dict[str, Any]] = {str(r["card_uid"]): dict(r) for r in a}
        for row in b:
            uid = str(row["card_uid"])
            prev = by_uid.get(uid)
            if prev is None or float(row.get("lexical_score", 0.0)) > float(prev.get("lexical_score", 0.0)):
                by_uid[uid] = dict(row)
        return list(by_uid.values())

    @staticmethod
    def _lexical_row_sort_key(row: dict[str, Any]) -> tuple:
        exact = (
            int(row.get("slug_exact", 0))
            + int(row.get("summary_exact", 0))
            + int(row.get("external_id_exact", 0))
            + int(row.get("person_exact", 0))
        )
        return (
            -exact,
            -float(row.get("lexical_score", 0.0)),
            str(row.get("activity_at", "")),
            str(row.get("rel_path", "")),
        )

    def _lexical_candidates(
        self,
        *,
        query: str,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int,
    ) -> list[dict[str, Any]]:
        """Lexical candidates via two branches on one connection: FTS then exact-match, merged.

        Split into two queries to avoid a single OR that forces Postgres into a
        slow plan (bitmap OR across GIN + btree). Sorting uses a wrapped subquery
        so ORDER BY references real columns (PG does not allow output aliases
        inside expressions in ORDER BY at the same SELECT level).
        """
        normalized_query = _normalize_exact_text(query)
        clauses, params = self._filter_clauses(
            alias="c",
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            start_date=start_date,
            end_date=end_date,
        )
        branch_limit = max(limit * 2, limit)

        select_sql = f"""
            SELECT c.uid AS card_uid, c.rel_path, c.summary, c.type, c.activity_at,
                   ts_rank_cd(c.search_document, plainto_tsquery('english', %s)) AS lexical_score,
                   CASE WHEN lower(c.slug) = %s THEN 1 ELSE 0 END AS slug_exact,
                   CASE WHEN lower(c.summary) = %s THEN 1 ELSE 0 END AS summary_exact,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM {self.schema}.external_ids ei
                       WHERE ei.card_uid = c.uid AND lower(ei.external_id) = %s
                   ) THEN 1 ELSE 0 END AS external_id_exact,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM {self.schema}.card_people cp
                       WHERE cp.card_uid = c.uid AND lower(cp.person) = %s
                   ) THEN 1 ELSE 0 END AS person_exact
            FROM {self.schema}.cards c
        """

        # Postgres allows ORDER BY output aliases only as bare identifiers, not inside
        # expressions—wrap so sort keys reference subquery columns (see PG ORDER BY rules).
        def _wrap_lexical_order(inner_sql: str) -> str:
            return f"""
                SELECT * FROM ({inner_sql}) AS _lex
                ORDER BY (_lex.slug_exact + _lex.summary_exact + _lex.external_id_exact + _lex.person_exact) DESC,
                         _lex.lexical_score DESC,
                         _lex.activity_at DESC,
                         _lex.rel_path ASC
                LIMIT %s
            """

        with self._connect() as conn:
            inner_fts = f"""
                {select_sql}
                WHERE {" AND ".join(clauses)}
                  AND c.search_document @@ plainto_tsquery('english', %s)
            """
            fts_sql = _wrap_lexical_order(inner_fts)
            fts_params = [
                query,
                normalized_query,
                normalized_query,
                normalized_query,
                normalized_query,
                *params,
                query,
                branch_limit,
            ]
            fts_rows = [dict(r) for r in conn.execute(fts_sql, fts_params).fetchall()]

            inner_exact = f"""
                {select_sql}
                WHERE {" AND ".join(clauses)}
                  AND (
                      c.slug = %s
                      OR EXISTS (
                          SELECT 1 FROM {self.schema}.external_ids ei
                          WHERE ei.card_uid = c.uid AND lower(ei.external_id) = %s
                      )
                      OR EXISTS (
                          SELECT 1 FROM {self.schema}.card_people cp
                          WHERE cp.card_uid = c.uid AND lower(cp.person) = %s
                      )
                  )
            """
            exact_sql = _wrap_lexical_order(inner_exact)
            exact_params = [
                query,
                normalized_query,
                normalized_query,
                normalized_query,
                normalized_query,
                *params,
                normalized_query,
                normalized_query,
                normalized_query,
                branch_limit,
            ]
            exact_rows = [dict(r) for r in conn.execute(exact_sql, exact_params).fetchall()]

        merged = self._merge_lexical_uid_rows(fts_rows, exact_rows)
        merged.sort(key=self._lexical_row_sort_key)
        return merged[:limit]

    def _vector_candidate_rows(
        self,
        conn,
        *,
        query_vector: list[float],
        embedding_model: str,
        embedding_version: int,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int,
    ) -> list[dict[str, Any]]:
        vector_value = _vector_literal(query_vector)
        clauses, params = self._filter_clauses(
            alias="card",
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            start_date=start_date,
            end_date=end_date,
        )
        sql = f"""
            SELECT chunk.card_uid, card.rel_path, card.summary, card.type, card.activity_at,
                   chunk.chunk_type, chunk.chunk_index, chunk.source_fields, chunk.content, chunk.token_count,
                   1 - (embedding.embedding <=> %s::vector) AS similarity
            FROM {self.schema}.chunks chunk
            JOIN {self.schema}.embeddings embedding
                ON embedding.chunk_key = chunk.chunk_key
            JOIN {self.schema}.cards card
                ON card.uid = chunk.card_uid
            WHERE embedding.embedding_model = %s
              AND embedding.embedding_version = %s
              AND {" AND ".join(clauses)}
            ORDER BY embedding.embedding <=> %s::vector ASC, card.activity_at DESC, chunk.chunk_index ASC
            LIMIT %s
        """
        rows = conn.execute(
            sql,
            [
                vector_value,
                embedding_model,
                embedding_version,
                *params,
                vector_value,
                limit,
            ],
        ).fetchall()
        return [dict(row) for row in rows]

    def _aggregate_vector_candidates(self, rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
        """Collapse chunk-level vector rows to card-level, keeping best similarity.

        Returns rows sorted by similarity desc with provenance metadata but no
        composite score. Callers that need a scored ranking (vector_search) apply
        _score_and_rank_vector separately; hybrid callers pass these rows directly
        to fuse_lexical_vector_rows which computes its own unified score.
        """
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            card_uid = str(row["card_uid"])
            similarity = float(row["similarity"])
            source_fields = _coerce_source_fields(row.get("source_fields", []))
            entry = grouped.setdefault(
                card_uid,
                {
                    "card_uid": card_uid,
                    "rel_path": str(row["rel_path"]),
                    "summary": str(row["summary"]),
                    "type": str(row["type"]),
                    "activity_at": str(row.get("activity_at", "")),
                    "matched_by": "vector",
                    "matched_chunk_count": 0,
                    "similarity": similarity,
                    "preview": str(row["content"]).replace("\n", " ")[:160],
                    "chunk_type": str(row["chunk_type"]),
                    "chunk_index": int(row["chunk_index"]),
                    "source_fields": source_fields,
                    "provenance_bias": _field_provenance_label(source_fields),
                    "provenance_score": _field_provenance_bonus(source_fields),
                    "score": 0.0,
                    "graph_hops": "",
                },
            )
            entry["matched_chunk_count"] = int(entry["matched_chunk_count"]) + 1
            if similarity > float(entry["similarity"]):
                entry["similarity"] = similarity
                entry["preview"] = str(row["content"]).replace("\n", " ")[:160]
                entry["chunk_type"] = str(row["chunk_type"])
                entry["chunk_index"] = int(row["chunk_index"])
                entry["source_fields"] = source_fields
                entry["provenance_bias"] = _field_provenance_label(source_fields)
                entry["provenance_score"] = _field_provenance_bonus(source_fields)
        ranked = sorted(
            grouped.values(),
            key=lambda e: (-float(e["similarity"]), str(e["rel_path"])),
        )
        return ranked[:limit]

    def _score_and_rank_vector(self, rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
        """Apply composite scoring to card-level vector candidates.

        Scores combine similarity, type priors, recency, provenance, and a
        multi-chunk match boost. Used only by vector_search; hybrid retrieval
        computes its own unified score in the fusion stage.
        """
        _apply_recency_boost(rows, key_name="recency_score")
        for entry in rows:
            chunk_match_boost = min(max(int(entry["matched_chunk_count"]) - 1, 0) * 0.025, 0.1)
            entry["score"] = round(
                (float(entry["similarity"]) * 1.35)
                + _card_type_prior(str(entry["type"]))
                + float(entry.get("recency_score", 0.0))
                + float(entry.get("provenance_score", 0.0))
                + chunk_match_boost,
                6,
            )
        rows.sort(
            key=lambda e: (
                -float(e["score"]),
                -float(e["similarity"]),
                str(e["rel_path"]),
            )
        )
        return rows[:limit]

    def _graph_neighbor_uids(self, conn, anchor_uids: list[str]) -> set[str]:
        if not anchor_uids:
            return set()
        seed_link_union = ""
        if get_seed_links_enabled():
            seed_link_union = f"""
                UNION ALL
                SELECT lc.source_card_uid AS source_uid, lc.target_card_uid AS target_uid, lc.target_kind
                FROM {self.schema}.link_candidates lc
                JOIN {self.schema}.promotion_queue pq
                  ON pq.candidate_id = lc.candidate_id
                 AND pq.promotion_target = 'derived_edge'
                 AND pq.promotion_status = 'applied'
            """
        rows = conn.execute(
            f"""
            WITH graph_edges AS (
                SELECT source_uid, target_uid, target_kind
                FROM {self.schema}.edges
                {seed_link_union}
            )
            SELECT DISTINCT
                CASE
                    WHEN source_uid = ANY(%s) THEN target_uid
                    ELSE source_uid
                END AS neighbor_uid
            FROM graph_edges
            WHERE target_kind = 'card'
              AND target_uid <> ''
              AND (source_uid = ANY(%s) OR target_uid = ANY(%s))
            """,
            (anchor_uids, anchor_uids, anchor_uids),
        ).fetchall()
        return {str(row["neighbor_uid"]) for row in rows if str(row["neighbor_uid"]).strip()}

    def vector_search(
        self,
        *,
        query_vector: list[float],
        embedding_model: str,
        embedding_version: int,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as conn:
            candidate_rows = self._vector_candidate_rows(
                conn,
                query_vector=query_vector,
                embedding_model=embedding_model,
                embedding_version=embedding_version,
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                start_date=start_date,
                end_date=end_date,
                limit=max(limit * VECTOR_CANDIDATE_MULTIPLIER, limit),
            )
        grouped = self._aggregate_vector_candidates(candidate_rows, limit=limit)
        return self._score_and_rank_vector(grouped, limit=limit)

    def fetch_hybrid_lexical_vector(
        self,
        *,
        query: str,
        query_vector: list[float],
        embedding_model: str,
        embedding_version: int,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        candidate_limit: int = 20,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fetch lexical and vector candidates in parallel on separate connections.

        Lexical and vector retrieval are genuinely independent (different SQL,
        different indexes) so they run in a two-thread pool. Each branch opens
        its own connection; peak concurrency is two connections per call.
        Errors from either branch are logged to stderr before re-raising so
        they are visible in MCP output even if the other branch is still running.
        """
        self.ensure_ready()
        cleaned = query.strip()
        if not cleaned:
            return [], []
        cap = max(candidate_limit, 1)

        def _lexical_job() -> list[dict[str, Any]]:
            return self._lexical_candidates(
                query=cleaned,
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                start_date=start_date,
                end_date=end_date,
                limit=cap,
            )

        def _vector_job() -> list[dict[str, Any]]:
            with self._connect() as conn:
                return self._aggregate_vector_candidates(
                    self._vector_candidate_rows(
                        conn,
                        query_vector=query_vector,
                        embedding_model=embedding_model,
                        embedding_version=embedding_version,
                        type_filter=type_filter,
                        source_filter=source_filter,
                        people_filter=people_filter,
                        start_date=start_date,
                        end_date=end_date,
                        limit=cap,
                    ),
                    limit=cap,
                )

        # See archive_mcp/log.py — stderr-only logging; never print to stdout in MCP mode.
        with ThreadPoolExecutor(max_workers=2) as pool:
            fut_lex = pool.submit(_lexical_job)
            fut_vec = pool.submit(_vector_job)
            try:
                t_lex = time.monotonic()
                lexical_rows = fut_lex.result()
                logger.info(
                    "lexical_candidates done rows=%d elapsed_ms=%d",
                    len(lexical_rows),
                    int((time.monotonic() - t_lex) * 1000),
                )
            except Exception as exc:
                logger.error("lexical retrieval failed: %r", exc)
                raise
            try:
                t_vec = time.monotonic()
                vector_rows = fut_vec.result()
                logger.info(
                    "vector_candidates done rows=%d elapsed_ms=%d",
                    len(vector_rows),
                    int((time.monotonic() - t_vec) * 1000),
                )
            except Exception as exc:
                logger.error("vector retrieval failed: %r", exc)
                raise
        return lexical_rows, vector_rows

    def fetch_graph_neighbors_for_uids(self, anchor_uids: list[str]) -> set[str]:
        if not anchor_uids:
            return set()
        self.ensure_ready()
        with self._connect() as conn:
            return self._graph_neighbor_uids(conn, anchor_uids)

    def hybrid_search(
        self,
        *,
        query: str,
        query_vector: list[float],
        embedding_model: str,
        embedding_version: int,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self.ensure_ready()
        cleaned = query.strip()
        if not cleaned:
            return []
        cap = max(limit * VECTOR_CANDIDATE_MULTIPLIER, limit)
        lexical_rows, vector_rows = self.fetch_hybrid_lexical_vector(
            query=query,
            query_vector=query_vector,
            embedding_model=embedding_model,
            embedding_version=embedding_version,
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            start_date=start_date,
            end_date=end_date,
            candidate_limit=cap,
        )
        anchor_uids = [
            str(row["card_uid"])
            for row in lexical_rows
            if int(row["slug_exact"])
            or int(row["summary_exact"])
            or int(row["external_id_exact"])
            or int(row["person_exact"])
        ]
        neighbor_uids = self.fetch_graph_neighbors_for_uids(anchor_uids)
        from .retrieval_pipeline import HybridFetchInputs, fuse_and_rank_hybrid

        return fuse_and_rank_hybrid(
            HybridFetchInputs(
                lexical_rows=lexical_rows,
                vector_rows=vector_rows,
                neighbor_uids=neighbor_uids,
                query_cleaned=cleaned,
                subqueries_used=(cleaned,),
            ),
            final_limit=limit,
        )

    def read_path_for_uid(self, uid: str) -> str | None:
        self.ensure_ready()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT rel_path FROM {self.schema}.cards WHERE uid = %s",
                (uid,),
            ).fetchone()
        return None if row is None else str(row["rel_path"])

    def person_path(self, name: str) -> str | None:
        self.ensure_ready()
        slug = _normalize_slug(name)
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT rel_path FROM {self.schema}.cards WHERE slug = %s AND type = 'person' LIMIT 1",
                (slug,),
            ).fetchone()
        return None if row is None else str(row["rel_path"])

    def query_cards(
        self,
        *,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        org_filter: str = "",
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self.ensure_ready()
        clauses, params = self._filter_clauses(
            alias="c",
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            org_filter=org_filter,
        )
        params.append(limit)
        sql = f"""
            SELECT DISTINCT c.rel_path, c.summary, c.type, c.activity_at
            FROM {self.schema}.cards c
            WHERE {" AND ".join(clauses)}
            ORDER BY c.activity_at DESC, c.rel_path ASC
            LIMIT %s
        """
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def timeline(self, *, start_date: str = "", end_date: str = "", limit: int = 20) -> list[dict[str, Any]]:
        self.ensure_ready()
        clauses, params = self._filter_clauses(alias="c", start_date=start_date, end_date=end_date)
        params.append(limit)
        sql = f"""
            SELECT c.activity_at AS created, c.rel_path, c.summary, c.type
            FROM {self.schema}.cards c
            WHERE {" AND ".join(clauses)}
            ORDER BY c.activity_at ASC, c.rel_path ASC
            LIMIT %s
        """
        with self._connect() as conn:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def stats(self) -> tuple[int, list[dict[str, Any]], list[dict[str, Any]]]:
        self.ensure_ready()
        with self._connect() as conn:
            total_row = conn.execute(f"SELECT COUNT(*) AS count FROM {self.schema}.cards").fetchone()
            by_type = conn.execute(
                f"SELECT type, COUNT(*) AS count FROM {self.schema}.cards GROUP BY type ORDER BY count DESC, type ASC"
            ).fetchall()
            by_source = conn.execute(
                f"SELECT source, COUNT(*) AS count FROM {self.schema}.card_sources GROUP BY source ORDER BY count DESC, source ASC"
            ).fetchall()
        return (
            int(total_row["count"]),
            [dict(row) for row in by_type],
            [dict(row) for row in by_source],
        )

    def graph(self, note_path: str, hops: int = 2) -> dict[str, list[str]] | None:
        self.ensure_ready()
        rel_path = note_path if note_path.endswith(".md") else f"{note_path}.md"
        with self._connect() as conn:
            exists = conn.execute(
                f"SELECT 1 FROM {self.schema}.cards WHERE rel_path = %s",
                (rel_path,),
            ).fetchone()
            if exists is None:
                return None
            visited = {rel_path}
            frontier = {rel_path}
            graph: dict[str, list[str]] = {}
            seed_link_union = ""
            if get_seed_links_enabled():
                seed_link_union = f"""
                    UNION ALL
                    SELECT lc.source_rel_path AS source_path, lc.target_rel_path AS target_path, lc.target_kind
                    FROM {self.schema}.link_candidates lc
                    JOIN {self.schema}.promotion_queue pq
                      ON pq.candidate_id = lc.candidate_id
                     AND pq.promotion_target = 'derived_edge'
                     AND pq.promotion_status = 'applied'
                """
            for _ in range(max(hops, 1)):
                next_frontier: set[str] = set()
                for current in frontier:
                    rows = conn.execute(
                        f"""
                        WITH graph_edges AS (
                            SELECT source_path, target_path, target_kind
                            FROM {self.schema}.edges
                            {seed_link_union}
                        )
                        SELECT target_path
                        FROM graph_edges
                        WHERE source_path = %s
                          AND target_kind = 'card'
                        ORDER BY target_path ASC
                        """,
                        (current,),
                    ).fetchall()
                    targets = [str(row["target_path"]) for row in rows]
                    graph[current] = targets
                    for target in targets:
                        if target not in visited:
                            visited.add(target)
                            next_frontier.add(target)
                frontier = next_frontier
                if not frontier:
                    break
        return graph

    def search(
        self,
        query: str,
        limit: int = 20,
        *,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
    ) -> list[dict[str, Any]]:
        self.ensure_ready()
        cleaned = query.strip()
        if not cleaned:
            return []
        rows = self._lexical_candidates(
            query=cleaned,
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        for row in rows:
            row["matched_by"] = "lexical"
            row["exact_match"] = bool(
                int(row.get("slug_exact", 0))
                or int(row.get("summary_exact", 0))
                or int(row.get("external_id_exact", 0))
                or int(row.get("person_exact", 0))
            )
        return rows

    def duplicate_uid_rows(self, *, limit: int = 20) -> list[dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT uid, preferred_rel_path, preferred_type, preferred_source_id, preferred_summary,
                       duplicate_rel_path, duplicate_type, duplicate_source_id, duplicate_summary, duplicate_group_size
                FROM {self.schema}.duplicate_uid_rows
                ORDER BY duplicate_group_size DESC, uid ASC, duplicate_rel_path ASC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

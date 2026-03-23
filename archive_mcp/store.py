"""Archive store/service abstraction for archive-mcp."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from hfa.vault import find_note_by_slug

from .config import load_archive_config
from .contracts import ArchiveStore
from .embedding_provider import get_embedding_provider
from .explain import retrieval_explain_payload, retrieval_explain_payload_v2
from .features import archive_context, build_context_json, build_context_text
from .index_config import get_seed_links_enabled
from .index_store import (PostgresArchiveIndex, get_default_embedding_model,
                          get_default_embedding_version)
from .projections.registry import projection_for_card_type
from .query_planner import build_query_plan, effective_filters_from_plan
from .reranker import blend_rerank_scores, reranker_for_config
from .retrieval_pipeline import (PIPELINE_VERSION, HybridFetchInputs,
                                 anchor_uids_from_lexical,
                                 fuse_and_rank_hybrid, merge_lexical_rows,
                                 merge_vector_rows, score_breakdown_for_row)

_SEED_LINKS_DISABLED = {"error": "Seed links are not enabled. Set PPA_SEED_LINKS_ENABLED=1 to enable."}


def _import_seed_links():
    from .seed_links import (compute_link_quality_gate,
                             get_link_candidate_details, get_seed_scope_rows,
                             get_surface_policy_rows, list_link_candidates,
                             review_link_candidate,
                             run_incremental_link_refresh,
                             run_seed_link_backfill, run_seed_link_enqueue,
                             run_seed_link_promotion_workers,
                             run_seed_link_report, run_seed_link_workers)
    return {
        "compute_link_quality_gate": compute_link_quality_gate,
        "get_link_candidate_details": get_link_candidate_details,
        "get_seed_scope_rows": get_seed_scope_rows,
        "get_surface_policy_rows": get_surface_policy_rows,
        "list_link_candidates": list_link_candidates,
        "review_link_candidate": review_link_candidate,
        "run_incremental_link_refresh": run_incremental_link_refresh,
        "run_seed_link_backfill": run_seed_link_backfill,
        "run_seed_link_enqueue": run_seed_link_enqueue,
        "run_seed_link_promotion_workers": run_seed_link_promotion_workers,
        "run_seed_link_report": run_seed_link_report,
        "run_seed_link_workers": run_seed_link_workers,
    }


class DefaultArchiveStore(ArchiveStore):
    def __init__(self, vault: Path | None = None, index: Any | None = None, provider_factory=None):
        self.config = load_archive_config()
        self.vault = Path(vault or self.config.vault_path)
        self.index = index or PostgresArchiveIndex(self.vault, dsn=self.config.index_dsn)
        self.provider_factory = provider_factory or get_embedding_provider

    def bootstrap(self) -> dict[str, Any]:
        return self.index.bootstrap()

    def rebuild(self, **kwargs) -> dict[str, Any]:
        allowed = {
            "workers",
            "batch_size",
            "commit_interval",
            "progress_every",
            "executor_kind",
            "force_full",
            "disable_manifest_cache",
        }
        filtered = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
        return self.index.rebuild_with_metrics(**filtered).counts

    def status(self) -> dict[str, Any]:
        payload = self.index.status()
        payload["backend"] = "postgres"
        payload["schema"] = getattr(self.index, "schema", str(payload.get("schema", "")))
        if hasattr(self.index, "projection_status"):
            payload["projection_counts"] = {
                row["typed_projection"]: row["materialized_row_count"]
                for row in self.projection_status()["projection_coverage"]
            }
        else:
            payload["projection_counts"] = {}
        payload["runtime_mode"] = str(self.config.runtime.get("mode", "stdio"))
        payload["retrieval"] = dict(self.config.retrieval)
        payload["local_model_runtime"] = dict(self.config.runtime.get("local_model_runtime") or {})
        return payload

    def read(self, path_or_uid: str) -> dict[str, Any]:
        if path_or_uid.endswith(".md"):
            path = (self.vault / path_or_uid).resolve()
            return {"path_or_uid": path_or_uid, "content": path.read_text(encoding="utf-8") if path.exists() else "", "found": path.exists()}
        rel_path = self.index.read_path_for_uid(path_or_uid)
        if rel_path is None:
            return {"path_or_uid": path_or_uid, "content": "", "found": False}
        return {"path_or_uid": path_or_uid, "content": (self.vault / rel_path).read_text(encoding="utf-8"), "found": True, "rel_path": rel_path}

    def query(self, *, type_filter: str = "", source_filter: str = "", people_filter: str = "", org_filter: str = "", limit: int = 20) -> dict[str, Any]:
        return {
            "rows": self.index.query_cards(
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                org_filter=org_filter,
                limit=limit,
            )
        }

    def search(self, query: str, *, limit: int = 20) -> dict[str, Any]:
        return {"rows": self.index.search(query, limit=limit)}

    def graph(self, note_path: str, *, hops: int = 2) -> dict[str, Any]:
        rel_path = note_path if note_path.endswith(".md") else f"{note_path}.md"
        return {"graph": self.index.graph(rel_path, hops=hops), "rel_path": rel_path}

    def timeline(self, *, start_date: str = "", end_date: str = "", limit: int = 20) -> dict[str, Any]:
        return {"rows": self.index.timeline(start_date=start_date, end_date=end_date, limit=limit)}

    def vector_search(self, query: str, **kwargs) -> dict[str, Any]:
        model = kwargs.get("embedding_model", "") or get_default_embedding_model()
        version = kwargs.get("embedding_version", 0) or get_default_embedding_version()
        provider = self.provider_factory(model=model)
        query_vector = provider.embed_texts([query.strip() or ""])[0]
        rows = self.index.vector_search(
            query_vector=query_vector,
            embedding_model=model,
            embedding_version=version,
            type_filter=str(kwargs.get("type_filter", "")),
            source_filter=str(kwargs.get("source_filter", "")),
            people_filter=str(kwargs.get("people_filter", "")),
            start_date=str(kwargs.get("start_date", "")),
            end_date=str(kwargs.get("end_date", "")),
            limit=int(kwargs.get("limit", 20) or 20),
        )
        return {"rows": rows, "embedding_model": model, "embedding_version": version}

    def _run_hybrid_retrieval(
        self,
        *,
        query: str,
        query_vector: list[float],
        embedding_model: str,
        embedding_version: int,
        type_filter: str,
        source_filter: str,
        people_filter: str,
        start_date: str,
        end_date: str,
        limit: int,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rc = self.config.retrieval
        mult = int(rc.get("candidate_multiplier", 8) or 8)
        cap = max(limit * mult, limit)
        plan = build_query_plan(query, config=rc)
        merge_filters = bool(rc.get("query_planner", {}).get("allow_filter_inference", True))
        eff_t, eff_s, eff_sd, eff_ed = effective_filters_from_plan(
            plan,
            type_filter=type_filter,
            source_filter=source_filter,
            start_date=start_date,
            end_date=end_date,
            allow_merge=merge_filters,
        )
        merged_lex: list[dict[str, Any]] = []
        merged_vec: list[dict[str, Any]] = []
        subqs: list[str] = []
        for pq in plan.queries:
            qt = pq.text.strip()
            if not qt:
                continue
            subqs.append(qt)
            lex, vec = self.index.fetch_hybrid_lexical_vector(
                query=qt,
                query_vector=query_vector,
                embedding_model=embedding_model,
                embedding_version=embedding_version,
                type_filter=eff_t,
                source_filter=eff_s,
                people_filter=people_filter,
                start_date=eff_sd,
                end_date=eff_ed,
                candidate_limit=cap,
            )
            merged_lex = merge_lexical_rows(merged_lex, lex)
            merged_vec = merge_vector_rows(merged_vec, vec)
        if not subqs:
            return [], {
                "plan": plan,
                "pipeline_meta": {},
                "subqueries": [],
                "reranker": {"enabled": False, "provider": "none"},
            }
        anchors = anchor_uids_from_lexical(merged_lex)
        neighbors = self.index.fetch_graph_neighbors_for_uids(anchors)
        pipeline_meta: dict[str, Any] = {}
        rr_cfg = rc.get("reranker", {})
        pool_limit = limit
        if rr_cfg.get("enabled") and str(rr_cfg.get("provider", "none")).lower() not in ("none", "", "noop"):
            pool_limit = max(limit, int(rr_cfg.get("top_k", 30) or 30))
        rows = fuse_and_rank_hybrid(
            HybridFetchInputs(
                lexical_rows=merged_lex,
                vector_rows=merged_vec,
                neighbor_uids=neighbors,
                query_cleaned=query.strip(),
                subqueries_used=tuple(subqs),
            ),
            final_limit=pool_limit,
            pipeline_meta=pipeline_meta,
        )
        rerank_note = {"provider": str(rr_cfg.get("provider", "none")), "enabled": bool(rr_cfg.get("enabled"))}
        if rr_cfg.get("enabled") and str(rr_cfg.get("provider", "none")).lower() not in ("none", "", "noop"):
            reranker = reranker_for_config(rc)
            top_k = min(int(rr_cfg.get("top_k", 30)), len(rows))
            head = rows[:top_k]
            ctx_on = bool(rc.get("context", {}).get("include_in_reranker_input", True))
            for row in head:
                if ctx_on:
                    cj = build_context_json(
                        card_type=str(row.get("type", "")),
                        summary=str(row.get("summary", "")),
                    )
                    row["context_text"] = build_context_text(cj)
                else:
                    row["context_text"] = ""
            rr_list = reranker.rerank(query, head)
            by_uid = {x.card_uid: x for x in rr_list}
            blend_cfg = rr_cfg.get("blend") or {}
            head = blend_rerank_scores(
                head,
                by_uid,
                top_1_3_retrieval_weight=float(blend_cfg.get("top_1_3_retrieval_weight", 0.75)),
                top_4_10_retrieval_weight=float(blend_cfg.get("top_4_10_retrieval_weight", 0.60)),
                rest_retrieval_weight=float(blend_cfg.get("rest_retrieval_weight", 0.40)),
                preserve_exact_match_floor=bool(rr_cfg.get("preserve_exact_match_floor", True)),
            )
            rows = head + rows[top_k:]
        rows = rows[:limit]
        return rows, {
            "plan": plan,
            "pipeline_meta": pipeline_meta,
            "subqueries": subqs,
            "reranker": rerank_note,
        }

    def hybrid_search(self, query: str, **kwargs) -> dict[str, Any]:
        model = kwargs.get("embedding_model", "") or get_default_embedding_model()
        version = kwargs.get("embedding_version", 0) or get_default_embedding_version()
        provider = self.provider_factory(model=model)
        query_vector = provider.embed_texts([query.strip() or ""])[0]
        rows, _trace = self._run_hybrid_retrieval(
            query=query,
            query_vector=query_vector,
            embedding_model=model,
            embedding_version=version,
            type_filter=str(kwargs.get("type_filter", "")),
            source_filter=str(kwargs.get("source_filter", "")),
            people_filter=str(kwargs.get("people_filter", "")),
            start_date=str(kwargs.get("start_date", "")),
            end_date=str(kwargs.get("end_date", "")),
            limit=int(kwargs.get("limit", 20) or 20),
        )
        return {"rows": rows, "embedding_model": model, "embedding_version": version}

    def embedding_status(self, *, embedding_model: str = "", embedding_version: int = 0) -> dict[str, Any]:
        model = embedding_model or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        return self.index.embedding_status(embedding_model=model, embedding_version=version)

    def embedding_backlog(self, *, limit: int = 20, embedding_model: str = "", embedding_version: int = 0) -> dict[str, Any]:
        model = embedding_model or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        return {
            "rows": self.index.embedding_backlog(limit=limit, embedding_model=model, embedding_version=version),
            "embedding_model": model,
            "embedding_version": version,
        }

    def embed_pending(self, *, limit: int = 0, embedding_model: str = "", embedding_version: int = 0) -> dict[str, Any]:
        model = embedding_model or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        provider = self.provider_factory(model=model)
        ctx = self.config.retrieval.get("context", {})
        include_ctx = bool(ctx.get("include_in_embeddings", True))
        return self.index.embed_pending(
            provider=provider,
            embedding_model=model,
            embedding_version=version,
            limit=limit,
            include_context_prefix=include_ctx,
        )

    def projection_inventory(self) -> dict[str, Any]:
        if hasattr(self.index, "projection_inventory"):
            return self.index.projection_inventory()
        from .explain import projection_inventory_payload
        from .projections.registry import PROJECTION_REGISTRY

        return projection_inventory_payload(list(PROJECTION_REGISTRY))

    def projection_status(self) -> dict[str, Any]:
        if hasattr(self.index, "projection_status"):
            return self.index.projection_status()
        return {"projection_coverage": []}

    def projection_explain(self, card_uid: str) -> dict[str, Any]:
        if hasattr(self.index, "projection_explain"):
            return self.index.projection_explain(card_uid)
        return {
            "card_uid": card_uid,
            "card_type": "",
            "typed_projection": "",
            "canonical_ready": False,
            "field_mappings": [],
            "migration_notes": ["projection explain unavailable"],
        }

    def _query_plan_dict(self, plan: Any) -> dict[str, Any]:
        inf = plan.inferred
        return {
            "planner_provider": plan.planner_provider,
            "queries": [{"text": q.text, "role": q.role, "weight": q.weight} for q in plan.queries],
            "inferred": {
                "type_hints": list(inf.type_hints),
                "source_hints": list(inf.source_hints),
                "start_date_hint": inf.start_date_hint,
                "end_date_hint": inf.end_date_hint,
                "phrases": list(inf.phrases),
                "emails": list(inf.emails),
            },
        }

    def _why_ranked(self, breakdown: dict[str, float], row: dict[str, Any]) -> str:
        items = sorted(breakdown.items(), key=lambda kv: -abs(kv[1]))
        top = items[0][0] if items else "unknown"
        if bool(row.get("exact_match")):
            return "exact lexical anchor dominates retrieval score"
        return f"dominant_component={top}"

    def retrieval_explain(self, query: str, **kwargs) -> dict[str, Any]:
        mode = str(kwargs.get("mode", "hybrid") or "hybrid")
        rc = self.config.retrieval
        ex_cfg = rc.get("explain", {})
        if not ex_cfg.get("enabled", True):
            result = self.vector_search(query, **kwargs) if mode == "vector" else self.hybrid_search(query, **kwargs)
            slim = []
            for row in result["rows"]:
                projection = projection_for_card_type(str(row.get("type", ""))) if row.get("type") else None
                context = archive_context(
                    card_type=str(row.get("type", "")),
                    frontmatter={},
                    provenance_bias=0.0,
                    typed_projection_names=(projection.table_name,) if projection else (),
                )
                slim.append(
                    {
                        "card_uid": str(row.get("card_uid", row.get("uid", ""))),
                        "rel_path": str(row.get("rel_path", "")),
                        "matched_by": [str(row.get("matched_by", ""))] if row.get("matched_by") else [],
                        "score_components": {
                            "lexical": float(row.get("lexical_score", 0.0) or 0.0),
                            "vector": float(row.get("vector_similarity", row.get("similarity", 0.0)) or 0.0),
                        },
                        "context": {"card_type": context.card_type, "typed_projection_names": list(context.typed_projection_names)},
                    }
                )
            return retrieval_explain_payload(query, mode, slim)

        model = kwargs.get("embedding_model", "") or get_default_embedding_model()
        version = kwargs.get("embedding_version", 0) or get_default_embedding_version()
        provider = self.provider_factory(model=model)
        query_vector = provider.embed_texts([query.strip() or ""])[0]
        limit = int(kwargs.get("limit", 20) or 20)
        trace: dict[str, Any] = {}
        if mode == "vector":
            rows = self.index.vector_search(
                query_vector=query_vector,
                embedding_model=model,
                embedding_version=version,
                type_filter=str(kwargs.get("type_filter", "")),
                source_filter=str(kwargs.get("source_filter", "")),
                people_filter=str(kwargs.get("people_filter", "")),
                start_date=str(kwargs.get("start_date", "")),
                end_date=str(kwargs.get("end_date", "")),
                limit=limit,
            )
            plan = build_query_plan(query, config=rc)
            trace = {
                "plan": plan,
                "pipeline_meta": {"pipeline_version": PIPELINE_VERSION, "mode": "vector"},
                "subqueries": [query.strip()],
                "reranker": {"enabled": False, "provider": "none"},
            }
        else:
            rows, trace = self._run_hybrid_retrieval(
                query=query,
                query_vector=query_vector,
                embedding_model=model,
                embedding_version=version,
                type_filter=str(kwargs.get("type_filter", "")),
                source_filter=str(kwargs.get("source_filter", "")),
                people_filter=str(kwargs.get("people_filter", "")),
                start_date=str(kwargs.get("start_date", "")),
                end_date=str(kwargs.get("end_date", "")),
                limit=limit,
            )

        plan_obj = trace["plan"]
        pipeline_meta = trace.get("pipeline_meta", {})
        fusion_strategy = str(pipeline_meta.get("fusion_strategy", "vector" if mode == "vector" else "lexical_vector_union_with_graph_boost"))
        include_ctx = bool(rc.get("context", {}).get("include_in_result_payloads", True))
        explain_rows: list[dict[str, Any]] = []
        for row in rows:
            projection = projection_for_card_type(str(row.get("type", ""))) if row.get("type") else None
            prov_label = row.get("provenance_bias", "")
            try:
                prov_float = float(prov_label)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                prov_float = 0.0
            context = archive_context(
                card_type=str(row.get("type", "")),
                frontmatter={},
                provenance_bias=prov_float,
                typed_projection_names=(projection.table_name,) if projection else (),
            )
            ctx_json = build_context_json(
                card_type=context.card_type,
                summary=str(row.get("summary", "")),
                provenance_bias=context.provenance_bias,
                typed_projection_names=context.typed_projection_names,
            )
            breakdown = score_breakdown_for_row(row) if mode == "hybrid" else {
                "lexical_component": float(row.get("lexical_score", 0.0) or 0.0),
                "vector_component": float(row.get("vector_similarity", row.get("similarity", 0.0)) or 0.0),
                "type_prior": 0.0,
                "recency": float(row.get("recency_score", 0.0) or 0.0),
                "provenance": float(row.get("provenance_score", 0.0) or 0.0),
                "exact_boost": 0.0,
                "multi_signal_boost": 0.0,
                "graph_boost": 0.0,
                "rerank_contribution": float(row.get("rerank_contribution", 0.0) or 0.0),
            }
            matched = str(row.get("matched_by", ""))
            winning_chunk = None
            if int(row.get("chunk_index", -1) or -1) >= 0 and row.get("chunk_type"):
                winning_chunk = {"chunk_type": row.get("chunk_type"), "chunk_index": row.get("chunk_index"), "preview": row.get("preview")}
            explain_rows.append(
                {
                    "card_uid": str(row.get("card_uid", "")),
                    "rel_path": str(row.get("rel_path", "")),
                    "matched_by": [matched] if matched else [],
                    "score_components": breakdown,
                    "rerank_score": float(row.get("rerank_score", 0.0) or 0.0),
                    "final_score": float(row.get("score", 0.0) or 0.0),
                    "context": ctx_json if include_ctx else {"card_type": context.card_type},
                    "context_text": build_context_text(ctx_json) if include_ctx else "",
                    "winning_chunk": winning_chunk,
                    "why_this_ranked_here": self._why_ranked(breakdown, row),
                }
            )

        candidate_generation = {
            "lexical_candidate_count": int(pipeline_meta.get("lexical_candidate_count", 0)),
            "vector_candidate_count": int(pipeline_meta.get("vector_candidate_count", 0)),
            "graph_neighbor_count": int(pipeline_meta.get("graph_neighbor_count", 0)),
            "subqueries": trace.get("subqueries", []),
        }
        if mode == "vector":
            candidate_generation = {
                "lexical_candidate_count": 0,
                "vector_candidate_count": len(rows),
                "graph_neighbor_count": 0,
                "subqueries": trace.get("subqueries", []),
            }

        reranker_payload = trace.get("reranker")
        return retrieval_explain_payload_v2(
            pipeline_version=str(pipeline_meta.get("pipeline_version", PIPELINE_VERSION)),
            query=query,
            mode=mode,
            query_plan=self._query_plan_dict(plan_obj),
            candidate_generation=candidate_generation,
            fusion_strategy=fusion_strategy,
            results=explain_rows,
            reranker=reranker_payload,
        )

    def read_many(self, paths_or_uids: list[str]) -> dict[str, Any]:
        items = [self.read(x) for x in paths_or_uids]
        return {"items": items, "count": len(items)}

    def seed_link_surface(self) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return {"scope": sl["get_seed_scope_rows"](), "policies": sl["get_surface_policy_rows"]()}

    def seed_link_enqueue(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return sl["run_seed_link_enqueue"](
            self.index,
            modules=kwargs.get("modules"),
            source_uids=kwargs.get("source_uids"),
            job_type=str(kwargs.get("job_type", "seed_backfill") or "seed_backfill"),
            reset_existing=bool(kwargs.get("reset_existing", False)),
        )

    def seed_link_backfill(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return sl["run_seed_link_backfill"](self.index, **kwargs)

    def seed_link_refresh(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return sl["run_incremental_link_refresh"](self.index, **kwargs)

    def seed_link_worker(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return sl["run_seed_link_workers"](self.index, **kwargs)

    def seed_link_promote(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return sl["run_seed_link_promotion_workers"](self.index, **kwargs)

    def seed_link_report(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        payload = sl["run_seed_link_report"](self.index, **kwargs)
        payload["quality_gate"] = sl["compute_link_quality_gate"](self.index)
        return payload

    def link_candidates(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return {"rows": sl["list_link_candidates"](self.index, **kwargs)}

    def link_candidate(self, candidate_id: int) -> dict[str, Any] | None:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return sl["get_link_candidate_details"](self.index, candidate_id)

    def review_link_candidate(self, **kwargs) -> dict[str, Any]:
        if not get_seed_links_enabled():
            return _SEED_LINKS_DISABLED
        sl = _import_seed_links()
        return sl["review_link_candidate"](self.index, **kwargs)

    def person(self, name: str) -> dict[str, Any]:
        rel_path = self.index.person_path(name)
        if rel_path:
            path = self.vault / rel_path
            if path.exists():
                return {"found": True, "content": path.read_text(encoding="utf-8"), "rel_path": rel_path}
        match = find_note_by_slug(self.vault, name.replace(" ", "-").lower())
        return {"found": bool(match), "content": match.read_text(encoding="utf-8") if match else ""}


def get_archive_store(vault: Path | None = None, index: Any | None = None, provider_factory=None) -> DefaultArchiveStore:
    return DefaultArchiveStore(vault=vault, index=index, provider_factory=provider_factory)

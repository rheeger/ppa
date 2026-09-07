"""Archive store/service abstraction for ppa."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from archive_engine.access import (
    access_request_fields,
    card_permitted,
    filter_records,
    is_restricted,
    not_found_payload,
    policy_identity,
    record_from_frontmatter,
    resolve_access_context,
)
from archive_engine.contracts import AccessContext
from archive_vault.paths import PathEscapeError, resolve_contained_path, resolve_existing_contained
from archive_vault.vault import find_note_by_slug, parse_note_content

from .config import load_archive_config
from .contracts import ArchiveStore
from .embedding_provider import get_embedding_provider
from .explain import retrieval_explain_payload, retrieval_explain_payload_v2
from .features import archive_context, build_context_json, build_context_text
from .index_config import (
    get_default_embedding_model,
    get_default_embedding_version,
    get_query_embed_cache_path,
    get_query_embed_cache_ram_entries,
    get_rerank_top_n,
    get_seed_links_enabled,
    get_vector_dimension,
)
from .index_store import PostgresArchiveIndex
from .projections.registry import projection_for_card_type
from .query_embed_cache import QueryEmbedCache, QueryEmbedSpec
from .query_planner import build_query_plan, effective_filters_from_plan
from .query_timing import QueryPhaseTimes, add_ms, log_phase_times
from .rank_fusion import FUSION_STRATEGY
from .reranker import (
    RerankError,
    apply_http_rerank_order,
    blend_rerank_scores,
    reranker_for_config,
)
from .retrieval_pipeline import (
    PIPELINE_VERSION,
    HybridFetchInputs,
    anchor_uids_from_lexical,
    fuse_and_rank_hybrid,
    merge_lexical_rows,
    merge_vector_rows,
    score_breakdown_for_row,
)

_SEED_LINKS_DISABLED = {"error": "Seed links are not enabled. Set PPA_SEED_LINKS_ENABLED=1 to enable."}


def _import_seed_links():
    from .seed_links import (
        compute_link_quality_gate,
        get_link_candidate_details,
        get_seed_scope_rows,
        get_surface_policy_rows,
        list_link_candidates,
        review_link_candidate,
        run_incremental_link_refresh,
        run_seed_link_backfill,
        run_seed_link_enqueue,
        run_seed_link_promotion_workers,
        run_seed_link_report,
        run_seed_link_workers,
    )

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
    def __init__(
        self,
        vault: Path | None = None,
        index: Any | None = None,
        provider_factory=None,
        access: AccessContext | None = None,
    ):
        self.config = load_archive_config()
        self.vault = Path(vault or self.config.vault_path)
        self.index = index or PostgresArchiveIndex(self.vault, dsn=self.config.index_dsn)
        self.provider_factory = provider_factory or get_embedding_provider
        self._query_embed_cache = QueryEmbedCache(
            get_query_embed_cache_path(self.vault),
            ram_entries=get_query_embed_cache_ram_entries(),
        )
        self._last_phase_times: QueryPhaseTimes | None = None
        from .engine_factory import build_runtime, resolve_archive_identity, schema_binding_for

        identity = resolve_archive_identity(self.vault, schema_binding=schema_binding_for(self.config.index_schema))
        if access is None:
            self.access = resolve_access_context(identity.archive_id, identity=identity)
        elif access.archive_id == identity.archive_id:
            self.access = access
        else:
            self.access = AccessContext(
                archive_id=identity.archive_id,
                principal=access.principal,
                profile=access.profile,
                allowed_tools=access.allowed_tools,
                allowed_sources=access.allowed_sources,
                allowed_domains=access.allowed_domains,
                egress_policy_revision=access.egress_policy_revision,
                deny=access.deny,
                deny_reason=access.deny_reason,
            )
        self.runtime = build_runtime(
            vault=self.vault,
            index=self.index,
            access=self.access,
            identity=identity,
            serving_factory=self._serving if self._is_warehouse_index() else None,
            schema_binding=schema_binding_for(self.config.index_schema),
            provider_factory=self.provider_factory,
            policy_fields=self._policy_kwargs,
            authorize_rows=self._authorized_rows,
        )

    def _is_warehouse_index(self) -> bool:
        return isinstance(self.index, PostgresArchiveIndex)

    def _serving(self):
        from .serving_index import get_serving_handle

        return get_serving_handle(self.vault)

    def _try_serving_query(self):
        return self.runtime.retrieval.serving_or_none()

    def _policy_kwargs(self) -> dict[str, Any]:
        return access_request_fields(self.access)

    def _authorized_rows(self, rows: list[dict[str, Any]], *, limit: int | None = None) -> list[dict[str, Any]]:
        if not is_restricted(self.access):
            return list(rows) if limit is None else list(rows)[:limit]
        filtered = filter_records(self.access, rows)
        if limit is None:
            return filtered
        return filtered[:limit]

    def _retrieval_generation(self) -> str:
        return self.runtime.retrieval.generation()

    def drain_pending_scopes(self):
        return self.runtime.drain_pending_scopes()

    def resolve_affected(self, uid: str, revision: str):
        return self.runtime.resolve_affected(uid, revision)

    def _with_envelope(
        self,
        payload: dict[str, Any],
        *,
        query: str,
        rows_key: str = "rows",
        limit: int | None = None,
        method: str = "",
    ) -> dict[str, Any]:
        from archive_cli.commands.confidence import attach_retrieval_envelope

        return attach_retrieval_envelope(
            payload,
            query=query,
            rows_key=rows_key,
            limit=limit,
            method=method,
            pipeline_version=PIPELINE_VERSION,
            generation=self._retrieval_generation(),
        )

    def bootstrap(self) -> dict[str, Any]:
        return self.runtime.warehouse.bootstrap()

    def rebuild(self, **kwargs) -> dict[str, Any]:
        return self.runtime.rebuild(**kwargs)

    def close(self) -> None:
        self.runtime.close()

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
        try:
            from .serving_index import serving_index_status

            payload.update(serving_index_status(self.vault))
        except Exception:
            payload.setdefault("serving_index_generation", "")
            payload.setdefault("serving_index_dirty_records", 0)
        payload["query_embed_cache_rows"] = self._query_embed_cache.stats().get("rows", 0)
        payload["query_embed_cache_hits"] = self._query_embed_cache.stats().get("hits", 0)
        payload["query_embed_cache_misses"] = self._query_embed_cache.stats().get("misses", 0)
        return payload

    def _contained_text(self, user_path: str) -> tuple[str | None, str]:
        """Read a vault-relative path. Escapes and missing files return ``(None, "")``."""

        try:
            path = resolve_contained_path(self.vault, user_path, purpose="read")
            rel = str(path.relative_to(Path(self.vault).resolve()))
        except (PathEscapeError, ValueError):
            return None, ""
        if not path.is_file():
            return None, rel
        return path.read_text(encoding="utf-8"), rel

    def _exact_read_service(self):
        return self.runtime.exact_read

    def _record_for_read(self, path_or_uid: str, payload: dict[str, Any]) -> dict[str, Any]:
        content = str(payload.get("content") or "")
        if content:
            try:
                frontmatter, _body, _prov = parse_note_content(content)
            except Exception:
                frontmatter = {}
            return record_from_frontmatter(frontmatter)
        return {"type": "", "sources": [], "required_sources": [], "lineage_complete": None}

    def read(self, path_or_uid: str) -> dict[str, Any]:
        payload = self.runtime.read(path_or_uid, access=self.access)
        if not payload.get("found"):
            return payload
        if not card_permitted(self.access, self._record_for_read(path_or_uid, payload)):
            return not_found_payload(path_or_uid, include_rel=str(path_or_uid).endswith(".md"))
        return payload

    def query(
        self,
        *,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        org_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 20,
    ) -> dict[str, Any]:
        fetch_limit = limit * 8 if is_restricted(self.access) else limit
        return self._with_envelope(
            self.runtime.query(
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                org_filter=org_filter,
                start_date=start_date,
                end_date=end_date,
                limit=fetch_limit,
                authorize_limit=limit,
            ),
            query=f"query:{type_filter}",
            limit=limit,
            method="query",
        )

    def search(self, query: str, *, limit: int = 20, **kwargs: Any) -> dict[str, Any]:
        fetch_limit = limit * 8 if is_restricted(self.access) else limit
        return self._with_envelope(
            self.runtime.search(query, limit=limit, fetch_limit=fetch_limit, **kwargs),
            query=query,
            limit=limit,
        )

    def card_stack_pointers(self, uids: list[str]) -> dict[str, dict[str, Any]]:
        raw = self.runtime.retrieval.pointers(uids)
        if not is_restricted(self.access):
            return raw
        return {uid: pointers for uid, pointers in (raw or {}).items() if uid}

    def evidence(
        self,
        *,
        query: str = "",
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
        start_date: str = "",
        end_date: str = "",
        limit: int = 12,
    ) -> dict[str, Any]:
        """Compact chronological evidence via existing search/query internals."""
        from archive_cli.card_traversal import clamp_evidence_limit, compact_hits, row_uid

        cap = clamp_evidence_limit(limit)
        cleaned = query.strip()
        if cleaned:
            rows = self.runtime.retrieval.search(
                cleaned,
                limit=cap,
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            result = self.query(
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                start_date=start_date,
                end_date=end_date,
                limit=cap,
            )
            rows = list(result.get("rows") or [])
        rows = self._authorized_rows(list(rows or []), limit=cap)
        uids = [row_uid(row) for row in rows if row_uid(row)]
        pointers = self.card_stack_pointers(uids) if uids else {}
        hits = compact_hits(rows, pointers_by_uid=pointers, question=cleaned, chronological=True)
        return self._with_envelope(
            {"hits": hits, "query": cleaned, "limit": cap},
            query=cleaned,
            rows_key="hits",
            limit=cap,
            method="evidence",
        )

    def graph(self, note_path: str, *, hops: int = 2) -> dict[str, Any]:
        rel_path = note_path if note_path.endswith(".md") else f"{note_path}.md"
        graph = self.runtime.graph(rel_path, hops=hops)
        if is_restricted(self.access) and isinstance(graph, dict):
            graph = {
                node: [
                    edge
                    for edge in (targets or [])
                    if card_permitted(self.access, edge if isinstance(edge, dict) else {"sources": []})
                ]
                for node, targets in graph.items()
            }
        return {"graph": graph, "rel_path": rel_path}

    def timeline(self, *, start_date: str = "", end_date: str = "", limit: int = 20) -> dict[str, Any]:
        fetch_limit = limit * 8 if is_restricted(self.access) else limit
        return {
            "rows": self.runtime.retrieval.timeline(
                start_date=start_date,
                end_date=end_date,
                limit=fetch_limit,
            )[:limit]
        }

    def temporal_neighbors(
        self,
        timestamp: str,
        *,
        direction: str = "both",
        limit: int = 20,
        type_filter: str = "",
        source_filter: str = "",
        people_filter: str = "",
    ) -> dict[str, Any]:
        serving = self._try_serving_query()
        if serving is not None:
            return serving.temporal_neighbors(
                timestamp,
                direction=direction,
                limit=limit,
                type_filter=type_filter,
                source_filter=source_filter,
                people_filter=people_filter,
                **self._policy_kwargs(),
            )
        result = self.index.temporal_neighbors(
            timestamp,
            direction=direction,
            limit=limit * 8 if is_restricted(self.access) else limit,
            type_filter=type_filter,
            source_filter=source_filter,
            people_filter=people_filter,
        )
        if isinstance(result, dict) and "results" in result:
            result = dict(result)
            result["results"] = self._authorized_rows(list(result.get("results") or []), limit=limit)
            result["count"] = len(result["results"])
        return result

    def knowledge_for_domain(
        self,
        domain: str,
        *,
        fallback_query: str = "",
        limit: int = 5,
    ) -> dict[str, Any]:
        return self.index.knowledge_for_domain(domain, fallback_query=fallback_query, limit=limit)

    def _embed_query(self, query: str, *, model: str, version: int, phases: QueryPhaseTimes) -> list[float]:
        provider = self.provider_factory(model=model)
        spec = QueryEmbedSpec(
            model=model,
            version=version,
            provider=str(getattr(provider, "name", "unknown")),
            dimension=int(getattr(provider, "dimension", 0) or get_vector_dimension()),
            policy_identity=policy_identity(self.access),
        )
        t0 = time.monotonic()
        cached = self._query_embed_cache.get(query, spec)
        if cached is not None:
            phases.embed_cache_hit = True
            add_ms(phases, "embed_ms", t0)
            return cached
        vector = provider.embed_texts([query.strip() or ""])[0]
        spec = QueryEmbedSpec(
            model=spec.model,
            version=spec.version,
            provider=spec.provider,
            dimension=len(vector),
            policy_identity=policy_identity(self.access),
        )
        self._query_embed_cache.put(query, spec, vector)
        phases.embed_cache_hit = False
        add_ms(phases, "embed_ms", t0)
        return vector

    def vector_search(self, query: str, **kwargs) -> dict[str, Any]:
        t_total = time.monotonic()
        phases = QueryPhaseTimes()
        model = kwargs.get("embedding_model", "") or get_default_embedding_model()
        version = kwargs.get("embedding_version", 0) or get_default_embedding_version()
        query_vector = self._embed_query(query, model=model, version=version, phases=phases)
        serving = self._try_serving_query()
        if serving is not None:
            rows = serving.vector(query_vector, **kwargs, **self._policy_kwargs())
            add_ms(phases, "total_ms", t_total)
            self._last_phase_times = phases
            log_phase_times("vector_search", phases)
            return self._with_envelope(
                {
                    "rows": rows,
                    "embedding_model": model,
                    "embedding_version": version,
                    "phase_times": phases.to_dict(),
                },
                query=query,
                limit=int(kwargs.get("limit", 20) or 20),
                method="vector",
            )
        fetch_limit = int(kwargs.get("limit", 20) or 20)
        if is_restricted(self.access):
            fetch_limit = fetch_limit * 8
        rows = self.index.vector_search(
            query_vector=query_vector,
            embedding_model=model,
            embedding_version=version,
            type_filter=str(kwargs.get("type_filter", "")),
            source_filter=str(kwargs.get("source_filter", "")),
            people_filter=str(kwargs.get("people_filter", "")),
            start_date=str(kwargs.get("start_date", "")),
            end_date=str(kwargs.get("end_date", "")),
            limit=fetch_limit,
        )
        return self._with_envelope(
            {
                "rows": self._authorized_rows(rows, limit=int(kwargs.get("limit", 20) or 20)),
                "embedding_model": model,
                "embedding_version": version,
            },
            query=query,
            limit=int(kwargs.get("limit", 20) or 20),
            method="vector",
        )

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
        graph_edge_type_filter: str,
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
        gtypes = [p.strip() for p in (graph_edge_type_filter or "").split(",") if p.strip()]
        neighbor_trust = self.index.fetch_graph_neighbors_for_uids(
            anchors,
            edge_type_filter=gtypes if gtypes else None,
        )
        pipeline_meta: dict[str, Any] = {}
        rr_cfg = rc.get("reranker", {})
        provider = str(rr_cfg.get("provider", "none")).strip().lower()
        pool_limit = limit
        if rr_cfg.get("enabled") and provider not in ("none", "", "noop"):
            pool_limit = max(limit, int(rr_cfg.get("top_k") or get_rerank_top_n() or 30))
        rows = fuse_and_rank_hybrid(
            HybridFetchInputs(
                lexical_rows=merged_lex,
                vector_rows=merged_vec,
                neighbor_trust=neighbor_trust,
                query_cleaned=query.strip(),
                subqueries_used=tuple(subqs),
            ),
            final_limit=pool_limit,
            pipeline_meta=pipeline_meta,
        )
        rerank_note: dict[str, Any] = {
            "provider": provider or "none",
            "enabled": bool(rr_cfg.get("enabled")),
            "degraded": False,
            "used": "rrf",
        }
        if rr_cfg.get("enabled") and provider not in ("none", "", "noop"):
            top_k = min(int(rr_cfg.get("top_k") or get_rerank_top_n() or 30), len(rows))
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
            try:
                reranker = reranker_for_config(rc)
                rr_list = reranker.rerank(query, head)
                by_uid = {item.card_uid: item for item in rr_list}
                preserve = bool(rr_cfg.get("preserve_exact_match_floor", True))
                if provider == "heuristic":
                    blend_cfg = rr_cfg.get("blend") or {}
                    head = blend_rerank_scores(
                        head,
                        by_uid,
                        top_1_3_retrieval_weight=float(blend_cfg.get("top_1_3_retrieval_weight", 0.75)),
                        top_4_10_retrieval_weight=float(blend_cfg.get("top_4_10_retrieval_weight", 0.60)),
                        rest_retrieval_weight=float(blend_cfg.get("rest_retrieval_weight", 0.40)),
                        preserve_exact_match_floor=preserve,
                    )
                    rerank_note["used"] = "heuristic"
                else:
                    head = apply_http_rerank_order(head, by_uid, preserve_exact_match_floor=preserve)
                    rerank_note["used"] = "http"
                    if rr_list:
                        rerank_note["model_revision"] = rr_list[0].model_revision
                rows = head + rows[top_k:]
            except RerankError as exc:
                rerank_note["degraded"] = True
                rerank_note["reason"] = str(exc)
                rerank_note["used"] = "rrf"
                if bool(rr_cfg.get("fail_closed")):
                    raise
        rows = rows[:limit]
        return rows, {
            "plan": plan,
            "pipeline_meta": pipeline_meta,
            "subqueries": subqs,
            "reranker": rerank_note,
        }

    def hybrid_search(self, query: str, **kwargs) -> dict[str, Any]:
        t_total = time.monotonic()
        phases = QueryPhaseTimes()
        model = kwargs.get("embedding_model", "") or get_default_embedding_model()
        version = kwargs.get("embedding_version", 0) or get_default_embedding_version()
        query_vector = self._embed_query(query, model=model, version=version, phases=phases)
        serving = self._try_serving_query()
        if serving is not None:
            rows = serving.hybrid(query, query_vector, **kwargs, **self._policy_kwargs())
            add_ms(phases, "total_ms", t_total)
            self._last_phase_times = phases
            log_phase_times("hybrid_search", phases)
            return self._with_envelope(
                {
                    "rows": rows,
                    "embedding_model": model,
                    "embedding_version": version,
                    "phase_times": phases.to_dict(),
                },
                query=query,
                limit=int(kwargs.get("limit", 20) or 20),
                method="hybrid",
            )
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
            graph_edge_type_filter=str(kwargs.get("graph_edge_type_filter", "")),
            limit=int(kwargs.get("limit", 20) or 20),
        )
        add_ms(phases, "total_ms", t_total)
        self._last_phase_times = phases
        log_phase_times("hybrid_search", phases)
        return self._with_envelope(
            {
                "rows": self._authorized_rows(rows, limit=int(kwargs.get("limit", 20) or 20)),
                "embedding_model": model,
                "embedding_version": version,
                "phase_times": phases.to_dict(),
            },
            query=query,
            limit=int(kwargs.get("limit", 20) or 20),
            method="hybrid",
        )

    def embedding_status(self, *, embedding_model: str = "", embedding_version: int = 0) -> dict[str, Any]:
        model = embedding_model or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        return self.index.embedding_status(embedding_model=model, embedding_version=version)

    def embedding_backlog(
        self, *, limit: int = 20, embedding_model: str = "", embedding_version: int = 0
    ) -> dict[str, Any]:
        model = embedding_model or get_default_embedding_model()
        version = embedding_version or get_default_embedding_version()
        return {
            "rows": self.index.embedding_backlog(limit=limit, embedding_model=model, embedding_version=version),
            "embedding_model": model,
            "embedding_version": version,
        }

    def embed_pending(
        self,
        *,
        limit: int = 0,
        embedding_model: str = "",
        embedding_version: int = 0,
        copy_from_schema: str = "",
        uid_allowlist: set[str] | list[str] | tuple[str, ...] | None = None,
        chunk_key_allowlist: set[str] | list[str] | tuple[str, ...] | None = None,
        embedding_spec: Any | None = None,
        unscoped: bool = False,
    ) -> dict[str, Any]:
        from archive_engine.contracts import EmbeddingSpec

        from .embedder import current_chunk_schema_id, normalize_embed_allowlist, require_embed_selection

        uid_allowlist = normalize_embed_allowlist(uid_allowlist)
        chunk_key_allowlist = normalize_embed_allowlist(chunk_key_allowlist)
        require_embed_selection(
            uid_allowlist=uid_allowlist,
            chunk_key_allowlist=chunk_key_allowlist,
            unscoped=unscoped,
        )
        spec = embedding_spec
        if spec is not None and not isinstance(spec, EmbeddingSpec):
            spec = EmbeddingSpec.from_payload(spec)
        if spec is not None:
            model = spec.model
            try:
                version = int(spec.model_revision)
            except ValueError:
                version = embedding_version or get_default_embedding_version()
        else:
            model = embedding_model or get_default_embedding_model()
            version = embedding_version or get_default_embedding_version()
            spec = EmbeddingSpec(
                provider_namespace=str(getattr(self.provider_factory, "name", "") or "hash"),
                model=model,
                model_revision=str(version),
                dimension=get_vector_dimension(),
                metric="cosine",
                normalization="none",
                chunk_schema=current_chunk_schema_id(),
            )
        copy_result: dict[str, Any] | None = None
        if copy_from_schema:
            copy_result = dict(
                self.index.copy_embeddings_from_schema(
                    source_schema=copy_from_schema,
                    embedding_model=model,
                    embedding_version=version,
                )
            )
        provider = self.provider_factory(model=model)
        ctx = self.config.retrieval.get("context", {})
        include_ctx = bool(ctx.get("include_in_embeddings", True))
        embed_result = dict(
            self.index.embed_pending(
                provider=provider,
                embedding_model=model,
                embedding_version=version,
                limit=limit,
                include_context_prefix=include_ctx,
                uid_allowlist=uid_allowlist,
                chunk_key_allowlist=chunk_key_allowlist,
                embedding_spec=spec,
                unscoped=unscoped,
            )
        )
        if copy_result is not None:
            embed_result["copy_from_schema"] = copy_result
        if self._is_warehouse_index():
            try:
                from archive_engine.changes import emit_embed_completion, request_reconciliation

                from .serving_index import mark_serving_index_dirty

                card_uids = [str(uid).strip() for uid in (embed_result.get("card_uids") or []) if str(uid).strip()]
                if card_uids:
                    emit_embed_completion(self.vault, card_uids, source="embed_pending")
                else:
                    request_reconciliation(self.vault, reason="embed_pending")
                mark_serving_index_dirty(self.vault, "embed_pending", card_uids)
            except Exception:
                pass
        return embed_result

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
                        "context": {
                            "card_type": context.card_type,
                            "typed_projection_names": list(context.typed_projection_names),
                        },
                    }
                )
            slim_payload = retrieval_explain_payload(query, mode, slim)
            return self._with_envelope(
                slim_payload,
                query=query,
                limit=int(kwargs.get("limit", 20) or 20),
                method=mode,
            )

        model = kwargs.get("embedding_model", "") or get_default_embedding_model()
        version = kwargs.get("embedding_version", 0) or get_default_embedding_version()
        limit = int(kwargs.get("limit", 20) or 20)
        trace: dict[str, Any] = {}
        if self._is_warehouse_index():
            result = self.vector_search(query, **kwargs) if mode == "vector" else self.hybrid_search(query, **kwargs)
            rows = list(result.get("rows") or [])
            plan = build_query_plan(query, config=rc)
            trace = {
                "plan": plan,
                "pipeline_meta": {"pipeline_version": PIPELINE_VERSION, "mode": mode, "engine": "serving_index"},
                "subqueries": [query.strip()],
                "reranker": {"enabled": False, "provider": "none"},
            }
        elif mode == "vector":
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
            provider = self.provider_factory(model=model)
            query_vector = provider.embed_texts([query.strip() or ""])[0]
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
                graph_edge_type_filter=str(kwargs.get("graph_edge_type_filter", "")),
                limit=limit,
            )

        plan_obj = trace["plan"]
        pipeline_meta = trace.get("pipeline_meta", {})
        fusion_strategy = str(pipeline_meta.get("fusion_strategy", "vector" if mode == "vector" else FUSION_STRATEGY))
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
            breakdown = (
                score_breakdown_for_row(row)
                if mode == "hybrid"
                else {
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
            )
            matched = str(row.get("matched_by", ""))
            winning_chunk = None
            if int(row.get("chunk_index", -1) or -1) >= 0 and row.get("chunk_type"):
                winning_chunk = {
                    "chunk_type": row.get("chunk_type"),
                    "chunk_index": row.get("chunk_index"),
                    "preview": row.get("preview"),
                }
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
        payload = retrieval_explain_payload_v2(
            pipeline_version=str(pipeline_meta.get("pipeline_version", PIPELINE_VERSION)),
            query=query,
            mode=mode,
            query_plan=self._query_plan_dict(plan_obj),
            candidate_generation=candidate_generation,
            fusion_strategy=fusion_strategy,
            results=explain_rows,
            reranker=reranker_payload,
        )
        return self._with_envelope(
            payload,
            query=query,
            rows_key="results",
            limit=limit,
            method=mode,
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
        serving = self._try_serving_query()
        if serving is not None:
            hit = serving.person(name, **self._policy_kwargs())
            if hit:
                rel_path = str(hit.get("rel_path") or "")
                if rel_path:
                    content, rel = self._contained_text(rel_path)
                    if content is not None:
                        payload = {"found": True, "content": content, "rel_path": rel or rel_path}
                        if not card_permitted(self.access, self._record_for_read(rel or rel_path, payload)):
                            return {"found": False, "content": ""}
                        return payload
                    return {"found": False, "content": ""}
                return {"found": bool(hit.get("found")), "content": str(hit.get("content") or ""), **hit}
        rel_path = self.index.person_path(name)
        if rel_path:
            content, rel = self._contained_text(str(rel_path))
            if content is not None:
                payload = {"found": True, "content": content, "rel_path": rel or str(rel_path)}
                if not card_permitted(self.access, self._record_for_read(rel or str(rel_path), payload)):
                    return {"found": False, "content": ""}
                return payload
        match = find_note_by_slug(self.vault, name.replace(" ", "-").lower())
        if match is None:
            return {"found": False, "content": ""}
        try:
            contained = resolve_existing_contained(self.vault, match)
        except PathEscapeError:
            return {"found": False, "content": ""}
        if not contained.is_file():
            return {"found": False, "content": ""}
        content = contained.read_text(encoding="utf-8")
        payload = {"found": True, "content": content}
        if not card_permitted(self.access, self._record_for_read(str(match), payload)):
            return {"found": False, "content": ""}
        return payload


def get_archive_store(
    vault: Path | None = None,
    index: Any | None = None,
    provider_factory=None,
    access: AccessContext | None = None,
) -> DefaultArchiveStore:
    return DefaultArchiveStore(vault=vault, index=index, provider_factory=provider_factory, access=access)

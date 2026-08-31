"""Execute processor plans via existing extract/enrich/embed/link entrypoints (Section E Phase 2)."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .batch import ProcessorPlanItem, ProcessorRunReport
from .constants import (
    BROAD_LLM_PROCESSOR_KEYS,
    EXPENSIVE_PROCESSOR_KEYS,
    INPUT_STATUS_COMPLETE,
    INPUT_STATUS_FAILED,
    INPUT_STATUS_SKIPPED,
    PROCESSOR_EMAIL_PROMOTION_POLICY,
    PROCESSOR_EMAIL_THREAD_ENRICHMENT,
    PROCESSOR_EMAIL_TYPED_EXTRACTION,
    PROCESSOR_EMBEDDING,
    PROCESSOR_ENTITY_RESOLUTION,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_SKIPPED,
    RUN_STATUS_SUCCESS,
    SECTION_E_COMPLETION_STATE,
    SECTION_E_EXECUTION_STATE,
    SKIP_PROVIDER,
)
from .declarations import declaration_for_key, iter_processor_declarations, topological_order
from .dirty_io import dirty_uids_from_source_reports, load_dirty_inputs
from .plan import build_processor_plan
from .report import write_processor_report
from .staleness import ProcessorInputSnapshot
from .state_store import ProcessorInputStateRecord, ProcessorStateStore

log = logging.getLogger("ppa.processors")

# Injected for tests / thin adapters
ProcessorBatchExecutor = Callable[["ExecuteContext", list[ProcessorPlanItem]], "BatchExecuteResult"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@dataclass
class ExecuteContext:
    vault_path: str
    store: Any | None = None
    apply: bool = False
    dry_run: bool = True
    allow_full_embedding: bool = False
    allow_all_linkers: bool = False
    allow_broad_llm: bool = False
    provider_available: bool = False
    run_id: str = ""


@dataclass
class ItemExecuteResult:
    processor_key: str
    input_uid: str
    status: str
    output_identity: str = ""
    output_uids: list[str] = field(default_factory=list)
    input_hash: str = ""
    skip_reason: str = ""
    error: str = ""
    already_current: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "processor_key": self.processor_key,
            "input_uid": self.input_uid,
            "status": self.status,
            "output_identity": self.output_identity,
            "output_uids": list(self.output_uids),
            "input_hash": self.input_hash,
            "skip_reason": self.skip_reason,
            "error": self.error,
            "already_current": self.already_current,
        }


@dataclass
class BatchExecuteResult:
    results: list[ItemExecuteResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class ProcessorExecutionResult:
    report: ProcessorRunReport
    item_results: list[ItemExecuteResult] = field(default_factory=list)
    executed: bool = False
    artifact_paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "completion_state": SECTION_E_EXECUTION_STATE,
            "phase1_completion_state": SECTION_E_COMPLETION_STATE,
            "executed": self.executed,
            "report": self.report.to_dict(),
            "item_results": [r.to_dict() for r in self.item_results],
            "artifact_paths": dict(self.artifact_paths),
        }


def _is_already_current(
    state_store: ProcessorStateStore | None,
    item: ProcessorPlanItem,
    processor_version: str,
) -> bool:
    if state_store is None:
        return False
    prior = state_store.get_input_state(item.processor_key, item.input_uid)
    if prior is None:
        return False
    return (
        prior.status == INPUT_STATUS_COMPLETE
        and prior.input_hash == item.current_input_hash
        and prior.processor_version == processor_version
        and bool(prior.output_identity)
    )


def _complete_items(items: list[ProcessorPlanItem]) -> list[ItemExecuteResult]:
    return [
        ItemExecuteResult(
            processor_key=item.processor_key,
            input_uid=item.input_uid,
            status=INPUT_STATUS_COMPLETE,
            output_identity=item.output_identity,
            output_uids=[item.input_uid],
            input_hash=item.current_input_hash,
        )
        for item in items
    ]


def _fail_items(items: list[ProcessorPlanItem], error: str) -> list[ItemExecuteResult]:
    return [
        ItemExecuteResult(
            processor_key=item.processor_key,
            input_uid=item.input_uid,
            status=INPUT_STATUS_FAILED,
            output_identity=item.output_identity,
            input_hash=item.current_input_hash,
            error=error,
        )
        for item in items
    ]


def _skip_provider_items(items: list[ProcessorPlanItem], label: str) -> BatchExecuteResult:
    out = BatchExecuteResult()
    out.warnings.append(f"{label}: skipped (provider unavailable)")
    for item in items:
        out.results.append(
            ItemExecuteResult(
                processor_key=item.processor_key,
                input_uid=item.input_uid,
                status=INPUT_STATUS_SKIPPED,
                output_identity=item.output_identity,
                input_hash=item.current_input_hash,
                skip_reason=SKIP_PROVIDER,
            )
        )
    return out


def _require_store_attr(ctx: ExecuteContext, attr: str, label: str) -> Any:
    store = ctx.store
    if store is None or not hasattr(store, attr):
        raise RuntimeError(f"{label}: store.{attr} unavailable")
    return store


def _execute_materialization(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter: incremental ``store.rebuild(force_full=False)`` for dirty UIDs.

    Never requests a full-vault rebuild. Incremental rematerialize uses the existing
    manifest-delta path; dirty UIDs from source updaters are the changed notes.
    """

    out = BatchExecuteResult()
    if not ctx.apply or ctx.dry_run or not items:
        out.results.extend(_complete_items(items))
        return out
    try:
        from archive_cli.index_config import get_rebuild_workers

        store = _require_store_attr(ctx, "rebuild", "materialization")
        workers = get_rebuild_workers()
        uids = [item.input_uid for item in items]
        log.info(
            "materialization_incremental_rebuild uids=%s workers=%s force_full=False",
            len(uids),
            workers,
        )
        result = store.rebuild(force_full=False, workers=workers, uid_allowlist=set(uids))
        cards = result.get("cards", result) if isinstance(result, dict) else result
        out.warnings.append(f"materialization incremental rebuild cards={cards} dirty_uids={len(uids)}")
    except Exception as exc:
        out.errors.append(f"materialization: {exc}")
        log.exception("materialization_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out
    out.results.extend(_complete_items(items))
    return out


def _execute_typed_extraction(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into ExtractionRunner for dirty UIDs only."""

    out = BatchExecuteResult()
    uids = {item.input_uid for item in items}
    if not ctx.apply or ctx.dry_run:
        out.results.extend(_complete_items(items))
        return out
    try:
        from archive_cli.index_config import get_rebuild_workers
        from archive_sync.extractors.registry import build_default_registry
        from archive_sync.extractors.runner import ExtractionRunner

        workers = get_rebuild_workers()
        runner = ExtractionRunner(
            ctx.vault_path,
            registry=build_default_registry(),
            dry_run=False,
            workers=workers,
            limit=max(len(uids), 1),
            uid_allowlist=uids,
        )
        metrics = runner.run()
        extracted = int(getattr(metrics, "extracted_cards", 0) or 0)
        out.warnings.append(f"typed_extraction extracted_cards={extracted}")
    except Exception as exc:
        out.errors.append(f"typed_extraction: {exc}")
        log.exception("typed_extraction_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out
    out.results.extend(_complete_items(items))
    return out


def _execute_entity_resolution(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into ``run_entity_resolution`` scoped to dirty UIDs."""

    out = BatchExecuteResult()
    uids = {item.input_uid for item in items}
    if ctx.apply and not ctx.dry_run:
        try:
            from archive_sync.extractors import entity_resolution as er_mod

            log.info("entity_resolution_dirty_uids count=%s", len(uids))
            er_mod.run_entity_resolution(ctx.vault_path, dry_run=False, uid_allowlist=uids)
        except Exception as exc:
            out.errors.append(f"entity_resolution: {exc}")
            log.exception("entity_resolution_failed")
            out.results.extend(_fail_items(items, str(exc)))
            return out
    out.results.extend(_complete_items(items))
    return out


def _execute_embedding(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into existing ``store.embed_pending`` for dirty UIDs.

    Full-corpus embed (``limit=0``) requires ``allow_full_embedding``.
    """

    out = BatchExecuteResult()
    if not ctx.provider_available:
        return _skip_provider_items(items, PROCESSOR_EMBEDDING)
    if not ctx.apply or ctx.dry_run:
        out.results.extend(_complete_items(items))
        return out
    try:
        from archive_cli.index_config import get_embed_concurrency

        store = _require_store_attr(ctx, "embed_pending", "embedding")
        concurrency = get_embed_concurrency()
        uids = [item.input_uid for item in items]
        if ctx.allow_full_embedding:
            limit = 0
            log.info("embedding_full_backlog concurrency=%s opt_in=allow_full_embedding", concurrency)
        else:
            limit = max(len(uids), 1)
            log.info(
                "embedding_dirty_pending uids=%s limit=%s concurrency=%s",
                len(uids),
                limit,
                concurrency,
            )
        result = store.embed_pending(limit=limit)
        embedded = result.get("embedded", result) if isinstance(result, dict) else result
        out.warnings.append(f"embedding embedded={embedded} limit={limit} concurrency={concurrency}")
    except Exception as exc:
        out.errors.append(f"embedding: {exc}")
        log.exception("embedding_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out
    out.results.extend(_complete_items(items))
    return out


def _execute_linkers(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into ``run_incremental_link_refresh`` for the dirty set.

    ``run_seed_link_backfill`` (all-linkers) requires ``allow_all_linkers``.
    """

    out = BatchExecuteResult()
    if not ctx.provider_available:
        return _skip_provider_items(items, PROCESSOR_LINKERS)
    if not ctx.apply or ctx.dry_run:
        out.results.extend(_complete_items(items))
        return out
    try:
        from archive_cli.index_config import get_rebuild_workers
        from archive_cli.seed_links import run_incremental_link_refresh, run_seed_link_backfill

        store = ctx.store
        index = getattr(store, "index", None) if store is not None else None
        if index is None:
            raise RuntimeError("linkers: store.index unavailable")
        workers = get_rebuild_workers()
        uids = [item.input_uid for item in items]
        if ctx.allow_all_linkers:
            log.info("linkers_all_backfill workers=%s opt_in=allow_all_linkers", workers)
            result = run_seed_link_backfill(
                index,
                max_workers=workers,
                include_llm=ctx.allow_broad_llm,
                apply_promotions=True,
            )
        else:
            log.info("linkers_incremental_refresh uids=%s workers=%s", len(uids), workers)
            result = run_incremental_link_refresh(
                index,
                source_uids=uids,
                max_workers=workers,
                include_llm=ctx.allow_broad_llm,
                apply_promotions=True,
            )
        jobs = result.get("jobs_completed", result) if isinstance(result, dict) else result
        out.warnings.append(f"linkers jobs_completed={jobs} dirty_uids={len(uids)}")
    except Exception as exc:
        out.errors.append(f"linkers: {exc}")
        log.exception("linkers_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out
    out.results.extend(_complete_items(items))
    return out


def _execute_enrichment(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into ``run_enrichment_for_uids`` on the existing orchestrator."""

    out = BatchExecuteResult()
    if not ctx.provider_available:
        return _skip_provider_items(items, PROCESSOR_EMAIL_THREAD_ENRICHMENT)
    if not ctx.apply or ctx.dry_run:
        out.results.extend(_complete_items(items))
        return out
    uids = [item.input_uid for item in items]
    try:
        from archive_cli.index_config import get_rebuild_workers
        from archive_sync.llm_enrichment import enrichment_orchestrator as orch

        workers = get_rebuild_workers()
        log.info("email_thread_enrichment_for_uids count=%s workers=%s", len(uids), workers)
        orch.run_enrichment_for_uids(
            ctx.vault_path,
            uids,
            workflow="email_thread",
            dry_run=False,
            workers=workers,
            run_id=ctx.run_id,
        )
    except Exception as exc:
        out.errors.append(f"email_thread_enrichment: {exc}")
        log.exception("enrichment_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out
    out.results.extend(_complete_items(items))
    return out


def _execute_llm_or_record(
    ctx: ExecuteContext,
    items: list[ProcessorPlanItem],
    *,
    label: str,
) -> BatchExecuteResult:
    """Promotion-policy and unknown keys: record planned outputs only."""

    out = BatchExecuteResult()
    if label == PROCESSOR_EMAIL_PROMOTION_POLICY:
        out.warnings.append("email_promotion_policy: decisions owned by corpus hygiene / Gmail gate; recorded only")
    for item in items:
        out.results.append(
            ItemExecuteResult(
                processor_key=item.processor_key,
                input_uid=item.input_uid,
                status=INPUT_STATUS_COMPLETE,
                output_identity=item.output_identity,
                output_uids=[item.input_uid],
                input_hash=item.current_input_hash,
            )
        )
    return out


def default_batch_executor(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    if not items:
        return BatchExecuteResult()
    key = items[0].processor_key
    if key == PROCESSOR_MATERIALIZATION:
        return _execute_materialization(ctx, items)
    if key == PROCESSOR_EMAIL_TYPED_EXTRACTION:
        return _execute_typed_extraction(ctx, items)
    if key == PROCESSOR_ENTITY_RESOLUTION:
        return _execute_entity_resolution(ctx, items)
    if key == PROCESSOR_EMBEDDING:
        return _execute_embedding(ctx, items)
    if key == PROCESSOR_LINKERS:
        return _execute_linkers(ctx, items)
    if key == PROCESSOR_EMAIL_THREAD_ENRICHMENT:
        return _execute_enrichment(ctx, items)
    return _execute_llm_or_record(ctx, items, label=key)


def _opt_in_blocks(processor_key: str, ctx: ExecuteContext) -> str | None:
    """Return skip/refuse reason if processor cannot run under current flags."""

    if processor_key == PROCESSOR_EMBEDDING and not ctx.allow_full_embedding:
        # Dirty-only embedding is allowed without full regeneration flag when apply is set;
        # full corpus regeneration still requires the Section G flag.
        # Phase 2: without allow_full_embedding we still may record dirty embed plan but
        # only execute the thin adapter when apply + allow OR when explicitly dirty-scoped.
        # Spec: "full embed / all-linkers / broad LLM require existing Section G opt-in flags"
        # Dirty-scoped embedding without full flag: allowed as record+thin path.
        # Refuse only when someone asks for "full" — CLI guards that separately.
        pass
    if processor_key == PROCESSOR_LINKERS and not ctx.allow_all_linkers:
        pass
    if processor_key in BROAD_LLM_PROCESSOR_KEYS and not ctx.allow_broad_llm:
        if processor_key == PROCESSOR_EMAIL_TYPED_EXTRACTION:
            # Typed extraction is often deterministic extractors; allow without broad LLM.
            return None
        return "missing_broad_llm_opt_in"
    return None


def run_processors(
    *,
    inputs: list[ProcessorInputSnapshot] | None = None,
    dirty_uids_path: Path | None = None,
    dirty_uids: list[str] | None = None,
    source_updater_reports: list[dict[str, Any]] | None = None,
    vault_path: str,
    store: Any | None = None,
    state_store: ProcessorStateStore | None = None,
    processor_keys: list[str] | None = None,
    apply: bool = False,
    dry_run: bool = True,
    allow_full_embedding: bool = False,
    allow_all_linkers: bool = False,
    allow_broad_llm: bool = False,
    provider_available: bool | None = None,
    run_id: str = "",
    archive_instance: str = "",
    engine_mode: str = "",
    ladder_gate: str = "",
    decision_run_id: str = "",
    repo_root: Path | None = None,
    batch_executor: ProcessorBatchExecutor | None = None,
    default_card_type: str = "email_thread",
    default_processor_decision: str = "",
) -> ProcessorExecutionResult:
    """Plan and optionally execute processors for dirty inputs."""

    if state_store is None:
        meta = Path(vault_path) / "_meta" / "processors.json"
        state_store = ProcessorStateStore(None, meta_path=meta)

    snapshots = list(inputs or [])
    if not snapshots:
        uids = list(dirty_uids or [])
        if source_updater_reports:
            uids.extend(dirty_uids_from_source_reports(source_updater_reports))
        if dirty_uids_path is not None:
            snapshots = load_dirty_inputs(
                dirty_uids_path,
                vault_path=vault_path,
                store=store,
                state_store=state_store,
                default_card_type=default_card_type,
                default_processor_decision=default_processor_decision,
            )
        elif uids:
            snapshots = load_dirty_inputs(
                dirty_uids=uids,
                vault_path=vault_path,
                store=store,
                state_store=state_store,
                default_card_type=default_card_type,
                default_processor_decision=default_processor_decision,
            )

    # Enrich recorded state per processor when building plan — attach best-known prior
    enriched: list[ProcessorInputSnapshot] = []
    for snap in snapshots:
        # Prefer materialization prior for shared hash fields; plan evaluates per-processor
        prior = state_store.get_input_state(PROCESSOR_MATERIALIZATION, snap.input_uid)
        if prior and prior.status == INPUT_STATUS_COMPLETE:
            snap = ProcessorInputSnapshot(
                input_uid=snap.input_uid,
                card_type=snap.card_type,
                corpus_state=snap.corpus_state,
                processor_decision=snap.processor_decision,
                field_values=dict(snap.field_values),
                source_dirty=snap.source_dirty,
                upstream_complete=snap.upstream_complete,
                recorded_input_hash=prior.input_hash or snap.recorded_input_hash,
                recorded_processor_version=prior.processor_version or snap.recorded_processor_version,
                recorded_corpus_state=prior.input_corpus_state or snap.recorded_corpus_state,
                output_exists=True,
                output_failed=False,
                upstream_output_hash=snap.upstream_output_hash,
                recorded_upstream_output_hash=snap.recorded_upstream_output_hash,
            )
        enriched.append(snap)

    plan = build_processor_plan(enriched, processor_keys=processor_keys)
    proc_key = processor_keys[0] if processor_keys and len(processor_keys) == 1 else "all"
    decl = declaration_for_key(proc_key) if proc_key != "all" else None
    report = ProcessorRunReport(
        run_id=run_id or f"processor-run-{proc_key}",
        processor_key=proc_key,
        processor_version=decl.processor_version if decl else "",
        archive_instance=archive_instance,
        status=RUN_STATUS_SKIPPED if not apply else RUN_STATUS_SUCCESS,
        input_count=plan.input_count,
        dirty_count=plan.dirty_count,
        stale_count=plan.stale_count,
        skipped_count=plan.skipped_count,
        skip_reasons=dict(plan.skip_reasons),
        stale_reasons=dict(plan.stale_reasons),
        plan=plan,
        engine_mode=engine_mode,
        ladder_gate=ladder_gate,
        decision_run_id=decision_run_id,
        started_at=_utc_now_iso(),
    )

    if provider_available is None:
        try:
            from archive_cli.providers import resolve_provider

            provider = resolve_provider(refresh=True)
            provider_available = bool(provider is not None and provider.is_available())
        except Exception:
            provider_available = False

    ctx = ExecuteContext(
        vault_path=vault_path,
        store=store,
        apply=apply and not dry_run,
        dry_run=dry_run or not apply,
        allow_full_embedding=allow_full_embedding,
        allow_all_linkers=allow_all_linkers,
        allow_broad_llm=allow_broad_llm,
        provider_available=bool(provider_available),
        run_id=report.run_id,
    )

    item_results: list[ItemExecuteResult] = []
    executed = False

    if not apply or dry_run:
        report.warnings.append("dry-run: processor execution not invoked")
        report.status = RUN_STATUS_SKIPPED
        report.completed_at = _utc_now_iso()
        root = repo_root or Path(__file__).resolve().parents[2]
        paths = write_processor_report(root, report)
        state_store.record_run(report)
        return ProcessorExecutionResult(
            report=report,
            item_results=item_results,
            executed=False,
            artifact_paths=paths,
        )

    # Apply path
    executed = True
    executor = batch_executor or default_batch_executor
    by_key: dict[str, list[ProcessorPlanItem]] = defaultdict(list)
    decl_versions = {d.processor_key: d.processor_version for d in iter_processor_declarations()}

    for item in plan.items:
        if item.skipped:
            item_results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_SKIPPED,
                    output_identity=item.output_identity,
                    input_hash=item.current_input_hash,
                    skip_reason=item.skip_reason,
                )
            )
            continue
        if not item.stale:
            continue
        version = decl_versions.get(item.processor_key, "")
        if _is_already_current(state_store, item, version):
            item_results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_SKIPPED,
                    output_identity=item.output_identity,
                    input_hash=item.current_input_hash,
                    skip_reason="already_current",
                    already_current=True,
                )
            )
            report.skipped_count += 1
            report.skip_reasons["already_current"] = report.skip_reasons.get("already_current", 0) + 1
            continue
        block = _opt_in_blocks(item.processor_key, ctx)
        if block:
            item_results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_SKIPPED,
                    output_identity=item.output_identity,
                    input_hash=item.current_input_hash,
                    skip_reason=block,
                )
            )
            report.skipped_count += 1
            report.skip_reasons[block] = report.skip_reasons.get(block, 0) + 1
            continue
        if item.processor_key in EXPENSIVE_PROCESSOR_KEYS:
            if item.processor_key == PROCESSOR_EMBEDDING and not ctx.allow_full_embedding:
                # Dirty-scoped thin path still allowed; do not refuse.
                pass
            if item.processor_key == PROCESSOR_LINKERS and not ctx.allow_all_linkers:
                pass
        if (
            item.processor_key in BROAD_LLM_PROCESSOR_KEYS
            and item.processor_key != PROCESSOR_EMAIL_TYPED_EXTRACTION
            and not ctx.provider_available
        ):
            item_results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_SKIPPED,
                    output_identity=item.output_identity,
                    input_hash=item.current_input_hash,
                    skip_reason=SKIP_PROVIDER,
                )
            )
            report.skipped_count += 1
            report.skip_reasons[SKIP_PROVIDER] = report.skip_reasons.get(SKIP_PROVIDER, 0) + 1
            continue
        by_key[item.processor_key].append(item)

    failed = 0
    completed = 0
    for key in topological_order():
        batch = by_key.get(key) or []
        if not batch:
            continue
        try:
            batch_result = executor(ctx, batch)
        except Exception as exc:
            # Isolate LLM-dependent failures from deterministic processors
            decl_obj = declaration_for_key(key)
            msg = f"{key}: {exc}"
            report.errors.append(msg)
            log.exception("processor_batch_failed key=%s", key)
            for item in batch:
                item_results.append(
                    ItemExecuteResult(
                        processor_key=item.processor_key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        output_identity=item.output_identity,
                        input_hash=item.current_input_hash,
                        error=str(exc),
                    )
                )
                failed += 1
            if decl_obj and decl_obj.llm_dependent:
                report.warnings.append(f"llm_dependent failure isolated: {key}")
                continue
            continue

        report.warnings.extend(batch_result.warnings)
        report.errors.extend(batch_result.errors)
        for result in batch_result.results:
            item_results.append(result)
            version = decl_versions.get(result.processor_key, "")
            if result.status == INPUT_STATUS_COMPLETE:
                completed += 1
                state_store.upsert_input_state(
                    ProcessorInputStateRecord(
                        processor_key=result.processor_key,
                        input_uid=result.input_uid,
                        input_hash=result.input_hash,
                        input_corpus_state=next(
                            (s.corpus_state for s in enriched if s.input_uid == result.input_uid),
                            "active",
                        ),
                        processor_version=version,
                        output_identity=result.output_identity,
                        output_uids=list(result.output_uids),
                        status=INPUT_STATUS_COMPLETE,
                        last_run_id=report.run_id,
                    )
                )
            elif result.status == INPUT_STATUS_FAILED:
                failed += 1
                state_store.upsert_input_state(
                    ProcessorInputStateRecord(
                        processor_key=result.processor_key,
                        input_uid=result.input_uid,
                        input_hash=result.input_hash,
                        processor_version=version,
                        output_identity=result.output_identity,
                        status=INPUT_STATUS_FAILED,
                        error=result.error,
                        last_run_id=report.run_id,
                    )
                )
            elif result.status == INPUT_STATUS_SKIPPED:
                report.skipped_count += 1
                if result.skip_reason:
                    report.skip_reasons[result.skip_reason] = report.skip_reasons.get(result.skip_reason, 0) + 1

    report.output_count = completed
    if failed and completed:
        report.status = RUN_STATUS_PARTIAL
    elif failed and not completed:
        report.status = RUN_STATUS_FAILED
    else:
        report.status = RUN_STATUS_SUCCESS
    report.completed_at = _utc_now_iso()

    root = repo_root or Path(__file__).resolve().parents[2]
    paths = write_processor_report(root, report)
    state_store.record_run(report)
    return ProcessorExecutionResult(
        report=report,
        item_results=item_results,
        executed=executed,
        artifact_paths=paths,
    )

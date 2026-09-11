"""Execute processor plans via existing extract/enrich/embed/link entrypoints (Section E Phase 2)."""

from __future__ import annotations

import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from archive_engine.contracts import AffectedContext, OutputReceipt, OutputRevision
from archive_sync.cli_logging import log_ratio_progress
from archive_sync.processors.input_hash import compute_output_revision

from .batch import ProcessorPlanItem, ProcessorRunReport
from .constants import (
    BROAD_LLM_PROCESSOR_KEYS,
    CONTEXT_RECONCILIATION_CAPABILITY,
    EMBEDDING_PROCESSOR_VERSION,
    EXPENSIVE_PROCESSOR_KEYS,
    INPUT_STATUS_BLOCKED_DEPENDENCY,
    INPUT_STATUS_COMPLETE,
    INPUT_STATUS_FAILED,
    INPUT_STATUS_PENDING,
    INPUT_STATUS_SKIPPED,
    MAX_OUTPUT_FEEDBACK_GENERATIONS,
    MAX_SELF_INVALIDATING_REVISIONS,
    PROCESSOR_EMAIL_PROMOTION_POLICY,
    PROCESSOR_EMAIL_THREAD_ENRICHMENT,
    PROCESSOR_EMAIL_TYPED_EXTRACTION,
    PROCESSOR_EMBEDDING,
    PROCESSOR_ENTITY_RESOLUTION,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    RUN_STATUS_BLOCKED,
    RUN_STATUS_FAILED,
    RUN_STATUS_PARTIAL,
    RUN_STATUS_SKIPPED,
    RUN_STATUS_SUCCESS,
    SECTION_E_COMPLETION_STATE,
    SECTION_E_EXECUTION_STATE,
    SKIP_NONCONVERGENT,
    SKIP_PROVIDER,
    SUCCESS_RECEIPT_STATUSES,
)
from .declarations import declaration_for_key, iter_processor_declarations
from .dirty_io import dirty_uids_from_source_reports, enqueue_output_snapshots, load_dirty_inputs
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
    affected_resolver: Any | None = None


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
    input_revision: str = ""
    receipt_status: str = ""
    valid_no_output: bool = False
    receipt: OutputReceipt | None = None

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
            "input_revision": self.input_revision,
            "receipt_status": self.receipt_status,
            "valid_no_output": self.valid_no_output,
            "receipt": None if self.receipt is None else self.receipt.to_payload(),
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
    receipts: list[OutputReceipt] = field(default_factory=list)
    feedback_generations: int = 0
    capability_markers: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "completion_state": SECTION_E_EXECUTION_STATE,
            "phase1_completion_state": SECTION_E_COMPLETION_STATE,
            "executed": self.executed,
            "report": self.report.to_dict(),
            "item_results": [r.to_dict() for r in self.item_results],
            "artifact_paths": dict(self.artifact_paths),
            "receipts": [receipt.to_payload() for receipt in self.receipts],
            "feedback_generations": self.feedback_generations,
            "capability_markers": list(self.capability_markers),
        }


def _is_already_current(
    prior: ProcessorInputStateRecord | None,
    item: ProcessorPlanItem,
    processor_version: str,
    receipt: Any | None = None,
) -> bool:
    """Complete only when a revision receipt exists. Legacy input_state is unknown."""

    del prior
    stored = receipt
    if stored is None:
        return False
    payload = getattr(stored, "receipt", stored)
    scheduler_status = str(getattr(stored, "scheduler_status", "") or "")
    version = str(getattr(payload, "processor_version", "") or "")
    revision = str(getattr(payload, "input_revision", "") or "")
    item_revision = item.input_revision or item.current_input_hash
    if version != processor_version or revision != item_revision:
        return False
    if scheduler_status:
        return scheduler_status in SUCCESS_RECEIPT_STATUSES
    return str(getattr(payload, "status", "")) == "completed"


def _item_revision(item: ProcessorPlanItem) -> str:
    return item.input_revision or item.current_input_hash or item.input_uid


def _item_version(item: ProcessorPlanItem) -> str:
    if item.processor_version:
        return item.processor_version
    decl = declaration_for_key(item.processor_key)
    return decl.processor_version if decl is not None else "unknown"


def _output_receipt(
    item: ProcessorPlanItem,
    *,
    status: str,
    outputs: tuple[OutputRevision, ...] = (),
    error_reason: str = "",
    dependency_reason: str = "",
) -> OutputReceipt:
    return OutputReceipt(
        processor=item.processor_key,
        processor_version=_item_version(item),
        input_uid=item.input_uid,
        input_revision=_item_revision(item),
        status=status,  # type: ignore[arg-type]
        outputs=outputs,
        error_reason=error_reason,
        dependency_reason=dependency_reason,
    )


def _result_with_outputs(
    item: ProcessorPlanItem,
    *,
    status: str,
    outputs: tuple[OutputRevision, ...] = (),
    error: str = "",
    skip_reason: str = "",
    valid_no_output: bool = False,
) -> ItemExecuteResult:
    if valid_no_output or status == INPUT_STATUS_COMPLETE:
        mapped = "completed"
    elif status == INPUT_STATUS_FAILED:
        mapped = "failed"
    else:
        mapped = "pending"
    receipt_outputs = () if valid_no_output else outputs
    receipt = _output_receipt(
        item,
        status=mapped,
        outputs=receipt_outputs,
        error_reason=error,
        dependency_reason=skip_reason,
    )
    return ItemExecuteResult(
        processor_key=item.processor_key,
        input_uid=item.input_uid,
        status=status,
        output_identity=item.output_identity,
        output_uids=[revision.uid for revision in receipt_outputs],
        input_hash=item.current_input_hash,
        input_revision=_item_revision(item),
        skip_reason=skip_reason,
        error=error,
        valid_no_output=valid_no_output,
        receipt=receipt,
    )


def _complete_items(items: list[ProcessorPlanItem]) -> list[ItemExecuteResult]:
    return [
        _result_with_outputs(
            item,
            status=INPUT_STATUS_COMPLETE,
            outputs=(OutputRevision(uid=item.input_uid, revision=_item_revision(item)),),
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
    for item in items:
        out.results.append(
            _result_with_outputs(
                item,
                status=INPUT_STATUS_COMPLETE,
                outputs=(OutputRevision(uid=item.input_uid, revision=_item_revision(item)),),
            )
        )
    return out


def _mark_processor_vault_written(vault_path: str | Path, uids: list[str]) -> None:
    """Invalidate process + serving cache after a processor writes derived cards."""

    written = [str(uid).strip() for uid in uids if str(uid).strip()]
    if not written:
        return
    try:
        from archive_cli.vault_cache_runtime import mark_vault_written

        mark_vault_written(vault_path, uids=written)
    except Exception:
        log.debug("mark_vault_written after processor writes failed", exc_info=True)


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
        written = [
            str(record.output_uid)
            for record in list(getattr(metrics, "created", []) or []) + list(getattr(metrics, "changed", []) or [])
            if getattr(record, "output_uid", None)
        ]
        _mark_processor_vault_written(ctx.vault_path, written)
    except Exception as exc:
        out.errors.append(f"typed_extraction: {exc}")
        log.exception("typed_extraction_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out
    failed = set(getattr(metrics, "failed_source_uids", []) or [])
    no_extraction = set(getattr(metrics, "no_extraction_source_uids", []) or [])
    by_source: dict[str, list[OutputRevision]] = defaultdict(list)
    for record in list(getattr(metrics, "created", []) or []) + list(getattr(metrics, "changed", []) or []):
        by_source[str(record.source_uid)].append(
            OutputRevision(uid=str(record.output_uid), revision=str(record.revision))
        )
    unchanged_by_source: dict[str, list[OutputRevision]] = defaultdict(list)
    for record in getattr(metrics, "unchanged", []) or []:
        unchanged_by_source[str(record.source_uid)].append(
            OutputRevision(uid=str(record.output_uid), revision=str(record.revision))
        )
    for item in items:
        if item.input_uid in failed:
            out.results.append(
                _result_with_outputs(
                    item,
                    status=INPUT_STATUS_FAILED,
                    error="extraction failed",
                )
            )
            continue
        outputs = tuple(by_source.get(item.input_uid) or ())
        if outputs:
            out.results.append(_result_with_outputs(item, status=INPUT_STATUS_COMPLETE, outputs=outputs))
            continue
        out.results.append(
            _result_with_outputs(
                item,
                status=INPUT_STATUS_COMPLETE,
                outputs=tuple(unchanged_by_source.get(item.input_uid) or ()),
                valid_no_output=True,
                skip_reason="no_extraction" if item.input_uid in no_extraction else "valid_no_output",
            )
        )
    return out


def _execute_entity_resolution(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into ``run_entity_resolution`` scoped to dirty UIDs."""

    out = BatchExecuteResult()
    uids = {item.input_uid for item in items}
    if ctx.apply and not ctx.dry_run:
        try:
            from archive_sync.extractors import entity_resolution as er_mod

            log.info("entity_resolution_dirty_uids count=%s", len(uids))
            er_result = er_mod.run_entity_resolution(ctx.vault_path, dry_run=False, uid_allowlist=uids)
            _mark_processor_vault_written(
                ctx.vault_path,
                list(er_result.get("created_uids") or []) + list(er_result.get("changed_uids") or []),
            )
        except Exception as exc:
            out.errors.append(f"entity_resolution: {exc}")
            log.exception("entity_resolution_failed")
            out.results.extend(_fail_items(items, str(exc)))
            return out
    else:
        out.results.extend(_complete_items(items))
        return out
    created = list(er_result.get("created_uids") or [])
    changed = list(er_result.get("changed_uids") or [])
    revisions = dict(er_result.get("output_revisions") or {})
    outputs = tuple(
        OutputRevision(
            uid=uid,
            revision=str(revisions.get(uid) or compute_output_revision(uid=uid, payload={"entity": uid})),
        )
        for uid in dict.fromkeys([*created, *changed])
        if uid
    )
    errors = list(er_result.get("errors") or [])
    for item in items:
        if errors:
            out.results.append(_result_with_outputs(item, status=INPUT_STATUS_FAILED, error="; ".join(errors[:3])))
            continue
        if not outputs:
            out.results.append(_result_with_outputs(item, status=INPUT_STATUS_COMPLETE, valid_no_output=True))
            continue
        out.results.append(_result_with_outputs(item, status=INPUT_STATUS_COMPLETE, outputs=outputs))
    return out


def _embedding_provider_ready() -> tuple[bool, str]:
    """Embedding readiness is independent of the enrichment LLM provider."""

    from archive_cli.embedding_provider import DEFAULT_EMBEDDING_PROVIDER
    from archive_cli.index_config import _ppa_env

    name = (_ppa_env("PPA_EMBEDDING_PROVIDER", default=DEFAULT_EMBEDDING_PROVIDER) or "hash").lower()
    if name in {"", "hash"}:
        return True, ""
    if name == "openai":
        if (os.environ.get("OPENAI_API_KEY") or "").strip():
            return True, ""
        return False, "OPENAI_API_KEY missing"
    return False, f"unsupported embedding provider: {name}"


def _execute_embedding(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Embed dirty cards by UID allowlist. Limit is a budget after selection.

    Full-corpus embed requires ``allow_full_embedding`` and uses the unscoped
    admin route. Provider failure or leftover pending chunks do not mark a
    card complete. Content-hash reuse runs inside ``embed_pending`` before
    any new vectors are paid for.
    """

    out = BatchExecuteResult()
    ready, reason = _embedding_provider_ready()
    if not ready:
        skipped = _skip_provider_items(items, PROCESSOR_EMBEDDING)
        if reason:
            skipped.warnings.append(reason)
        return skipped
    if not ctx.apply or ctx.dry_run:
        out.results.extend(_complete_items(items))
        return out
    try:
        from archive_cli.engine_factory import embedding_spec_from_env
        from archive_cli.index_config import get_embed_concurrency

        store = _require_store_attr(ctx, "embed_pending", "embedding")
        concurrency = get_embed_concurrency()
        spec = embedding_spec_from_env()
        uids = [item.input_uid for item in items]
        kwargs: dict[str, Any] = {"limit": 0, "embedding_spec": spec}
        if ctx.allow_full_embedding:
            kwargs["unscoped"] = True
            log.info("embedding_full_backlog concurrency=%s opt_in=allow_full_embedding", concurrency)
        else:
            kwargs["uid_allowlist"] = set(uids)
            log.info(
                "embedding_dirty_pending uids=%s budget=unlimited_after_allowlist concurrency=%s",
                len(uids),
                concurrency,
            )
        result = store.embed_pending(**kwargs)
        if not isinstance(result, dict):
            result = {"embedded": result, "failed": 0}
        embedded = result.get("embedded", 0)
        reused_by_content = int(result.get("reused_by_content") or 0)
        out.warnings.append(
            f"embedding embedded={embedded} reused={result.get('reused', 0)} "
            f"reused_by_content={reused_by_content} "
            f"pending_after={len(result.get('pending_chunk_keys') or [])} "
            f"selected={result.get('selected', 0)} "
            f"failed={result.get('failed', 0)} concurrency={concurrency}"
        )
    except Exception as exc:
        out.errors.append(f"embedding: {exc}")
        log.exception("embedding_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out

    last_error = str(result.get("last_error") or "")
    if ctx.allow_full_embedding:
        if int(result.get("failed") or 0) > 0:
            out.results.extend(_fail_items(items, last_error or "embedding provider failed"))
        else:
            out.results.extend(_complete_items(items))
        return out

    selected_by_uid = result.get("chunk_keys_by_uid") or {}
    completed_keys = {str(key) for key in (result.get("completed_chunk_keys") or [])}
    failed_keys = {str(key) for key in (result.get("failed_chunk_keys") or [])}
    pending_keys = {str(key) for key in (result.get("pending_chunk_keys") or [])}
    spec_payload = result.get("embedding_spec")
    from archive_engine.contracts import EmbeddingSpec

    parsed_spec = None
    if spec_payload is not None:
        parsed_spec = (
            spec_payload if isinstance(spec_payload, EmbeddingSpec) else EmbeddingSpec.from_payload(spec_payload)
        )

    def _receipt(*, status: str, keys: tuple[str, ...], error_reason: str = "") -> OutputReceipt:
        return OutputReceipt(
            processor=PROCESSOR_EMBEDDING,
            processor_version=EMBEDDING_PROCESSOR_VERSION,
            input_uid=item.input_uid,
            input_revision=revision,
            status=status,  # type: ignore[arg-type]
            chunk_keys=keys,
            embedding_spec=parsed_spec,
            error_reason=error_reason,
        )

    for item in items:
        keys = [str(key) for key in (selected_by_uid.get(item.input_uid) or [])]
        keyset = set(keys)
        item_failed = keyset & failed_keys
        item_pending = keyset & pending_keys
        revision = item.input_revision or item.current_input_hash
        if item_failed:
            out.results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_FAILED,
                    output_identity=item.output_identity,
                    input_hash=item.current_input_hash,
                    input_revision=revision,
                    error=last_error or "embedding provider failed",
                    receipt=_receipt(
                        status="failed",
                        keys=tuple(keys),
                        error_reason=last_error or "embedding provider failed",
                    ),
                )
            )
            continue
        if item_pending:
            out.results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_PENDING,
                    output_identity=item.output_identity,
                    input_hash=item.current_input_hash,
                    input_revision=revision,
                    receipt=_receipt(status="pending", keys=tuple(keys)),
                    receipt_status=INPUT_STATUS_PENDING,
                )
            )
            continue
        if not keys:
            out.results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_COMPLETE,
                    output_identity=item.output_identity,
                    input_hash=item.current_input_hash,
                    input_revision=revision,
                    valid_no_output=True,
                    receipt=_receipt(status="completed", keys=()),
                )
            )
            continue
        done_keys = tuple(key for key in keys if key in completed_keys)
        out.results.append(
            ItemExecuteResult(
                processor_key=item.processor_key,
                input_uid=item.input_uid,
                status=INPUT_STATUS_COMPLETE,
                output_identity=item.output_identity,
                output_uids=[item.input_uid],
                input_hash=item.current_input_hash,
                input_revision=revision,
                receipt=_receipt(status="completed", keys=done_keys),
            )
        )
    return out


def _execute_linkers(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into ``run_incremental_link_refresh`` for the dirty set.

    ``run_seed_link_backfill`` (all-linkers) requires ``allow_all_linkers``.
    """

    out = BatchExecuteResult()
    if not ctx.apply or ctx.dry_run:
        out.results.extend(_complete_items(items))
        return out
    include_llm = bool(ctx.provider_available and ctx.allow_broad_llm)
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
            log.info("linkers_all_backfill workers=%s opt_in=allow_all_linkers include_llm=%s", workers, include_llm)
            result = run_seed_link_backfill(
                index,
                max_workers=workers,
                include_llm=include_llm,
                apply_promotions=True,
            )
        else:
            log.info("linkers_incremental_refresh uids=%s workers=%s include_llm=%s", len(uids), workers, include_llm)
            result = run_incremental_link_refresh(
                index,
                source_uids=uids,
                max_workers=workers,
                include_llm=include_llm,
                apply_promotions=True,
            )
        jobs = result.get("jobs_completed", result) if isinstance(result, dict) else result
        out.warnings.append(
            f"linkers jobs_completed={jobs} dirty_uids={len(uids)} deterministic_only={not include_llm}"
        )
        if not include_llm:
            out.warnings.append("provider_links_pending")
    except Exception as exc:
        out.errors.append(f"linkers: {exc}")
        log.exception("linkers_failed")
        out.results.extend(_fail_items(items, str(exc)))
        return out
    output_uids = [str(uid) for uid in (result.get("output_uids") or []) if uid]
    outputs = tuple(
        OutputRevision(uid=uid, revision=compute_output_revision(uid=uid, payload={"linker": "deterministic"}))
        for uid in dict.fromkeys(output_uids)
    )
    failed_jobs = int(result.get("jobs_failed") or 0)
    for item in items:
        if failed_jobs:
            out.results.append(
                _result_with_outputs(
                    item,
                    status=INPUT_STATUS_FAILED,
                    error="linker jobs failed",
                )
            )
            continue
        if not outputs:
            out.results.append(
                _result_with_outputs(
                    item,
                    status=INPUT_STATUS_COMPLETE,
                    valid_no_output=True,
                    skip_reason="" if include_llm else "provider_links_pending",
                )
            )
            continue
        out.results.append(_result_with_outputs(item, status=INPUT_STATUS_COMPLETE, outputs=outputs))
    return out


def _card_types_for_uids(vault: Path | str, uids: list[str]) -> dict[str, str]:
    """Resolve card types in one cache IN-query. Per-UID read only for leftovers."""

    wanted = [str(uid).strip() for uid in uids if str(uid).strip()]
    types: dict[str, str] = {}
    try:
        from archive_cli.vault_cache_runtime import peek_process_cache

        cache = peek_process_cache(vault)
    except Exception:
        cache = None
    if cache is not None:
        for row in cache.frontmatter_rows_for_uids(wanted):
            uid = str(row.get("uid") or "").strip()
            fm = row.get("frontmatter") or {}
            if uid:
                types[uid] = str(fm.get("type") or "")
    from archive_vault.vault import read_note_by_uid

    for uid in wanted:
        if uid in types:
            continue
        note = read_note_by_uid(vault, uid)
        types[uid] = str((note[1] if note else {}).get("type") or "")
    return types


def _execute_enrichment(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Thin adapter into ``run_enrichment_for_uids`` on the existing orchestrator."""

    out = BatchExecuteResult()
    if not ctx.apply or ctx.dry_run:
        out.results.extend(_complete_items(items))
        return out
    from archive_sync.llm_enrichment import enrichment_orchestrator as orch
    from archive_sync.llm_enrichment.card_enrichment_runner import DERIVED_ENRICHMENT_TYPES
    from archive_vault.vault import read_note_by_uid

    derived: list[ProcessorPlanItem] = []
    threads: list[ProcessorPlanItem] = []
    type_by_uid = _card_types_for_uids(ctx.vault_path, [item.input_uid for item in items])
    for item in items:
        card_type = type_by_uid.get(item.input_uid, "")
        if card_type in DERIVED_ENRICHMENT_TYPES:
            derived.append(item)
        else:
            threads.append(item)

    if derived:
        try:
            metrics = orch.run_deterministic_derived_enrichment(
                ctx.vault_path, [item.input_uid for item in derived], dry_run=False
            )
            _mark_processor_vault_written(
                ctx.vault_path,
                list(getattr(metrics, "enriched_card_uids", []) or []),
            )
        except Exception as exc:
            out.errors.append(f"derived_enrichment: {exc}")
            log.exception("derived_enrichment_failed")
            out.results.extend(_fail_items(derived, str(exc)))
        else:
            failed = set(getattr(metrics, "failed_card_uids", []) or [])
            changed = set(getattr(metrics, "enriched_card_uids", []) or [])
            revisions = dict(getattr(metrics, "output_revisions", {}) or {})
            for item in derived:
                if item.input_uid in failed:
                    out.results.append(
                        _result_with_outputs(item, status=INPUT_STATUS_FAILED, error="derived enrichment failed")
                    )
                    continue
                revision = str(revisions.get(item.input_uid) or _item_revision(item))
                if item.input_uid in changed:
                    out.results.append(
                        _result_with_outputs(
                            item,
                            status=INPUT_STATUS_COMPLETE,
                            outputs=(OutputRevision(uid=item.input_uid, revision=revision),),
                        )
                    )
                    continue
                out.results.append(_result_with_outputs(item, status=INPUT_STATUS_COMPLETE, valid_no_output=True))

    if not threads:
        return out
    if not ctx.provider_available:
        thread_skip = _skip_provider_items(threads, PROCESSOR_EMAIL_THREAD_ENRICHMENT)
        out.results.extend(thread_skip.results)
        out.warnings.extend(thread_skip.warnings)
        return out
    try:
        from archive_cli.index_config import get_rebuild_workers

        workers = get_rebuild_workers()
        log.info("email_thread_enrichment_for_uids count=%s workers=%s", len(threads), workers)
        metrics = orch.run_enrichment_for_uids(
            ctx.vault_path,
            [item.input_uid for item in threads],
            workflow="email_thread",
            dry_run=False,
            workers=workers,
            run_id=ctx.run_id,
        )
    except Exception as exc:
        out.errors.append(f"email_thread_enrichment: {exc}")
        log.exception("enrichment_failed")
        out.results.extend(_fail_items(threads, str(exc)))
        return out
    enriched = set(getattr(metrics, "enriched_card_uids", []) or [])
    errors = int(getattr(metrics, "errors", 0) or 0)
    for item in threads:
        if errors and item.input_uid not in enriched:
            out.results.append(_result_with_outputs(item, status=INPUT_STATUS_FAILED, error="enrichment failed"))
            continue
        if item.input_uid in enriched:
            out.results.append(
                _result_with_outputs(
                    item,
                    status=INPUT_STATUS_COMPLETE,
                    outputs=(OutputRevision(uid=item.input_uid, revision=_item_revision(item)),),
                )
            )
            continue
        out.results.append(_result_with_outputs(item, status=INPUT_STATUS_COMPLETE, valid_no_output=True))
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
        if processor_key in {
            PROCESSOR_EMAIL_TYPED_EXTRACTION,
            PROCESSOR_ENTITY_RESOLUTION,
            PROCESSOR_EMAIL_THREAD_ENRICHMENT,
        }:
            # Deterministic extract/ER/derived enrichment run without broad LLM.
            return None
        return "missing_broad_llm_opt_in"
    return None


def _resolve_affected_context(ctx: ExecuteContext, uid: str, revision: str) -> AffectedContext:
    resolver = ctx.affected_resolver
    if resolver is None and ctx.store is not None:
        resolver = getattr(ctx.store, "engine", None)
    if resolver is None or not hasattr(resolver, "resolve_affected"):
        return AffectedContext(uid=uid, revision=revision, status="reconciliation_pending")
    resolved = resolver.resolve_affected(uid, revision)
    if resolved is None:
        return AffectedContext(uid=uid, revision=revision, status="reconciliation_pending")
    return resolved


def _record_item_states(
    results: list[ItemExecuteResult],
    *,
    state_store: ProcessorStateStore,
    report: ProcessorRunReport,
    decl_versions: dict[str, str],
    corpus_by_uid: dict[str, str],
) -> tuple[int, int]:
    completed = 0
    failed = 0
    for result in results:
        version = decl_versions.get(result.processor_key, "")
        if result.receipt is None and result.status == INPUT_STATUS_COMPLETE:
            report.errors.append(f"{result.processor_key}:{result.input_uid}: complete without receipt (ignored)")
            continue
        if result.status == INPUT_STATUS_COMPLETE or result.valid_no_output:
            completed += 1
            state_store.upsert_input_state(
                ProcessorInputStateRecord(
                    processor_key=result.processor_key,
                    input_uid=result.input_uid,
                    input_hash=result.input_hash,
                    input_corpus_state=corpus_by_uid.get(result.input_uid, "active"),
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
        elif result.status == INPUT_STATUS_BLOCKED_DEPENDENCY:
            state_store.upsert_input_state(
                ProcessorInputStateRecord(
                    processor_key=result.processor_key,
                    input_uid=result.input_uid,
                    input_hash=result.input_hash,
                    processor_version=version,
                    output_identity=result.output_identity,
                    status=INPUT_STATUS_BLOCKED_DEPENDENCY,
                    skip_reason=result.skip_reason,
                    error=result.error,
                    last_run_id=report.run_id,
                )
            )
        elif result.status == INPUT_STATUS_SKIPPED:
            report.skipped_count += 1
            if result.skip_reason:
                report.skip_reasons[result.skip_reason] = report.skip_reasons.get(result.skip_reason, 0) + 1
        elif result.status == INPUT_STATUS_PENDING:
            state_store.upsert_input_state(
                ProcessorInputStateRecord(
                    processor_key=result.processor_key,
                    input_uid=result.input_uid,
                    input_hash=result.input_hash,
                    processor_version=version,
                    output_identity=result.output_identity,
                    status=INPUT_STATUS_PENDING,
                    skip_reason=result.skip_reason,
                    error=result.error,
                    last_run_id=report.run_id,
                )
            )
    return completed, failed


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
    affected_resolver: Any | None = None,
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
            if uids:
                seen = {snap.input_uid for snap in snapshots}
                extra = [uid for uid in uids if uid not in seen]
                if extra:
                    snapshots.extend(
                        load_dirty_inputs(
                            dirty_uids=extra,
                            vault_path=vault_path,
                            store=store,
                            state_store=state_store,
                            default_card_type=default_card_type,
                            default_processor_decision=default_processor_decision,
                        )
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
    log.info("processor plan enrich start snapshots=%s", len(snapshots))
    prior_by_uid = state_store.get_input_states_for_uids([snap.input_uid for snap in snapshots])
    enriched: list[ProcessorInputSnapshot] = []
    for snap in snapshots:
        # Prefer materialization prior for shared hash fields; plan evaluates per-processor
        prior = (prior_by_uid.get(snap.input_uid) or {}).get(PROCESSOR_MATERIALIZATION)
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
    materialize_uids = {
        item.input_uid for item in plan.items if item.processor_key == PROCESSOR_MATERIALIZATION and not item.skipped
    }
    planned_dirty = {snap.input_uid for snap in enriched if snap.source_dirty}
    dirty_unplanned = sorted(planned_dirty - materialize_uids)
    log.info(
        "processor plan built inputs=%s dirty=%s stale=%s skipped=%s materialize_uids=%s dirty_without_materialization=%s",
        plan.input_count,
        plan.dirty_count,
        plan.stale_count,
        plan.skipped_count,
        len(materialize_uids),
        len(dirty_unplanned),
    )
    if dirty_unplanned:
        log.warning(
            "dirty uids have no materialization plan sample=%s",
            dirty_unplanned[:12],
        )
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
        affected_resolver=affected_resolver,
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
    corpus_by_uid = {snap.input_uid: snap.corpus_state for snap in enriched}
    classify_started = time.monotonic()
    log.info("processor execute classify start items=%s", len(plan.items))
    receipts_by_uid = state_store.get_receipts_for_uids([snap.input_uid for snap in enriched])

    for item_i, item in enumerate(plan.items, start=1):
        log_ratio_progress(
            log,
            "processor execute classify",
            item_i,
            len(plan.items),
            classify_started,
            every=5000,
        )
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
        prior = (prior_by_uid.get(item.input_uid) or {}).get(item.processor_key)
        prior_receipt = (receipts_by_uid.get(item.input_uid) or {}).get(item.processor_key)
        if _is_already_current(prior, item, version, prior_receipt):
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
            and item.processor_key
            not in {
                PROCESSOR_EMAIL_TYPED_EXTRACTION,
                PROCESSOR_ENTITY_RESOLUTION,
                PROCESSOR_EMAIL_THREAD_ENRICHMENT,
            }
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

    log.info(
        "processor execute classify done items=%s queued=%s elapsed=%.1fs",
        len(plan.items),
        sum(len(batch) for batch in by_key.values()),
        time.monotonic() - classify_started,
    )

    from .scheduler import ProcessorScheduler

    scheduler = ProcessorScheduler(state_store, lease_owner=report.run_id)
    scheduled = scheduler.run_batches(
        ctx=ctx,
        by_key=by_key,
        executor=executor,
        run_id=report.run_id,
        snapshots=enriched,
        decl_versions=decl_versions,
    )
    item_results.extend(scheduled.item_results)
    report.warnings.extend(scheduled.warnings)
    report.errors.extend(scheduled.errors)
    report.scheduler_events = [event.to_dict() for event in scheduled.events]
    report.blocked_count = scheduled.blocked_count
    all_receipts = list(scheduled.receipts)
    completed, failed = _record_item_states(
        scheduled.item_results,
        state_store=state_store,
        report=report,
        decl_versions=decl_versions,
        corpus_by_uid=corpus_by_uid,
    )

    seen_uids = {snap.input_uid for snap in enriched}
    seen_output_revisions: dict[str, str] = {}
    invalidation_counts: dict[str, int] = {}
    capability_markers: list[str] = []
    last_receipts = list(scheduled.receipts)
    snapshots_pool = list(enriched)
    generation = 0

    while last_receipts and generation < MAX_OUTPUT_FEEDBACK_GENERATIONS:
        generation += 1
        next_uids: list[str] = []
        requeued: set[str] = set()
        nonconvergent: list[tuple[str, str, str]] = []
        for receipt in last_receipts:
            for output in receipt.outputs:
                previous = seen_output_revisions.get(output.uid)
                if previous is None:
                    seen_output_revisions[output.uid] = output.revision
                    if output.uid and output.uid not in seen_uids:
                        next_uids.append(output.uid)
                elif previous != output.revision:
                    seen_output_revisions[output.uid] = output.revision
                    invalidation_counts[output.uid] = invalidation_counts.get(output.uid, 0) + 1
                    if invalidation_counts[output.uid] >= MAX_SELF_INVALIDATING_REVISIONS:
                        nonconvergent.append((receipt.processor, receipt.input_uid, output.uid))
                    else:
                        next_uids.append(output.uid)
                        requeued.add(output.uid)
                affected = _resolve_affected_context(ctx, output.uid or receipt.input_uid, output.revision)
                if affected.status != "resolved":
                    if CONTEXT_RECONCILIATION_CAPABILITY not in capability_markers:
                        capability_markers.append(CONTEXT_RECONCILIATION_CAPABILITY)
                else:
                    for related in affected.related_uids:
                        if related not in seen_uids:
                            next_uids.append(related)
        unique_uids = list(dict.fromkeys(uid for uid in next_uids if uid))
        blocked_outputs = {uid for _p, _i, uid in nonconvergent}
        unique_uids = [uid for uid in unique_uids if uid not in blocked_outputs]
        for processor_key, input_uid, _output_uid in nonconvergent:
            version = decl_versions.get(processor_key, "unknown")
            item_results.append(
                ItemExecuteResult(
                    processor_key=processor_key,
                    input_uid=input_uid,
                    status=INPUT_STATUS_PENDING,
                    skip_reason=SKIP_NONCONVERGENT,
                    error="self-invalidating output revisions exceeded bound",
                    receipt=OutputReceipt(
                        processor=processor_key,
                        processor_version=version,
                        input_uid=input_uid,
                        input_revision="nonconvergent",
                        status="pending",
                        error_reason="self-invalidating output revisions exceeded bound",
                        dependency_reason=SKIP_NONCONVERGENT,
                    ),
                )
            )
        if not unique_uids:
            generation -= 1
            break
        if generation >= MAX_OUTPUT_FEEDBACK_GENERATIONS:
            for uid in unique_uids:
                item_results.append(
                    ItemExecuteResult(
                        processor_key="feedback",
                        input_uid=uid,
                        status=INPUT_STATUS_PENDING,
                        skip_reason=SKIP_NONCONVERGENT,
                        error="output feedback generation bound reached",
                        receipt=OutputReceipt(
                            processor="feedback",
                            processor_version="feedback-v1",
                            input_uid=uid,
                            input_revision="generation-bound",
                            status="pending",
                            error_reason="output feedback generation bound reached",
                            dependency_reason=SKIP_NONCONVERGENT,
                        ),
                    )
                )
            break
        new_snaps = enqueue_output_snapshots(
            unique_uids,
            vault_path=vault_path,
            store=store,
            state_store=state_store,
            source_dirty=True,
            feedback_generation=generation,
            default_card_type=default_card_type,
            default_processor_decision=default_processor_decision,
        )
        seen_uids.update(unique_uids)
        for snap in new_snaps:
            if snap.input_uid in requeued:
                token = f"fb{generation}"
                snap.field_values["frontmatter_hash"] = f"{snap.field_values.get('frontmatter_hash')}:{token}"
                snap.field_values["body_sha"] = f"{snap.field_values.get('body_sha')}:{token}"
                snap.field_values["chunk_hash"] = f"{snap.field_values.get('chunk_hash')}:{token}"
            corpus_by_uid[snap.input_uid] = snap.corpus_state
        snapshots_pool.extend(new_snaps)
        prior_by_uid.update(state_store.get_input_states_for_uids(unique_uids))
        plan_wave = build_processor_plan(new_snaps, processor_keys=processor_keys)
        receipts_wave = state_store.get_receipts_for_uids(unique_uids)
        by_key_wave: dict[str, list[ProcessorPlanItem]] = defaultdict(list)
        for item in plan_wave.items:
            if item.skipped or not item.stale:
                continue
            version = decl_versions.get(item.processor_key, "")
            prior = (prior_by_uid.get(item.input_uid) or {}).get(item.processor_key)
            prior_receipt = (receipts_wave.get(item.input_uid) or {}).get(item.processor_key)
            if item.input_uid not in requeued and _is_already_current(prior, item, version, prior_receipt):
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
                continue
            if _opt_in_blocks(item.processor_key, ctx):
                continue
            by_key_wave[item.processor_key].append(item)
        if not by_key_wave:
            break
        wave = scheduler.run_batches(
            ctx=ctx,
            by_key=by_key_wave,
            executor=executor,
            run_id=report.run_id,
            snapshots=snapshots_pool,
            decl_versions=decl_versions,
        )
        item_results.extend(wave.item_results)
        all_receipts.extend(wave.receipts)
        report.warnings.extend(wave.warnings)
        report.errors.extend(wave.errors)
        report.scheduler_events.extend(event.to_dict() for event in wave.events)
        report.blocked_count += wave.blocked_count
        wave_completed, wave_failed = _record_item_states(
            wave.item_results,
            state_store=state_store,
            report=report,
            decl_versions=decl_versions,
            corpus_by_uid=corpus_by_uid,
        )
        completed += wave_completed
        failed += wave_failed
        last_receipts = list(wave.receipts)
        scheduled = wave

    report.feedback_generations = max(generation, 0)
    report.capability_markers = list(capability_markers)
    if capability_markers:
        report.warnings.append(f"context_reconciliation_pending capability={CONTEXT_RECONCILIATION_CAPABILITY}")

    report.output_count = completed
    if scheduled.blocked_count and not failed and not completed:
        report.status = RUN_STATUS_BLOCKED
    elif (failed or scheduled.blocked_count) and completed:
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
        receipts=all_receipts,
        feedback_generations=report.feedback_generations,
        capability_markers=list(capability_markers),
    )

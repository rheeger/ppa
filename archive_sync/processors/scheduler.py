"""Per-revision processor scheduler (P03-A).

A failed required prerequisite blocks only that input's descendants. Unrelated
inputs continue. Completion is an ``OutputReceipt`` from
``archive_engine.contracts`` — this module does not redefine it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable

from archive_engine.contracts import OutputReceipt, OutputRevision
from archive_sync.cli_logging import log_ratio_progress

from .batch import ProcessorPlanItem
from .constants import (
    BLOCKING_RECEIPT_STATUSES,
    DEP_CONDITIONAL,
    DEP_OPTIONAL,
    DEP_REQUIRED,
    INPUT_STATUS_BLOCKED_DEPENDENCY,
    INPUT_STATUS_COMPLETE,
    INPUT_STATUS_FAILED,
    INPUT_STATUS_SKIPPED,
    LEASE_SECONDS_DEFAULT,
    LEGACY_UNKNOWN,
    RECEIPT_STATUS_BLOCKED_DEPENDENCY,
    RECEIPT_STATUS_COMPLETE,
    RECEIPT_STATUS_PERMANENT_FAILURE,
    RECEIPT_STATUS_RETRYABLE_FAILURE,
    RECEIPT_STATUS_SUPERSEDED,
    RECEIPT_STATUS_VALID_NO_OUTPUT,
    SKIP_BLOCKED_DEPENDENCY,
    SUCCESS_RECEIPT_STATUSES,
    output_receipt_status,
)
from .declarations import (
    ProcessorDeclaration,
    ProcessorDependency,
    ProcessorGraphError,
    dependencies_for,
    iter_processor_declarations,
    topological_order,
    validate_processor_graph,
)
from .runner import BatchExecuteResult, ExecuteContext, ItemExecuteResult
from .staleness import ProcessorInputSnapshot
from .state_store import ProcessorStateStore, StoredReceipt

log = logging.getLogger("ppa.processors")

ProcessorBatchExecutor = Callable[[ExecuteContext, list[ProcessorPlanItem]], BatchExecuteResult]


class SchedulerError(RuntimeError):
    """Static graph or scheduling invariant failed."""


@dataclass
class SchedulerEvent:
    event: str
    processor_key: str
    input_uid: str
    input_revision: str
    detail: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "event": self.event,
            "processor_key": self.processor_key,
            "input_uid": self.input_uid,
            "input_revision": self.input_revision,
            "detail": self.detail,
        }


@dataclass
class SchedulerResult:
    item_results: list[ItemExecuteResult] = field(default_factory=list)
    events: list[SchedulerEvent] = field(default_factory=list)
    receipts: list[OutputReceipt] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    blocked_count: int = 0
    completed_count: int = 0
    failed_count: int = 0


def dependency_receipt_digest(receipts: Iterable[OutputReceipt]) -> str:
    payload = [
        {
            "processor": item.processor,
            "processor_version": item.processor_version,
            "input_uid": item.input_uid,
            "input_revision": item.input_revision,
            "status": item.status,
            "outputs": [out.to_payload() for out in item.outputs],
        }
        for item in sorted(receipts, key=lambda rec: rec.processor)
    ]
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def dependency_applies(dep: ProcessorDependency, snapshot: ProcessorInputSnapshot | None) -> bool:
    if dep.kind != DEP_CONDITIONAL:
        return True
    if snapshot is None or not dep.when.strip():
        return False
    field, _, expected = dep.when.partition("=")
    field = field.strip()
    expected = expected.strip()
    if field == "processor_decision":
        actual = snapshot.processor_decision
    elif field == "corpus_state":
        actual = snapshot.corpus_state
    else:
        actual = str(snapshot.field_values.get(field, ""))
    return actual == expected


def scheduler_status_from_result(result: ItemExecuteResult) -> str:
    if result.valid_no_output:
        return RECEIPT_STATUS_VALID_NO_OUTPUT
    if result.receipt_status:
        return result.receipt_status
    if result.status == INPUT_STATUS_COMPLETE:
        return RECEIPT_STATUS_COMPLETE
    if result.status == INPUT_STATUS_FAILED:
        error = (result.error or "").lower()
        if error.startswith("permanent:") or "validation" in error:
            return RECEIPT_STATUS_PERMANENT_FAILURE
        return RECEIPT_STATUS_RETRYABLE_FAILURE
    if result.status == INPUT_STATUS_BLOCKED_DEPENDENCY:
        return RECEIPT_STATUS_BLOCKED_DEPENDENCY
    if result.status == INPUT_STATUS_SKIPPED:
        return INPUT_STATUS_SKIPPED
    return result.status or RECEIPT_STATUS_RETRYABLE_FAILURE


def receipt_from_item_result(
    result: ItemExecuteResult,
    *,
    processor_version: str,
    input_revision: str,
    scheduler_status: str,
) -> OutputReceipt:
    outputs = tuple(
        OutputRevision(uid=uid, revision=input_revision)
        for uid in result.output_uids
        if uid
    )
    if scheduler_status == RECEIPT_STATUS_VALID_NO_OUTPUT:
        outputs = ()
    prior = result.receipt
    return OutputReceipt(
        processor=result.processor_key,
        processor_version=processor_version,
        input_uid=result.input_uid,
        input_revision=input_revision,
        status=output_receipt_status(scheduler_status),  # type: ignore[arg-type]
        outputs=outputs,
        chunk_keys=() if prior is None else prior.chunk_keys,
        embedding_spec=None if prior is None else prior.embedding_spec,
        error_reason=result.error or ("" if prior is None else prior.error_reason),
        dependency_reason=result.skip_reason if scheduler_status == RECEIPT_STATUS_BLOCKED_DEPENDENCY else "",
    )


class ProcessorScheduler:
    """Evaluate per-item prerequisites, take leases, and commit revision receipts."""

    def __init__(
        self,
        state_store: ProcessorStateStore,
        *,
        declarations: Iterable[ProcessorDeclaration] | None = None,
        lease_seconds: int = LEASE_SECONDS_DEFAULT,
        lease_owner: str = "",
    ) -> None:
        self._store = state_store
        self._declarations = list(declarations) if declarations is not None else list(iter_processor_declarations())
        self._by_key = {decl.processor_key: decl for decl in self._declarations}
        self._lease_seconds = lease_seconds
        self._lease_owner = lease_owner
        graph_errors = validate_processor_graph(self._declarations)
        if graph_errors:
            raise ProcessorGraphError("; ".join(graph_errors))

    def validate_graph(self) -> list[str]:
        return validate_processor_graph(self._declarations)

    def run_batches(
        self,
        *,
        ctx: ExecuteContext,
        by_key: dict[str, list[ProcessorPlanItem]],
        executor: ProcessorBatchExecutor,
        run_id: str,
        snapshots: Iterable[ProcessorInputSnapshot] = (),
        decl_versions: dict[str, str] | None = None,
    ) -> SchedulerResult:
        snap_by_uid = {snap.input_uid: snap for snap in snapshots}
        versions = decl_versions or {decl.processor_key: decl.processor_version for decl in self._declarations}
        planned_keys = {key for key, items in by_key.items() if items}
        order = topological_order(self._declarations)
        out = SchedulerResult()
        owner = self._lease_owner or run_id or "scheduler"
        receipts: dict[tuple[str, str], StoredReceipt] = {}
        uids = {item.input_uid for items in by_key.values() for item in items}
        for uid, per_key in self._store.get_receipts_for_uids(sorted(uids)).items():
            for key, stored in per_key.items():
                receipts[(key, uid)] = stored

        started = time.monotonic()
        work_items = [(key, item) for key in order for item in by_key.get(key) or []]
        log.info("processor scheduler drain start items=%s keys=%s", len(work_items), len(planned_keys))

        for key in order:
            batch = list(by_key.get(key) or [])
            if not batch:
                continue
            eligible: list[ProcessorPlanItem] = []
            for item_i, item in enumerate(batch, start=1):
                log_ratio_progress(
                    log,
                    f"processor scheduler {key}",
                    item_i,
                    len(batch),
                    started,
                    every=5000,
                )
                decision = self._evaluate_item(
                    item,
                    planned_keys=planned_keys,
                    receipts=receipts,
                    snapshot=snap_by_uid.get(item.input_uid),
                    processor_version=versions.get(key, item.processor_version),
                )
                item.dependency_receipt_digest = decision.digest
                if decision.already_current:
                    prior = receipts.get((item.processor_key, item.input_uid))
                    out.item_results.append(
                        ItemExecuteResult(
                            processor_key=item.processor_key,
                            input_uid=item.input_uid,
                            status=INPUT_STATUS_SKIPPED,
                            output_identity=item.output_identity,
                            input_hash=item.current_input_hash,
                            input_revision=item.input_revision or item.current_input_hash,
                            skip_reason="already_current",
                            already_current=True,
                            receipt=prior.receipt if prior else None,
                        )
                    )
                    out.events.append(
                        SchedulerEvent(
                            "already_current",
                            item.processor_key,
                            item.input_uid,
                            item.input_revision or item.current_input_hash,
                        )
                    )
                    continue
                if decision.block:
                    blocked = self._commit_blocked(
                        item,
                        processor_version=versions.get(key, item.processor_version),
                        digest=decision.digest,
                        run_id=run_id,
                        reason=decision.reason,
                    )
                    out.item_results.append(blocked)
                    if blocked.receipt is not None:
                        out.receipts.append(blocked.receipt)
                        receipts[(item.processor_key, item.input_uid)] = StoredReceipt(
                            receipt=blocked.receipt,
                            scheduler_status=RECEIPT_STATUS_BLOCKED_DEPENDENCY,
                            dependency_receipt_digest=decision.digest,
                            last_run_id=run_id,
                        )
                    out.blocked_count += 1
                    out.events.append(
                        SchedulerEvent(
                            "blocked_dependency",
                            item.processor_key,
                            item.input_uid,
                            item.input_revision or item.current_input_hash,
                            decision.reason,
                        )
                    )
                    continue
                if not self._store.acquire_lease(
                    processor_key=item.processor_key,
                    processor_version=versions.get(key, item.processor_version),
                    input_uid=item.input_uid,
                    input_revision=item.input_revision or item.current_input_hash,
                    digest=decision.digest,
                    owner=owner,
                    lease_seconds=self._lease_seconds,
                ):
                    head = self._store.get_head_receipt(item.processor_key, item.input_uid)
                    if head is not None and head.scheduler_status in SUCCESS_RECEIPT_STATUSES:
                        out.item_results.append(
                            ItemExecuteResult(
                                processor_key=item.processor_key,
                                input_uid=item.input_uid,
                                status=INPUT_STATUS_SKIPPED,
                                output_identity=item.output_identity,
                                input_hash=item.current_input_hash,
                                input_revision=item.input_revision or item.current_input_hash,
                                skip_reason="already_current",
                                already_current=True,
                                receipt=head.receipt,
                            )
                        )
                        continue
                    out.item_results.append(
                        ItemExecuteResult(
                            processor_key=item.processor_key,
                            input_uid=item.input_uid,
                            status=RECEIPT_STATUS_SUPERSEDED,
                            output_identity=item.output_identity,
                            input_hash=item.current_input_hash,
                            input_revision=item.input_revision or item.current_input_hash,
                            skip_reason="lease_not_acquired",
                        )
                    )
                    out.events.append(
                        SchedulerEvent(
                            "lease_denied",
                            item.processor_key,
                            item.input_uid,
                            item.input_revision or item.current_input_hash,
                        )
                    )
                    continue
                out.events.append(
                    SchedulerEvent(
                        "lease_acquired",
                        item.processor_key,
                        item.input_uid,
                        item.input_revision or item.current_input_hash,
                    )
                )
                eligible.append(item)

            if not eligible:
                continue
            log.info("processor execute batch start key=%s items=%s", key, len(eligible))
            try:
                batch_result = executor(ctx, eligible)
            except Exception as exc:
                msg = f"{key}: {exc}"
                out.errors.append(msg)
                log.exception("processor_batch_failed key=%s", key)
                for item in eligible:
                    failed = ItemExecuteResult(
                        processor_key=item.processor_key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        output_identity=item.output_identity,
                        input_hash=item.current_input_hash,
                        input_revision=item.input_revision or item.current_input_hash,
                        error=str(exc),
                        receipt_status=RECEIPT_STATUS_RETRYABLE_FAILURE,
                    )
                    committed = self._commit_result(
                        failed,
                        item=item,
                        processor_version=versions.get(key, item.processor_version),
                        digest=item.dependency_receipt_digest,
                        run_id=run_id,
                        lease_owner=owner,
                    )
                    out.item_results.append(committed)
                    out.failed_count += 1
                    if committed.receipt is not None:
                        out.receipts.append(committed.receipt)
                        receipts[(item.processor_key, item.input_uid)] = StoredReceipt(
                            receipt=committed.receipt,
                            scheduler_status=RECEIPT_STATUS_RETRYABLE_FAILURE,
                            dependency_receipt_digest=item.dependency_receipt_digest,
                            last_run_id=run_id,
                        )
                continue

            out.warnings.extend(batch_result.warnings)
            out.errors.extend(batch_result.errors)
            seen = {result.input_uid for result in batch_result.results}
            for item in eligible:
                if item.input_uid in seen:
                    continue
                batch_result.results.append(
                    ItemExecuteResult(
                        processor_key=item.processor_key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        output_identity=item.output_identity,
                        input_hash=item.current_input_hash,
                        error="executor returned no result for input",
                    )
                )
            for result in batch_result.results:
                item = next((it for it in eligible if it.input_uid == result.input_uid), None)
                revision = (item.input_revision if item else "") or result.input_revision or result.input_hash
                digest = item.dependency_receipt_digest if item else ""
                committed = self._commit_result(
                    result,
                    item=item,
                    processor_version=versions.get(result.processor_key, ""),
                    digest=digest,
                    run_id=run_id,
                    lease_owner=owner,
                    input_revision=revision,
                )
                out.item_results.append(committed)
                if committed.status == INPUT_STATUS_COMPLETE or committed.valid_no_output:
                    out.completed_count += 1
                elif committed.status == INPUT_STATUS_FAILED:
                    out.failed_count += 1
                if committed.receipt is not None:
                    out.receipts.append(committed.receipt)
                    receipts[(committed.processor_key, committed.input_uid)] = StoredReceipt(
                        receipt=committed.receipt,
                        scheduler_status=scheduler_status_from_result(committed),
                        dependency_receipt_digest=digest,
                        last_run_id=run_id,
                    )
                    out.events.append(
                        SchedulerEvent(
                            "receipt_committed",
                            committed.processor_key,
                            committed.input_uid,
                            revision,
                            committed.receipt.status,
                        )
                    )

        log.info(
            "processor scheduler drain done items=%s completed=%s blocked=%s failed=%s elapsed=%.1fs",
            len(work_items),
            out.completed_count,
            out.blocked_count,
            out.failed_count,
            time.monotonic() - started,
        )
        return out

    def _evaluate_item(
        self,
        item: ProcessorPlanItem,
        *,
        planned_keys: set[str],
        receipts: dict[tuple[str, str], StoredReceipt],
        snapshot: ProcessorInputSnapshot | None,
        processor_version: str,
    ) -> "_DepDecision":
        revision = item.input_revision or item.current_input_hash
        existing = receipts.get((item.processor_key, item.input_uid))
        if (
            existing is not None
            and existing.receipt.processor_version == processor_version
            and existing.receipt.input_revision == revision
            and existing.scheduler_status in SUCCESS_RECEIPT_STATUSES
        ):
            return _DepDecision(already_current=True, digest=existing.dependency_receipt_digest)

        decl = self._by_key.get(item.processor_key)
        required_receipts: list[OutputReceipt] = []
        if decl is None:
            return _DepDecision(digest=dependency_receipt_digest(required_receipts))

        for dep in dependencies_for(decl):
            if not dependency_applies(dep, snapshot):
                continue
            stored = receipts.get((dep.processor_key, item.input_uid))
            if stored is None:
                if dep.processor_key in planned_keys and dep.kind == DEP_REQUIRED:
                    return _DepDecision(
                        block=True,
                        reason=f"required {dep.processor_key} produced no receipt for {item.input_uid}",
                    )
                # Missing and not in this run: unknown lineage, do not silently
                # treat legacy input_state as complete, but do not block CLI
                # single-processor invocation either.
                continue
            if stored.scheduler_status == LEGACY_UNKNOWN:
                continue
            if stored.scheduler_status in BLOCKING_RECEIPT_STATUSES:
                if dep.kind == DEP_OPTIONAL:
                    continue
                return _DepDecision(
                    block=True,
                    reason=(
                        f"required {dep.processor_key} is {stored.scheduler_status} "
                        f"for {item.input_uid} rev={stored.receipt.input_revision}"
                    ),
                )
            if dep.kind != DEP_OPTIONAL:
                required_receipts.append(stored.receipt)
        return _DepDecision(digest=dependency_receipt_digest(required_receipts))

    def _commit_blocked(
        self,
        item: ProcessorPlanItem,
        *,
        processor_version: str,
        digest: str,
        run_id: str,
        reason: str,
    ) -> ItemExecuteResult:
        revision = item.input_revision or item.current_input_hash
        receipt = OutputReceipt(
            processor=item.processor_key,
            processor_version=processor_version,
            input_uid=item.input_uid,
            input_revision=revision,
            status="dependency_unmet",
            dependency_reason=reason,
        )
        self._store.acquire_lease(
            processor_key=item.processor_key,
            processor_version=processor_version,
            input_uid=item.input_uid,
            input_revision=revision,
            digest=digest,
            owner=run_id,
            lease_seconds=self._lease_seconds,
        )
        self._store.commit_receipt(
            receipt,
            scheduler_status=RECEIPT_STATUS_BLOCKED_DEPENDENCY,
            digest=digest,
            run_id=run_id,
            lease_owner=run_id,
        )
        return ItemExecuteResult(
            processor_key=item.processor_key,
            input_uid=item.input_uid,
            status=INPUT_STATUS_BLOCKED_DEPENDENCY,
            output_identity=item.output_identity,
            input_hash=item.current_input_hash,
            input_revision=revision,
            skip_reason=SKIP_BLOCKED_DEPENDENCY,
            error=reason,
            receipt=receipt,
            receipt_status=RECEIPT_STATUS_BLOCKED_DEPENDENCY,
        )

    def _commit_result(
        self,
        result: ItemExecuteResult,
        *,
        item: ProcessorPlanItem | None,
        processor_version: str,
        digest: str,
        run_id: str,
        lease_owner: str,
        input_revision: str = "",
    ) -> ItemExecuteResult:
        revision = (
            input_revision
            or result.input_revision
            or (item.input_revision if item else "")
            or result.input_hash
        )
        scheduler_status = scheduler_status_from_result(result)
        receipt = receipt_from_item_result(
            result,
            processor_version=processor_version,
            input_revision=revision,
            scheduler_status=scheduler_status,
        )
        accepted = self._store.commit_receipt(
            receipt,
            scheduler_status=scheduler_status,
            digest=digest,
            run_id=run_id,
            lease_owner=lease_owner,
        )
        if not accepted:
            result.status = RECEIPT_STATUS_SUPERSEDED
            result.skip_reason = result.skip_reason or "superseded_by_newer_revision"
            result.receipt_status = RECEIPT_STATUS_SUPERSEDED
            result.receipt = OutputReceipt(
                processor=receipt.processor,
                processor_version=receipt.processor_version,
                input_uid=receipt.input_uid,
                input_revision=receipt.input_revision,
                status="skipped",
                outputs=receipt.outputs,
                error_reason="superseded_by_newer_revision",
            )
            return result
        result.input_revision = revision
        result.receipt = receipt
        result.receipt_status = scheduler_status
        if scheduler_status == RECEIPT_STATUS_VALID_NO_OUTPUT:
            result.valid_no_output = True
            result.status = INPUT_STATUS_COMPLETE
        return result


@dataclass
class _DepDecision:
    block: bool = False
    already_current: bool = False
    reason: str = ""
    digest: str = ""

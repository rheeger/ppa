"""P03-A acceptance: a failed prerequisite blocks only its dependents."""

from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime, inspect_warehouse_card
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

FAIL_UID = "hfa-person-p03afail01"
OK_UID = "hfa-person-p03aok0001"


class ScenarioAssertionError(AssertionError):
    """P03-A product assertion failed."""


def _write_person(vault, *, uid: str, name: str, rel: str) -> None:
    card = PersonCard(
        uid=uid,
        type="person",
        source=["acceptance.p03a"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=name,
        first_name=name.split()[0],
        last_name=name.split()[-1],
        emails=[f"{uid}@example.test"],
        tags=["p03-acceptance", "synthetic"],
    )
    prov = {
        field: ProvenanceEntry("acceptance.p03a", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name", "emails", "tags")
    }
    write_card(vault, rel, card, body=f"{name} is a synthetic P03-A fixture.", provenance=prov)


def run_p03_dependencies(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    _write_person(runtime.vault, uid=FAIL_UID, name="P03 Fail Card", rel="People/p03-fail.md")
    _write_person(runtime.vault, uid=OK_UID, name="P03 Ok Card", rel="People/p03-ok.md")

    from archive_cli.store import DefaultArchiveStore
    from archive_sync.processors.batch import ProcessorPlanItem
    from archive_sync.processors.constants import (
        CORPUS_ACTIVE,
        INPUT_STATUS_BLOCKED_DEPENDENCY,
        INPUT_STATUS_COMPLETE,
        INPUT_STATUS_FAILED,
        PROCESSOR_EMBEDDING,
        PROCESSOR_LINKERS,
        PROCESSOR_MATERIALIZATION,
    )
    from archive_sync.processors.runner import (
        BatchExecuteResult,
        ExecuteContext,
        ItemExecuteResult,
        default_batch_executor,
        run_processors,
    )
    from archive_sync.processors.staleness import ProcessorInputSnapshot
    from archive_sync.processors.state_store import ProcessorStateStore

    store = DefaultArchiveStore(vault=runtime.vault)
    store.bootstrap()
    state = ProcessorStateStore(None, meta_path=runtime.vault / "_meta" / "processors.json")
    executed: list[tuple[str, str]] = []

    def _snap(uid: str) -> ProcessorInputSnapshot:
        return ProcessorInputSnapshot(
            input_uid=uid,
            card_type="person",
            corpus_state=CORPUS_ACTIVE,
            field_values={
                "body_sha": uid,
                "frontmatter_hash": uid,
                "chunk_hash": uid,
                "source_hash": uid,
                "target_hash": uid,
                "corpus_state": CORPUS_ACTIVE,
            },
            source_dirty=True,
            upstream_complete=True,
        )

    def _executor(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        key = items[0].processor_key
        if key == PROCESSOR_MATERIALIZATION:
            out = default_batch_executor(ctx, items)
            executed.extend((key, item.input_uid) for item in items)
            return out
        results = []
        for item in items:
            executed.append((key, item.input_uid))
            if key == PROCESSOR_EMBEDDING and item.input_uid == FAIL_UID:
                results.append(
                    ItemExecuteResult(
                        processor_key=key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        input_hash=item.current_input_hash,
                        error="injected embedding failure",
                    )
                )
                continue
            results.append(
                ItemExecuteResult(
                    processor_key=key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_COMPLETE,
                    output_uids=[item.input_uid],
                    input_hash=item.current_input_hash,
                )
            )
        return BatchExecuteResult(results=results)

    result = run_processors(
        inputs=[_snap(FAIL_UID), _snap(OK_UID)],
        vault_path=str(runtime.vault),
        store=store,
        state_store=state,
        processor_keys=[PROCESSOR_MATERIALIZATION, PROCESSOR_EMBEDDING, PROCESSOR_LINKERS],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03a-acceptance",
        batch_executor=_executor,
    )

    fail_row = inspect_warehouse_card(runtime.dsn, runtime.schema, FAIL_UID)
    ok_row = inspect_warehouse_card(runtime.dsn, runtime.schema, OK_UID)
    if fail_row is None or ok_row is None:
        raise ScenarioAssertionError(f"materialized rows missing fail={fail_row} ok={ok_row}")
    if (PROCESSOR_LINKERS, FAIL_UID) in executed:
        raise ScenarioAssertionError("blocked descendant called linker executor")
    if (PROCESSOR_LINKERS, OK_UID) not in executed:
        raise ScenarioAssertionError("independent card did not run linkers")
    blocked = [
        item
        for item in result.item_results
        if item.input_uid == FAIL_UID and item.processor_key == PROCESSOR_LINKERS
    ]
    if not blocked or blocked[0].status != INPUT_STATUS_BLOCKED_DEPENDENCY:
        raise ScenarioAssertionError(f"expected blocked linkers for failed card, got {blocked}")
    ok_mat = [
        item
        for item in result.item_results
        if item.input_uid == OK_UID and item.processor_key == PROCESSOR_MATERIALIZATION
    ]
    if not ok_mat or ok_mat[0].receipt is None or ok_mat[0].receipt.status != "completed":
        raise ScenarioAssertionError("completed card missing OutputReceipt")

    receipts = [item.to_payload() for item in result.receipts]
    events = list(result.report.scheduler_events)
    return {
        "id": "p03.dependencies_block_descendants",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "executed": executed,
        "warehouse": {"fail": fail_row, "ok": ok_row},
        "receipts": receipts,
        "scheduler_events": events,
        "blocked_count": result.report.blocked_count,
        "report_status": result.report.status,
        "schema": runtime.schema,
    }


register(
    Scenario(
        id="p03.dependencies_block_descendants",
        suite="p03",
        product_guarantee=(
            "A failed prerequisite blocks only that card's descendants; the "
            "independent card is materialized and its downstream work runs"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p03_dependencies,
    )
)

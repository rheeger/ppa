"""P03-A — per-revision dependency execution and durable receipts."""

from __future__ import annotations

from pathlib import Path

import pytest

from archive_cli.migrations import discover_migrations
from archive_engine.contracts import OutputReceipt
from archive_sync.processors.batch import ProcessorPlanItem
from archive_sync.processors.constants import (
    CORPUS_ACTIVE,
    DEP_CONDITIONAL,
    DEP_OPTIONAL,
    INPUT_STATUS_BLOCKED_DEPENDENCY,
    INPUT_STATUS_COMPLETE,
    INPUT_STATUS_FAILED,
    MATERIALIZATION_VERSION,
    PROCESSOR_EMBEDDING,
    PROCESSOR_ENTITY_RESOLUTION,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    RECEIPT_STATUS_SUPERSEDED,
    RECEIPT_STATUS_VALID_NO_OUTPUT,
)
from archive_sync.processors.declarations import (
    ProcessorDeclaration,
    ProcessorDependency,
    ProcessorGraphError,
    topological_order,
    validate_processor_graph,
)
from archive_sync.processors.runner import (
    BatchExecuteResult,
    ExecuteContext,
    ItemExecuteResult,
    run_processors,
)
from archive_sync.processors.scheduler import ProcessorScheduler
from archive_sync.processors.staleness import ProcessorInputSnapshot
from archive_sync.processors.state_store import ProcessorInputStateRecord, ProcessorStateStore


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Attachments", "EmailThreads", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    return vault


def _snap(uid: str, *, body: str = "") -> ProcessorInputSnapshot:
    token = body or uid
    return ProcessorInputSnapshot(
        input_uid=uid,
        card_type="person",
        corpus_state=CORPUS_ACTIVE,
        field_values={
            "body_sha": token,
            "frontmatter_hash": token,
            "chunk_hash": token,
            "source_hash": token,
            "target_hash": token,
            "corpus_state": CORPUS_ACTIVE,
        },
        source_dirty=True,
        upstream_complete=True,
    )


def _complete_executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    return BatchExecuteResult(
        results=[
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
    )


def test_migration_010_is_reserved_for_processor_receipts() -> None:
    migrations = discover_migrations()
    versions = [item.version for item in migrations]
    assert versions.count(10) == 1
    receipt = next(item for item in migrations if item.version == 10)
    assert receipt.name == "processor_receipts"


def test_cycle_detection_uses_visiting_visited() -> None:
    decls = (
        ProcessorDeclaration(
            processor_key=PROCESSOR_MATERIALIZATION,
            processor_version="v1",
            input_card_types=("person",),
            output_kinds=("cards",),
            output_identity="m:{input_uid}",
            input_hash_fields=("body_sha",),
            depends_on=(PROCESSOR_EMBEDDING,),
        ),
        ProcessorDeclaration(
            processor_key=PROCESSOR_EMBEDDING,
            processor_version="v1",
            input_card_types=("person",),
            output_kinds=("embeddings",),
            output_identity="e:{input_uid}",
            input_hash_fields=("chunk_hash",),
            depends_on=(PROCESSOR_MATERIALIZATION,),
        ),
    )
    with pytest.raises(ProcessorGraphError, match="cycle"):
        topological_order(decls, require_declared_deps=True)
    errors = validate_processor_graph(decls)
    assert any("cycle" in item for item in errors)


def test_unknown_dependency_is_rejected() -> None:
    decls = (
        ProcessorDeclaration(
            processor_key=PROCESSOR_MATERIALIZATION,
            processor_version="v1",
            input_card_types=("person",),
            output_kinds=("cards",),
            output_identity="m:{input_uid}",
            input_hash_fields=("body_sha",),
            depends_on=("not_a_processor",),
        ),
    )
    errors = validate_processor_graph(decls)
    assert any("unknown" in item for item in errors)


def test_failed_prerequisite_blocks_only_its_descendants(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    called: list[tuple[str, str]] = []

    def _executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        results = []
        for item in items:
            called.append((item.processor_key, item.input_uid))
            if item.processor_key == PROCESSOR_MATERIALIZATION and item.input_uid == "card-fail":
                results.append(
                    ItemExecuteResult(
                        processor_key=item.processor_key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        output_identity=item.output_identity,
                        input_hash=item.current_input_hash,
                        error="injected materialization failure",
                    )
                )
                continue
            results.append(
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_COMPLETE,
                    output_identity=item.output_identity,
                    output_uids=[item.input_uid],
                    input_hash=item.current_input_hash,
                )
            )
        return BatchExecuteResult(results=results)

    result = run_processors(
        inputs=[_snap("card-fail"), _snap("card-ok")],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION, PROCESSOR_EMBEDDING, PROCESSOR_LINKERS],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03a-block-one",
        repo_root=tmp_path,
        batch_executor=_executor,
    )
    assert ("embedding", "card-fail") not in called
    assert ("linkers", "card-fail") not in called
    assert ("materialization", "card-ok") in called
    assert ("embedding", "card-ok") in called
    blocked = [
        item
        for item in result.item_results
        if item.input_uid == "card-fail" and item.processor_key != PROCESSOR_MATERIALIZATION
    ]
    assert blocked
    assert all(item.status == INPUT_STATUS_BLOCKED_DEPENDENCY for item in blocked)
    ok_mat = next(
        item
        for item in result.item_results
        if item.processor_key == PROCESSOR_MATERIALIZATION and item.input_uid == "card-ok"
    )
    assert ok_mat.status == INPUT_STATUS_COMPLETE
    assert ok_mat.receipt is not None
    assert isinstance(ok_mat.receipt, OutputReceipt)
    assert ok_mat.receipt.status == "completed"
    assert ok_mat.receipt.outputs
    fail_embed = store.get_head_receipt(PROCESSOR_EMBEDDING, "card-fail")
    assert fail_embed is not None
    assert fail_embed.receipt.status == "dependency_unmet"


def test_legacy_input_state_complete_is_not_trusted(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    store.upsert_input_state(
        ProcessorInputStateRecord(
            processor_key=PROCESSOR_MATERIALIZATION,
            input_uid="legacy-uid",
            input_hash="will-not-match-unless-same",
            processor_version=MATERIALIZATION_VERSION,
            output_identity="materialization:legacy-uid:chunk",
            status=INPUT_STATUS_COMPLETE,
        )
    )
    called: list[str] = []

    def _executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        called.extend(item.input_uid for item in items)
        return _complete_executor(_ctx, items)

    result = run_processors(
        inputs=[_snap("legacy-uid")],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="p03a-legacy",
        repo_root=tmp_path,
        batch_executor=_executor,
    )
    assert "legacy-uid" in called
    assert any(item.receipt is not None and item.status == INPUT_STATUS_COMPLETE for item in result.item_results)


def test_rerun_is_already_current_after_receipt(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    first = run_processors(
        inputs=[_snap("rerun-uid")],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="p03a-rerun-1",
        repo_root=tmp_path,
        batch_executor=_complete_executor,
    )
    assert first.receipts
    second = run_processors(
        inputs=[_snap("rerun-uid")],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="p03a-rerun-2",
        repo_root=tmp_path,
        batch_executor=_complete_executor,
    )
    assert any(item.already_current for item in second.item_results)


def test_restart_retries_retryable_failure(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    attempts = {"n": 0}

    def _flaky(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        attempts["n"] += 1
        if attempts["n"] == 1:
            return BatchExecuteResult(
                results=[
                    ItemExecuteResult(
                        processor_key=item.processor_key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        input_hash=item.current_input_hash,
                        error="transient provider timeout",
                    )
                    for item in items
                ]
            )
        return _complete_executor(_ctx, items)

    first = run_processors(
        inputs=[_snap("retry-uid")],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="p03a-retry-1",
        repo_root=tmp_path,
        batch_executor=_flaky,
    )
    assert any(item.status == INPUT_STATUS_FAILED for item in first.item_results)
    second = run_processors(
        inputs=[_snap("retry-uid")],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="p03a-retry-2",
        repo_root=tmp_path,
        batch_executor=_flaky,
    )
    assert any(item.status == INPUT_STATUS_COMPLETE and item.receipt is not None for item in second.item_results)
    assert attempts["n"] == 2


def test_optional_failure_does_not_block(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    decls = (
        ProcessorDeclaration(
            processor_key=PROCESSOR_MATERIALIZATION,
            processor_version="v1",
            input_card_types=("person",),
            output_kinds=("cards",),
            output_identity="m:{input_uid}",
            input_hash_fields=("body_sha",),
        ),
        ProcessorDeclaration(
            processor_key=PROCESSOR_ENTITY_RESOLUTION,
            processor_version="v1",
            input_card_types=("person",),
            output_kinds=("person_links",),
            output_identity="er:{input_uid}",
            input_hash_fields=("body_sha",),
            dependencies=(ProcessorDependency(PROCESSOR_MATERIALIZATION, DEP_OPTIONAL),),
        ),
    )
    called: list[str] = []

    def _executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        results = []
        for item in items:
            called.append(item.processor_key)
            if item.processor_key == PROCESSOR_MATERIALIZATION:
                results.append(
                    ItemExecuteResult(
                        processor_key=item.processor_key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        input_hash=item.current_input_hash,
                        error="optional upstream failed",
                    )
                )
            else:
                results.append(
                    ItemExecuteResult(
                        processor_key=item.processor_key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_COMPLETE,
                        output_uids=[item.input_uid],
                        input_hash=item.current_input_hash,
                    )
                )
        return BatchExecuteResult(results=results)

    scheduler = ProcessorScheduler(store, declarations=decls, lease_owner="opt")
    item_m = ProcessorPlanItem(
        processor_key=PROCESSOR_MATERIALIZATION,
        input_uid="opt-uid",
        stale=True,
        current_input_hash="rev-opt",
        input_revision="rev-opt",
        processor_version="v1",
    )
    item_er = ProcessorPlanItem(
        processor_key=PROCESSOR_ENTITY_RESOLUTION,
        input_uid="opt-uid",
        stale=True,
        current_input_hash="rev-opt",
        input_revision="rev-opt",
        processor_version="v1",
    )
    out = scheduler.run_batches(
        ctx=ExecuteContext(vault_path=str(vault), apply=True, dry_run=False),
        by_key={PROCESSOR_MATERIALIZATION: [item_m], PROCESSOR_ENTITY_RESOLUTION: [item_er]},
        executor=_executor,
        run_id="p03a-optional",
        snapshots=[_snap("opt-uid")],
        decl_versions={PROCESSOR_MATERIALIZATION: "v1", PROCESSOR_ENTITY_RESOLUTION: "v1"},
    )
    assert PROCESSOR_ENTITY_RESOLUTION in called
    assert not any(item.status == INPUT_STATUS_BLOCKED_DEPENDENCY for item in out.item_results)


def test_conditional_dependency_only_when_matched(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    decls = (
        ProcessorDeclaration(
            processor_key=PROCESSOR_MATERIALIZATION,
            processor_version="v1",
            input_card_types=("person",),
            output_kinds=("cards",),
            output_identity="m:{input_uid}",
            input_hash_fields=("body_sha",),
        ),
        ProcessorDeclaration(
            processor_key=PROCESSOR_EMBEDDING,
            processor_version="v1",
            input_card_types=("person",),
            output_kinds=("embeddings",),
            output_identity="e:{input_uid}",
            input_hash_fields=("chunk_hash",),
            dependencies=(
                ProcessorDependency(PROCESSOR_MATERIALIZATION, DEP_CONDITIONAL, when="processor_decision=needs_embed"),
            ),
        ),
    )
    called: list[tuple[str, str]] = []

    def _executor(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        for item in items:
            called.append((item.processor_key, item.input_uid))
        return _complete_executor(_ctx, items)

    scheduler = ProcessorScheduler(store, declarations=decls, lease_owner="cond")
    snap_match = _snap("cond-match")
    snap_match.processor_decision = "needs_embed"
    snap_skip = _snap("cond-skip")
    snap_skip.processor_decision = "plain"
    items = []
    by_key: dict[str, list[ProcessorPlanItem]] = {PROCESSOR_MATERIALIZATION: [], PROCESSOR_EMBEDDING: []}
    for uid in ("cond-match", "cond-skip"):
        for key in (PROCESSOR_MATERIALIZATION, PROCESSOR_EMBEDDING):
            item = ProcessorPlanItem(
                processor_key=key,
                input_uid=uid,
                stale=True,
                current_input_hash=f"rev-{uid}",
                input_revision=f"rev-{uid}",
                processor_version="v1",
            )
            by_key[key].append(item)
            items.append(item)
    out = scheduler.run_batches(
        ctx=ExecuteContext(vault_path=str(vault), apply=True, dry_run=False),
        by_key=by_key,
        executor=_executor,
        run_id="p03a-cond",
        snapshots=[snap_match, snap_skip],
        decl_versions={PROCESSOR_MATERIALIZATION: "v1", PROCESSOR_EMBEDDING: "v1"},
    )
    assert ("embedding", "cond-match") in called
    assert ("embedding", "cond-skip") in called
    assert not any(item.status == INPUT_STATUS_BLOCKED_DEPENDENCY for item in out.item_results)


def test_valid_no_output_receipt(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")

    def _empty(_ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        return BatchExecuteResult(
            results=[
                ItemExecuteResult(
                    processor_key=item.processor_key,
                    input_uid=item.input_uid,
                    status=INPUT_STATUS_COMPLETE,
                    input_hash=item.current_input_hash,
                    valid_no_output=True,
                    receipt_status=RECEIPT_STATUS_VALID_NO_OUTPUT,
                )
                for item in items
            ]
        )

    result = run_processors(
        inputs=[_snap("empty-uid")],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="p03a-empty",
        repo_root=tmp_path,
        batch_executor=_empty,
    )
    rec = next(item.receipt for item in result.item_results if item.receipt is not None)
    assert rec.status == "completed"
    assert rec.outputs == ()
    stored = store.get_head_receipt(PROCESSOR_MATERIALIZATION, "empty-uid")
    assert stored is not None
    assert stored.scheduler_status == RECEIPT_STATUS_VALID_NO_OUTPUT


def test_old_revision_cannot_overwrite_newer_head(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    assert store.acquire_lease(
        processor_key=PROCESSOR_MATERIALIZATION,
        processor_version="v1",
        input_uid="cas-uid",
        input_revision="rev-old",
        digest="",
        owner="worker-old",
    )
    assert store.acquire_lease(
        processor_key=PROCESSOR_MATERIALIZATION,
        processor_version="v1",
        input_uid="cas-uid",
        input_revision="rev-new",
        digest="",
        owner="worker-new",
    )
    from archive_engine.contracts import OutputReceipt as OR
    from archive_engine.contracts import OutputRevision

    old = OR(
        processor=PROCESSOR_MATERIALIZATION,
        processor_version="v1",
        input_uid="cas-uid",
        input_revision="rev-old",
        status="completed",
        outputs=(OutputRevision(uid="cas-uid", revision="rev-old"),),
    )
    assert (
        store.commit_receipt(old, scheduler_status="complete", digest="", run_id="old", lease_owner="worker-old")
        is False
    )
    head = store.get_head_receipt(PROCESSOR_MATERIALIZATION, "cas-uid")
    assert head is not None
    assert head.receipt.input_revision == "rev-new"
    stored_old = store.get_stored_receipt(PROCESSOR_MATERIALIZATION, "v1", "cas-uid", "rev-old", "")
    assert stored_old is not None
    assert stored_old.scheduler_status == RECEIPT_STATUS_SUPERSEDED


def test_real_materialization_side_effects_for_completed_card(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)

    class _Store:
        def __init__(self) -> None:
            self.rebuild_calls: list[dict] = []

        def rebuild(self, **kwargs):
            self.rebuild_calls.append(dict(kwargs))
            return {"cards": 2}

    index_store = _Store()
    state = ProcessorStateStore(None, meta_path=vault / "_meta" / "processors.json")
    called: list[tuple[str, str]] = []

    def _executor(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        key = items[0].processor_key
        if key == PROCESSOR_MATERIALIZATION:
            from archive_sync.processors.runner import default_batch_executor

            out = default_batch_executor(ctx, items)
            for item in items:
                called.append((key, item.input_uid))
            return out
        results = []
        for item in items:
            called.append((key, item.input_uid))
            if item.input_uid == "hfa-person-p03afail01":
                results.append(
                    ItemExecuteResult(
                        processor_key=key,
                        input_uid=item.input_uid,
                        status=INPUT_STATUS_FAILED,
                        input_hash=item.current_input_hash,
                        error="injected downstream",
                    )
                )
            else:
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
        inputs=[_snap("hfa-person-p03afail01"), _snap("hfa-person-p03aok0001")],
        vault_path=str(vault),
        store=index_store,
        state_store=state,
        processor_keys=[PROCESSOR_MATERIALIZATION, PROCESSOR_EMBEDDING, PROCESSOR_LINKERS],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03a-real-mat",
        repo_root=tmp_path,
        batch_executor=_executor,
    )
    assert index_store.rebuild_calls
    assert index_store.rebuild_calls[0]["force_full"] is False
    assert index_store.rebuild_calls[0]["uid_allowlist"] == {
        "hfa-person-p03afail01",
        "hfa-person-p03aok0001",
    }
    assert ("linkers", "hfa-person-p03afail01") not in called
    assert ("embedding", "hfa-person-p03aok0001") in called
    assert ("linkers", "hfa-person-p03aok0001") in called
    assert any(item.receipt is not None for item in result.item_results if item.input_uid == "hfa-person-p03aok0001")


@pytest.mark.integration
def test_receipts_round_trip_postgres(pgvector_dsn: str, tmp_path: Path) -> None:
    from psycopg import connect
    from psycopg.rows import dict_row

    schema = "plan_p03a_receipts"
    with connect(pgvector_dsn, row_factory=dict_row, autocommit=True) as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        store = ProcessorStateStore(conn, schema=schema)
        store.ensure_tables()
        result = run_processors(
            inputs=[_snap("pg-uid")],
            vault_path=str(_minimal_vault(tmp_path)),
            state_store=store,
            processor_keys=[PROCESSOR_MATERIALIZATION],
            apply=True,
            dry_run=False,
            run_id="p03a-pg",
            repo_root=tmp_path,
            batch_executor=_complete_executor,
        )
        assert result.receipts
        head = store.get_head_receipt(PROCESSOR_MATERIALIZATION, "pg-uid")
        assert head is not None
        assert head.receipt.status == "completed"
        row = conn.execute(
            f"SELECT status, contract_status FROM {schema}.processor_receipts WHERE input_uid = %s",
            ("pg-uid",),
        ).fetchone()
        assert row is not None
        assert row["contract_status"] == "completed"

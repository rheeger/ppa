"""Section E Phase 2 — processor DAG execution tests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from archive_cli.processors.cli import cmd_run, cmd_status
from archive_cli.validation_gates.constants import EXIT_BLOCKED, EXIT_REFUSED
from archive_sync.processors.batch import ProcessorPlanItem
from archive_sync.processors.constants import (
    CORPUS_ACTIVE,
    CORPUS_SUPPRESSED,
    PROCESSOR_EMAIL_THREAD_ENRICHMENT,
    PROCESSOR_EMBEDDING,
    PROCESSOR_ENTITY_RESOLUTION,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    SECTION_E_EXECUTION_STATE,
    SKIP_SUPPRESSED,
)
from archive_sync.processors.dirty_io import load_dirty_uids, resolve_snapshots_for_uids
from archive_sync.processors.runner import (
    BatchExecuteResult,
    ExecuteContext,
    ItemExecuteResult,
    run_processors,
)
from archive_sync.processors.staleness import ProcessorInputSnapshot
from archive_sync.processors.state_store import ProcessorStateStore


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Attachments", "EmailThreads", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    return vault


def _fixture_executor(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
    """Deterministic executor — no LLM, no vault mutation beyond state store."""

    results = [
        ItemExecuteResult(
            processor_key=item.processor_key,
            input_uid=item.input_uid,
            status="complete",
            output_identity=item.output_identity,
            output_uids=[f"out:{item.input_uid}"],
            input_hash=item.current_input_hash,
        )
        for item in items
    ]
    return BatchExecuteResult(results=results)


def test_load_dirty_uids_jsonl(tmp_path: Path) -> None:
    path = tmp_path / "dirty_uids.jsonl"
    path.write_text("uid-a\nuid-b\n# comment\nuid-c\n", encoding="utf-8")
    assert load_dirty_uids(path) == ["uid-a", "uid-b", "uid-c"]


def test_dirty_uids_jsonl_plan_apply_executes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    dirty = tmp_path / "dirty_uids.jsonl"
    dirty.write_text("email-thread-fixture-1\n", encoding="utf-8")
    meta = vault / "_meta" / "processors.json"
    store = ProcessorStateStore(None, meta_path=meta)

    result = run_processors(
        dirty_uids_path=dirty,
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="e2-fixture-apply",
        archive_instance="fixture:test",
        engine_mode="python",
        ladder_gate="synthetic_fixtures",
        repo_root=tmp_path,
        batch_executor=_fixture_executor,
        default_card_type="email_thread",
    )
    assert result.executed is True
    assert result.report.output_count >= 1
    assert any(r.status == "complete" for r in result.item_results)
    assert result.artifact_paths.get("report")
    prior = store.get_input_state(PROCESSOR_MATERIALIZATION, "email-thread-fixture-1")
    assert prior is not None
    assert prior.status == "complete"
    assert prior.output_identity


def test_idempotent_rerun_skips_already_current(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    dirty = tmp_path / "dirty_uids.jsonl"
    dirty.write_text("email-thread-idem-1\n", encoding="utf-8")
    meta = vault / "_meta" / "processors.json"
    store = ProcessorStateStore(None, meta_path=meta)

    first = run_processors(
        dirty_uids_path=dirty,
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="e2-idem-1",
        repo_root=tmp_path,
        batch_executor=_fixture_executor,
    )
    assert first.report.output_count >= 1

    second = run_processors(
        dirty_uids_path=dirty,
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        run_id="e2-idem-2",
        repo_root=tmp_path,
        batch_executor=_fixture_executor,
    )
    assert any(r.already_current for r in second.item_results)
    assert second.report.skip_reasons.get("already_current", 0) >= 1


def test_suppressed_inputs_skip_active_only_on_apply(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    meta = vault / "_meta" / "processors.json"
    store = ProcessorStateStore(None, meta_path=meta)
    snap = ProcessorInputSnapshot(
        input_uid="email-thread-suppressed",
        card_type="email_thread",
        corpus_state=CORPUS_SUPPRESSED,
        processor_decision="typed_extraction",
        field_values={"body_sha": "x", "chunk_hash": "x", "corpus_state": CORPUS_SUPPRESSED},
        source_dirty=True,
    )
    result = run_processors(
        inputs=[snap],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_EMBEDDING],
        apply=True,
        dry_run=False,
        run_id="e2-suppress",
        repo_root=tmp_path,
        batch_executor=_fixture_executor,
    )
    assert result.executed is True
    assert all(r.status == "skipped" for r in result.item_results)
    assert any(r.skip_reason == SKIP_SUPPRESSED for r in result.item_results)
    assert result.report.output_count == 0


def test_llm_failure_does_not_block_deterministic(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    meta = vault / "_meta" / "processors.json"
    store = ProcessorStateStore(None, meta_path=meta)
    snap = ProcessorInputSnapshot(
        input_uid="email-thread-mix-1",
        card_type="email_thread",
        corpus_state=CORPUS_ACTIVE,
        processor_decision="thread_enrichment",
        field_values={"body_sha": "mix", "thread_uid": "email-thread-mix-1", "corpus_state": CORPUS_ACTIVE},
        source_dirty=True,
    )

    def _mixed_executor(ctx: ExecuteContext, items: list[ProcessorPlanItem]) -> BatchExecuteResult:
        key = items[0].processor_key
        if key == PROCESSOR_EMAIL_THREAD_ENRICHMENT:
            raise RuntimeError("provider boom")
        return _fixture_executor(ctx, items)

    result = run_processors(
        inputs=[snap],
        vault_path=str(vault),
        state_store=store,
        processor_keys=[PROCESSOR_MATERIALIZATION, PROCESSOR_EMAIL_THREAD_ENRICHMENT],
        apply=True,
        dry_run=False,
        allow_broad_llm=True,
        provider_available=True,
        run_id="e2-isolate",
        repo_root=tmp_path,
        batch_executor=_mixed_executor,
    )
    mat = [r for r in result.item_results if r.processor_key == PROCESSOR_MATERIALIZATION]
    enrich = [r for r in result.item_results if r.processor_key == PROCESSOR_EMAIL_THREAD_ENRICHMENT]
    assert mat and mat[0].status == "complete"
    assert enrich and enrich[0].status == "failed"
    assert result.report.status in ("partial", "success", "failed")


def test_cli_run_apply_materialization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    dirty = tmp_path / "dirty_uids.jsonl"
    dirty.write_text("email-thread-cli-1\n", encoding="utf-8")

    # Patch runner's default executor via run_processors batch_executor by patching default_batch_executor
    import archive_sync.processors.runner as runner_mod

    monkeypatch.setattr(runner_mod, "default_batch_executor", _fixture_executor)

    args = argparse.Namespace(
        processor=PROCESSOR_MATERIALIZATION,
        vault=str(vault),
        instance_role="fixture",
        format="json",
        run_id="cli-apply-1",
        decision_run_id="",
        ladder_gate="synthetic_fixtures",
        dirty_uids=str(dirty),
        apply=True,
        dry_run=False,
        allow_full_embedding=False,
        allow_all_linkers=False,
        allow_broad_llm=False,
        require_full_embedding_opt_in=False,
        require_all_linkers_opt_in=False,
        require_provider=False,
    )
    rc = cmd_run(args)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is True
    assert payload["execution_state"] == SECTION_E_EXECUTION_STATE
    assert payload["output_count"] >= 1


def test_cli_run_without_apply_is_plan_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    dirty = tmp_path / "dirty_uids.jsonl"
    dirty.write_text("email-thread-plan-1\n", encoding="utf-8")
    args = argparse.Namespace(
        processor=PROCESSOR_MATERIALIZATION,
        vault=str(vault),
        instance_role="fixture",
        format="json",
        run_id="cli-plan-1",
        decision_run_id="",
        ladder_gate="synthetic_fixtures",
        dirty_uids=str(dirty),
        apply=False,
        dry_run=True,
        allow_full_embedding=False,
        allow_all_linkers=False,
        allow_broad_llm=False,
        require_full_embedding_opt_in=False,
        require_all_linkers_opt_in=False,
        require_provider=False,
    )
    rc = cmd_run(args)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is False
    assert payload["plan_only"] is True


def test_enrichment_without_broad_llm_opt_in_refused(capsys: pytest.CaptureFixture) -> None:
    args = argparse.Namespace(
        processor=PROCESSOR_EMAIL_THREAD_ENRICHMENT,
        vault="",
        instance_role="",
        format="json",
        run_id="",
        decision_run_id="",
        ladder_gate="synthetic_fixtures",
        dirty_uids="",
        apply=True,
        dry_run=False,
        allow_full_embedding=False,
        allow_all_linkers=False,
        allow_broad_llm=False,
        require_full_embedding_opt_in=False,
        require_all_linkers_opt_in=False,
        require_provider=False,
    )
    rc = cmd_run(args)
    assert rc == EXIT_REFUSED
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    payload = json.loads(captured.out)
    assert payload["refused"] is True


def test_embedding_full_opt_in_required_when_flag_set(capsys: pytest.CaptureFixture) -> None:
    args = argparse.Namespace(
        processor=PROCESSOR_EMBEDDING,
        vault="",
        instance_role="",
        format="json",
        run_id="",
        decision_run_id="",
        ladder_gate="synthetic_fixtures",
        dirty_uids="",
        apply=True,
        dry_run=False,
        allow_full_embedding=False,
        allow_all_linkers=False,
        allow_broad_llm=False,
        require_full_embedding_opt_in=True,
        require_all_linkers_opt_in=False,
        require_provider=False,
    )
    rc = cmd_run(args)
    assert rc == EXIT_REFUSED
    payload = json.loads(capsys.readouterr().out)
    assert payload["refused"] is True


def test_missing_vault_returns_blocked_exit_4(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    monkeypatch.setenv("PPA_PATH", "/nonexistent/vault-section-e2")
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    args = argparse.Namespace(vault="", instance_role="", format="json")
    rc = cmd_status(args)
    assert rc == EXIT_BLOCKED
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert json.loads(captured.out)["blocked"] is True


class _MockIndex:
    vault = ""


class _MockStore:
    def __init__(self, vault: Path) -> None:
        self.vault = vault
        self.index = _MockIndex()
        self.index.vault = str(vault)
        self.rebuild_calls: list[dict] = []
        self.embed_calls: list[dict] = []

    def rebuild(self, **kwargs):
        self.rebuild_calls.append(dict(kwargs))
        return {"cards": 1}

    def embed_pending(self, **kwargs):
        self.embed_calls.append(dict(kwargs))
        return {"embedded": 1, "failed": 0}


def _active_snap(uid: str, *, processor_decision: str = "typed_extraction") -> ProcessorInputSnapshot:
    return ProcessorInputSnapshot(
        input_uid=uid,
        card_type="email_thread",
        corpus_state=CORPUS_ACTIVE,
        processor_decision=processor_decision,
        field_values={
            "body_sha": uid,
            "chunk_hash": uid,
            "thread_uid": uid,
            "corpus_state": CORPUS_ACTIVE,
            "source_hash": uid,
            "target_hash": uid,
        },
        source_dirty=True,
    )


def _apply_default(
    tmp_path: Path,
    *,
    processor_key: str,
    uid: str,
    store: _MockStore,
    processor_decision: str = "typed_extraction",
    allow_full_embedding: bool = False,
    allow_all_linkers: bool = False,
    allow_broad_llm: bool = False,
    provider_available: bool = True,
):
    vault = store.vault
    meta = vault / "_meta" / "processors.json"
    state = ProcessorStateStore(None, meta_path=meta)
    return run_processors(
        inputs=[_active_snap(uid, processor_decision=processor_decision)],
        vault_path=str(vault),
        store=store,
        state_store=state,
        processor_keys=[processor_key],
        apply=True,
        dry_run=False,
        allow_full_embedding=allow_full_embedding,
        allow_all_linkers=allow_all_linkers,
        allow_broad_llm=allow_broad_llm,
        provider_available=provider_available,
        run_id=f"e2-adapter-{processor_key}",
        repo_root=tmp_path,
    )


def test_apply_materialization_calls_incremental_rebuild(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    result = _apply_default(tmp_path, processor_key=PROCESSOR_MATERIALIZATION, uid="uid-mat-1", store=store)
    assert result.executed is True
    assert store.rebuild_calls
    assert store.rebuild_calls[0]["force_full"] is False
    assert "workers" in store.rebuild_calls[0]
    assert all(r.status == "complete" for r in result.item_results)


def test_apply_embedding_calls_embed_pending_dirty_limit(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    result = _apply_default(tmp_path, processor_key=PROCESSOR_EMBEDDING, uid="uid-emb-1", store=store)
    assert result.executed is True
    assert store.embed_calls
    assert store.embed_calls[0]["limit"] == 1
    assert store.embed_calls[0]["limit"] != 0


def test_apply_embedding_full_backlog_requires_opt_in(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    result = _apply_default(
        tmp_path,
        processor_key=PROCESSOR_EMBEDDING,
        uid="uid-emb-full",
        store=store,
        allow_full_embedding=True,
    )
    assert store.embed_calls
    assert store.embed_calls[0]["limit"] == 0
    assert result.executed is True


def test_apply_linkers_calls_incremental_refresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    calls: list[dict] = []

    def _refresh(index, *, source_uids, **kwargs):
        calls.append({"index": index, "source_uids": list(source_uids), **kwargs})
        return {"jobs_completed": 1}

    def _backfill(*_a, **_k):
        raise AssertionError("all-linker backfill must not run without --allow-all-linkers")

    monkeypatch.setattr("archive_cli.seed_links.run_incremental_link_refresh", _refresh)
    monkeypatch.setattr("archive_cli.seed_links.run_seed_link_backfill", _backfill)
    result = _apply_default(tmp_path, processor_key=PROCESSOR_LINKERS, uid="uid-link-1", store=store)
    assert result.executed is True
    assert calls
    assert calls[0]["source_uids"] == ["uid-link-1"]
    assert calls[0]["include_llm"] is False


def test_apply_linkers_all_requires_opt_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    backfill_calls: list[dict] = []

    def _refresh(*_a, **_k):
        raise AssertionError("incremental refresh should not run when allow_all_linkers is set")

    def _backfill(index, **kwargs):
        backfill_calls.append(dict(kwargs))
        return {"jobs_completed": 2}

    monkeypatch.setattr("archive_cli.seed_links.run_incremental_link_refresh", _refresh)
    monkeypatch.setattr("archive_cli.seed_links.run_seed_link_backfill", _backfill)
    result = _apply_default(
        tmp_path,
        processor_key=PROCESSOR_LINKERS,
        uid="uid-link-all",
        store=store,
        allow_all_linkers=True,
        allow_broad_llm=True,
    )
    assert result.executed is True
    assert backfill_calls
    assert backfill_calls[0].get("source_uids") in (None, set())


def test_apply_enrichment_calls_run_enrichment_for_uids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    calls: list[tuple] = []

    def _enrich(vault_path, uids, **kwargs):
        calls.append((vault_path, list(uids), kwargs))
        return {"ok": True}

    monkeypatch.setattr(
        "archive_sync.llm_enrichment.enrichment_orchestrator.run_enrichment_for_uids",
        _enrich,
    )
    result = _apply_default(
        tmp_path,
        processor_key=PROCESSOR_EMAIL_THREAD_ENRICHMENT,
        uid="uid-enr-1",
        store=store,
        processor_decision="thread_enrichment",
        allow_broad_llm=True,
    )
    assert result.executed is True
    assert calls
    assert calls[0][1] == ["uid-enr-1"]
    assert calls[0][2].get("workflow") == "email_thread"


def test_apply_enrichment_without_broad_llm_skips(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)

    def _enrich(*_a, **_k):
        raise AssertionError("enrichment must not run without --allow-broad-llm")

    monkeypatch.setattr(
        "archive_sync.llm_enrichment.enrichment_orchestrator.run_enrichment_for_uids",
        _enrich,
    )
    result = _apply_default(
        tmp_path,
        processor_key=PROCESSOR_EMAIL_THREAD_ENRICHMENT,
        uid="uid-enr-skip",
        store=store,
        processor_decision="thread_enrichment",
        allow_broad_llm=False,
    )
    assert result.executed is True
    assert all(r.status == "skipped" for r in result.item_results)
    assert any(r.skip_reason == "missing_broad_llm_opt_in" for r in result.item_results)


def test_apply_entity_resolution_passes_uid_allowlist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    calls: list[dict] = []

    def _er(vault_path, **kwargs):
        calls.append({"vault_path": vault_path, **kwargs})
        return {"places_created": 0}

    monkeypatch.setattr("archive_sync.extractors.entity_resolution.run_entity_resolution", _er)
    result = _apply_default(
        tmp_path,
        processor_key=PROCESSOR_ENTITY_RESOLUTION,
        uid="uid-er-1",
        store=store,
        allow_broad_llm=True,
    )
    assert result.executed is True
    assert calls
    assert calls[0]["uid_allowlist"] == {"uid-er-1"}
    assert calls[0]["dry_run"] is False


def test_apply_entity_resolution_without_broad_llm_skips(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)

    def _er(*_a, **_k):
        raise AssertionError("entity resolution must not run without --allow-broad-llm")

    monkeypatch.setattr("archive_sync.extractors.entity_resolution.run_entity_resolution", _er)
    result = _apply_default(
        tmp_path,
        processor_key=PROCESSOR_ENTITY_RESOLUTION,
        uid="uid-er-skip",
        store=store,
        allow_broad_llm=False,
    )
    assert all(r.status == "skipped" for r in result.item_results)
    assert any(r.skip_reason == "missing_broad_llm_opt_in" for r in result.item_results)


def test_suppressed_inputs_skip_without_calling_embed_pending(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    store = _MockStore(vault)
    meta = vault / "_meta" / "processors.json"
    state = ProcessorStateStore(None, meta_path=meta)
    snap = ProcessorInputSnapshot(
        input_uid="email-thread-suppressed-adapter",
        card_type="email_thread",
        corpus_state=CORPUS_SUPPRESSED,
        processor_decision="typed_extraction",
        field_values={"body_sha": "x", "chunk_hash": "x", "corpus_state": CORPUS_SUPPRESSED},
        source_dirty=True,
    )
    result = run_processors(
        inputs=[snap],
        vault_path=str(vault),
        store=store,
        state_store=state,
        processor_keys=[PROCESSOR_EMBEDDING],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="e2-suppress-adapter",
        repo_root=tmp_path,
    )
    assert result.executed is True
    assert store.embed_calls == []
    assert all(r.status == "skipped" for r in result.item_results)
    assert any(r.skip_reason == SKIP_SUPPRESSED for r in result.item_results)


def test_run_enrichment_for_uids_uses_card_runner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from archive_sync.llm_enrichment.enrichment_orchestrator import run_enrichment_for_uids

    vault = _minimal_vault(tmp_path)
    seen: list[object] = []

    class _FakeMetrics:
        def to_dict(self) -> dict:
            return {"ok": True}

    class _FakeRunner:
        def __init__(self, **kwargs):
            seen.append(kwargs)

        def run(self):
            return _FakeMetrics()

    monkeypatch.setattr(
        "archive_sync.llm_enrichment.enrichment_orchestrator.CardEnrichmentRunner",
        _FakeRunner,
    )
    out = run_enrichment_for_uids(vault, ["uid-a", "uid-b"], dry_run=True, workers=2, run_id="enrich-test")
    assert seen
    kwargs = seen[0]
    assert kwargs["workflow"] == "email_thread"
    uid_file = Path(kwargs["uid_filter_file"])
    assert uid_file.is_file()
    lines = [ln for ln in uid_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert set(lines) == {"uid-a", "uid-b"}
    assert out.to_dict()["ok"] is True


def test_resolve_snapshots_defaults_active(tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    snaps = resolve_snapshots_for_uids(
        ["uid-1"],
        vault_path=vault,
        default_card_type="email_thread",
        source_dirty=True,
    )
    assert len(snaps) == 1
    assert snaps[0].corpus_state == CORPUS_ACTIVE
    assert snaps[0].source_dirty is True

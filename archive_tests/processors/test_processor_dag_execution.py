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

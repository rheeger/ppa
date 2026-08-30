"""Section E — processor DAG contract tests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from archive_cli.processors.cli import cmd_plan, cmd_run, cmd_status
from archive_cli.validation_gates.constants import EXIT_BLOCKED, EXIT_REFUSED
from archive_sync.processors.batch import ProcessorRunReport
from archive_sync.processors.constants import (
    CORPUS_ACTIVE,
    CORPUS_QUARANTINE,
    CORPUS_SUPPRESSED,
    EMAIL_TYPED_EXTRACTION_VERSION,
    PROCESSOR_EMAIL_TYPED_EXTRACTION,
    PROCESSOR_EMBEDDING,
    PROCESSOR_LINKERS,
    PROCESSOR_MATERIALIZATION,
    SKIP_QUARANTINE,
    SKIP_SUPPRESSED,
    STALE_DIRTY_INPUT,
    STALE_INPUT_HASH,
    STALE_PROCESSOR_VERSION,
)
from archive_sync.processors.declarations import (
    iter_processor_declarations,
    validate_all_declarations,
    validate_declaration,
)
from archive_sync.processors.input_hash import compute_input_hash
from archive_sync.processors.plan import build_processor_plan, processors_for_dirty_input
from archive_sync.processors.staleness import ProcessorInputSnapshot, evaluate_staleness
from archive_sync.processors.state_store import ProcessorInputStateRecord, ProcessorStateStore


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Attachments", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    return vault


def _active_email_snapshot(
    uid: str = "email-thread-1",
    *,
    processor_decision: str = "typed_extraction",
    body_sha: str = "sha-v1",
    source_dirty: bool = True,
    **kwargs: object,
) -> ProcessorInputSnapshot:
    return ProcessorInputSnapshot(
        input_uid=uid,
        card_type="email_thread",
        corpus_state=CORPUS_ACTIVE,
        processor_decision=processor_decision,
        field_values={"body_sha": body_sha, "thread_uid": uid},
        source_dirty=source_dirty,
        upstream_complete=True,
        **kwargs,  # type: ignore[arg-type]
    )


def test_declaration_validation_passes_for_all_processors() -> None:
    errors = validate_all_declarations()
    assert errors == {}
    keys = {d.processor_key for d in iter_processor_declarations()}
    assert keys == {
        "email_promotion_policy",
        "email_typed_extraction",
        "email_thread_enrichment",
        "materialization",
        "embedding",
        "linkers",
        "entity_resolution",
    }
    for decl in iter_processor_declarations():
        assert validate_declaration(decl) == []
        assert decl.processor_key
        assert decl.processor_version
        assert decl.output_identity
        assert decl.input_hash_fields
        assert decl.rollback_strategy


def test_input_hash_change_marks_output_stale() -> None:
    decl = next(d for d in iter_processor_declarations() if d.processor_key == PROCESSOR_EMAIL_TYPED_EXTRACTION)
    snap = _active_email_snapshot(body_sha="sha-v1", recorded_input_hash="sha-old", output_exists=True)
    h_new = compute_input_hash(
        input_uid=snap.input_uid,
        fields={**snap.field_values, "corpus_state": snap.corpus_state, "processor_decision": snap.processor_decision},
        hash_field_names=decl.input_hash_fields,
        processor_version=decl.processor_version,
    )
    result = evaluate_staleness(decl, snap, current_input_hash=h_new)
    assert result.stale is True
    assert STALE_INPUT_HASH in result.stale_reasons


def test_processor_version_bump_marks_expected_inputs_stale() -> None:
    decl = next(d for d in iter_processor_declarations() if d.processor_key == PROCESSOR_EMAIL_TYPED_EXTRACTION)
    snap = _active_email_snapshot(
        recorded_processor_version="email-typed-extraction-v0",
        recorded_input_hash="same",
        output_exists=True,
    )
    h = compute_input_hash(
        input_uid=snap.input_uid,
        fields={**snap.field_values, "corpus_state": snap.corpus_state, "processor_decision": snap.processor_decision},
        hash_field_names=decl.input_hash_fields,
        processor_version=decl.processor_version,
    )
    snap.recorded_input_hash = h
    result = evaluate_staleness(decl, snap, current_input_hash=h)
    assert STALE_PROCESSOR_VERSION in result.stale_reasons


@pytest.mark.parametrize(
    "corpus_state,expected_skip",
    [
        (CORPUS_SUPPRESSED, SKIP_SUPPRESSED),
        (CORPUS_QUARANTINE, SKIP_QUARANTINE),
    ],
)
def test_active_only_processors_skip_suppressed_quarantine(
    corpus_state: str,
    expected_skip: str,
) -> None:
    decl = next(d for d in iter_processor_declarations() if d.processor_key == PROCESSOR_EMBEDDING)
    snap = ProcessorInputSnapshot(
        input_uid="email-thread-suppressed",
        card_type="email_thread",
        corpus_state=corpus_state,
        processor_decision="typed_extraction",
        field_values={"chunk_hash": "c1"},
        source_dirty=True,
        upstream_complete=True,
    )
    h = compute_input_hash(
        input_uid=snap.input_uid,
        fields={**snap.field_values, "corpus_state": snap.corpus_state},
        hash_field_names=decl.input_hash_fields,
        processor_version=decl.processor_version,
    )
    result = evaluate_staleness(decl, snap, current_input_hash=h)
    assert result.skipped is True
    assert result.skip_reason == expected_skip


def test_dirty_input_triggers_only_expected_processors() -> None:
    snap = _active_email_snapshot(processor_decision="typed_extraction", source_dirty=True)
    triggered = processors_for_dirty_input(snap)
    assert PROCESSOR_MATERIALIZATION in triggered
    assert PROCESSOR_EMAIL_TYPED_EXTRACTION in triggered
    assert PROCESSOR_EMBEDDING in triggered
    assert PROCESSOR_LINKERS in triggered

    suppressed = ProcessorInputSnapshot(
        input_uid="email-thread-suppressed",
        card_type="email_thread",
        corpus_state=CORPUS_SUPPRESSED,
        processor_decision="typed_extraction",
        field_values={"body_sha": "x", "thread_uid": "email-thread-suppressed"},
        source_dirty=True,
    )
    plan = build_processor_plan([suppressed])
    embedding_items = [i for i in plan.items if i.processor_key == PROCESSOR_EMBEDDING]
    assert embedding_items
    assert all(i.skipped for i in embedding_items)


def test_source_dirty_marks_stale() -> None:
    decl = next(d for d in iter_processor_declarations() if d.processor_key == PROCESSOR_MATERIALIZATION)
    snap = _active_email_snapshot(source_dirty=True, output_exists=True)
    h = compute_input_hash(
        input_uid=snap.input_uid,
        fields={**snap.field_values, "corpus_state": snap.corpus_state},
        hash_field_names=decl.input_hash_fields,
        processor_version=decl.processor_version,
    )
    snap.recorded_input_hash = h
    result = evaluate_staleness(decl, snap, current_input_hash=h)
    assert STALE_DIRTY_INPUT in result.stale_reasons


def test_processor_state_persistence_meta_fallback(tmp_path: Path) -> None:
    meta = tmp_path / "processors.json"
    store = ProcessorStateStore(None, meta_path=meta)
    report = ProcessorRunReport(
        run_id="proc-run-1",
        processor_key=PROCESSOR_MATERIALIZATION,
        processor_version="materialization-v1",
        status="success",
        stale_count=2,
    )
    store.record_run(report)
    state = store.get_state(PROCESSOR_MATERIALIZATION)
    assert state is not None
    assert state.last_run_id == "proc-run-1"
    assert state.stale_count == 2

    store.upsert_input_state(
        ProcessorInputStateRecord(
            processor_key=PROCESSOR_EMAIL_TYPED_EXTRACTION,
            input_uid="email-thread-1",
            input_hash="abc",
            output_identity=f"{PROCESSOR_EMAIL_TYPED_EXTRACTION}:email-thread-1:{EMAIL_TYPED_EXTRACTION_VERSION}",
            status="complete",
            last_run_id="proc-run-1",
        )
    )
    loaded = store.get_input_state(PROCESSOR_EMAIL_TYPED_EXTRACTION, "email-thread-1")
    assert loaded is not None
    assert loaded.output_identity.startswith(PROCESSOR_EMAIL_TYPED_EXTRACTION)


def test_status_read_does_not_execute_processors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)

    def _boom(*_a, **_k):
        raise AssertionError("processors must not execute for status read")

    monkeypatch.setattr("archive_cli.embedder.run_embedder", _boom, raising=False)
    monkeypatch.setattr("archive_cli.seed_links.run_seed_links", _boom, raising=False)

    args = argparse.Namespace(vault=str(vault), instance_role="fixture", format="json")
    rc = cmd_status(args)
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert "processors" in out
    assert len(out["processors"]) == 7


def test_status_missing_vault_returns_blocked_exit_4(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    monkeypatch.setenv("PPA_PATH", "/nonexistent/vault-section-e")
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    args = argparse.Namespace(vault="", instance_role="", format="json")
    rc = cmd_status(args)
    assert rc == EXIT_BLOCKED
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    payload = json.loads(captured.out)
    assert payload["blocked"] is True


def test_plan_dry_run_without_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    inputs_path = tmp_path / "dirty.json"
    inputs_path.write_text(
        json.dumps(
            [
                {
                    "input_uid": "email-thread-plan-1",
                    "card_type": "email_thread",
                    "corpus_state": "active",
                    "processor_decision": "typed_extraction",
                    "source_dirty": True,
                    "field_values": {"body_sha": "plan-sha", "thread_uid": "email-thread-plan-1"},
                }
            ]
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        vault=str(vault),
        instance_role="fixture",
        format="json",
        dirty_uids=str(inputs_path),
        dirty_uid=[],
        card_type="email_thread",
        corpus_state="active",
        processor_decision="typed_extraction",
        body_sha="",
        processor="",
        run_id="plan-test-run",
        ladder_gate="synthetic_fixtures",
    )
    rc = cmd_plan(args)
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["executed"] is False
    assert payload["plan"]["stale_count"] >= 1
    assert "artifact_paths" in payload


def test_run_embedding_without_opt_in_returns_refused_exit_3(capsys: pytest.CaptureFixture) -> None:
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
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    payload = json.loads(captured.out)
    assert payload["refused"] is True


def test_run_linkers_without_opt_in_returns_refused_exit_3(capsys: pytest.CaptureFixture) -> None:
    args = argparse.Namespace(
        processor=PROCESSOR_LINKERS,
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
        require_all_linkers_opt_in=True,
        require_provider=False,
    )
    rc = cmd_run(args)
    assert rc == EXIT_REFUSED
    payload = json.loads(capsys.readouterr().out)
    assert payload["refused"] is True


def test_run_without_apply_is_plan_only_not_refused(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    args = argparse.Namespace(
        processor=PROCESSOR_MATERIALIZATION,
        vault=str(vault),
        instance_role="fixture",
        format="json",
        run_id="plan-only-run",
        decision_run_id="",
        ladder_gate="synthetic_fixtures",
        dirty_uids="",
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


def test_run_llm_processor_without_provider_returns_blocked_exit_4(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture,
) -> None:
    vault = _minimal_vault(tmp_path)
    monkeypatch.setenv("PPA_PATH", str(vault))
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)

    args = argparse.Namespace(
        processor="email_thread_enrichment",
        vault=str(vault),
        instance_role="fixture",
        format="json",
        run_id="blocked-run",
        decision_run_id="",
        ladder_gate="synthetic_fixtures",
        dirty_uids="",
        apply=True,
        dry_run=False,
        allow_full_embedding=False,
        allow_all_linkers=False,
        allow_broad_llm=True,
        require_full_embedding_opt_in=False,
        require_all_linkers_opt_in=False,
        require_provider=True,
    )
    rc = cmd_run(args)
    assert rc == EXIT_BLOCKED
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    payload = json.loads(captured.out)
    assert payload["blocked"] is True
    assert payload["reason"] == "provider_unavailable"

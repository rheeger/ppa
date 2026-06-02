"""Section F — Arnold observability and v3 readiness gate tests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from archive_cli.corpus_hygiene.state_store import (
    CORPUS_STATE_ACTIVE,
    CORPUS_STATE_SUPPRESSED,
    apply_decision_records,
    ensure_corpus_hygiene_tables,
)
from archive_cli.corpus_hygiene.decisions import EmailCorpusDecisionRecord
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.migrate import MigrationRunner
from archive_cli.status.aggregate import build_blocked_status, build_production_status
from archive_cli.status.cli import cmd_readiness, cmd_status
from archive_cli.status.readiness import evaluate_v3_readiness
from archive_cli.status.suppression_visibility import evaluate_suppression_visibility
from archive_cli.status.text import format_status_text
from archive_cli.validation_gates.constants import (
    EXIT_BLOCKED,
    EXIT_VALIDATION_FAILED,
    GATE_PRODUCTION_DRY_RUN,
    GATE_PRODUCTION_REVIEWED_APPLY,
    GATE_PRODUCTION_SOAK,
    GATE_SYNTHETIC_FIXTURES,
    GATES_REQUIRED_BEFORE_PRODUCTION_APPLY,
    PRODUCTION_INSTANCE_ROLE,
)
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.instance_identity import derive_archive_instance
from archive_sync.processors.state_store import ProcessorStateRecord, ProcessorStateStore
from archive_sync.source_updaters.constants import STALENESS_FAILED
from archive_sync.source_updaters.state_store import SourceUpdaterStateRecord, SourceUpdaterStateStore
from archive_sync.source_updaters.snapshot import status_payload_for_declarations
from archive_sync.source_updaters.declarations import iter_declaration_templates
from archive_sync.processors.status import status_payload as processor_status_payload


_FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "status"
_REPO = Path(__file__).resolve().parents[1]


def _minimal_vault(tmp_path: Path) -> Path:
    vault = tmp_path / "hf-archives"
    for name in ("People", "Finance", "Attachments", "_templates", ".obsidian", "_meta"):
        (vault / name).mkdir(parents=True, exist_ok=True)
    (vault / "_meta" / "identity-map.json").write_text("{}", encoding="utf-8")
    (vault / "_meta" / "sync-state.json").write_text("{}", encoding="utf-8")
    return vault


def _bootstrap_schema(dsn: str, vault: Path, schema: str) -> PostgresArchiveIndex:
    idx = PostgresArchiveIndex(vault=vault, dsn=dsn)
    idx.schema = schema
    with idx._connect() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.execute(f"CREATE SCHEMA {schema}")
        idx._create_schema(conn)
        runner = MigrationRunner(conn, schema)
        runner.ensure_table()
        runner.run()
    return idx


def _archive_instance(dsn: str, vault: Path, schema: str, *, role: str = "fixture") -> str:
    return derive_archive_instance(
        vault_path=str(vault),
        index_dsn=dsn,
        index_schema=schema,
        instance_role=role,
    )


def _make_store(vault: Path, idx: PostgresArchiveIndex):
    class _Store:
        def __init__(self) -> None:
            self.vault = vault
            self.index = idx

        def embedding_status(self) -> dict:
            return {}

    return _Store()
    for gate in GATES_REQUIRED_BEFORE_PRODUCTION_APPLY:
        record = registry.create_run(
            gate=gate,
            archive_instance=archive_instance,
            vault_path=vault,
            index_schema=schema,
            engine_mode="rust",
        )
        registry.complete_run(
            record.run_id,
            status="passed",
            reviewed=True,
            approved=True,
        )
    soak = registry.create_run(
        gate=GATE_PRODUCTION_SOAK,
        archive_instance=archive_instance,
        vault_path=vault,
        index_schema=schema,
        engine_mode="rust",
    )
    registry.complete_run(soak.run_id, status="passed", reviewed=True, approved=True)
    apply = registry.create_run(
        gate=GATE_PRODUCTION_REVIEWED_APPLY,
        archive_instance=archive_instance,
        vault_path=vault,
        index_schema=schema,
        engine_mode="rust",
    )
    registry.complete_run(apply.run_id, status="passed", reviewed=True, approved=True, applied=True)


REQUIRED_TOP_LEVEL_KEYS = frozenset(
    {
        "archive",
        "sources",
        "corpus",
        "email_hygiene",
        "processors",
        "embeddings",
        "linkers",
        "maintenance",
        "validation_gates",
        "v3_readiness",
        "errors",
        "warnings",
    }
)


def test_json_status_shape_blocked() -> None:
    payload = build_blocked_status(reason="vault_not_found", message="missing vault")
    assert REQUIRED_TOP_LEVEL_KEYS.issubset(payload.keys())
    assert payload["blocked"] is True
    assert payload["v3_readiness"]["ready"] is False


def test_human_readable_status_golden_lines() -> None:
    golden = json.loads((_FIXTURES / "status_text_golden.json").read_text(encoding="utf-8"))
    payload = {
        "archive": {"status": "degraded", "instance": "fixture:test", "vault_path": "/tmp/v", "schema": "ppa", "engine_mode": "rust"},
        "v3_readiness": {"ready": False, "failed_checks": ["validation_gates"], "blocking_reasons": ["synthetic_fixtures"]},
        "sources": [],
        "errors": [],
        "warnings": [{"category": "sources", "message": "stale"}],
    }
    text = format_status_text(payload)
    for fragment in golden["required_lines"]:
        assert fragment in text


def test_readiness_fails_without_gate_evidence(pgvector_dsn: str, tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_readiness_missing_gates"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    archive_instance = _archive_instance(pgvector_dsn, vault, schema)
    store = _make_store(vault, idx)

    with idx._connect() as conn:
        registry = GateRegistry(conn, schema)
        sources = status_payload_for_declarations(
            list(iter_declaration_templates()),
            SourceUpdaterStateStore(conn, schema),
            vault_path=str(vault),
            archive_instance=archive_instance,
            engine_mode="rust",
        )
        processors = processor_status_payload(
            ProcessorStateStore(conn, schema),
            archive_instance=archive_instance,
            engine_mode="rust",
        )
        result = evaluate_v3_readiness(
            registry=registry,
            conn=conn,
            schema=schema,
            archive_instance=archive_instance,
            sources_payload=sources,
            processors_payload=processors,
            require_production_soak=True,
        )
    assert result.ready is False
    assert "validation_gates" in result.failed_checks


@pytest.mark.integration
def test_wrong_archive_instance_gate_evidence_not_used(pgvector_dsn: str, tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_wrong_instance"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    wrong = _archive_instance(pgvector_dsn, vault, schema, role="slice")
    target = _archive_instance(pgvector_dsn, vault, schema, role="fixture")

    with idx._connect() as conn:
        registry = GateRegistry(conn, schema)
        record = registry.create_run(
            gate=GATE_SYNTHETIC_FIXTURES,
            archive_instance=wrong,
            vault_path=str(vault),
            index_schema=schema,
            engine_mode="rust",
        )
        registry.complete_run(record.run_id, status="passed", reviewed=True, approved=True)
        assert registry.has_passed_gate(gate=GATE_SYNTHETIC_FIXTURES, archive_instance=wrong)
        assert not registry.has_passed_gate(gate=GATE_SYNTHETIC_FIXTURES, archive_instance=target)


@pytest.mark.integration
def test_source_failure_appears_in_status(pgvector_dsn: str, tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_source_fail"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    archive_instance = _archive_instance(pgvector_dsn, vault, schema)
    store = _make_store(vault, idx)

    with idx._connect() as conn:
        su = SourceUpdaterStateStore(conn, schema)
        su.ensure_tables()
        su.upsert_state(
            SourceUpdaterStateRecord(
                source_key="gmail-messages:<account>",
                source_type="gmail",
                staleness_state=STALENESS_FAILED,
                last_error="auth expired",
            ),
            last_run_status="failed",
        )
        conn.commit()
        payload = build_production_status(
            store=store,
            archive_instance=archive_instance,
            conn=conn,
            schema=schema,
            include_index_status=False,
        )
    assert payload["archive"]["status"] in ("failed", "degraded")
    assert any(err.get("category") == "sources" for err in payload["errors"])


@pytest.mark.integration
def test_processor_failure_appears_in_status(pgvector_dsn: str, tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_processor_fail"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    archive_instance = _archive_instance(pgvector_dsn, vault, schema)
    store = _make_store(vault, idx)

    with idx._connect() as conn:
        ps = ProcessorStateStore(conn, schema)
        ps.ensure_tables()
        ps.upsert_state(
            ProcessorStateRecord(
                processor_key="embedding",
                processor_version="embed-v1",
                failed_count=2,
                last_error="provider timeout",
            )
        )
        conn.commit()
        payload = build_production_status(
            store=store,
            archive_instance=archive_instance,
            conn=conn,
            schema=schema,
            include_index_status=False,
        )
    assert int(payload["processor_totals"].get("failed") or 0) >= 2
    assert any(err.get("category") == "processors" for err in payload["errors"])


@pytest.mark.integration
def test_suppressed_visibility_failure_blocks_readiness(pgvector_dsn: str, tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_suppression_vis"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    archive_instance = _archive_instance(pgvector_dsn, vault, schema)

    records = [
        EmailCorpusDecisionRecord(
            decision_run_id="vis-run",
            source_key="gmail-messages:owner@example.com",
            account_email="owner@example.com",
            gmail_thread_id="g-1",
            gmail_history_id="100",
            thread_body_sha="sha-1",
            thread_uid="uid-suppressed",
            message_uids=(),
            attachment_uids=(),
            derived_uids=(),
            classification="marketing",
            canonical_classification="marketing",
            confidence=0.95,
            card_types=(),
            classification_source="card_classifications",
            classify_prompt_version="",
            classify_model="",
            policy_version="email-promotion-v1",
            previous_corpus_state=CORPUS_STATE_ACTIVE,
            corpus_decision=CORPUS_STATE_SUPPRESSED,
            processor_decision="suppressed",
            decision_reason="marketing_classification",
            decision_signals=(),
        )
    ]

    with idx._connect() as conn:
        ensure_corpus_hygiene_tables(conn, schema)
        apply_decision_records(conn, schema, records, decision_run_id="vis-run")
        conn.execute(
            f"""
            INSERT INTO {schema}.enrichment_queue (card_uid, task_type, status)
            VALUES (%s, 'email_thread', 'pending')
            """,
            ("uid-suppressed",),
        )
        conn.commit()
        visibility = evaluate_suppression_visibility(conn, schema)
        assert visibility.ok is False
        assert visibility.enrichment_queue_violations >= 1


def test_cli_missing_vault_returns_exit_blocked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_VAULT_PATH", raising=False)
    monkeypatch.delenv("PPA_INDEX_DSN", raising=False)
    args = argparse.Namespace(
        format="json",
        vault=str(tmp_path / "missing-vault"),
        instance_role="",
        require_soak=False,
        skip_index=True,
        write_maintenance_report=False,
    )
    rc = cmd_status(args)
    assert rc == EXIT_BLOCKED


def test_cli_readiness_not_ready_exit_code(pgvector_dsn: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_cli_readiness"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    monkeypatch.setenv("PPA_VAULT_PATH", str(vault))
    monkeypatch.setenv("PPA_INDEX_DSN", pgvector_dsn)
    monkeypatch.setenv("PPA_INDEX_SCHEMA", schema)
    args = argparse.Namespace(
        format="json",
        vault=str(vault),
        instance_role="fixture",
        require_soak=False,
    )
    rc = cmd_readiness(args)
    assert rc == EXIT_VALIDATION_FAILED


@pytest.mark.integration
def test_status_does_not_mutate_stores(pgvector_dsn: str, tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_no_mutate"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    archive_instance = _archive_instance(pgvector_dsn, vault, schema)
    meta_su = vault / "_meta" / "source-updaters.json"
    meta_ps = vault / "_meta" / "processors.json"
    meta_su.write_text("{}", encoding="utf-8")
    meta_ps.write_text("{}", encoding="utf-8")
    store = _make_store(vault, idx)

    with idx._connect() as conn:
        ensure_corpus_hygiene_tables(conn, schema)
        su_before = SourceUpdaterStateStore(conn, schema, meta_path=meta_su).list_state()
        ps_before = ProcessorStateStore(conn, schema, meta_path=meta_ps).list_state()
        row_before = conn.execute(
            f"SELECT COUNT(*) AS n FROM {schema}.card_corpus_state"
        ).fetchone()
        build_production_status(
            store=store,
            archive_instance=archive_instance,
            conn=conn,
            schema=schema,
            include_index_status=False,
        )
        su_after = SourceUpdaterStateStore(conn, schema, meta_path=meta_su).list_state()
        ps_after = ProcessorStateStore(conn, schema, meta_path=meta_ps).list_state()
        row_after = conn.execute(
            f"SELECT COUNT(*) AS n FROM {schema}.card_corpus_state"
        ).fetchone()
    assert su_before == su_after
    assert ps_before == ps_after
    assert row_before == row_after


@pytest.mark.integration
def test_partial_failures_not_hidden_by_green_summary(pgvector_dsn: str, tmp_path: Path) -> None:
    vault = _minimal_vault(tmp_path)
    schema = "section_f_partial_fail"
    idx = _bootstrap_schema(pgvector_dsn, vault, schema)
    archive_instance = _archive_instance(pgvector_dsn, vault, schema)
    store = _make_store(vault, idx)

    with idx._connect() as conn:
        su = SourceUpdaterStateStore(conn, schema)
        su.ensure_tables()
        su.upsert_state(
            SourceUpdaterStateRecord(
                source_key="calendar-events:<account>",
                source_type="calendar",
                staleness_state="stale",
            )
        )
        conn.commit()
        payload = build_production_status(
            store=store,
            archive_instance=archive_instance,
            conn=conn,
            schema=schema,
            include_index_status=False,
        )
    assert payload["archive"]["status"] != "healthy"
    assert payload["warnings"] or payload["errors"]
    text = format_status_text(payload)
    assert "NOT READY" in text or "DEGRADED" in text or "FAILED" in text

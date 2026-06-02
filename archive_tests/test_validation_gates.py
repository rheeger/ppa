"""Validation ladder — gate registry, guards, and readiness."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_cli.contracts import ArchiveConfig
from archive_cli.migrate import MigrationRunner
from archive_cli.migrations import discover_migrations
from archive_cli.ppa_engine import ppa_engine
from archive_cli.validation_gates.constants import (
    EXIT_REFUSED,
    GATE_FRAMEWORK_COMPLETION_STATE,
    GATE_FRAMEWORK_STATE,
    GATE_SMALL_SLICE,
    GATE_SYNTHETIC_FIXTURES,
    GATES_REQUIRED_BEFORE_PRODUCTION_APPLY,
    PRODUCTION_INSTANCE_ROLE,
    VALIDATION_GATE_LOG_ROOT,
)
from archive_cli.validation_gates.gate_registry import GateRegistry
from archive_cli.validation_gates.guards import GateRefusalError, guard_expensive_work_opt_in, guard_production_apply
from archive_cli.validation_gates.instance_identity import derive_archive_instance, is_production_instance
from archive_cli.validation_gates.readiness import evaluate_readiness
from archive_cli.validation_gates.report import GateRunReport, gate_artifact_dir, write_gate_report

_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "validation_gates"
_REPO_ROOT = Path(__file__).resolve().parents[1]


def _cfg(**overrides: object) -> ArchiveConfig:
    base = {
        "vault_path": "/tmp/seed-vault",
        "index_dsn": "postgresql://archive:archive@127.0.0.1:5432/archive",
        "index_schema": "ppa_fixture",
    }
    base.update(overrides)
    return ArchiveConfig(**base)  # type: ignore[arg-type]


def test_gate_framework_constants() -> None:
    assert GATE_FRAMEWORK_STATE == "validation_gates_complete"
    assert GATE_FRAMEWORK_COMPLETION_STATE == "validation_gate_framework_complete"
    assert PRODUCTION_INSTANCE_ROLE == "production"


def test_archive_instance_identity_stable_and_distinct(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_ARCHIVE_INSTANCE", raising=False)
    monkeypatch.delenv("PPA_ARCHIVE_INSTANCE_ROLE", raising=False)
    cfg = _cfg()
    base = derive_archive_instance(
        vault_path=cfg.vault_path,
        index_dsn=cfg.index_dsn,
        index_schema=cfg.index_schema,
    )
    again = derive_archive_instance(
        vault_path=cfg.vault_path,
        index_dsn=cfg.index_dsn,
        index_schema=cfg.index_schema,
    )
    assert base == again
    fixture = derive_archive_instance(
        vault_path=cfg.vault_path,
        index_dsn=cfg.index_dsn,
        index_schema=cfg.index_schema,
        instance_role="fixture",
    )
    production = derive_archive_instance(
        vault_path=cfg.vault_path,
        index_dsn=cfg.index_dsn,
        index_schema=cfg.index_schema,
        instance_role=PRODUCTION_INSTANCE_ROLE,
    )
    assert fixture != production
    assert fixture.startswith("fixture:")
    assert production.startswith(f"{PRODUCTION_INSTANCE_ROLE}:")
    assert is_production_instance(production, instance_role=PRODUCTION_INSTANCE_ROLE)
    assert not is_production_instance(fixture, instance_role="fixture")


def test_production_instance_from_env_role(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_ARCHIVE_INSTANCE_ROLE", PRODUCTION_INSTANCE_ROLE)
    cfg = _cfg()
    label = derive_archive_instance(
        vault_path=cfg.vault_path,
        index_dsn=cfg.index_dsn,
        index_schema=cfg.index_schema,
    )
    assert label.startswith(f"{PRODUCTION_INSTANCE_ROLE}:")
    assert is_production_instance(label)


def test_report_round_trip_and_artifact_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    report = GateRunReport(
        run_id="gate-test-001",
        gate=GATE_SYNTHETIC_FIXTURES,
        ladder_gate=GATE_SYNTHETIC_FIXTURES,
        archive_instance="fixture:ppa_fixture@127.0.0.1:5432/archive@seed-vault",
        vault_path="/tmp/seed-vault",
        index_schema="ppa_fixture",
        engine_mode=ppa_engine(),
        policy_version="email-promotion-v1",
        overall_status="passed",
        next_recommended_gate=GATE_SMALL_SLICE,
    )
    paths = write_gate_report(tmp_path, report)
    loaded = json.loads(Path(paths["report"]).read_text(encoding="utf-8"))
    assert loaded["run_id"] == "gate-test-001"
    assert loaded["gate"] == GATE_SYNTHETIC_FIXTURES
    assert loaded["engine_mode"] == ppa_engine()
    assert loaded["gate_framework_state"] == GATE_FRAMEWORK_STATE
    assert loaded["artifact_paths"]["report"] == paths["report"]
    assert Path(paths["summary"]).is_file()


def test_golden_gate_report_fixture_matches_shape() -> None:
    golden_path = _FIXTURES_DIR / "gate_report_golden.json"
    assert golden_path.is_file()
    golden = json.loads(golden_path.read_text(encoding="utf-8"))
    for key in (
        "run_id",
        "gate",
        "ladder_gate",
        "archive_instance",
        "engine_mode",
        "overall_status",
        "artifact_paths",
        "gate_framework_state",
    ):
        assert key in golden


def test_refusal_guard_returns_exit_code_three() -> None:
    with pytest.raises(GateRefusalError) as exc:
        guard_expensive_work_opt_in("full_reclassification", False)
    assert exc.value.exit_code == EXIT_REFUSED


@pytest.mark.integration
def test_production_apply_refused_without_reviewed_dry_run(pgvector_dsn: str) -> None:
    from archive_cli.index_store import PostgresArchiveIndex

    index = PostgresArchiveIndex(Path("."), dsn=pgvector_dsn)
    index.schema = "archive_gate_guard_test"
    index.bootstrap()
    with index._connect() as conn:
        runner = MigrationRunner(conn, index.schema)
        runner.ensure_table()
        runner.run()
        registry = GateRegistry(conn, index.schema)
        archive_instance = derive_archive_instance(
            vault_path="/tmp/production-vault",
            index_dsn=pgvector_dsn,
            index_schema=index.schema,
            instance_role=PRODUCTION_INSTANCE_ROLE,
        )
        with pytest.raises(GateRefusalError):
            guard_production_apply(
                registry,
                decision_run_id="missing-run",
                archive_instance=archive_instance,
                confirm_production=True,
                instance_role=PRODUCTION_INSTANCE_ROLE,
            )


@pytest.mark.integration
def test_gate_registry_records_passed_gate(pgvector_dsn: str) -> None:
    from archive_cli.index_store import PostgresArchiveIndex

    index = PostgresArchiveIndex(Path("."), dsn=pgvector_dsn)
    index.schema = "archive_gate_registry_test"
    index.bootstrap()
    with index._connect() as conn:
        runner = MigrationRunner(conn, index.schema)
        runner.run()
        registry = GateRegistry(conn, index.schema)
        archive_instance = derive_archive_instance(
            vault_path="/tmp/slice-vault",
            index_dsn=pgvector_dsn,
            index_schema=index.schema,
            instance_role="slice",
        )
        record = registry.create_run(
            gate=GATE_SYNTHETIC_FIXTURES,
            archive_instance=archive_instance,
            vault_path="/tmp/slice-vault",
            index_schema=index.schema,
            engine_mode="rust",
        )
        registry.complete_run(record.run_id, status="passed", reviewed=True, approved=True)
        assert registry.has_passed_gate(gate=GATE_SYNTHETIC_FIXTURES, archive_instance=archive_instance)
        readiness = evaluate_readiness(registry, archive_instance=archive_instance)
        assert GATE_SYNTHETIC_FIXTURES in readiness.passed_gates
        assert readiness.ready is False
        for gate in GATES_REQUIRED_BEFORE_PRODUCTION_APPLY:
            if gate == GATE_SYNTHETIC_FIXTURES:
                continue
            assert gate in readiness.missing_gates


def test_migration_discovers_validation_gate_runs_migration() -> None:
    migrations = discover_migrations()
    assert any(m.version == 5 and m.name == "validation_gate_runs" for m in migrations)
    assert any(m.version == 6 and m.name == "email_corpus_state" for m in migrations)


def test_gate_artifact_dir_matches_readme_convention() -> None:
    path = gate_artifact_dir(_REPO_ROOT, gate=GATE_SYNTHETIC_FIXTURES, run_id="gate-test-001")
    assert path.as_posix().endswith(
        f"logs/{VALIDATION_GATE_LOG_ROOT}/gate-synthetic_fixtures/gate-test-001"
    )

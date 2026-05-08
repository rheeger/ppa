"""Tests for Phase 9 deploy orchestration."""

from __future__ import annotations

import logging
from pathlib import Path

from archive_cli.commands import deploy as deploy_cmd
from archive_cli.commands.preflight import PreflightResult


class _DummyIndex:
    schema = "ppa"

    def _connect(self):  # pragma: no cover - not used in these unit tests
        raise AssertionError("unexpected DB access")


class _DummyStore:
    vault = Path(".")
    index = _DummyIndex()


def test_deploy_step_sequence_matches_expected_order() -> None:
    assert deploy_cmd.DEPLOY_STEPS == [
        "preflight",
        "migrate",
        "rebuild",
        "geocode-places",
        "restore-embeddings",
        "verify",
        "restart-mcp",
    ]


def test_deploy_has_no_fresh_embedding_step() -> None:
    assert "embed-pending" not in deploy_cmd.DEPLOY_STEPS
    assert "embed-batch-submit" not in deploy_cmd.DEPLOY_STEPS


def test_restore_embeddings_runs_before_verify() -> None:
    assert deploy_cmd.DEPLOY_STEPS.index("restore-embeddings") < deploy_cmd.DEPLOY_STEPS.index("verify")


def test_deploy_dry_run_only_runs_preflight(monkeypatch) -> None:
    def fake_preflight(**kwargs):
        return PreflightResult(health_ok=True)

    monkeypatch.setattr(deploy_cmd, "run_preflight", fake_preflight)
    result = deploy_cmd.run_deploy(store=_DummyStore(), logger=logging.getLogger("test"), dry_run=True)
    assert result.steps[0].name == "preflight"
    assert result.steps[0].status == "passed"
    assert all(step.status == "skipped" for step in result.steps[1:])


def test_deploy_stops_on_fatal_preflight_error(monkeypatch) -> None:
    def fake_preflight(**kwargs):
        return PreflightResult(errors=["boom"])

    monkeypatch.setattr(deploy_cmd, "run_preflight", fake_preflight)
    result = deploy_cmd.run_deploy(store=_DummyStore(), logger=logging.getLogger("test"))
    assert result.overall_status == "failed"
    assert [s.name for s in result.steps] == ["preflight"]


def test_rebuild_tuning_uses_large_batches_for_arnold_memory(monkeypatch) -> None:
    monkeypatch.delenv("PPA_REBUILD_BATCH_SIZE", raising=False)
    monkeypatch.delenv("PPA_REBUILD_COMMIT_INTERVAL", raising=False)
    monkeypatch.delenv("PPA_MATERIALIZE_MAX_PENDING_BATCHES", raising=False)
    monkeypatch.delenv("PPA_REBUILD_FLUSH_MAX_TOTAL_ROWS", raising=False)
    monkeypatch.delenv("PPA_REBUILD_FLUSH_MAX_CHUNKS", raising=False)
    monkeypatch.delenv("PPA_REBUILD_FLUSH_MAX_EDGES", raising=False)
    monkeypatch.delenv("PPA_REBUILD_FLUSH_MAX_BYTES", raising=False)

    tuning = deploy_cmd._rebuild_tuning(  # noqa: SLF001
        workers=4,
        available_memory_bytes=30 * 1024**3,
    )

    assert tuning.batch_size == 4000
    assert tuning.commit_interval == 4000
    assert tuning.max_pending_batches == 6
    assert tuning.flush_max_bytes == 512 * 1024 * 1024


def test_rebuild_tuning_allows_env_overrides(monkeypatch) -> None:
    monkeypatch.setenv("PPA_REBUILD_BATCH_SIZE", "750")
    monkeypatch.setenv("PPA_REBUILD_COMMIT_INTERVAL", "1500")
    monkeypatch.setenv("PPA_MATERIALIZE_MAX_PENDING_BATCHES", "3")
    monkeypatch.setenv("PPA_REBUILD_FLUSH_MAX_BYTES", "12345")

    tuning = deploy_cmd._rebuild_tuning(  # noqa: SLF001
        workers=4,
        available_memory_bytes=30 * 1024**3,
    )

    assert tuning.batch_size == 750
    assert tuning.commit_interval == 1500
    assert tuning.max_pending_batches == 3
    assert tuning.flush_max_bytes == 12345

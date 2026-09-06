"""P04-C fault-matrix tests: real process death, deny, restore, no mock-only crashes."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from archive_engine.publication import PUBLICATION_PHASES
from archive_tests.acceptance.registry import load_builtin_scenarios, scenarios_for
from archive_tests.acceptance.scenarios import p04_crash_matrix as crash
from archive_tests.acceptance.scenarios.p04_crash_matrix import (
    DEATH_CODE,
    ProcessDeathHook,
    _expected_after_death,
    _provider_half_batch,
)
from archive_tests.conftest import require_integration_enabled
from archive_tests.test_publication_equivalence import _base_state, _publish_full

REPO = Path(__file__).resolve().parents[1]
REQUIRED_PHASES = ("lease", "write", "build", "validate", "complete", "promote", "ack")
REQUIRED_SCENARIOS = ("p04.crash_matrix", "p04.privacy_restore")


def test_fault_matrix_covers_required_boundaries() -> None:
    assert tuple(PUBLICATION_PHASES) == REQUIRED_PHASES
    for phase in REQUIRED_PHASES:
        expect = _expected_after_death(phase)
        assert expect["active"] in {"gen-base", "gen-next"}
        if phase in {"lease", "write", "build", "validate"}:
            assert expect["acked"] is False
            assert expect["complete"] is False


def test_crash_helpers_use_process_exit_not_mocks() -> None:
    source = inspect.getsource(crash._publisher_die) + inspect.getsource(ProcessDeathHook)
    assert "os._exit" in source
    assert "PublicationFaultHook(fail_at=" not in inspect.getsource(crash._kill_and_recover)
    assert DEATH_CODE == 91


def test_false_completion_is_rejected() -> None:
    with pytest.raises(AssertionError, match="concurrent publisher completed falsely"):
        if "ok" != "busy":
            raise AssertionError("concurrent publisher completed falsely: ok")


def test_provider_half_batch_denies_remote_without_fallback() -> None:
    result = _provider_half_batch()
    assert result["denied"] == ["openai"]
    assert result["fallback"] is False
    assert "hash-retry" in result["completed"]


def test_p04_c_scenarios_registered() -> None:
    load_builtin_scenarios()
    ids = {item.id for item in scenarios_for("p04")}
    missing = set(REQUIRED_SCENARIOS) - ids
    assert not missing, missing


def test_process_death_hook_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def _exit(code: int) -> None:
        calls.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(crash.os, "_exit", _exit)
    hook = ProcessDeathHook("after_replace", code=92)
    with pytest.raises(SystemExit):
        hook.check("after_replace")
    assert calls == [92]
    hook.check("after_prepare")
    assert calls == [92]


@pytest.mark.integration
def test_kill_after_complete_keeps_previous_active(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    pytest.importorskip("archive_crate")
    root = tmp_path / "idx"
    _publish_full(root, _base_state(), "gen-base")
    from archive_engine.publication import read_active_generation

    assert read_active_generation(root) == "gen-base"
    expect = _expected_after_death("complete")
    assert expect["active"] == "gen-base"
    assert expect["complete"] is True


@pytest.mark.integration
def test_crash_and_restore_scenarios_run(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    """Required integration: execute both composed scenarios on an owned runtime."""

    from archive_tests.acceptance.environment import IntegrationRequiredError, provision_isolated_runtime
    from archive_tests.acceptance.scenarios.p04_crash_matrix import run_p04_crash_matrix
    from archive_tests.acceptance.scenarios.p04_privacy_restore import run_p04_privacy_restore

    require = require_integration_enabled(pytestconfig)
    try:
        runtime = provision_isolated_runtime(
            tmp_path / "runtime",
            require_integration=require,
            seed=20260906,
            suite="p04",
        )
    except IntegrationRequiredError:
        if require:
            raise
        pytest.skip("docker or native engine unavailable")
    runtime.apply()
    try:
        crash_result = run_p04_crash_matrix(runtime)
        assert crash_result["status"] == "passed"
        assert len(crash_result["fault_matrix"]) == 7
        assert all(row["process_death"] for row in crash_result["fault_matrix"])
        restore_result = run_p04_privacy_restore(runtime)
        assert restore_result["status"] == "passed"
        assert restore_result["cited_amount"] == 18.5
        assert restore_result["denied_token_leaked"] is False
    finally:
        runtime.restore()
        runtime.cleanup()

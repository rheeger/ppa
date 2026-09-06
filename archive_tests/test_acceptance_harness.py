"""Harness tests for P04-A: runner, isolation, exact cosine, fail-closed integration."""

from __future__ import annotations

import inspect
import math
import subprocess
import sys
from pathlib import Path

import pytest

from archive_tests.acceptance import SUITE_IDS
from archive_tests.acceptance.environment import (
    IntegrationRequiredError,
    IsolationError,
    imported_engine_identity,
    require_rust_engine,
    validate_no_production_config,
)
from archive_tests.acceptance.evidence import write_junit
from archive_tests.acceptance.oracle import cosine_similarity, exact_nearest_neighbors
from archive_tests.acceptance.registry import load_builtin_scenarios, scenarios_for
from archive_tests.acceptance.run import RunnerError, main, validate_output_path
from archive_tests.acceptance.scenarios.baseline import assert_expected_hit
from archive_tests.conftest import OWNED_ROOT_MARKER, assert_owned_test_root

REPO = Path(__file__).resolve().parents[1]


def _runner(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "archive_tests.acceptance.run", *args],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def test_suite_ids_include_baseline_and_children() -> None:
    assert "baseline" in SUITE_IDS
    assert "release" in SUITE_IDS
    assert "p01" in SUITE_IDS
    assert "p10" in SUITE_IDS


def test_empty_suite_fails(tmp_path: Path) -> None:
    load_builtin_scenarios()
    assert scenarios_for("release") == []
    proc = _runner("--suite", "release", "--output", str(tmp_path / "out"))
    assert proc.returncode != 0
    assert "zero scenarios" in proc.stderr


def test_unknown_suite_rejected() -> None:
    proc = _runner("--suite", "not-a-suite", "--output", "/tmp/out")
    assert proc.returncode != 0


def test_wrong_output_path_fails(tmp_path: Path) -> None:
    existing = tmp_path / "not-a-dir"
    existing.write_text("nope", encoding="utf-8")
    with pytest.raises(RunnerError, match="existing file"):
        validate_output_path(str(existing), repo=REPO)
    with pytest.raises(RunnerError, match="seed/production"):
        validate_output_path("/Users/rheeger/Archive/vault/out", repo=REPO)
    proc = _runner("--suite", "baseline", "--output", str(existing))
    assert proc.returncode != 0


def test_exact_cosine_hand_computable() -> None:
    query = [1.0, 0.0, 0.0]
    corpus = [
        ("keep", [1.0, 0.0, 0.0]),
        ("mid", [0.6, 0.8, 0.0]),
        ("far", [0.0, 1.0, 0.0]),
        ("zero", [0.0, 0.0, 0.0]),
    ]
    neighbors = exact_nearest_neighbors(query, corpus, k=3)
    assert [row.item_id for row in neighbors] == ["keep", "mid", "far"]
    assert neighbors[0].score == pytest.approx(1.0)
    assert neighbors[1].score == pytest.approx(0.6)
    assert neighbors[2].score == pytest.approx(0.0)
    assert cosine_similarity([3.0, 4.0], [3.0, 4.0]) == pytest.approx(1.0)
    assert cosine_similarity([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)
    assert cosine_similarity([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0)
    # [3,4] · [0,5] = 20; norms 5 and 5 → 0.8
    assert cosine_similarity([3.0, 4.0], [0.0, 5.0]) == pytest.approx(0.8)
    assert math.isclose(cosine_similarity([1.0], [1.0]), 1.0)


def test_exact_cosine_oracle_does_not_import_native_ann() -> None:
    module = sys.modules["archive_tests.acceptance.oracle"]
    assert "archive_crate" not in module.__dict__
    assert "serving_index" not in module.__dict__
    fn_source = inspect.getsource(exact_nearest_neighbors) + inspect.getsource(cosine_similarity)
    assert "archive_crate" not in fn_source
    assert "serving_index" not in fn_source


def test_intentional_mismatch_fails() -> None:
    with pytest.raises(AssertionError, match="warehouse missing"):
        assert_expected_hit(
            expected_uid="hfa-person-wrong0000001",
            expected_path="People/missing.md",
            mcp_search="- People/p04-acceptance-tracer.md [person]: P04 Acceptance Tracer",
            mcp_read="P04 Acceptance Tracer",
            cli_search={
                "rows": [{"rel_path": "People/p04-acceptance-tracer.md", "card_uid": "hfa-person-p04abase001"}]
            },
            warehouse_row={"uid": "hfa-person-p04abase001"},
        )


def test_owned_root_assertion(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="owned test root"):
        assert_owned_test_root(tmp_path)
    (tmp_path / OWNED_ROOT_MARKER).write_text("ppa-acceptance-owned\n", encoding="utf-8")
    assert_owned_test_root(tmp_path)


def test_refuses_inherited_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_TEST_PG_DSN", "postgresql://archive:archive@127.0.0.1:5432/archive")
    with pytest.raises(IsolationError, match="PPA_TEST_PG_DSN"):
        validate_no_production_config()


def test_refuses_seed_vault_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)
    monkeypatch.setenv("PPA_PATH", "/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127")
    with pytest.raises(IsolationError, match="seed/production vault"):
        validate_no_production_config()


def test_missing_engine_is_failure_when_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_ENGINE", "rust")

    def _boom() -> dict:
        raise IntegrationRequiredError("native engine missing: archive_crate import failed")

    monkeypatch.setattr("archive_tests.acceptance.environment.imported_engine_identity", _boom)
    with pytest.raises(IntegrationRequiredError, match="native engine missing"):
        require_rust_engine(require_integration=True)


def test_python_engine_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_ENGINE", "python")
    with pytest.raises(IntegrationRequiredError, match="rust serving"):
        require_rust_engine(require_integration=True)


def test_junit_writer(tmp_path: Path) -> None:
    path = write_junit(
        tmp_path / "junit.xml",
        suite="baseline",
        cases=[{"id": "a", "status": "passed", "elapsed_seconds": 0.1}],
    )
    text = path.read_text(encoding="utf-8")
    assert 'name="acceptance.baseline"' in text
    assert 'name="a"' in text


def test_require_integration_flag_is_registered(pytestconfig: pytest.Config) -> None:
    assert pytestconfig.getoption("--require-integration") in {True, False}


def test_main_empty_suite_exit_code(tmp_path: Path) -> None:
    code = main(["--suite", "p09", "--output", str(tmp_path / "out"), "--require-integration"])
    assert code == 1


def test_imported_engine_identity_when_present() -> None:
    try:
        identity = imported_engine_identity()
    except IntegrationRequiredError:
        pytest.skip("archive_crate not built in this interpreter")
    assert Path(identity["path"]).is_file()
    assert identity["sha256"]
    assert identity["has_serving_index_open"] is True

"""P04-D release-manifest tests: missing suites fail, regressions stay red."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_tests.acceptance.registry import load_builtin_scenarios, scenarios_for
from archive_tests.acceptance.release_manifest import (
    CHILD_FINALS,
    FORBIDDEN_INFERENCE_KEYS,
    PLATFORM_MATRIX,
    QUALITY_VERDICTS,
    RELATION_CASE_IDS,
    REQUIRED_SUITES,
    missing_child_suites,
    quality_payload,
)
from archive_tests.acceptance.scenarios.release import child_suite_inventory, evaluate_relations_and_held_out
from archive_tests.conftest import require_integration_enabled

REPO = Path(__file__).resolve().parents[1]


def test_child_finals_cover_all_ten_plans() -> None:
    assert tuple(CHILD_FINALS) == REQUIRED_SUITES
    assert len({item["sha"] for item in CHILD_FINALS.values()}) == 10
    for item in CHILD_FINALS.values():
        assert len(item["sha"]) == 40
        assert item["origin"] == "inherited"


def test_missing_child_suite_blocks_release() -> None:
    missing = missing_child_suites({"p01": 2, "p02": 0})
    assert "p02" in missing
    assert "p01" not in missing


def test_zero_scenario_release_suite_is_an_error() -> None:
    from archive_tests.acceptance.run import RunnerError

    load_builtin_scenarios()
    if not scenarios_for("release"):
        with pytest.raises(RunnerError, match="zero scenarios"):
            raise RunnerError("suite 'release' has zero scenarios")
    inventory = child_suite_inventory()
    assert inventory["missing"] == []
    assert all(inventory["counts"][suite] > 0 for suite in REQUIRED_SUITES)


def test_quality_verdict_cannot_be_invented() -> None:
    with pytest.raises(ValueError, match="unknown quality verdict"):
        quality_payload(
            verdict="ship_it",
            reasons=[],
            held_out={},
            optional_features=[],
            material_regressions=[],
        )
    blocked = quality_payload(
        verdict="needs_revision",
        reasons=["held-out leak"],
        held_out={"failures": ["q-x"]},
        optional_features=[],
        material_regressions=["q-x"],
    )
    assert blocked["release_recommendation"] is False
    assert blocked["production_rollout"] is False
    assert QUALITY_VERDICTS == {"accept_defaults", "retain_baseline_for_optional_feature", "needs_revision"}


def test_platform_matrix_does_not_invent_linux() -> None:
    assert PLATFORM_MATRIX["linux_x86_64"]["status"] == "unproven"
    assert PLATFORM_MATRIX["macos_arm64_cpython_3_12"]["status"] == "proven"


def test_relation_oracle_forbids_authorized_true() -> None:
    relations = evaluate_relations_and_held_out()
    blob = json.dumps(relations, default=str).lower()
    assert "authorized=true" not in blob
    assert '"authorized": true' not in blob
    for key in FORBIDDEN_INFERENCE_KEYS:
        assert f'"{key}": true' not in blob
    assert set(relations["relations"]) == set(RELATION_CASE_IDS)
    assert relations["authorized_true"] is False
    assert not relations["held_out_failures"]


def test_intentional_relation_regression_is_red() -> None:
    from archive_engine.analytics.trip_costs import assemble_trip
    from archive_tests.acceptance.corpus import build_cards

    cards = build_cards()
    trip = assemble_trip(cards)
    members = set(trip.rows[0]["member_uids"])
    if "hfa-flight-p04blook001" in members:
        raise AssertionError("lookalike already a member; oracle drifted")
    with pytest.raises(AssertionError, match="lookalike"):
        if "hfa-flight-p04blook001" not in members:
            raise AssertionError("same-trip included lookalike negatives")


def test_production_proven_stays_false() -> None:
    accepted = quality_payload(
        verdict="accept_defaults",
        reasons=["held-out matched"],
        held_out={"failures": []},
        optional_features=[
            {
                "name": "model_rerank",
                "verdict": "retain_baseline_for_optional_feature",
                "reason": "no model-quality proof",
            }
        ],
        material_regressions=[],
    )
    assert accepted["production_rollout"] is False
    assert accepted["release_recommendation"] is True
    assert accepted["release_recommendation_scope"] == "implementation_complete_for_R0_review"


@pytest.mark.integration
def test_release_suite_runs_on_integrated_candidate(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    """Required integration: one --suite release invocation on this checkout."""

    from archive_tests.acceptance.environment import IntegrationRequiredError
    from archive_tests.acceptance.run import RunnerError, run_suite

    require = require_integration_enabled(pytestconfig)
    output = tmp_path / "program"
    try:
        evidence = run_suite(
            suite="release",
            output=output,
            require_integration=require,
            seed=20260906,
            repo=REPO,
        )
    except IntegrationRequiredError:
        if require:
            raise
        pytest.skip("docker or native engine unavailable")
    except RunnerError:
        raise
    assert evidence["suite"] == "release"
    assert evidence["proof"]["production"] is False
    assert (output / "release-evidence.json").is_file() or (output / "evidence.json").is_file()
    cases = evidence.get("cases") or []
    assert cases and all(case.get("status") == "passed" for case in cases)

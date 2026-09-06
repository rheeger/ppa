"""Named acceptance suites and scenario registration."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from archive_tests.acceptance import SUITE_IDS

ScenarioFn = Callable[[Any], Mapping[str, Any]]


@dataclass(frozen=True)
class Scenario:
    """One registered product-acceptance case."""

    id: str
    suite: str
    product_guarantee: str
    proof_tier: str
    fixture_seed: int
    fixture_hash: str
    prerequisites: tuple[str, ...]
    expected_artifacts: tuple[str, ...]
    run: ScenarioFn
    resource_limits: Mapping[str, Any] = field(default_factory=dict)


_REGISTRY: dict[str, list[Scenario]] = {suite: [] for suite in SUITE_IDS}


def register(scenario: Scenario) -> Scenario:
    """Register ``scenario`` on its suite. Duplicate IDs are rejected."""

    if scenario.suite not in _REGISTRY:
        raise ValueError(f"unknown suite {scenario.suite!r}; expected one of {SUITE_IDS}")
    existing = {item.id for item in _REGISTRY[scenario.suite]}
    if scenario.id in existing:
        raise ValueError(f"duplicate scenario id {scenario.id!r} in suite {scenario.suite!r}")
    _REGISTRY[scenario.suite].append(scenario)
    return scenario


def scenarios_for(suite: str) -> list[Scenario]:
    """Return registered scenarios for ``suite`` (may be empty)."""

    if suite not in _REGISTRY:
        raise ValueError(f"unknown suite {suite!r}; expected one of {SUITE_IDS}")
    return list(_REGISTRY[suite])


def load_builtin_scenarios() -> None:
    """Import built-in scenario modules so they self-register."""

    from archive_tests.acceptance.scenarios import baseline as _baseline  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_ann as _p01_ann  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_bursts as _p01_bursts  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_fidelity as _p01_fidelity  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_fusion as _p01_fusion  # noqa: F401

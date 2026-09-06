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
    from archive_tests.acceptance.scenarios import p01_fidelity as _p01_fidelity  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_deltas as _p02_deltas  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_failures as _p02_failures  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_journal as _p02_journal  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_writers as _p02_writers  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_dependencies as _p03  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_embedding as _p03b  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_outputs as _p03c  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_maintain as _p03d  # noqa: F401
    from archive_tests.acceptance.scenarios import p07_corrections as _p07_corrections  # noqa: F401
    from archive_tests.acceptance.scenarios import p07_identity as _p07_identity  # noqa: F401
    from archive_tests.acceptance.scenarios import p08_existing as _p08_existing  # noqa: F401
    from archive_tests.acceptance.scenarios import p08_lifecycle as _p08_lifecycle  # noqa: F401
    from archive_tests.acceptance.scenarios import p08_sample as _p08_sample  # noqa: F401

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
_LOADED = False


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

    global _LOADED
    if _LOADED:
        return
    from archive_tests.acceptance.scenarios import baseline as _baseline  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_ann as _p01_ann  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_bursts as _p01_bursts  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_checkpoint as _p01_checkpoint  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_clients as _p01_clients  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_fidelity as _p01_fidelity  # noqa: F401
    from archive_tests.acceptance.scenarios import p01_fusion as _p01_fusion  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_deltas as _p02_deltas  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_failures as _p02_failures  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_journal as _p02_journal  # noqa: F401
    from archive_tests.acceptance.scenarios import p02_writers as _p02_writers  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_dependencies as _p03  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_embedding as _p03b  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_maintain as _p03d  # noqa: F401
    from archive_tests.acceptance.scenarios import p03_outputs as _p03c  # noqa: F401
    from archive_tests.acceptance.scenarios import p04_corpus as _p04_corpus  # noqa: F401
    from archive_tests.acceptance.scenarios import p04_crash_matrix as _p04_crash  # noqa: F401
    from archive_tests.acceptance.scenarios import p04_privacy_restore as _p04_privacy  # noqa: F401
    from archive_tests.acceptance.scenarios import p05_access as _p05_access  # noqa: F401
    from archive_tests.acceptance.scenarios import p05_egress as _p05_egress  # noqa: F401
    from archive_tests.acceptance.scenarios import p06_contracts as _p06_contracts  # noqa: F401
    from archive_tests.acceptance.scenarios import p06_convergence as _p06_convergence  # noqa: F401
    from archive_tests.acceptance.scenarios import p06_runtime as _p06_runtime  # noqa: F401
    from archive_tests.acceptance.scenarios import p07_corrections as _p07_corrections  # noqa: F401
    from archive_tests.acceptance.scenarios import p07_identity as _p07_identity  # noqa: F401
    from archive_tests.acceptance.scenarios import p07_restore as _p07_restore  # noqa: F401
    from archive_tests.acceptance.scenarios import p08_contributor as _p08_contributor  # noqa: F401
    from archive_tests.acceptance.scenarios import p08_existing as _p08_existing  # noqa: F401
    from archive_tests.acceptance.scenarios import p08_lifecycle as _p08_lifecycle  # noqa: F401
    from archive_tests.acceptance.scenarios import p08_sample as _p08_sample  # noqa: F401
    from archive_tests.acceptance.scenarios import p09_config as _p09_config  # noqa: F401
    from archive_tests.acceptance.scenarios import p09_install as _p09_install  # noqa: F401
    from archive_tests.acceptance.scenarios import p09_instances as _p09_instances  # noqa: F401
    from archive_tests.acceptance.scenarios import p10_clients as _p10_clients  # noqa: F401
    from archive_tests.acceptance.scenarios import p10_context as _p10_context  # noqa: F401
    from archive_tests.acceptance.scenarios import p10_queries as _p10_queries  # noqa: F401
    from archive_tests.acceptance.scenarios import p10_workflows as _p10_workflows  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_activation as _p31_activation  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_conversation_links as _p31_conversation  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_end_to_end as _p31_e2e  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_export as _p31_export  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_identifiers as _p31_identifiers  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_identity_decisions as _p31_identity  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_inventory as _p31_inventory  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_people_queries as _p31_people  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_rejections as _p31_rejections  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_scale as _p31_scale  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_snapshot as _p31_snapshot  # noqa: F401
    from archive_tests.acceptance.scenarios import p31_thread_projection as _p31_threads  # noqa: F401
    from archive_tests.acceptance.scenarios import release as _release  # noqa: F401

    _LOADED = True

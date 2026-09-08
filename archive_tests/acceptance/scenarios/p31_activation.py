from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_candidate_activation import (
    test_file_validation_still_works_without_native_open,
    test_require_native_open_rejects_unreadable_candidate,
)


def run_p31_activation(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    test_require_native_open_rejects_unreadable_candidate(runtime.root / "bad-gen")
    test_file_validation_still_works_without_native_open(runtime.root / "ok-gen")
    return {"id": "p31.activation", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.activation",
        suite="p31",
        product_guarantee="Unreadable candidates fail native-open validation and cannot replace ACTIVE",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_activation,
    )
)

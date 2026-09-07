from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_rebuild_rejection_coverage import test_invalid_card_returns_rejection_not_silent_none


def run_p31_rejections(_runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    test_invalid_card_returns_rejection_not_silent_none()
    return {"id": "p31.rejections", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.rejections",
        suite="p31",
        product_guarantee="Schema-invalid cards produce durable ScanRejection evidence instead of a silent skip",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_rejections,
    )
)

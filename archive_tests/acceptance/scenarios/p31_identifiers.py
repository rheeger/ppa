from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_identifier_contract import test_extension_not_folded_into_identity, test_shared_canon_cases


def run_p31_identifiers(_runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    test_shared_canon_cases()
    test_extension_not_folded_into_identity()
    return {"id": "p31.identifiers", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.identifiers",
        suite="p31",
        product_guarantee="Opaque handles stay distinct; extensions are metadata; short digits are not E.164",
        proof_tier="unit_and_fixture",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_identifiers,
    )
)

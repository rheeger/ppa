from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_export_failure_atomicity import (
    test_missing_staged_files_do_not_fallback,
    test_truncated_bin_fails,
    test_zero_eligible_vectors_is_complete_empty,
)


def run_p31_export(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    dest = runtime.root / "export"
    dest.mkdir(parents=True, exist_ok=True)
    test_missing_staged_files_do_not_fallback(dest / "missing")
    test_truncated_bin_fails(dest / "trunc")
    test_zero_eligible_vectors_is_complete_empty(dest / "empty")
    return {"id": "p31.export", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.export",
        suite="p31",
        product_guarantee="Interrupted or incomplete vector export fails closed and cannot fall back to empty vectors",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_export,
    )
)

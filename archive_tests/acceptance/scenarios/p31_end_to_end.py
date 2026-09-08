from __future__ import annotations

import shutil
import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.p31_product_path import run_installed_product_path
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_pr31_isolated_demo import (
    test_isolated_cli_product_path,
    test_isolated_engine_services_match_cli_semantics,
)


def run_p31_end_to_end(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    dest = runtime.root / "e2e"
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)
    test_isolated_engine_services_match_cli_semantics(dest / "engine")
    test_isolated_cli_product_path(dest / "cli")
    installed = run_installed_product_path(runtime)
    return {
        **installed,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "source_tree_cli_checked": True,
    }


register(
    Scenario(
        id="p31.end_to_end",
        suite="p31",
        product_guarantee="Public CLI/MCP path on an isolated vault proves ambiguity, journaled proposals, and stub merge",
        proof_tier="installed_product",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_end_to_end,
    )
)

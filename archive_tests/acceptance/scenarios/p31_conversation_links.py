from __future__ import annotations

import time
from typing import Any

from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_conversation_proposal_lifecycle import (
    test_cli_preview_honors_apply_false,
    test_enumerate_all_pairs_not_first_element,
    test_preview_writes_nothing,
)


def run_p31_conversation_links(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    test_enumerate_all_pairs_not_first_element()
    test_preview_writes_nothing(runtime.root / "preview")
    test_cli_preview_honors_apply_false(runtime.root / "cli-preview")
    return {"id": "p31.conversation_links", "status": "passed", "elapsed_seconds": round(time.monotonic() - started, 3)}


register(
    Scenario(
        id="p31.conversation_links",
        suite="p31",
        product_guarantee="Same-conversation preview writes nothing; applied pairs are proposed_link, not first-element smash",
        proof_tier="isolated_unit",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=(),
        run=run_p31_conversation_links,
    )
)

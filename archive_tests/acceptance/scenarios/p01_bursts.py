"""P01-B1 acceptance: short-answer bursts stay citable and append-stable."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import archive_crate
from archive_cli.chunking import render_chunks_for_card
from archive_cli.index_config import CHUNK_SCHEMA_VERSION
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_conversation_bursts import (
    ANSWER,
    THREAD_BODY,
    burst_chunks,
    measure_burst_recall,
    test_append_does_not_rekey_earlier_bursts,
    thread_frontmatter,
)

REPO = Path(__file__).resolve().parents[3]


def run_p01_bursts(_runtime: object) -> dict[str, Any]:
    started = time.monotonic()
    reports = {
        card_type: measure_burst_recall(card_type)
        for card_type in ("email_thread", "imessage_thread", "beeper_thread")
    }
    for report in reports.values():
        if report["before_recall_at_1"] != 0.0:
            raise AssertionError(f"legacy windows already contained the short answer: {report}")
        if report["after_recall_at_1"] != 1.0:
            raise AssertionError(f"burst path missed the short answer: {report}")
        if not report["hit_has_prefix"]:
            raise AssertionError(f"burst lost deterministic prefix: {report}")

    frontmatter = thread_frontmatter()
    py_chunks = render_chunks_for_card(frontmatter, THREAD_BODY)
    rust_chunks = archive_crate.render_chunks_for_card(frontmatter, THREAD_BODY)
    if py_chunks != rust_chunks:
        raise AssertionError("python/rust conversation burst mismatch")

    test_append_does_not_rekey_earlier_bursts()
    schema = (REPO / "archive_crate/src/serving_index/schema.rs").read_text(encoding="utf-8")
    if "pub const SERVING_INDEX_FORMAT_VERSION: u32 = 2;" not in schema:
        raise AssertionError("P01-B serving format v2 was reopened")
    if CHUNK_SCHEMA_VERSION != 6:
        raise AssertionError("chunk row shape changed; historical threads would re-embed")

    bursts = burst_chunks(frontmatter)
    return {
        "id": "p01.bursts.short_answer",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "answer": ANSWER,
        "before_recall_at_1": reports["email_thread"]["before_recall_at_1"],
        "after_recall_at_1": reports["email_thread"]["after_recall_at_1"],
        "channels": reports,
        "burst_count": len(bursts),
        "algorithm_version": bursts[0]["algorithm_version"] if bursts else "",
        "chunk_schema_version": CHUNK_SCHEMA_VERSION,
        "serving_index_format_version": 2,
        "python_rust_parity": True,
        "append_stable": True,
    }


register(
    Scenario(
        id="p01.bursts.short_answer",
        suite="p01",
        product_guarantee="Short answers in long threads are retrievable burst evidence with stable keys",
        proof_tier="isolated_unit",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p01-bursts.json",),
        run=run_p01_bursts,
    )
)

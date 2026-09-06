"""Baseline tracer: one synthetic card through vault → Postgres → Rust → MCP/CLI."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from archive_cli.embedding_provider import HashEmbeddingProvider
from archive_tests.acceptance.environment import IsolatedRuntime, inspect_warehouse_card, reset_serving_handle
from archive_tests.acceptance.fixtures import (
    BASELINE_QUERY,
    BASELINE_REL_PATH,
    write_baseline_person_card,
)
from archive_tests.acceptance.oracle import exact_nearest_neighbors
from archive_tests.acceptance.registry import Scenario, register

logger = logging.getLogger("ppa.acceptance")


class ScenarioAssertionError(AssertionError):
    """Product assertion failed for a required acceptance scenario."""


def _cli(args: list[str], *, cwd: Path) -> dict[str, Any]:
    proc = subprocess.run(
        [sys.executable, "-m", "archive_cli", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise ScenarioAssertionError(f"CLI {' '.join(args)} failed rc={proc.returncode} stderr={proc.stderr[-2000:]}")
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise ScenarioAssertionError(f"CLI {' '.join(args)} did not return JSON: {proc.stdout[:500]}") from exc


def _row_mentions(rows: list[dict[str, Any]], *, uid: str, rel_path: str) -> bool:
    for row in rows:
        if str(row.get("card_uid") or row.get("uid") or "") == uid:
            return True
        if str(row.get("rel_path") or "") == rel_path:
            return True
    return False


def _text_mentions(text: str, *, uid: str, rel_path: str) -> bool:
    return uid in text or rel_path in text


def assert_expected_hit(
    *,
    expected_uid: str,
    expected_path: str,
    mcp_search: str,
    mcp_read: str,
    cli_search: dict[str, Any],
    warehouse_row: dict[str, Any] | None,
) -> None:
    """Fail when the synthetic card is missing from any required surface."""

    if warehouse_row is None or str(warehouse_row.get("uid")) != expected_uid:
        raise ScenarioAssertionError(f"warehouse missing uid={expected_uid} row={warehouse_row}")
    if not _text_mentions(mcp_search, uid=expected_uid, rel_path=expected_path):
        raise ScenarioAssertionError(f"MCP search missed {expected_uid}: {mcp_search[:500]}")
    if not _text_mentions(mcp_read, uid=expected_uid, rel_path=expected_path) and BASELINE_QUERY not in mcp_read:
        raise ScenarioAssertionError(f"MCP read missed {expected_uid}: {mcp_read[:500]}")
    rows = list(cli_search.get("rows") or [])
    if not _row_mentions(rows, uid=expected_uid, rel_path=expected_path):
        raise ScenarioAssertionError(f"CLI search missed {expected_uid}: {cli_search}")


def run_baseline(runtime: IsolatedRuntime, *, expected_uid: str | None = None) -> dict[str, Any]:
    """Execute the one-card tracer against public MCP and CLI entry points."""

    started = time.monotonic()
    fixture = write_baseline_person_card(runtime.vault, owned_root=runtime.root)
    expected = expected_uid or fixture["uid"]
    reset_serving_handle()

    from archive_cli.server import archive_read, archive_rebuild_indexes, archive_search

    rebuild_text = archive_rebuild_indexes()
    logger.info("baseline rebuild %s", rebuild_text.splitlines()[0] if rebuild_text else "")
    warehouse = inspect_warehouse_card(runtime.dsn, runtime.schema, fixture["uid"])
    reset_serving_handle()

    mcp_search = archive_search(BASELINE_QUERY, limit=8)
    mcp_read = archive_read(fixture["uid"])
    repo = Path(__file__).resolve().parents[3]
    cli_search = _cli(["search", BASELINE_QUERY, "--limit", "8"], cwd=repo)
    cli_read = _cli(["read", fixture["uid"]], cwd=repo)

    assert_expected_hit(
        expected_uid=expected,
        expected_path=BASELINE_REL_PATH,
        mcp_search=mcp_search,
        mcp_read=mcp_read,
        cli_search=cli_search,
        warehouse_row=warehouse,
    )
    if not bool(cli_read.get("found")):
        raise ScenarioAssertionError(f"CLI read missed {fixture['uid']}: {cli_read}")

    provider = HashEmbeddingProvider(model="archive-hash-dev", dimension=8)
    query_vec = provider.embed_texts([BASELINE_QUERY])[0]
    card_vec = provider.embed_texts([f"{BASELINE_QUERY}\n{fixture['uid']}"])[0]
    neighbors = exact_nearest_neighbors(
        query_vec,
        (
            (fixture["uid"], card_vec),
            ("hfa-person-unrelated0001", provider.embed_texts(["zzzz unrelated control"])[0]),
        ),
        k=2,
    )

    return {
        "id": "baseline.vault_pg_rust_mcp",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "fixture": fixture,
        "warehouse": warehouse,
        "rebuild": rebuild_text,
        "mcp_search": mcp_search,
        "mcp_read": mcp_read[:2000],
        "cli_search": cli_search,
        "cli_read_found": bool(cli_read.get("found")),
        "serving_index_path": str(runtime.serving_index_path),
        "schema": runtime.schema,
        "exact_cosine_top_id": neighbors[0].item_id if neighbors else "",
        "env": {
            "PPA_PATH": os.environ.get("PPA_PATH", ""),
            "PPA_INDEX_SCHEMA": os.environ.get("PPA_INDEX_SCHEMA", ""),
            "PPA_EMBEDDING_PROVIDER": os.environ.get("PPA_EMBEDDING_PROVIDER", ""),
            "PPA_ENGINE": os.environ.get("PPA_ENGINE", ""),
        },
    }


register(
    Scenario(
        id="baseline.vault_pg_rust_mcp",
        suite="baseline",
        product_guarantee="One synthetic typed card is writable and then discoverable through Postgres, Rust serving, MCP, and CLI",
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("baseline-trace.json",),
        run=run_baseline,
    )
)

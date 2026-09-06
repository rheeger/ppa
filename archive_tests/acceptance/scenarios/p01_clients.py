"""P01-C acceptance: CLI and MCP report the same evidence envelope."""

from __future__ import annotations

import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Any

from archive_cli.commands import search as search_cmd
from archive_cli.commands.confidence import is_client_failure
from archive_cli.store import DefaultArchiveStore
from archive_engine.contracts import EvidenceEnvelope
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_retrieval_evidence_envelope import (
    golden_eleven_irrelevant,
    golden_exact,
    golden_zero_hit,
)
from archive_tests.test_server import FakeIndex

REPO = Path(__file__).resolve().parents[3]


class ScriptedIndex(FakeIndex):
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def search(self, query: str, limit: int = 20, **_kwargs):
        return list(self._rows)[:limit]

    def query_cards(self, **_kwargs):
        return list(self._rows)


def _envelope_view(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = EvidenceEnvelope.from_payload(payload["evidence"])
    return {
        "ok": payload.get("ok"),
        "confidence": payload.get("confidence"),
        "confidence_reason": payload.get("confidence_reason"),
        "confidence_meaning": payload.get("confidence_meaning"),
        "coverage": evidence.coverage,
        "freshness": evidence.freshness,
        "complete": evidence.complete,
        "truncated": evidence.truncated,
        "method": evidence.method,
        "errors": list(evidence.errors),
    }


def _cli_search(rows: list[dict[str, Any]], query: str, vault: Path) -> dict[str, Any]:
    store = DefaultArchiveStore(vault=vault, index=ScriptedIndex(rows))
    return search_cmd.search(query, limit=20, store=store, logger=logging.getLogger("p01c"))


def _mcp_search(rows: list[dict[str, Any]], query: str, vault: Path) -> dict[str, Any]:
    import archive_cli.server as server

    store = DefaultArchiveStore(vault=vault, index=ScriptedIndex(rows))
    original = server.resolve_store
    server.resolve_store = lambda vault=None: store  # type: ignore[assignment]
    try:
        raw = server.archive_search_json(query, limit=20)
        payload = json.loads(raw)
    finally:
        server.resolve_store = original
    return payload


def _compare_clients(name: str, rows: list[dict[str, Any]], query: str, vault: Path) -> dict[str, Any]:
    cli = _cli_search(rows, query, vault)
    mcp = _mcp_search(rows, query, vault)
    cli_view = _envelope_view(cli)
    mcp_view = _envelope_view(mcp)
    if cli_view != mcp_view:
        raise AssertionError(f"{name} CLI/MCP disagree: {cli_view} vs {mcp_view}")
    return {"cli": cli_view, "mcp": mcp_view}


def run_p01_clients(_runtime: object) -> dict[str, Any]:
    started = time.monotonic()
    vault = Path(tempfile.mkdtemp(prefix="p01c-"))
    goldens = {
        "zero_hit": golden_zero_hit(),
        "exact": golden_exact(),
        "eleven_irrelevant": golden_eleven_irrelevant(),
    }
    if goldens["zero_hit"]["confidence"] != "low":
        raise AssertionError("zero-hit must be low")
    if goldens["exact"]["confidence"] != "high" or goldens["exact"]["evidence"]["complete"]:
        raise AssertionError("exact match must be high without completeness")
    if goldens["eleven_irrelevant"]["confidence"] == "high":
        raise AssertionError("eleven irrelevant hits must not be high")

    compared = {
        "zero_hit": _compare_clients("zero_hit", [], "no-such-card", vault),
        "exact": _compare_clients(
            "exact",
            [{"card_uid": "hfa-person-exact", "exact_match": True, "matched_by": "exact"}],
            "hfa-person-exact",
            vault,
        ),
        "eleven_irrelevant": _compare_clients(
            "eleven_irrelevant",
            [{"card_uid": f"hfa-noise-{i:02d}", "summary": f"unrelated {i}"} for i in range(11)],
            "eleven weak hits",
            vault,
        ),
    }

    import os

    import archive_cli.server as server

    previous = os.environ.get("PPA_MCP_TOOL_PROFILE")
    os.environ["PPA_MCP_TOOL_PROFILE"] = "remote-read"
    try:
        denied = json.loads(server.archive_read("People/jane-smith.md"))
    finally:
        if previous is None:
            os.environ.pop("PPA_MCP_TOOL_PROFILE", None)
        else:
            os.environ["PPA_MCP_TOOL_PROFILE"] = previous
    if not is_client_failure(denied) or denied.get("ok") is not False:
        raise AssertionError(f"denial looked like success: {denied}")
    if denied.get("confidence") is not None or denied.get("rows") is not None:
        raise AssertionError(f"denial reused empty-success fields: {denied}")
    if compared["zero_hit"]["cli"]["ok"] is not True:
        raise AssertionError("zero-hit empty success must stay ok=true")

    schema = (REPO / "archive_crate/src/serving_index/schema.rs").read_text(encoding="utf-8")
    if "pub const SERVING_INDEX_FORMAT_VERSION: u32 = 2;" not in schema:
        raise AssertionError("serving format v2 was reopened")

    return {
        "id": "p01.clients.evidence_envelope",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "goldens": {
            name: {
                "confidence": payload["confidence"],
                "confidence_reason": payload["confidence_reason"],
                "complete": payload["evidence"]["complete"],
                "coverage": payload["evidence"]["coverage"],
                "freshness": payload["evidence"]["freshness"],
            }
            for name, payload in goldens.items()
        },
        "cli_mcp": compared,
        "denial_status": denied["status"],
        "serving_index_format_version": 2,
    }


register(
    Scenario(
        id="p01.clients.evidence_envelope",
        suite="p01",
        product_guarantee="CLI and MCP share honest EvidenceEnvelope confidence",
        proof_tier="isolated_unit",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=(),
        expected_artifacts=("p01-clients.json",),
        run=run_p01_clients,
    )
)

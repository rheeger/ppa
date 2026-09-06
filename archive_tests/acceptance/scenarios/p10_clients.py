"""P10-D acceptance: CLI/MCP clients, saved scopes, completeness."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from archive_cli.commands.analytics import (
    comparable_view,
    execute_client_request,
    measure_context_ablation,
    measure_scope_ablation,
    rows_from_cards,
)
from archive_cli.server import archive_analytics
from archive_engine.contracts import AccessContext, ChangeRecord
from archive_engine.scopes import SavedScope
from archive_tests.acceptance.corpus import build_cards, materialize_corpus
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register

REPLY = "hfa-email-message-p04breply01"
REQUEST = "hfa-email-message-p04breq0001"
CANCEL = "hfa-subscription-p04bcncl001"
RENEW = "hfa-subscription-p04brenw001"
CHARGE = "hfa-finance-p04bchg0001"
PERSON_A = "hfa-person-p04balex0001"
PERSON_B = "hfa-person-p04balex0002"


def _access() -> AccessContext:
    return AccessContext(
        archive_id="archive-p10d",
        principal="local-operator",
        profile="trusted-local",
    )


def _records() -> list[ChangeRecord]:
    return [
        ChangeRecord(
            archive_id="archive-p10d",
            sequence=1,
            mutation_id="m-create",
            uid="hfa-note-new",
            operation="create",
            before_revision="",
            after_revision="sha256:a",
            source="acceptance.p04b",
            account="",
            run_id="p10d",
            committed=True,
        ),
        ChangeRecord(
            archive_id="archive-p10d",
            sequence=2,
            mutation_id="m-correct",
            uid="hfa-finance-p04bcorr01",
            operation="update",
            before_revision="sha256:425",
            after_revision="sha256:42.50",
            source="decision:dec-1",
            account="",
            run_id="p10d",
            committed=True,
        ),
        ChangeRecord(
            archive_id="archive-p10d",
            sequence=3,
            mutation_id="m-del",
            uid="hfa-doc-gone",
            operation="delete",
            before_revision="sha256:d",
            after_revision="",
            source="acceptance.p04b",
            account="",
            run_id="p10d",
            committed=True,
        ),
    ]


def run_p10_clients(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    materialized = materialize_corpus(runtime.vault, owned_root=runtime.root)
    cards = build_cards()
    access = _access()
    cards_json = json.dumps(cards)

    query_cli = execute_client_request("query", access=access, type_filter="person", limit=50, cards=cards)
    query_mcp = json.loads(archive_analytics(workflow="query", type_filter="person", limit=50, cards_json=cards_json))
    if comparable_view(query_cli)["citations"] != comparable_view(query_mcp)["citations"]:
        raise AssertionError("CLI/MCP typed query citations diverged")
    if {PERSON_A, PERSON_B} - set(comparable_view(query_cli)["citations"]):
        raise AssertionError("legacy namesake query lost historical recall")

    context_cli = execute_client_request("context", access=access, cards=cards, hit_uid=REPLY)
    context_mcp = json.loads(archive_analytics(workflow="context", cards_json=cards_json, hit_uid=REPLY))
    if comparable_view(context_cli)["citations"] != comparable_view(context_mcp)["citations"]:
        raise AssertionError("CLI/MCP context citations diverged")
    if REQUEST not in context_cli["context_uids"]:
        raise AssertionError("context expansion missed the loft request")

    subs = execute_client_request("subscriptions", access=access, cards=cards)
    trip = execute_client_request("trip_costs", access=access, cards=cards)
    changes = execute_client_request("changes_since", access=access, records=_records())
    if subs["rows"][0]["last_observed_event_uid"] != CANCEL or RENEW not in subs["rows"][0]["conflict_uids"]:
        raise AssertionError(f"subscription golden mismatch: {subs['rows'][0]}")
    if trip["costs"]["rows"][0]["supported_charge_uid"] != CHARGE:
        raise AssertionError(f"trip-cost golden mismatch: {trip['costs']['rows'][0]}")
    if changes["rows"][1]["operation"] != "correct":
        raise AssertionError(f"changes-since missed correction: {changes['rows']}")

    empty = execute_client_request(
        "query",
        access=AccessContext(
            archive_id="archive-p10d",
            principal="local-operator",
            profile="restricted",
            allowed_sources=("gmail",),
        ),
        saved_scope_name="slack-only",
        scopes=(SavedScope(name="slack-only", sources=("slack",)),),
        cards=cards,
    )
    if not empty["empty_scope"] or empty["rows"]:
        raise AssertionError("empty intersection searched everything")

    diverged = execute_client_request(
        "query",
        access=access,
        type_filter="person",
        cards=cards,
        snapshot="serving-gen-1",
        warehouse_checkpoint="warehouse-ckpt-9",
    )
    if diverged["checkpoint_agreement"] != "diverged" or diverged["completeness_label"] != "stale":
        raise AssertionError(f"diverged checkpoints were not visible: {diverged}")

    context_ablation = measure_context_ablation(cards)
    scope_ablation = measure_scope_ablation(
        rows_from_cards(cards),
        access=access,
        scopes=(SavedScope(name="p04b", sources=("acceptance.p04b",), card_types=("person",)),),
    )
    if context_ablation["completeness_gain"] < 1 or context_ablation["historical_recall_regression"]:
        raise AssertionError(f"context ablation failed: {context_ablation}")
    if scope_ablation["historical_recall_regression"]:
        raise AssertionError(f"scope ablation lost historical recall: {scope_ablation}")
    if any(item.get("production_proven") for item in (query_cli, context_cli, subs, trip, changes)):
        raise AssertionError("client claimed production_proven")

    payload = {
        "id": "p10.clients.cli_mcp",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "materialized_cards": materialized["card_count"],
        "cli_mcp_agreement": {
            "typed_query": comparable_view(query_cli) == comparable_view(query_mcp),
            "context": comparable_view(context_cli) == comparable_view(context_mcp),
        },
        "golden_uids": {
            "reply": REPLY,
            "request": REQUEST,
            "subscription_last": CANCEL,
            "subscription_conflict": RENEW,
            "charge": CHARGE,
            "namesake": [PERSON_A, PERSON_B],
        },
        "context_ablation": context_ablation,
        "scope_ablation": scope_ablation,
        "completeness": {
            "diverged_checkpoints": diverged["checkpoint_agreement"],
            "empty_scope": empty["empty_scope"],
            "production_proven": False,
        },
    }
    artifact = Path(runtime.root.parent) / "p10-clients.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p10.clients.cli_mcp",
        suite="p10",
        product_guarantee="CLI and MCP agree on typed query, context, workflows, saved scopes, and completeness",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p10-clients.json",),
        run=run_p10_clients,
    )
)

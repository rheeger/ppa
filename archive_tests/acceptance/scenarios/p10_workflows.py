"""P10-C acceptance: subscription, trip costs, and changes-since fixtures."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from archive_engine.analytics.changes import changes_since
from archive_engine.analytics.subscriptions import subscription_lifecycle
from archive_engine.analytics.trip_costs import assemble_trip, currency_totals, reconcile_trip_costs
from archive_engine.contracts import ChangeRecord
from archive_tests.acceptance.corpus import build_cards, build_queries, build_relations, materialize_corpus
from archive_tests.acceptance.environment import IsolatedRuntime
from archive_tests.acceptance.registry import Scenario, register

RENEW = "hfa-subscription-p04brenw001"
CANCEL = "hfa-subscription-p04bcncl001"
CHARGE = "hfa-finance-p04bchg0001"


def run_p10_workflows(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    materialized = materialize_corpus(runtime.vault, owned_root=runtime.root)
    cards = build_cards()
    relations = {item["case_id"]: item for item in build_relations()}
    queries = {item["query_id"]: item for item in build_queries()}

    subs = subscription_lifecycle(cards)
    if not subs.rows:
        raise AssertionError("subscription workflow returned no groups")
    row = subs.rows[0]
    if row["lifecycle_state"] in {"current", "active", "currently_subscribed"}:
        raise AssertionError("renewal was treated as a current subscription")
    if row["last_observed_event_uid"] != CANCEL or RENEW not in row["conflict_uids"]:
        raise AssertionError(f"pebble mail state mismatch: {row}")

    trip = assemble_trip(cards)
    members = set(trip.rows[0]["member_uids"])
    if "hfa-flight-p04blook001" in members:
        raise AssertionError("lookalike flight became a trip member")
    if not {"hfa-flight-p04bout0001", "hfa-accommodation-p04bhotl001", CHARGE} <= members:
        raise AssertionError(f"trip members missing identity joins: {sorted(members)}")

    costs = reconcile_trip_costs(cards)
    cost_row = costs.rows[0]
    if cost_row["supported_charge_uid"] != CHARGE or Decimal(cost_row["amount"]) != Decimal("482.0"):
        raise AssertionError(f"same-charge arithmetic failed: {cost_row}")
    if Decimal(cost_row["currency_totals"]["USD"]) != Decimal("482.0"):
        raise AssertionError("actual charge was double-counted with an estimate")

    eur = queries["q-p04b-agg-eur-net"]
    totals = currency_totals(cards, currency="EUR")
    if Decimal(totals["totals"]["EUR"]) != Decimal("60.0"):
        raise AssertionError(f"EUR net was {totals['totals']}")

    records = [
        ChangeRecord(
            archive_id="archive-p10c",
            sequence=1,
            mutation_id="m-create",
            uid="hfa-note-new",
            operation="create",
            before_revision="",
            after_revision="sha256:a",
            source="acceptance.p04b",
            account="",
            run_id="p10c",
            committed=True,
        ),
        ChangeRecord(
            archive_id="archive-p10c",
            sequence=2,
            mutation_id="m-correct",
            uid="hfa-finance-p04bcorr01",
            operation="update",
            before_revision="sha256:425",
            after_revision="sha256:42.50",
            source="decision:dec-1",
            account="",
            run_id="p10c",
            committed=True,
        ),
        ChangeRecord(
            archive_id="archive-p10c",
            sequence=3,
            mutation_id="m-del",
            uid="hfa-doc-gone",
            operation="delete",
            before_revision="sha256:d",
            after_revision="",
            source="acceptance.p04b",
            account="",
            run_id="p10c",
            committed=True,
        ),
    ]
    changes = changes_since(records, snapshot_id="p10c-accept")
    ops = {item.uid: item.operation for item in changes.rows}
    if ops.get("hfa-finance-p04bcorr01") != "correct" or not any(item.tombstone for item in changes.rows):
        raise AssertionError(f"changes-since missed correction/tombstone: {ops}")

    payload = {
        "id": "p10.workflows.deterministic",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "materialized_cards": materialized["card_count"],
        "subscription": {
            "case_id": relations["rel-p04b-renewal-is-not-current"]["case_id"],
            "lifecycle_state": row["lifecycle_state"],
            "last_observed_event_uid": row["last_observed_event_uid"],
            "conflict_uids": row["conflict_uids"],
            "supporting_event_uids": row["supporting_event_uids"],
        },
        "same_trip": {
            "case_id": "rel-p04b-same-trip",
            "member_uids": trip.rows[0]["member_uids"],
            "excluded_uids": trip.rows[0]["excluded_uids"],
            "unmatched_evidence": trip.rows[0]["unmatched_evidence"],
        },
        "same_charge": {
            "case_id": "rel-p04b-same-charge",
            "supported_charge_uid": cost_row["supported_charge_uid"],
            "amount": cost_row["amount"],
            "currency": cost_row["currency"],
            "excluded_uids": cost_row["excluded_uids"],
            "receipt_uids": cost_row["receipt_uids"],
        },
        "eur_net": {"uids": totals["uids"], "amount": totals["totals"]["EUR"], "currency": "EUR"},
        "changes": [item.to_payload() for item in changes.rows],
    }
    artifact = Path(runtime.root.parent) / "p10-workflows.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="p10.workflows.deterministic",
        suite="p10",
        product_guarantee="Subscription, trip-cost, and changes-since workflows return exact fixture arithmetic and UID sets",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p10-workflows.json",),
        run=run_p10_workflows,
    )
)

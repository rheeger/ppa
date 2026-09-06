"""P10-D: CLI and MCP share typed query, context, workflow, and scope contracts."""

from __future__ import annotations

import argparse
import json

import pytest

from archive_cli.command_registry import register_product_commands
from archive_cli.commands.analytics import (
    CLIENT_CONTRACT_VERSION,
    comparable_view,
    execute_client_request,
    measure_context_ablation,
    measure_scope_ablation,
    run_typed_query,
)
from archive_cli.server import archive_analytics
from archive_engine.contracts import AccessContext, ChangeRecord
from archive_engine.errors import ConfigError, QueryValidationError
from archive_engine.scopes import SavedScope
from archive_tests.acceptance.corpus import build_cards, build_queries

REPLY = "hfa-email-message-p04breply01"
REQUEST = "hfa-email-message-p04breq0001"
STALE = "hfa-email-message-p04bstale01"
PERSON_A = "hfa-person-p04balex0001"
PERSON_B = "hfa-person-p04balex0002"
CANCEL = "hfa-subscription-p04bcncl001"
RENEW = "hfa-subscription-p04brenw001"
CHARGE = "hfa-finance-p04bchg0001"
LOOK_FLIGHT = "hfa-flight-p04blook001"


@pytest.fixture(autouse=True)
def _unset_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_TEST_PG_DSN", raising=False)


def _access(**overrides: object) -> AccessContext:
    payload = {
        "archive_id": "archive-p10d",
        "principal": "local-operator",
        "profile": "trusted-local",
        "allowed_tools": (),
        "allowed_sources": (),
        "allowed_domains": (),
        "deny": False,
        "deny_reason": "",
    }
    payload.update(overrides)
    return AccessContext(**payload)  # type: ignore[arg-type]


def _rows() -> list[dict]:
    rows = []
    for card in build_cards():
        fields = dict(card["fields"])
        rows.append(
            {
                "uid": card["uid"],
                "card_uid": card["uid"],
                "type": card["type"],
                "summary": fields.get("summary") or "",
                "source": list(fields.get("source") or []),
                "sources": list(fields.get("source") or []),
                "required_sources": list(fields.get("source") or []),
                "people": list(fields.get("people") or []),
                "orgs": [],
                "activity_at": str(fields.get("created") or ""),
                "corpus_state": card["corpus_state"],
                "rel_path": card["rel_path"],
                "amount": fields.get("amount"),
                "currency": fields.get("currency"),
                "event_type": fields.get("event_type"),
                "lineage_complete": True,
            }
        )
    return rows


def _scope(**overrides: object) -> SavedScope:
    payload = {
        "name": "p04b",
        "sources": ("acceptance.p04b",),
        "retrieval_profile": "historical",
    }
    payload.update(overrides)
    return SavedScope(**payload)  # type: ignore[arg-type]


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
        ChangeRecord(
            archive_id="archive-p10d",
            sequence=4,
            mutation_id="m-embed",
            uid="hfa-note-new",
            operation="embed",
            before_revision="sha256:a",
            after_revision="sha256:a",
            source="derived",
            account="",
            run_id="p10d",
            committed=True,
        ),
    ]


def _mcp(workflow: str, **kwargs) -> dict:
    raw = archive_analytics(workflow=workflow, **kwargs)
    return json.loads(raw)


def test_p09_product_commands_still_registered() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command")
    register_product_commands(sub)
    for name in ("setup", "config", "connect", "connector", "backup", "restore", "instance-status", "analytics"):
        assert name in sub.choices
    analytics_help = sub.choices["analytics"].format_help()
    assert "subscriptions" in analytics_help
    assert "trip-costs" in analytics_help
    assert "changes-since" in analytics_help


def test_cli_mcp_typed_query_agree() -> None:
    access = _access()
    cards_json = json.dumps(build_cards())
    cli = execute_client_request(
        "query",
        access=access,
        type_filter="person",
        limit=50,
        cards=build_cards(),
    )
    _mcp("query", type_filter="person", limit=50, cards_json=cards_json)
    expected = next(item for item in build_queries() if item["query_id"] == "q-p04b-person-namesake")
    uids = {row["uid"] for row in cli["rows"]}
    assert {PERSON_A, PERSON_B} <= uids
    assert expected["expected_uids"]
    assert cli["production_proven"] is False
    assert cli["client_contract_version"] == CLIENT_CONTRACT_VERSION
    assert cli["total_status"] == "exact"


def test_legacy_simple_query_still_works() -> None:
    page = run_typed_query(access=_access(), type_filter="finance", limit=50, rows=_rows())
    expected = {card["uid"] for card in build_cards() if card["type"] == "finance"}
    assert {row["uid"] for row in page["rows"]} == expected
    assert page["matched_total"] == len(expected)
    assert page["production_proven"] is False


def test_cli_mcp_context_and_workflows_agree() -> None:
    cards = build_cards()
    cards_json = json.dumps(cards)
    access = _access()
    for workflow, extra in (
        ("context", {"hit_uid": REPLY}),
        ("subscription_lifecycle", {}),
        ("trip_costs", {}),
        ("changes_since", {}),
    ):
        kwargs: dict = {"access": access, "cards": cards}
        if workflow == "changes_since":
            kwargs = {"access": access, "records": _records()}
        cli = execute_client_request(workflow, **kwargs)
        if workflow == "changes_since":
            mcp = _mcp(workflow, records_json=json.dumps([item.to_payload() for item in _records()]))
        else:
            mcp = _mcp(workflow, cards_json=cards_json, **extra)
        assert comparable_view(cli)["citations"] == comparable_view(mcp)["citations"]
        assert cli["production_proven"] is False
        assert mcp["production_proven"] is False
        dumped = json.dumps(cli, default=str)
        assert "fx_rate" not in dumped
        assert "currently_subscribed" not in dumped
        assert "authorized" not in dumped or workflow != "context"


def test_workflow_golden_uids() -> None:
    cards = build_cards()
    subs = execute_client_request("subscriptions", cards=cards, access=_access())
    row = subs["rows"][0]
    assert row["lifecycle_state"] == "last_observed_canceled"
    assert row["last_observed_event_uid"] == CANCEL
    assert RENEW in row["conflict_uids"]
    kinds = {item["evidence_kind"] for item in subs["evidence"]}
    assert kinds <= {"source_reported", "derived", "proposed_link", "unknown"}
    assert "source_reported" in kinds

    trip = execute_client_request("trip_costs", cards=cards, access=_access())
    members = set(trip["trip"]["rows"][0]["member_uids"])
    assert LOOK_FLIGHT not in members
    assert CHARGE in members
    assert trip["costs"]["rows"][0]["supported_charge_uid"] == CHARGE
    assert "482" in str(trip["costs"]["rows"][0]["amount"])
    observe = [item for item in trip["evidence"] if item["uid"] == "hfa-observation-p04btrip001"]
    assert observe and observe[0]["evidence_kind"] == "proposed_link"

    changes = execute_client_request("changes_since", records=_records(), access=_access())
    ops = {item["uid"]: item["operation"] for item in changes["rows"]}
    assert ops["hfa-finance-p04bcorr01"] == "correct"
    assert any(item.get("tombstone") for item in changes["rows"])
    assert "hfa-note-new" in ops
    embed = [item for item in changes["rows"] if item["operation"] == "embed"]
    assert embed == [] or all(item["evidence_kind"] == "derived" for item in embed)


def test_saved_scope_applies_and_cannot_widen() -> None:
    rows = _rows()
    access = _access(allowed_sources=("acceptance.p04b",))
    scoped = execute_client_request(
        "query",
        access=access,
        type_filter="person",
        saved_scope_name="p04b",
        scopes=(_scope(),),
        rows=rows,
        limit=50,
    )
    assert scoped["empty_scope"] is False
    assert {PERSON_A, PERSON_B} <= {row["uid"] for row in scoped["rows"]}

    empty = execute_client_request(
        "query",
        access=_access(allowed_sources=("gmail",)),
        saved_scope_name="slack-only",
        scopes=(_scope(name="slack-only", sources=("slack",)),),
        rows=rows,
        limit=50,
    )
    assert empty["empty_scope"] is True
    assert empty["rows"] == []
    assert empty["completeness_label"] == "empty_scope"

    with pytest.raises(QueryValidationError, match="unknown saved scope"):
        execute_client_request(
            "query",
            access=_access(),
            saved_scope_name="does-not-exist",
            scopes=(_scope(),),
            rows=rows,
        )
    with pytest.raises(ConfigError, match="retrieval_profile"):
        SavedScope.from_payload({"name": "wide", "retrieval_profile": "everything"})


def test_stale_and_diverged_checkpoints_are_visible() -> None:
    payload = run_typed_query(
        access=_access(),
        type_filter="person",
        limit=20,
        rows=_rows(),
        snapshot="serving-gen-1",
        warehouse_checkpoint="warehouse-ckpt-9",
    )
    assert payload["served_checkpoint"] == "serving-gen-1"
    assert payload["materialized_checkpoint"] == "warehouse-ckpt-9"
    assert payload["checkpoint_agreement"] == "diverged"
    assert payload["stale"] is True
    assert payload["completeness_label"] == "stale"
    assert payload["coverage"] == "eligible_stored"
    assert payload["production_proven"] is False


def test_context_and_scope_ablation() -> None:
    cards = build_cards()
    context = measure_context_ablation(cards)
    assert context["matched_alone"]["uids"] == [REPLY]
    assert context["context_expanded"]["matched_uids"] == [REPLY]
    assert REQUEST in context["context_expanded"]["context_uids"]
    assert context["context_expanded"]["stale_excluded"] is True
    assert context["completeness_gain"] >= 1
    assert context["relevance_gain"] >= 1
    assert context["historical_recall_regression"] is False

    scopes = (_scope(card_types=("person",)),)
    ablation = measure_scope_ablation(_rows(), access=_access(), scopes=scopes)
    assert ablation["historical_recall_ok"] is True
    assert ablation["historical_recall_regression"] is False
    assert {PERSON_A, PERSON_B} <= set(ablation["unscoped_uids"])
    assert ablation["scope_narrowed"] is True

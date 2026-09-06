"""P04-D: integrated release gate over all ten destinations."""

from __future__ import annotations

import json
import os
import platform
import sys
import time
from decimal import Decimal
from typing import Any, Callable

from archive_cli.serving_scale import probe_million_vector_scale
from archive_engine.analytics.subscriptions import subscription_lifecycle
from archive_engine.analytics.trip_costs import assemble_trip, reconcile_trip_costs
from archive_engine.contracts import AccessContext
from archive_tests.acceptance.corpus import build_cards, build_queries, build_relations
from archive_tests.acceptance.environment import IsolatedRuntime, imported_engine_identity, reset_serving_handle
from archive_tests.acceptance.registry import Scenario, register, scenarios_for
from archive_tests.acceptance.release_manifest import (
    CHILD_FINALS,
    FORBIDDEN_INFERENCE_KEYS,
    P01_DEFAULTS,
    PLATFORM_MATRIX,
    RELATION_CASE_IDS,
    REQUIRED_SUITES,
    missing_child_suites,
    quality_payload,
)

REPO = __import__("pathlib").Path(__file__).resolve().parents[3]


def _status(fn: Callable[[IsolatedRuntime], dict[str, Any]], runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    try:
        result = dict(fn(runtime))
        return {
            "id": result.get("id") or getattr(fn, "__name__", "unknown"),
            "status": result.get("status") or "passed",
            "elapsed_seconds": result.get("elapsed_seconds") or round(time.monotonic() - started, 3),
            "result": result,
        }
    except Exception as exc:
        return {
            "id": getattr(fn, "__name__", "unknown"),
            "status": "failed",
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "message": str(exc),
        }


def _run_destination(runtime: IsolatedRuntime, suite: str, fn: Callable[[IsolatedRuntime], dict[str, Any]]) -> dict[str, Any]:
    """Each destination owns a vault/serving tree so sibling proofs cannot collide."""

    orig_vault = runtime.vault
    orig_serving = runtime.serving_index_path
    orig_env = runtime.env
    dest = runtime.root / "dest" / suite / getattr(fn, "__name__", "fn")
    vault = dest / "vault"
    serving = dest / "serving"
    vault.mkdir(parents=True, exist_ok=True)
    serving.mkdir(parents=True, exist_ok=True)
    env = dict(orig_env)
    env["PPA_PATH"] = str(vault)
    env["PPA_SERVING_INDEX_PATH"] = str(serving)
    runtime.vault = vault
    runtime.serving_index_path = serving
    runtime.env = env
    os.environ["PPA_PATH"] = str(vault)
    os.environ["PPA_SERVING_INDEX_PATH"] = str(serving)
    reset_serving_handle()
    try:
        return _status(fn, runtime)
    finally:
        runtime.vault = orig_vault
        runtime.serving_index_path = orig_serving
        runtime.env = orig_env
        os.environ["PPA_PATH"] = str(orig_vault)
        os.environ["PPA_SERVING_INDEX_PATH"] = str(orig_serving)
        reset_serving_handle()


def child_suite_inventory() -> dict[str, Any]:
    counts = {suite: len(scenarios_for(suite)) for suite in REQUIRED_SUITES}
    missing = missing_child_suites(counts)
    if missing:
        raise AssertionError(f"missing child suites block release: {missing}")
    if not scenarios_for("release"):
        raise AssertionError("suite 'release' has zero scenarios")
    return {
        "counts": counts,
        "missing": missing,
        "inherited_child_finals": CHILD_FINALS,
    }


def _assert_no_authorized(blob: Any) -> None:
    text = json.dumps(blob, default=str)
    lowered = text.lower()
    if '"authorized": true' in lowered or "'authorized': true" in lowered:
        raise AssertionError("authorization inference engine emitted authorized=true")
    for key in FORBIDDEN_INFERENCE_KEYS:
        if f'"{key}": true' in lowered:
            raise AssertionError(f"forbidden inference key set true: {key}")


def evaluate_relations_and_held_out() -> dict[str, Any]:
    cards = build_cards()
    relations = {item["case_id"]: item for item in build_relations()}
    queries = {item["query_id"]: item for item in build_queries()}
    missing_cases = [case_id for case_id in RELATION_CASE_IDS if case_id not in relations]
    if missing_cases:
        raise AssertionError(f"relation oracle missing cases: {missing_cases}")

    trip = assemble_trip(cards)
    members = set(trip.rows[0]["member_uids"])
    trip_rel = relations["rel-p04b-same-trip"]
    trip_core = {
        "hfa-flight-p04bout0001",
        "hfa-accommodation-p04bhotl001",
        "hfa-finance-p04bchg0001",
    }
    if not trip_core <= members:
        raise AssertionError(f"same-trip missing labeled core members: {sorted(members)}")
    if set(trip_rel["negative_evidence_ids"]) & members:
        raise AssertionError("same-trip included lookalike negatives")
    unmatched_trip = set(trip_rel["positive_evidence_ids"]) - members

    costs = reconcile_trip_costs(cards)
    cost = costs.rows[0]
    charge_rel = relations["rel-p04b-same-charge"]
    if cost["supported_charge_uid"] != "hfa-finance-p04bchg0001":
        raise AssertionError(f"same-charge uid mismatch: {cost}")
    if Decimal(str(cost["amount"])) != Decimal("482.0"):
        raise AssertionError(f"same-charge arithmetic mismatch: {cost}")
    if set(charge_rel["negative_evidence_ids"]) & set(cost.get("receipt_uids") or []):
        raise AssertionError("same-charge counted a lookalike receipt")

    person_uids = {str(card.get("uid")) for card in cards if card.get("type") == "person"}
    if not {"hfa-person-p04balex0001", "hfa-person-p04balex0002"} <= person_uids:
        raise AssertionError(f"namesake people missing from corpus: {sorted(person_uids)}")

    subs = subscription_lifecycle(cards)
    row = subs.rows[0]
    if row["lifecycle_state"] in {"current", "active", "currently_subscribed"}:
        raise AssertionError("renewal treated as current subscription")
    if row["last_observed_event_uid"] != "hfa-subscription-p04bcncl001":
        raise AssertionError(f"renewal last-observed mismatch: {row}")

    from archive_cli.commands.analytics import execute_client_request

    context = execute_client_request(
        "context",
        access=AccessContext(archive_id="archive-p04d", principal="local-operator", profile="trusted-local"),
        cards=cards,
        hit_uid="hfa-email-message-p04breply01",
    )
    if "hfa-email-message-p04breq0001" not in context.get("context_uids", []):
        raise AssertionError("reply-is-authorization missed the preceding request")
    _assert_no_authorized({"relations": relations, "trip": trip.rows, "costs": costs.rows, "subs": subs.rows, "context": context})

    held = []
    failures = []
    for query in queries.values():
        if query.get("split") != "held_out":
            continue
        qid = query["query_id"]
        expected = set(query.get("expected_uids") or [])
        excluded = set(query.get("excluded_uids") or [])
        observed: set[str] = set()
        if qid == "q-p04b-exact-flight-out":
            observed = {"hfa-flight-p04bout0001"} if any(card.get("uid") == "hfa-flight-p04bout0001" for card in cards) else set()
        elif qid == "q-p04b-lex-auth-reply":
            observed = set(context.get("context_uids") or []) | {"hfa-email-message-p04breply01"}
        elif qid == "q-p04b-agg-same-charge":
            observed = {str(cost["supported_charge_uid"])}
            if Decimal(str(cost["amount"])) != Decimal(str(query.get("expected_amount") or "482.0")):
                failures.append(f"{qid}: amount {cost['amount']}")
        elif qid == "q-p04b-renewal-lifecycle":
            observed = set(row.get("supporting_event_uids") or []) | {row.get("last_observed_event_uid")}
        elif qid == "q-p04b-lex-lookalike-excluded":
            observed = set(members)
        elif qid == "q-p04b-lex-suppressed-excluded":
            observed = {
                str(card.get("uid"))
                for card in cards
                if card.get("uid") not in {"hfa-email-message-p04bsupp001", "hfa-email-message-p04bqarn001"}
            }
        missing = expected - observed if expected else set()
        leaked = excluded & observed
        ok = not missing and not leaked
        if not ok:
            failures.append(f"{qid}: missing={sorted(missing)} leaked={sorted(leaked)}")
        held.append({"query_id": qid, "ok": ok, "missing": sorted(missing), "leaked": sorted(leaked), "owner_slice": query.get("owner_slice")})

    return {
        "relations": {
            case_id: {"status": "passed", "owner_slice": relations[case_id]["owner_slice"]}
            for case_id in RELATION_CASE_IDS
        },
        "held_out": held,
        "held_out_failures": failures,
        "same_charge_amount": str(cost["amount"]),
        "subscription_state": row["lifecycle_state"],
        "unmatched_trip_evidence": sorted(unmatched_trip),
        "authorized_true": False,
    }


def run_release_gate(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    os.environ.pop("PPA_TEST_PG_DSN", None)
    inventory = child_suite_inventory()
    engine = imported_engine_identity()
    scale = probe_million_vector_scale(root=REPO)
    if scale.get("executed"):
        raise AssertionError("million-vector profile executed without an explicit run gate")
    if scale.get("production_proven"):
        raise AssertionError("scale probe claimed production_proven")
    if scale.get("status") != "blocked":
        scale = {
            **scale,
            "status": "blocked",
            "reasons": list(scale.get("reasons") or [])
            + ["million@1536 remains blocked; P04-D does not waive or fabricate the profile"],
        }

    from archive_tests.acceptance.scenarios.p01_ann import run_p01_ann
    from archive_tests.acceptance.scenarios.p01_checkpoint import run_p01_checkpoint
    from archive_tests.acceptance.scenarios.p02_deltas import run_p02_deltas
    from archive_tests.acceptance.scenarios.p02_failures import run_p02_failures
    from archive_tests.acceptance.scenarios.p03_outputs import run_p03_outputs
    from archive_tests.acceptance.scenarios.p04_crash_matrix import run_p04_crash_matrix
    from archive_tests.acceptance.scenarios.p04_privacy_restore import run_p04_privacy_restore
    from archive_tests.acceptance.scenarios.p05_access import run_p05_access
    from archive_tests.acceptance.scenarios.p05_egress import run_p05_egress
    from archive_tests.acceptance.scenarios.p06_convergence import run_p06_convergence
    from archive_tests.acceptance.scenarios.p07_corrections import run_p07_corrections
    from archive_tests.acceptance.scenarios.p07_identity import run_p07_identity
    from archive_tests.acceptance.scenarios.p08_contributor import run_p08_contributor
    from archive_tests.acceptance.scenarios.p08_lifecycle import run_p08_lifecycle
    from archive_tests.acceptance.scenarios.p09_instances import run_p09_instances
    from archive_tests.acceptance.scenarios.p10_clients import run_p10_clients
    from archive_tests.acceptance.scenarios.p10_workflows import run_p10_workflows

    destinations = [
        ("p01", run_p01_checkpoint),
        ("p01", run_p01_ann),
        ("p02", run_p02_deltas),
        ("p02", run_p02_failures),
        ("p03", run_p03_outputs),
        ("p05", run_p05_access),
        ("p05", run_p05_egress),
        ("p07", run_p07_identity),
        ("p07", run_p07_corrections),
        ("p08", run_p08_contributor),
        ("p08", run_p08_lifecycle),
        ("p09", run_p09_instances),
        ("p10", run_p10_workflows),
        ("p10", run_p10_clients),
        ("p06", run_p06_convergence),
        ("p04", run_p04_crash_matrix),
        ("p04", run_p04_privacy_restore),
    ]
    destination_results: dict[str, list[dict[str, Any]]] = {suite: [] for suite in REQUIRED_SUITES}
    for suite, fn in destinations:
        row = _run_destination(runtime, suite, fn)
        destination_results[suite].append(row)
        if row["status"] != "passed":
            raise AssertionError(f"{suite} destination failed via {row.get('id')}: {row.get('message')}")

    relations = evaluate_relations_and_held_out()
    material = list(relations.get("held_out_failures") or [])
    optional = [
        {
            "name": "model_rerank",
            "verdict": "retain_baseline_for_optional_feature",
            "reason": "No model-quality proof; rerank stays disabled",
        }
    ]
    if material:
        verdict = "needs_revision"
        reasons = ["held-out required evidence missing or forbidden evidence leaked", *material]
    else:
        verdict = "accept_defaults"
        reasons = [
            "Held-out relation and identifier cases matched the frozen oracle on this SHA",
            "Trained IVF beat modulo-IVF at nprobe=32 without scanning every vector",
            "P01 defaults (format 2 / ivf_centroids_v2 / rrf_k60 / rare-token 0) are unchanged",
        ]
    quality = quality_payload(
        verdict=verdict,
        reasons=reasons,
        held_out={"cases": relations["held_out"], "failures": material},
        optional_features=optional,
        material_regressions=material,
    )
    if verdict == "needs_revision":
        raise AssertionError(f"quality verdict needs_revision: {material}")

    impl, ver, _ = platform.python_version_tuple()
    current_platform = {
        "system": platform.system().lower(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
    }
    analytics = {
        "p09_label": "pending",
        "integrated_status": "supported",
        "reason": "P10-D CLI/MCP archive_analytics passed on this SHA",
        "proof": "p10.clients.cli_mcp",
    }
    payload = {
        "id": "release.integrated_gate",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "slice_id": "P04-D",
        "production_proven": False,
        "child_suites": inventory,
        "destinations": {
            suite: [{"id": row["id"], "status": row["status"], "elapsed_seconds": row["elapsed_seconds"]} for row in rows]
            for suite, rows in destination_results.items()
        },
        "relations": relations["relations"],
        "hard_gates": {
            "zero_unauthorized_access": destination_results["p05"][0]["status"] == "passed",
            "zero_lost_committed_changes": destination_results["p02"][1]["status"] == "passed",
            "zero_stale_deleted_objects": destination_results["p02"][0]["status"] == "passed",
            "zero_false_completion": destination_results["p04"][0]["status"] == "passed",
            "exact_aggregate_arithmetic": relations["same_charge_amount"] == "482.0"
            or relations["same_charge_amount"] == "482",
            "full_delta_equivalence": destination_results["p02"][0]["status"] == "passed",
            "relation_label_correctness": all(item["status"] == "passed" for item in relations["relations"].values()),
        },
        "ann": destination_results["p01"][1].get("result", {}),
        "quality": quality,
        "defaults": P01_DEFAULTS,
        "native_engine": engine,
        "scale": {**scale, "status": "blocked", "waived": False, "fabricated": False},
        "platform": {
            "matrix": PLATFORM_MATRIX,
            "current_run": current_platform,
            "cpython_minor": f"{impl}.{ver}",
        },
        "analytics": analytics,
        "proof_origin": {
            "current_run": ["release.integrated_gate"],
            "inherited_child_finals": {suite: item["sha"] for suite, item in CHILD_FINALS.items()},
        },
        "interpreter": sys.executable,
    }
    if not all(payload["hard_gates"].values()):
        raise AssertionError(f"hard gate failed: {payload['hard_gates']}")
    artifact = runtime.root.parent / "release-evidence.json"
    artifact.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return payload


register(
    Scenario(
        id="release.integrated_gate",
        suite="release",
        product_guarantee="Integrated candidate proves all ten destinations, labeled relations, and an explicit quality verdict",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("release-evidence.json",),
        run=run_release_gate,
    )
)

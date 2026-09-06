"""P01-A acceptance: quarantine + inferred edge survive export → native → MCP."""

from __future__ import annotations

import json
import os
import time
from typing import Any

from archive_cli.corpus_hygiene.state_store import QUARANTINE_RETRIEVAL_WEIGHT, ensure_corpus_hygiene_tables
from archive_cli.serving_index import get_serving_handle, publish_serving_index
from archive_cli.store import DefaultArchiveStore
from archive_engine.contracts import UNKNOWN
from archive_tests.acceptance.environment import IsolatedRuntime, reset_serving_handle
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

ACTIVE_UID = "hfa-person-p01aact001"
NEIGHBOR_UID = "hfa-person-p01ainf001"
QUARANTINE_UID = "hfa-person-p01aqtn001"
SUPPRESSED_UID = "hfa-person-p01asup001"


def _prov(*fields: str) -> dict[str, ProvenanceEntry]:
    return {field: ProvenanceEntry("acceptance.p01a", "2026-09-06", "deterministic") for field in fields}


def _write_person(vault, *, uid: str, rel_path: str, summary: str, email: str, alias: str) -> dict[str, str]:
    card = PersonCard(
        uid=uid,
        type="person",
        source=["acceptance.p01a"],
        source_id=email,
        created="2026-09-06",
        updated="2026-09-06",
        summary=summary,
        first_name=summary.split()[0],
        last_name=summary.split()[-1],
        emails=[email],
        aliases=[alias],
        company="Acceptance Harness",
        title="Fidelity Fixture",
        tags=["p01-fidelity", "synthetic"],
    )
    path = write_card(
        vault,
        rel_path,
        card,
        body=f"{summary} is a synthetic P01-A fidelity fixture.",
        provenance=_prov("summary", "first_name", "last_name", "emails", "aliases", "company", "title", "tags"),
    )
    return {"uid": uid, "rel_path": rel_path, "path": str(path), "email": email, "alias": alias}


def run_p01_fidelity(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    os.environ["PPA_SEED_LINKS_ENABLED"] = "1"
    os.environ["PPA_PLAN_ID"] = "p01"
    init_vault(runtime.vault, owned_root=runtime.root)
    active = _write_person(
        runtime.vault,
        uid=ACTIVE_UID,
        rel_path="People/p01-active.md",
        summary="P01 Active Person",
        email="p01-active@example.test",
        alias="p01-acty",
    )
    neighbor = _write_person(
        runtime.vault,
        uid=NEIGHBOR_UID,
        rel_path="People/p01-neighbor.md",
        summary="P01 Inferred Neighbor",
        email="p01-neighbor@example.test",
        alias="p01-inf",
    )
    quarantine = _write_person(
        runtime.vault,
        uid=QUARANTINE_UID,
        rel_path="People/p01-quarantine.md",
        summary="P01 Quarantine Person",
        email="p01-quarantine@example.test",
        alias="p01-q",
    )
    suppressed = _write_person(
        runtime.vault,
        uid=SUPPRESSED_UID,
        rel_path="People/p01-suppressed.md",
        summary="P01 Suppressed Person",
        email="p01-suppressed@example.test",
        alias="p01-s",
    )
    reset_serving_handle()
    from archive_cli.server import archive_graph, archive_rebuild_indexes, archive_search, archive_search_json

    rebuild_text = archive_rebuild_indexes()
    from archive_cli.index_store import get_archive_index

    index = get_archive_index(runtime.vault)
    with index._connect() as conn:
        ensure_corpus_hygiene_tables(conn, runtime.schema)
        conn.execute(
            f"""
            INSERT INTO {runtime.schema}.card_corpus_state (card_uid, corpus_state)
            VALUES (%s, 'active'), (%s, 'quarantine'), (%s, 'suppressed')
            ON CONFLICT (card_uid) DO UPDATE SET corpus_state = EXCLUDED.corpus_state
            """,
            (ACTIVE_UID, QUARANTINE_UID, SUPPRESSED_UID),
        )
        cand_id = conn.execute(
            f"""
            INSERT INTO {runtime.schema}.link_candidates
                (job_id, module_name, linker_version, source_card_uid, source_rel_path,
                 target_card_uid, target_rel_path, target_kind, proposed_link_type,
                 input_hash, evidence_hash, status)
            VALUES (NULL, 'identityLinker', 1, %s, %s, %s, %s, 'card', 'possible_same_person',
                    'p01a-input', 'p01a-evidence', 'approved')
            RETURNING candidate_id
            """,
            (ACTIVE_UID, active["rel_path"], NEIGHBOR_UID, neighbor["rel_path"]),
        ).fetchone()["candidate_id"]
        conn.execute(
            f"""
            INSERT INTO {runtime.schema}.link_decisions
                (candidate_id, deterministic_score, lexical_score, graph_score, llm_score, risk_penalty,
                 embedding_score, final_confidence, decision, decision_reason,
                 auto_approved_floor, review_floor, discard_floor, policy_version, llm_model, llm_output_json)
            VALUES (%s, 0, 0, 0, 0.55, 0, 0, 0.58, 'auto_promote', 'p01a', 0.8, 0.45, 0, 1, 'mock', '{{}}'::jsonb)
            """,
            (cand_id,),
        )
        conn.execute(
            f"""
            INSERT INTO {runtime.schema}.promotion_queue
                (candidate_id, promotion_target, target_field_name, promotion_status)
            VALUES (%s, 'derived_edge', '', 'applied')
            """,
            (cand_id,),
        )
        conn.commit()

    published = publish_serving_index(DefaultArchiveStore(vault=runtime.vault, index=index))
    reset_serving_handle()
    handle = get_serving_handle(runtime.vault)
    exact = handle.search(ACTIVE_UID, limit=5)
    listed = handle.query(limit=20)
    listed_uids = {row.get("card_uid") for row in listed}
    graph = handle.graph(active["rel_path"], hops=1)
    mcp_search = archive_search(ACTIVE_UID, limit=8)
    mcp_search_json = json.loads(archive_search_json(ACTIVE_UID, limit=8))
    mcp_graph = archive_graph(active["rel_path"], hops=1)

    if not any(row.get("card_uid") == ACTIVE_UID and row.get("match_channel") == "exact" for row in exact):
        raise AssertionError(f"exact UID miss: {exact}")
    if QUARANTINE_UID not in listed_uids:
        raise AssertionError(f"quarantine missing from query: {listed}")
    if SUPPRESSED_UID in listed_uids:
        raise AssertionError(f"suppressed leaked into query: {listed}")
    q_row = next(row for row in listed if row.get("card_uid") == QUARANTINE_UID)
    if q_row.get("corpus_state") != "quarantine":
        raise AssertionError(f"quarantine state lost: {q_row}")
    if abs(float(q_row.get("retrieval_weight") or 0) - QUARANTINE_RETRIEVAL_WEIGHT) > 1e-9:
        raise AssertionError(f"quarantine weight lost: {q_row}")
    neighbors = graph.get(active["rel_path"]) or []
    inferred = next((item for item in neighbors if item.get("edge_type") == "possible_same_person"), None)
    if inferred is None or inferred.get("method") != "inferred":
        raise AssertionError(f"inferred edge lost method: {graph}")
    if abs(float(inferred.get("confidence") or 0) - 0.58) > 1e-9:
        raise AssertionError(f"inferred confidence lost: {inferred}")
    json_uids = [str(row.get("card_uid") or "") for row in mcp_search_json.get("rows") or []]
    if ACTIVE_UID not in json_uids and active["rel_path"] not in mcp_search:
        raise AssertionError(f"MCP search missed active card: text={mcp_search[:500]} json={json_uids}")
    if "possible_same_person" not in mcp_graph and "inferred" not in mcp_graph:
        raise AssertionError(f"MCP graph missed inferred edge: {mcp_graph[:800]}")

    return {
        "id": "p01.fidelity.export_native_mcp",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "rebuild": rebuild_text.splitlines()[0] if rebuild_text else "",
        "published": {"generation": published.get("generation"), "ok": published.get("ok")},
        "exact": exact,
        "listed_uids": sorted(str(uid) for uid in listed_uids if uid),
        "graph": graph,
        "mcp_search": mcp_search[:1500],
        "mcp_search_json_uids": json_uids,
        "mcp_search_confidence": mcp_search_json.get("confidence"),
        "mcp_graph": mcp_graph[:1500],
        "unknown": UNKNOWN,
        "fixtures": {
            "active": active,
            "neighbor": neighbor,
            "quarantine": quarantine,
            "suppressed": suppressed,
        },
    }


register(
    Scenario(
        id="p01.fidelity.export_native_mcp",
        suite="p01",
        product_guarantee="Quarantine weight and inferred-edge method survive warehouse → export → native query → MCP",
        proof_tier="isolated_integration",
        fixture_seed=1,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("p01-fidelity.json",),
        run=run_p01_fidelity,
    )
)

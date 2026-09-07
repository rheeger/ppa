"""P04-B: materialize the frozen corpus and exercise public read paths."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from archive_tests.acceptance.corpus import (
    assert_query_splits,
    assert_relation_closure,
    build_cards,
    build_queries,
    build_relations,
    cardinalities,
    collect_corpus_text,
    forbidden_text_hits,
    materialize_corpus,
    p01_consumption_notes,
    p10_consumption_notes,
)
from archive_tests.acceptance.environment import IsolatedRuntime, inspect_warehouse_card, reset_serving_handle
from archive_tests.acceptance.fixtures import CONTRACT_VERSION
from archive_tests.acceptance.oracle import (
    build_adversarial_modulo_ivf_dataset,
    exact_nearest_neighbors,
    modulo_ivf_knn,
    recall_at_k,
)
from archive_tests.acceptance.registry import Scenario, register

logger = logging.getLogger("ppa.acceptance")


class ScenarioAssertionError(AssertionError):
    """Frozen corpus scenario failed a required assertion."""


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


def _mentions(text: str, uid: str, rel_path: str) -> bool:
    return uid in text or rel_path in text


def _warehouse_uids(dsn: str, schema: str) -> set[str]:
    from psycopg import connect

    with connect(dsn) as conn:
        rows = conn.execute(f"SELECT uid FROM {schema}.cards").fetchall()
    return {str(row[0]) for row in rows}


def _ann_proof() -> dict[str, Any]:
    dataset = build_adversarial_modulo_ivf_dataset()
    exact = exact_nearest_neighbors(dataset.query, dataset.items, k=5)
    simulated = modulo_ivf_knn(dataset.query, dataset.items, k=5)
    exact_ids = [row.item_id for row in exact]
    sim_ids = [row.item_id for row in simulated.neighbors]
    if exact_ids[0] != dataset.true_neighbor_id:
        raise ScenarioAssertionError(f"exact cosine missed authored neighbor {dataset.true_neighbor_id}: {exact_ids}")
    if dataset.true_neighbor_id in sim_ids:
        raise ScenarioAssertionError("modulo-IVF adversary is not a failure; old algorithm found the true neighbor")
    if dataset.unprobed_list in simulated.probed_lists:
        raise ScenarioAssertionError(f"list {dataset.unprobed_list} should be the skipped list")
    if simulated.nlist <= 32:
        raise ScenarioAssertionError(f"nlist must be > 32, got {simulated.nlist}")
    return {
        "n": len(dataset.items),
        "nlist": simulated.nlist,
        "nprobe": simulated.nprobe,
        "exact_top1": exact_ids[0],
        "modulo_top": sim_ids,
        "modulo_recall_at_5": recall_at_k(sim_ids, [dataset.true_neighbor_id], k=5),
        "unprobed_list": dataset.unprobed_list,
        "vector_sha256": dataset.vector_sha256,
        "owner": "P01",
        "status": "expected_regression",
        "detail": "Independent modulo-IVF misses ann-p04b-0065 when nlist=33 and nprobe=32.",
    }


def run_p04_corpus(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    cards = build_cards()
    relations = build_relations(cards)
    queries = build_queries()
    cards_by_uid = {card["uid"]: card for card in cards}
    assert_relation_closure(relations, cards_by_uid)
    assert_query_splits(queries)
    hits = forbidden_text_hits(collect_corpus_text(cards))
    if hits:
        raise ScenarioAssertionError(f"private/seed text in corpus: {hits}")

    written = materialize_corpus(runtime.vault, owned_root=runtime.root)
    reset_serving_handle()
    from archive_cli.server import archive_read, archive_rebuild_indexes, archive_search

    rebuild_text = archive_rebuild_indexes()
    reset_serving_handle()
    warehouse_uids = _warehouse_uids(runtime.dsn, runtime.schema)
    missing = sorted(set(cards_by_uid) - warehouse_uids)
    if missing:
        raise ScenarioAssertionError(f"warehouse missing {len(missing)} cards, first={missing[:5]}")

    probe = [
        cards_by_uid["hfa-person-p04balex0001"],
        cards_by_uid["hfa-flight-p04bout0001"],
        cards_by_uid["hfa-email-message-p04breply01"],
        cards_by_uid["hfa-finance-p04bchg0001"],
    ]
    repo = Path(__file__).resolve().parents[3]
    public_hits = []
    for spec in probe:
        token = next(part for part in spec["body"].split() if part.startswith("P04B-TOKEN-"))
        mcp_search = archive_search(token, limit=8)
        mcp_read = archive_read(spec["uid"])
        cli_search = _cli(["search", token, "--limit", "8"], cwd=repo)
        cli_read = _cli(["read", spec["uid"]], cwd=repo)
        warehouse = inspect_warehouse_card(runtime.dsn, runtime.schema, spec["uid"])
        if warehouse is None or warehouse.get("uid") != spec["uid"]:
            raise ScenarioAssertionError(f"warehouse miss {spec['uid']}")
        if not _mentions(mcp_search, spec["uid"], spec["rel_path"]):
            raise ScenarioAssertionError(f"MCP search miss {spec['uid']} token={token}: {mcp_search[:400]}")
        if spec["uid"] not in mcp_read and spec["rel_path"] not in mcp_read:
            raise ScenarioAssertionError(f"MCP read miss {spec['uid']}")
        if not bool(cli_read.get("found")):
            raise ScenarioAssertionError(f"CLI read miss {spec['uid']}")
        rows = list(cli_search.get("rows") or [])
        if not any(
            str(row.get("card_uid") or row.get("uid") or "") == spec["uid"]
            or str(row.get("rel_path") or "") == spec["rel_path"]
            for row in rows
        ):
            raise ScenarioAssertionError(f"CLI search miss {spec['uid']} token={token}: {cli_search}")
        public_hits.append({"uid": spec["uid"], "rel_path": spec["rel_path"], "token": token})

    hotel = cards_by_uid["hfa-accommodation-p04bhotl001"]
    token_hotel = archive_search("P04B-TOKEN-HOTEL", limit=8)
    if not _mentions(token_hotel, hotel["uid"], hotel["rel_path"]):
        raise ScenarioAssertionError(f"lexical hotel token missed: {token_hotel[:400]}")

    ann = _ann_proof()
    counts = cardinalities(cards)
    return {
        "id": "p04.frozen_corpus",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "contract_version": CONTRACT_VERSION,
        "card_count": written["card_count"],
        "cardinalities": counts,
        "hashes": written["hashes"],
        "rebuild": rebuild_text.splitlines()[0] if rebuild_text else "",
        "warehouse_card_count": len(warehouse_uids),
        "public_hits": public_hits,
        "ann": ann,
        "regressions": [
            {
                "id": "ann.modulo_ivf_selective_probe",
                "owner": "P01",
                "status": "expected_regression",
                "detail": ann["detail"],
            }
        ],
        "p01_consumption": p01_consumption_notes(),
        "p10_consumption": p10_consumption_notes(),
        "schema": runtime.schema,
        "env": {
            "PPA_PATH": os.environ.get("PPA_PATH", ""),
            "PPA_INDEX_SCHEMA": os.environ.get("PPA_INDEX_SCHEMA", ""),
            "PPA_EMBEDDING_PROVIDER": os.environ.get("PPA_EMBEDDING_PROVIDER", ""),
            "PPA_ENGINE": os.environ.get("PPA_ENGINE", ""),
        },
    }


register(
    Scenario(
        id="p04.frozen_corpus",
        suite="p04",
        product_guarantee="Frozen labeled corpus is writable and independently scored; ANN expectations stay independent of the broken IVF path",
        proof_tier="isolated_integration",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("corpus-trace.json",),
        run=run_p04_corpus,
    )
)

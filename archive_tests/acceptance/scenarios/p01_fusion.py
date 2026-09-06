"""P01-B2 acceptance: RRF, diversity, freshness, and honest rerank transport."""

from __future__ import annotations

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import Any

from archive_cli.rank_fusion import ndcg_at_k, recall_at_k
from archive_cli.reranker import HttpReranker, ModelReranker, RerankUnavailable, apply_http_rerank_order
from archive_cli.retrieval_pipeline import HybridFetchInputs, fuse_and_rank_hybrid
from archive_tests.acceptance.registry import Scenario, register
from archive_tests.test_rank_fusion import _lex, _vec
from archive_tests.test_temporal_ranking_profiles import _hit

REPO = Path(__file__).resolve().parents[3]


def _ablations() -> dict[str, Any]:
    relevant = {"rel"}
    lexical = [_lex("rel", 0.2), _lex("distractor", 9.0)]
    vector = [_vec("rel", 0.95), _vec("distractor", 0.01)]
    rrf = fuse_and_rank_hybrid(
        HybridFetchInputs(lexical_rows=lexical, vector_rows=vector, neighbor_trust={}, query_cleaned="q"),
        final_limit=5,
    )
    rrf_ids = [row["card_uid"] for row in rrf]
    raw_ids = ["distractor", "rel"]
    diversity_rows = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[_lex(f"a-{i}", 1.0 - i * 0.01, thread="thread-a") for i in range(6)]
            + [
                item
                for label, score in (("thread-b", 0.4), ("thread-c", 0.3), ("thread-d", 0.2), ("thread-e", 0.1))
                for item in (_lex(f"{label}-0", score, thread=label), _lex(f"{label}-1", score - 0.01, thread=label))
            ],
            vector_rows=[],
            neighbor_trust={},
            query_cleaned="q",
        ),
        final_limit=10,
    )
    parents = [row.get("parent_thread") for row in diversity_rows[:10]]
    fresh = fuse_and_rank_hybrid(
        HybridFetchInputs(
            lexical_rows=[
                _hit("old", activity="2024-01-01", score=0.81),
                _hit("fresh", activity="2026-09-01", score=0.80),
            ],
            vector_rows=[],
            neighbor_trust={},
            query_cleaned="latest status",
        ),
        final_limit=5,
    )
    return {
        "rrf_order": rrf_ids,
        "raw_blend_order": raw_ids,
        "rrf_ndcg_at_2": ndcg_at_k(rrf_ids, relevant, k=2),
        "raw_ndcg_at_2": ndcg_at_k(raw_ids, relevant, k=2),
        "rrf_recall_at_1": recall_at_k(rrf_ids, relevant, k=1),
        "diversity_parents_top10": parents,
        "current_ops_winner": fresh[0]["card_uid"],
        "history_retained": sorted({row["card_uid"] for row in fresh}),
        "rare_token_weight": 0.0,
        "p04_labeled_corpus": "unavailable",
        "model_quality_proof": "unavailable",
    }


def run_p01_fusion(_runtime: object) -> dict[str, Any]:
    started = time.monotonic()
    ablations = _ablations()
    if ablations["rrf_recall_at_1"] != 1.0:
        raise AssertionError(f"RRF missed the relevant neighbor: {ablations}")
    if "thread-b" not in ablations["diversity_parents_top10"]:
        raise AssertionError(f"diversity failed to surface the second thread: {ablations}")
    if ablations["diversity_parents_top10"].count("thread-a") > 2:
        raise AssertionError(f"thread-a still dominates the top ten: {ablations}")
    if ablations["current_ops_winner"] != "fresh":
        raise AssertionError(f"current_ops did not promote the fresh hit: {ablations}")
    if set(ablations["history_retained"]) != {"old", "fresh"}:
        raise AssertionError("current_ops deleted historical evidence")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            results = [{"id": item["id"], "score": 0.9 if item["id"] == "b" else 0.1} for item in payload["candidates"]]
            self.wfile.write(
                json.dumps(
                    {
                        "schema_version": payload["schema_version"],
                        "model_revision": "test-echo-1",
                        "results": results,
                    }
                ).encode("utf-8")
            )

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address[:2]
        reranker = HttpReranker(endpoint=f"http://{host}:{port}/rerank")
        scores = reranker.rerank("q", [{"card_uid": "a", "summary": "a"}, {"card_uid": "b", "summary": "b"}])
        ordered = apply_http_rerank_order(
            [{"card_uid": "a", "score": 1.0, "exact_match": False}, {"card_uid": "b", "score": 0.2, "exact_match": False}],
            {item.card_uid: item for item in scores},
        )
        if [row["card_uid"] for row in ordered] != ["b", "a"]:
            raise AssertionError("HTTP rerank did not apply returned scores")
        try:
            ModelReranker(model="missing").rerank("q", [{"card_uid": "a", "summary": "endaoment"}])
            raise AssertionError("unconfigured model rerank must be unavailable")
        except RerankUnavailable:
            pass
    finally:
        server.shutdown()

    schema = (REPO / "archive_crate/src/serving_index/schema.rs").read_text(encoding="utf-8")
    if "pub const SERVING_INDEX_FORMAT_VERSION: u32 = 2;" not in schema:
        raise AssertionError("serving format v2 was reopened")
    return {
        "id": "p01.fusion.rrf_rerank_freshness",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "ablations": ablations,
        "serving_index_format_version": 2,
        "http_rerank_called": True,
        "model_quality_proof": "unavailable",
    }


register(
    Scenario(
        id="p01.fusion.rrf_rerank_freshness",
        suite="p01",
        product_guarantee="RRF fusion, thread diversity, query-aware freshness, and honest HTTP rerank",
        proof_tier="isolated_unit",
        fixture_seed=20260906,
        fixture_hash="",
        prerequisites=("rust_engine",),
        expected_artifacts=("p01-fusion.json",),
        run=run_p01_fusion,
    )
)

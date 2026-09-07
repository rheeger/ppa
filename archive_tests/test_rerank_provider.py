"""P01-B2: HTTP rerank transport, never heuristic-as-model."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

from archive_cli.reranker import (
    RERANK_SCHEMA_VERSION,
    HeuristicReranker,
    HttpReranker,
    ModelReranker,
    RerankError,
    RerankUnavailable,
    apply_http_rerank_order,
    reranker_for_config,
)


class _DeterministicHandler(BaseHTTPRequestHandler):
    scores: dict[str, float] = {}
    last_payload: dict = {}
    status: int = 200
    fail_schema: bool = False

    def log_message(self, *_args):
        return

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode("utf-8"))
        type(self).last_payload = payload
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if self.status >= 400:
            self.wfile.write(b"{}")
            return
        schema = "wrong" if self.fail_schema else payload.get("schema_version")
        results = [
            {"id": item["id"], "score": self.scores.get(item["id"], 0.0)} for item in payload.get("candidates", [])
        ]
        body = {
            "schema_version": schema,
            "model_revision": "test-echo-1",
            "results": results,
        }
        self.wfile.write(json.dumps(body).encode("utf-8"))


def _serve(handler_cls) -> tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    return server, f"http://{host}:{port}/rerank"


def test_http_reranker_posts_versioned_request_and_uses_scores():
    class Handler(_DeterministicHandler):
        scores = {"a": 0.1, "b": 0.9}

    server, url = _serve(Handler)
    try:
        reranker = HttpReranker(endpoint=url, model="local-test")
        rows = [
            {"card_uid": "a", "summary": "alpha", "context_text": ""},
            {"card_uid": "b", "summary": "beta", "context_text": "context"},
        ]
        out = reranker.rerank("port", rows)
        by_uid = {item.card_uid: item.score for item in out}
        assert by_uid["b"] > by_uid["a"]
        assert Handler.last_payload["schema_version"] == RERANK_SCHEMA_VERSION
        assert Handler.last_payload["query"] == "port"
        assert [item["id"] for item in Handler.last_payload["candidates"]] == ["a", "b"]
        assert out[0].model_revision == "test-echo-1" or out[1].model_revision == "test-echo-1"
        ordered = apply_http_rerank_order(
            [
                {"card_uid": "a", "score": 1.0, "exact_match": False},
                {"card_uid": "b", "score": 0.5, "exact_match": False},
            ],
            {item.card_uid: item for item in out},
        )
        assert [row["card_uid"] for row in ordered] == ["b", "a"]
    finally:
        server.shutdown()


def test_http_failure_is_transport_error_not_heuristic():
    class Handler(_DeterministicHandler):
        status = 503

    server, url = _serve(Handler)
    try:
        reranker = HttpReranker(endpoint=url)
        with pytest.raises(RerankError, match="http_rerank_status_503"):
            reranker.rerank("q", [{"card_uid": "a", "summary": "a"}])
    finally:
        server.shutdown()


def test_model_reranker_without_endpoint_is_unavailable():
    reranker = ModelReranker(model="cross-encoder")
    with pytest.raises(RerankUnavailable, match="model_rerank_unconfigured"):
        reranker.rerank("q", [{"card_uid": "a", "summary": "endaoment donor"}])
    heuristic = HeuristicReranker().rerank("endaoment donor", [{"card_uid": "a", "summary": "endaoment donor"}])
    assert heuristic[0].score > 0


def test_reranker_for_config_model_without_endpoint_does_not_look_like_heuristic():
    cfg = {"reranker": {"enabled": True, "provider": "model", "model": "x"}}
    reranker = reranker_for_config(cfg)
    assert isinstance(reranker, ModelReranker)
    with pytest.raises(RerankUnavailable):
        reranker.rerank("endaoment", [{"card_uid": "a", "summary": "endaoment donor"}])


def test_http_schema_mismatch_fails_closed_to_error():
    class Handler(_DeterministicHandler):
        fail_schema = True
        scores = {"a": 1.0}

    server, url = _serve(Handler)
    try:
        with pytest.raises(RerankError, match="schema_mismatch"):
            HttpReranker(endpoint=url).rerank("q", [{"card_uid": "a", "summary": "a"}])
    finally:
        server.shutdown()


def test_http_rerank_protects_exact_match():
    rows = [
        {"card_uid": "exact", "score": 0.2, "exact_match": True},
        {"card_uid": "other", "score": 0.9, "exact_match": False},
    ]
    from archive_cli.reranker import RerankResult

    out = apply_http_rerank_order(
        rows,
        {
            "exact": RerankResult(card_uid="exact", score=0.0, model_revision="test-echo-1"),
            "other": RerankResult(card_uid="other", score=1.0, model_revision="test-echo-1"),
        },
    )
    assert out[0]["card_uid"] == "exact"
    assert out[0]["score"] == 0.2

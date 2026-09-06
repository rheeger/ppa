"""P05-C: provider egress hits only authorized destinations."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import request

import pytest

from archive_cli.embedding_provider import HashEmbeddingProvider, OpenAIEmbeddingProvider
from archive_engine.adapters.providers import EmbeddingProviderAdapter
from archive_engine.contracts import AccessContext
from archive_engine.egress import (
    EgressDeniedError,
    authorize_destination,
    authorize_request,
    capture_egress,
    destination_from_url,
    guarded_urlopen,
)
from archive_engine.redaction import redact_text

SYN_GMAIL = "synthetic-p05c-gmail-ok"
SYN_MEDICAL = "synthetic-p05c-medical-denied"
SYN_KEY = "sk-test-synthetic-p05c-not-real"
REMOTE_REDIRECT = "http://example.invalid/remote-process"


@pytest.fixture(autouse=True)
def _clear_egress_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_EGRESS_MODE", raising=False)
    monkeypatch.delenv("PPA_ACCESS_EGRESS_POLICY_REVISION", raising=False)
    monkeypatch.delenv("OLLAMA_HOST", raising=False)


def _access(**kwargs: Any) -> AccessContext:
    payload = {
        "archive_id": "p05c",
        "principal": "local-operator",
        "profile": "trusted-local",
        "egress_policy_revision": "p05c-unrestricted",
    }
    payload.update(kwargs)
    return AccessContext(**payload)


class _MockHandler(BaseHTTPRequestHandler):
    def log_message(self, *_args: object) -> None:
        return

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self.server.calls.append(  # type: ignore[attr-defined]
            {
                "method": self.command,
                "path": self.path,
                "host": self.headers.get("Host", ""),
                "authorization": self.headers.get("Authorization", ""),
                "body": body.decode("utf-8", "replace"),
            }
        )
        if self.path.startswith("/redirect-remote"):
            self.send_response(302)
            self.send_header("Location", REMOTE_REDIRECT)
            self.end_headers()
            return
        if "embeddings" in self.path:
            payload = json.loads(body.decode("utf-8") or "{}")
            texts = payload.get("input") or [1]
            dim = int(payload.get("dimensions") or 8)
            raw = json.dumps({"data": [{"embedding": [0.01] * dim} for _ in texts]}).encode()
        else:
            raw = json.dumps(
                {
                    "ok": True,
                    "models": [],
                    "response": "ok",
                    "message": {"content": "ok"},
                    "choices": [{"message": {"content": "ok"}}],
                }
            ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class MockHTTP:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []
        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), _MockHandler)
        self._httpd.calls = self.calls  # type: ignore[attr-defined]
        self.port = int(self._httpd.server_address[1])
        self.base = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def __enter__(self) -> MockHTTP:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._httpd.shutdown()
        self._thread.join(timeout=2)


def test_unknown_destination_never_hits_transport() -> None:
    with MockHTTP() as http, capture_egress() as events:
        with pytest.raises(EgressDeniedError, match="unknown"):
            authorize_request(destination="mystery-cdn", url=f"{http.base}/x")
        assert http.calls == []
    assert all(not ev.allowed for ev in events)


def test_hash_provider_is_local_and_silent() -> None:
    adapter = EmbeddingProviderAdapter(lambda **_: HashEmbeddingProvider(dimension=8), access=_access())
    with MockHTTP() as http, capture_egress() as events:
        vectors = adapter.embed_texts(["ping"], sources=("gmail",))
        assert len(vectors) == 1
        assert http.calls == []
    assert any(ev.destination == "hash" and ev.allowed for ev in events)


def test_local_only_blocks_openai_before_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", SYN_KEY)
    access = _access(egress_policy_revision="p05c-local-only", principal="alice", profile="read-only")
    with MockHTTP() as http:
        adapter = EmbeddingProviderAdapter(
            lambda **_: OpenAIEmbeddingProvider(dimension=8, base_url=http.base, model="test"),
            access=access,
        )
        with capture_egress() as events:
            with pytest.raises(EgressDeniedError, match="local-only"):
                adapter.embed_texts([SYN_GMAIL], sources=("gmail",))
        assert http.calls == []
        assert not any(ev.phase == "transport" for ev in events)


def test_restricted_unlabeled_remote_denied() -> None:
    access = _access(
        principal="alice",
        profile="read-only",
        allowed_sources=("gmail",),
        egress_policy_revision="p05c-restricted",
    )
    with pytest.raises(EgressDeniedError, match="explicit payload sources"):
        authorize_destination("openai", access=access, url="https://api.openai.com/v1/embeddings")


def test_restricted_source_blocked_from_remote(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", SYN_KEY)
    access = _access(
        principal="alice",
        profile="read-only",
        allowed_sources=("gmail",),
        egress_policy_revision="p05c-restricted",
    )
    with MockHTTP() as http:
        adapter = EmbeddingProviderAdapter(
            lambda **_: OpenAIEmbeddingProvider(dimension=8, base_url=http.base, model="test"),
            access=access,
        )
        with pytest.raises(EgressDeniedError, match="medical"):
            adapter.embed_texts([SYN_MEDICAL], sources=("medical",))
        assert http.calls == []
        assert SYN_MEDICAL not in json.dumps(http.calls)


def test_authorized_openai_reaches_mock_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", SYN_KEY)
    access = _access()
    with MockHTTP() as http, capture_egress() as events:
        adapter = EmbeddingProviderAdapter(
            lambda **_: OpenAIEmbeddingProvider(dimension=8, base_url=http.base, model="test"),
            access=access,
        )
        vectors = adapter.embed_texts([SYN_GMAIL], sources=("gmail",))
        assert len(vectors) == 1
        assert len(http.calls) == 1
        assert SYN_GMAIL in http.calls[0]["body"]
        assert SYN_MEDICAL not in http.calls[0]["body"]
        assert any(ev.phase == "transport" and ev.allowed for ev in events)


def test_local_only_cannot_follow_remote_redirect() -> None:
    access = _access(egress_policy_revision="p05c-local-only")
    with MockHTTP() as http, capture_egress() as events:
        req = request.Request(f"{http.base}/redirect-remote", method="GET")
        with pytest.raises(EgressDeniedError, match="redirect"):
            guarded_urlopen(req, timeout=2, destination="ollama", access=access)
        transport_urls = [ev.url for ev in events if ev.phase == "transport"]
        assert all("example.invalid" not in url for url in transport_urls)
        assert all(call["path"].startswith("/redirect-remote") for call in http.calls)
        assert not any("example.invalid" in json.dumps(call) for call in http.calls)


def test_ollama_cannot_use_remote_host() -> None:
    with pytest.raises(EgressDeniedError, match="remote URL"):
        authorize_request(destination="ollama", url="https://api.openai.com/v1/chat", access=_access())


def test_local_only_blocks_firecrawl() -> None:
    access = _access(egress_policy_revision="p05c-local-only")
    with pytest.raises(EgressDeniedError, match="firecrawl"):
        authorize_destination("firecrawl", access=access, url="https://api.firecrawl.dev/")


def test_openclaw_has_no_transport() -> None:
    with pytest.raises(EgressDeniedError, match="no authorized transport"):
        authorize_destination("openclaw", access=_access())


def test_no_fallback_after_authorized_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    class Boom:
        name = "openai"

        def embed_texts(self, texts: list[str]) -> list[list[float]]:
            seen.append("openai")
            raise RuntimeError("synthetic provider failure")

    class Other:
        name = "ollama"

        def embed_texts(self, texts: list[str]) -> list[list[float]]:
            seen.append("ollama")
            return [[0.0]]

    adapter = EmbeddingProviderAdapter(lambda **_: Boom(), access=_access())
    with pytest.raises(RuntimeError, match="synthetic provider failure"):
        adapter.embed_texts(["x"], sources=("gmail",))
    assert seen == ["openai"]
    other = EmbeddingProviderAdapter(lambda **_: Other(), access=_access())
    # A later adapter is a new call, not a fallback from the failed one.
    assert other._factory().name == "ollama"


def test_destination_from_url_classifies_known_hosts() -> None:
    assert destination_from_url("https://api.openai.com/v1/embeddings") == "openai"
    assert destination_from_url("https://generativelanguage.googleapis.com/v1beta/models") == "gemini"
    assert destination_from_url("http://127.0.0.1:11434/api/tags") == "ollama"
    assert destination_from_url("https://evil.example/cdn") == "unknown"


def test_decide_same_person_does_not_fallback_after_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_vault import llm_provider as llm

    class FailThen:
        name = "openai"

        def __init__(self, model: str = "") -> None:
            self.model = model

        def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
            return None

    class WouldFallback:
        name = "ollama"

        def __init__(self, model: str = "", base_url: str = "") -> None:
            self.model = model

        def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
            raise AssertionError("fallback must not run after authorized failure")

    monkeypatch.setitem(llm.PROVIDER_REGISTRY, "openai", FailThen)
    monkeypatch.setitem(llm.PROVIDER_REGISTRY, "ollama", WouldFallback)
    (tmp_path / "_meta").mkdir()
    (tmp_path / "_meta" / "llm-config.json").write_text(
        json.dumps(
            {
                "primary": {"provider": "openai", "model": "m"},
                "fallback": {"provider": "ollama", "model": "m"},
            }
        ),
        encoding="utf-8",
    )
    assert llm.decide_same_person(tmp_path, {"n": "a"}, {"n": "b"}) == "UNSURE"


def build_outbound_matrix(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Acceptance + evidence helper. Synthetic data only."""

    monkeypatch.setenv("OPENAI_API_KEY", SYN_KEY)
    rows: list[dict[str, Any]] = []
    with MockHTTP() as http:
        cases = [
            (
                "hash-local",
                _access(),
                lambda: EmbeddingProviderAdapter(lambda **_: HashEmbeddingProvider(dimension=8), access=_access()).embed_texts(
                    [SYN_GMAIL], sources=("gmail",)
                ),
                True,
                False,
            ),
            (
                "openai-unrestricted",
                _access(),
                lambda: EmbeddingProviderAdapter(
                    lambda **_: OpenAIEmbeddingProvider(dimension=8, base_url=http.base, model="test"),
                    access=_access(),
                ).embed_texts([SYN_GMAIL], sources=("gmail",)),
                True,
                True,
            ),
            (
                "openai-local-only",
                _access(egress_policy_revision="p05c-local-only"),
                lambda: EmbeddingProviderAdapter(
                    lambda **_: OpenAIEmbeddingProvider(dimension=8, base_url=http.base, model="test"),
                    access=_access(egress_policy_revision="p05c-local-only"),
                ).embed_texts([SYN_GMAIL], sources=("gmail",)),
                False,
                False,
            ),
            (
                "openai-medical-restricted",
                _access(principal="alice", profile="read-only", allowed_sources=("gmail",), egress_policy_revision="p05c-restricted"),
                lambda: EmbeddingProviderAdapter(
                    lambda **_: OpenAIEmbeddingProvider(dimension=8, base_url=http.base, model="test"),
                    access=_access(
                        principal="alice",
                        profile="read-only",
                        allowed_sources=("gmail",),
                        egress_policy_revision="p05c-restricted",
                    ),
                ).embed_texts([SYN_MEDICAL], sources=("medical",)),
                False,
                False,
            ),
        ]
        before = 0
        for name, _access_ctx, fn, expect_ok, expect_http in cases:
            before = len(http.calls)
            allowed = True
            error = ""
            try:
                fn()
            except EgressDeniedError as exc:
                allowed = False
                error = type(exc).__name__
            hit = len(http.calls) > before
            rows.append(
                {
                    "case": name,
                    "allowed": allowed,
                    "http_hit": hit,
                    "expect_ok": expect_ok,
                    "expect_http": expect_http,
                    "error": error,
                }
            )
            if expect_ok != allowed or expect_http != hit:
                raise AssertionError(f"matrix row failed: {rows[-1]}")
        bodies = [call["body"] for call in http.calls]
        blob = redact_text(json.dumps({"rows": rows, "bodies": bodies, "calls": http.calls}))
        if SYN_MEDICAL in blob or SYN_KEY in blob:
            raise AssertionError("synthetic secret leaked into redacted matrix")
        if any(SYN_MEDICAL in call["body"] for call in http.calls):
            raise AssertionError("denied medical sentinel reached transport")
    return {"rows": rows, "ok": True, "mock_base_used": True}


def test_outbound_matrix(monkeypatch: pytest.MonkeyPatch) -> None:
    matrix = build_outbound_matrix(monkeypatch)
    assert matrix["ok"]
    assert len(matrix["rows"]) == 4

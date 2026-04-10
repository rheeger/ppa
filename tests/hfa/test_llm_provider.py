import json
from unittest.mock import patch

from hfa.llm_provider import (DEFAULT_LLM_CONFIG, PROVIDER_REGISTRY,
                              OllamaProvider, decide_same_person, get_provider,
                              load_llm_config)


class FakeProvider:
    name = "mock"

    def __init__(self, model: str = "mock-v1"):
        self.model = model

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        return "YES"


def test_load_llm_config_returns_defaults(tmp_vault):
    assert load_llm_config(tmp_vault)["primary"] == DEFAULT_LLM_CONFIG["primary"]


def test_decide_same_person_uses_cache(tmp_vault, monkeypatch):
    monkeypatch.setitem(PROVIDER_REGISTRY, "mock", FakeProvider)
    (tmp_vault / "_meta" / "llm-config.json").write_text(
        json.dumps({"primary": {"provider": "mock", "model": "mock-v1"}}),
        encoding="utf-8",
    )

    first = decide_same_person(tmp_vault, {"summary": "Jane"}, {"summary": "Jane"})
    second = decide_same_person(tmp_vault, {"summary": "Jane"}, {"summary": "Jane"})

    assert first == "YES"
    assert second == "YES"
    cache = json.loads((tmp_vault / "_meta" / "llm-cache.json").read_text(encoding="utf-8"))
    assert len(cache) == 1


class _FakeHTTP:
    """Minimal context manager for ``urlopen``."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def test_ollama_chat_json_parses_response() -> None:
    api_body = {
        "message": {"role": "assistant", "content": '{"status": "ok"}'},
        "prompt_eval_count": 3,
        "eval_count": 5,
    }
    raw = json.dumps(api_body).encode("utf-8")
    with patch("hfa.llm_provider.request.urlopen", return_value=_FakeHTTP(raw)):
        p = OllamaProvider(model="gemma4:31b", base_url="http://localhost:11434")
        r = p.chat_json([{"role": "user", "content": "x"}])
    assert r.parsed_json == {"status": "ok"}
    assert r.prompt_tokens == 3
    assert r.completion_tokens == 5


def test_ollama_health_check_requires_model_name() -> None:
    tags = {"models": [{"name": "gemma4:31b", "size": 1}]}
    raw = json.dumps(tags).encode("utf-8")
    with patch("hfa.llm_provider.request.urlopen", return_value=_FakeHTTP(raw)):
        assert OllamaProvider(model="gemma4:31b").health_check() is True
    with patch("hfa.llm_provider.request.urlopen", return_value=_FakeHTTP(raw)):
        assert OllamaProvider(model="missing:latest").health_check() is False


def test_get_provider_ollama_respects_base_url(tmp_vault) -> None:
    (tmp_vault / "_meta" / "llm-config.json").write_text(
        json.dumps(
            {
                "primary": {
                    "provider": "ollama",
                    "model": "gemma4:31b",
                    "base_url": "http://127.0.0.1:11434",
                }
            }
        ),
        encoding="utf-8",
    )
    prov = get_provider(tmp_vault)
    assert isinstance(prov, OllamaProvider)
    assert prov.model == "gemma4:31b"
    assert "127.0.0.1" in prov.base_url


def test_ollama_chat_json_retries_once_on_invalid_json() -> None:
    bad = json.dumps({"message": {"content": "NOT_JSON"}}).encode()
    good = json.dumps({"message": {"content": '{"ok": true}'}}).encode()
    calls = {"n": 0}

    def side_effect(*_a: object, **_k: object) -> _FakeHTTP:
        calls["n"] += 1
        if calls["n"] == 1:
            return _FakeHTTP(bad)
        return _FakeHTTP(good)

    with patch("hfa.llm_provider.request.urlopen", side_effect=side_effect):
        p = OllamaProvider(model="m")
        r = p.chat_json([{"role": "user", "content": "q"}])
    assert r.parsed_json == {"ok": True}
    assert calls["n"] == 2


def test_ollama_complete_plain_text() -> None:
    api_body = {"message": {"content": "YES"}}
    raw = json.dumps(api_body).encode("utf-8")
    with patch("hfa.llm_provider.request.urlopen", return_value=_FakeHTTP(raw)):
        p = OllamaProvider(model="m")
        assert p.complete("test?") == "YES"

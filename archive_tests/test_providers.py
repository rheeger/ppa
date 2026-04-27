"""Tests for the ModelProvider interface and provider registry."""

from __future__ import annotations

import json

import pytest

from archive_cli.providers import OllamaModelProvider, OpenAIModelProvider, resolve_provider
from archive_cli.providers.openclaw import OpenClawModelProvider


def test_resolve_provider_openai(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_ENRICHMENT_MODEL", "openai:gpt-4o-mini")
    p = resolve_provider(refresh=True)
    assert p is not None
    assert p.name == "openai"
    assert p.model == "gpt-4o-mini"


def test_resolve_provider_ollama(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_ENRICHMENT_MODEL", "ollama:llama3.2:3b")
    p = resolve_provider(refresh=True)
    assert p is not None
    assert p.name == "ollama"
    assert p.model == "llama3.2:3b"


def test_resolve_provider_openclaw(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_ENRICHMENT_MODEL", "openclaw")
    p = resolve_provider(refresh=True)
    assert p is not None
    assert isinstance(p, OpenClawModelProvider)
    assert p.is_available() is False


def test_resolve_provider_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PPA_ENRICHMENT_MODEL", raising=False)
    assert resolve_provider(refresh=True) is None


def test_resolve_provider_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PPA_ENRICHMENT_MODEL", "bogus:model")
    with pytest.raises(ValueError, match="Unknown provider"):
        resolve_provider(refresh=True)


def test_provider_cost_estimates() -> None:
    for cls, model in (
        (OpenAIModelProvider, "gpt-4o-mini"),
        (OpenAIModelProvider, "gpt-4o"),
        (OpenAIModelProvider, "unknown-model"),
        (OllamaModelProvider, "llama3.2:3b"),
        (OpenClawModelProvider, ""),
    ):
        inst = cls(model=model) if model else cls()
        assert inst.estimated_cost_per_1k_tokens() >= 0.0


def test_openai_is_available_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    assert OpenAIModelProvider().is_available() is True


def test_openai_is_available_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    assert OpenAIModelProvider().is_available() is False


def test_openai_generate_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    def fake_post(url, headers, payload):
        assert "chat/completions" in url
        return {
            "choices": [{"message": {"content": "hello from model"}}],
        }

    monkeypatch.setattr("archive_cli.providers.openai._post_json", fake_post)
    out = OpenAIModelProvider(model="gpt-4o-mini").generate("ping", max_tokens=32)
    assert out == "hello from model"


def test_openai_generate_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr("archive_cli.providers.openai._post_json", lambda *a, **k: None)
    with pytest.raises(RuntimeError):
        OpenAIModelProvider().generate("ping")


def test_ollama_is_available_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OLLAMA_HOST", "http://localhost:11434")

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, _n: int = -1):
            return b'{"models":[]}'

    monkeypatch.setattr(
        "archive_cli.providers.ollama.request.urlopen",
        lambda *a, **k: Resp(),
    )
    assert OllamaModelProvider().is_available() is True


def test_ollama_is_available_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OLLAMA_HOST", "http://127.0.0.1:1")

    def boom(*a, **k):
        raise OSError("nope")

    monkeypatch.setattr("archive_cli.providers.ollama.request.urlopen", boom)
    assert OllamaModelProvider().is_available() is False


def test_ollama_generate_success(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"response": "generated text"}

    class Resp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, _n: int = -1):
            return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(
        "archive_cli.providers.ollama.request.urlopen",
        lambda *a, **k: Resp(),
    )
    out = OllamaModelProvider(model="m").generate("hi")
    assert out == "generated text"


def test_openclaw_is_available() -> None:
    assert OpenClawModelProvider().is_available() is False

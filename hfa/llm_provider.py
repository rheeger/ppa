"""LLM provider protocol, config, and simple decision helpers."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib import error, request

from hfa.provenance import compute_input_hash

GROUNDING_INSTRUCTION = (
    "Use ONLY the provided data. Do not add information from your training data. "
    "If you cannot determine something from the provided data, answer UNSURE."
)

DEFAULT_LLM_CONFIG = {
    "primary": {"provider": "gemini", "model": "gemini-2.0-flash-lite"},
    "fallback": {"provider": "openai", "model": "gpt-4o-mini"},
    "max_tokens_tiebreak": 4,
    "max_tokens_enrichment": 256,
}


@runtime_checkable
class LLMProvider(Protocol):
    name: str
    model: str

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        """Return response text or None on failure."""


def _post_json(url: str, headers: dict[str, str], payload: dict[str, Any]) -> dict[str, Any] | None:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except (error.HTTPError, error.URLError, TimeoutError, json.JSONDecodeError):
        return None


class GeminiProvider:
    """Gemini REST provider."""

    name = "gemini"

    def __init__(self, model: str = "gemini-2.0-flash-lite"):
        self.model = model

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            return None
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
            f"?key={api_key}"
        )
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": max_tokens},
        }
        response = _post_json(url, {"Content-Type": "application/json"}, payload)
        if not response:
            return None
        candidates = response.get("candidates") or []
        try:
            parts = candidates[0]["content"]["parts"]
        except (IndexError, KeyError, TypeError):
            return None
        texts = [part.get("text", "") for part in parts if isinstance(part, dict)]
        answer = "".join(texts).strip()
        return answer or None


class OpenAIProvider:
    """OpenAI REST provider."""

    name = "openai"

    def __init__(self, model: str = "gpt-4o-mini"):
        self.model = model

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            return None
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
        }
        response = _post_json(
            "https://api.openai.com/v1/chat/completions",
            {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
            payload,
        )
        if not response:
            return None
        try:
            answer = response["choices"][0]["message"]["content"]
        except (IndexError, KeyError, TypeError):
            return None
        return str(answer).strip() or None


PROVIDER_REGISTRY: dict[str, type[LLMProvider]] = {
    "gemini": GeminiProvider,
    "openai": OpenAIProvider,
}


def _llm_config_path(vault_path: str | Path) -> Path:
    return Path(vault_path) / "_meta" / "llm-config.json"


def _llm_cache_path(vault_path: str | Path) -> Path:
    return Path(vault_path) / "_meta" / "llm-cache.json"


def load_llm_config(vault_path: str | Path) -> dict[str, Any]:
    """Load LLM config, merging with defaults."""

    path = _llm_config_path(vault_path)
    if not path.exists():
        return dict(DEFAULT_LLM_CONFIG)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(DEFAULT_LLM_CONFIG)
    if not isinstance(payload, dict):
        return dict(DEFAULT_LLM_CONFIG)
    merged = dict(DEFAULT_LLM_CONFIG)
    for key, value in payload.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def _build_provider(spec: dict[str, Any]) -> LLMProvider:
    provider_name = str(spec.get("provider", ""))
    model = str(spec.get("model", ""))
    provider_cls = PROVIDER_REGISTRY[provider_name]
    return provider_cls(model=model)


def get_provider(vault_path: str | Path) -> LLMProvider:
    """Return the configured primary provider."""

    config = load_llm_config(vault_path)
    return _build_provider(config["primary"])


def get_provider_chain(vault_path: str | Path) -> list[LLMProvider]:
    """Return the provider fallback chain."""

    config = load_llm_config(vault_path)
    chain = [_build_provider(config["primary"])]
    fallback = config.get("fallback")
    if isinstance(fallback, dict) and fallback.get("provider"):
        chain.append(_build_provider(fallback))
    return chain


def _load_llm_cache(vault_path: str | Path) -> dict[str, str]:
    path = _llm_cache_path(vault_path)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_llm_cache(vault_path: str | Path, cache: dict[str, str]) -> None:
    path = _llm_cache_path(vault_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix="llm-cache-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(cache, handle, indent=2, sort_keys=True)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def decide_same_person(vault_path: str | Path, person_a: dict[str, Any], person_b: dict[str, Any]) -> str:
    """Ask the configured provider chain for a YES / NO / UNSURE decision."""

    cache = _load_llm_cache(vault_path)
    cache_key = compute_input_hash({"person_a": person_a, "person_b": person_b})
    if cache_key in cache:
        return cache[cache_key]

    prompt = (
        f"{GROUNDING_INSTRUCTION}\n\n"
        "Decide if these records refer to the same person. Answer only YES, NO, or UNSURE.\n\n"
        f"A: {json.dumps(person_a, sort_keys=True)}\n"
        f"B: {json.dumps(person_b, sort_keys=True)}"
    )
    max_tokens = int(load_llm_config(vault_path).get("max_tokens_tiebreak", 4))
    for provider in get_provider_chain(vault_path):
        response = provider.complete(prompt, max_tokens=max_tokens)
        if not response:
            continue
        normalized = response.strip().upper()
        if normalized in {"YES", "NO", "UNSURE"}:
            cache[cache_key] = normalized
            _save_llm_cache(vault_path, cache)
            return normalized

    cache[cache_key] = "UNSURE"
    _save_llm_cache(vault_path, cache)
    return "UNSURE"

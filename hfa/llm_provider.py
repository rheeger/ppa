"""LLM provider protocol, config, and simple decision helpers."""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
from dataclasses import dataclass
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


_DEFAULT_HTTP_TIMEOUT: float = 30.0
_LLM_HTTP_TIMEOUT: float = 600.0

_llm_log = logging.getLogger("ppa.llm_provider")


_MAX_RETRIES = 5
_RETRY_BASE_WAIT = 2.0
_MIN_REQUEST_INTERVAL = 0.05  # 50ms between requests = max ~20 RPS per thread


def _post_json(
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    *,
    timeout: float = _DEFAULT_HTTP_TIMEOUT,
) -> dict[str, Any] | None:
    data = json.dumps(payload).encode("utf-8")
    import random as _random
    for attempt in range(_MAX_RETRIES + 1):
        req = request.Request(url, data=data, headers=headers, method="POST")
        try:
            with request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            if exc.code in (429, 503) and attempt < _MAX_RETRIES:
                wait = _RETRY_BASE_WAIT * (2 ** attempt) + _random.uniform(0.5, 2.0)
                _llm_log.info("%d retry %d/%d in %.1fs", exc.code, attempt + 1, _MAX_RETRIES, wait)
                time.sleep(wait)
                continue
            _llm_log.warning("_post_json failed url=%s: %s", url, exc)
            return None
        except (error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            _llm_log.warning("_post_json failed url=%s: %s", url, exc)
            return None
    return None


class GeminiProvider:
    """Gemini REST provider — supports both ``complete()`` and ``chat_json()``."""

    name = "gemini"

    def __init__(self, model: str = "gemini-2.0-flash-lite"):
        self.model = model

    def _api_key(self) -> str | None:
        return os.environ.get("GEMINI_API_KEY")

    def _generate_url(self, model: str | None = None) -> str:
        m = model or self.model
        key = self._api_key()
        return f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={key}"

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        if not self._api_key():
            return None
        url = self._generate_url()
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"maxOutputTokens": max_tokens},
        }
        response = _post_json(url, {"Content-Type": "application/json"}, payload)
        if not response:
            return None
        return self._extract_text(response)

    def chat_json(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.0,
        seed: int = 42,
        max_tokens: int = 4096,
        json_schema: dict[str, Any] | None = None,
    ) -> "LLMResponse":
        """Gemini REST chat with JSON response parsing."""

        _ = seed  # Gemini doesn't support seed
        use_model = model or self.model
        api_key = self._api_key()
        if not api_key:
            return LLMResponse(content="", parsed_json=None, model=use_model,
                               prompt_tokens=0, completion_tokens=0, latency_ms=0.0)

        contents: list[dict[str, Any]] = []
        system_text = ""
        for msg in messages:
            role = msg.get("role", "user")
            text = msg.get("content", "")
            if role == "system":
                system_text = text
                continue
            gemini_role = "model" if role == "assistant" else "user"
            contents.append({"role": gemini_role, "parts": [{"text": text}]})

        payload: dict[str, Any] = {
            "contents": contents,
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": temperature,
                "responseMimeType": "application/json",
            },
        }
        if system_text:
            payload["systemInstruction"] = {"parts": [{"text": system_text}]}

        url = self._generate_url(use_model)
        headers = {"Content-Type": "application/json"}

        t0 = time.perf_counter()
        response = _post_json(url, headers, payload, timeout=_LLM_HTTP_TIMEOUT)
        latency_ms = (time.perf_counter() - t0) * 1000.0

        content = self._extract_text(response) or ""
        usage = (response or {}).get("usageMetadata") or {}
        pt = int(usage.get("promptTokenCount") or 0)
        ct = int(usage.get("candidatesTokenCount") or 0)
        parsed = _parse_json_from_model_text(content)

        r = LLMResponse(content=content, parsed_json=parsed, model=use_model,
                        prompt_tokens=pt, completion_tokens=ct, latency_ms=latency_ms)
        if r.parsed_json is not None:
            return r

        retry_contents = contents + [
            {"role": "model", "parts": [{"text": content}]},
            {"role": "user", "parts": [{"text": "Your previous reply was not valid JSON. Reply with a single JSON object only."}]},
        ]
        payload["contents"] = retry_contents
        t0 = time.perf_counter()
        response = _post_json(url, headers, payload, timeout=_LLM_HTTP_TIMEOUT)
        latency_ms = (time.perf_counter() - t0) * 1000.0
        content = self._extract_text(response) or ""
        usage = (response or {}).get("usageMetadata") or {}
        parsed = _parse_json_from_model_text(content)
        return LLMResponse(content=content, parsed_json=parsed, model=use_model,
                           prompt_tokens=int(usage.get("promptTokenCount") or 0),
                           completion_tokens=int(usage.get("candidatesTokenCount") or 0),
                           latency_ms=latency_ms)

    def health_check(self) -> bool:
        """True if Gemini API responds with a valid key."""
        if not self._api_key():
            return False
        url = f"https://generativelanguage.googleapis.com/v1beta/models?key={self._api_key()}"
        try:
            data = _get_json(url, timeout=10.0)
            return bool(data and "models" in data)
        except Exception:
            return False

    @staticmethod
    def _extract_text(response: dict[str, Any] | None) -> str | None:
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


def _ollama_root_url(base_url: str) -> str:
    """Normalize base URL for Ollama native ``/api/tags`` vs OpenAI ``/v1`` routes."""

    b = base_url.rstrip("/")
    if b.endswith("/v1"):
        b = b[:-3].rstrip("/")
    return b


def _get_json(url: str, timeout: float = 30.0) -> dict[str, Any] | None:
    req = request.Request(url, method="GET")
    try:
        with request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (error.HTTPError, error.URLError, TimeoutError, json.JSONDecodeError):
        return None


def _parse_json_from_model_text(text: str) -> dict[str, Any] | None:
    """Parse JSON from model output; tolerate optional ```json fences."""

    raw = text.strip()
    fence = re.match(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", raw, re.DOTALL | re.IGNORECASE)
    if fence:
        raw = fence.group(1).strip()
    try:
        out = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return out if isinstance(out, dict) else None


@dataclass
class LLMResponse:
    """Structured chat completion result (Ollama / enrichment workloads)."""

    content: str
    parsed_json: dict[str, Any] | None
    model: str
    prompt_tokens: int
    completion_tokens: int
    latency_ms: float
    cached: bool = False


class OllamaProvider:
    """Local Ollama via native ``/api/chat`` (think=false for Gemma 4 speed)."""

    name = "ollama"

    def __init__(self, model: str = "gemma4:31b", base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url.rstrip("/")

    def _native_chat_url(self) -> str:
        return f"{_ollama_root_url(self.base_url)}/api/chat"

    def _chat_completions_url(self) -> str:
        return f"{_ollama_root_url(self.base_url)}/v1/chat/completions"

    def _tags_url(self) -> str:
        return f"{_ollama_root_url(self.base_url)}/api/tags"

    def complete(self, prompt: str, max_tokens: int = 4) -> str | None:
        """Plain text completion (no JSON mode) — suitable for tiebreak YES/NO/UNSURE."""

        url = self._native_chat_url()
        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "think": False,
            "options": {"temperature": 0.0, "num_predict": max_tokens},
        }
        response = _post_json(url, {"Content-Type": "application/json"}, payload, timeout=_LLM_HTTP_TIMEOUT)
        content = _ollama_native_content(response)
        return content.strip() or None

    def chat_json(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.0,
        seed: int = 42,
        max_tokens: int = 4096,
        json_schema: dict[str, Any] | None = None,
    ) -> LLMResponse:
        """Chat via native ``/api/chat`` with ``think: false`` for speed.

        Gemma 4 defaults to thinking mode which wastes tokens on reasoning.
        Native endpoint + ``think: false`` disables it. JSON is parsed in Python.
        """

        _ = json_schema
        use_model = model or self.model
        url = self._native_chat_url()
        headers = {"Content-Type": "application/json"}

        def _call(msgs: list[dict[str, str]]) -> LLMResponse:
            payload: dict[str, Any] = {
                "model": use_model,
                "messages": msgs,
                "stream": False,
                "think": False,
                "options": {
                    "temperature": temperature,
                    "seed": seed,
                    "num_predict": max_tokens,
                },
            }
            t0 = time.perf_counter()
            response = _post_json(url, headers, payload, timeout=_LLM_HTTP_TIMEOUT)
            latency_ms = (time.perf_counter() - t0) * 1000.0
            content = _ollama_native_content(response)
            pt, ct = _ollama_native_usage(response)
            parsed = _parse_json_from_model_text(content)
            return LLMResponse(
                content=content,
                parsed_json=parsed,
                model=use_model,
                prompt_tokens=pt,
                completion_tokens=ct,
                latency_ms=latency_ms,
            )

        r = _call(messages)
        if r.parsed_json is not None:
            return r

        retry_msgs = [
            *messages,
            {"role": "assistant", "content": r.content},
            {"role": "user", "content": "Your previous reply was not valid JSON. Reply with a single JSON object only."},
        ]
        return _call(retry_msgs)

    def health_check(self) -> bool:
        """True if Ollama responds and ``self.model`` appears in ``/api/tags``."""

        data = _get_json(self._tags_url(), timeout=10.0)
        if not data or "models" not in data:
            return False
        models = data.get("models") or []
        want = self.model.strip()
        for m in models:
            if not isinstance(m, dict):
                continue
            name = str(m.get("name") or "").strip()
            if name == want or name.startswith(want + ":"):
                return True
        return False

    def list_models(self) -> list[dict[str, Any]]:
        """Return installed model metadata from ``GET /api/tags``."""

        data = _get_json(self._tags_url(), timeout=15.0)
        if not data:
            return []
        raw = data.get("models") or []
        return [m for m in raw if isinstance(m, dict)]


def _ollama_native_content(response: dict[str, Any] | None) -> str:
    if not response:
        return ""
    msg = response.get("message")
    if isinstance(msg, dict):
        return str(msg.get("content") or "")
    return ""


def _ollama_native_usage(response: dict[str, Any] | None) -> tuple[int, int]:
    if not response:
        return 0, 0
    return int(response.get("prompt_eval_count") or 0), int(response.get("eval_count") or 0)


def _openai_chat_message_and_usage(response: dict[str, Any] | None) -> tuple[str, int, int]:
    if not response:
        return "", 0, 0
    try:
        content = response["choices"][0]["message"]["content"]
        usage = response.get("usage") or {}
        pt = int(usage.get("prompt_tokens") or 0)
        ct = int(usage.get("completion_tokens") or 0)
        return str(content or ""), pt, ct
    except (IndexError, KeyError, TypeError):
        return "", 0, 0


PROVIDER_REGISTRY: dict[str, type[LLMProvider]] = {
    "gemini": GeminiProvider,
    "openai": OpenAIProvider,
    "ollama": OllamaProvider,
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
    if provider_name == "ollama":
        base_url = str(spec.get("base_url") or "http://localhost:11434")
        return OllamaProvider(model=model, base_url=base_url)
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

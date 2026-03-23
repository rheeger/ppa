"""Embedding provider abstractions for archive-mcp.

The OpenAI-compatible client path is the same transport family intended for
`runtime.local_model_runtime` (CPU/local or remote). Planner and reranker
providers can reuse that pattern when model-backed stages are enabled in config.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from typing import Protocol
from urllib import error, request

from .index_store import get_default_embedding_model, get_vector_dimension

DEFAULT_EMBEDDING_PROVIDER = "hash"
DEFAULT_OPENAI_EMBEDDING_BASE_URL = "https://api.openai.com/v1"
DEFAULT_OPENAI_TIMEOUT_SECONDS = 60
DEFAULT_OPENAI_MAX_RETRIES = 3


class EmbeddingProvider(Protocol):
    name: str
    model: str
    dimension: int

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Return one embedding vector per input text."""


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _run_op_read(reference: str, env: dict[str, str]) -> str | None:
    try:
        result = subprocess.run(
            ["op", "read", reference],
            capture_output=True,
            text=True,
            timeout=20,
            env=env,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


def _resolve_service_account_token() -> str | None:
    configured = os.environ.get("OP_SERVICE_ACCOUNT_TOKEN", "").strip()
    if configured:
        return configured
    token_ref = os.environ.get("PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF", "").strip()
    if token_ref:
        resolved = _run_op_read(token_ref, env=dict(os.environ))
        if resolved:
            return resolved
    token_file = os.environ.get("PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE", "").strip()
    if not token_file:
        return None
    try:
        token = open(token_file, encoding="utf-8").read().strip()
    except OSError:
        return None
    return token or None


def _op_env() -> dict[str, str]:
    env = dict(os.environ)
    token = _resolve_service_account_token()
    if token:
        env["OP_SERVICE_ACCOUNT_TOKEN"] = token
    return env


def _read_1password_secret(reference: str) -> str | None:
    cleaned = reference.strip()
    if not cleaned:
        return None
    return _run_op_read(cleaned, env=_op_env())


def _resolve_openai_api_key() -> str:
    configured = os.environ.get("OPENAI_API_KEY", "").strip()
    if configured and not configured.startswith("op://"):
        return configured
    use_arnold_key = _env_truthy("PPA_USE_ARNOLD_OPENAI_KEY")
    if not configured and not use_arnold_key:
        raise RuntimeError("OPENAI_API_KEY is required for openai embedding provider")
    if configured.startswith("op://") and not use_arnold_key:
        raise RuntimeError(
            "OPENAI_API_KEY is an op:// reference. Set PPA_USE_ARNOLD_OPENAI_KEY=1 to resolve it via 1Password."
        )
    reference = configured or os.environ.get("PPA_OPENAI_API_KEY_OP_REF", "")
    resolved = _read_1password_secret(reference)
    if resolved:
        return resolved
    raise RuntimeError(
        "Could not resolve OPENAI_API_KEY from 1Password. "
        "Check op CLI auth or set PPA_OP_SERVICE_ACCOUNT_TOKEN_OP_REF / PPA_OP_SERVICE_ACCOUNT_TOKEN_FILE."
    )


def _post_json(url: str, headers: dict[str, str], payload: dict[str, object]) -> dict[str, object] | None:
    data = json.dumps(payload).encode("utf-8")
    req = request.Request(url, data=data, headers=headers, method="POST")
    from .index_config import _ppa_env
    timeout_seconds = float(_ppa_env("PPA_OPENAI_TIMEOUT_SECONDS", default=str(DEFAULT_OPENAI_TIMEOUT_SECONDS)))
    max_retries = int(_ppa_env("PPA_OPENAI_MAX_RETRIES", default=str(DEFAULT_OPENAI_MAX_RETRIES)))
    for attempt in range(max_retries + 1):
        try:
            with request.urlopen(req, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            should_retry = exc.code in {408, 409, 429, 500, 502, 503, 504}
            if attempt >= max_retries or not should_retry:
                return None
        except (error.URLError, TimeoutError, json.JSONDecodeError):
            if attempt >= max_retries:
                return None
        time.sleep(min(0.5 * (2**attempt), 4.0))
    return None


class HashEmbeddingProvider:
    """Deterministic built-in embedding provider for local/dev/test use."""

    name = "hash"

    def __init__(self, *, model: str | None = None, dimension: int | None = None):
        self.model = (model or get_default_embedding_model()).strip() or get_default_embedding_model()
        self.dimension = int(dimension or get_vector_dimension())

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        return [self._embed_text(text) for text in texts]

    def _embed_text(self, text: str) -> list[float]:
        seed = text.encode("utf-8")
        values: list[float] = []
        counter = 0
        while len(values) < self.dimension:
            digest = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
            counter += 1
            for idx in range(0, len(digest), 2):
                if len(values) >= self.dimension:
                    break
                pair = digest[idx : idx + 2]
                number = int.from_bytes(pair, "big")
                values.append((number / 65535.0) * 2.0 - 1.0)
        return values


class OpenAIEmbeddingProvider:
    """OpenAI-compatible embeddings REST provider."""

    name = "openai"

    def __init__(self, *, model: str | None = None, dimension: int | None = None, base_url: str | None = None):
        self.model = (model or get_default_embedding_model()).strip() or get_default_embedding_model()
        self.dimension = int(dimension or get_vector_dimension())
        from .index_config import _ppa_env
        self.base_url = (
            (base_url or _ppa_env("PPA_OPENAI_BASE_URL") or os.environ.get("OPENAI_BASE_URL") or DEFAULT_OPENAI_EMBEDDING_BASE_URL)
            .rstrip("/")
        )

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        api_key = _resolve_openai_api_key()
        response = _post_json(
            f"{self.base_url}/embeddings",
            {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            {
                "model": self.model,
                "input": texts,
                "dimensions": self.dimension,
            },
        )
        if not isinstance(response, dict):
            raise RuntimeError("OpenAI embedding request failed")
        data = response.get("data")
        if not isinstance(data, list):
            raise RuntimeError("OpenAI embedding response missing data")
        vectors: list[list[float]] = []
        for item in data:
            if not isinstance(item, dict):
                raise RuntimeError("OpenAI embedding response item is invalid")
            embedding = item.get("embedding")
            if not isinstance(embedding, list):
                raise RuntimeError("OpenAI embedding response missing embedding")
            vector = [float(value) for value in embedding]
            if len(vector) != self.dimension:
                raise RuntimeError(
                    f"OpenAI embedding dimension mismatch: got {len(vector)} expected {self.dimension}"
                )
            vectors.append(vector)
        if len(vectors) != len(texts):
            raise RuntimeError("OpenAI embedding response count mismatch")
        return vectors


def get_embedding_provider(model: str = "") -> EmbeddingProvider:
    from .index_config import _ppa_env
    provider_name = _ppa_env("PPA_EMBEDDING_PROVIDER", default=DEFAULT_EMBEDDING_PROVIDER).lower()
    resolved_model = model.strip() or get_default_embedding_model()
    if provider_name == "hash":
        return HashEmbeddingProvider(model=resolved_model)
    if provider_name == "openai":
        return OpenAIEmbeddingProvider(model=resolved_model)
    raise ValueError(f"Unsupported embedding provider: {provider_name}")

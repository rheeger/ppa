"""Model provider interface for PPA maintenance and enrichment tasks.

Distinct from archive_vault/llm_provider.py (which serves identity resolution
and is reused by the archive_sync/llm_enrichment/ pipeline). This module adds:
is_available() for fallback cascade, generate() for longer outputs,
estimated_cost_per_1k_tokens() for budget gating.

PPA_ENRICHMENT_MODEL format: 'provider:model' (e.g. 'openai:gpt-4o-mini',
'ollama:llama3.2:3b'). The first colon splits provider from model so model
names containing colons (Ollama tags) are preserved.
"""

from __future__ import annotations

import logging
import os
from typing import Protocol, runtime_checkable

from .ollama import OllamaModelProvider
from .openai import OpenAIModelProvider
from .openclaw import OpenClawModelProvider

log = logging.getLogger("ppa.providers")

__all__ = [
    "ModelProvider",
    "OpenAIModelProvider",
    "OllamaModelProvider",
    "OpenClawModelProvider",
    "PROVIDER_REGISTRY",
    "resolve_provider",
]


@runtime_checkable
class ModelProvider(Protocol):
    name: str
    model: str

    def generate(self, prompt: str, max_tokens: int = 1024) -> str: ...

    def is_available(self) -> bool: ...

    def estimated_cost_per_1k_tokens(self) -> float: ...


PROVIDER_REGISTRY: dict[str, type[ModelProvider]] = {
    "openai": OpenAIModelProvider,
    "ollama": OllamaModelProvider,
    "openclaw": OpenClawModelProvider,
}

_CACHED_PROVIDER: ModelProvider | None = None
_RESOLVED: bool = False


def resolve_provider(*, refresh: bool = False) -> ModelProvider | None:
    """Resolve a ModelProvider from PPA_ENRICHMENT_MODEL env var.

    Format: 'provider:model' (e.g. 'openai:gpt-4o-mini', 'ollama:llama3.2:3b').
    The first colon splits provider from model, allowing colons in model names.
    Returns None if the env var is not set.
    Raises ValueError if the provider name is not in PROVIDER_REGISTRY.

    Result is cached at module level; pass refresh=True (tests only) to
    force re-read of the env var.
    """
    global _CACHED_PROVIDER, _RESOLVED
    if _RESOLVED and not refresh:
        return _CACHED_PROVIDER
    raw = os.environ.get("PPA_ENRICHMENT_MODEL", "").strip()
    if not raw:
        _CACHED_PROVIDER = None
        _RESOLVED = True
        return None
    parts = raw.split(":", 1)
    provider_name = parts[0].lower()
    model_name = parts[1] if len(parts) > 1 else ""
    if provider_name not in PROVIDER_REGISTRY:
        raise ValueError(f"Unknown provider '{provider_name}'. Available: {', '.join(sorted(PROVIDER_REGISTRY))}")
    cls = PROVIDER_REGISTRY[provider_name]
    _CACHED_PROVIDER = cls(model=model_name) if model_name else cls()
    _RESOLVED = True
    return _CACHED_PROVIDER


def _reset_provider_cache_for_tests() -> None:
    """Test helper — clears module-level resolve cache."""
    global _CACHED_PROVIDER, _RESOLVED
    _CACHED_PROVIDER = None
    _RESOLVED = False

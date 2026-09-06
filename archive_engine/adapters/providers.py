"""Provider adapter. Embeddings stay behind an injected factory."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class EmbeddingProviderAdapter:
    """Instance-scoped embedding provider. No env reads in the hot path."""

    def __init__(self, factory: Callable[..., Any]):
        self._factory = factory

    def embed_texts(self, texts: list[str], *, model: str = "") -> list[list[float]]:
        provider = self._factory(model=model) if model else self._factory()
        return list(provider.embed_texts(texts))

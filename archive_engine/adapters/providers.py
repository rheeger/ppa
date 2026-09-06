"""Provider adapter. Embeddings stay behind an injected factory."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from archive_engine.contracts import AccessContext
from archive_engine.egress import authorize_destination, egress_scope


class EmbeddingProviderAdapter:
    """Instance-scoped embedding provider. No env reads in the hot path."""

    def __init__(self, factory: Callable[..., Any], access: AccessContext | None = None):
        self._factory = factory
        self._access = access

    def embed_texts(
        self,
        texts: list[str],
        *,
        model: str = "",
        sources: Sequence[str] = (),
        domains: Sequence[str] = (),
    ) -> list[list[float]]:
        provider = self._factory(model=model) if model else self._factory()
        dest = getattr(provider, "name", None) or "unknown"
        payload_sources = tuple(str(item) for item in sources if str(item).strip())
        payload_domains = tuple(str(item) for item in domains if str(item).strip())
        with egress_scope(self._access, sources=payload_sources, domains=payload_domains):
            authorize_destination(
                dest,
                access=self._access,
                sources=payload_sources,
                domains=payload_domains,
            )
            return list(provider.embed_texts(texts))

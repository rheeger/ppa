"""Enrichment helpers for iMessage thread summaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from hfa.config import load_config
from hfa.enrichment import EnrichmentStep
from hfa.llm_provider import (GROUNDING_INSTRUCTION, get_provider_chain,
                              load_llm_config)
from hfa.provenance import ProvenanceEntry
from hfa.schema import BaseCard, IMessageThreadCard
from hfa.thread_hash import (compute_imessage_thread_body_sha,
                             imessage_thread_messages_payload)


@dataclass
class IMessageThreadSummaryEnrichment(EnrichmentStep):
    name: str = "imessage-thread-summary"
    version: int = 1
    target_fields: list[str] = field(default_factory=lambda: ["thread_summary"])
    method: str = "llm"

    def should_run(
        self,
        card: BaseCard,
        body: str,
        provenance: dict[str, ProvenanceEntry],
        vault_path: str | Path = "",
    ) -> bool:
        if not isinstance(card, IMessageThreadCard) or not vault_path:
            return False
        if not load_config(vault_path).imessage_thread_body_sha_cache_enabled:
            return True
        current_hash = card.thread_body_sha or compute_imessage_thread_body_sha(card, vault_path)
        for field_name in self.target_fields:
            entry = provenance.get(field_name)
            if entry is None:
                return True
            if entry.enrichment_version < self.version:
                return True
            if entry.input_hash != current_hash:
                return True
        return False

    def run(
        self,
        card: BaseCard,
        body: str,
        vault_path: str,
    ) -> dict[str, tuple[Any, ProvenanceEntry]]:
        if not isinstance(card, IMessageThreadCard):
            return {}
        messages = imessage_thread_messages_payload(card, vault_path)
        if not messages:
            return {}
        current_hash = card.thread_body_sha or compute_imessage_thread_body_sha(card, vault_path)
        prompt = (
            f"{GROUNDING_INSTRUCTION}\n\n"
            "Summarize this iMessage conversation in 2-4 sentences. "
            "Focus on concrete logistics, plans, and notable decisions. "
            "Do not invent context that is not present.\n\n"
            f"Thread: {card.summary}\n"
            f"Participants: {card.participant_handles}\n"
            f"Messages: {messages[-30:]}"
        )
        max_tokens = int(load_llm_config(vault_path).get("max_tokens_enrichment", 256))
        for provider in get_provider_chain(vault_path):
            response = provider.complete(prompt, max_tokens=max_tokens)
            if not response:
                continue
            summary = response.strip()
            if not summary:
                continue
            return {
                "thread_summary": (
                    summary,
                    ProvenanceEntry(
                        source=self.name,
                        date=date.today().isoformat(),
                        method=self.method,
                        model=provider.model,
                        enrichment_version=self.version,
                        input_hash=current_hash,
                    ),
                )
            }
        return {}

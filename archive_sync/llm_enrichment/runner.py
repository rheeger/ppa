"""Orchestration helpers — triage + hydrate + extract for one thread."""

from __future__ import annotations

from pathlib import Path

from archive_cli.vault_cache import VaultScanCache
from archive_sync.llm_enrichment.cache import InferenceCache
from archive_sync.llm_enrichment.extract import ExtractResult, extract_cards_for_thread
from archive_sync.llm_enrichment.threads import (
    ThreadDocument,
    ThreadStub,
    build_thread_index,
    hydrate_thread,
    load_email_stubs_for_vault,
    render_thread_for_triage,
)
from archive_sync.llm_enrichment.triage import TriageResult, triage_thread
from archive_vault.llm_provider import OllamaProvider


def stubs_for_thread_id(vault: Path, thread_id: str) -> list[ThreadStub]:
    """Return ordered stubs for ``gmail_thread_id`` (or ``_singleton:...``)."""

    stubs = load_email_stubs_for_vault(Path(vault))
    index = build_thread_index(stubs)
    return list(index.get(thread_id, []))


def run_thread_pipeline(
    vault: Path,
    thread_id: str,
    provider: OllamaProvider,
    *,
    scan_cache: VaultScanCache | None = None,
    inference_cache: InferenceCache | None = None,
    triage_model: str | None = None,
    extract_model: str | None = None,
    run_id: str = "",
) -> tuple[TriageResult, ExtractResult | None, ThreadDocument | None]:
    """Triage → (optional) hydrate + extract. Returns ``(triage, extract_or_none, doc_or_none)``."""

    vault = Path(vault)
    group = stubs_for_thread_id(vault, thread_id)
    if not group:
        raise ValueError(f"No email_message stubs for thread_id={thread_id!r}")

    triage_text = render_thread_for_triage(group)
    tri = triage_thread(
        provider,
        triage_text,
        model=triage_model,
        cache=inference_cache,
        run_id=run_id,
    )
    if tri.skip:
        return tri, None, None

    cache = scan_cache
    if cache is None:
        cache = VaultScanCache.build_or_load(vault, tier=2, progress_every=0)

    doc = hydrate_thread(group, vault, scan_cache=cache)
    ex = extract_cards_for_thread(
        provider,
        doc,
        tri.card_types,
        model=extract_model,
        cache=inference_cache,
        run_id=run_id,
    )
    return tri, ex, doc

"""Phase 2.75 — local LLM email enrichment (Ollama / Gemma)."""

from archive_sync.llm_enrichment.cache import (InferenceCache,
                                               build_inference_cache_key)
from archive_sync.llm_enrichment.extract import (EXTRACT_PROMPT_VERSION,
                                                 ExtractedCard, ExtractResult,
                                                 extract_cards_for_thread)
from archive_sync.llm_enrichment.runner import (run_thread_pipeline,
                                                stubs_for_thread_id)
from archive_sync.llm_enrichment.schema_gen import (
    EXTRACTABLE_TYPES, LLM_OMIT_FIELDS, all_extractable_card_types,
    card_type_to_llm_json_schema, combined_schema_version,
    extractable_types_reference_markdown, schema_version_for_card_type)
from archive_sync.llm_enrichment.threads import (
    ThreadDocument, ThreadMessage, ThreadStub, build_thread_index,
    build_thread_index_from_cache, email_message_stubs_from_sqlite,
    hydrate_thread, load_email_stubs_for_vault, render_thread_for_extraction,
    render_thread_for_triage, stubs_from_filesystem_walk,
    thread_stub_from_frontmatter)
from archive_sync.llm_enrichment.triage import (SKIP_CLASSIFICATIONS,
                                                TRIAGE_PROMPT_VERSION,
                                                TriageResult, triage_thread)

__all__ = [
    "EXTRACTABLE_TYPES",
    "EXTRACT_PROMPT_VERSION",
    "ExtractResult",
    "ExtractedCard",
    "InferenceCache",
    "LLM_OMIT_FIELDS",
    "SKIP_CLASSIFICATIONS",
    "TRIAGE_PROMPT_VERSION",
    "ThreadDocument",
    "ThreadMessage",
    "ThreadStub",
    "TriageResult",
    "all_extractable_card_types",
    "build_inference_cache_key",
    "build_thread_index",
    "build_thread_index_from_cache",
    "card_type_to_llm_json_schema",
    "combined_schema_version",
    "email_message_stubs_from_sqlite",
    "extract_cards_for_thread",
    "extractable_types_reference_markdown",
    "hydrate_thread",
    "load_email_stubs_for_vault",
    "render_thread_for_extraction",
    "render_thread_for_triage",
    "run_thread_pipeline",
    "schema_version_for_card_type",
    "stubs_for_thread_id",
    "stubs_from_filesystem_walk",
    "thread_stub_from_frontmatter",
    "triage_thread",
]

"""Default Ollama model tags for ``enrich-emails`` (Gemma 4 only).

**Triage** uses ``render_thread_for_triage`` — subject, snippets, participants, not full
bodies. Task: classification + ``card_types`` routing. See Google’s Gemma 4 overview:
https://ai.google.dev/gemma/docs/core — **E4B** (effective ~4B) targets edge/fast inference
and fits this shallower task.

**Extraction** hydrates the full thread and emits schema-constrained JSON (Pydantic cards).
**31B dense** is the default quality anchor; consider ``gemma4:26b`` MoE only if benchmarks
match 31B on your ground truth.

**E2B** — smaller triage if you need minimum latency; **26B MoE** still loads full weight
footprint for routing, so it is not the default “light triage” pick.

Override: ``--triage-model`` / ``--extract-model``, or Makefile ``ENRICH_TRIAGE_MODEL`` /
``ENRICH_EXTRACT_MODEL``.
"""

DEFAULT_ENRICH_TRIAGE_MODEL = "gemma4:e4b"  # Legacy — triage stage removed; kept for backward compat
DEFAULT_ENRICH_EXTRACT_MODEL = "gemma4:31b"

# Phase 2.75: LLM Email Enrichment Pipeline — Summary

## Goal

Replace regex-based email extraction (Phase 2.5/3) with LLM-powered extraction that understands email content, handles any provider without per-provider code, and deduplicates at the thread level.

## Outcome

Production-ready 3-stage pipeline that extracts structured transaction cards from email threads using Gemini API. Tested on 1% and 10% vault slices with consistent results.

### Key metrics (1pct final run, v11)

| Metric | Value |
|--------|-------|
| Extraction yield | **84%** (up from 20% at start of phase) |
| Cards written | 486 (1pct), 4,469 (10pct) |
| Schema failures | 0 |
| Errors | 0 |
| Card types | 11 transaction types |
| Wall clock (1pct) | 81 seconds |
| Estimated full seed cost | ~$10 |

### Comparison to regex baseline (Phase 3)

| | Regex (Phase 3) | LLM (Phase 2.75) |
|--|---:|---:|
| Cards extracted | 3,365 | ~4,860 (10x from 1pct) |
| Card types covered | 7 (regex extractors exist) | 11 (any sender) |
| Items populated | 27% (meal orders) | ~70%+ |
| Duplicate-suspect rate | 41% | Near zero (thread-level extraction) |
| Per-provider code required | Yes (EDL per sender) | No |
| New provider support | Weeks of EDL | Add domain to `known_senders.py` |

## Architecture

```
Stage 0: Domain Gate (free, instant)
   known_senders.py: 150+ sender domains → fast-track / skip / classify
   ↓
Stage 1: LLM Classify (~200 tokens per thread)
   classify_system.txt → gemini-3.1-flash-lite-preview
   "transactional" / "personal" / "marketing" / "automated" / "noise"
   Returns card_types for targeted extraction
   Results stored in persistent classify_index.db
   ↓
Stage 2: LLM Extract (~1,700 tokens per thread)
   extract_system.txt → gemini-3.1-flash-lite-preview (or 2.5-flash-lite)
   Produces typed card JSON validated by Pydantic
   Writes to staging directory with provenance
```

## Key files

| File | Purpose |
|------|---------|
| `archive_sync/llm_enrichment/enrich_runner.py` | 3-stage pipeline orchestrator |
| `archive_sync/llm_enrichment/classify.py` | Stage 1 classifier module |
| `archive_sync/llm_enrichment/classify_index.py` | Persistent thread classification index |
| `archive_sync/llm_enrichment/extract.py` | Stage 2 card extractor |
| `archive_sync/llm_enrichment/known_senders.py` | Domain registry + pre-filter |
| `archive_sync/llm_enrichment/schema_gen.py` | JSON Schema generation with field descriptions |
| `archive_sync/llm_enrichment/cache.py` | SQLite inference cache |
| `archive_sync/llm_enrichment/threads.py` | Thread assembly from vault |
| `archive_sync/llm_enrichment/defaults.py` | Default model configuration |
| `archive_sync/llm_enrichment/prompts/classify_system.txt` | Classification prompt |
| `archive_sync/llm_enrichment/prompts/extract_system.txt` | Extraction prompt |
| `hfa/llm_provider.py` | OllamaProvider + GeminiProvider with retry/backoff |
| `docs/gemini-api-privacy.md` | Gemini API data retention documentation |
| `docs/gemma4-local-models.md` | Local model documentation |

## Model decisions

### Final configuration
- **Classify model:** `gemini-3.1-flash-lite-preview` (smarter classification, separate TPM pool)
- **Extract model:** `gemini-3.1-flash-lite-preview` (84% yield) or `gemini-2.5-flash-lite` (79% yield, 2.5x cheaper)
- **Workers:** 18 extract, 54 classify (with 10K RPM tier)

### Models tested and rejected
- **Ollama gemma4:31b** — too slow locally (17 tok/s), thinking mode caused 80s+ per call
- **Ollama gemma4:26b MoE** — slower than 31b on Apple Silicon due to routing overhead
- **Ollama gemma4:e4b** — fast triage but extraction quality insufficient
- **Ollama qwen3:8b** — 74 tok/s, 5 thr/min with parallelism, but 72 schema failures per run
- **Gemini 2.0 Flash** — deprecated, 404 on API
- **Gemini 2.5 Flash** — works but 1M TPM limit vs 10M for flash-lite

### Key technical discoveries
- Gemma 4 defaults to **thinking mode** on Ollama, burning 90% of tokens on reasoning. Fix: native `/api/chat` with `think: false`
- Ollama `NUM_PARALLEL` requires server restart and `OLLAMA_CONTEXT_LENGTH` cap to avoid memory thrashing
- Apple Silicon M4 Max: 21 tok/s decode for gemma4:31b at Q4_K_M (below benchmarks due to KV cache overhead)
- Gemini API `responseMimeType: "application/json"` for structured output

## Prompt evolution

### Extract prompt (10+ iterations)
1. Basic extraction rules → type hallucinations (invented types)
2. Added valid type enum → fixed
3. Added items format example → fixed dict-vs-list
4. Added ISO-8601 date rules → fixed prose dates
5. Added IATA airport rules → fixed city-name airports
6. Added P2P payment capture → Venmo/PayPal now extracted
7. Added cancellation support → flight/booking cancellations captured
8. Added exclusions for fraud alerts, delivery status, payroll admin
9. Added invoice-paid-as-purchase rule

### Classify prompt (5+ iterations)
1. Basic 5-category classification → too conservative
2. Added "bias toward transactional" → better recall
3. Added card_types output → targeted extraction routing
4. Added money-mention != transactional rule → reduced false positives
5. Added confirmation-vs-update distinction → 84% extraction yield

## Yield improvement journey

| Version | Cards | Extract calls | Yield | Wall clock |
|---------|------:|-------------:|------:|---------:|
| No-gate (all threads) | 650 | 8,698 | 7% | 25 min |
| V5 (first pipeline) | 509 | 1,944 | 37% | 9.3 min |
| V8 (all classify) | 674 | 1,124 | 49% | 5.4 min |
| V9 (3.1FL classify) | 620 | 841 | 61% | 6.8 min |
| V10 (tighter rules) | 540 | 649 | 71% | 6.8 min |
| **V11 (final, 3.1FL)** | **486** | **482** | **84%** | **1.4 min** |

## What remains (Phase 3 execution)

1. **Full seed run** — `make enrich-emails-gemini` against production vault (~$10, ~5 hours estimated)
2. **Quality report** — compare `_staging-llm/` vs `_staging/` (regex)
3. **Human review gate** — approve LLM staging before vault promotion
4. **Promote** — `make promote-staging` to write cards to vault
5. **Future card types** — `finance`, `calendar_event`, `medical_record` extraction prompts
6. **Conversation extraction** — personal thread analysis (different model, thinking-enabled)
7. **Incremental enrichment** — classify index enables re-run on only new/changed threads

## Tests

579 tests passing, including:
- 18 classify module tests (card_types parsing, caching, edge cases)
- Triage/extract integration tests
- LLM provider tests (Ollama native API + Gemini REST)
- Schema generation tests
- Cache tests
- Runner helper tests

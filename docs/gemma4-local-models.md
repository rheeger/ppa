# Gemma 4 — local runs (Ollama) vs official sizes

Phase 2.75 uses **Gemma 4 only** (not Gemma 3). The core family has **four** sizes; see Google’s overview for capabilities and licensing: [Gemma 4 model overview](https://ai.google.dev/gemma/docs/core).

## Official parameter line-up

| Size (Google)     | Role                                                                                    |
| ----------------- | --------------------------------------------------------------------------------------- |
| **E2B** / **E4B** | Small _effective-parameter_ models (edge / browser-class); “E” = effective params + PLE |
| **26B A4B**       | Mixture-of-Experts — ~4B active per token, **full weight set still loaded** for routing |
| **31B**           | Dense — strongest local “workstation” tier in the family                                |

## Approximate inference memory (weights only)

From the same doc (BF16 / SFP8 / Q4_0 — actual RAM depends on tool, KV cache, and context):

| Parameters      | BF16    | SFP8    | Q4_0    |
| --------------- | ------- | ------- | ------- |
| Gemma 4 E2B     | 9.6 GB  | 4.6 GB  | 3.2 GB  |
| Gemma 4 E4B     | 15 GB   | 7.5 GB  | 5 GB    |
| Gemma 4 31B     | 58.3 GB | 30.4 GB | 17.4 GB |
| Gemma 4 26B A4B | 48 GB   | 25 GB   | 15.6 GB |

KV cache and software overhead sit **on top** of these figures.

## Ollama tags in this repo

Use the **`gemma4`** library on Ollama; typical pulls:

- `gemma4:e2b` — E2B
- `gemma4:e4b` — E4B
- `gemma4:26b` — 26B MoE (A4B)
- `gemma4:31b` — 31B dense

Quantization on disk differs by Ollama build; treat the table above as planning guidance, not exact `ollama list` bytes.

## Enrichment pipeline defaults (`enrich-emails`)

Triage only sees thread **metadata + snippets** (classification / routing). Extraction sees **full bodies** and must emit **typed JSON** aligned with Pydantic — heavier task.

| Stage       | Default tag  | Rationale                                                                                                       |
| ----------- | ------------ | --------------------------------------------------------------------------------------------------------------- |
| **Triage**  | `gemma4:e4b` | Gemma 4 “E4B” edge tier — enough for skip vs transactional + `card_types` without loading 31B twice per thread. |
| **Extract** | `gemma4:31b` | Dense 31B for schema-following extraction from full thread text.                                                |

Overrides: CLI `--triage-model` / `--extract-model`, code `archive_sync/llm_enrichment/defaults.py`, or Make `ENRICH_TRIAGE_MODEL` / `ENRICH_EXTRACT_MODEL` on `enrich-emails-staging`.

Use **`gemma4:e2b`** if you need minimum triage latency; use **`gemma4:26b`** for extraction only if benchmarks show quality parity (MoE still loads a large weight set).

## Benchmarks

- **Single model:** `make run-enrichment-benchmark` (override `STEP9_MODELS`).
- **Full Gemma 4 matrix:** `make run-enrichment-benchmark-matrix` — then `make aggregate-benchmark-results` → `_benchmark/results/index.md`.
- **Model choice doc:** `docs/phase275-step10-model-selection.md` (Step 10 / 10b).

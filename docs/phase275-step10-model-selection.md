# Phase 2.75 — Step 10 model selection

Fill this after **`make run-enrichment-benchmark-matrix`** (or a custom `STEP9_MODELS=...` run) and **`make aggregate-benchmark-results`**.

See **`_artifacts/_benchmark/results/index.md`** for the comparison table and per-model folders for `summary.md` / `failures.json`.

## Chosen models (Gemma 4 only)

| Role           | Ollama tag        | Rationale |
| -------------- | ----------------- | --------- |
| **Triage**     | e.g. `gemma4:e4b` |           |
| **Extraction** | e.g. `gemma4:31b` |           |

## Quality gates (from plan — adjust if you change thresholds)

- Triage: classification usable if false-negative rate on positives and FP rate on negatives are acceptable for your risk tolerance.
- Extraction: prefer higher field match + schema-validated counts; watch `failures.json` for systematic misses.

## Wall-time note (rough)

Record expected triage vs extract model loads for Step 12–13 planning (measure on your machine).

## Step 10b decision

- [ ] **APPROVE** — proceed to prompt iteration (Step 11) and/or 10% pilot (Step 12)
- [ ] **REVISE** — re-run benchmark with different tags or prompts first

**Approver / date:**

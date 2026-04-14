# Phase 3 — Step 8.5 human review gate

**Status:** Ready — Step 8 artifacts generated after full-seed extraction (7c).

**Paths:** Links to `_artifacts/_staging/` use three `../` segments from this folder (`extraction-quality/` → `ppa/`). If links still 404 in a tool, open files from the repo root: `ppa/_artifacts/_staging/…`. That directory is often **gitignored** (large); it exists on disk after extraction but may not appear in Git-hosted previews.

## Headline metrics (`ppa/_artifacts/_staging/_metrics.json`)

| Metric              |     Value |
| ------------------- | --------: |
| Emails scanned      |   461,216 |
| Matched             |     9,936 |
| **Cards extracted** | **3,365** |
| Errors              |     **0** |
| Wall clock (s)      |    ~2,605 |

## Artifacts to read (in order)

1. **Automated quality report** — flags, confidence histograms, problem/clean YAML samples per type:  
   [`phase3-full-seed.md`](phase3-full-seed.md)  
   _(Generated without `--vault` in the report step to keep runtime fast; **source round-trip** for samples is covered in factual trace notes below.)_

2. **Staging volume check** — JSON + human table:  
   [`_artifacts/_staging/_staging-report.json`](../../../_artifacts/_staging/_staging-report.json) · [`_artifacts/_staging/_staging-report-human.txt`](../../../_artifacts/_staging/_staging-report-human.txt)

3. **Factual trace** — 5 random cards per type, `validate_provenance_round_trip` vs source email:  
   [`_artifacts/_staging/factual-trace-notes.md`](../../../_artifacts/_staging/factual-trace-notes.md)

4. **Dedup suspects** — same key as extractor dedup heuristics:  
   [`_artifacts/_staging/dedup-check-notes.md`](../../../_artifacts/_staging/dedup-check-notes.md)

5. **Template-era scan** — counts by calendar year from staging path segments:  
   [`_artifacts/_staging/template-era-scan.md`](../../../_artifacts/_staging/template-era-scan.md)

6. **Confidence-tier samples (Step 8.5)** — one high / mid / low per type when the pool has one:  
   [`_artifacts/_staging/review-samples-by-confidence.md`](../../../_artifacts/_staging/review-samples-by-confidence.md)

## Your call (reply with one)

- **APPROVE** — proceed to Step 9 (`promote-staging` into the seed vault).
- **FIX** — list extractors/issues; we iterate (Step 8.6) and re-staging/re-report.
- **REJECT** — stop Phase 3; broader extractor / process rework.

---

_Preflight / vault parity notes (Arnold SSH optional): [`phase3-preflight-notes.md`](phase3-preflight-notes.md)_

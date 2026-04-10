# Phase 3 Step 6.5 — Pre-flight notes

**Date:** 2026-04-07

## 6.5a Quality baseline currency

- `docs/reports/extraction-quality/10pct.md` is labeled **post-Phase 2.5** (Generated: 2026-04-07).
- Git history on `archive_sync/extractors/` includes `cb35660` (Phase 2.5 review feedback) after `d109256` (main Phase 2.5). Per plan, **Step 7b re-runs** `extract-emails-10pct-slice` and `make extraction-quality-reports` so the 10% baseline is refreshed against current `HEAD` before full-seed extraction.

## 6.5b Vault parity (local seed)

| Check              | Result                                                         |
| ------------------ | -------------------------------------------------------------- |
| Seed vault path    | `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` |
| `Email/*.md` count | **461,216**                                                    |

## Arnold production vault

Automated `ssh arnold` count was **not** obtained from this environment (`Host key verification failed`). Before Step 12b, run manually:

```bash
ssh arnold "find /srv/hfa-secure/vault/Email -name '*.md' 2>/dev/null | wc -l"
```

Compare to local **461,216**; reconcile with rsync if counts diverge meaningfully.

## Quality gate (Phase 2.5 baseline)

Proceeding to Step 7 assumes Step 7b’s fresh `_artifacts/_staging-10pct/_metrics.json` and regenerated `10pct.md` meet the non-regression table in the Phase 3 plan (meal_order items ≥20%, zero URL-noise flags, flight IATA 100%, errors 0, etc.).

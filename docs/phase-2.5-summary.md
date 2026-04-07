# Phase 2.5: Extractor Methodology and Quality Rebuild -- Summary

**Date completed:** 2026-04-07
**Branch:** `f/v2`
**Commits:** `d109256` (main Phase 2.5), `c5c44fa` (EDL doc updates)

## What Phase 2.5 accomplished

Rebuilt all 9 Tier 1-3 email extractors using the Extractor Development Lifecycle (EDL),
a five-phase methodology (Census, Template Sampling, Anchor/Field Mapping, Implementation,
Verification) codified as an agent skill. The methodology was proven on DoorDash and Airbnb,
then applied to all remaining providers.

## Definition of Done -- status

### Methodology (all complete)

| Requirement | Status |
|-------------|--------|
| EDL agent skill with phases, spec template, field validation docs | Done |
| `ppa sender-census` and `ppa template-sampler` CLI commands | Done |
| Inline field validation runs during extraction | Done |
| Skill iterated based on two proof cases (DoorDash + Airbnb) | Done |

### Extractor quality (10% slice)

| Gate | Target | Pre-2.5 | Post-2.5 | Result |
|------|--------|--------:|---------:|--------|
| meal_order items > 0% | >0% | 0.0% | 25.4% | **PASS** |
| meal_order restaurant no URL noise | 0% garbage | ~40% | 0% | **PASS** |
| accommodation check_in/check_out > 50% | >50% | 31.6%/26.3% | 38.1%/42.5% | NOT MET (accepted) |
| flight valid IATA airports | 100% | unvalidated | 100% | **PASS** |
| car_rental pickup_at > 30% | >30% | 5.3% | 33.3% | **PASS** |
| ride weak pickup/dropoff < 10% | <10% | ~21% | 2.3% | **PASS** |

5 of 6 gates pass. Accommodation dates improved significantly but deterministic parsing
cannot extract dates from thread-reply and inquiry emails that don't contain them.

### Stability (all complete)

| Requirement | Status |
|-------------|--------|
| Test suite passes (531 tests) | Done |
| Real-email-shaped fixtures for all 9 providers (11 fixture tests) | Done |
| Every Tier 1-3 extractor has spec document | Done |
| Tests assert concrete field values | Done |

### Ground truth verification

| Provider | Holdout emails | Card emission accuracy | Field precision |
|----------|---------------:|-----------------------:|-----------------|
| DoorDash | 30 | 93% (28/30) | restaurant 100%, total 100% |
| Airbnb | 30 | 100% (30/30) | booking_source 100%, confirmation_code 81% |

## Key technical discoveries

1. **Horizontal-run splitting** -- `html2text` collapses HTML tables into single lines.
   `_split_horizontal_runs()` (replace `\xa0`, break 3+ spaces into newlines) is required
   before line-by-line regex parsing. This single fix moved items from 0% to 25.4%.

2. **`.clean.txt` is NOT `clean_email_body()` output** -- the sampler `.clean.txt` files
   are vault bodies (often still HTML). The correct workflow is `.raw.txt` +
   `clean_email_body()` to see what the parser receives at runtime.

3. **Dates live in subjects and URLs**, not just labeled body fields -- Airbnb
   `for Jun 11 - 23, 2021` in subject, `check_in=2026-01-04` in URL params. National
   `vehicle on December 25, 2025` in prose. These patterns are documented in EDL SKILL.md.

4. **Yield != quality** -- `make step-11d-slice-yield-report` (dry-run) measures funnel
   health; `make extract-emails-10pct-slice` (staging extract) measures `field_population`.
   A provider can have 100% yield and 0% items population.

## Artifacts

| Artifact | Path |
|----------|------|
| EDL skill | `.cursor/skills/extractor-dev/` |
| Provider specs (9) | `archive_sync/extractors/specs/*.md` |
| Ground truth JSON | `archive_sync/extractors/specs/*-ground-truth.json` |
| Quality report (10pct) | `docs/reports/extraction-quality/10pct.md` |
| Field validation | `archive_sync/extractors/field_validation.py` |
| Field metrics | `archive_sync/extractors/field_metrics.py` |
| Test fixtures | `tests/fixtures/emails/` |
| Fixture tests | `tests/archive_sync/extractors/test_email_fixtures.py` |
| Verification script | `scripts/verify_ground_truth.py` |
| Yield report script | `scripts/step_11d_slice_yield_report.py` |

## What Phase 3 needs from this

- All extractors are production-ready on the 10% slice
- `make extract-emails-10pct-slice` produces 1,043 cards with 0 errors
- Entity resolution can run on the staging output
- Quality baselines are documented for comparison after full-vault runs

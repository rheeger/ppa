# Extractor specs (EDL)

This directory holds **extractor specifications** and **ground truth** JSON files used in the Extractor Development Lifecycle (Phase 3 and Phase 5).

- **`<provider>.md`** — taxonomy, template eras, field anchors, fixture coverage matrix, dedup strategy.
- **`<provider>-ground-truth.json`** — holdout email UIDs and expected extracted fields for verification (`scripts/verify_ground_truth.py`).
- **`_template.md`** — blank spec outline (copy when starting a new provider).
- **`_ground_truth_schema.json`** — JSON Schema for ground truth files.

Specs are written after **census** and **template-sampler** runs against the real vault, not from imagined formats.

## Template sampler evidence (paths)

- **`samples_seed/<provider>/`** — output from the **full seed** batch (`make step-11a-template-samplers-seed`). Use this for **Phase 11b–11c** and spec anchors when you need **quality coverage** across years. See [`samples_seed/README.md`](samples_seed/README.md).
- **`samples/<provider>/`** — output from the **10% stratified slice** (`make step-11a-template-samplers`). Faster to regenerate; good for **smoke** and CI-sized checks.

When documenting field anchors in `<provider>.md`, prefer file paths under **`samples_seed/`** if that run exists.

## Step 11 (2.5) — remaining workflow

| Step    | What                         | Command / artifact                                                                                        |
| ------- | ---------------------------- | --------------------------------------------------------------------------------------------------------- |
| **11a** | Template samples (seed)      | `make step-11a-template-samplers-seed` → `samples_seed/<provider>/`                                       |
| **11b** | Parser pass vs `.clean.txt`  | Edit `archive_sync/extractors/*.py`; cite paths in `<provider>.md`                                        |
| **11c** | Fixtures from sampler bodies | Copy PII-redacted `.clean.txt` into `archive_tests/fixtures/emails/`                                              |
| **11d** | Yield on slice               | `python scripts/step_11d_slice_yield_report.py` (dry-run, one pass) or `make step-11d-slice-yield-report` |

## Current specs (Phase 2.5)

Tier 1–3 extractors have a short spec note (methodology + key parser behavior). **Ground truth JSON** is only required where listed; bootstrap via `build_ground_truth_holdouts.py` currently supports **doordash** and **airbnb** only.

| Provider    | Spec                                                                                              | Ground truth                                               |
| ----------- | ------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| DoorDash    | [`doordash.md`](doordash.md), smoke census [`doordash-census-smoke.md`](doordash-census-smoke.md) | [`doordash-ground-truth.json`](doordash-ground-truth.json) |
| Airbnb      | [`airbnb.md`](airbnb.md)                                                                          | [`airbnb-ground-truth.json`](airbnb-ground-truth.json)     |
| Uber Eats   | [`ubereats.md`](ubereats.md)                                                                      | (extend script to add)                                     |
| United      | [`united.md`](united.md)                                                                          | (extend script to add)                                     |
| Instacart   | [`instacart.md`](instacart.md)                                                                    | (extend script to add)                                     |
| Shipping    | [`shipping.md`](shipping.md)                                                                      | —                                                          |
| Rental cars | [`rental_cars.md`](rental_cars.md)                                                                | —                                                          |
| Lyft        | [`lyft.md`](lyft.md)                                                                              | —                                                          |
| Amazon      | [`amazon.md`](amazon.md)                                                                          | —                                                          |

Regenerate holdouts (same vault as extraction tests):

```bash
PPA_PATH=.slices/10pct .venv/bin/python scripts/build_ground_truth_holdouts.py --vault .slices/10pct --provider doordash
PPA_PATH=.slices/10pct .venv/bin/python scripts/build_ground_truth_holdouts.py --vault .slices/10pct --provider airbnb
```

Verify:

```bash
.venv/bin/python scripts/verify_ground_truth.py --ground-truth archive_sync/extractors/specs/doordash-ground-truth.json --vault-path .slices/10pct
```

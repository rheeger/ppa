# Extraction quality — local slices (1% / 5% / 10%)

Runs use `PPA_PATH=ppa/.slices/{1pct,5pct,10pct}`, staging under `_staging-{1,5,10}pct/`, Phase-3 derived dirs cleaned on each slice before extract. **Volume “expected” columns in the deep reports are calibrated for full seed runs**, not stratified slices — treat LOW/HIGH there as informational.

## Run summary

| Slice | Emails scanned | Matched | Cards written | Wall (approx.) | Logs                           |
| ----- | -------------: | ------: | ------------: | -------------: | ------------------------------ |
| 1%    |         34,081 |     297 |            66 |       ~3.5 min | `logs/extract-1pct-slice.log`  |
| 5%    |        108,103 |   1,429 |           389 |       ~9.4 min | `logs/extract-5pct-slice.log`  |
| 10%   |        166,637 |   2,548 |           716 |      ~13.8 min | `logs/extract-10pct-slice.log` |

## Deep-dive reports

| Slice | Report                 |
| ----- | ---------------------- |
| 1%    | [1pct.md](./1pct.md)   |
| 5%    | [5pct.md](./5pct.md)   |
| 10%   | [10pct.md](./10pct.md) |

Regenerate after a new extract:

```bash
cd ppa
.venv/bin/python scripts/generate_extraction_quality_report.py \
  --staging-dir _staging-10pct --label 10pct --out docs/reports/extraction-quality/10pct.md \
  --problem-samples 10 --clean-samples 5
```

## Cross-slice signals (priority for extractor upgrades)

1. **`meal_order` — `items` 0% strict population** on all slices; `restaurant` / `total` often pass. Many `restaurant` values are footer/marketing noise (see flagged samples).
2. **DoorDash — ~9–11% yield** on 5% / 10% with large `rejected` counts.
3. **Uber Rides — ~25–30% yield**; check weak pickup/dropoff heuristics in reports.
4. **United — ~52% yield**; audit airport codes on flagged flights.
5. **Airbnb — ~13–21% yield**; check_in/out ~30% on cards.
6. **Instacart — 0 cards** on these runs (low match counts — confirm with seed fixtures).
7. **Car rental — `pickup_at` ~5–10%** on extracted cards.

## Suggested review order

`doordash` → `uber_eats` → `uber_rides` → `united` → `airbnb` → `shipping` (spot-check) → `instacart` → `rental_cars` → `amazon` / `lyft`.

## Artifacts

- Staging: `_staging-1pct/`, `_staging-5pct/`, `_staging-10pct/` (each has `_metrics.json`).
- Generator: `scripts/generate_extraction_quality_report.py`.

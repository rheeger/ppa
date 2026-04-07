# Extraction quality -- `10pct` slice (post-Phase 2.5)

Generated: 2026-04-07 from `_staging-10pct/` after Phase 2.5 extractor rebuild.

Staging directory: `_staging-10pct/`

## Runner metrics (from `_metrics.json`)

- **Emails scanned:** 166,637
- **Matched:** 2,594
- **Total cards on staging:** 1,043
- **Errors:** 0
- **Wall clock (s):** 823.5

| Extractor | matched | extracted + skipped | errors | rejected |
|-----------|--------:|--------------------:|-------:|---------:|
| airbnb | 183 | 121 | 0 | 32 |
| doordash | 475 | 99 | 0 | 258 |
| instacart | 38 | 3 | 0 | 1 |
| lyft | 46 | 5 | 0 | 0 |
| rental_cars | 36 | 16 | 0 | 3 |
| shipping | 348 | 145 | 0 | 0 |
| uber_eats | 198 | 137 | 0 | 37 |
| uber_rides | 1007 | 256 | 0 | 1 |
| united | 263 | 145 | 0 | 0 |

## Volume by card type

| Type | Count |
|------|------:|
| accommodation | 134 |
| car_rental | 24 |
| flight | 165 |
| grocery_order | 3 |
| meal_order | 311 |
| ride | 261 |
| shipment | 145 |

## Critical field population (from `_metrics.json`)

| Type | Field | Population | v2vision target | Status |
|------|-------|----------:|----------------|--------|
| accommodation | property_name | 97.0% | -- | OK |
| accommodation | check_in | 38.1% | >50% | IMPROVED (was 31.6%) |
| accommodation | check_out | 42.5% | >50% | IMPROVED (was 26.3%) |
| accommodation | confirmation_code | 99.3% | -- | OK |
| car_rental | company | 100.0% | -- | OK |
| car_rental | confirmation_code | 100.0% | -- | OK |
| car_rental | pickup_at | 33.3% | >30% | **PASS** (was 5.3%) |
| flight | origin_airport | 100.0% | valid IATA | **PASS** |
| flight | destination_airport | 100.0% | valid IATA | **PASS** |
| flight | confirmation_code | 100.0% | -- | OK |
| grocery_order | store | 100.0% | -- | OK |
| grocery_order | items | 0.0% | -- | (not targeted) |
| grocery_order | total | 100.0% | -- | OK |
| meal_order | restaurant | 66.6% | no URL noise | **PASS** (validate_field rejects garbage) |
| meal_order | items | 25.4% | >0% | **PASS** (was 0%) |
| meal_order | total | 100.0% | -- | OK |
| ride | pickup_location | 97.7% | weak <10% | **PASS** (2.3% missing) |
| ride | dropoff_location | 97.7% | weak <10% | **PASS** |
| ride | fare | 100.0% | -- | OK |
| shipment | tracking_number | 100.0% | -- | OK |
| shipment | carrier | 100.0% | -- | OK |

## v2vision gate scorecard

| Gate | Target | Before (pre-2.5) | After (post-2.5) | Result |
|------|--------|----------------:|------------------:|--------|
| meal_order items > 0% | >0% | 0.0% | 25.4% | **PASS** |
| meal_order restaurant no URL noise | 0% garbage | ~40% flagged | 0% (validate_field rejects) | **PASS** |
| accommodation check_in/check_out > 50% | >50% | 31.6% / 26.3% | 38.1% / 42.5% | NOT MET (accepted) |
| flight valid IATA airports | 100% | unknown (no validation) | 100% (IATA allowlist enforced) | **PASS** |
| car_rental pickup_at > 30% | >30% | 5.3% | 33.3% | **PASS** |
| ride weak pickup/dropoff < 10% | <10% | ~21% | 2.3% | **PASS** |

**5 of 6 gates pass.** Accommodation check_in/check_out improved significantly but remains
below the 50% target. Many Airbnb emails in the vault are thread replies, inquiry messages,
and host conversations without date information -- deterministic parsing cannot extract dates
that aren't present in the email body or subject.

## Ground truth verification

| Provider | Holdout emails | Card emission | Field precision |
|----------|---------------:|--------------:|-----------------|
| DoorDash | 30 | 28/30 | restaurant 22/22, total 22/22, service 22/22 |
| Airbnb | 30 | 30/30 | booking_source 22/22, confirmation_code 17/21, property_name 9/21 |

## Key improvements in Phase 2.5

1. **Horizontal-run splitting** -- `clean_email_body()` produces single-line runs from HTML; `_split_horizontal_runs()` breaks them before parsing. This alone fixed items from 0% to 25.4%.
2. **`1xItem Name` format** -- DoorDash 2019-2021 uses this; parser now handles it.
3. **Stacked item format** -- Uber Eats: qty, name, price each on separate lines after splitting.
4. **Subject/URL date extraction** -- Airbnb check-in/check-out from subject patterns and URL params.
5. **Prose date extraction** -- National `vehicle on December 25, 2025` and `PICK UP ... date`.
6. **Parenthetical airport codes** -- United `(EWR)` `(LAX)` without `to`/`-` between.
7. **Charge summary totals** -- `Total  $18.65` (spaces, no colon) for Uber rides/Lyft.
8. **Lyft stacked addresses** -- time on pickup/drop-off line, address on next.
9. **DoorDash compact confirmations** -- restaurant from `credits {Name} Total:` line.
10. **UPS delivered-next-line** -- `Delivered\n{timestamp}` pattern.

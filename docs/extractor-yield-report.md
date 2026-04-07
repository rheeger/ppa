# Extractor yield report

Staging runs are **environment-specific**. To measure live yield against a seed vault:

```bash
export PPA_PATH=/path/to/seed/vault
ppa extract-emails --sender doordash --staging-dir _staging/ --workers 4 --log-file logs/extract-doordash.log
# Inspect _staging/, then delete when done (promotion to production vault is Phase 3).
```

Repeat per extractor (`uber_eats`, `uber_rides`, `amazon`, `instacart`, `shipping`, `lyft`, `united`, `airbnb`, `rental_cars`, `doordash`).

## Implemented extractors (Tier 1–3)

| Extractor id  | Card type       | Notes                                                                   |
| ------------- | --------------- | ----------------------------------------------------------------------- |
| `doordash`    | `meal_order`    | Plaintext line items + totals; summary fallback when structure missing. |
| `uber_eats`   | `meal_order`    | Registered before `uber_rides` for `uber.com` disambiguation.           |
| `uber_rides`  | `ride`          | Subject excludes “Uber Eats”.                                           |
| `amazon`      | `purchase`      | Tight subject gate (`order`/`shipment`/`delivery` + `confirm`).         |
| `instacart`   | `grocery_order` | Store + totals.                                                         |
| `shipping`    | `shipment`      | UPS/FedEx/USPS + Amazon ship subjects only for `@amazon.com`.           |
| `lyft`        | `ride`          | Receipt / ride subjects.                                                |
| `united`      | `flight`        | Confirmation + `SFO to LAX`-style route.                                |
| `airbnb`      | `accommodation` | Confirmation + trip/property lines.                                     |
| `rental_cars` | `car_rental`    | National / Hertz / Emerald Club senders.                                |

**CI / synthetic validation:** `pytest tests/archive_sync/extractors/` covers parsers, runner, registry, entity resolution, and end-to-end staging + idempotency (no live vault required).

## Tier 4–6 — deferred (post–Phase 2)

These are **not implemented** in this phase to keep scope bounded; each has high template churn or duplicate coverage with existing meal/ride extractors. Revisit after production yield data.

| Module (planned)                            | Rationale                                                                                                      |
| ------------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `postmates`, `caviar`, `grubhub`            | Same `meal_order` shape as DoorDash/Uber Eats; add when merchant volume justifies separate parsers.            |
| `micromobility` (Lime/Bird/Scoot/Citi Bike) | Low structured plaintext quality in vault samples; ride card `ride_type` scooter/bike needs template research. |
| `delta`, `jetblue`, `hawaiian`              | Clone of `united.py` pattern per airline; defer until travel cluster volume validated.                         |
| `booking_aggregators`, `booking_hotels`     | Highly variable HTML-stripped bodies; better after Phase 3 staging metrics.                                    |
| `retail` (multi-merchant)                   | Config-driven regex approach; start once top merchants are ranked from production matches.                     |
| `subscription_lifecycle`                    | 30+ services, lifecycle vs marketing email disambiguation; expect \<10% first-pass yield.                      |
| `tickets` (event_ticket)                    | Venue/ticket vendors fragmented; add incrementally.                                                            |
| `payroll`                                   | Sensitive + heterogeneous formats; explicit scope per employer.                                                |

When an extractor is added, register it in `build_default_registry()` (watch order for overlapping domains) and extend this document with matched / extracted / fallback counts from a real `--staging-dir` run.

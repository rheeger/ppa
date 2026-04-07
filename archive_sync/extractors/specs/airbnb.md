# Airbnb Extractor Spec

## Template sampler evidence (seed)

`archive_sync/extractors/specs/samples_seed/airbnb/`. Regenerate: `make step-11a-template-samplers-seed`.

## Email Taxonomy

193 `@airbnb.com` emails in 10% slice (census-style scan). Mix of reservation confirmations, receipts, host/guest messages, and marketing; confirmations use `reject_subject_patterns` and missing confirmation code to skip non-actionable mail.

| Category                | Typical subject                             | Extractable?                                     |
| ----------------------- | ------------------------------------------- | ------------------------------------------------ |
| Reservation / itinerary | "Reservation at …", RE: reservation threads | YES if numeric/alphanumeric confirmation in body |
| Receipt only            | "Your receipt from Airbnb"                  | Often NO if no reservation block parsed          |
| Social / tips           | "Travel tip", "Thinking of …"               | NO                                               |

## Deduplication Strategy

- **Discriminator:** confirmation code (prefer long numeric token in body, then `confirmation`/`record locator` regex, then uppercase alnum fallback).

## Template Eras

### Era `default`: 2000–2099

- **Property:** Prefer subject after em dash / en dash / hyphen (segment after first separator when not generic); else body "reservation at …" style patterns.
- **Dates:** `check-in` / `check-out` line regex (first line segment).
- **Total:** First `Total` money line when present.

## Holdout Ground Truth

- **Path:** `specs/airbnb-ground-truth.json`
- **Count:** 30 holdouts (22 expected card, 8 expected no card), bootstrapped with `scripts/build_ground_truth_holdouts.py` on `.slices/10pct`.

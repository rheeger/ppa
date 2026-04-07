# Amazon purchase (order confirmation)

## Template sampler evidence (seed)

`archive_sync/extractors/specs/samples_seed/amazon/` (ship-track emails: see Shipping spec + `samples_seed/` UPS/FedEx). Regenerate: `make step-11a-template-samplers-seed`.

## Phase 2.5 notes

- **Order id:** Parsed from body `order #…` or standard `###-#######-#######` in subject/body when body lines are sparse.
- **Shipment vs purchase:** Registry order + `ShippingExtractor.matches` for `@amazon.com` ship subjects — do not overlap “shipment confirmation” with this extractor’s subject gate.
- **Rejects:** `reject_subject_patterns` screens “rate/review your…” style mail that still mentions totals.

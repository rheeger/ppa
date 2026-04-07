# Shipping extractor (UPS / FedEx / USPS / Amazon ship-track)

## Template sampler evidence (seed)

Carrier samples: `archive_sync/extractors/specs/samples_seed/ups/`, `samples_seed/fedex/` (USPS not in the Tier-1 batch JSON — add a job if needed). Regenerate: `make step-11a-template-samplers-seed`.

## Phase 2.5 notes

- **Tracking-first:** `_tracking()` prefers body patterns (1Z…, FedEx digits, TBA…, USPS) over sender domain alone.
- **Estimated delivery:** `_sanitize_estimated_delivery` drops relative phrases and carrier boilerplate (`limitations`, `based on the selected service`, etc.).
- **Amazon @amazon.com:** `matches()` requires ship/track/deliver in subject so purchase vs ship routing stays split with `AmazonExtractor`.

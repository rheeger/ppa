# Lyft ride receipts

## Template sampler evidence (seed)

`archive_sync/extractors/specs/samples_seed/lyft/` (`@lyftmail.com` batch). Regenerate: `make step-11a-template-samplers-seed`.

## Phase 2.5 notes

- **Shared parser:** Uses `ride_common.parse_ride_receipt_fields` (same Location:/address-line fix as Uber Rides).
- **Service:** Cards are tagged `service: Lyft`; `ride_type` defaults to `car`.

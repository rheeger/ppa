# Rental cars (National / Hertz / Emerald Club)

## Template sampler evidence (seed)

National-heavy batch: `archive_sync/extractors/specs/samples_seed/national/`. Regenerate: `make step-11a-template-samplers-seed`.

## Phase 2.5 notes

- **Confirmation:** Regex capture is validated with `validate_field("car_rental", "confirmation_code", …)` so tokens like `NUMBER` / blocklist words emit no card.
- **Locations:** `_trim_location_noise` removes “click below to cancel…” tails from pickup/return lines when html2text merges paragraphs.
- **Pickup times:** `pickup_at` / `dropoff_at` still depend on body labels; sparse templates may leave `pickup_at` empty (field validation).

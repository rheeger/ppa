# United Airlines (flight)

## Template sampler evidence (seed)

`archive_sync/extractors/specs/samples_seed/united/`. Regenerate: `make step-11a-template-samplers-seed`.

## Phase 2.5 notes

- **Route:** `_best_airport_route` scans all `XXX→YYY` matches and keeps the first pair where both codes pass `is_valid_iata_airport()` (avoids template text like SUB→TAL before real airport pairs).
- **Confirmation:** Six-character record locators; placeholders like `NUMBER` rejected.

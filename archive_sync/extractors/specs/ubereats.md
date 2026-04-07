# Uber Eats Extractor Spec (in progress — Phase 2.5)

## Template sampler evidence (seed)

`archive_sync/extractors/specs/samples_seed/ubereats/` (Uber rides: `samples_seed/uber_rides/`). Regenerate: `make step-11a-template-samplers-seed`.

## Status

Parser updated for **subject-first restaurant** (`Your Uber Eats order with {Name}`), **horizontal body trim** (cut before `Picked up from` / `Delivered to` / `Rate order`), and **Total Charged** totals. Line-item parsing for html2text table output remains a follow-up (see `docs/reports/extraction-quality/10pct.md` — `meal_order.items` still often empty).

## Taxonomy

Run census on `uber.com` with subject filter for Uber Eats (see `UberEatsExtractor.matches` — `ubereats@uber.com` or `@uber.com` + `Uber Eats` in subject).

## Template eras

Single `default` era until template-sampler shows structural breakpoints by year.

## Ground truth

Not yet added — add `ubereats-ground-truth.json` after census + template-sampler on `.slices/10pct`, same pattern as DoorDash.

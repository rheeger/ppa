# DoorDash Extractor Spec

## Template sampler evidence (seed)

Real bodies for Phase 11b/11c: `archive_sync/extractors/specs/samples_seed/doordash/`. Regenerate: `make step-11a-template-samplers-seed`.

## Email Taxonomy

See `doordash-census-smoke.md` (10% slice): ~475 `@doordash.com` emails; largest bucket is receipt/order confirmations; promos and account mail are rejected via `reject_subject_patterns` and `should_extract` receipt indicators.

| Category           | Subject pattern                                                   | Volume (10% est.) | Extractable? |
| ------------------ | ----------------------------------------------------------------- | ----------------- | ------------ |
| Receipt / order    | "Order Confirmation for … from {Restaurant}", "we got your order" | ~130              | YES          |
| Promo / DashPass   | "% off", "deals", "credit"                                        | ~82               | NO           |
| Account / security | "login", etc.                                                     | ~5                | NO           |
| Other / threads    | "Re: First Game"                                                  | ~256              | Usually NO   |

## Deduplication Strategy

- **Discriminator:** `_discriminator` = normalized restaurant key (subject/body-derived).
- **Primary source:** Order confirmation and receipt emails with itemized totals.

## Template Eras

### Era `plain`: 2000–2099

- **Subject:** Prefer `Order Confirmation for … from {Restaurant}` (`_restaurant_from_subject`); fallback body patterns for "order from" / "from …" before Subtotal; Prop 65 / URL lines skipped in body path.
- **Totals:** `Subtotal` / `Tax` / `Delivery Fee` / `Tip` / `Total` lines; **`Total Charged $X.XX`** parsed when `Total:` line is absent.
- **Items:** Table rows, `- Item x N $`, and similar plaintext lines (`_parse_items_block`).

## Holdout Ground Truth

- **Path:** `specs/doordash-ground-truth.json`
- **Count:** 30 holdouts (22 expected card, 8 expected no card), bootstrapped with `scripts/build_ground_truth_holdouts.py` on `.slices/10pct` — re-annotate fields if parser behavior intentionally changes.

## Validation Rules

- Restaurant must not resolve to generic/footer garbage (`_BAD_RESTAURANTS`, Prop 65 / URL guards in body extraction).

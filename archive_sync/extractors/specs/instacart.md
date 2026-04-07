# Instacart (grocery_order)

## Template sampler evidence (seed)

`archive_sync/extractors/specs/samples_seed/instacart/` (both `instacartemail.com` and `instacart.com` jobs land here). Regenerate: `make step-11a-template-samplers-seed`.

## Phase 2.5 notes

- **Store name:** Subject `Your order from {Store}…` first; then body `From:` line; then legacy “your order from” / “shopping at” patterns.
- **Items:** Still empty until table/html2text item parsing is improved (see slice quality reports).

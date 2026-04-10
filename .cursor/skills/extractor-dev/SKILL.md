---
name: extractor-dev
description: >-
  Build or rebuild an email extractor for a service provider using the five-phase
  Extractor Development Lifecycle (EDL). Use when creating a new extractor, upgrading
  an existing one, or when extraction quality reports show field population or yield
  problems for a specific extractor.
---

# Extractor Development Lifecycle (EDL)

## When to use this skill

- Building a new extractor for a provider not yet covered
- Rebuilding an existing extractor after quality report flags
- Expanding coverage to new template eras for an existing provider

## The five phases (sequential, gated)

### Phase 1: Census

Run `ppa sender-census --domain <domain>` to discover all email types from this sender.
Produce a taxonomy table classifying each email type as receipt/confirmation (extractable),
promo/marketing (reject), account/transactional (skip), or unknown.

**Gate:** At least 20 extractable emails exist. If fewer, document "insufficient volume" and stop.

**Deduplication planning:** Identify which email types represent the same transaction (e.g.,
DoorDash sends "Order confirmed", "On the way", "Delivered" for one order). Document which
type is the primary extraction target and which should be skipped or merged via discriminator.

See [phases.md](phases.md) Phase 1 for detailed instructions.

### Phase 2: Template Sampling -- THE critical step

**This is the most important phase.** Every parser bug we've found traces back to writing
regex against imagined email formats instead of reading real `clean_email_body()` output.
The template sampler grounds every decision in what extractors actually see.

**Run template sampling with enough breadth that every era is represented.** Prefer the **full seed** batch (one walk, all providers, high `--per-year`):

```bash
# from ppa/ -- writes specs/samples_seed/<provider>/
make step-11a-template-samplers-seed STEP_11A_PER_YEAR_SEED=15
```

For a **quick** pass on the 10% slice only:

```bash
ppa template-sampler --domain <domain> --category <keyword> --per-year 5 \
  --out-dir specs/samples/<provider> --vault .slices/10pct
```

**What to do with the output (mandatory, not optional):**

1. **Run `clean_email_body()` on the `.raw.txt` files** to see what your parser's `body`
   argument actually contains at runtime. The `.clean.txt` is the vault body as-is and may
   still be HTML -- it is NOT the post-processed output. See "Sample discovery method" below.
2. **For each target field** (restaurant, total, items, dates, confirmation, locations):
   find the **exact text** surrounding it in the **cleaned** output. Copy the 3-5 lines around it.
   This becomes your regex anchor and boundary.
3. **Document structural eras** -- when the layout changes between years, that's a template
   era boundary. Note what changed: labels, table vs plain, footer additions, line merging.
4. **For line items specifically:** look at how `html2text` renders the item table. It is
   almost never `- Item x2 $12.00`. It's usually multi-line (`1xItem Name\n$price\n`) or
   a pipe-delimited table or a single horizontal run. Write the regex for what you see.
5. **Check for horizontal runs** -- `html2text` frequently collapses entire receipts into
   one line. Apply `_split_horizontal_runs()` before parsing. See lessons below.
6. **Save / cite the sampler output** under **`specs/samples_seed/<provider>/`** after a seed run
   (primary for quality coverage). **`specs/samples/<provider>/`** is for slice smoke only.

**Gate:** Samples saved for every year with extractable emails. Each target field
has a documented anchor/boundary from the actual cleaned output. Era map with date ranges.

See [phases.md](phases.md) Phase 2 for detailed instructions.

### Phase 3: Anchor and Field Mapping + Ground Truth

**Build directly from Phase 2 sampler output** -- not from memory, not from one email you
glanced at. For each era, examine 10-15 real emails (the "development set" -- 70% of
extractable emails). Map each target field using the cleaned sampler output:

- **Anchor:** The exact preceding text from the cleaned output (copy-paste from sampler files).
- **Boundary:** The exact following text or pattern that ends the field value.
- **Format:** The exact shape observed across multiple sampler years.
- **False positives:** Strings from OTHER sampler files that match the pattern but are wrong.
- **Validation:** Rules from `field-validation.md` plus provider-specific checks.

Produce the extractor spec document.

**Hold back 30% of extractable emails as the "holdout set" -- do NOT examine these during
development.** Write ground truth annotations for 20-30 holdout emails: the correct field
values that the extractor should produce. Store as `specs/<provider>-ground-truth.json`.

**Gate:** Spec written with field anchors traced to specific sampler files.
Ground truth file written with >=20 annotated emails.

See [phases.md](phases.md) Phase 3 for details and [spec-template.md](spec-template.md).

### Phase 4: Implementation

Write the extractor **from the spec and sampler output** -- not from imagination. Key requirements:

- Each template era maps to a `TemplateVersion` with correct date range
- **Apply `_split_horizontal_runs()`** to the body before item/total parsing for HTML providers
- **Fixtures use real email shapes** from sampler output (PII redacted, structure intact).
  Never invent a plaintext fixture from imagination.
- At least 1 fixture per template era + 1 promo/reject fixture + 1 edge case
- All fixtures tagged with which fields they test (coverage matrix in spec)
- Negative fixtures assert `len(results) == 0`
- All tests assert concrete field values, not just `len(results) == 1`
- Inline field validation via `field_validation.py` rejects garbage values
- Source round-trip validation confirms extracted values exist in source email
- **Per-provider extraction smoke:** After implementation, run the extractor against ALL
  its matched emails in the 10% slice. Print yield and flag summary. If yield is below
  30% for a receipt-type extractor, go back to the sampler output and find what's failing.

**Gate:** Unit tests pass against sampler-derived fixtures. Per-provider smoke yield is
reasonable. Fixture coverage matrix shows every critical field tested by at least 2 fixtures.

### Phase 5: Verification

Run the extractor against the **holdout set** (the 30% of emails not used during development).
Compare output against ground truth annotations. Compute precision and recall per field.

Also run against the 10% slice with **staging extract** (not dry-run) and check
`field_population` in `_metrics.json`. Compare to baseline quality gates.

**Gate:**

- Holdout precision >= 80% per critical field (extracted value matches ground truth)
- Holdout recall >= 50% per critical field (field populated when ground truth has a value)
- `field_population` rates meet v2vision targets for the card type
- Zero critical-flag garbage values in quality report

See [phases.md](phases.md) Phase 5 for detailed instructions.

## Quick reference: commands

- `ppa sender-census --domain <domain> [--sample N] [--out <path>] [--vault <path>]`
- `ppa template-sampler --domain <domain> --category <type> [--per-year N] [--out-dir <path>] [--vault <path>]`
- **Yield (dry-run, fast):** `make step-11d-slice-yield-report`
- **Staging extract (real cards + field_population):** `make extract-emails-10pct-slice`
- **Quality reports:** `make extraction-quality-reports`
- **Ground truth:** `python scripts/verify_ground_truth.py --ground-truth <json> --vault-path <vault>`

### Template sampler -- recommended invocations

**Preferred (breadth):** one batch on full seed:

```bash
make step-11a-template-samplers-seed STEP_11A_PER_YEAR_SEED=15
```

**Per-domain smoke (10% slice):**

```bash
ppa template-sampler --domain doordash.com --category order --per-year 5 \
  --out-dir archive_sync/extractors/specs/samples/doordash --vault .slices/10pct
```

## Sample discovery method (critical -- read this)

The **correct** workflow for discovering what your parser sees at runtime:

1. Read `specs/samples_seed/<provider>/<year>/<uid>.raw.txt` -- this is the vault body (often HTML).
2. Run `clean_email_body()` on it -- this is what the extractor parser's `body` arg contains.
3. Search the **cleaned** output for your target field values.
4. Write regex against that cleaned text.

**WARNING:** The `.clean.txt` in `samples_seed/` is the vault body as-is. For HTML emails,
it IS the HTML. It is **NOT** the output of `clean_email_body()`. Regex written against
`.clean.txt` will fail at runtime if the email is HTML.

Quick test:

```python
from archive_sync.extractors.preprocessing import clean_email_body
import pathlib
raw = pathlib.Path("specs/samples_seed/<provider>/<year>/<uid>.raw.txt").read_text()
cleaned = clean_email_body(raw)
# Search `cleaned` for your fields, write regex against this text
```

## Proof-case lessons (Phase 2.5 -- all providers)

These came from running the full EDL on real slice data. Fold them into new extractors.

### The #1 discovery: horizontal runs destroy line-by-line parsing

`clean_email_body()` uses `html2text` on HTML emails. For many providers (Uber Eats,
DoorDash, Lyft, Airbnb), html2text collapses entire receipt tables into **single lines**
with `\xa0` (non-breaking spaces) and long runs of regular spaces as separators:

```
Total  $30.23   1   ORCHARD      $16.62   1   DESIGN YOUR OWN SALAD      $12.59   Subtotal  $29.21
```

Any regex that operates line by line will see this as one giant line and fail. The fix is a
**horizontal-run splitter** that converts `\xa0` to spaces and breaks `3+` consecutive
spaces into newlines before regex parsing. Both `doordash.py` and `ubereats.py` implement
`_split_horizontal_runs()`.

**Rule:** Every new extractor should apply `_split_horizontal_runs()` to the body before
parsing items or totals if the provider's emails are HTML-rendered.

### Item parsing: three real formats

1. **Stacked (post-split):** qty on one line, name on next, price on next -- Uber Eats
2. **`NxItem Name (Category)` + price on next line** -- older DoorDash (2019-2021)
3. **`N   Item Name   $price`** on one line (after horizontal split) -- Uber Eats, DoorDash

Never assume `- Item x2 $12.00`. Always check sampler output.

### Date extraction: subjects and URLs carry critical data

- **Airbnb:** `for Jun 11 - 23, 2021` in subject; `check_in=2026-01-04` in URL params;
  `Sunday Jul 26, 2020 - Friday Aug 07, 2020` in body. Body `check-in:` labels are rare.
- **National:** `vehicle on December 25, 2025` in prose; `PICK UP ... Thu, December 25, 2025`.
- **United:** airport codes in `(EWR)` parentheticals; calendar uses `Depart: EWR -`.

### Provider-specific patterns

- **Frontmatter field is `from_email`, not `from`.** The runner passes `from_email` into `matches()`.
- **Prefer subject before body for merchant names.** DoorDash, Uber Eats, Airbnb all have more reliable names in the subject.
- **Totals:** `Total  $X.XX` (spaces, no colon), `Total Charged`, `Estimated Total` + amount on next line, `Order Totals: 184.53` (no `$`). Parse all variants.
- **DoorDash compact (2024+):** No `Order from`; restaurant only in `credits {Name} Total: $XX.XX`.
- **Lyft:** No `Total:` label; last `$XX.XX` before `You've already paid` is the charge. Pickup/drop-off with time on one line, address on next.
- **Uber charge summaries:** `Total  $18.65` (spaces, no colon, mid-line).
- **UPS delivered:** `Delivered` on its own line, timestamp on next line.
- **Instacart:** `Order Totals: 184.53` (no `$`); store from `Your order from {Store} was delivered`; multiline `Total\n$107.79`.

### Measurement: yield vs field population

- **`make step-11d-slice-yield-report`** = dry-run, funnel health (matched vs extracted count). Fast (~13min) but does NOT measure field quality.
- **`make extract-emails-10pct-slice`** = real staging extract that writes cards AND computes `field_population` in `_artifacts/_staging-10pct/_metrics.json`. This measures the v2vision quality gates.
- **Always run the staging extract** before declaring a provider done -- yield can be 100% while critical fields are empty.
- The `field_population` block maps `{card_type: {field: rate}}` using critical-field definitions in `field_metrics.py`.

## Anti-patterns

- **Skipping the template sampler** -- the #1 cause of bad extractors.
- **Writing regex against `.clean.txt` directly** -- run `clean_email_body()` on `.raw.txt` to see what the parser receives. `.clean.txt` is the vault body, not post-processed output.
- **Line-by-line parsing without horizontal-run splitting** -- HTML emails produce single-line runs. Use `_split_horizontal_runs()`.
- **Inventing fixture bodies** -- fixtures must use real email shapes from sampler output.
- **Assuming item format is `- Item x2 $12.00`** -- it's almost always stacked, `NxName`, or horizontal.
- **Measuring only yield (11d) without field_population** -- cards can be emitted with total=0 and items=[]; run the staging extract.
- Emitting a card with garbage fields (use field validation to return None/empty)
- Testing only `len(results) == 1` (assert specific field values)
- Examining holdout emails during development
- Using the same discriminator for different transactions from the same sender
- Extracting from multiple email types for the same transaction without dedup strategy
- **Not checking per-provider yield** -- if below 30% for receipts, something is wrong

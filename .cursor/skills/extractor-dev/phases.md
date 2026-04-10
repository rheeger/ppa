# EDL phases — detailed playbook

## Phase 1: Census

### Command

```bash
ppa sender-census --domain <domain> [--sample 100] [--out path/to/census.md] [--vault $PPA_PATH]
```

Uses `PPA_PATH` (or `--vault`) as the vault root. Scans all `email_message` cards whose `from_email` domain matches the given domain (subdomains included: `doordash.com` matches `noreply@messages.doordash.com`).

**Performance:** Census reads only notes under **`Email/`** (not the whole vault). On a large slice it can still take several minutes (one file read per email). For smoke tests, point `--vault` at a **stratified slice** (e.g. `ppa/.slices/10pct`) instead of the full seed.

### Reading the taxonomy table

- **Receipt / order** — subjects with order/receipt/confirmation language; usually **extractable** for transaction extractors.
- **Delivery / shipping** — "on the way", "delivered"; often **skip** or **merge** (same order as receipt) depending on dedup strategy.
- **Promotion** — discounts, DashPass promos; **reject** (no card or `should_extract` false).
- **Account** — password, verify, payment method; **skip**.
- **Other** — classify manually; may be **unknown** until sampled.

### Extractable vs reject vs skip

- **Extractable:** structured transaction data you want as a card (order total, itinerary, ride fare).
- **Reject:** marketing; extractor returns zero cards (`should_extract` or parser empty).
- **Skip:** operational email with no new transaction facts; optionally still matched by sender but produces no card.

### Deduplication planning

1. List email types that refer to the **same** transaction (e.g. order confirmation + out-for-delivery + delivered).
2. Pick a **primary** type that has the most complete fields (often the receipt or confirmation).
3. Choose a **discriminator** stable across types: order id, confirmation code, or hash of (provider + date + amount).
4. Document **merge** vs **skip**: e.g. "only extract from subject line containing Your order from".

### Example: DoorDash taxonomy (illustrative)

| Category   | Examples                      | Extractable?                  |
| ---------- | ----------------------------- | ----------------------------- |
| Receipt    | "Your order from Joe's Diner" | YES — primary                 |
| In transit | "Your order is on the way"    | NO (skip; duplicate of order) |
| Promo      | "50% off"                     | NO                            |

---

## Phase 2: Template Sampling — the quality foundation

**This phase is where extraction quality is won or lost.** Every regex you write in Phase 4
must trace back to patterns you observed in sampler `.clean.txt` output. Skip this or rush it,
and you'll write parsers against imagined formats that fail on real email bodies.

### Command

**Breadth (recommended for parser quality and Phase 11b–11c):** run the **seed** batch once — one `Email/` walk, all Tier 1–3 jobs, high `--per-year`:

```bash
# from ppa/
make step-11a-template-samplers-seed STEP_11A_PER_YEAR_SEED=15
# → writes archive_sync/extractors/specs/samples_seed/<provider>/
```

**Fast smoke (10% slice):** quick iteration or CI-sized checks:

```bash
ppa template-sampler --domain <domain> --category <keyword> --per-year 5 \
  --out-dir archive_sync/extractors/specs/samples/<provider> --vault .slices/10pct
```

- **`--per-year`:** minimum **5** on slice; on seed runs use **12–15+** when you need dense per-year coverage.
- **`--category`** filters subjects (case-insensitive). Batch jobs are defined in `scripts/step_11a_template_sampler_jobs.json`.
- **Primary evidence for specs and fixtures:** **`specs/samples_seed/<provider>/`** (full seed). Use **`specs/samples/`** for slice smoke only.

### Understanding `.clean.txt` vs `.raw.txt` vs `clean_email_body()`

**CRITICAL DISTINCTION (learned the hard way in Phase 2.5):**

- **`.raw.txt`** = vault markdown body. For HTML emails, this IS the HTML source.
- **`.clean.txt`** = the vault body as-is. For HTML emails, this is ALSO the HTML. It is
  **NOT** the output of `clean_email_body()`.
- **`clean_email_body(raw_txt)`** = what your parser's `body` argument actually contains
  at runtime. The runner calls this on every email before passing the body to `extract()`.

**The `.clean.txt` is not what your parser sees.** To discover the real parser input, run:

```python
from archive_sync.extractors.preprocessing import clean_email_body
import pathlib
raw = pathlib.Path("specs/samples_seed/<provider>/<year>/<uid>.raw.txt").read_text()
cleaned = clean_email_body(raw)
# Write regex against `cleaned`, not against .clean.txt
```

- **`.meta.json`** = frontmatter fields (`uid`, `subject`, `from_email`, `from_name`, `sent_at`, plus `subject_category` / `subject_shape` snapshots when using current tooling).

### Mandatory reading protocol

For **each** sampler file, you must:

1. **Open the `.clean.txt`** and find each target field's value in the text.
2. **Record the exact anchor text** (the 3-10 words before the field value).
3. **Record the exact boundary** (what ends the field: newline, next label, `\n\n`, etc.).
4. **Note the line structure:** Is the value on the same line as the label? Next line?
   Multiple lines later? Across a horizontal run without line breaks?
5. **Note false positive risk:** What other text in the body matches a similar pattern?

### Field extraction worksheet (do this per field per era)

For every target field, fill out this mentally or in the spec:

```
Field: restaurant
Era: 2020-2024
Anchor: "Your order from " (body) OR "Order Confirmation for {Name} from " (subject)
Boundary: first newline after anchor
Value format: plain text, < 120 chars
False positives: "WARNING <https://www.p65warnings.ca.gov/...>" (Prop 65 footer)
Sampler evidence: specs/samples_seed/doordash/2021/hfa-email-message-xxxx.clean.txt line 14
```

```
Field: items (meal_order)
Era: 2020-2024
Anchor: none (multi-line block between address and Subtotal)
Structure: "1xItem Name (Category)\n• Modifier\n\n$XX.XX\n" repeating
Boundary: "Subtotal" line
Sampler evidence: specs/samples_seed/doordash/2020/hfa-email-message-b433ea23c991.clean.txt lines 28-42
```

### Structural breakpoints — what triggers a new era

Document a new **era** when you see:

- Table layout vs plain paragraphs for line items
- Label changes ("Subtotal" vs "Order summary")
- New sections (fees, Prop 65 footer, legal blocks) that break naive regexes
- Subject line format changes
- Body rendering changes (single-line horizontal runs vs multi-line)

### Documenting eras in the spec

For each era: date range (inclusive), short name, 2–3 example UIDs from sampler output,
bullet list of what changed vs the previous era, and **file paths to representative
`.clean.txt` samples**.

### Horizontal-run splitting (required for HTML providers)

`clean_email_body()` uses `html2text` which frequently collapses entire HTML tables into
a single line with `\xa0` and long space runs as separators:

```
Total  $30.23   1   ORCHARD      $16.62   1   DESIGN YOUR OWN SALAD      $12.59   Subtotal  $29.21
```

**Before any line-by-line parsing**, apply a horizontal-run splitter:

```python
def _split_horizontal_runs(body: str) -> str:
    body = body.replace("\xa0", " ")
    body = re.sub(r" {3,}", "\n", body)
    return body
```

Both `doordash.py` and `ubereats.py` implement this. Every new extractor for HTML
providers should do the same.

### Item parsing — special attention required

`meal_order.items` was 0% populated in the baseline quality report. The reason: every item
parser expected `- Item x2 $12.00` on one line but `html2text` produces horizontal runs or
stacked formats. Real patterns after horizontal-run splitting:

**Format 1 -- Stacked (Uber Eats):** qty, name, and price each on separate lines:

```
1
Pesto Parmesan Zoodles
$16.25
1
Alfredo Zoodles
$17.25
```

**Format 2 -- `NxName` (DoorDash 2019-2021):**

```
1xChicken Shish Tawook Plate (Proteins)

$12.00
```

**Format 3 -- Same-line after split (Uber Eats, DoorDash):**

```
1   ORCHARD      $16.62
```

**You must look at actual `clean_email_body()` output per provider to see which format
applies.** Then write the regex for what you actually see, not what you imagine.

### Saving samples (required)

**Quality work (Phase 11b–11c):** treat **`archive_sync/extractors/specs/samples_seed/<provider>/`**
as the canonical tree after a full-seed batch run. **Slice smoke:** `specs/samples/<provider>/`.
Structure (same for both roots):

```
specs/samples_seed/doordash/
  2020/
    hfa-email-message-xxxx.raw.txt
    hfa-email-message-xxxx.clean.txt
    hfa-email-message-xxxx.meta.json
  2021/
    ...
```

See `specs/samples_seed/README.md`.

---

## Phase 3: Anchor and Field Mapping + Ground Truth

### 70/30 split (deterministic)

Use `hash(uid) % 10 < 7` → **development**; else **holdout**. Implement in scripts or notebooks; do not open holdout emails while writing parsers.

### Per-field record

For each target field:

- **Anchor:** stable preceding text (or subject pattern).
- **Boundary:** newline, next label, or max length.
- **Format:** regex or examples.
- **False positives:** strings that match but are wrong (footer text, CSS).
- **Validation:** rules from `field-validation.md` plus provider-specific checks.

### Bootstrap vs true holdouts

- **`scripts/build_ground_truth_holdouts.py`** writes `specs/<provider>-ground-truth.json` by running the **current** extractor on a vault slice. Use it for **regression** (parser must not drift) and CI.
- **Strict EDL holdouts:** Do not inspect holdout emails while writing parsers. Prefer a deterministic split (e.g. `hash(uid) % 10 >= 7` → holdout) and **hand-annotate** expected fields, or label a fresh random sample after the parser is frozen.
- **`verify_ground_truth.py`** compares JSON to live extraction; it uses one pass over `Email/` for UID index, then runs `extract()` per holdout.

### Ground truth JSON shape

```json
{
  "holdout_emails": [
    {
      "uid": "hfa-email-message-abc123",
      "expected_cards": [
        {
          "type": "meal_order",
          "fields": {
            "restaurant": "Mixt - Valencia",
            "total": 16.62,
            "items": [{ "name": "Chicken Bowl", "qty": 1, "price": "12.95" }],
            "service": "DoorDash"
          }
        }
      ]
    },
    {
      "uid": "hfa-email-message-def456",
      "expected_cards": []
    }
  ]
}
```

Include emails that should produce **zero** cards (promos, wrong type).

### Subject vs body vs raw HTML

Prefer **subject** when it contains stable identifiers (e.g. "Your order from X", DoorDash `Order Confirmation for … from {Restaurant}`, Uber Eats `Your Uber Eats order with {Restaurant}`). Use **body** for totals and line items; trim **horizontal-layout noise** (e.g. cut at `Picked up from` / `Delivered to` when html2text merges lines). Use **raw_body** only when HTML structure is required and `clean_email_body` destroys it.

### Testing extractors locally

Use `from_email` and `subject` in synthetic frontmatter — matching what `ExtractionRunner` supplies — not a generic `from` field.

---

## Phase 4: Implementation

### `template_versions()`

Return versions **newest first**. Each `TemplateVersion` has inclusive `date_range` `(start_iso, end_iso)` and a parser `parser(frontmatter, body)`.

### Fixtures — from sampler output, not imagination

- Minimum: 1 per era + 1 promo (0 cards) + 1 edge case.
- **Copy fixture bodies from sampler `.clean.txt` files.** Redact personal details but
  keep the structural shape intact — labels, whitespace, line breaks, table formatting.
- **`.expected.json`:** concrete field values for every populated field. Get these values
  by reading the `.clean.txt` file and the `.meta.json` (for subject).
- **Coverage matrix** in spec: table Fixture × field → YES/NO/N/A.

### Tests

- Assert field values, not only `len(results) == 1`.
- Negative fixtures: `len(results) == 0`.

### Per-provider extraction smoke (required before moving to Phase 5)

After writing the parser, run it against **all** matched emails in the 10% slice:

```python
# Quick script pattern — single-pass extraction yield check
for note in iter_email_message_notes(VAULT):
    fm = note.frontmatter
    if ext.matches(str(fm.get("from_email") or ""), str(fm.get("subject") or "")):
        body = clean_email_body(note.body)
        results = ext.extract(fm, body, uid, rel_path, raw_body=note.body)
        # count positive / negative / empty
```

If yield is below **30%** for a receipt-type extractor (DoorDash, Uber Eats, Airbnb, etc.):
go back to sampler output and find emails that matched but produced no card. Read their
`.clean.txt` to understand why the parser failed. Fix the regex. Repeat until yield is
reasonable.

---

## Phase 5: Verification

### Holdout run

Run `scripts/verify_ground_truth.py` against the holdout JSON and vault.

### Precision / recall (per field F)

- **Precision** = (matches where extracted F equals ground truth F) / (cards where F was populated).
- **Recall** = (matches) / (ground truth rows where F has a value).
- **Match:** normalize text (lower, strip, collapse space); money within $0.01.

### Slice quality and field population

Run a **staging extract** (not dry-run) to get field population rates:

```bash
make extract-emails-10pct-slice
# Check _artifacts/_staging-10pct/_metrics.json -> field_population block
```

The `field_population` section in `_metrics.json` reports the fraction of extracted cards
where each critical field is populated. Critical fields are defined in
`archive_sync/extractors/field_metrics.py` (`CRITICAL_FIELDS` dict).

**This is different from yield.** `make step-11d-slice-yield-report` measures how many
matched emails produce a card (funnel health). `field_population` measures whether the
emitted cards have their critical fields filled (quality). A provider can have 100% yield
and 0% items population.

Optionally generate markdown quality reports:

```bash
make extraction-quality-reports
```

### Pass criteria (defaults)

- Holdout precision >= 80% on agreed critical fields.
- Holdout recall >= 50% on critical fields.
- `field_population` rates meet v2vision targets for the card type.
- Slice report: no systematic heuristic flags for known failure modes.

### v2vision quality gates (Phase 2.5 reference)

| Card type     | Field                      | Target                |
| ------------- | -------------------------- | --------------------- |
| meal_order    | items                      | > 0%                  |
| meal_order    | restaurant                 | zero URL/footer noise |
| accommodation | check_in/check_out         | > 50% with real dates |
| flight        | origin/destination_airport | valid IATA codes      |
| car_rental    | pickup_at                  | > 30%                 |
| ride          | pickup/dropoff_location    | weak < 10%            |

# [Provider] Extractor Spec

## Email Taxonomy

| Category | Subject pattern | Volume | Extractable? | Dedup role                  |
| -------- | --------------- | ------ | ------------ | --------------------------- |
| ...      | ...             | ...    | ...          | primary / skip / merge-by-X |

## Deduplication Strategy

- **Discriminator field:** [e.g., order number, confirmation code]
- **Multiple emails per transaction:** [list which email types map to the same transaction]
- **Primary extraction source:** [which email type to extract from]

## Template Sampler Output

- **Sampler directory (quality / seed):** `specs/samples_seed/<provider>/` — primary for anchors and fixtures after `make step-11a-template-samplers-seed`.
- **Sampler directory (slice smoke):** `specs/samples/<provider>/` — optional; from `make step-11a-template-samplers` or manual `--vault .slices/10pct`.
- **Command used:** [e.g. seed batch `make step-11a-template-samplers-seed` or per-domain `ppa template-sampler … --vault <path>`]
- **Years covered:** [YYYY–YYYY]
- **Files per year:** [N] `.clean.txt` / `.raw.txt` / `.meta.json` each

## Template Eras

### Era N: YYYY-YYYY ("description")

- Structure: [HTML tables / plaintext / mixed]
- Key structural features: [what makes this era distinct]
- Example emails (development set): [uid1], [uid2], [uid3]
- **Representative sampler file:** `specs/samples_seed/<provider>/YYYY/<uid>.clean.txt` (or `samples/` if seed not run)

## Field Extraction — Era N

### field_name

- **Primary source**: [subject line / body text / raw HTML]
- **Anchor**: [stable text preceding the field — copy from `.clean.txt`]
- **Boundary**: [what marks the end of the field value — copy from `.clean.txt`]
- **Format**: [expected value format, e.g., "$XX.XX", 6-char alphanumeric]
- **False positives**: [patterns that look like this field but aren't — found in other `.clean.txt` files]
- **Validation**: [sanity checks — length, content, format]
- **Round-trip expectation**: [should the extracted value appear verbatim in the source email? yes/no/normalized]
- **Sampler evidence**: [path to `.clean.txt` file + line range where this field appears]

## Known False Positives

- [Document specific noise patterns discovered during field mapping]

## Validation Rules (provider-specific)

- [Per-field validation rules beyond the shared rules in field-validation.md]

## Fixture Coverage Matrix

Fixtures are bodies from sampler `.clean.txt` files (PII-redacted, structure intact).

| Fixture         | Source sampler file                                  | field1        | field2 | field3 | ... |
| --------------- | ---------------------------------------------------- | ------------- | ------ | ------ | --- |
| era1_receipt.md | `specs/samples_seed/<provider>/YYYY/<uid>.clean.txt` | YES           | YES    | YES    | ... |
| era2_receipt.md | `specs/samples_seed/<provider>/YYYY/<uid>.clean.txt` | YES           | NO     | YES    | ... |
| promo_reject.md | `specs/samples_seed/<provider>/YYYY/<uid>.clean.txt` | N/A (0 cards) |        |        |     |

## Holdout Ground Truth

Path: `specs/<provider>-ground-truth.json`
Count: [N] annotated emails ([M] expected to produce cards, [K] expected to produce 0 cards)

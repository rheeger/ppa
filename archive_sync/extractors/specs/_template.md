# [Provider] Extractor Spec

## Email Taxonomy
| Category | Subject pattern | Volume | Extractable? | Dedup role |
|----------|----------------|--------|-------------|------------|
| ... | ... | ... | ... | primary / skip / merge-by-X |

## Deduplication Strategy
- **Discriminator field:** [e.g., order number, confirmation code]
- **Multiple emails per transaction:** [list which email types map to the same transaction]
- **Primary extraction source:** [which email type to extract from]

## Template Eras
### Era N: YYYY-YYYY ("description")
- Structure: [HTML tables / plaintext / mixed]
- Key structural features: [what makes this era distinct]
- Example emails (development set): [uid1], [uid2], [uid3]

## Field Extraction — Era N
### field_name
- **Primary source**: [subject line / body text / raw HTML]
- **Anchor**: [stable text preceding the field]
- **Boundary**: [what marks the end of the field value]
- **Format**: [expected value format, e.g., "$XX.XX", 6-char alphanumeric]
- **False positives**: [patterns that look like this field but aren't]
- **Validation**: [sanity checks — length, content, format]
- **Round-trip expectation**: [should the extracted value appear verbatim in the source email? yes/no/normalized]

## Known False Positives
- [Document specific noise patterns discovered during field mapping]

## Validation Rules (provider-specific)
- [Per-field validation rules beyond the shared rules in field-validation.md]

## Fixture Coverage Matrix
| Fixture | field1 | field2 | field3 | ... |
|---------|--------|--------|--------|-----|
| era1_receipt.md | YES | YES | YES | ... |
| era2_receipt.md | YES | NO | YES | ... |
| promo_reject.md | N/A (0 cards) | | | |

## Holdout Ground Truth
Path: `specs/<provider>-ground-truth.json`
Count: [N] annotated emails ([M] expected to produce cards, [K] expected to produce 0 cards)

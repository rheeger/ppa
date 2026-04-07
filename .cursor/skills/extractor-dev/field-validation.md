# Shared field validation rules

All extractors use `archive_sync/extractors/field_validation.py` via `validate_field()` in `_instantiate_card()`. Rules below specify: field name(s), card type(s), the check, and failure behavior (omit field with `None`, or reject entire card — currently we omit fields only).

## Field-level rules (return None to omit the field)

- **`restaurant`** (`meal_order`, `grocery_order` store uses similar rules): reject if contains URL (`http`), CSS class (`.eats_`), >120 chars, or known footer phrases ("bank statement", "Rate order", "Learn More", ".eats_footer", "Contact support", "WARNING")
- **`confirmation_code`** (`flight`): reject if not 6-char alphanumeric, or if in English blocklist
- **`confirmation_code`** (`car_rental`, `accommodation`): reject if in English blocklist (THAT, CONFIRMATION, VACATION, PEACEFUL, EXPECTED, NUMBER, etc.)
- **`origin_airport` / `destination_airport`** (`flight`): reject if not in IATA code set (~500 codes: US + major international)
- **`fare_amount`** (`flight`): reject if > $50,000 (likely miles, not dollars)
- **`fare`** (`ride`): reject if > $500 (implausible for a single ride)
- **`total`** (`meal_order`, `grocery_order`): reject if > $2,000 (implausible for a single order)
- **`property_name`** (`accommodation`): reject if >200 chars, contains URL, or contains review-text indicators ("would absolutely", "left clean", "great condition", "highly recommend")
- **`pickup_location` / `dropoff_location`** (`ride`, `car_rental`): reject if bare "Location:" with no following address, or <4 chars, or >300 chars (paragraph grabbed instead of address)
- **`check_in` / `check_out`** (`accommodation`): reject if no date-like content (no digits and no month names), contains "flexible", "as soon as possible", or is a single word like "Checkout"
- **`pickup_at`** (`car_rental`): reject if no date-like content
- **`delivered_at` / `estimated_delivery`** (`shipment`): reject if contains "based on the selected service", "click", or "Limitations and ex"
- **`departure_at` / `arrival_at`** (`flight`): reject if contains "City and Time", "Cabin", "Arrive" as the entire value, or >100 chars
- **`store`** (`grocery_order`): reject if matches "instacart", "the instacart app", "your cart" (generic, not a real store)
- **`items`** (`meal_order`, `grocery_order`): reject individual items where name is >200 chars or contains URL (list is filtered)

## Horizontal-run preprocessing (before field extraction)

For HTML-rendered emails, `clean_email_body()` often collapses tables into single-line runs
with `\xa0` and long space sequences. Before applying field-level regexes, run:

```python
def _split_horizontal_runs(body: str) -> str:
    body = body.replace("\xa0", " ")
    body = re.sub(r" {3,}", "\n", body)
    return body
```

This is implemented in `doordash.py` and `ubereats.py` and should be used in any new
extractor that parses items, totals, or other structured fields from HTML-origin emails.

## Round-trip validation (per-field, logged as warning, does not reject)

After extraction, for each populated scalar field, check whether the extracted value (or normalized: lowercase, whitespace-collapsed) appears somewhere in the source email's `clean_email_body` output. If not found, log a warning. Implemented in `validate_provenance_round_trip()`.

## Critical fields for field_population measurement

Defined in `archive_sync/extractors/field_metrics.py` (`CRITICAL_FIELDS`). These are the
fields checked by the staging extract's `field_population` output:

| Card type     | Critical fields                                        |
| ------------- | ------------------------------------------------------ |
| meal_order    | restaurant, items (non-empty list), total (> 0)        |
| ride          | pickup_location, dropoff_location, fare (> 0)          |
| flight        | origin_airport, destination_airport, confirmation_code |
| accommodation | property_name, check_in, check_out, confirmation_code  |
| grocery_order | store, items, total                                    |
| shipment      | tracking_number, carrier                               |
| car_rental    | company, confirmation_code, pickup_at                  |

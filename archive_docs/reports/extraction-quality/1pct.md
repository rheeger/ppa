# Extraction quality — `1pct` slice

Staging directory: `/Users/rheeger/Code/rheeger/ppa/_artifacts/_staging-1pct`

## Runner metrics (from `_metrics.json`)

- **Emails scanned:** 34081
- **Matched:** 302
- **Cards extracted:** 68
- **Wall clock (s):** 226.8

| Extractor   | matched | extracted | errors | skipped | rejected |
| ----------- | ------: | --------: | -----: | ------: | -------: |
| airbnb      |      32 |        21 |      0 |       1 |        4 |
| doordash    |      62 |        10 |      0 |       0 |       39 |
| instacart   |       7 |         1 |      0 |       0 |        0 |
| lyft        |       5 |         0 |      0 |       0 |        0 |
| rental_cars |       2 |         0 |      0 |       0 |        0 |
| shipping    |      40 |         8 |      0 |       7 |        0 |
| uber_eats   |      16 |        11 |      0 |       0 |        1 |
| uber_rides  |     114 |         8 |      0 |      22 |        0 |
| united      |      24 |         9 |      0 |       1 |        0 |

**Yield (extracted / matched)** — low values often mean parse/skip inside extractor.

- `airbnb`: 0.656
- `doordash`: 0.161
- `instacart`: 0.143
- `lyft`: 0.000
- `rental_cars`: 0.000
- `shipping`: 0.200
- `uber_eats`: 0.688
- `uber_rides`: 0.070
- `united`: 0.375

## Volume + field population (staging scan)

| Type          | Count | Expected | Status                             |
| ------------- | ----: | -------- | ---------------------------------- |
| accommodation |    22 | 206-384  | LOW (below 50% of lower bound 206) |
| flight        |    15 | 423-787  | LOW (below 50% of lower bound 423) |
| grocery_order |     1 | 11-23    | LOW (below 50% of lower bound 11)  |
| meal_order    |    25 | 717-1333 | LOW (below 50% of lower bound 717) |
| ride          |    30 | 473-881  | LOW (below 50% of lower bound 473) |
| shipment      |    15 | 486-904  | LOW (below 50% of lower bound 486) |

**Critical field population** (fraction of cards with field populated)

| Type          | Field               | Populated |
| ------------- | ------------------- | --------: |
| accommodation | check_in            |     77.3% |
| accommodation | check_out           |     77.3% |
| accommodation | confirmation_code   |    100.0% |
| accommodation | property_name       |    100.0% |
| flight        | confirmation_code   |    100.0% |
| flight        | destination_airport |    100.0% |
| flight        | origin_airport      |    100.0% |
| grocery_order | items               |      0.0% |
| grocery_order | store               |    100.0% |
| grocery_order | total               |    100.0% |
| meal_order    | items               |      8.0% |
| meal_order    | restaurant          |     72.0% |
| meal_order    | total               |    100.0% |
| ride          | dropoff_location    |     93.3% |
| ride          | fare                |    100.0% |
| ride          | pickup_location     |     93.3% |
| shipment      | carrier             |    100.0% |
| shipment      | tracking_number     |    100.0% |

**Warnings:**

- accommodation: volume LOW (below 50% of lower bound 206) (count=22)
- flight: volume LOW (below 50% of lower bound 423) (count=15)
- grocery_order: volume LOW (below 50% of lower bound 11) (count=1)
- meal_order: volume LOW (below 50% of lower bound 717) (count=25)
- ride: volume LOW (below 50% of lower bound 473) (count=30)
- shipment: volume LOW (below 50% of lower bound 486) (count=15)

## Per card type — flags, rates, and examples

Flags combine **critical_fail:** (strict predicate from `field_metrics.CRITICAL_FIELDS`) and **heuristic:** (common junk patterns). Use flagged rows first when upgrading extractors.
Round-trip source checks use vault `/Users/rheeger/Code/rheeger/ppa/.slices/1pct`.

### `accommodation` (22 cards)

_Typical extractors:_ `airbnb`

- **Confidence distribution:** >=0.8: 17 (77%), 0.5–0.8: 4 (18%), <0.5: 0 (0%), n/a: 1

- **Share with ≥1 flag:** 22/22 (100.0%)
- **Flag counts (cards can have multiple):**
  - `heuristic:round_trip_fail:property_type`: 22
  - `heuristic:duplicate_suspect`: 19
  - `critical_fail:check_in`: 5
  - `critical_fail:check_out`: 5
  - `heuristic:missing_check_in`: 5
  - `heuristic:missing_check_out`: 5

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Accommodations/2023-06/hfa-accommodation-8ba4e65a1007.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 1507018364
created: 2023-06-10
extraction_confidence: 0.5
property_name: 16, 2023
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-e1be2c39fec9]]
source_id: hfa-accommodation-8ba4e65a1007
summary: hfa-accommodation-8ba4e65a1007
type: accommodation
uid: hfa-accommodation-8ba4e65a1007
updated: 2023-06-10
```

Body (truncated):

```markdown
# Airbnb 16, 2023
```

---

**File:** `Transactions/Accommodations/2018-08/hfa-accommodation-587e8cc2f110.md`
**Flags:** `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
check_in: Aug 13, 2018
check_out: Aug 18, 2018
confirmation_code: 531201570
created: 2018-08-17
extraction_confidence: 1.0
property_name: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-ff7b50534b3d]]
source_id: hfa-accommodation-587e8cc2f110
summary: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES Aug 13, 2018 to Aug 18, 2018
type: accommodation
uid: hfa-accommodation-587e8cc2f110
updated: 2018-08-17
```

Body (truncated):

```markdown
# Airbnb Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
```

---

**File:** `Transactions/Accommodations/2018-08/hfa-accommodation-1c49d51059ab.md`
**Flags:** `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
check_in: Aug 13, 2018
check_out: Aug 18, 2018
confirmation_code: 531201570
created: 2018-08-13
extraction_confidence: 1.0
property_name: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-5d03c9f329a7]]
source_id: hfa-accommodation-1c49d51059ab
summary: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES Aug 13, 2018 to Aug 18, 2018
type: accommodation
uid: hfa-accommodation-1c49d51059ab
updated: 2018-08-13
```

Body (truncated):

```markdown
# Airbnb Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
```

---

**File:** `Transactions/Accommodations/2018-08/hfa-accommodation-eb8421900af1.md`
**Flags:** `heuristic:round_trip_fail:property_type`

```yaml
booking_source: Airbnb
check_in: Aug 13, 2018
check_out: Aug 18, 2018
confirmation_code: 17359953
created: 2018-08-17
extraction_confidence: 1.0
property_name: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-6b9155998050]]
source_id: hfa-accommodation-eb8421900af1
summary: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES Aug 13, 2018 to Aug 18, 2018
type: accommodation
uid: hfa-accommodation-eb8421900af1
updated: 2018-08-17
```

Body (truncated):

```markdown
# Airbnb Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
```

---

**File:** `Transactions/Accommodations/2018-08/hfa-accommodation-cc474e881f5e.md`
**Flags:** `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
check_in: Aug 13, 2018
check_out: Aug 18, 2018
confirmation_code: 531201570
created: 2018-08-13
extraction_confidence: 1.0
property_name: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-2bba776edfbc]]
source_id: hfa-accommodation-cc474e881f5e
summary: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES Aug 13, 2018 to Aug 18, 2018
type: accommodation
uid: hfa-accommodation-cc474e881f5e
updated: 2018-08-13
```

Body (truncated):

```markdown
# Airbnb Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
```

---

**File:** `Transactions/Accommodations/2018-08/hfa-accommodation-d5cdc1f64b69.md`
**Flags:** `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
check_in: Aug 13, 2018
check_out: Aug 18, 2018
confirmation_code: 531201570
created: 2018-08-13
extraction_confidence: 1.0
property_name: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-7bf8c07200c7]]
source_id: hfa-accommodation-d5cdc1f64b69
summary: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES Aug 13, 2018 to Aug 18, 2018
type: accommodation
uid: hfa-accommodation-d5cdc1f64b69
updated: 2018-08-13
```

Body (truncated):

```markdown
# Airbnb Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
```

---

**File:** `Transactions/Accommodations/2018-08/hfa-accommodation-f6c6e296ef12.md`
**Flags:** `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
check_in: Aug 13, 2018
check_out: Aug 18, 2018
confirmation_code: 531201570
created: 2018-08-11
extraction_confidence: 1.0
property_name: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-2faa58e4aa5d]]
source_id: hfa-accommodation-f6c6e296ef12
summary: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES Aug 13, 2018 to Aug 18, 2018
type: accommodation
uid: hfa-accommodation-f6c6e296ef12
updated: 2018-08-11
```

Body (truncated):

```markdown
# Airbnb Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
```

---

**File:** `Transactions/Accommodations/2022-12/hfa-accommodation-544cc4cc26cd.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 1343149593
created: 2022-12-03
extraction_confidence: 0.5
property_name: Davos Private Room in Chalet/Top Lage for Jan 14 - 23, 2023
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-bc51ecdbeac4]]
source_id: hfa-accommodation-544cc4cc26cd
summary: hfa-accommodation-544cc4cc26cd
type: accommodation
uid: hfa-accommodation-544cc4cc26cd
updated: 2022-12-03
```

Body (truncated):

```markdown
# Airbnb Davos Private Room in Chalet/Top Lage for Jan 14 - 23, 2023
```

---

### `flight` (15 cards)

_Typical extractors:_ `united`

- **Confidence distribution:** >=0.8: 9 (60%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 6

- **Share with ≥1 flag:** 8/15 (53.3%)
- **Flag counts (cards can have multiple):**
  - `heuristic:non_iata_destination_airport`: 5
  - `heuristic:non_iata_origin_airport`: 4
  - `heuristic:duplicate_suspect`: 4
  - `heuristic:round_trip_fail:fare_amount`: 3

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Flights/2015-09/hfa-flight-d089805cd183.md`
**Flags:** `heuristic:non_iata_destination_airport`, `heuristic:round_trip_fail:fare_amount`

```yaml
airline: United
booking_source: United
confirmation_code: FE510T
created: 2015-09-27
destination_airport: TXL
fare_amount: 30000.0
origin_airport: LHR
source: ["email_extraction"]
source_email: [[hfa-email-message-f2d71790224c]]
source_id: hfa-flight-d089805cd183
summary: United LHR to TXL
type: flight
uid: hfa-flight-d089805cd183
updated: 2015-09-27
```

Body (truncated):

```markdown
# United LHR→TXL FE510T
```

---

**File:** `Transactions/Flights/2011-11/hfa-flight-a094d7ae11a0.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`

```yaml
airline: United
arrival_at: Cabin
booking_source: United
confirmation_code: Q8457M
created: 2011-11-20
departure_at: Arrive
destination_airport: TAL
fare_amount: 577.8
origin_airport: SUB
source: ["email_extraction"]
source_email: [[hfa-email-message-a0045dfb2faa]]
source_id: hfa-flight-a094d7ae11a0
summary: United SUB to TAL
type: flight
uid: hfa-flight-a094d7ae11a0
updated: 2011-11-20
```

Body (truncated):

```markdown
# United SUB→TAL Q8457M
```

---

**File:** `Transactions/Flights/2011-09/hfa-flight-8962c18601f9.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`

```yaml
airline: United
arrival_at: Cabin
booking_source: United
confirmation_code: NPFPXR
created: 2011-09-21
departure_at: Arrive
destination_airport: TAL
fare_amount: 458.8
origin_airport: SUB
source: ["email_extraction"]
source_email: [[hfa-email-message-0427e2b12263]]
source_id: hfa-flight-8962c18601f9
summary: United SUB to TAL
type: flight
uid: hfa-flight-8962c18601f9
updated: 2011-09-21
```

Body (truncated):

```markdown
# United SUB→TAL NPFPXR
```

---

**File:** `Transactions/Flights/2023-01/hfa-flight-6d2cf5977300.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: PK30W7
created: 2023-01-03
destination_airport: SLC
extraction_confidence: 1.0
origin_airport: EWR
source: ["email_extraction"]
source_email: [[hfa-email-message-47329bd6e291]]
source_id: hfa-flight-6d2cf5977300
summary: United EWR to SLC
type: flight
uid: hfa-flight-6d2cf5977300
updated: 2023-01-03
```

Body (truncated):

```markdown
# United EWR→SLC PK30W7
```

---

**File:** `Transactions/Flights/2016-09/hfa-flight-9bb10c9961b9.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`, `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: City and Time
booking_source: United
confirmation_code: BPW226
created: 2016-09-30
departure_at: City and Time
destination_airport: FAA
origin_airport: DUE
source: ["email_extraction"]
source_email: [[hfa-email-message-47fd81ee40f9]]
source_id: hfa-flight-9bb10c9961b9
summary: United DUE to FAA
type: flight
uid: hfa-flight-9bb10c9961b9
updated: 2016-09-30
```

Body (truncated):

```markdown
# United DUE→FAA BPW226
```

---

**File:** `Transactions/Flights/2023-01/hfa-flight-04da0daf259e.md`
**Flags:** `heuristic:round_trip_fail:fare_amount`, `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: PK30W7
created: 2023-01-03
destination_airport: SLC
extraction_confidence: 1.0
fare_amount: 1379.7
origin_airport: EWR
source: ["email_extraction"]
source_email: [[hfa-email-message-4ac7bad81c7f]]
source_id: hfa-flight-04da0daf259e
summary: United EWR to SLC
type: flight
uid: hfa-flight-04da0daf259e
updated: 2023-01-03
```

Body (truncated):

```markdown
# United EWR→SLC PK30W7
```

---

**File:** `Transactions/Flights/2019-10/hfa-flight-63608c0c20a4.md`
**Flags:** `heuristic:round_trip_fail:fare_amount`

```yaml
airline: United
booking_source: United
confirmation_code: I73GJ6
created: 2019-10-26
destination_airport: SFO
extraction_confidence: 1.0
fare_amount: 20000.0
origin_airport: LAX
source: ["email_extraction"]
source_email: [[hfa-email-message-2188aa14c6d1]]
source_id: hfa-flight-63608c0c20a4
summary: United LAX to SFO
type: flight
uid: hfa-flight-63608c0c20a4
updated: 2019-10-26
```

Body (truncated):

```markdown
# United LAX→SFO I73GJ6
```

---

**File:** `Transactions/Flights/2016-09/hfa-flight-41464c35f73e.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`, `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: City and Time
booking_source: United
confirmation_code: BPW226
created: 2016-09-30
departure_at: City and Time
destination_airport: FAA
origin_airport: DUE
source: ["email_extraction"]
source_email: [[hfa-email-message-69910a9b6654]]
source_id: hfa-flight-41464c35f73e
summary: United DUE to FAA
type: flight
uid: hfa-flight-41464c35f73e
updated: 2016-09-30
```

Body (truncated):

```markdown
# United DUE→FAA BPW226
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Flights/2024-01/hfa-flight-2af7a74b2c26.md`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: MYDMV0
created: 2024-01-07
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: SFO
extraction_confidence: 1.0
fare_amount: 104.19
origin_airport: LAX
source: ["email_extraction"]
source_email: [[hfa-email-message-335aca44c1a2]]
source_id: hfa-flight-2af7a74b2c26
summary: United LAX to SFO
type: flight
uid: hfa-flight-2af7a74b2c26
updated: 2024-01-07
```

Body (truncated):

```markdown
# United LAX→SFO MYDMV0
```

---

**File:** `Transactions/Flights/2021-09/hfa-flight-82d216bac6af.md`

```yaml
airline: United
booking_source: United
confirmation_code: O5D37R
created: 2021-09-14
destination_airport: DEN
extraction_confidence: 1.0
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-178bd9a3ce8e]]
source_id: hfa-flight-82d216bac6af
summary: United SFO to DEN
type: flight
uid: hfa-flight-82d216bac6af
updated: 2021-09-14
```

Body (truncated):

```markdown
# United SFO→DEN O5D37R
```

---

**File:** `Transactions/Flights/2022-05/hfa-flight-59e93ec83a7f.md`

```yaml
airline: United
arrival_at: Original itinerary UA 2166 June 3, 2022 1:00 p.m. San Francis
booking_source: United
confirmation_code: FDFTBM
created: 2022-05-26
departure_at: Arrive Original itinerary UA 2166 June 3, 2
destination_airport: LAX
extraction_confidence: 1.0
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-b39063cdccba]]
source_id: hfa-flight-59e93ec83a7f
summary: United SFO to LAX
type: flight
uid: hfa-flight-59e93ec83a7f
updated: 2022-05-26
```

Body (truncated):

```markdown
# United SFO→LAX FDFTBM
```

---

**File:** `Transactions/Flights/2012-05/hfa-flight-e468c3cd27a3.md`

```yaml
airline: United
arrival_at: 5:44 p.m.Travel Time:<span class="PHead">
booking_source: United
confirmation_code: G0CFL4
created: 2012-05-24
departure_at: 2:40 p.m.Fri., May. 25, 2012Los Angeles, CA (LAX)Arrive:5:44 p.m.Travel Time:<sp
destination_airport: LAX
extraction_confidence: 1.0
origin_airport: LIH
source: ["email_extraction"]
source_email: [[hfa-email-message-7e52a01c9d7f]]
source_id: hfa-flight-e468c3cd27a3
summary: United LIH to LAX
type: flight
uid: hfa-flight-e468c3cd27a3
updated: 2012-05-24
```

Body (truncated):

```markdown
# United LIH→LAX G0CFL4
```

---

### `grocery_order` (1 cards)

_Typical extractors:_ `instacart`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 1 (100%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 1/1 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 1
  - `heuristic:empty_items`: 1

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Groceries/2023-04/hfa-grocery_order-8d77484b25f5.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2023-04-27
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-6b60f283dd3a]]
source_id: hfa-grocery_order-8d77484b25f5
store: Stop & Shop
summary: Instacart order from Stop & Shop
total: 188.03
type: grocery_order
uid: hfa-grocery_order-8d77484b25f5
updated: 2023-04-27
```

Body (truncated):

```markdown
# Instacart — Stop & Shop
```

---

### `meal_order` (25 cards)

_Typical extractors:_ `uber_eats`, `doordash`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 16 (64%), <0.5: 5 (20%), n/a: 4

- **Share with ≥1 flag:** 25/25 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 23
  - `heuristic:empty_items`: 23
  - `heuristic:round_trip_fail:mode`: 9
  - `critical_fail:restaurant`: 7
  - `heuristic:url_in_restaurant`: 3
  - `heuristic:round_trip_fail:service`: 2
  - `heuristic:duplicate_suspect`: 2

#### Flagged examples (prioritize fixes)

**File:** `Transactions/MealOrders/2023-12/hfa-meal_order-99c694a4e69d.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2023-12-12
extraction_confidence: 0.67
mode: delivery
restaurant: Taim Mediterranean Kitchen
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-d0f118bae2c4]]
source_id: hfa-meal_order-99c694a4e69d
summary: DoorDash order from Taim Mediterranean Kitchen
total: 31.29
type: meal_order
uid: hfa-meal_order-99c694a4e69d
updated: 2023-12-12
```

Body (truncated):

```markdown
# DoorDash — Taim Mediterranean Kitchen

- **Total**: $31.29
```

---

**File:** `Transactions/MealOrders/2024-01/hfa-meal_order-a7b2f9982b48.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2024-01-04
extraction_confidence: 0.67
mode: delivery
restaurant: 5ive Spice
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-26f0aaad9e5f]]
source_id: hfa-meal_order-a7b2f9982b48
summary: DoorDash order from 5ive Spice
total: 49.68
type: meal_order
uid: hfa-meal_order-a7b2f9982b48
updated: 2024-01-04
```

Body (truncated):

```markdown
# DoorDash — 5ive Spice

- **Total**: $49.68
```

---

**File:** `Transactions/MealOrders/2025-07/hfa-meal_order-b7dab43038be.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-07-17
extraction_confidence: 0.67
mode: delivery
restaurant: Brooklyn Hero Shop
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-d8a7266eb15a]]
source_id: hfa-meal_order-b7dab43038be
summary: DoorDash order from Brooklyn Hero Shop
total: 32.28
type: meal_order
uid: hfa-meal_order-b7dab43038be
updated: 2025-07-17
```

Body (truncated):

```markdown
# DoorDash — Brooklyn Hero Shop

- **Total**: $32.28
```

---

**File:** `Transactions/MealOrders/2025-01/hfa-meal_order-491449dd158d.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-01-15
extraction_confidence: 0.67
mode: delivery
restaurant: Em Vietnamese Bistro
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-45c78f20c0ac]]
source_id: hfa-meal_order-491449dd158d
summary: DoorDash order from Em Vietnamese Bistro
total: 23.74
type: meal_order
uid: hfa-meal_order-491449dd158d
updated: 2025-01-15
```

Body (truncated):

```markdown
# DoorDash — Em Vietnamese Bistro

- **Total**: $23.74
```

---

**File:** `Transactions/MealOrders/2024-01/hfa-meal_order-677bc1fba44f.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`, `heuristic:round_trip_fail:mode`

```yaml
created: 2024-01-26
extraction_confidence: 0.67
mode: delivery
restaurant: Chicken Stop (Fort Greene)
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-26c568f83ab3]]
source_id: hfa-meal_order-677bc1fba44f
summary: Uber Eats order from Chicken Stop (Fort Greene)
total: 27.21
type: meal_order
uid: hfa-meal_order-677bc1fba44f
updated: 2024-01-26
```

Body (truncated):

```markdown
# Uber Eats — Chicken Stop (Fort Greene)
```

---

**File:** `Transactions/MealOrders/2025-05/hfa-meal_order-032457a2cec4.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`, `heuristic:round_trip_fail:mode`

```yaml
created: 2025-05-01
extraction_confidence: 0.67
mode: delivery
restaurant: Black iris
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-d028f51c966d]]
source_id: hfa-meal_order-032457a2cec4
summary: DoorDash order from Black iris
total: 15.24
type: meal_order
uid: hfa-meal_order-032457a2cec4
updated: 2025-05-01
```

Body (truncated):

```markdown
# DoorDash — Black iris

- **Total**: $15.24
```

---

**File:** `Transactions/MealOrders/2024-01/hfa-meal_order-802cafb72dc2.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2024-01-04
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-26f0aaad9e5f]]
source_id: hfa-meal_order-802cafb72dc2
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
total: 49.68
type: meal_order
uid: hfa-meal_order-802cafb72dc2
updated: 2024-01-04
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Total**: $49.68
```

---

**File:** `Transactions/MealOrders/2019-10/hfa-meal_order-ffd00f939682.md`
**Flags:** `critical_fail:restaurant`, `heuristic:round_trip_fail:service`, `heuristic:duplicate_suspect`

```yaml
created: 2019-10-31
extraction_confidence: 0.67
items:
  [
    { "name": "Pad Thai", "quantity": 1, "price": "13.95" },
    { "name": "Green Curry", "quantity": 1, "price": "16.45" },
  ]
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-61357207c7c3]]
source_id: hfa-meal_order-ffd00f939682
subtotal: 30.4
summary: hfa-meal_order-ffd00f939682
tax: 2.58
total: 5.0
type: meal_order
uid: hfa-meal_order-ffd00f939682
updated: 2019-10-31
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly. Learn More xide2f155bc-d84b-52c3-9f4b-8a4f7ac96a03 pGvlI2ANUbXFfyEOgxta1RMV082993
```

---

### `ride` (30 cards)

_Typical extractors:_ `uber_rides`, `lyft`

- **Confidence distribution:** >=0.8: 6 (20%), 0.5–0.8: 0 (0%), <0.5: 2 (7%), n/a: 22

- **Share with ≥1 flag:** 30/30 (100.0%)
- **Flag counts (cards can have multiple):**
  - `heuristic:round_trip_fail:pickup_at`: 30
  - `heuristic:round_trip_fail:duration_minutes`: 12
  - `heuristic:round_trip_fail:ride_type`: 6
  - `critical_fail:pickup_location`: 2
  - `critical_fail:dropoff_location`: 2
  - `heuristic:weak_pickup`: 2
  - `heuristic:weak_dropoff`: 2

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Rides/2013-03/hfa-ride-8ca55653e007.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`, `heuristic:round_trip_fail:duration_minutes`

```yaml
created: 2013-03-29
distance_miles: 4.1
driver_name: Meseret (# 527)
dropoff_location: Location: 645-655 Bush Street, San Francisco, CA
duration_minutes: 17.0
fare: 22.36
pickup_at: 2013-03-29T21:33:08Z
pickup_location: Location: 5334 Geary Street, San Francisco, CA
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-0f8b25258567]]
source_id: hfa-ride-8ca55653e007
summary: Uber from Location: 5334 Geary Street, San Francisco, CA to Location: 645-655 Bush Street, San Francisco, CA
type: ride
uid: hfa-ride-8ca55653e007
updated: 2013-03-29
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location: 5334 Geary Street, San Francisco, CA
- **Dropoff**: Location: 645-655 Bush Street, San Francisco, CA
- **Pickup at**: 2013-03-29T21:33:08Z
- **Fare**: 22.36
- **Distance (mi)**: 4.1
- **Duration (min)**: 17.0
- **Driver**: Meseret (# 527)
```

---

**File:** `Transactions/Rides/2016-01/hfa-ride-746edbf545d6.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2016-01-02
dropoff_location: 2904-2908 23rd St, San Francisco, CA
fare: 8.96
pickup_at: 2016-01-02T08:36:17Z
pickup_location: 910 Harrison St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-ab49a5a7edb9]]
source_id: hfa-ride-746edbf545d6
summary: Uber from 910 Harrison St, San Francisco, CA to 2904-2908 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-746edbf545d6
updated: 2016-01-02
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 910 Harrison St, San Francisco, CA
- **Dropoff**: 2904-2908 23rd St, San Francisco, CA
- **Pickup at**: 2016-01-02T08:36:17Z
- **Fare**: 8.96
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2015-11/hfa-ride-2b769de3ba37.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2015-11-09
dropoff_location: 2572-2578 Pine St, San Francisco, CA
fare: 32.59
pickup_at: 2015-11-09T07:03:02Z
pickup_location: Domestic Terminals Departures Level, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-26b6e4e7203f]]
source_id: hfa-ride-2b769de3ba37
summary: Uber from Domestic Terminals Departures Level, San Francisco, CA to 2572-2578 Pine St, San Francisco, CA
type: ride
uid: hfa-ride-2b769de3ba37
updated: 2015-11-09
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Domestic Terminals Departures Level, San Francisco, CA
- **Dropoff**: 2572-2578 Pine St, San Francisco, CA
- **Pickup at**: 2015-11-09T07:03:02Z
- **Fare**: 32.59
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-08/hfa-ride-57bc36445df4.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2016-08-14
driver_name: DANNA
dropoff_location: 550 Divisadero St, San Francisco, CA
fare: 8.18
pickup_at: 2016-08-14T18:21:19Z
pickup_location: 2919-2923 23rd St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-85bb5ccf55c1]]
source_id: hfa-ride-57bc36445df4
summary: Uber from 2919-2923 23rd St, San Francisco, CA to 550 Divisadero St, San Francisco, CA
type: ride
uid: hfa-ride-57bc36445df4
updated: 2016-08-14
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2919-2923 23rd St, San Francisco, CA
- **Dropoff**: 550 Divisadero St, San Francisco, CA
- **Pickup at**: 2016-08-14T18:21:19Z
- **Fare**: 8.18
- **Driver**: DANNA
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-01/hfa-ride-3550863eb542.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`, `heuristic:round_trip_fail:duration_minutes`

```yaml
created: 2016-01-02
dropoff_location: 1-91 Franklin St, Oakland, CA
duration_minutes: 35.0
fare: 5.35
pickup_at: 2016-01-02T02:40:20Z
pickup_location: 1401-1423 Broadway, Oakland, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-9df79bb2f724]]
source_id: hfa-ride-3550863eb542
summary: Uber from 1401-1423 Broadway, Oakland, CA to 1-91 Franklin St, Oakland, CA
type: ride
uid: hfa-ride-3550863eb542
updated: 2016-01-02
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 1401-1423 Broadway, Oakland, CA
- **Dropoff**: 1-91 Franklin St, Oakland, CA
- **Pickup at**: 2016-01-02T02:40:20Z
- **Fare**: 5.35
- **Duration (min)**: 35.0
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-08/hfa-ride-645d0e34c08c.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2016-08-31
driver_name: BRIANA
dropoff_location: 958-968 Geary Blvd, San Francisco, CA
fare: 9.8
pickup_at: 2016-08-31T02:49:53Z
pickup_location: 2915b 23rd St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-5f8e32c63165]]
source_id: hfa-ride-645d0e34c08c
summary: Uber from 2915b 23rd St, San Francisco, CA to 958-968 Geary Blvd, San Francisco, CA
type: ride
uid: hfa-ride-645d0e34c08c
updated: 2016-08-31
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2915b 23rd St, San Francisco, CA
- **Dropoff**: 958-968 Geary Blvd, San Francisco, CA
- **Pickup at**: 2016-08-31T02:49:53Z
- **Fare**: 9.8
- **Driver**: BRIANA
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-08/hfa-ride-518481f346a0.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2016-08-31
driver_name: FREDY
dropoff_location: 2926 23rd St, San Francisco, CA
fare: 8.77
pickup_at: 2016-08-31T05:47:22Z
pickup_location: 935 Geary Blvd, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-a74e191e87cc]]
source_id: hfa-ride-518481f346a0
summary: Uber from 935 Geary Blvd, San Francisco, CA to 2926 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-518481f346a0
updated: 2016-08-31
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 935 Geary Blvd, San Francisco, CA
- **Dropoff**: 2926 23rd St, San Francisco, CA
- **Pickup at**: 2016-08-31T05:47:22Z
- **Fare**: 8.77
- **Driver**: FREDY
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-01/hfa-ride-2965c9981a17.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2016-01-19
dropoff_location: 2608 Ocean Ave, San Francisco, CA
fare: 13.38
pickup_at: 2016-01-19T01:53:48Z
pickup_location: 2927 23rd St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-a7fce81452e8]]
source_id: hfa-ride-2965c9981a17
summary: Uber from 2927 23rd St, San Francisco, CA to 2608 Ocean Ave, San Francisco, CA
type: ride
uid: hfa-ride-2965c9981a17
updated: 2016-01-19
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2927 23rd St, San Francisco, CA
- **Dropoff**: 2608 Ocean Ave, San Francisco, CA
- **Pickup at**: 2016-01-19T01:53:48Z
- **Fare**: 13.38
- **Vehicle**: uberX
```

---

### `shipment` (15 cards)

_Typical extractors:_ `shipping`

- **Confidence distribution:** >=0.8: 8 (53%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 7

- **Share with ≥1 flag:** 1/15 (6.7%)
- **Flag counts (cards can have multiple):**
  - `heuristic:round_trip_fail:carrier`: 1

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Shipments/2020-12/hfa-shipment-8a135cd65626.md`
**Flags:** `heuristic:round_trip_fail:carrier`

```yaml
carrier: FedEx
created: 2020-12-19
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-342fa1984db0]]
source_id: hfa-shipment-8a135cd65626
summary: FedEx 40708938006301
tracking_number: 40708938006301
type: shipment
uid: hfa-shipment-8a135cd65626
updated: 2020-12-19
```

Body (truncated):

```markdown
# Shipment FedEx 40708938006301
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Shipments/2025-10/hfa-shipment-5a85fa3dc8f2.md`

```yaml
carrier: FedEx
created: 2025-10-23
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-53d96d834741]]
source_id: hfa-shipment-5a85fa3dc8f2
summary: FedEx 480794158021
tracking_number: 480794158021
type: shipment
uid: hfa-shipment-5a85fa3dc8f2
updated: 2025-10-23
```

Body (truncated):

```markdown
# Shipment FedEx 480794158021
```

---

**File:** `Transactions/Shipments/2025-09/hfa-shipment-5c5533871274.md`

```yaml
carrier: FedEx
created: 2025-09-23
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-ec7fe6b6adf0]]
source_id: hfa-shipment-5c5533871274
summary: FedEx 393427669057
tracking_number: 393427669057
type: shipment
uid: hfa-shipment-5c5533871274
updated: 2025-09-23
```

Body (truncated):

```markdown
# Shipment FedEx 393427669057
```

---

**File:** `Transactions/Shipments/2023-07/hfa-shipment-c1e8bae02dd3.md`

```yaml
carrier: UPS
created: 2023-07-26
source: ["email_extraction"]
source_email: [[hfa-email-message-c4da7c3f40b8]]
source_id: hfa-shipment-c1e8bae02dd3
summary: UPS 1Z9770W4A244357797
tracking_number: 1Z9770W4A244357797
type: shipment
uid: hfa-shipment-c1e8bae02dd3
updated: 2023-07-26
```

Body (truncated):

```markdown
# Shipment UPS 1Z9770W4A244357797
```

---

**File:** `Transactions/Shipments/2024-12/hfa-shipment-c32b06a995ac.md`

```yaml
carrier: UPS
created: 2024-12-13
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-c314a53ad805]]
source_id: hfa-shipment-c32b06a995ac
summary: UPS 1Z4430800227530220
tracking_number: 1Z4430800227530220
type: shipment
uid: hfa-shipment-c32b06a995ac
updated: 2024-12-13
```

Body (truncated):

```markdown
# Shipment UPS 1Z4430800227530220
```

---

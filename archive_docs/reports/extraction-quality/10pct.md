# Extraction quality — `10pct` slice

Staging directory: `/Users/rheeger/Code/rheeger/ppa/_artifacts/_staging-10pct`

## Runner metrics (from `_metrics.json`)

- **Emails scanned:** 166637
- **Matched:** 2594
- **Cards extracted:** 927
- **Wall clock (s):** 878.5

| Extractor   | matched | extracted | errors | skipped | rejected |
| ----------- | ------: | --------: | -----: | ------: | -------: |
| airbnb      |     183 |       121 |      0 |       0 |       32 |
| doordash    |     475 |        99 |      0 |       0 |      258 |
| instacart   |      38 |         3 |      0 |       0 |        1 |
| lyft        |      46 |         5 |      0 |       0 |        0 |
| rental_cars |      36 |        16 |      0 |       0 |        3 |
| shipping    |     348 |       145 |      0 |       0 |        0 |
| uber_eats   |     198 |       137 |      0 |       0 |       37 |
| uber_rides  |    1007 |       256 |      0 |       0 |        1 |
| united      |     263 |       145 |      0 |       0 |        0 |

**Yield (extracted / matched)** — low values often mean parse/skip inside extractor.

- `airbnb`: 0.661
- `doordash`: 0.208
- `instacart`: 0.079
- `lyft`: 0.109
- `rental_cars`: 0.444
- `shipping`: 0.417
- `uber_eats`: 0.692
- `uber_rides`: 0.254
- `united`: 0.551

## Volume + field population (staging scan)

| Type          | Count | Expected | Status                             |
| ------------- | ----: | -------- | ---------------------------------- |
| accommodation |   121 | 206-384  | LOW (below range 206-384)          |
| car_rental    |    16 | 35-67    | LOW (below 50% of lower bound 35)  |
| flight        |   145 | 423-787  | LOW (below 50% of lower bound 423) |
| grocery_order |     3 | 11-23    | LOW (below 50% of lower bound 11)  |
| meal_order    |   236 | 717-1333 | LOW (below 50% of lower bound 717) |
| ride          |   261 | 473-881  | LOW (below range 473-881)          |
| shipment      |   145 | 486-904  | LOW (below 50% of lower bound 486) |

**Critical field population** (fraction of cards with field populated)

| Type          | Field               | Populated |
| ------------- | ------------------- | --------: |
| accommodation | check_in            |     37.2% |
| accommodation | check_out           |     43.0% |
| accommodation | confirmation_code   |     99.2% |
| accommodation | property_name       |     96.7% |
| car_rental    | company             |    100.0% |
| car_rental    | confirmation_code   |    100.0% |
| car_rental    | pickup_at           |     50.0% |
| flight        | confirmation_code   |    100.0% |
| flight        | destination_airport |    100.0% |
| flight        | origin_airport      |    100.0% |
| grocery_order | items               |      0.0% |
| grocery_order | store               |    100.0% |
| grocery_order | total               |    100.0% |
| meal_order    | items               |     33.5% |
| meal_order    | restaurant          |     55.9% |
| meal_order    | total               |    100.0% |
| ride          | dropoff_location    |     97.7% |
| ride          | fare                |    100.0% |
| ride          | pickup_location     |     97.7% |
| shipment      | carrier             |    100.0% |
| shipment      | tracking_number     |    100.0% |

**Warnings:**

- accommodation: volume LOW (below range 206-384) (count=121)
- car_rental: volume LOW (below 50% of lower bound 35) (count=16)
- flight: volume LOW (below 50% of lower bound 423) (count=145)
- grocery_order: volume LOW (below 50% of lower bound 11) (count=3)
- meal_order: volume LOW (below 50% of lower bound 717) (count=236)
- ride: volume LOW (below range 473-881) (count=261)
- shipment: volume LOW (below 50% of lower bound 486) (count=145)

## Per card type — flags, rates, and examples

Flags combine **critical_fail:** (strict predicate from `field_metrics.CRITICAL_FIELDS`) and **heuristic:** (common junk patterns). Use flagged rows first when upgrading extractors.
Round-trip source checks use vault `/Users/rheeger/Code/rheeger/ppa/.slices/10pct`.

### `accommodation` (121 cards)

_Typical extractors:_ `airbnb`

- **Confidence distribution:** >=0.8: 41 (34%), 0.5–0.8: 76 (63%), <0.5: 4 (3%), n/a: 0

- **Share with ≥1 flag:** 121/121 (100.0%)
- **Flag counts (cards can have multiple):**
  - `heuristic:round_trip_fail:property_type`: 121
  - `heuristic:duplicate_suspect`: 83
  - `critical_fail:check_in`: 76
  - `heuristic:missing_check_in`: 76
  - `critical_fail:check_out`: 69
  - `heuristic:missing_check_out`: 69
  - `heuristic:round_trip_fail:property_name`: 9
  - `critical_fail:property_name`: 4
  - `critical_fail:confirmation_code`: 1

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Accommodations/2018-08/hfa-accommodation-18d467e8d9aa.md`
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
source_email: [[hfa-email-message-6c05c87e49a9]]
source_id: hfa-accommodation-18d467e8d9aa
summary: Quiet 1 Bdr in the heart of ST GERMAIN DES PRES Aug 13, 2018 to Aug 18, 2018
type: accommodation
uid: hfa-accommodation-18d467e8d9aa
updated: 2018-08-13
```

Body (truncated):

```markdown
# Airbnb Quiet 1 Bdr in the heart of ST GERMAIN DES PRES
```

---

**File:** `Transactions/Accommodations/2023-07/hfa-accommodation-284e37fe4373.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 1507018364
created: 2023-07-08
extraction_confidence: 0.5
property_name: Wood Cottage
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-c3728bd588e1]]
source_id: hfa-accommodation-284e37fe4373
summary: hfa-accommodation-284e37fe4373
type: accommodation
uid: hfa-accommodation-284e37fe4373
updated: 2023-07-08
```

Body (truncated):

```markdown
# Airbnb Wood Cottage
```

---

**File:** `Transactions/Accommodations/2022-06/hfa-accommodation-ddadb06bcde6.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

```yaml
booking_source: Airbnb
confirmation_code: 51891431
created: 2022-06-07
extraction_confidence: 0.5
property_name: Napa.%opentrack%
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-c92ce7ca54c6]]
source_id: hfa-accommodation-ddadb06bcde6
summary: hfa-accommodation-ddadb06bcde6
type: accommodation
uid: hfa-accommodation-ddadb06bcde6
updated: 2022-06-07
```

Body (truncated):

```markdown
# Airbnb Napa.%opentrack%
```

---

**File:** `Transactions/Accommodations/2026-01/hfa-accommodation-50ac08cc71da.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 2394348729
created: 2026-01-03
extraction_confidence: 0.5
property_name: Mar 14
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-b04e2553c29e]]
source_id: hfa-accommodation-50ac08cc71da
summary: hfa-accommodation-50ac08cc71da
type: accommodation
uid: hfa-accommodation-50ac08cc71da
updated: 2026-01-03
```

Body (truncated):

```markdown
# Airbnb Mar 14
```

---

**File:** `Transactions/Accommodations/2023-07/hfa-accommodation-263c4016b364.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 1507018364
created: 2023-07-13
extraction_confidence: 0.5
property_name: Wood Cottage
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-a0fdbcbaf53a]]
source_id: hfa-accommodation-263c4016b364
summary: hfa-accommodation-263c4016b364
type: accommodation
uid: hfa-accommodation-263c4016b364
updated: 2023-07-13
```

Body (truncated):

```markdown
# Airbnb Wood Cottage
```

---

**File:** `Transactions/Accommodations/2019-08/hfa-accommodation-81a01384b098.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

```yaml
booking_source: Airbnb
confirmation_code: 18650017
created: 2019-08-10
extraction_confidence: 0.5
property_name: in Pollonia
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-e4e69c1f3631]]
source_id: hfa-accommodation-81a01384b098
summary: hfa-accommodation-81a01384b098
type: accommodation
uid: hfa-accommodation-81a01384b098
updated: 2019-08-10
```

Body (truncated):

```markdown
# Airbnb in Pollonia
```

---

**File:** `Transactions/Accommodations/2023-07/hfa-accommodation-6a81b779ec04.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 1507018364
created: 2023-07-09
extraction_confidence: 0.5
property_name: Wood Cottage
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-095d7080a76e]]
source_id: hfa-accommodation-6a81b779ec04
summary: hfa-accommodation-6a81b779ec04
type: accommodation
uid: hfa-accommodation-6a81b779ec04
updated: 2023-07-09
```

Body (truncated):

```markdown
# Airbnb Wood Cottage
```

---

**File:** `Transactions/Accommodations/2023-07/hfa-accommodation-48de1a463b2e.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 1507018364
created: 2023-07-13
extraction_confidence: 0.5
property_name: Wood Cottage
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-4fc1005c84aa]]
source_id: hfa-accommodation-48de1a463b2e
summary: hfa-accommodation-48de1a463b2e
type: accommodation
uid: hfa-accommodation-48de1a463b2e
updated: 2023-07-13
```

Body (truncated):

```markdown
# Airbnb Wood Cottage
```

---

### `car_rental` (16 cards)

_Typical extractors:_ `rental_cars`

- **Confidence distribution:** >=0.8: 8 (50%), 0.5–0.8: 8 (50%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 9/16 (56.2%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:pickup_at`: 8
  - `heuristic:duplicate_suspect`: 4

#### Flagged examples (prioritize fixes)

**File:** `Transactions/CarRentals/2017-10/hfa-car_rental-d5d7c1de0912.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1151150851
created: 2017-10-06
dropoff_at: and port of entry.
extraction_confidence: 0.67
source: ["email_extraction"]
source_email: [[hfa-email-message-6b280c8e7a1e]]
source_id: hfa-car_rental-d5d7c1de0912
summary: National
type: car_rental
uid: hfa-car_rental-d5d7c1de0912
updated: 2017-10-06
```

Body (truncated):

```markdown
# National rental 1151150851
```

---

**File:** `Transactions/CarRentals/2018-04/hfa-car_rental-9aef90112ff9.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 1163638511
created: 2018-04-22
dropoff_at: and port of entry.
extraction_confidence: 0.67
source: ["email_extraction"]
source_email: [[hfa-email-message-5f8be5684c6c]]
source_id: hfa-car_rental-9aef90112ff9
summary: National
type: car_rental
uid: hfa-car_rental-9aef90112ff9
updated: 2018-04-22
```

Body (truncated):

```markdown
# National rental 1163638511
```

---

**File:** `Transactions/CarRentals/2024-07/hfa-car_rental-d19f237bd3f9.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1664360407
created: 2024-07-31
dropoff_location: LOS ANGELES INTL ARPT ( LAX ) Sun, August 4, 2024 3:00 PM
extraction_confidence: 0.67
pickup_location: LA ONTARIO INTL ARPT ( ONT ) Tue, July 30, 2024 11:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-abb327ff17d1]]
source_id: hfa-car_rental-d19f237bd3f9
summary: National
type: car_rental
uid: hfa-car_rental-d19f237bd3f9
updated: 2024-07-31
```

Body (truncated):

```markdown
# National rental 1664360407
```

---

**File:** `Transactions/CarRentals/2017-03/hfa-car_rental-a99ebccdcfec.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 285386197
created: 2017-03-06
dropoff_at: and port of entry. Renters using a debit card or money order as a deposit may re
extraction_confidence: 0.67
source: ["email_extraction"]
source_email: [[hfa-email-message-e33b21f2198c]]
source_id: hfa-car_rental-a99ebccdcfec
summary: National
type: car_rental
uid: hfa-car_rental-a99ebccdcfec
updated: 2017-03-06
```

Body (truncated):

```markdown
# National rental 285386197
```

---

**File:** `Transactions/CarRentals/2018-11/hfa-car_rental-cc853ea12c52.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 1177578825
created: 2018-11-12
dropoff_at: and port of entry. DEPOSIT AMOUNT – A major credit card or debit card in
dropoff_location: LOS ANGELES INTL ARPT ( LAX ) Tue, December 4, 2018 12:00 PM
extraction_confidence: 0.67
pickup_location: LOS ANGELES INTL ARPT ( LAX ) Fri, November 30, 2018 12:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-72a708e066ed]]
source_id: hfa-car_rental-cc853ea12c52
summary: National
type: car_rental
uid: hfa-car_rental-cc853ea12c52
updated: 2018-11-12
```

Body (truncated):

```markdown
# National rental 1177578825
```

---

**File:** `Transactions/CarRentals/2023-08/hfa-car_rental-25300382cb7e.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: Hertz
confirmation_code: K5881551950
created: 2023-08-18
extraction_confidence: 0.67
source: ["email_extraction"]
source_email: [[hfa-email-message-f005c969477b]]
source_id: hfa-car_rental-25300382cb7e
summary: Hertz
type: car_rental
uid: hfa-car_rental-25300382cb7e
updated: 2023-08-18
```

Body (truncated):

```markdown
# Hertz rental K5881551950
```

---

**File:** `Transactions/CarRentals/2015-07/hfa-car_rental-5e3ef1ea53e8.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 1100884967
created: 2015-07-30
extraction_confidence: 0.67
pickup_location: your car from National
source: ["email_extraction"]
source_email: [[hfa-email-message-d5284fa69d41]]
source_id: hfa-car_rental-5e3ef1ea53e8
summary: National
type: car_rental
uid: hfa-car_rental-5e3ef1ea53e8
updated: 2015-07-30
```

Body (truncated):

```markdown
# National rental 1100884967
```

---

**File:** `Transactions/CarRentals/2024-07/hfa-car_rental-85d4f0996439.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1664360407
created: 2024-07-17
dropoff_location: LOS ANGELES INTL ARPT ( LAX ) Sun, August 4, 2024 3:00 PM
extraction_confidence: 1.0
pickup_at: July 30, 2024
pickup_location: LA ONTARIO INTL ARPT ( ONT ) Tue, July 30, 2024 11:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-32d47433ae07]]
source_id: hfa-car_rental-85d4f0996439
summary: National rental July 30, 2
type: car_rental
uid: hfa-car_rental-85d4f0996439
updated: 2024-07-17
```

Body (truncated):

```markdown
# National rental 1664360407
```

---

#### Clean examples (quality bar)

**File:** `Transactions/CarRentals/2022-01/hfa-car_rental-4a0ef356df00.md`

```yaml
company: National
confirmation_code: 1334785108
created: 2022-01-25
dropoff_location: SALT LAKE CITY-NATIONAL ( SLC ) Sun, February 6, 2022 2:00 PM
extraction_confidence: 1.0
pickup_at: February 3, 2022
pickup_location: SALT LAKE CITY-NATIONAL ( SLC ) Thu, February 3, 2022 5:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-476385a683d0]]
source_id: hfa-car_rental-4a0ef356df00
summary: National rental February 3
type: car_rental
uid: hfa-car_rental-4a0ef356df00
updated: 2022-01-25
```

Body (truncated):

```markdown
# National rental 1334785108
```

---

**File:** `Transactions/CarRentals/2021-07/hfa-car_rental-fba2f7e0ffe8.md`

```yaml
company: National
confirmation_code: 1881264760
created: 2021-07-03
dropoff_location: NAPLES CAPODICHINO ARPT ( NAP ) Sat, July 10, 2021 9:00 AM
extraction_confidence: 1.0
pickup_at: July 4, 2021
pickup_location: NAPLES CAPODICHINO ARPT ( NAP ) Sun, July 4, 2021 2:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-5fab73a04050]]
source_id: hfa-car_rental-fba2f7e0ffe8
summary: National rental July 4, 20
type: car_rental
uid: hfa-car_rental-fba2f7e0ffe8
updated: 2021-07-03
```

Body (truncated):

```markdown
# National rental 1881264760
```

---

**File:** `Transactions/CarRentals/2021-05/hfa-car_rental-164cfd5b3c9d.md`

```yaml
company: National
confirmation_code: 1626267553
created: 2021-05-24
dropoff_location: MIAMI INTL ARPT ( MIA ) Sun, June 6, 2021 12:00 PM
extraction_confidence: 1.0
pickup_at: June 3, 2021
pickup_location: MIAMI INTL ARPT ( MIA ) Thu, June 3, 2021 4:30 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-19a7ed1bbb33]]
source_id: hfa-car_rental-164cfd5b3c9d
summary: National rental June 3, 20
type: car_rental
uid: hfa-car_rental-164cfd5b3c9d
updated: 2021-05-24
```

Body (truncated):

```markdown
# National rental 1626267553
```

---

**File:** `Transactions/CarRentals/2018-09/hfa-car_rental-ca5fdf91fa98.md`

```yaml
company: National
confirmation_code: 1173787757
created: 2018-09-20
extraction_confidence: 1.0
pickup_at: Friday, September 21, 2018 4:00 PM Pick Up Rental Office Address and Pho
pickup_location: your car from National
source: ["email_extraction"]
source_email: [[hfa-email-message-2a6743873f95]]
source_id: hfa-car_rental-ca5fdf91fa98
summary: National rental Friday, Se
type: car_rental
uid: hfa-car_rental-ca5fdf91fa98
updated: 2018-09-20
```

Body (truncated):

```markdown
# National rental 1173787757
```

---

### `flight` (145 cards)

_Typical extractors:_ `united`

- **Confidence distribution:** >=0.8: 145 (100%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 90/145 (62.1%)
- **Flag counts (cards can have multiple):**
  - `heuristic:duplicate_suspect`: 84
  - `heuristic:round_trip_fail:fare_amount`: 11

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Flights/2016-06/hfa-flight-cf7897e3986a.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: LLX8GC
created: 2016-06-28
destination_airport: PDX
extraction_confidence: 1.0
fare_amount: 229.76
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-51a84602d33d]]
source_id: hfa-flight-cf7897e3986a
summary: United SFO to PDX
type: flight
uid: hfa-flight-cf7897e3986a
updated: 2016-06-28
```

Body (truncated):

```markdown
# United SFO→PDX LLX8GC
```

---

**File:** `Transactions/Flights/2016-07/hfa-flight-e7a42ebcc8df.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: PWQZJG
created: 2016-07-09
destination_airport: CDG
extraction_confidence: 1.0
origin_airport: CDG
source: ["email_extraction"]
source_email: [[hfa-email-message-f717dc1a572e]]
source_id: hfa-flight-e7a42ebcc8df
summary: United CDG to CDG
type: flight
uid: hfa-flight-e7a42ebcc8df
updated: 2016-07-09
```

Body (truncated):

```markdown
# United CDG→CDG PWQZJG
```

---

**File:** `Transactions/Flights/2015-04/hfa-flight-a07623eeb0c2.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: GQMB9Z
created: 2015-04-05
destination_airport: DEN
extraction_confidence: 1.0
fare_amount: 568.37
origin_airport: SJC
source: ["email_extraction"]
source_email: [[hfa-email-message-b7a6cbd7231a]]
source_id: hfa-flight-a07623eeb0c2
summary: United SJC to DEN
type: flight
uid: hfa-flight-a07623eeb0c2
updated: 2015-04-05
```

Body (truncated):

```markdown
# United SJC→DEN GQMB9Z
```

---

**File:** `Transactions/Flights/2025-02/hfa-flight-92c3ebb881c6.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: FR181D
created: 2025-02-10
departure_at: gate with their boarding pass at least 15 minutes prior to scheduled departure.
destination_airport: DEN
extraction_confidence: 1.0
fare_amount: 161.1
origin_airport: LGA
source: ["email_extraction"]
source_email: [[hfa-email-message-c79a38c637f2]]
source_id: hfa-flight-92c3ebb881c6
summary: United LGA to DEN
type: flight
uid: hfa-flight-92c3ebb881c6
updated: 2025-02-10
```

Body (truncated):

```markdown
# United LGA→DEN FR181D
```

---

**File:** `Transactions/Flights/2012-05/hfa-flight-e468c3cd27a3.md`
**Flags:** `heuristic:duplicate_suspect`

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

**File:** `Transactions/Flights/2013-03/hfa-flight-e59be9a8caf7.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: LWZXHB
created: 2013-03-04
departure_at: time of 9:00 p.m., as the departure time could be revised again. Information is
destination_airport: LAX
extraction_confidence: 1.0
origin_airport: LAX
source: ["email_extraction"]
source_email: [[hfa-email-message-3a3c90ce3b61]]
source_id: hfa-flight-e59be9a8caf7
summary: United LAX to LAX
type: flight
uid: hfa-flight-e59be9a8caf7
updated: 2013-03-04
```

Body (truncated):

```markdown
# United LAX→LAX LWZXHB
```

---

**File:** `Transactions/Flights/2018-12/hfa-flight-715d086e24f1.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: MZJGVS
created: 2018-12-04
destination_airport: LAX
extraction_confidence: 1.0
fare_amount: 89.3
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-f00ac5c8c08c]]
source_id: hfa-flight-715d086e24f1
summary: United SFO to LAX
type: flight
uid: hfa-flight-715d086e24f1
updated: 2018-12-04
```

Body (truncated):

```markdown
# United SFO→LAX MZJGVS
```

---

**File:** `Transactions/Flights/2022-04/hfa-flight-e5204486345b.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: PS6JFR
created: 2022-04-22
destination_airport: PSP
extraction_confidence: 1.0
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-c8476671644b]]
source_id: hfa-flight-e5204486345b
summary: United SFO to PSP
type: flight
uid: hfa-flight-e5204486345b
updated: 2022-04-22
```

Body (truncated):

```markdown
# United SFO→PSP PS6JFR
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Flights/2014-08/hfa-flight-689761c1a608.md`

```yaml
airline: United
booking_source: United
confirmation_code: MYG0MP
created: 2014-08-15
destination_airport: ORD
extraction_confidence: 1.0
fare_amount: 391.6
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-2ef11c9b2f70]]
source_id: hfa-flight-689761c1a608
summary: United SFO to ORD
type: flight
uid: hfa-flight-689761c1a608
updated: 2014-08-15
```

Body (truncated):

```markdown
# United SFO→ORD MYG0MP
```

---

**File:** `Transactions/Flights/2019-05/hfa-flight-a034124cc1c1.md`

```yaml
airline: United
booking_source: United
confirmation_code: GZWJN5
created: 2019-05-14
destination_airport: LAX
extraction_confidence: 1.0
fare_amount: 541.2
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-975bfa445ef1]]
source_id: hfa-flight-a034124cc1c1
summary: United SFO to LAX
type: flight
uid: hfa-flight-a034124cc1c1
updated: 2019-05-14
```

Body (truncated):

```markdown
# United SFO→LAX GZWJN5
```

---

**File:** `Transactions/Flights/2021-04/hfa-flight-09cdffdb54c6.md`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: CBB301
created: 2021-04-29
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: PDX
extraction_confidence: 1.0
fare_amount: 356.28
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-0cc26157a95e]]
source_id: hfa-flight-09cdffdb54c6
summary: United SFO to PDX
type: flight
uid: hfa-flight-09cdffdb54c6
updated: 2021-04-29
```

Body (truncated):

```markdown
# United SFO→PDX CBB301
```

---

**File:** `Transactions/Flights/2023-12/hfa-flight-45639b405130.md`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: JNVVVH
created: 2023-12-03
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: HNL
extraction_confidence: 1.0
fare_amount: 4797.99
origin_airport: EWR
source: ["email_extraction"]
source_email: [[hfa-email-message-f75e2f5ed0a9]]
source_id: hfa-flight-45639b405130
summary: United EWR to HNL
type: flight
uid: hfa-flight-45639b405130
updated: 2023-12-03
```

Body (truncated):

```markdown
# United EWR→HNL JNVVVH
```

---

### `grocery_order` (3 cards)

_Typical extractors:_ `instacart`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 3 (100%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 3/3 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 3
  - `heuristic:empty_items`: 3

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Groceries/2025-10/hfa-grocery_order-de91e7b6969a.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-10-04
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-653d1a09287e]]
source_id: hfa-grocery_order-de91e7b6969a
store: Wegmans
summary: Instacart order from Wegmans
total: 100.89
type: grocery_order
uid: hfa-grocery_order-de91e7b6969a
updated: 2025-10-04
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

**File:** `Transactions/Groceries/2025-08/hfa-grocery_order-05d143fea4d2.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-08-03
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-601492a7f5ef]]
source_id: hfa-grocery_order-05d143fea4d2
store: Wegmans
summary: Instacart order from Wegmans
total: 148.89
type: grocery_order
uid: hfa-grocery_order-05d143fea4d2
updated: 2025-08-03
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

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

### `meal_order` (236 cards)

_Typical extractors:_ `uber_eats`, `doordash`

- **Confidence distribution:** >=0.8: 34 (14%), 0.5–0.8: 143 (61%), <0.5: 59 (25%), n/a: 0

- **Share with ≥1 flag:** 203/236 (86.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 157
  - `heuristic:empty_items`: 157
  - `critical_fail:restaurant`: 104
  - `heuristic:round_trip_fail:mode`: 85
  - `heuristic:round_trip_fail:service`: 9
  - `heuristic:duplicate_suspect`: 2

#### Flagged examples (prioritize fixes)

**File:** `Transactions/MealOrders/2022-04/hfa-meal_order-87c2e86aa078.md`
**Flags:** `critical_fail:restaurant`, `critical_fail:items`, `heuristic:empty_items`, `heuristic:round_trip_fail:mode`

```yaml
created: 2022-04-26
extraction_confidence: 0.33
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-f2e92db5283c]]
source_id: hfa-meal_order-87c2e86aa078
summary: hfa-meal_order-87c2e86aa078
total: 35.38
type: meal_order
uid: hfa-meal_order-87c2e86aa078
updated: 2022-04-26
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
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

**File:** `Transactions/MealOrders/2021-06/hfa-meal_order-5d9396039519.md`
**Flags:** `critical_fail:restaurant`, `critical_fail:items`, `heuristic:empty_items`, `heuristic:round_trip_fail:mode`

```yaml
created: 2021-06-08
extraction_confidence: 0.33
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-2941c8ad5640]]
source_id: hfa-meal_order-5d9396039519
summary: hfa-meal_order-5d9396039519
total: 32.37
type: meal_order
uid: hfa-meal_order-5d9396039519
updated: 2021-06-08
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2022-10/hfa-meal_order-8585328840b5.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2022-10-07
extraction_confidence: 0.67
mode: delivery
restaurant: Chuko
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-909f977d816b]]
source_id: hfa-meal_order-8585328840b5
summary: DoorDash order from Chuko
total: 78.07
type: meal_order
uid: hfa-meal_order-8585328840b5
updated: 2022-10-07
```

Body (truncated):

```markdown
# DoorDash — Chuko

- **Total**: $78.07
```

---

**File:** `Transactions/MealOrders/2020-10/hfa-meal_order-fdf189df2b5e.md`
**Flags:** `critical_fail:restaurant`

```yaml
created: 2020-10-02
extraction_confidence: 0.67
items:
  [
    { "name": "spindrift half + half", "quantity": 2, "price": "2.95" },
    { "name": "chicken pesto parm", "quantity": 1, "price": "18.25" },
  ]
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-c9c806d47d25]]
source_id: hfa-meal_order-fdf189df2b5e
subtotal: 24.15
summary: hfa-meal_order-fdf189df2b5e
tax: 2.78
total: 29.34
type: meal_order
uid: hfa-meal_order-fdf189df2b5e
updated: 2020-10-02
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2022-01/hfa-meal_order-12af32917c77.md`
**Flags:** `critical_fail:restaurant`, `critical_fail:items`, `heuristic:empty_items`, `heuristic:round_trip_fail:mode`

```yaml
created: 2022-01-22
extraction_confidence: 0.33
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-ba3bd0bd5810]]
source_id: hfa-meal_order-12af32917c77
summary: hfa-meal_order-12af32917c77
total: 26.06
type: meal_order
uid: hfa-meal_order-12af32917c77
updated: 2022-01-22
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2021-11/hfa-meal_order-418ed3024a12.md`
**Flags:** `critical_fail:restaurant`, `critical_fail:items`, `heuristic:empty_items`, `heuristic:round_trip_fail:mode`

```yaml
created: 2021-11-10
extraction_confidence: 0.33
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-871b861e0ae1]]
source_id: hfa-meal_order-418ed3024a12
summary: hfa-meal_order-418ed3024a12
total: 50.9
type: meal_order
uid: hfa-meal_order-418ed3024a12
updated: 2021-11-10
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2023-01/hfa-meal_order-89be07e9d30a.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2023-01-25
extraction_confidence: 0.67
mode: delivery
restaurant: Cafe Mogador
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-7fb4b2d22172]]
source_id: hfa-meal_order-89be07e9d30a
summary: DoorDash order from Cafe Mogador
total: 50.04
type: meal_order
uid: hfa-meal_order-89be07e9d30a
updated: 2023-01-25
```

Body (truncated):

```markdown
# DoorDash — Cafe Mogador

- **Total**: $50.04
```

---

#### Clean examples (quality bar)

**File:** `Transactions/MealOrders/2020-11/hfa-meal_order-f6435376249a.md`

```yaml
created: 2020-11-24
extraction_confidence: 1.0
items: [{ "name": "Veggie Soft Tofu Stew (Stew & Soup)", "quantity": 1, "price": "" }]
mode: delivery
restaurant: Purple Rice
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-4b35919414ca]]
source_id: hfa-meal_order-f6435376249a
subtotal: 16.0
summary: DoorDash order from Purple Rice
tip: 4.0
total: 22.16
type: meal_order
uid: hfa-meal_order-f6435376249a
updated: 2020-11-24
```

Body (truncated):

```markdown
# DoorDash — Purple Rice

| Item                                | Qty | Price |
| ----------------------------------- | --- | ----- |
| Veggie Soft Tofu Stew (Stew & Soup) | 1   |       |

- **Subtotal**: $16.00
- **Delivery Fee**: $0.00
- **Tip**: $4.00
- **Total**: $22.16
```

---

**File:** `Transactions/MealOrders/2020-10/hfa-meal_order-1a0004605774.md`

```yaml
created: 2020-10-23
extraction_confidence: 1.0
items: [{ "name": "Quarter Chicken Combo", "quantity": 1, "price": "15.00" }]
mode: delivery
restaurant: Roasted Chicken by Sweetgreen
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-519f00ff5b1c]]
source_id: hfa-meal_order-1a0004605774
subtotal: 15.0
summary: Uber Eats order from Roasted Chicken by Sweetgreen
tax: 1.02
total: 19.34
type: meal_order
uid: hfa-meal_order-1a0004605774
updated: 2020-10-23
```

Body (truncated):

```markdown
# Uber Eats — Roasted Chicken by Sweetgreen
```

---

**File:** `Transactions/MealOrders/2020-10/hfa-meal_order-4b8f227572e6.md`

```yaml
created: 2020-10-28
extraction_confidence: 1.0
items:
  [
    { "name": "Silverware", "quantity": 1, "price": "0.00" },
    { "name": "Canned Diet Coke", "quantity": 1, "price": "3.10" },
    { "name": "Chicken Skewer", "quantity": 1, "price": "20.95" },
  ]
mode: delivery
restaurant: Oren's Hummus Shop - SF
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-cf3331dc38d8]]
source_id: hfa-meal_order-4b8f227572e6
subtotal: 24.05
summary: Uber Eats order from Oren's Hummus Shop - SF
tax: 2.1
total: 33.09
type: meal_order
uid: hfa-meal_order-4b8f227572e6
updated: 2020-10-28
```

Body (truncated):

```markdown
# Uber Eats — Oren's Hummus Shop - SF
```

---

**File:** `Transactions/MealOrders/2020-08/hfa-meal_order-cb8c38b2a7cd.md`

```yaml
created: 2020-08-17
extraction_confidence: 1.0
items: [{ "name": "ORCHARD", "quantity": 1, "price": "15.40" }]
mode: delivery
restaurant: Mixt - Valencia
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-a675408ef2bd]]
source_id: hfa-meal_order-cb8c38b2a7cd
subtotal: 15.4
summary: Uber Eats order from Mixt - Valencia
tax: 1.31
total: 26.45
type: meal_order
uid: hfa-meal_order-cb8c38b2a7cd
updated: 2020-08-17
```

Body (truncated):

```markdown
# Uber Eats — Mixt - Valencia
```

---

### `ride` (261 cards)

_Typical extractors:_ `uber_rides`, `lyft`

- **Confidence distribution:** >=0.8: 255 (98%), 0.5–0.8: 0 (0%), <0.5: 6 (2%), n/a: 0

- **Share with ≥1 flag:** 261/261 (100.0%)
- **Flag counts (cards can have multiple):**
  - `heuristic:round_trip_fail:pickup_at`: 261
  - `heuristic:round_trip_fail:duration_minutes`: 71
  - `heuristic:round_trip_fail:ride_type`: 35
  - `critical_fail:pickup_location`: 6
  - `critical_fail:dropoff_location`: 6
  - `heuristic:weak_pickup`: 6
  - `heuristic:weak_dropoff`: 6

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Rides/2015-02/hfa-ride-dd41082f6bc6.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2015-02-06
dropoff_location: 649 Jones Street, San Francisco, CA
extraction_confidence: 1.0
fare: 5.0
pickup_at: 2015-02-06T03:42:22Z
pickup_location: 2511-2599 Bush Street, San Francisco, CA
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-22d19363c73f]]
source_id: hfa-ride-dd41082f6bc6
summary: Uber from 2511-2599 Bush Street, San Francisco, CA to 649 Jones Street, San Francisco, CA
type: ride
uid: hfa-ride-dd41082f6bc6
updated: 2015-02-06
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2511-2599 Bush Street, San Francisco, CA
- **Dropoff**: 649 Jones Street, San Francisco, CA
- **Pickup at**: 2015-02-06T03:42:22Z
- **Fare**: 5.0
```

---

**File:** `Transactions/Rides/2016-02/hfa-ride-19b341835305.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`, `heuristic:round_trip_fail:duration_minutes`

```yaml
created: 2016-02-29
dropoff_location: 2919-2923 23rd St, San Francisco, CA
duration_minutes: 10.0
extraction_confidence: 1.0
fare: 7.29
pickup_at: 2016-02-29T04:25:08Z
pickup_location: 29-99 US-101, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-6f655a17d9d9]]
source_id: hfa-ride-19b341835305
summary: Uber from 29-99 US-101, San Francisco, CA to 2919-2923 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-19b341835305
updated: 2016-02-29
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 29-99 US-101, San Francisco, CA
- **Dropoff**: 2919-2923 23rd St, San Francisco, CA
- **Pickup at**: 2016-02-29T04:25:08Z
- **Fare**: 7.29
- **Duration (min)**: 10.0
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2014-02/hfa-ride-2f4367e7ff51.md`
**Flags:** `heuristic:round_trip_fail:ride_type`, `heuristic:round_trip_fail:pickup_at`, `heuristic:round_trip_fail:duration_minutes`

```yaml
created: 2014-02-08
distance_miles: 2.99
driver_name: zafar
dropoff_location: 1159-1187 Folsom Street, San Francisco, CA
duration_minutes: 12.0
extraction_confidence: 1.0
fare: 11.23
pickup_at: 2014-02-08T11:00:47Z
pickup_location: 2867-2899 Sacramento Street, San Francisco, CA
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-1a2d3d0c8c5f]]
source_id: hfa-ride-2f4367e7ff51
summary: Uber from 2867-2899 Sacramento Street, San Francisco, CA to 1159-1187 Folsom Street, San Francisco, CA
type: ride
uid: hfa-ride-2f4367e7ff51
updated: 2014-02-08
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2867-2899 Sacramento Street, San Francisco, CA
- **Dropoff**: 1159-1187 Folsom Street, San Francisco, CA
- **Pickup at**: 2014-02-08T11:00:47Z
- **Fare**: 11.23
- **Distance (mi)**: 2.99
- **Duration (min)**: 12.0
- **Driver**: zafar
```

---

**File:** `Transactions/Rides/2014-08/hfa-ride-b4cacc931cc0.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2014-08-21
dropoff_location: 2572 Pine Street, San Francisco, CA
extraction_confidence: 1.0
fare: 5.26
pickup_at: 2014-08-21T06:59:23Z
pickup_location: 1000 Steiner Street, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-25e9e44ae87c]]
source_id: hfa-ride-b4cacc931cc0
summary: Uber from 1000 Steiner Street, San Francisco, CA to 2572 Pine Street, San Francisco, CA
type: ride
uid: hfa-ride-b4cacc931cc0
updated: 2014-08-21
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 1000 Steiner Street, San Francisco, CA
- **Dropoff**: 2572 Pine Street, San Francisco, CA
- **Pickup at**: 2014-08-21T06:59:23Z
- **Fare**: 5.26
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-08/hfa-ride-a898356b8918.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2016-08-29
driver_name: Isam
dropoff_location: 2927 23rd St, San Francisco, CA
extraction_confidence: 1.0
fare: 14.38
pickup_at: 2016-08-29T00:45:32Z
pickup_location: 726 Clement St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-fc5b3cbe0774]]
source_id: hfa-ride-a898356b8918
summary: Uber from 726 Clement St, San Francisco, CA to 2927 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-a898356b8918
updated: 2016-08-29
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 726 Clement St, San Francisco, CA
- **Dropoff**: 2927 23rd St, San Francisco, CA
- **Pickup at**: 2016-08-29T00:45:32Z
- **Fare**: 14.38
- **Driver**: Isam
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2015-07/hfa-ride-61c5b65e17a4.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2015-07-31
dropoff_location: 400 Clipper Street, San Francisco, CA
extraction_confidence: 1.0
fare: 6.02
pickup_at: 2015-07-31T04:39:57Z
pickup_location: 84 29th Street, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-b3301360c4eb]]
source_id: hfa-ride-61c5b65e17a4
summary: Uber from 84 29th Street, San Francisco, CA to 400 Clipper Street, San Francisco, CA
type: ride
uid: hfa-ride-61c5b65e17a4
updated: 2015-07-31
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 84 29th Street, San Francisco, CA
- **Dropoff**: 400 Clipper Street, San Francisco, CA
- **Pickup at**: 2015-07-31T04:39:57Z
- **Fare**: 6.02
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2015-01/hfa-ride-046668bcfda1.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2015-01-15
dropoff_location: 1844-1898 Scott Street, San Francisco, CA
extraction_confidence: 1.0
fare: 6.27
pickup_at: 2015-01-15T06:39:48Z
pickup_location: 1697 Oak Street, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-e17ca5717ec0]]
source_id: hfa-ride-046668bcfda1
summary: Uber from 1697 Oak Street, San Francisco, CA to 1844-1898 Scott Street, San Francisco, CA
type: ride
uid: hfa-ride-046668bcfda1
updated: 2015-01-15
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 1697 Oak Street, San Francisco, CA
- **Dropoff**: 1844-1898 Scott Street, San Francisco, CA
- **Pickup at**: 2015-01-15T06:39:48Z
- **Fare**: 6.27
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2015-01/hfa-ride-e6cd2dd8add2.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2015-01-06
dropoff_location: 2590-2598 Pine Street, San Francisco, CA
extraction_confidence: 1.0
fare: 16.29
pickup_at: 2015-01-06T02:22:01Z
pickup_location: 484-488 29th Street, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-0eae513b09e8]]
source_id: hfa-ride-e6cd2dd8add2
summary: Uber from 484-488 29th Street, San Francisco, CA to 2590-2598 Pine Street, San Francisco, CA
type: ride
uid: hfa-ride-e6cd2dd8add2
updated: 2015-01-06
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 484-488 29th Street, San Francisco, CA
- **Dropoff**: 2590-2598 Pine Street, San Francisco, CA
- **Pickup at**: 2015-01-06T02:22:01Z
- **Fare**: 16.29
- **Vehicle**: uberX
```

---

### `shipment` (145 cards)

_Typical extractors:_ `shipping`

- **Confidence distribution:** >=0.8: 145 (100%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 61/145 (42.1%)
- **Flag counts (cards can have multiple):**
  - `heuristic:duplicate_suspect`: 47
  - `heuristic:round_trip_fail:carrier`: 16

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Shipments/2025-01/hfa-shipment-e9ec996e1847.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2025-01-06
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-d5acb775e045]]
source_id: hfa-shipment-e9ec996e1847
summary: UPS 1Z90WV170321662122
tracking_number: 1Z90WV170321662122
type: shipment
uid: hfa-shipment-e9ec996e1847
updated: 2025-01-06
```

Body (truncated):

```markdown
# Shipment UPS 1Z90WV170321662122
```

---

**File:** `Transactions/Shipments/2021-01/hfa-shipment-1227460ebce8.md`
**Flags:** `heuristic:round_trip_fail:carrier`

```yaml
carrier: FedEx
created: 2021-01-25
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-d097144f8b15]]
source_id: hfa-shipment-1227460ebce8
summary: FedEx 42139974881301
tracking_number: 42139974881301
type: shipment
uid: hfa-shipment-1227460ebce8
updated: 2021-01-25
```

Body (truncated):

```markdown
# Shipment FedEx 42139974881301
```

---

**File:** `Transactions/Shipments/2011-09/hfa-shipment-09de26fe9d20.md`
**Flags:** `heuristic:round_trip_fail:carrier`, `heuristic:duplicate_suspect`

```yaml
carrier: USPS
created: 2011-09-26
extraction_confidence: 1.0
shipped_at: the following item(s) in your order 105-6953047-5822647, placed on September 24,
source: ["email_extraction"]
source_email: [[hfa-email-message-bad51411e11e]]
source_id: hfa-shipment-09de26fe9d20
summary: USPS 9400110200881164572441
tracking_number: 9400110200881164572441
type: shipment
uid: hfa-shipment-09de26fe9d20
updated: 2011-09-26
```

Body (truncated):

```markdown
# Shipment USPS 9400110200881164572441
```

---

**File:** `Transactions/Shipments/2024-12/hfa-shipment-465b4baf0719.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2024-12-13
estimated_delivery: today. From GLF JAMES AND JAMES.
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-541ef5a220dc]]
source_id: hfa-shipment-465b4baf0719
summary: UPS 1Z4430800227530220
tracking_number: 1Z4430800227530220
type: shipment
uid: hfa-shipment-465b4baf0719
updated: 2024-12-13
```

Body (truncated):

```markdown
# Shipment UPS 1Z4430800227530220
```

---

**File:** `Transactions/Shipments/2024-06/hfa-shipment-755b397b806d.md`
**Flags:** `heuristic:round_trip_fail:carrier`

```yaml
carrier: FedEx
created: 2024-06-12
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-e0c3345b8c17]]
source_id: hfa-shipment-755b397b806d
summary: FedEx 23769769189025
tracking_number: 23769769189025
type: shipment
uid: hfa-shipment-755b397b806d
updated: 2024-06-12
```

Body (truncated):

```markdown
# Shipment FedEx 23769769189025
```

---

**File:** `Transactions/Shipments/2008-02/hfa-shipment-b05a005f293e.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2008-02-05
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-3d74c68f7766]]
source_id: hfa-shipment-b05a005f293e
summary: FedEx 790930846866
tracking_number: 790930846866
type: shipment
uid: hfa-shipment-b05a005f293e
updated: 2008-02-05
```

Body (truncated):

```markdown
# Shipment FedEx 790930846866
```

---

**File:** `Transactions/Shipments/2024-10/hfa-shipment-c5f7394f3568.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2024-10-16
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-7ffba802ef7c]]
source_id: hfa-shipment-c5f7394f3568
summary: FedEx 419923456226
tracking_number: 419923456226
type: shipment
uid: hfa-shipment-c5f7394f3568
updated: 2024-10-16
```

Body (truncated):

```markdown
# Shipment FedEx 419923456226
```

---

**File:** `Transactions/Shipments/2025-01/hfa-shipment-da8c9098b81b.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2025-01-04
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-44e861cea278]]
source_id: hfa-shipment-da8c9098b81b
summary: UPS 1Z90WV170331126344
tracking_number: 1Z90WV170331126344
type: shipment
uid: hfa-shipment-da8c9098b81b
updated: 2025-01-04
```

Body (truncated):

```markdown
# Shipment UPS 1Z90WV170331126344
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Shipments/2025-02/hfa-shipment-74b25a01d827.md`

```yaml
carrier: FedEx
created: 2025-02-21
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-96304dfd2d79]]
source_id: hfa-shipment-74b25a01d827
summary: FedEx 772224956650
tracking_number: 772224956650
type: shipment
uid: hfa-shipment-74b25a01d827
updated: 2025-02-21
```

Body (truncated):

```markdown
# Shipment FedEx 772224956650
```

---

**File:** `Transactions/Shipments/2020-03/hfa-shipment-94c84073484d.md`

```yaml
carrier: UPS
created: 2020-03-24
estimated_delivery: Date: Tuesday, 03/24/2020
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-9313192c45be]]
source_id: hfa-shipment-94c84073484d
summary: UPS 1Z0W37F11363669751
tracking_number: 1Z0W37F11363669751
type: shipment
uid: hfa-shipment-94c84073484d
updated: 2020-03-24
```

Body (truncated):

```markdown
# Shipment UPS 1Z0W37F11363669751
```

---

**File:** `Transactions/Shipments/2012-03/hfa-shipment-603f88b60ff3.md`

```yaml
carrier: UPS
created: 2012-03-08
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-0ee3c091688a]]
source_id: hfa-shipment-603f88b60ff3
summary: UPS 1Z161Y6V4294216963
tracking_number: 1Z161Y6V4294216963
type: shipment
uid: hfa-shipment-603f88b60ff3
updated: 2012-03-08
```

Body (truncated):

```markdown
# Shipment UPS 1Z161Y6V4294216963
```

---

**File:** `Transactions/Shipments/2024-02/hfa-shipment-ef15963aa324.md`

```yaml
carrier: UPS
created: 2024-02-16
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-ad28e551cebd]]
source_id: hfa-shipment-ef15963aa324
summary: UPS 1Z43A15E0390391539
tracking_number: 1Z43A15E0390391539
type: shipment
uid: hfa-shipment-ef15963aa324
updated: 2024-02-16
```

Body (truncated):

```markdown
# Shipment UPS 1Z43A15E0390391539
```

---

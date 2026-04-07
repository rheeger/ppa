# Extraction quality — `5pct` slice

Staging directory: `/Users/rheeger/Code/rheeger/ppa/_staging-5pct`

## Runner metrics (from `_metrics.json`)

- **Emails scanned:** 108103
- **Matched:** 1429
- **Cards extracted:** 389
- **Wall clock (s):** 562.6

| Extractor | matched | extracted | errors | skipped | rejected |
|-----------|--------:|----------:|-------:|--------:|---------:|
| airbnb | 126 | 16 | 0 | 0 | 20 |
| doordash | 278 | 26 | 0 | 0 | 160 |
| instacart | 19 | 0 | 0 | 0 | 0 |
| rental_cars | 21 | 10 | 0 | 0 | 3 |
| shipping | 165 | 62 | 0 | 0 | 0 |
| uber_eats | 95 | 26 | 0 | 0 | 15 |
| uber_rides | 584 | 175 | 0 | 0 | 0 |
| united | 141 | 74 | 0 | 0 | 0 |

**Yield (extracted / matched)** — low values often mean parse/skip inside extractor.
- `airbnb`: 0.127
- `doordash`: 0.094
- `instacart`: 0.000
- `rental_cars`: 0.476
- `shipping`: 0.376
- `uber_eats`: 0.274
- `uber_rides`: 0.300
- `united`: 0.525

## Volume + field population (staging scan)

| Type | Count | Expected | Status |
|------|------:|----------|--------|
| accommodation | 16 | 30-50 | LOW (below range 30-50) |
| car_rental | 10 | 10-30 | OK |
| flight | 74 | 50-100 | OK |
| meal_order | 52 | 1000-1500 | LOW (below 50% of lower bound 1000) |
| ride | 175 | 500-2000 | LOW (below 50% of lower bound 500) |
| shipment | 62 | 500-2000 | LOW (below 50% of lower bound 500) |

**Critical field population** (fraction of cards with field populated)

| Type | Field | Populated |
|------|-------|----------:|
| accommodation | check_in | 31.2% |
| accommodation | check_out | 31.2% |
| accommodation | confirmation_code | 100.0% |
| accommodation | property_name | 100.0% |
| car_rental | company | 100.0% |
| car_rental | confirmation_code | 100.0% |
| car_rental | pickup_at | 10.0% |
| flight | confirmation_code | 100.0% |
| flight | destination_airport | 100.0% |
| flight | origin_airport | 100.0% |
| meal_order | items | 0.0% |
| meal_order | restaurant | 100.0% |
| meal_order | total | 100.0% |
| ride | dropoff_location | 100.0% |
| ride | fare | 100.0% |
| ride | pickup_location | 100.0% |
| shipment | carrier | 100.0% |
| shipment | tracking_number | 100.0% |

**Warnings:**
- accommodation: volume LOW (below range 30-50) (count=16)
- meal_order: volume LOW (below 50% of lower bound 1000) (count=52)
- ride: volume LOW (below 50% of lower bound 500) (count=175)
- shipment: volume LOW (below 50% of lower bound 500) (count=62)

## Per card type — flags, rates, and examples

Flags combine **critical_fail:** (strict predicate from `field_metrics.CRITICAL_FIELDS`) and **heuristic:** (common junk patterns). Use flagged rows first when upgrading extractors.

### `accommodation` (16 cards)
*Typical extractors:* `airbnb`

- **Share with ≥1 flag:** 13/16 (81.2%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:check_in`: 11
  - `heuristic:missing_check_in`: 11
  - `critical_fail:check_out`: 11
  - `heuristic:missing_check_out`: 11

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Accommodations/2021-09/hfa-accommodation-d462a5d6d6d6.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
address: 3380 Vigilante Rd, Sonoma, CA 95476, United States
booking_source: Airbnb
confirmation_code: 17037578
created: 2021-09-15
property_name: to Sonoma.
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-401470c037c8]]
source_id: hfa-accommodation-d462a5d6d6d6
summary: hfa-accommodation-d462a5d6d6d6
type: accommodation
uid: hfa-accommodation-d462a5d6d6d6
updated: 2021-09-15
```

Body (truncated):

```markdown
# Airbnb to Sonoma.
```

---

**File:** `Transactions/Accommodations/2021-09/hfa-accommodation-bc43d76886a8.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
booking_source: Airbnb
confirmation_code: VACATION
created: 2021-09-27
property_name: TURNKEY VACATION RENTALS’S PLACE?
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-dcbe2be6d72e]]
source_id: hfa-accommodation-bc43d76886a8
summary: hfa-accommodation-bc43d76886a8
type: accommodation
uid: hfa-accommodation-bc43d76886a8
updated: 2021-09-27
```

Body (truncated):

```markdown
# Airbnb TURNKEY VACATION RENTALS’S PLACE?
```

---

**File:** `Transactions/Accommodations/2019-07/hfa-accommodation-5c42e4263a1c.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
address: 38229 Greenvale Close, Sea Ranch, CA 95497, United States
booking_source: Airbnb
confirmation_code: 36524772
created: 2019-07-11
property_name: w/ private hot tub, blue ocean view & shared pools/saunas!
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-93ddaa91c58e]]
source_id: hfa-accommodation-5c42e4263a1c
summary: hfa-accommodation-5c42e4263a1c
type: accommodation
uid: hfa-accommodation-5c42e4263a1c
updated: 2019-07-11
```

Body (truncated):

```markdown
# Airbnb w/ private hot tub, blue ocean view & shared pools/saunas!
```

---

**File:** `Transactions/Accommodations/2022-06/hfa-accommodation-eea29c8d0732.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
booking_source: Airbnb
confirmation_code: 25029501
created: 2022-06-16
property_name: St. Helena.%opentrack%
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-7d7b90479e87]]
source_id: hfa-accommodation-eea29c8d0732
summary: hfa-accommodation-eea29c8d0732
type: accommodation
uid: hfa-accommodation-eea29c8d0732
updated: 2022-06-16
```

Body (truncated):

```markdown
# Airbnb St. Helena.%opentrack%
```

---

**File:** `Transactions/Accommodations/2024-04/hfa-accommodation-e52e08dd2963.md`
**Flags:** `critical_fail:check_in`, `heuristic:missing_check_in`

```yaml
booking_source: Airbnb
check_out: these homes for your upcoming trip
confirmation_code: 54112595
created: 2024-04-30
property_name: in Roma Sur ★5.0Rating 5 out of 5; 5 reviews Home in Roma Norte ★4.99Rating 4.99 out of 5; 86 reviews
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-05b72aed16cf]]
source_id: hfa-accommodation-e52e08dd2963
summary: hfa-accommodation-e52e08dd2963
type: accommodation
uid: hfa-accommodation-e52e08dd2963
updated: 2024-04-30
```

Body (truncated):

```markdown
# Airbnb in Roma Sur                                                                                                                                                                                                                                                                                                                                                                                                                             ★5.0Rating 5 out of 5; 5 reviews   Home in Roma Norte                                                                                                                                                                                                                                                                                                                                                                                             ★4.99Rating 4.99 out of 5; 86 reviews
```

---

**File:** `Transactions/Accommodations/2021-05/hfa-accommodation-006e995f9427.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
booking_source: Airbnb
confirmation_code: 765188312
created: 2021-05-31
property_name: RAYMOND'S PLACE
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-4cd219604811]]
source_id: hfa-accommodation-006e995f9427
summary: hfa-accommodation-006e995f9427
type: accommodation
uid: hfa-accommodation-006e995f9427
updated: 2021-05-31
```

Body (truncated):

```markdown
# Airbnb RAYMOND'S PLACE
```

---

**File:** `Transactions/Accommodations/2022-12/hfa-accommodation-d4f63ef5042b.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
booking_source: Airbnb
confirmation_code: 17497419
created: 2022-12-12
property_name: Vancouver.
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-32fed4399c11]]
source_id: hfa-accommodation-d4f63ef5042b
summary: hfa-accommodation-d4f63ef5042b
type: accommodation
uid: hfa-accommodation-d4f63ef5042b
updated: 2022-12-12
```

Body (truncated):

```markdown
# Airbnb Vancouver.
```

---

**File:** `Transactions/Accommodations/2022-06/hfa-accommodation-ddadb06bcde6.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
booking_source: Airbnb
confirmation_code: 51891431
created: 2022-06-07
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

#### Clean examples (quality bar)

**File:** `Transactions/Accommodations/2023-07/hfa-accommodation-155a8c3b7ec3.md`

```yaml
address: Via Femminamorta, 99, 00123 Roma RM, Italy
booking_source: Airbnb
check_in: Checkout
check_out: SUN, JUL 9 FRI, JUL 14
confirmation_code: 22144316
created: 2023-07-01
property_name: No carbon monoxide alarm
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-5c1b5148dc71]]
source_id: hfa-accommodation-155a8c3b7ec3
summary: No carbon monoxide alarm Checkout to SUN, JUL 9 FRI, JUL 14
type: accommodation
uid: hfa-accommodation-155a8c3b7ec3
updated: 2023-07-01
```

Body (truncated):

```markdown
# Airbnb No carbon monoxide alarm
```

---

**File:** `Transactions/Accommodations/2026-01/hfa-accommodation-3feb610a7311.md`

```yaml
booking_source: Airbnb
check_in: Checkout
check_out: SUNDAY SATURDAY
confirmation_code: 2394348729
created: 2026-01-02
property_name: on Airb&b for Unit
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-a3e1f01c5b81]]
source_id: hfa-accommodation-3feb610a7311
summary: on Airb&b for Unit Checkout to SUNDAY SATURDAY
type: accommodation
uid: hfa-accommodation-3feb610a7311
updated: 2026-01-02
```

Body (truncated):

```markdown
# Airbnb on Airb&b for Unit
```

---

**File:** `Transactions/Accommodations/2021-05/hfa-accommodation-ceffa0a75c5b.md`

```yaml
address: 21885 Bonness Rd, Sonoma, CA 95476, USA
booking_source: Airbnb
check_in: is 4:00 PM - 12:00 AM
check_out: by 11:00 AM
confirmation_code: 27312024
created: 2021-05-05
property_name: has self check-in. Instructions on how to access will be visible in the itinerary 3 days before checkin date.
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-06d64730b17d]]
source_id: hfa-accommodation-ceffa0a75c5b
summary: has self check-in. Instructions on how to access will be visible in the itinerary 3 days before checkin date. is 4:00 PM - 12:00 AM to by 11:00 AM
type: accommodation
uid: hfa-accommodation-ceffa0a75c5b
updated: 2021-05-05
```

Body (truncated):

```markdown
# Airbnb has self check-in. Instructions on how to access will be visible in the itinerary 3 days before checkin date.
```

---

### `car_rental` (10 cards)
*Typical extractors:* `rental_cars`

- **Share with ≥1 flag:** 9/10 (90.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:pickup_at`: 9

#### Flagged examples (prioritize fixes)

**File:** `Transactions/CarRentals/2021-11/hfa-car_rental-29072662eb36.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 1532645249
created: 2021-11-21
dropoff_location: of your vehicle, a National representative will assist you with transportation options for your return back to the terminal - Please allow yourself additional time upon arrival and return due to La...
pickup_location: LAGUARDIA ARPT ( LGA ) Wed, November 24, 2021 12:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-e4e3f938eb34]]
source_id: hfa-car_rental-29072662eb36
summary: National
type: car_rental
uid: hfa-car_rental-29072662eb36
updated: 2021-11-21
```

Body (truncated):

```markdown
# National rental 1532645249
```

---

**File:** `Transactions/CarRentals/2022-01/hfa-car_rental-4a0ef356df00.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 1334785108
created: 2022-01-25
dropoff_location: SALT LAKE CITY-NATIONAL ( SLC ) Sun, February 6, 2022 2:00 PM
pickup_location: SALT LAKE CITY-NATIONAL ( SLC ) Thu, February 3, 2022 5:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-476385a683d0]]
source_id: hfa-car_rental-4a0ef356df00
summary: National
type: car_rental
uid: hfa-car_rental-4a0ef356df00
updated: 2022-01-25
```

Body (truncated):

```markdown
# National rental 1334785108
```

---

**File:** `Transactions/CarRentals/2021-02/hfa-car_rental-238c5555b5ed.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: THAT
created: 2021-02-11
source: ["email_extraction"]
source_email: [[hfa-email-message-d76f82e08c46]]
source_id: hfa-car_rental-238c5555b5ed
summary: National
type: car_rental
uid: hfa-car_rental-238c5555b5ed
updated: 2021-02-11
```

Body (truncated):

```markdown
# National rental THAT
```

---

**File:** `Transactions/CarRentals/2017-10/hfa-car_rental-0e68ef85cfba.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: CONFIRMATION
created: 2017-10-06
dropoff_at: and port of entry.
dropoff_location: Boston Logan Intl Airport (BOS) October 9, 2017 04:00 PM Rates, Taxes and Fees Rental Rate 3 Day(s) @33.00 $ 99.00 9 Additional Drivers INCLUDED Coverages Add-Ons Mileage UNLIMITED MILEAGE INCLUDED...
pickup_location: area. Go out the terminal doors marked *Bus Stop* Board shuttle bus which runs every 5 minutes Please proceed to the counter to obtain your rental agreement. Your Information Driver Name ROBERT HEE...
source: ["email_extraction"]
source_email: [[hfa-email-message-92cf57d72a28]]
source_id: hfa-car_rental-0e68ef85cfba
summary: National
type: car_rental
uid: hfa-car_rental-0e68ef85cfba
updated: 2017-10-06
```

Body (truncated):

```markdown
# National rental CONFIRMATION
```

---

**File:** `Transactions/CarRentals/2021-07/hfa-car_rental-fba2f7e0ffe8.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 1881264760
created: 2021-07-03
dropoff_location: NAPLES CAPODICHINO ARPT ( NAP ) Sat, July 10, 2021 9:00 AM
pickup_location: NAPLES CAPODICHINO ARPT ( NAP ) Sun, July 4, 2021 2:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-5fab73a04050]]
source_id: hfa-car_rental-fba2f7e0ffe8
summary: National
type: car_rental
uid: hfa-car_rental-fba2f7e0ffe8
updated: 2021-07-03
```

Body (truncated):

```markdown
# National rental 1881264760
```

---

**File:** `Transactions/CarRentals/2021-07/hfa-car_rental-c2b83a3083ef.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: THAT
created: 2021-07-03
source: ["email_extraction"]
source_email: [[hfa-email-message-8c6c9c643700]]
source_id: hfa-car_rental-c2b83a3083ef
summary: National
type: car_rental
uid: hfa-car_rental-c2b83a3083ef
updated: 2021-07-03
```

Body (truncated):

```markdown
# National rental THAT
```

---

**File:** `Transactions/CarRentals/2024-07/hfa-car_rental-85d4f0996439.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: 1664360407
created: 2024-07-17
dropoff_location: LOS ANGELES INTL ARPT ( LAX ) Sun, August 4, 2024 3:00 PM
pickup_location: LA ONTARIO INTL ARPT ( ONT ) Tue, July 30, 2024 11:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-32d47433ae07]]
source_id: hfa-car_rental-85d4f0996439
summary: National
type: car_rental
uid: hfa-car_rental-85d4f0996439
updated: 2024-07-17
```

Body (truncated):

```markdown
# National rental 1664360407
```

---

**File:** `Transactions/CarRentals/2021-07/hfa-car_rental-2df93edfb40b.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: National
confirmation_code: THAT
created: 2021-07-03
source: ["email_extraction"]
source_email: [[hfa-email-message-54a6b9182517]]
source_id: hfa-car_rental-2df93edfb40b
summary: National
type: car_rental
uid: hfa-car_rental-2df93edfb40b
updated: 2021-07-03
```

Body (truncated):

```markdown
# National rental THAT
```

---

#### Clean examples (quality bar)

**File:** `Transactions/CarRentals/2018-09/hfa-car_rental-ca5fdf91fa98.md`

```yaml
company: National
confirmation_code: 1173787757
created: 2018-09-20
pickup_at: Friday, September 21, 2018 4:00 PM Pick Up Rental Office Address and Pho
pickup_location: your car from National, please click on the link below to cancel your reservation. Your confirmation number is: 1173787757
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

### `flight` (74 cards)
*Typical extractors:* `united`

- **Share with ≥1 flag:** 0/74 (0.0%)

#### Clean examples (quality bar)

**File:** `Transactions/Flights/2025-02/hfa-flight-92c3ebb881c6.md`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: FR181D
created: 2025-02-10
departure_at: gate with their boarding pass at least 15 minutes prior to scheduled departure.
destination_airport: CSS
fare_amount: 161.1
origin_airport: COM
source: ["email_extraction"]
source_email: [[hfa-email-message-c79a38c637f2]]
source_id: hfa-flight-92c3ebb881c6
summary: United COM to CSS
type: flight
uid: hfa-flight-92c3ebb881c6
updated: 2025-02-10
```

Body (truncated):

```markdown
# United COM→CSS FR181D
```

---

**File:** `Transactions/Flights/2023-01/hfa-flight-04da0daf259e.md`

```yaml
airline: United
booking_source: United
confirmation_code: PK30W7
created: 2023-01-03
destination_airport: MER
fare_amount: 1379.7
origin_airport: CUS
source: ["email_extraction"]
source_email: [[hfa-email-message-4ac7bad81c7f]]
source_id: hfa-flight-04da0daf259e
summary: United CUS to MER
type: flight
uid: hfa-flight-04da0daf259e
updated: 2023-01-03
```

Body (truncated):

```markdown
# United CUS→MER PK30W7
```

---

**File:** `Transactions/Flights/2023-01/hfa-flight-c1e0a78996b7.md`

```yaml
airline: United
arrival_at: AKL: Sun, Sep 3
booking_source: United
confirmation_code: P1R66B
created: 2023-01-04
departure_at: SFO: Fri, Sep 1
destination_airport: MER
fare_amount: 540000.0
origin_airport: CUS
source: ["email_extraction"]
source_email: [[hfa-email-message-192ff80cfa18]]
source_id: hfa-flight-c1e0a78996b7
summary: United CUS to MER
type: flight
uid: hfa-flight-c1e0a78996b7
updated: 2023-01-04
```

Body (truncated):

```markdown
# United CUS→MER P1R66B
```

---

**File:** `Transactions/Flights/2013-02/hfa-flight-6448e725aac3.md`

```yaml
airline: United
arrival_at: 11:18 a.m.Travel Time:<span class="PHead">
booking_source: United
confirmation_code: LWZXHB
created: 2013-02-28
departure_at: time.</b>
destination_airport: WEB
origin_airport: COM
source: ["email_extraction"]
source_email: [[hfa-email-message-b0542d45c150]]
source_id: hfa-flight-6448e725aac3
summary: United COM to WEB
type: flight
uid: hfa-flight-6448e725aac3
updated: 2013-02-28
```

Body (truncated):

```markdown
# United COM→WEB LWZXHB
```

---

### `meal_order` (52 cards)
*Typical extractors:* `uber_eats`, `doordash`

- **Share with ≥1 flag:** 52/52 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 52
  - `heuristic:empty_items`: 52
  - `heuristic:url_in_restaurant`: 23
  - `heuristic:footer_noise_restaurant`: 19

#### Flagged examples (prioritize fixes)

**File:** `Transactions/MealOrders/2021-03/hfa-meal_order-ef5ab14a1746.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2021-03-24
mode: delivery
restaurant: your bank statement shortly.
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-8fc3d115c866]]
source_id: hfa-meal_order-ef5ab14a1746
subtotal: 21.45
summary: Uber Eats order from your bank statement shortly.
tax: 1.82
total: 21.45
type: meal_order
uid: hfa-meal_order-ef5ab14a1746
updated: 2021-03-24
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2020-09/hfa-meal_order-7aa041b12868.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2020-09-17
mode: delivery
restaurant: Mixt - Valencia Picked up from 903 Valencia St, San Francisco, CA 94110, USA Delivered to 94 Jack London Alley, San Francisco, CA 94107, USA Delivered by Brandon Rate order Rate order .eats_footer_...
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-2ad2f642af62]]
source_id: hfa-meal_order-7aa041b12868
subtotal: 16.62
summary: Uber Eats order from Mixt - Valencia Picked up from 903 Valencia St, San Francisco, CA 94110, USA Delivered to 94 Jack London Alley, San Francisco, CA 94107, USA Delivered by Brandon Rate order Rate order .eats_footer_table{width:100%!important} Contact support
tax: 1.41
total: 16.62
type: meal_order
uid: hfa-meal_order-7aa041b12868
updated: 2020-09-17
```

Body (truncated):

```markdown
# Uber Eats — Mixt - Valencia                     Picked up from    903 Valencia St, San Francisco, CA 94110, USA                              Delivered to    94 Jack London Alley, San Francisco, CA 94107, USA                                                                                             Delivered by Brandon                        Rate order             Rate order                                                                       .eats_footer_table{width:100%!important}                 Contact support
```

---

**File:** `Transactions/MealOrders/2023-02/hfa-meal_order-45693c42fa6d.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2023-02-14
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-be428b7ab5df]]
source_id: hfa-meal_order-45693c42fa6d
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
total: 65.82
type: meal_order
uid: hfa-meal_order-45693c42fa6d
updated: 2023-02-14
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Total**: $65.82
```

---

**File:** `Transactions/MealOrders/2020-10/hfa-meal_order-74e7947306a1.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2020-10-12
mode: delivery
restaurant: your bank statement shortly.
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-fe641c91f02e]]
source_id: hfa-meal_order-74e7947306a1
subtotal: 27.15
summary: Uber Eats order from your bank statement shortly.
tax: 2.31
total: 27.15
type: meal_order
uid: hfa-meal_order-74e7947306a1
updated: 2020-10-12
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2020-11/hfa-meal_order-c260e3ce0ee2.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2020-11-10
mode: delivery
restaurant: Roasted Chicken by Sweetgreen Picked up from 60 Morris St, San Francisco, CA 94107, USA Delivered to 94 Jack London Alley, San Francisco, CA 94107, USA Delivered by Albaraa Rate order Rate order .e...
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-d9e9f00a0e9c]]
source_id: hfa-meal_order-c260e3ce0ee2
subtotal: 17.75
summary: Uber Eats order from Roasted Chicken by Sweetgreen Picked up from 60 Morris St, San Francisco, CA 94107, USA Delivered to 94 Jack London Alley, San Francisco, CA 94107, USA Delivered by Albaraa Rate order Rate order .eats_footer_table{width:100%!important} Contact support
tax: 1.51
total: 17.75
type: meal_order
uid: hfa-meal_order-c260e3ce0ee2
updated: 2020-11-10
```

Body (truncated):

```markdown
# Uber Eats — Roasted Chicken by Sweetgreen                     Picked up from    60 Morris St, San Francisco, CA 94107, USA                              Delivered to    94 Jack London Alley, San Francisco, CA 94107, USA                                                                                             Delivered by Albaraa                        Rate order             Rate order                                                                       .eats_footer_table{width:100%!important}                 Contact support
```

---

**File:** `Transactions/MealOrders/2020-12/hfa-meal_order-b8ba06982765.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2020-12-20
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-af3cca6dccec]]
source_id: hfa-meal_order-b8ba06982765
subtotal: 32.0
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
tip: 8.0
total: 44.32
type: meal_order
uid: hfa-meal_order-b8ba06982765
updated: 2020-12-20
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Subtotal**: $32.00
- **Delivery Fee**: $0.00
- **Tip**: $8.00
- **Total**: $44.32
```

---

**File:** `Transactions/MealOrders/2020-09/hfa-meal_order-502f2cc49ab9.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2020-09-17
mode: delivery
restaurant: your bank statement shortly.
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-8eaa9614bebe]]
source_id: hfa-meal_order-502f2cc49ab9
subtotal: 16.62
summary: Uber Eats order from your bank statement shortly.
tax: 1.41
total: 16.62
type: meal_order
uid: hfa-meal_order-502f2cc49ab9
updated: 2020-09-17
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2021-01/hfa-meal_order-2f16aed56930.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2021-01-12
mode: delivery
restaurant: your bank statement shortly.
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-62845279bf07]]
source_id: hfa-meal_order-2f16aed56930
subtotal: 24.2
summary: Uber Eats order from your bank statement shortly.
tax: 2.78
total: 24.2
type: meal_order
uid: hfa-meal_order-2f16aed56930
updated: 2021-01-12
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

### `ride` (175 cards)
*Typical extractors:* `uber_rides`, `lyft`

- **Share with ≥1 flag:** 38/175 (21.7%)
- **Flag counts (cards can have multiple):**
  - `heuristic:weak_pickup`: 38
  - `heuristic:weak_dropoff`: 38

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Rides/2013-09/hfa-ride-683a84580e3a.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-09-15
distance_miles: 1.42
driver_name: Jeff
dropoff_location: Location:
duration_minutes: 7.0
fare: 11.14
pickup_at: 2013-09-15T06:17:19Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-8938919a6275]]
source_id: hfa-ride-683a84580e3a
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-683a84580e3a
updated: 2013-09-15
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-09-15T06:17:19Z
- **Fare**: 11.14
- **Distance (mi)**: 1.42
- **Duration (min)**: 7.0
- **Driver**: Jeff
```

---

**File:** `Transactions/Rides/2013-12/hfa-ride-a74458c5210c.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-12-02
distance_miles: 14.82
driver_name: Adel
dropoff_location: Location:
duration_minutes: 18.0
fare: 50.0
pickup_at: 2013-12-02T07:11:03Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-a2eb203942d7]]
source_id: hfa-ride-a74458c5210c
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-a74458c5210c
updated: 2013-12-02
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-12-02T07:11:03Z
- **Fare**: 50.0
- **Distance (mi)**: 14.82
- **Duration (min)**: 18.0
- **Driver**: Adel
```

---

**File:** `Transactions/Rides/2013-08/hfa-ride-201b938cb3d8.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-08-31
distance_miles: 1.6
driver_name: Cody
dropoff_location: Location:
duration_minutes: 8.0
fare: 9.24
pickup_at: 2013-08-31T06:02:43Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-6204197212dc]]
source_id: hfa-ride-201b938cb3d8
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-201b938cb3d8
updated: 2013-08-31
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-08-31T06:02:43Z
- **Fare**: 9.24
- **Distance (mi)**: 1.6
- **Duration (min)**: 8.0
- **Driver**: Cody
```

---

**File:** `Transactions/Rides/2013-12/hfa-ride-f03a2d5226f9.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-12-05
distance_miles: 3.16
driver_name: STEPHEN
dropoff_location: Location:
duration_minutes: 16.0
fare: 19.28
pickup_at: 2013-12-05T16:57:49Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-1674eabaa66e]]
source_id: hfa-ride-f03a2d5226f9
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-f03a2d5226f9
updated: 2013-12-05
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-12-05T16:57:49Z
- **Fare**: 19.28
- **Distance (mi)**: 3.16
- **Duration (min)**: 16.0
- **Driver**: STEPHEN
```

---

**File:** `Transactions/Rides/2013-09/hfa-ride-72f0d5e2d0dd.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-09-21
distance_miles: 0.86
driver_name: Mansoor
dropoff_location: Location:
fare: 8.0
pickup_at: 2013-09-21T05:55:24Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-fb9659015d90]]
source_id: hfa-ride-72f0d5e2d0dd
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-72f0d5e2d0dd
updated: 2013-09-21
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-09-21T05:55:24Z
- **Fare**: 8.0
- **Distance (mi)**: 0.86
- **Driver**: Mansoor
```

---

**File:** `Transactions/Rides/2013-10/hfa-ride-987466591e8b.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-10-05
driver_name: Hussein
dropoff_location: Location:
duration_minutes: 25.0
fare: 69.88
pickup_at: 2013-10-05T11:53:00Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-986581849cbd]]
source_id: hfa-ride-987466591e8b
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-987466591e8b
updated: 2013-10-05
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-10-05T11:53:00Z
- **Fare**: 69.88
- **Duration (min)**: 25.0
- **Driver**: Hussein
```

---

**File:** `Transactions/Rides/2014-02/hfa-ride-73b2e7057131.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-02-22
distance_miles: 1.93
driver_name: samir
dropoff_location: Location:
duration_minutes: 8.0
fare: 8.54
pickup_at: 2014-02-22T08:38:02Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-c2a748387581]]
source_id: hfa-ride-73b2e7057131
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-73b2e7057131
updated: 2014-02-22
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-02-22T08:38:02Z
- **Fare**: 8.54
- **Distance (mi)**: 1.93
- **Duration (min)**: 8.0
- **Driver**: samir
```

---

**File:** `Transactions/Rides/2013-11/hfa-ride-130d2a1fa2c3.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-11-06
distance_miles: 2.14
driver_name: Aftab
dropoff_location: Location:
duration_minutes: 15.0
fare: 13.84
pickup_at: 2013-11-06T03:52:18Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-f68c8228f50e]]
source_id: hfa-ride-130d2a1fa2c3
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-130d2a1fa2c3
updated: 2013-11-06
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-11-06T03:52:18Z
- **Fare**: 13.84
- **Distance (mi)**: 2.14
- **Duration (min)**: 15.0
- **Driver**: Aftab
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Rides/2016-08/hfa-ride-d549d7435bd2.md`

```yaml
created: 2016-08-12
driver_name: JESSE
dropoff_location: 3126-3132 Mission St, San Francisco, CA
fare: 24.18
pickup_at: 2016-08-12T05:19:07Z
pickup_location: 5 Burgoyne Ct, San Mateo, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-d5a8ccac32fb]]
source_id: hfa-ride-d549d7435bd2
summary: Uber from 5 Burgoyne Ct, San Mateo, CA to 3126-3132 Mission St, San Francisco, CA
type: ride
uid: hfa-ride-d549d7435bd2
updated: 2016-08-12
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 5 Burgoyne Ct, San Mateo, CA
- **Dropoff**: 3126-3132 Mission St, San Francisco, CA
- **Pickup at**: 2016-08-12T05:19:07Z
- **Fare**: 24.18
- **Driver**: JESSE
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2015-03/hfa-ride-d6b5143b731e.md`

```yaml
created: 2015-03-28
dropoff_location: 2-30 Bannam Place, San Francisco, CA
fare: 6.05
pickup_at: 2015-03-28T03:19:30Z
pickup_location: 2572 Pine Street, San Francisco, CA
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-13c944aafdba]]
source_id: hfa-ride-d6b5143b731e
summary: Uber from 2572 Pine Street, San Francisco, CA to 2-30 Bannam Place, San Francisco, CA
type: ride
uid: hfa-ride-d6b5143b731e
updated: 2015-03-28
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2572 Pine Street, San Francisco, CA
- **Dropoff**: 2-30 Bannam Place, San Francisco, CA
- **Pickup at**: 2015-03-28T03:19:30Z
- **Fare**: 6.05
```

---

**File:** `Transactions/Rides/2014-10/hfa-ride-cc2758d19679.md`

```yaml
created: 2014-10-16
dropoff_location: 18 Moore Place, San Francisco, CA
fare: 39.08
pickup_at: 2014-10-16T05:18:27Z
pickup_location: Pickup Location
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-3be8b9d074aa]]
source_id: hfa-ride-cc2758d19679
summary: Uber from Pickup Location to 18 Moore Place, San Francisco, CA
type: ride
uid: hfa-ride-cc2758d19679
updated: 2014-10-16
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Pickup Location
- **Dropoff**: 18 Moore Place, San Francisco, CA
- **Pickup at**: 2014-10-16T05:18:27Z
- **Fare**: 39.08
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-05/hfa-ride-aafe1a26f032.md`

```yaml
created: 2016-05-16
driver_name: DAMASO
dropoff_location: 10700 S De Anza Blvd, Cupertino, CA
duration_minutes: 55.0
fare: 5.0
pickup_at: 2016-05-16T19:05:56Z
pickup_location: 1 Results Way, Cupertino, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-f844648edb28]]
source_id: hfa-ride-aafe1a26f032
summary: Uber from 1 Results Way, Cupertino, CA to 10700 S De Anza Blvd, Cupertino, CA
type: ride
uid: hfa-ride-aafe1a26f032
updated: 2016-05-16
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 1 Results Way, Cupertino, CA
- **Dropoff**: 10700 S De Anza Blvd, Cupertino, CA
- **Pickup at**: 2016-05-16T19:05:56Z
- **Fare**: 5.0
- **Duration (min)**: 55.0
- **Driver**: DAMASO
- **Vehicle**: uberX
```

---

### `shipment` (62 cards)
*Typical extractors:* `shipping`

- **Share with ≥1 flag:** 0/62 (0.0%)

#### Clean examples (quality bar)

**File:** `Transactions/Shipments/2025-01/hfa-shipment-e9ec996e1847.md`

```yaml
carrier: UPS
created: 2025-01-06
delivered_at: Monday 01/06/2025 4:30 PM
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

**File:** `Transactions/Shipments/2020-12/hfa-shipment-8a135cd65626.md`

```yaml
carrier: FedEx
created: 2020-12-19
delivered_at: and placed in your garage.
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

**File:** `Transactions/Shipments/2020-09/hfa-shipment-657d0dc494f8.md`

```yaml
carrier: UPS
created: 2020-09-02
estimated_delivery: time for most UPS packages, click Continue
source: ["email_extraction"]
source_email: [[hfa-email-message-2b05af5aea20]]
source_id: hfa-shipment-657d0dc494f8
summary: UPS 1Z37641X0324753860
tracking_number: 1Z37641X0324753860
type: shipment
uid: hfa-shipment-657d0dc494f8
updated: 2020-09-02
```

Body (truncated):

```markdown
# Shipment UPS 1Z37641X0324753860
```

---

**File:** `Transactions/Shipments/2024-02/hfa-shipment-4484b15e3d13.md`

```yaml
carrier: UPS
created: 2024-02-02
source: ["email_extraction"]
source_email: [[hfa-email-message-01bc80a54877]]
source_id: hfa-shipment-4484b15e3d13
summary: UPS 1Z6FE4211352625958
tracking_number: 1Z6FE4211352625958
type: shipment
uid: hfa-shipment-4484b15e3d13
updated: 2024-02-02
```

Body (truncated):

```markdown
# Shipment UPS 1Z6FE4211352625958
```

---

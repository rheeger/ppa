# Extraction quality — `1pct` slice

Staging directory: `/Users/rheeger/Code/rheeger/ppa/_staging-1pct`

## Runner metrics (from `_metrics.json`)

- **Emails scanned:** 34081
- **Matched:** 297
- **Cards extracted:** 66
- **Wall clock (s):** 210.6

| Extractor | matched | extracted | errors | skipped | rejected |
|-----------|--------:|----------:|-------:|--------:|---------:|
| airbnb | 32 | 1 | 0 | 0 | 4 |
| doordash | 62 | 3 | 0 | 0 | 39 |
| instacart | 7 | 0 | 0 | 0 | 0 |
| rental_cars | 2 | 0 | 0 | 0 | 0 |
| shipping | 40 | 15 | 0 | 0 | 0 |
| uber_eats | 16 | 5 | 0 | 0 | 1 |
| uber_rides | 114 | 30 | 0 | 0 | 0 |
| united | 24 | 12 | 0 | 0 | 0 |

**Yield (extracted / matched)** — low values often mean parse/skip inside extractor.
- `airbnb`: 0.031
- `doordash`: 0.048
- `instacart`: 0.000
- `rental_cars`: 0.000
- `shipping`: 0.375
- `uber_eats`: 0.312
- `uber_rides`: 0.263
- `united`: 0.500

## Volume + field population (staging scan)

| Type | Count | Expected | Status |
|------|------:|----------|--------|
| accommodation | 1 | 30-50 | LOW (below 50% of lower bound 30) |
| flight | 12 | 50-100 | LOW (below 50% of lower bound 50) |
| meal_order | 8 | 1000-1500 | LOW (below 50% of lower bound 1000) |
| ride | 30 | 500-2000 | LOW (below 50% of lower bound 500) |
| shipment | 15 | 500-2000 | LOW (below 50% of lower bound 500) |

**Critical field population** (fraction of cards with field populated)

| Type | Field | Populated |
|------|-------|----------:|
| accommodation | check_in | 0.0% |
| accommodation | check_out | 0.0% |
| accommodation | confirmation_code | 100.0% |
| accommodation | property_name | 100.0% |
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
- accommodation: volume LOW (below 50% of lower bound 30) (count=1)
- flight: volume LOW (below 50% of lower bound 50) (count=12)
- meal_order: volume LOW (below 50% of lower bound 1000) (count=8)
- ride: volume LOW (below 50% of lower bound 500) (count=30)
- shipment: volume LOW (below 50% of lower bound 500) (count=15)

## Per card type — flags, rates, and examples

Flags combine **critical_fail:** (strict predicate from `field_metrics.CRITICAL_FIELDS`) and **heuristic:** (common junk patterns). Use flagged rows first when upgrading extractors.

### `accommodation` (1 cards)
*Typical extractors:* `airbnb`

- **Share with ≥1 flag:** 1/1 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:check_in`: 1
  - `critical_fail:check_out`: 1
  - `heuristic:missing_check_in`: 1
  - `heuristic:missing_check_out`: 1

#### Flagged examples (prioritize fixes)

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

### `flight` (12 cards)
*Typical extractors:* `united`

- **Share with ≥1 flag:** 0/12 (0.0%)

#### Clean examples (quality bar)

**File:** `Transactions/Flights/2019-10/hfa-flight-63608c0c20a4.md`

```yaml
airline: United
booking_source: United
confirmation_code: I73GJ6
created: 2019-10-26
destination_airport: MER
fare_amount: 20000.0
origin_airport: CUS
source: ["email_extraction"]
source_email: [[hfa-email-message-2188aa14c6d1]]
source_id: hfa-flight-63608c0c20a4
summary: United CUS to MER
type: flight
uid: hfa-flight-63608c0c20a4
updated: 2019-10-26
```

Body (truncated):

```markdown
# United CUS→MER I73GJ6
```

---

**File:** `Transactions/Flights/2016-09/hfa-flight-41464c35f73e.md`

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

**File:** `Transactions/Flights/2011-11/hfa-flight-a094d7ae11a0.md`

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

**File:** `Transactions/Flights/2022-04/hfa-flight-edf266ad6ceb.md`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: PS6JFR
created: 2022-04-09
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: CSS
fare_amount: 1608.37
origin_airport: COM
source: ["email_extraction"]
source_email: [[hfa-email-message-08204323e624]]
source_id: hfa-flight-edf266ad6ceb
summary: United COM to CSS
type: flight
uid: hfa-flight-edf266ad6ceb
updated: 2022-04-09
```

Body (truncated):

```markdown
# United COM→CSS PS6JFR
```

---

### `meal_order` (8 cards)
*Typical extractors:* `uber_eats`, `doordash`

- **Share with ≥1 flag:** 8/8 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 8
  - `heuristic:empty_items`: 8
  - `heuristic:footer_noise_restaurant`: 4
  - `heuristic:url_in_restaurant`: 3

#### Flagged examples (prioritize fixes)

**File:** `Transactions/MealOrders/2020-11/hfa-meal_order-8ce4a2db2428.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2020-11-13
mode: delivery
restaurant: your bank statement shortly.
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-2e92f375cbde]]
source_id: hfa-meal_order-8ce4a2db2428
subtotal: 18.95
summary: Uber Eats order from your bank statement shortly.
tax: 1.61
total: 18.95
type: meal_order
uid: hfa-meal_order-8ce4a2db2428
updated: 2020-11-13
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2022-10/hfa-meal_order-482e74e04e94.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2022-10-01
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-aa20f718c313]]
source_id: hfa-meal_order-482e74e04e94
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
total: 46.52
type: meal_order
uid: hfa-meal_order-482e74e04e94
updated: 2022-10-01
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Total**: $46.52
```

---

**File:** `Transactions/MealOrders/2020-11/hfa-meal_order-15af66102dbe.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2020-11-13
mode: delivery
restaurant: Limon Rotisserie - Valencia Picked up from 520 Valencia St, San Francisco, CA 94110, USA Delivered to 94 Jack London Alley, San Francisco, CA 94107, USA Delivered by Dawa Rate order Rate order .eat...
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-62222f552371]]
source_id: hfa-meal_order-15af66102dbe
subtotal: 18.95
summary: Uber Eats order from Limon Rotisserie - Valencia Picked up from 520 Valencia St, San Francisco, CA 94110, USA Delivered to 94 Jack London Alley, San Francisco, CA 94107, USA Delivered by Dawa Rate order Rate order .eats_footer_table{width:100%!important} Contact support
tax: 1.61
total: 18.95
type: meal_order
uid: hfa-meal_order-15af66102dbe
updated: 2020-11-13
```

Body (truncated):

```markdown
# Uber Eats — Limon Rotisserie - Valencia                     Picked up from    520 Valencia St, San Francisco, CA 94110, USA                              Delivered to    94 Jack London Alley, San Francisco, CA 94107, USA                                                                                             Delivered by Dawa                        Rate order             Rate order                                                                       .eats_footer_table{width:100%!important}                 Contact support
```

---

**File:** `Transactions/MealOrders/2020-12/hfa-meal_order-069e07d66d44.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2020-12-24
mode: delivery
restaurant: your bank statement shortly.
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-d4e7404893ff]]
source_id: hfa-meal_order-069e07d66d44
subtotal: 18.95
summary: Uber Eats order from your bank statement shortly.
tax: 1.61
total: 18.95
type: meal_order
uid: hfa-meal_order-069e07d66d44
updated: 2020-12-24
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2019-10/hfa-meal_order-ffd00f939682.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2019-10-31
mode: delivery
restaurant: your bank statement shortly. Learn More xide2f155bc-d84b-52c3-9f4b-8a4f7ac96a03 pGvlI2ANUbXFfyEOgxta1RMV082993
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-61357207c7c3]]
source_id: hfa-meal_order-ffd00f939682
subtotal: 30.4
summary: Uber Eats order from your bank statement shortly. Learn More xide2f155bc-d84b-52c3-9f4b-8a4f7ac96a03 pGvlI2ANUbXFfyEOgxta1RMV082993
tax: 2.58
total: 5.0
type: meal_order
uid: hfa-meal_order-ffd00f939682
updated: 2019-10-31
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.    Learn More                         xide2f155bc-d84b-52c3-9f4b-8a4f7ac96a03      pGvlI2ANUbXFfyEOgxta1RMV082993
```

---

**File:** `Transactions/MealOrders/2023-12/hfa-meal_order-928e9931438a.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2023-12-12
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-d0f118bae2c4]]
source_id: hfa-meal_order-928e9931438a
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
total: 31.29
type: meal_order
uid: hfa-meal_order-928e9931438a
updated: 2023-12-12
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Total**: $31.29
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

**File:** `Transactions/MealOrders/2019-10/hfa-meal_order-ba42bb2b8dc3.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`

```yaml
created: 2019-10-31
mode: delivery
restaurant: your bank statement shortly. Learn More xide2f155bc-d84b-52c3-9f4b-8a4f7ac96a03 pGvlI2ANUbXFfyEOgxta1RMV082993
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-e0bbc98ede1d]]
source_id: hfa-meal_order-ba42bb2b8dc3
subtotal: 30.4
summary: Uber Eats order from your bank statement shortly. Learn More xide2f155bc-d84b-52c3-9f4b-8a4f7ac96a03 pGvlI2ANUbXFfyEOgxta1RMV082993
tax: 2.58
total: 5.0
type: meal_order
uid: hfa-meal_order-ba42bb2b8dc3
updated: 2019-10-31
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.    Learn More                         xide2f155bc-d84b-52c3-9f4b-8a4f7ac96a03      pGvlI2ANUbXFfyEOgxta1RMV082993
```

---

### `ride` (30 cards)
*Typical extractors:* `uber_rides`, `lyft`

- **Share with ≥1 flag:** 8/30 (26.7%)
- **Flag counts (cards can have multiple):**
  - `heuristic:weak_pickup`: 8
  - `heuristic:weak_dropoff`: 8

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Rides/2013-11/hfa-ride-bb55a6e28c31.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-11-09
distance_miles: 1.37
driver_name: Jhalman
dropoff_location: Location:
duration_minutes: 8.0
fare: 10.8
pickup_at: 2013-11-09T22:56:11Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-c84993a9cb38]]
source_id: hfa-ride-bb55a6e28c31
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-bb55a6e28c31
updated: 2013-11-09
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-11-09T22:56:11Z
- **Fare**: 10.8
- **Distance (mi)**: 1.37
- **Duration (min)**: 8.0
- **Driver**: Jhalman
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

**File:** `Transactions/Rides/2013-07/hfa-ride-42784dd320c4.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-07-14
distance_miles: 6.36
driver_name: Djamal
dropoff_location: Location:
duration_minutes: 18.0
fare: 35.75
pickup_at: 2013-07-14T04:42:47Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-6d2dd5e0e68a]]
source_id: hfa-ride-42784dd320c4
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-42784dd320c4
updated: 2013-07-14
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-07-14T04:42:47Z
- **Fare**: 35.75
- **Distance (mi)**: 6.36
- **Duration (min)**: 18.0
- **Driver**: Djamal
```

---

**File:** `Transactions/Rides/2014-06/hfa-ride-6665063070ce.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-06-28
distance_miles: 0.76
driver_name: Marcos
dropoff_location: Location:
duration_minutes: 4.0
fare: 6.4
pickup_at: 2014-06-28T03:01:00Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-b9ba5260399a]]
source_id: hfa-ride-6665063070ce
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-6665063070ce
updated: 2014-06-28
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-06-28T03:01:00Z
- **Fare**: 6.4
- **Distance (mi)**: 0.76
- **Duration (min)**: 4.0
- **Driver**: Marcos
```

---

**File:** `Transactions/Rides/2013-12/hfa-ride-e18d8f542e29.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-12-03
distance_miles: 1.56
driver_name: Cristian
dropoff_location: Location:
duration_minutes: 10.0
fare: 11.15
pickup_at: 2013-12-03T19:36:25Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-33d11b4f8b86]]
source_id: hfa-ride-e18d8f542e29
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-e18d8f542e29
updated: 2013-12-03
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-12-03T19:36:25Z
- **Fare**: 11.15
- **Distance (mi)**: 1.56
- **Duration (min)**: 10.0
- **Driver**: Cristian
```

---

**File:** `Transactions/Rides/2014-05/hfa-ride-ba17b4cb9b51.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-05-01
distance_miles: 1.61
driver_name: Bhupendra
dropoff_location: Location:
duration_minutes: 6.0
fare: 11.94
pickup_at: 2014-05-01T03:01:02Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-f1c12c124762]]
source_id: hfa-ride-ba17b4cb9b51
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-ba17b4cb9b51
updated: 2014-05-01
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-05-01T03:01:02Z
- **Fare**: 11.94
- **Distance (mi)**: 1.61
- **Duration (min)**: 6.0
- **Driver**: Bhupendra
```

---

**File:** `Transactions/Rides/2014-02/hfa-ride-8213063ec65d.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-02-23
distance_miles: 2.42
driver_name: Bee
dropoff_location: Location:
duration_minutes: 9.0
fare: 9.55
pickup_at: 2014-02-23T12:38:07Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-95d60833f16c]]
source_id: hfa-ride-8213063ec65d
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-8213063ec65d
updated: 2014-02-23
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-02-23T12:38:07Z
- **Fare**: 9.55
- **Distance (mi)**: 2.42
- **Duration (min)**: 9.0
- **Driver**: Bee
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

#### Clean examples (quality bar)

**File:** `Transactions/Rides/2016-04/hfa-ride-9bd988452cd2.md`

```yaml
created: 2016-04-02
driver_name: Faiz
dropoff_location: 2908-2918 23rd St, San Francisco, CA
fare: 8.41
pickup_at: 2016-04-02T08:48:17Z
pickup_location: 166 2nd St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-42b65c31f4ba]]
source_id: hfa-ride-9bd988452cd2
summary: Uber from 166 2nd St, San Francisco, CA to 2908-2918 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-9bd988452cd2
updated: 2016-04-02
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 166 2nd St, San Francisco, CA
- **Dropoff**: 2908-2918 23rd St, San Francisco, CA
- **Pickup at**: 2016-04-02T08:48:17Z
- **Fare**: 8.41
- **Driver**: Faiz
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2015-11/hfa-ride-2b769de3ba37.md`

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

**File:** `Transactions/Rides/2016-08/hfa-ride-518481f346a0.md`

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

**File:** `Transactions/Rides/2016-01/hfa-ride-4cb22a9f0fb7.md`

```yaml
created: 2016-01-17
dropoff_location: 2915b 23rd St, San Francisco, CA
duration_minutes: 35.0
fare: 5.35
pickup_at: 2016-01-17T23:37:14Z
pickup_location: 1601-1623 Bryant St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-50348dc8079a]]
source_id: hfa-ride-4cb22a9f0fb7
summary: Uber from 1601-1623 Bryant St, San Francisco, CA to 2915b 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-4cb22a9f0fb7
updated: 2016-01-17
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 1601-1623 Bryant St, San Francisco, CA
- **Dropoff**: 2915b 23rd St, San Francisco, CA
- **Pickup at**: 2016-01-17T23:37:14Z
- **Fare**: 5.35
- **Duration (min)**: 35.0
- **Vehicle**: uberX
```

---

### `shipment` (15 cards)
*Typical extractors:* `shipping`

- **Share with ≥1 flag:** 0/15 (0.0%)

#### Clean examples (quality bar)

**File:** `Transactions/Shipments/2013-06/hfa-shipment-917f94944b0e.md`

```yaml
carrier: UPS
created: 2013-06-18
shipped_at: your items, and that this completes your order. Your order is being shipped and
source: ["email_extraction"]
source_email: [[hfa-email-message-d236a288a14a]]
source_id: hfa-shipment-917f94944b0e
summary: UPS 1Z293E1F1341112502
tracking_number: 1Z293E1F1341112502
type: shipment
uid: hfa-shipment-917f94944b0e
updated: 2013-06-18
```

Body (truncated):

```markdown
# Shipment UPS 1Z293E1F1341112502
```

---

**File:** `Transactions/Shipments/2024-10/hfa-shipment-c5f7394f3568.md`

```yaml
carrier: FedEx
created: 2024-10-16
delivered_at: by, based on the selected service, destination and ship date. Limitations and ex
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

**File:** `Transactions/Shipments/2025-03/hfa-shipment-989772be0774.md`

```yaml
carrier: FedEx
created: 2025-03-17
delivered_at: by, based on the selected service, destination and ship date. Limitations and ex
source: ["email_extraction"]
source_email: [[hfa-email-message-b1d902973c6f]]
source_id: hfa-shipment-989772be0774
summary: FedEx 447027374050
tracking_number: 447027374050
type: shipment
uid: hfa-shipment-989772be0774
updated: 2025-03-17
```

Body (truncated):

```markdown
# Shipment FedEx 447027374050
```

---

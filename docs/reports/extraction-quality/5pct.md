# Extraction quality — `5pct` slice

Staging directory: `/Users/rheeger/Code/rheeger/ppa/_artifacts/_staging-5pct`

## Runner metrics (from `_metrics.json`)

- **Emails scanned:** 108103
- **Matched:** 1429
- **Cards extracted:** 389
- **Wall clock (s):** 562.6

| Extractor   | matched | extracted | errors | skipped | rejected |
| ----------- | ------: | --------: | -----: | ------: | -------: |
| airbnb      |     126 |        16 |      0 |       0 |       20 |
| doordash    |     278 |        26 |      0 |       0 |      160 |
| instacart   |      19 |         0 |      0 |       0 |        0 |
| rental_cars |      21 |        10 |      0 |       0 |        3 |
| shipping    |     165 |        62 |      0 |       0 |        0 |
| uber_eats   |      95 |        26 |      0 |       0 |       15 |
| uber_rides  |     584 |       175 |      0 |       0 |        0 |
| united      |     141 |        74 |      0 |       0 |        0 |

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

| Type          | Count | Expected | Status                             |
| ------------- | ----: | -------- | ---------------------------------- |
| accommodation |    16 | 206-384  | LOW (below 50% of lower bound 206) |
| car_rental    |    10 | 35-67    | LOW (below 50% of lower bound 35)  |
| flight        |    74 | 423-787  | LOW (below 50% of lower bound 423) |
| meal_order    |    52 | 717-1333 | LOW (below 50% of lower bound 717) |
| ride          |   175 | 473-881  | LOW (below 50% of lower bound 473) |
| shipment      |    62 | 486-904  | LOW (below 50% of lower bound 486) |

**Critical field population** (fraction of cards with field populated)

| Type          | Field               | Populated |
| ------------- | ------------------- | --------: |
| accommodation | check_in            |     31.2% |
| accommodation | check_out           |     31.2% |
| accommodation | confirmation_code   |    100.0% |
| accommodation | property_name       |    100.0% |
| car_rental    | company             |    100.0% |
| car_rental    | confirmation_code   |    100.0% |
| car_rental    | pickup_at           |     10.0% |
| flight        | confirmation_code   |    100.0% |
| flight        | destination_airport |    100.0% |
| flight        | origin_airport      |    100.0% |
| meal_order    | items               |      0.0% |
| meal_order    | restaurant          |    100.0% |
| meal_order    | total               |    100.0% |
| ride          | dropoff_location    |    100.0% |
| ride          | fare                |    100.0% |
| ride          | pickup_location     |    100.0% |
| shipment      | carrier             |    100.0% |
| shipment      | tracking_number     |    100.0% |

**Warnings:**

- accommodation: volume LOW (below 50% of lower bound 206) (count=16)
- car_rental: volume LOW (below 50% of lower bound 35) (count=10)
- flight: volume LOW (below 50% of lower bound 423) (count=74)
- meal_order: volume LOW (below 50% of lower bound 717) (count=52)
- ride: volume LOW (below 50% of lower bound 473) (count=175)
- shipment: volume LOW (below 50% of lower bound 486) (count=62)

## Per card type — flags, rates, and examples

Flags combine **critical_fail:** (strict predicate from `field_metrics.CRITICAL_FIELDS`) and **heuristic:** (common junk patterns). Use flagged rows first when upgrading extractors.
Round-trip source checks use vault `/Users/rheeger/Code/rheeger/ppa/.slices/5pct`.

### `accommodation` (16 cards)

_Typical extractors:_ `airbnb`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 16

- **Share with ≥1 flag:** 16/16 (100.0%)
- **Flag counts (cards can have multiple):**
  - `heuristic:round_trip_fail:property_type`: 16
  - `critical_fail:check_in`: 11
  - `heuristic:missing_check_in`: 11
  - `critical_fail:check_out`: 11
  - `heuristic:missing_check_out`: 11

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Accommodations/2021-09/hfa-accommodation-bc43d76886a8.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

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

**File:** `Transactions/Accommodations/2022-06/hfa-accommodation-ddadb06bcde6.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

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

**File:** `Transactions/Accommodations/2021-05/hfa-accommodation-006e995f9427.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

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

**File:** `Transactions/Accommodations/2021-05/hfa-accommodation-ceffa0a75c5b.md`
**Flags:** `heuristic:round_trip_fail:property_type`

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

**File:** `Transactions/Accommodations/2024-04/hfa-accommodation-e52e08dd2963.md`
**Flags:** `critical_fail:check_in`, `heuristic:missing_check_in`, `heuristic:round_trip_fail:property_type`

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
# Airbnb in Roma Sur ★5.0Rating 5 out of 5; 5 reviews Home in Roma Norte ★4.99Rating 4.99 out of 5; 86 reviews
```

---

**File:** `Transactions/Accommodations/2022-06/hfa-accommodation-eea29c8d0732.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

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

**File:** `Transactions/Accommodations/2022-12/hfa-accommodation-d4f63ef5042b.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

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

**File:** `Transactions/Accommodations/2021-09/hfa-accommodation-d462a5d6d6d6.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:round_trip_fail:property_type`

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

### `car_rental` (10 cards)

_Typical extractors:_ `rental_cars`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 10

- **Share with ≥1 flag:** 9/10 (90.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:pickup_at`: 9
  - `heuristic:duplicate_suspect`: 5

#### Flagged examples (prioritize fixes)

**File:** `Transactions/CarRentals/2021-02/hfa-car_rental-238c5555b5ed.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

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

**File:** `Transactions/CarRentals/2017-10/hfa-car_rental-2f8191f126b6.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: CONFIRMATION
created: 2017-10-06
dropoff_at: and port of entry.
dropoff_location: Boston Logan Intl Airport (BOS) October 9, 2017 04:00 PM Rates, Taxes and Fees Rental Rate 3 Day(s) @33.00 $ 99.00 9 Additional Drivers INCLUDED Coverages Add-Ons Mileage UNLIMITED MILEAGE INCLUDED...
pickup_location: Boston Logan Intl Airport (BOS) October 6, 2017 10:30 PM 15 Transportation Way East Boston MA 02128 United States Phone: (888)826-6890 ext: MAIN Hours: Mon - Sun 12:00AM - 11:59PM Return Boston Log...
source: ["email_extraction"]
source_email: [[hfa-email-message-6b280c8e7a1e]]
source_id: hfa-car_rental-2f8191f126b6
summary: National
type: car_rental
uid: hfa-car_rental-2f8191f126b6
updated: 2017-10-06
```

Body (truncated):

```markdown
# National rental CONFIRMATION
```

---

**File:** `Transactions/CarRentals/2021-07/hfa-car_rental-c2b83a3083ef.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

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

**File:** `Transactions/CarRentals/2017-10/hfa-car_rental-0e68ef85cfba.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

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

_Typical extractors:_ `united`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 74

- **Share with ≥1 flag:** 68/74 (91.9%)
- **Flag counts (cards can have multiple):**
  - `heuristic:non_iata_destination_airport`: 65
  - `heuristic:non_iata_origin_airport`: 63
  - `heuristic:duplicate_suspect`: 25
  - `heuristic:round_trip_fail:fare_amount`: 10

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Flights/2024-03/hfa-flight-ee5de9b91e7d.md`
**Flags:** `heuristic:non_iata_destination_airport`

```yaml
airline: United
arrival_at: MIA - Miami on Thu, Apr 18 2024 at 22:06 PM Fare class: United Economy Meal: Sna
booking_source: United
confirmation_code: N6N2XG
created: 2024-03-25
departure_at: EWR - New York/Newark on Thu, Apr 18 2024 at 18:56 PM Arrive: MIA - Miami on Thu
destination_airport: NEW
origin_airport: EWR
source: ["email_extraction"]
source_email: [[hfa-email-message-3db0bdbb2989]]
source_id: hfa-flight-ee5de9b91e7d
summary: United EWR to NEW
type: flight
uid: hfa-flight-ee5de9b91e7d
updated: 2024-03-25
```

Body (truncated):

```markdown
# United EWR→NEW N6N2XG
```

---

**File:** `Transactions/Flights/2013-05/hfa-flight-93d959f5a60f.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`, `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: 10:53 a.m.Travel Time:<span class="PHead">
booking_source: United
confirmation_code: DML0QX
created: 2013-05-06
departure_at: time.</b>
destination_airport: WEB
origin_airport: COM
source: ["email_extraction"]
source_email: [[hfa-email-message-dd8b9f251f75]]
source_id: hfa-flight-93d959f5a60f
summary: United COM to WEB
type: flight
uid: hfa-flight-93d959f5a60f
updated: 2013-05-06
```

Body (truncated):

```markdown
# United COM→WEB DML0QX
```

---

**File:** `Transactions/Flights/2020-02/hfa-flight-08368573a8ac.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`

```yaml
airline: United
arrival_at: City and Time
booking_source: United
confirmation_code: PZFQDW
created: 2020-02-25
departure_at: City and Time
destination_airport: MER
origin_airport: CUS
source: ["email_extraction"]
source_email: [[hfa-email-message-dba4c7ba3f5e]]
source_id: hfa-flight-08368573a8ac
summary: United CUS to MER
type: flight
uid: hfa-flight-08368573a8ac
updated: 2020-02-25
```

Body (truncated):

```markdown
# United CUS→MER PZFQDW
```

---

**File:** `Transactions/Flights/2010-11/hfa-flight-b09c3eda3b92.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`

```yaml
airline: United
arrival_at: 11:58 PM on
booking_source: United
confirmation_code: X889SG
created: 2010-11-28
departure_at: 8:25 PM
destination_airport: MER
origin_airport: CUS
source: ["email_extraction"]
source_email: [[hfa-email-message-d84381e0d006]]
source_id: hfa-flight-b09c3eda3b92
summary: United CUS to MER
type: flight
uid: hfa-flight-b09c3eda3b92
updated: 2010-11-28
```

Body (truncated):

```markdown
# United CUS→MER X889SG
```

---

**File:** `Transactions/Flights/2025-04/hfa-flight-006a81f37d1d.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: DDHCFG
created: 2025-04-28
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: CSS
fare_amount: 269.01
origin_airport: COM
source: ["email_extraction"]
source_email: [[hfa-email-message-0a7901365483]]
source_id: hfa-flight-006a81f37d1d
summary: United COM to CSS
type: flight
uid: hfa-flight-006a81f37d1d
updated: 2025-04-28
```

Body (truncated):

```markdown
# United COM→CSS DDHCFG
```

---

**File:** `Transactions/Flights/2024-01/hfa-flight-2af7a74b2c26.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: MYDMV0
created: 2024-01-07
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: CSS
fare_amount: 104.19
origin_airport: COM
source: ["email_extraction"]
source_email: [[hfa-email-message-335aca44c1a2]]
source_id: hfa-flight-2af7a74b2c26
summary: United COM to CSS
type: flight
uid: hfa-flight-2af7a74b2c26
updated: 2024-01-07
```

Body (truncated):

```markdown
# United COM→CSS MYDMV0
```

---

**File:** `Transactions/Flights/2014-11/hfa-flight-268e72192460.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`, `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: City and Time
booking_source: United
confirmation_code: FHSVPM
created: 2014-11-30
departure_at: City and Time
destination_airport: EDD
fare_amount: 671.63
origin_airport: FEE
source: ["email_extraction"]
source_email: [[hfa-email-message-eb56cb6af7a8]]
source_id: hfa-flight-268e72192460
summary: United FEE to EDD
type: flight
uid: hfa-flight-268e72192460
updated: 2014-11-30
```

Body (truncated):

```markdown
# United FEE→EDD FHSVPM
```

---

**File:** `Transactions/Flights/2017-01/hfa-flight-1c069f3918dc.md`
**Flags:** `heuristic:non_iata_origin_airport`, `heuristic:non_iata_destination_airport`

```yaml
airline: United
booking_source: United
confirmation_code: IZ8WK9
created: 2017-01-19
destination_airport: MER
fare_amount: 286.4
origin_airport: CUS
source: ["email_extraction"]
source_email: [[hfa-email-message-fb7cca3a8a81]]
source_id: hfa-flight-1c069f3918dc
summary: United CUS to MER
type: flight
uid: hfa-flight-1c069f3918dc
updated: 2017-01-19
```

Body (truncated):

```markdown
# United CUS→MER IZ8WK9
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Flights/2015-03/hfa-flight-10520de32e1e.md`

```yaml
airline: United
booking_source: United
confirmation_code: GQMB9Z
created: 2015-03-24
destination_airport: DEN
fare_amount: 639.2
origin_airport: SJC
source: ["email_extraction"]
source_email: [[hfa-email-message-c86c3d8eb03e]]
source_id: hfa-flight-10520de32e1e
summary: United SJC to DEN
type: flight
uid: hfa-flight-10520de32e1e
updated: 2015-03-24
```

Body (truncated):

```markdown
# United SJC→DEN GQMB9Z
```

---

**File:** `Transactions/Flights/2013-10/hfa-flight-a22e16ad75a4.md`

```yaml
airline: United
booking_source: United
confirmation_code: B5H3JL
created: 2013-10-28
destination_airport: JFK
fare_amount: 701.8
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-817a98610bd7]]
source_id: hfa-flight-a22e16ad75a4
summary: United SFO to JFK
type: flight
uid: hfa-flight-a22e16ad75a4
updated: 2013-10-28
```

Body (truncated):

```markdown
# United SFO→JFK B5H3JL
```

---

**File:** `Transactions/Flights/2024-02/hfa-flight-1cb285d35245.md`

```yaml
airline: United
booking_source: United
confirmation_code: A9PN97
created: 2024-02-28
destination_airport: DEN
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-2d1a9b821897]]
source_id: hfa-flight-1cb285d35245
summary: United SFO to DEN
type: flight
uid: hfa-flight-1cb285d35245
updated: 2024-02-28
```

Body (truncated):

```markdown
# United SFO→DEN A9PN97
```

---

**File:** `Transactions/Flights/2009-01/hfa-flight-361bb2936d59.md`

```yaml
airline: United
arrival_at: SFO 2:54 PM
booking_source: United
confirmation_code: STATUS
created: 2009-01-13
departure_at: LAX 1:31 PM Arrive: SFO 2:54 PM
destination_airport: SFO
origin_airport: LAX
source: ["email_extraction"]
source_email: [[hfa-email-message-6d29daeb941d]]
source_id: hfa-flight-361bb2936d59
summary: United LAX to SFO
type: flight
uid: hfa-flight-361bb2936d59
updated: 2009-01-13
```

Body (truncated):

```markdown
# United LAX→SFO STATUS
```

---

### `meal_order` (52 cards)

_Typical extractors:_ `uber_eats`, `doordash`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 52

- **Share with ≥1 flag:** 52/52 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 52
  - `heuristic:empty_items`: 52
  - `heuristic:url_in_restaurant`: 23
  - `heuristic:footer_noise_restaurant`: 19
  - `heuristic:duplicate_suspect`: 8
  - `heuristic:round_trip_fail:service`: 4
  - `heuristic:round_trip_fail:mode`: 2

#### Flagged examples (prioritize fixes)

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
# Uber Eats — Mixt - Valencia Picked up from 903 Valencia St, San Francisco, CA 94110, USA Delivered to 94 Jack London Alley, San Francisco, CA 94107, USA Delivered by Brandon Rate order Rate order .eats_footer_table{width:100%!important} Contact support
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

**File:** `Transactions/MealOrders/2023-04/hfa-meal_order-79c791fea599.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2023-04-07
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-c2242fe25261]]
source_id: hfa-meal_order-79c791fea599
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
total: 28.45
type: meal_order
uid: hfa-meal_order-79c791fea599
updated: 2023-04-07
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Total**: $28.45
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

**File:** `Transactions/MealOrders/2021-03/hfa-meal_order-ef5ab14a1746.md`
**Flags:** `critical_fail:items`, `heuristic:footer_noise_restaurant`, `heuristic:empty_items`, `heuristic:duplicate_suspect`

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

**File:** `Transactions/MealOrders/2023-06/hfa-meal_order-dba75163024d.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2023-06-20
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-a545a2b455da]]
source_id: hfa-meal_order-dba75163024d
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
total: 25.66
type: meal_order
uid: hfa-meal_order-dba75163024d
updated: 2023-06-20
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Total**: $25.66
```

---

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

**File:** `Transactions/MealOrders/2023-09/hfa-meal_order-d2fe6bd075c3.md`
**Flags:** `critical_fail:items`, `heuristic:url_in_restaurant`, `heuristic:empty_items`

```yaml
created: 2023-09-02
mode: delivery
restaurant: WARNING <https://www.p65warnings.ca.gov/places/restaurants>
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-c6480ddad893]]
source_id: hfa-meal_order-d2fe6bd075c3
summary: DoorDash order from WARNING <https://www.p65warnings.ca.gov/places/restaurants>
total: 42.83
type: meal_order
uid: hfa-meal_order-d2fe6bd075c3
updated: 2023-09-02
```

Body (truncated):

```markdown
# DoorDash — WARNING <https://www.p65warnings.ca.gov/places/restaurants>

- **Total**: $42.83
```

---

### `ride` (175 cards)

_Typical extractors:_ `uber_rides`, `lyft`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 175

- **Share with ≥1 flag:** 175/175 (100.0%)
- **Flag counts (cards can have multiple):**
  - `heuristic:round_trip_fail:pickup_at`: 175
  - `heuristic:round_trip_fail:duration_minutes`: 49
  - `heuristic:weak_pickup`: 38
  - `heuristic:weak_dropoff`: 38
  - `heuristic:round_trip_fail:ride_type`: 24

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Rides/2016-06/hfa-ride-26c583785136.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2016-06-13
driver_name: Jawad
dropoff_location: 2919-2923 23rd St, San Francisco, CA
fare: 11.96
pickup_at: 2016-06-13T02:08:38Z
pickup_location: 106-108 Marina Blvd, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-9ff5bc5b9f38]]
source_id: hfa-ride-26c583785136
summary: Uber from 106-108 Marina Blvd, San Francisco, CA to 2919-2923 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-26c583785136
updated: 2016-06-13
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 106-108 Marina Blvd, San Francisco, CA
- **Dropoff**: 2919-2923 23rd St, San Francisco, CA
- **Pickup at**: 2016-06-13T02:08:38Z
- **Fare**: 11.96
- **Driver**: Jawad
- **Vehicle**: uberX
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

**File:** `Transactions/Rides/2015-01/hfa-ride-f7f5b0814770.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2015-01-16
dropoff_location: 2590 Pine Street, San Francisco, CA
fare: 13.98
pickup_at: 2015-01-16T06:58:04Z
pickup_location: 1845 Mission Street, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-f06c9e0d1151]]
source_id: hfa-ride-f7f5b0814770
summary: Uber from 1845 Mission Street, San Francisco, CA to 2590 Pine Street, San Francisco, CA
type: ride
uid: hfa-ride-f7f5b0814770
updated: 2015-01-16
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 1845 Mission Street, San Francisco, CA
- **Dropoff**: 2590 Pine Street, San Francisco, CA
- **Pickup at**: 2015-01-16T06:58:04Z
- **Fare**: 13.98
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-05/hfa-ride-d66128c59ffd.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`, `heuristic:round_trip_fail:duration_minutes`

```yaml
created: 2016-05-18
driver_name: ANTONIO
dropoff_location: 2944 23rd St, San Francisco, CA
duration_minutes: 55.0
fare: 5.0
pickup_at: 2016-05-18T21:29:31Z
pickup_location: 3853 24th St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-aa8fb83376d1]]
source_id: hfa-ride-d66128c59ffd
summary: Uber from 3853 24th St, San Francisco, CA to 2944 23rd St, San Francisco, CA
type: ride
uid: hfa-ride-d66128c59ffd
updated: 2016-05-18
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 3853 24th St, San Francisco, CA
- **Dropoff**: 2944 23rd St, San Francisco, CA
- **Pickup at**: 2016-05-18T21:29:31Z
- **Fare**: 5.0
- **Duration (min)**: 55.0
- **Driver**: ANTONIO
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-01/hfa-ride-4cb22a9f0fb7.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`, `heuristic:round_trip_fail:duration_minutes`

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

**File:** `Transactions/Rides/2015-07/hfa-ride-c46af50e57b3.md`
**Flags:** `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2015-07-31
dropoff_location: 407A Clipper Street, San Francisco, CA
fare: 11.02
pickup_at: 2015-07-31T02:32:09Z
pickup_location: 2578-2588 Pine Street, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-47f29b378d8e]]
source_id: hfa-ride-c46af50e57b3
summary: Uber from 2578-2588 Pine Street, San Francisco, CA to 407A Clipper Street, San Francisco, CA
type: ride
uid: hfa-ride-c46af50e57b3
updated: 2015-07-31
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2578-2588 Pine Street, San Francisco, CA
- **Dropoff**: 407A Clipper Street, San Francisco, CA
- **Pickup at**: 2015-07-31T02:32:09Z
- **Fare**: 11.02
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2013-09/hfa-ride-96d513d18c83.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`, `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2013-09-12
distance_miles: 2.83
driver_name: Mesafint
dropoff_location: Location:
duration_minutes: 13.0
fare: 13.93
pickup_at: 2013-09-12T04:17:57Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-fb2cc4bb5708]]
source_id: hfa-ride-96d513d18c83
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-96d513d18c83
updated: 2013-09-12
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-09-12T04:17:57Z
- **Fare**: 13.93
- **Distance (mi)**: 2.83
- **Duration (min)**: 13.0
- **Driver**: Mesafint
```

---

**File:** `Transactions/Rides/2014-02/hfa-ride-dbdc50bb4d20.md`
**Flags:** `heuristic:weak_pickup`, `heuristic:weak_dropoff`, `heuristic:round_trip_fail:ride_type`, `heuristic:round_trip_fail:pickup_at`

```yaml
created: 2014-02-22
distance_miles: 0.93
driver_name: Muhammad
dropoff_location: Location:
duration_minutes: 3.0
fare: 5.54
pickup_at: 2014-02-22T12:25:26Z
pickup_location: Location:
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-ac9e8df54f3e]]
source_id: hfa-ride-dbdc50bb4d20
summary: Uber from Location: to Location:
type: ride
uid: hfa-ride-dbdc50bb4d20
updated: 2014-02-22
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-02-22T12:25:26Z
- **Fare**: 5.54
- **Distance (mi)**: 0.93
- **Duration (min)**: 3.0
- **Driver**: Muhammad
```

---

### `shipment` (62 cards)

_Typical extractors:_ `shipping`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 62

- **Share with ≥1 flag:** 16/62 (25.8%)
- **Flag counts (cards can have multiple):**
  - `heuristic:duplicate_suspect`: 10
  - `heuristic:round_trip_fail:carrier`: 6

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Shipments/2020-03/hfa-shipment-7db0d9a89d89.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2020-03-20
estimated_delivery: Time: End Of Day
source: ["email_extraction"]
source_email: [[hfa-email-message-2b8cafa80789]]
source_id: hfa-shipment-7db0d9a89d89
summary: UPS 1Z8303120396448839
tracking_number: 1Z8303120396448839
type: shipment
uid: hfa-shipment-7db0d9a89d89
updated: 2020-03-20
```

Body (truncated):

```markdown
# Shipment UPS 1Z8303120396448839
```

---

**File:** `Transactions/Shipments/2021-09/hfa-shipment-a900a1120af4.md`
**Flags:** `heuristic:round_trip_fail:carrier`

```yaml
carrier: FedEx
created: 2021-09-28
delivered_at: and placed in your garage.
source: ["email_extraction"]
source_email: [[hfa-email-message-7d624ac6658b]]
source_id: hfa-shipment-a900a1120af4
summary: FedEx 77480211923301
tracking_number: 77480211923301
type: shipment
uid: hfa-shipment-a900a1120af4
updated: 2021-09-28
```

Body (truncated):

```markdown
# Shipment FedEx 77480211923301
```

---

**File:** `Transactions/Shipments/2025-07/hfa-shipment-1387ee462d8c.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2025-07-01
delivered_at: by, based on the selected service, destination and ship date. Limitations and ex
estimated_delivery: time for packages. And if you won't be home, request to redirect your package
source: ["email_extraction"]
source_email: [[hfa-email-message-6ae4e2432d89]]
source_id: hfa-shipment-1387ee462d8c
summary: FedEx 882385711944
tracking_number: 882385711944
type: shipment
uid: hfa-shipment-1387ee462d8c
updated: 2025-07-01
```

Body (truncated):

```markdown
# Shipment FedEx 882385711944
```

---

**File:** `Transactions/Shipments/2025-10/hfa-shipment-5a85fa3dc8f2.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2025-10-23
delivered_at: Date
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

**File:** `Transactions/Shipments/2021-03/hfa-shipment-96337083fada.md`
**Flags:** `heuristic:round_trip_fail:carrier`

```yaml
carrier: FedEx
created: 2021-03-20
source: ["email_extraction"]
source_email: [[hfa-email-message-05808b035aa8]]
source_id: hfa-shipment-96337083fada
summary: FedEx 43704701891301
tracking_number: 43704701891301
type: shipment
uid: hfa-shipment-96337083fada
updated: 2021-03-20
```

Body (truncated):

```markdown
# Shipment FedEx 43704701891301
```

---

**File:** `Transactions/Shipments/2011-09/hfa-shipment-09de26fe9d20.md`
**Flags:** `heuristic:round_trip_fail:carrier`

```yaml
carrier: USPS
created: 2011-09-26
delivered_at: by US Postal Service.
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

**File:** `Transactions/Shipments/2025-09/hfa-shipment-162bd79c4b08.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2025-09-22
delivered_at: by, based on the selected service, destination and ship date. Limitations and ex
estimated_delivery: time for packages. And if you won’t be home, request to redirect your package
source: ["email_extraction"]
source_email: [[hfa-email-message-e1f2b858875b]]
source_id: hfa-shipment-162bd79c4b08
summary: FedEx 393427669057
tracking_number: 393427669057
type: shipment
uid: hfa-shipment-162bd79c4b08
updated: 2025-09-22
```

Body (truncated):

```markdown
# Shipment FedEx 393427669057
```

---

**File:** `Transactions/Shipments/2020-12/hfa-shipment-36db12adab97.md`
**Flags:** `heuristic:round_trip_fail:carrier`

```yaml
carrier: FedEx
created: 2020-12-13
delivered_at: and placed in your garage.
source: ["email_extraction"]
source_email: [[hfa-email-message-d2955701d617]]
source_id: hfa-shipment-36db12adab97
summary: FedEx 40203880805301
tracking_number: 40203880805301
type: shipment
uid: hfa-shipment-36db12adab97
updated: 2020-12-13
```

Body (truncated):

```markdown
# Shipment FedEx 40203880805301
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Shipments/2008-02/hfa-shipment-7d07c15957e5.md`

```yaml
carrier: FedEx
created: 2008-02-03
source: ["email_extraction"]
source_email: [[hfa-email-message-aaab15f9b8d3]]
source_id: hfa-shipment-7d07c15957e5
summary: FedEx 790930846866
tracking_number: 790930846866
type: shipment
uid: hfa-shipment-7d07c15957e5
updated: 2008-02-03
```

Body (truncated):

```markdown
# Shipment FedEx 790930846866
```

---

**File:** `Transactions/Shipments/2020-04/hfa-shipment-dbd66adda70b.md`

```yaml
carrier: UPS
created: 2020-04-02
source: ["email_extraction"]
source_email: [[hfa-email-message-68af524362ad]]
source_id: hfa-shipment-dbd66adda70b
summary: UPS 1Z1R44W70309362082
tracking_number: 1Z1R44W70309362082
type: shipment
uid: hfa-shipment-dbd66adda70b
updated: 2020-04-02
```

Body (truncated):

```markdown
# Shipment UPS 1Z1R44W70309362082
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

**File:** `Transactions/Shipments/2009-08/hfa-shipment-eab27630120a.md`

```yaml
carrier: FedEx
created: 2009-08-25
delivered_at: Ship (P/U) date: Aug 21, 2009 Delivery date: Aug 25, 2009 9:54 AM Sign f
source: ["email_extraction"]
source_email: [[hfa-email-message-0f85d12dd5de]]
source_id: hfa-shipment-eab27630120a
summary: FedEx 797868835669
tracking_number: 797868835669
type: shipment
uid: hfa-shipment-eab27630120a
updated: 2009-08-25
```

Body (truncated):

```markdown
# Shipment FedEx 797868835669
```

---

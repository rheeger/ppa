# Extraction quality — `phase3-full-seed` slice

Staging directory: `/Users/rheeger/Code/rheeger/ppa/_staging`

## Runner metrics (from `_metrics.json`)

- **Emails scanned:** 461216
- **Matched:** 9936
- **Cards extracted:** 3365
- **Wall clock (s):** 2605.0

| Extractor   | matched | extracted | errors | skipped | rejected |
| ----------- | ------: | --------: | -----: | ------: | -------: |
| airbnb      |     567 |       295 |      0 |       0 |      106 |
| doordash    |    2170 |       583 |      0 |       0 |      953 |
| instacart   |     239 |        17 |      0 |       0 |        6 |
| lyft        |     237 |        41 |      0 |       0 |       11 |
| rental_cars |     102 |        51 |      0 |       0 |        6 |
| shipping    |    1690 |       695 |      0 |       0 |        7 |
| uber_eats   |     779 |       442 |      0 |       0 |      231 |
| uber_rides  |    3058 |       636 |      0 |       0 |       30 |
| united      |    1094 |       605 |      0 |       0 |        0 |

**Yield (extracted / matched)** — low values often mean parse/skip inside extractor.

- `airbnb`: 0.520
- `doordash`: 0.269
- `instacart`: 0.071
- `lyft`: 0.173
- `rental_cars`: 0.500
- `shipping`: 0.411
- `uber_eats`: 0.567
- `uber_rides`: 0.208
- `united`: 0.553

## Volume + field population (staging scan)

| Type          | Count | Expected | Status |
| ------------- | ----: | -------- | ------ |
| accommodation |   295 | 206-384  | OK     |
| car_rental    |    51 | 35-67    | OK     |
| flight        |   605 | 423-787  | OK     |
| grocery_order |    17 | 11-23    | OK     |
| meal_order    | 1,025 | 717-1333 | OK     |
| ride          |   677 | 473-881  | OK     |
| shipment      |   695 | 486-904  | OK     |

**Critical field population** (fraction of cards with field populated)

| Type          | Field               | Populated |
| ------------- | ------------------- | --------: |
| accommodation | check_in            |     32.9% |
| accommodation | check_out           |     40.3% |
| accommodation | confirmation_code   |     99.3% |
| accommodation | property_name       |     92.9% |
| car_rental    | company             |    100.0% |
| car_rental    | confirmation_code   |    100.0% |
| car_rental    | pickup_at           |     54.9% |
| flight        | confirmation_code   |    100.0% |
| flight        | destination_airport |    100.0% |
| flight        | origin_airport      |    100.0% |
| grocery_order | items               |      0.0% |
| grocery_order | store               |    100.0% |
| grocery_order | total               |    100.0% |
| meal_order    | items               |     26.7% |
| meal_order    | restaurant          |     68.7% |
| meal_order    | total               |     99.8% |
| ride          | dropoff_location    |     98.2% |
| ride          | fare                |    100.0% |
| ride          | pickup_location     |     98.7% |
| shipment      | carrier             |    100.0% |
| shipment      | tracking_number     |    100.0% |

## Per card type — flags, rates, and examples

Flags combine **critical_fail:** (strict predicate from `field_metrics.CRITICAL_FIELDS`) and **heuristic:** (common junk patterns). Use flagged rows first when upgrading extractors.

### `accommodation` (295 cards)

_Typical extractors:_ `airbnb`

- **Confidence distribution:** >=0.8: 88 (30%), 0.5–0.8: 187 (63%), <0.5: 20 (7%), n/a: 0

- **Share with ≥1 flag:** 275/295 (93.2%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:check_in`: 198
  - `heuristic:missing_check_in`: 198
  - `heuristic:duplicate_suspect`: 196
  - `critical_fail:check_out`: 176
  - `heuristic:missing_check_out`: 176
  - `critical_fail:property_name`: 21
  - `critical_fail:confirmation_code`: 2

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Accommodations/2023-07/hfa-accommodation-e3df8ad33eda.md`
**Flags:** `critical_fail:check_in`, `heuristic:missing_check_in`, `heuristic:duplicate_suspect`

```yaml
address: Via Femminamorta, 99, 00123 Roma RM, Italy
booking_source: Airbnb
check_out: SUN, JUL 9 FRI, JUL 14
confirmation_code: HM5XSYFZRD
created: 2023-07-07
extraction_confidence: 0.75
property_name: July 9, 2023
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-6a851d2cd66c]]
source_id: hfa-accommodation-e3df8ad33eda
summary: hfa-accommodation-e3df8ad33eda
type: accommodation
uid: hfa-accommodation-e3df8ad33eda
updated: 2023-07-07
```

Body (truncated):

```markdown
# Airbnb July 9, 2023
```

---

**File:** `Transactions/Accommodations/2017-08/hfa-accommodation-09fc03228686.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 385573116
created: 2017-08-01
extraction_confidence: 0.5
property_name: 27, 2017
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-fefb3db44713]]
source_id: hfa-accommodation-09fc03228686
summary: hfa-accommodation-09fc03228686
type: accommodation
uid: hfa-accommodation-09fc03228686
updated: 2017-08-01
```

Body (truncated):

```markdown
# Airbnb 27, 2017
```

---

**File:** `Transactions/Accommodations/2026-01/hfa-accommodation-61fbafd01310.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 2394348729
created: 2026-01-01
extraction_confidence: 0.5
property_name: Mar 14
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-ce99de6da60f]]
source_id: hfa-accommodation-61fbafd01310
summary: hfa-accommodation-61fbafd01310
type: accommodation
uid: hfa-accommodation-61fbafd01310
updated: 2026-01-01
```

Body (truncated):

```markdown
# Airbnb Mar 14
```

---

**File:** `Transactions/Accommodations/2024-04/hfa-accommodation-e52e08dd2963.md`
**Flags:** `critical_fail:property_name`, `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 54112595
created: 2024-04-30
extraction_confidence: 0.25
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

**File:** `Transactions/Accommodations/2022-12/hfa-accommodation-544cc4cc26cd.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

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

**File:** `Transactions/Accommodations/2021-04/hfa-accommodation-71f9d8cd33c5.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 749214154
created: 2021-04-16
extraction_confidence: 0.5
property_name: JAMES AND SIOBHAN’S PLACE?
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-b703b3c02904]]
source_id: hfa-accommodation-71f9d8cd33c5
summary: hfa-accommodation-71f9d8cd33c5
type: accommodation
uid: hfa-accommodation-71f9d8cd33c5
updated: 2021-04-16
```

Body (truncated):

```markdown
# Airbnb JAMES AND SIOBHAN’S PLACE?
```

---

**File:** `Transactions/Accommodations/2020-08/hfa-accommodation-78db963fd21c.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 647811758
created: 2020-08-07
extraction_confidence: 0.5
property_name: Tim's place
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-a01356267112]]
source_id: hfa-accommodation-78db963fd21c
summary: hfa-accommodation-78db963fd21c
type: accommodation
uid: hfa-accommodation-78db963fd21c
updated: 2020-08-07
```

Body (truncated):

```markdown
# Airbnb Tim's place
```

---

**File:** `Transactions/Accommodations/2017-08/hfa-accommodation-b769db883dcf.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 385573116
created: 2017-08-02
extraction_confidence: 0.5
property_name: 27, 2017
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-450fad15b151]]
source_id: hfa-accommodation-b769db883dcf
summary: hfa-accommodation-b769db883dcf
type: accommodation
uid: hfa-accommodation-b769db883dcf
updated: 2017-08-02
```

Body (truncated):

```markdown
# Airbnb 27, 2017
```

---

**File:** `Transactions/Accommodations/2017-08/hfa-accommodation-797d9b6a81c0.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`

```yaml
booking_source: Airbnb
confirmation_code: 1504413741
created: 2017-08-04
extraction_confidence: 0.5
property_name: pay for your next trip
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-c2c6b30160ae]]
source_id: hfa-accommodation-797d9b6a81c0
summary: hfa-accommodation-797d9b6a81c0
type: accommodation
uid: hfa-accommodation-797d9b6a81c0
updated: 2017-08-04
```

Body (truncated):

```markdown
# Airbnb pay for your next trip
```

---

**File:** `Transactions/Accommodations/2023-09/hfa-accommodation-4e2be22d96dd.md`
**Flags:** `critical_fail:check_in`, `critical_fail:check_out`, `heuristic:missing_check_in`, `heuristic:missing_check_out`, `heuristic:duplicate_suspect`

```yaml
booking_source: Airbnb
confirmation_code: 1420230341
created: 2023-09-14
extraction_confidence: 0.5
property_name: Stargazer’s Luxury Retreat
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-1e1475c0a1f1]]
source_id: hfa-accommodation-4e2be22d96dd
summary: hfa-accommodation-4e2be22d96dd
type: accommodation
uid: hfa-accommodation-4e2be22d96dd
updated: 2023-09-14
```

Body (truncated):

```markdown
# Airbnb Stargazer’s Luxury Retreat
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Accommodations/2018-02/hfa-accommodation-96ce810307b7.md`

```yaml
booking_source: Airbnb
check_in: April 27, 2018
check_out: April 29, 2018
confirmation_code: HMFN4CFRDZ
created: 2018-02-06
extraction_confidence: 1.0
property_name: before the next guests.
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-b2ecde3d1e69]]
source_id: hfa-accommodation-96ce810307b7
summary: before the next guests. April 27, 2018 to April 29, 2018
type: accommodation
uid: hfa-accommodation-96ce810307b7
updated: 2018-02-06
```

Body (truncated):

```markdown
# Airbnb before the next guests.
```

---

**File:** `Transactions/Accommodations/2017-08/hfa-accommodation-49775dfd734d.md`

```yaml
address: 634 Orchard Avenue, Montecito, CA 93108, United States
booking_source: Airbnb
check_in: Fri, Aug 25 Anytime after 2PM
check_out: Sun, Aug 27 11AM
confirmation_code: HMK5Q4532Q
created: 2017-08-02
extraction_confidence: 1.0
property_name: PROVENCE IN MONTECITO
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-d6ed20a4f708]]
source_id: hfa-accommodation-49775dfd734d
summary: PROVENCE IN MONTECITO Fri, Aug 25 Anytime after 2PM to Sun, Aug 27 11AM
total_cost: 703.06
type: accommodation
uid: hfa-accommodation-49775dfd734d
updated: 2017-08-02
```

Body (truncated):

```markdown
# Airbnb PROVENCE IN MONTECITO
```

---

**File:** `Transactions/Accommodations/2025-12/hfa-accommodation-875864ec05c7.md`

```yaml
booking_source: Airbnb
check_in: 2026-01-04
check_out: 2026-03-14
confirmation_code: 45618680
created: 2025-12-31
extraction_confidence: 1.0
property_name: is worth another look
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-4855446438ea]]
source_id: hfa-accommodation-875864ec05c7
summary: is worth another look 2026-01-04 to 2026-03-14
type: accommodation
uid: hfa-accommodation-875864ec05c7
updated: 2025-12-31
```

Body (truncated):

```markdown
# Airbnb is worth another look
```

---

**File:** `Transactions/Accommodations/2019-08/hfa-accommodation-d1cced0f35e3.md`

```yaml
booking_source: Airbnb
check_in: Aug 22, 2019
check_out: Aug 27, 2019
confirmation_code: 742838320
created: 2019-08-10
extraction_confidence: 1.0
property_name: 27, 2019
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-21a6783ce49a]]
source_id: hfa-accommodation-d1cced0f35e3
summary: 27, 2019 Aug 22, 2019 to Aug 27, 2019
type: accommodation
uid: hfa-accommodation-d1cced0f35e3
updated: 2019-08-10
```

Body (truncated):

```markdown
# Airbnb 27, 2019
```

---

**File:** `Transactions/Accommodations/2019-03/hfa-accommodation-e04b7e380638.md`

```yaml
address: 2540 Clay St, Denver, CO 80211, USA
booking_source: Airbnb
check_in: May 8, 2019
check_out: May 11, 2019
confirmation_code: HMAPTKD8NY
created: 2019-03-25
extraction_confidence: 1.0
property_name: with Huge Views
property_type: short_term_rental
source: ["email_extraction"]
source_email: [[hfa-email-message-969634ae56f9]]
source_id: hfa-accommodation-e04b7e380638
summary: with Huge Views May 8, 2019 to May 11, 2019
type: accommodation
uid: hfa-accommodation-e04b7e380638
updated: 2019-03-25
```

Body (truncated):

```markdown
# Airbnb with Huge Views
```

---

### `car_rental` (51 cards)

_Typical extractors:_ `rental_cars`

- **Confidence distribution:** >=0.8: 28 (55%), 0.5–0.8: 23 (45%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 37/51 (72.5%)
- **Flag counts (cards can have multiple):**
  - `heuristic:duplicate_suspect`: 31
  - `critical_fail:pickup_at`: 23

#### Flagged examples (prioritize fixes)

**File:** `Transactions/CarRentals/2021-06/hfa-car_rental-ad05acb87cba.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: Hertz
confirmation_code: J7891124847
created: 2021-06-10
extraction_confidence: 0.67
pickup_location: date. If you wish to receive Gold Service
source: ["email_extraction"]
source_email: [[hfa-email-message-d99fa9320816]]
source_id: hfa-car_rental-ad05acb87cba
summary: Hertz
type: car_rental
uid: hfa-car_rental-ad05acb87cba
updated: 2021-06-10
```

Body (truncated):

```markdown
# Hertz rental J7891124847
```

---

**File:** `Transactions/CarRentals/2018-04/hfa-car_rental-e6d8a5db475f.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1163638511
created: 2018-04-24
extraction_confidence: 1.0
pickup_at: Wednesday, April 25, 2018 8:00 AM Pick Up Rental Office Address and Phon
pickup_location: your car from National
source: ["email_extraction"]
source_email: [[hfa-email-message-b4676c7ffe68]]
source_id: hfa-car_rental-e6d8a5db475f
summary: National rental Wednesday,
type: car_rental
uid: hfa-car_rental-e6d8a5db475f
updated: 2018-04-24
```

Body (truncated):

```markdown
# National rental 1163638511
```

---

**File:** `Transactions/CarRentals/2023-02/hfa-car_rental-1774ebcdf5b0.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1646083935
created: 2023-02-24
dropoff_location: DENVER INTL ARPT ( DEN ) Fri, March 3, 2023 2:30 PM
extraction_confidence: 0.67
pickup_location: DENVER INTL ARPT ( DEN ) Fri, February 24, 2023 11:30 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-50287778607a]]
source_id: hfa-car_rental-1774ebcdf5b0
summary: National
type: car_rental
uid: hfa-car_rental-1774ebcdf5b0
updated: 2023-02-24
```

Body (truncated):

```markdown
# National rental 1646083935
```

---

**File:** `Transactions/CarRentals/2017-08/hfa-car_rental-df5f6d8eb5eb.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1148342614
created: 2017-08-19
dropoff_at: and port of entry.
extraction_confidence: 0.67
source: ["email_extraction"]
source_email: [[hfa-email-message-8e799406f092]]
source_id: hfa-car_rental-df5f6d8eb5eb
summary: National
type: car_rental
uid: hfa-car_rental-df5f6d8eb5eb
updated: 2017-08-19
```

Body (truncated):

```markdown
# National rental 1148342614
```

---

**File:** `Transactions/CarRentals/2019-10/hfa-car_rental-d9c209ccaf02.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1060719998
created: 2019-10-12
dropoff_at: and port of entry. DEPOSIT AMOUNT – A deposit equal to the cost of the r
dropoff_location: SEA TAC INTL ARPT ( SEA ) Sat, October 19, 2019 3:00 PM
extraction_confidence: 1.0
pickup_at: October 19, 2019
pickup_location: SEA TAC INTL ARPT ( SEA ) Sat, October 19, 2019 11:30 AM
source: ["email_extraction"]
source_email: [[hfa-email-message-f51065e0095c]]
source_id: hfa-car_rental-d9c209ccaf02
summary: National rental October 19
type: car_rental
uid: hfa-car_rental-d9c209ccaf02
updated: 2019-10-12
```

Body (truncated):

```markdown
# National rental 1060719998
```

---

**File:** `Transactions/CarRentals/2017-03/hfa-car_rental-12779c4fdc36.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 285386197
created: 2017-03-11
extraction_confidence: 1.0
pickup_at: Sunday, March 12, 2017 12:00 PM Pick Up Rental Office Address and Phone N
pickup_location: your car from National
source: ["email_extraction"]
source_email: [[hfa-email-message-e763622d4c4e]]
source_id: hfa-car_rental-12779c4fdc36
summary: National rental Sunday, Ma
type: car_rental
uid: hfa-car_rental-12779c4fdc36
updated: 2017-03-11
```

Body (truncated):

```markdown
# National rental 285386197
```

---

**File:** `Transactions/CarRentals/2012-04/hfa-car_rental-fd1be59939a0.md`
**Flags:** `critical_fail:pickup_at`

```yaml
company: Hertz
confirmation_code: F4422294023
created: 2012-04-19
extraction_confidence: 0.67
source: ["email_extraction"]
source_email: [[hfa-email-message-00a91589f84a]]
source_id: hfa-car_rental-fd1be59939a0
summary: Hertz
type: car_rental
uid: hfa-car_rental-fd1be59939a0
updated: 2012-04-19
```

Body (truncated):

```markdown
# Hertz rental F4422294023
```

---

**File:** `Transactions/CarRentals/2021-08/hfa-car_rental-fb88410a3c09.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1757215751
created: 2021-08-20
dropoff_location: NEWARK LIBERTY INTL ARPT ( EWR ) Sun, September 5, 2021 12:00 PM
extraction_confidence: 0.67
pickup_location: NEWARK LIBERTY INTL ARPT ( EWR ) Fri, August 27, 2021 6:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-1303121017eb]]
source_id: hfa-car_rental-fb88410a3c09
summary: National
type: car_rental
uid: hfa-car_rental-fb88410a3c09
updated: 2021-08-20
```

Body (truncated):

```markdown
# National rental 1757215751
```

---

**File:** `Transactions/CarRentals/2015-07/hfa-car_rental-5e3ef1ea53e8.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

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

**File:** `Transactions/CarRentals/2017-10/hfa-car_rental-c171fc3a2eff.md`
**Flags:** `critical_fail:pickup_at`, `heuristic:duplicate_suspect`

```yaml
company: National
confirmation_code: 1151150851
created: 2017-10-02
dropoff_at: and port of entry.
extraction_confidence: 0.67
source: ["email_extraction"]
source_email: [[hfa-email-message-7529785392d7]]
source_id: hfa-car_rental-c171fc3a2eff
summary: National
type: car_rental
uid: hfa-car_rental-c171fc3a2eff
updated: 2017-10-02
```

Body (truncated):

```markdown
# National rental 1151150851
```

---

#### Clean examples (quality bar)

**File:** `Transactions/CarRentals/2014-08/hfa-car_rental-a6d446f4e396.md`

```yaml
company: Hertz
confirmation_code: G3092518674
created: 2014-08-26
dropoff_at: Thu, Aug 28, 2014 at 12:00 PM
dropoff_location: Time: Thu, Aug 28, 2014 at 12:00 PM
extraction_confidence: 1.0
pickup_at: Wed, Aug 27, 2014 at 09:00 PM
pickup_location: Time: Wed, Aug 27, 2014 at 09:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-46bc201af678]]
source_id: hfa-car_rental-a6d446f4e396
summary: Hertz rental Wed, Aug 2
total_cost: 157.57
type: car_rental
uid: hfa-car_rental-a6d446f4e396
updated: 2014-08-26
```

Body (truncated):

```markdown
# Hertz rental G3092518674
```

---

**File:** `Transactions/CarRentals/2021-11/hfa-car_rental-cf1925184c4f.md`

```yaml
company: National
confirmation_code: 1081185556
created: 2021-11-20
dropoff_location: JFK INTL ARPT ( JFK ) Sun, November 28, 2021 12:00 PM
extraction_confidence: 1.0
pickup_at: November 24, 2021
pickup_location: MANHATTAN W 44TH ST Wed, November 24, 2021 12:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-c27feec46320]]
source_id: hfa-car_rental-cf1925184c4f
summary: National rental November 2
type: car_rental
uid: hfa-car_rental-cf1925184c4f
updated: 2021-11-20
```

Body (truncated):

```markdown
# National rental 1081185556
```

---

**File:** `Transactions/CarRentals/2021-06/hfa-car_rental-d1c8a8e37160.md`

```yaml
company: National
confirmation_code: 1428799237
created: 2021-06-29
dropoff_location: INSTRUCTIONS Upon returning your car, please remember that the pick-up and drop off locations are different. To return your car at Fontanarossa Airport, follow car rental signs and look for the par...
extraction_confidence: 1.0
pickup_at: June 29, 2021
pickup_location: CATANIA AIRPORT ( CTA ) Tue, June 29, 2021 12:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-23c31e29e418]]
source_id: hfa-car_rental-d1c8a8e37160
summary: National rental June 29, 2
type: car_rental
uid: hfa-car_rental-d1c8a8e37160
updated: 2021-06-29
```

Body (truncated):

```markdown
# National rental 1428799237
```

---

**File:** `Transactions/CarRentals/2025-12/hfa-car_rental-bb2a80284b6e.md`

```yaml
company: National
confirmation_code: 1591335071
created: 2025-12-21
dropoff_location: LOS ANGELES INTL ARPT ( LAX ) Wed, January 7, 2026 2:00 PM
extraction_confidence: 1.0
pickup_at: December 25, 2025
pickup_location: LOS ANGELES INTL ARPT ( LAX ) Thu, December 25, 2025 9:00 PM
source: ["email_extraction"]
source_email: [[hfa-email-message-07c387566ab3]]
source_id: hfa-car_rental-bb2a80284b6e
summary: National rental December 2
type: car_rental
uid: hfa-car_rental-bb2a80284b6e
updated: 2025-12-21
```

Body (truncated):

```markdown
# National rental 1591335071
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

### `flight` (605 cards)

_Typical extractors:_ `united`

- **Confidence distribution:** >=0.8: 605 (100%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 563/605 (93.1%)
- **Flag counts (cards can have multiple):**
  - `heuristic:duplicate_suspect`: 563

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Flights/2012-04/hfa-flight-87ffae7fa167.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: PJRLST
created: 2012-04-20
destination_airport: LAX
extraction_confidence: 1.0
fare_amount: 25000.0
origin_airport: LAS
source: ["email_extraction"]
source_email: [[hfa-email-message-10035439a81b]]
source_id: hfa-flight-87ffae7fa167
summary: United LAS to LAX
type: flight
uid: hfa-flight-87ffae7fa167
updated: 2012-04-20
```

Body (truncated):

```markdown
# United LAS→LAX PJRLST
```

---

**File:** `Transactions/Flights/2023-01/hfa-flight-0dcc09d6e3bd.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: ELY8BQ
created: 2023-01-04
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: MUC
extraction_confidence: 1.0
fare_amount: 4375.0
origin_airport: EWR
source: ["email_extraction"]
source_email: [[hfa-email-message-d63620af53e1]]
source_id: hfa-flight-0dcc09d6e3bd
summary: United EWR to MUC
type: flight
uid: hfa-flight-0dcc09d6e3bd
updated: 2023-01-04
```

Body (truncated):

```markdown
# United EWR→MUC ELY8BQ
```

---

**File:** `Transactions/Flights/2019-04/hfa-flight-e57d9be9b123.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: AF5RCT
created: 2019-04-01
destination_airport: MUC
extraction_confidence: 1.0
origin_airport: JFK
source: ["email_extraction"]
source_email: [[hfa-email-message-4db107257472]]
source_id: hfa-flight-e57d9be9b123
summary: United JFK to MUC
type: flight
uid: hfa-flight-e57d9be9b123
updated: 2019-04-01
```

Body (truncated):

```markdown
# United JFK→MUC AF5RCT
```

---

**File:** `Transactions/Flights/2013-11/hfa-flight-f6e3858cdcc4.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: G0NM8E
created: 2013-11-18
destination_airport: SFO
extraction_confidence: 1.0
origin_airport: LAX
source: ["email_extraction"]
source_email: [[hfa-email-message-96a9c782430d]]
source_id: hfa-flight-f6e3858cdcc4
summary: United LAX to SFO
type: flight
uid: hfa-flight-f6e3858cdcc4
updated: 2013-11-18
```

Body (truncated):

```markdown
# United LAX→SFO G0NM8E
```

---

**File:** `Transactions/Flights/2013-04/hfa-flight-1ed227835e3a.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: 8:58 p.m.Travel Time:<span class="PHead">
booking_source: United
confirmation_code: JZ4E74
created: 2013-04-15
departure_at: time.</b>
destination_airport: LAX
extraction_confidence: 1.0
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-24be9b2441c0]]
source_id: hfa-flight-1ed227835e3a
summary: United SFO to LAX
type: flight
uid: hfa-flight-1ed227835e3a
updated: 2013-04-15
```

Body (truncated):

```markdown
# United SFO→LAX JZ4E74
```

---

**File:** `Transactions/Flights/2018-04/hfa-flight-a0083d9b9294.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: O90YYX
created: 2018-04-04
destination_airport: LAX
extraction_confidence: 1.0
fare_amount: 436.8
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-a8cd12d5748b]]
source_id: hfa-flight-a0083d9b9294
summary: United SFO to LAX
type: flight
uid: hfa-flight-a0083d9b9294
updated: 2018-04-04
```

Body (truncated):

```markdown
# United SFO→LAX O90YYX
```

---

**File:** `Transactions/Flights/2019-11/hfa-flight-29afdc785b27.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: EK794M
created: 2019-11-13
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: ORD
extraction_confidence: 1.0
fare_amount: 110.7
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-b272c06a8a9f]]
source_id: hfa-flight-29afdc785b27
summary: United SFO to ORD
type: flight
uid: hfa-flight-29afdc785b27
updated: 2019-11-13
```

Body (truncated):

```markdown
# United SFO→ORD EK794M
```

---

**File:** `Transactions/Flights/2019-03/hfa-flight-b4f37c8582d8.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
booking_source: United
confirmation_code: M1TMN5
created: 2019-03-10
departure_at: 10:57 a.m. from Salt Lake City, UT, US (SLC)
destination_airport: SFO
extraction_confidence: 1.0
origin_airport: SLC
source: ["email_extraction"]
source_email: [[hfa-email-message-f048be32d54e]]
source_id: hfa-flight-b4f37c8582d8
summary: United SLC to SFO
type: flight
uid: hfa-flight-b4f37c8582d8
updated: 2019-03-10
```

Body (truncated):

```markdown
# United SLC→SFO M1TMN5
```

---

**File:** `Transactions/Flights/2021-07/hfa-flight-11a01ae7c82a.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: PXGQJY
created: 2021-07-04
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Robertevan H
destination_airport: MUC
extraction_confidence: 1.0
fare_amount: 401.0
origin_airport: FCO
source: ["email_extraction"]
source_email: [[hfa-email-message-c321035fc434]]
source_id: hfa-flight-11a01ae7c82a
summary: United FCO to MUC
type: flight
uid: hfa-flight-11a01ae7c82a
updated: 2021-07-04
```

Body (truncated):

```markdown
# United FCO→MUC PXGQJY
```

---

**File:** `Transactions/Flights/2021-07/hfa-flight-2babcc409f92.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: PXGQJY
created: 2021-07-04
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Ariel Friedm
destination_airport: MUC
extraction_confidence: 1.0
fare_amount: 401.0
origin_airport: FCO
source: ["email_extraction"]
source_email: [[hfa-email-message-c6b6dc8ad38b]]
source_id: hfa-flight-2babcc409f92
summary: United FCO to MUC
type: flight
uid: hfa-flight-2babcc409f92
updated: 2021-07-04
```

Body (truncated):

```markdown
# United FCO→MUC PXGQJY
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Flights/2022-03/hfa-flight-f03c58f71a7d.md`

```yaml
airline: United
arrival_at: of the flight on which the baggage was or was to be transported and submit a wri
booking_source: United
confirmation_code: A7DV21
created: 2022-03-19
departure_at: time or TICKET HAS NO VALUE. MileagePlus Accrual Details Ariel Friedm
destination_airport: EWR
extraction_confidence: 1.0
fare_amount: 1680.0
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-76f9f543efa9]]
source_id: hfa-flight-f03c58f71a7d
summary: United SFO to EWR
type: flight
uid: hfa-flight-f03c58f71a7d
updated: 2022-03-19
```

Body (truncated):

```markdown
# United SFO→EWR A7DV21
```

---

**File:** `Transactions/Flights/2012-06/hfa-flight-ee8a47ceb384.md`

```yaml
airline: United
arrival_at: 6:58 a.m.+1 DayTravel Time:<span class="PHead">
booking_source: United
confirmation_code: L21N9J
created: 2012-06-28
departure_at: 10:29 p.m.Thu., Jun. 28, 2012San Francisco, CA (SFO)Arrive:6:58 a.m.+1 DayTravel
destination_airport: SFO
extraction_confidence: 1.0
origin_airport: JFK
source: ["email_extraction"]
source_email: [[hfa-email-message-b675af002365]]
source_id: hfa-flight-ee8a47ceb384
summary: United JFK to SFO
type: flight
uid: hfa-flight-ee8a47ceb384
updated: 2012-06-28
```

Body (truncated):

```markdown
# United JFK→SFO L21N9J
```

---

**File:** `Transactions/Flights/2019-11/hfa-flight-31d2247b732c.md`

```yaml
airline: United
arrival_at: TLV - Tel Aviv on Fri, Dec 20 2019 at 9:40 PM Fare class: United Polaris busines
booking_source: United
confirmation_code: MDZ7CK
created: 2019-11-12
departure_at: SFO - San Francisco on Thu, Dec 19 2019 at 9:40 PM Arrive: TLV - Tel Aviv on Fri
destination_airport: SAN
extraction_confidence: 1.0
origin_airport: SFO
source: ["email_extraction"]
source_email: [[hfa-email-message-d79fb234f46a]]
source_id: hfa-flight-31d2247b732c
summary: United SFO to SAN
type: flight
uid: hfa-flight-31d2247b732c
updated: 2019-11-12
```

Body (truncated):

```markdown
# United SFO→SAN MDZ7CK
```

---

**File:** `Transactions/Flights/2018-04/hfa-flight-21625f8eb6ac.md`

```yaml
airline: United
booking_source: United
confirmation_code: BQM5QK
created: 2018-04-14
destination_airport: SFO
extraction_confidence: 1.0
fare_amount: 212.09
origin_airport: PSP
source: ["email_extraction"]
source_email: [[hfa-email-message-35812d45b963]]
source_id: hfa-flight-21625f8eb6ac
summary: United PSP to SFO
type: flight
uid: hfa-flight-21625f8eb6ac
updated: 2018-04-14
```

Body (truncated):

```markdown
# United PSP→SFO BQM5QK
```

---

**File:** `Transactions/Flights/2012-03/hfa-flight-6613f826fa32.md`

```yaml
airline: United
arrival_at: 5:28 p.m.Travel Time:<span class="PHead">
booking_source: United
confirmation_code: O1P20B
created: 2012-03-15
departure_at: 3:55 p.m.Fri., Mar. 16, 2012San Francisco, CA (SFO)Arrive:5:28 p.m.Travel Time:<
destination_airport: SFO
extraction_confidence: 1.0
origin_airport: LAS
source: ["email_extraction"]
source_email: [[hfa-email-message-ea9b3cd263e6]]
source_id: hfa-flight-6613f826fa32
summary: United LAS to SFO
type: flight
uid: hfa-flight-6613f826fa32
updated: 2012-03-15
```

Body (truncated):

```markdown
# United LAS→SFO O1P20B
```

---

### `grocery_order` (17 cards)

_Typical extractors:_ `instacart`

- **Confidence distribution:** >=0.8: 0 (0%), 0.5–0.8: 17 (100%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 17/17 (100.0%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 17
  - `heuristic:empty_items`: 17

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Groceries/2025-12/hfa-grocery_order-c36f5544b84e.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-12-02
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-8afcf293a0bb]]
source_id: hfa-grocery_order-c36f5544b84e
store: Wegmans
summary: Instacart order from Wegmans
total: 278.91
type: grocery_order
uid: hfa-grocery_order-c36f5544b84e
updated: 2025-12-02
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

**File:** `Transactions/Groceries/2023-04/hfa-grocery_order-975c2b411e1a.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2023-04-13
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-992b8a3ce108]]
source_id: hfa-grocery_order-975c2b411e1a
store: Walgreens
summary: Instacart order from Walgreens
total: 24.72
type: grocery_order
uid: hfa-grocery_order-975c2b411e1a
updated: 2023-04-13
```

Body (truncated):

```markdown
# Instacart — Walgreens
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

**File:** `Transactions/Groceries/2025-08/hfa-grocery_order-2cdbc99397b2.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-08-18
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-3705c19aeb78]]
source_id: hfa-grocery_order-2cdbc99397b2
store: Wegmans
summary: Instacart order from Wegmans
total: 125.39
type: grocery_order
uid: hfa-grocery_order-2cdbc99397b2
updated: 2025-08-18
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

**File:** `Transactions/Groceries/2025-10/hfa-grocery_order-cd1d469fb31c.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-10-23
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-0e506b40c7d2]]
source_id: hfa-grocery_order-cd1d469fb31c
store: Wegmans
summary: Instacart order from Wegmans
total: 184.53
type: grocery_order
uid: hfa-grocery_order-cd1d469fb31c
updated: 2025-10-23
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

**File:** `Transactions/Groceries/2025-09/hfa-grocery_order-137fab9cd9f0.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-09-07
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-2daef003eb77]]
source_id: hfa-grocery_order-137fab9cd9f0
store: Wegmans
summary: Instacart order from Wegmans
total: 150.14
type: grocery_order
uid: hfa-grocery_order-137fab9cd9f0
updated: 2025-09-07
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

**File:** `Transactions/Groceries/2025-10/hfa-grocery_order-465b8e5d2201.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-10-18
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-2da974aa5edb]]
source_id: hfa-grocery_order-465b8e5d2201
store: Wegmans
summary: Instacart order from Wegmans
total: 182.7
type: grocery_order
uid: hfa-grocery_order-465b8e5d2201
updated: 2025-10-18
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

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

**File:** `Transactions/Groceries/2025-12/hfa-grocery_order-5ccd8abfa27f.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2025-12-09
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-252bfdab030e]]
source_id: hfa-grocery_order-5ccd8abfa27f
store: Wegmans
summary: Instacart order from Wegmans
total: 95.34
type: grocery_order
uid: hfa-grocery_order-5ccd8abfa27f
updated: 2025-12-09
```

Body (truncated):

```markdown
# Instacart — Wegmans
```

---

**File:** `Transactions/Groceries/2023-11/hfa-grocery_order-98ba6da7c603.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2023-11-23
extraction_confidence: 0.67
service: Instacart
source: ["email_extraction"]
source_email: [[hfa-email-message-6b20af9280cf]]
source_id: hfa-grocery_order-98ba6da7c603
store: Target
summary: Instacart order from Target
total: 59.17
type: grocery_order
uid: hfa-grocery_order-98ba6da7c603
updated: 2023-11-23
```

Body (truncated):

```markdown
# Instacart — Target
```

---

### `meal_order` (1025 cards)

_Typical extractors:_ `uber_eats`, `doordash`

- **Confidence distribution:** >=0.8: 135 (13%), 0.5–0.8: 706 (69%), <0.5: 184 (18%), n/a: 0

- **Share with ≥1 flag:** 894/1025 (87.2%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:items`: 751
  - `heuristic:empty_items`: 751
  - `critical_fail:restaurant`: 321
  - `heuristic:duplicate_suspect`: 12
  - `critical_fail:total`: 2

#### Flagged examples (prioritize fixes)

**File:** `Transactions/MealOrders/2022-03/hfa-meal_order-be0abf71486f.md`
**Flags:** `critical_fail:restaurant`, `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2022-03-29
extraction_confidence: 0.33
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-156b5597b2e3]]
source_id: hfa-meal_order-be0abf71486f
summary: hfa-meal_order-be0abf71486f
total: 30.82
type: meal_order
uid: hfa-meal_order-be0abf71486f
updated: 2022-03-29
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2020-10/hfa-meal_order-d8a7d00236a1.md`
**Flags:** `critical_fail:restaurant`

```yaml
created: 2020-10-15
extraction_confidence: 0.67
items: [{ "name": "Caesar Salad", "quantity": 2, "price": "19.05" }]
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-dd82c12b39c4]]
source_id: hfa-meal_order-d8a7d00236a1
subtotal: 38.1
summary: hfa-meal_order-d8a7d00236a1
tax: 4.5
total: 45.19
type: meal_order
uid: hfa-meal_order-d8a7d00236a1
updated: 2020-10-15
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2024-04/hfa-meal_order-befcbde0f629.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2024-04-28
extraction_confidence: 0.67
mode: delivery
restaurant: Brooklyn Hero Shop
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-aabec6022ed2]]
source_id: hfa-meal_order-befcbde0f629
summary: DoorDash order from Brooklyn Hero Shop
total: 31.85
type: meal_order
uid: hfa-meal_order-befcbde0f629
updated: 2024-04-28
```

Body (truncated):

```markdown
# DoorDash — Brooklyn Hero Shop

- **Total**: $31.85
```

---

**File:** `Transactions/MealOrders/2020-09/hfa-meal_order-0ce01528c78a.md`
**Flags:** `critical_fail:restaurant`

```yaml
created: 2020-09-04
extraction_confidence: 0.67
items:
  [
    { "name": "Ají Amarillo Huacatay", "quantity": 1, "price": "2.00" },
    { "name": "Quarter White", "quantity": 1, "price": "14.95" },
  ]
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-104dfb089989]]
source_id: hfa-meal_order-0ce01528c78a
subtotal: 16.95
summary: hfa-meal_order-0ce01528c78a
tax: 1.44
total: 20.93
type: meal_order
uid: hfa-meal_order-0ce01528c78a
updated: 2020-09-04
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2022-08/hfa-meal_order-694557fc42b3.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2022-08-19
extraction_confidence: 0.67
mode: delivery
restaurant: Em Vietnamese Bistro
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-35288fba448a]]
source_id: hfa-meal_order-694557fc42b3
summary: DoorDash order from Em Vietnamese Bistro
total: 32.82
type: meal_order
uid: hfa-meal_order-694557fc42b3
updated: 2022-08-19
```

Body (truncated):

```markdown
# DoorDash — Em Vietnamese Bistro

- **Total**: $32.82
```

---

**File:** `Transactions/MealOrders/2024-12/hfa-meal_order-3d92d081d088.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2024-12-09
extraction_confidence: 0.67
mode: delivery
restaurant: Em Vietnamese Bistro
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-04df647d44ee]]
source_id: hfa-meal_order-3d92d081d088
summary: DoorDash order from Em Vietnamese Bistro
total: 37.46
type: meal_order
uid: hfa-meal_order-3d92d081d088
updated: 2024-12-09
```

Body (truncated):

```markdown
# DoorDash — Em Vietnamese Bistro

- **Total**: $37.46
```

---

**File:** `Transactions/MealOrders/2022-04/hfa-meal_order-38647b8ef4ec.md`
**Flags:** `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2022-04-28
extraction_confidence: 0.67
mode: delivery
restaurant: tanglad
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-c3ad0e0baa9e]]
source_id: hfa-meal_order-38647b8ef4ec
summary: DoorDash order from tanglad
total: 20.05
type: meal_order
uid: hfa-meal_order-38647b8ef4ec
updated: 2022-04-28
```

Body (truncated):

```markdown
# DoorDash — tanglad

- **Total**: $20.05
```

---

**File:** `Transactions/MealOrders/2021-07/hfa-meal_order-582b42e5c8c6.md`
**Flags:** `critical_fail:restaurant`, `critical_fail:items`, `heuristic:empty_items`

```yaml
created: 2021-07-26
extraction_confidence: 0.33
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-444cfcbebf49]]
source_id: hfa-meal_order-582b42e5c8c6
summary: hfa-meal_order-582b42e5c8c6
total: 27.71
type: meal_order
uid: hfa-meal_order-582b42e5c8c6
updated: 2021-07-26
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly.
```

---

**File:** `Transactions/MealOrders/2019-10/hfa-meal_order-0f917e408719.md`
**Flags:** `critical_fail:restaurant`

```yaml
created: 2019-10-24
extraction_confidence: 0.67
items: [{ "name": "Royal Couscous", "quantity": 1, "price": "21.00" }]
mode: delivery
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-a86f3daa5dd6]]
source_id: hfa-meal_order-0f917e408719
subtotal: 21.0
summary: hfa-meal_order-0f917e408719
tax: 1.79
total: 24.89
type: meal_order
uid: hfa-meal_order-0f917e408719
updated: 2019-10-24
```

Body (truncated):

```markdown
# Uber Eats — your bank statement shortly. Learn More xid5987fd7c-61d3-5deb-b208-83d9822b22b3 pGvlI2ANUbXFfyEOgxta1RMV082993
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

#### Clean examples (quality bar)

**File:** `Transactions/MealOrders/2020-09/hfa-meal_order-3fa45b57ab94.md`

```yaml
created: 2020-09-27
extraction_confidence: 1.0
items:
  [
    { "name": "Seasonal Horiatiki", "quantity": 1, "price": "27.60" },
    { "name": "Lamb Gyro", "quantity": 1, "price": "20.70" },
  ]
mode: delivery
restaurant: NoVY Restaurant
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-0e1816367778]]
source_id: hfa-meal_order-3fa45b57ab94
subtotal: 48.3
summary: Uber Eats order from NoVY Restaurant
tax: 4.11
total: 60.23
type: meal_order
uid: hfa-meal_order-3fa45b57ab94
updated: 2020-09-27
```

Body (truncated):

```markdown
# Uber Eats — NoVY Restaurant
```

---

**File:** `Transactions/MealOrders/2021-06/hfa-meal_order-7644949fb8be.md`

```yaml
created: 2021-06-09
delivery_fee: 2.99
extraction_confidence: 1.0
items: [{ "name": "Chicken Shish Tawook Plate (Proteins)", "quantity": 1, "price": "" }]
mode: delivery
restaurant: Beit Rima
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-ee27c32d5d6f]]
source_id: hfa-meal_order-7644949fb8be
subtotal: 20.0
summary: DoorDash order from Beit Rima
tip: 5.0
total: 33.69
type: meal_order
uid: hfa-meal_order-7644949fb8be
updated: 2021-06-09
```

Body (truncated):

```markdown
# DoorDash — Beit Rima

| Item                                  | Qty | Price |
| ------------------------------------- | --- | ----- |
| Chicken Shish Tawook Plate (Proteins) | 1   |       |

- **Subtotal**: $20.00
- **Delivery Fee**: $2.99
- **Tip**: $5.00
- **Total**: $33.69
```

---

**File:** `Transactions/MealOrders/2021-05/hfa-meal_order-56fe78f4a70d.md`

```yaml
created: 2021-05-16
extraction_confidence: 1.0
items:
  [
    { "name": "Can Sodas (Drinks)", "quantity": 1, "price": "" },
    { "name": "Deluxe Wrap (Wraps)", "quantity": 1, "price": "" },
  ]
mode: delivery
restaurant: Yumma's
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-9cbeacbfd9fd]]
source_id: hfa-meal_order-56fe78f4a70d
subtotal: 16.84
summary: DoorDash order from Yumma's
total: 18.27
type: meal_order
uid: hfa-meal_order-56fe78f4a70d
updated: 2021-05-16
```

Body (truncated):

```markdown
# DoorDash — Yumma's

| Item                | Qty | Price |
| ------------------- | --- | ----- |
| Can Sodas (Drinks)  | 1   |       |
| Deluxe Wrap (Wraps) | 1   |       |

- **Subtotal**: $16.84
- **Delivery Fee**: $0.00
- **Total**: $18.27
```

---

**File:** `Transactions/MealOrders/2021-02/hfa-meal_order-efff61767314.md`

```yaml
created: 2021-02-02
extraction_confidence: 1.0
items: [{"name": "Chicken Shish Tawook Plate (Proteins)", "quantity": 1, "price": ""}, {"name": "Diet Coke (Drinks)", "quantity": 1, "price": ""}, {"name": "Sparking Water (Drinks)", "quantity": 1, "price...
mode: delivery
restaurant: Beit Rima
service: DoorDash
source: ["email_extraction"]
source_email: [[hfa-email-message-c7fecfe7cf1b]]
source_id: hfa-meal_order-efff61767314
subtotal: 26.0
summary: DoorDash order from Beit Rima
tip: 6.0
total: 35.51
type: meal_order
uid: hfa-meal_order-efff61767314
updated: 2021-02-02
```

Body (truncated):

```markdown
# DoorDash — Beit Rima

| Item                                  | Qty | Price |
| ------------------------------------- | --- | ----- |
| Chicken Shish Tawook Plate (Proteins) | 1   |       |
| Diet Coke (Drinks)                    | 1   |       |
| Sparking Water (Drinks)               | 1   |       |

- **Subtotal**: $26.00
- **Delivery Fee**: $0.00
- **Tip**: $6.00
- **Total**: $35.51
```

---

**File:** `Transactions/MealOrders/2020-09/hfa-meal_order-dd217c931b49.md`

```yaml
created: 2020-09-02
extraction_confidence: 1.0
items: [{"name": "Garlic Naan", "quantity": 1, "price": "2.95"}, {"name": "Basmati Rice", "quantity": 2, "price": "3.25"}, {"name": "Tandoori Mixed Grilled", "quantity": 1, "price": "19.95"}, {"name": "On...
mode: delivery
restaurant: Mission Curry House
service: Uber Eats
source: ["email_extraction"]
source_email: [[hfa-email-message-be3564067f2f]]
source_id: hfa-meal_order-dd217c931b49
subtotal: 74.8
summary: Uber Eats order from Mission Curry House
tax: 6.36
total: 83.87
type: meal_order
uid: hfa-meal_order-dd217c931b49
updated: 2020-09-02
```

Body (truncated):

```markdown
# Uber Eats — Mission Curry House
```

---

### `ride` (677 cards)

_Typical extractors:_ `uber_rides`, `lyft`

- **Confidence distribution:** >=0.8: 665 (98%), 0.5–0.8: 3 (0%), <0.5: 9 (1%), n/a: 0

- **Share with ≥1 flag:** 12/677 (1.8%)
- **Flag counts (cards can have multiple):**
  - `critical_fail:dropoff_location`: 12
  - `heuristic:weak_dropoff`: 12
  - `critical_fail:pickup_location`: 9
  - `heuristic:weak_pickup`: 9

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Rides/2014-06/hfa-ride-d4385fafde6c.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-06-09
distance_miles: 14.8
driver_name: Mahfoud
duration_minutes: 20.0
extraction_confidence: 0.33
fare: 36.49
pickup_at: 2014-06-09T06:36:38Z
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-45fcd5ad6f02]]
source_id: hfa-ride-d4385fafde6c
summary: hfa-ride-d4385fafde6c
type: ride
uid: hfa-ride-d4385fafde6c
updated: 2014-06-09
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-06-09T06:36:38Z
- **Fare**: 36.49
- **Distance (mi)**: 14.8
- **Duration (min)**: 20.0
- **Driver**: Mahfoud
```

---

**File:** `Transactions/Rides/2013-12/hfa-ride-2fdf6f517e76.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-12-08
distance_miles: 1.93
driver_name: Munir
duration_minutes: 7.0
extraction_confidence: 0.33
fare: 9.83
pickup_at: 2013-12-08T05:26:35Z
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-451041f24ed3]]
source_id: hfa-ride-2fdf6f517e76
summary: hfa-ride-2fdf6f517e76
type: ride
uid: hfa-ride-2fdf6f517e76
updated: 2013-12-08
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2013-12-08T05:26:35Z
- **Fare**: 9.83
- **Distance (mi)**: 1.93
- **Duration (min)**: 7.0
- **Driver**: Munir
```

---

**File:** `Transactions/Rides/2013-10/hfa-ride-987466591e8b.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-10-05
driver_name: Hussein
duration_minutes: 25.0
extraction_confidence: 0.33
fare: 69.88
pickup_at: 2013-10-05T11:53:00Z
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-986581849cbd]]
source_id: hfa-ride-987466591e8b
summary: hfa-ride-987466591e8b
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

**File:** `Transactions/Rides/2022-09/hfa-ride-3fe9eb34fe4c.md`
**Flags:** `critical_fail:dropoff_location`, `heuristic:weak_dropoff`

```yaml
created: 2022-09-24
distance_miles: 4.87
driver_name: FIND LOST ITEM
extraction_confidence: 0.67
fare: 77.97
pickup_at: 2022-09-24T08:06:45Z
pickup_location: and recipient.ride.dropoff */
ride_type: car
service: Lyft
source: ["email_extraction"]
source_email: [[hfa-email-message-fa6c6b5a02f7]]
source_id: hfa-ride-3fe9eb34fe4c
summary: hfa-ride-3fe9eb34fe4c
type: ride
uid: hfa-ride-3fe9eb34fe4c
updated: 2022-09-24
vehicle: Lyft XL
```

Body (truncated):

```markdown
# Lyft ride

- **Pickup**: and recipient.ride.dropoff \*/
- **Dropoff**: \*/
- **Pickup at**: 2022-09-24T08:06:45Z
- **Fare**: 77.97
- **Driver**: FIND LOST ITEM
```

---

**File:** `Transactions/Rides/2013-11/hfa-ride-bb55a6e28c31.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-11-09
distance_miles: 1.37
driver_name: Jhalman
duration_minutes: 8.0
extraction_confidence: 0.33
fare: 10.8
pickup_at: 2013-11-09T22:56:11Z
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-c84993a9cb38]]
source_id: hfa-ride-bb55a6e28c31
summary: hfa-ride-bb55a6e28c31
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

**File:** `Transactions/Rides/2024-11/hfa-ride-8b956339609a.md`
**Flags:** `critical_fail:dropoff_location`, `heuristic:weak_dropoff`

```yaml
created: 2024-11-14
distance_miles: 18.03
driver_name: Find lost item
extraction_confidence: 0.67
fare: 67.88
pickup_at: 2024-11-14T03:48:57Z
pickup_location: and recipient.ride.dropoff */
ride_type: car
service: Lyft
source: ["email_extraction"]
source_email: [[hfa-email-message-5f22e22a566c]]
source_id: hfa-ride-8b956339609a
summary: hfa-ride-8b956339609a
type: ride
uid: hfa-ride-8b956339609a
updated: 2024-11-14
vehicle: green
```

Body (truncated):

```markdown
# Lyft ride

- **Pickup**: and recipient.ride.dropoff \*/
- **Dropoff**: \*/
- **Pickup at**: 2024-11-14T03:48:57Z
- **Fare**: 67.88
- **Driver**: Find lost item
```

---

**File:** `Transactions/Rides/2014-07/hfa-ride-c4a75e72f0a4.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-07-03
distance_miles: 15.6
driver_name: Jacob
duration_minutes: 36.0
extraction_confidence: 0.33
fare: 42.48
pickup_at: 2014-07-03T23:23:23Z
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-a6534817308c]]
source_id: hfa-ride-c4a75e72f0a4
summary: hfa-ride-c4a75e72f0a4
type: ride
uid: hfa-ride-c4a75e72f0a4
updated: 2014-07-03
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-07-03T23:23:23Z
- **Fare**: 42.48
- **Distance (mi)**: 15.6
- **Duration (min)**: 36.0
- **Driver**: Jacob
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2013-12/hfa-ride-a74458c5210c.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2013-12-02
distance_miles: 14.82
driver_name: Adel
duration_minutes: 18.0
extraction_confidence: 0.33
fare: 50.0
pickup_at: 2013-12-02T07:11:03Z
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-a2eb203942d7]]
source_id: hfa-ride-a74458c5210c
summary: hfa-ride-a74458c5210c
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

**File:** `Transactions/Rides/2014-01/hfa-ride-1b9c4dc7447b.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-01-17
distance_miles: 15.34
driver_name: Fares
duration_minutes: 30.0
extraction_confidence: 0.33
fare: 35.14
pickup_at: 2014-01-17T16:12:58Z
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-d63246043359]]
source_id: hfa-ride-1b9c4dc7447b
summary: hfa-ride-1b9c4dc7447b
type: ride
uid: hfa-ride-1b9c4dc7447b
updated: 2014-01-17
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-01-17T16:12:58Z
- **Fare**: 35.14
- **Distance (mi)**: 15.34
- **Duration (min)**: 30.0
- **Driver**: Fares
```

---

**File:** `Transactions/Rides/2014-04/hfa-ride-742b44fa1220.md`
**Flags:** `critical_fail:pickup_location`, `critical_fail:dropoff_location`, `heuristic:weak_pickup`, `heuristic:weak_dropoff`

```yaml
created: 2014-04-05
distance_miles: 2.16
driver_name: mitchell
duration_minutes: 14.0
extraction_confidence: 0.33
fare: 10.6
pickup_at: 2014-04-05T00:02:24Z
ride_type: car
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-7b85cfe8b598]]
source_id: hfa-ride-742b44fa1220
summary: hfa-ride-742b44fa1220
type: ride
uid: hfa-ride-742b44fa1220
updated: 2014-04-05
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: Location:
- **Dropoff**: Location:
- **Pickup at**: 2014-04-05T00:02:24Z
- **Fare**: 10.6
- **Distance (mi)**: 2.16
- **Duration (min)**: 14.0
- **Driver**: mitchell
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Rides/2015-02/hfa-ride-e10358a0eed6.md`

```yaml
created: 2015-02-01
dropoff_location: 49 Duboce Avenue, San Francisco, CA
extraction_confidence: 1.0
fare: 16.71
pickup_at: 2015-02-01T07:22:42Z
pickup_location: 2572 Pine Street, San Francisco, CA
ride_type: uberXL
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-c98e5f2551a3]]
source_id: hfa-ride-e10358a0eed6
summary: Uber from 2572 Pine Street, San Francisco, CA to 49 Duboce Avenue, San Francisco, CA
type: ride
uid: hfa-ride-e10358a0eed6
updated: 2015-02-01
vehicle: uberXL
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2572 Pine Street, San Francisco, CA
- **Dropoff**: 49 Duboce Avenue, San Francisco, CA
- **Pickup at**: 2015-02-01T07:22:42Z
- **Fare**: 16.71
- **Vehicle**: uberXL
```

---

**File:** `Transactions/Rides/2016-09/hfa-ride-368e86f0353a.md`

```yaml
created: 2016-09-02
driver_name: VALERIO
dropoff_location: 2120 Colorado Ave, Santa Monica, CA
extraction_confidence: 1.0
fare: 9.59
pickup_at: 2016-09-02T01:33:15Z
pickup_location: 4375 Admiralty Way, Marina Del Rey, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-3465b950d759]]
source_id: hfa-ride-368e86f0353a
summary: Uber from 4375 Admiralty Way, Marina Del Rey, CA to 2120 Colorado Ave, Santa Monica, CA
type: ride
uid: hfa-ride-368e86f0353a
updated: 2016-09-02
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 4375 Admiralty Way, Marina Del Rey, CA
- **Dropoff**: 2120 Colorado Ave, Santa Monica, CA
- **Pickup at**: 2016-09-02T01:33:15Z
- **Fare**: 9.59
- **Driver**: VALERIO
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2023-10/hfa-ride-fdbd3f16d517.md`

```yaml
created: 2023-10-06
distance_miles: 25.62
driver_name: Benefits Fee
dropoff_location: 3758 21st St, San Francisco, CA
extraction_confidence: 1.0
fare: 63.45
pickup_at: 2023-10-06T03:25:45Z
pickup_location: 2115 Broadway St, Redwood City, CA
ride_type: car
service: Lyft
source: ["email_extraction"]
source_email: [[hfa-email-message-a160b3b5b7c5]]
source_id: hfa-ride-fdbd3f16d517
summary: Lyft from 2115 Broadway St, Redwood City, CA to 3758 21st St, San Francisco, CA
type: ride
uid: hfa-ride-fdbd3f16d517
updated: 2023-10-06
vehicle: green
```

Body (truncated):

```markdown
# Lyft ride

- **Pickup**: 2115 Broadway St, Redwood City, CA
- **Dropoff**: 3758 21st St, San Francisco, CA
- **Pickup at**: 2023-10-06T03:25:45Z
- **Fare**: 63.45
- **Driver**: Benefits Fee
```

---

**File:** `Transactions/Rides/2015-01/hfa-ride-07b26d1cc7db.md`

```yaml
created: 2015-01-16
dropoff_location: 676 Oak Street, San Francisco, CA
extraction_confidence: 1.0
fare: 7.36
pickup_at: 2015-01-16T03:55:29Z
pickup_location: 2572 Pine Street, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-7834fa4f042a]]
source_id: hfa-ride-07b26d1cc7db
summary: Uber from 2572 Pine Street, San Francisco, CA to 676 Oak Street, San Francisco, CA
type: ride
uid: hfa-ride-07b26d1cc7db
updated: 2015-01-16
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2572 Pine Street, San Francisco, CA
- **Dropoff**: 676 Oak Street, San Francisco, CA
- **Pickup at**: 2015-01-16T03:55:29Z
- **Fare**: 7.36
- **Vehicle**: uberX
```

---

**File:** `Transactions/Rides/2016-07/hfa-ride-c3cf945e5e8b.md`

```yaml
created: 2016-07-27
driver_name: Suki
dropoff_location: 307 Hayes St, San Francisco, CA
extraction_confidence: 1.0
fare: 7.0
pickup_at: 2016-07-27T15:01:43Z
pickup_location: 2915b 23rd St, San Francisco, CA
ride_type: uberX
service: Uber
source: ["email_extraction"]
source_email: [[hfa-email-message-bb70c4b3f614]]
source_id: hfa-ride-c3cf945e5e8b
summary: Uber from 2915b 23rd St, San Francisco, CA to 307 Hayes St, San Francisco, CA
type: ride
uid: hfa-ride-c3cf945e5e8b
updated: 2016-07-27
vehicle: uberX
```

Body (truncated):

```markdown
# Uber ride

- **Pickup**: 2915b 23rd St, San Francisco, CA
- **Dropoff**: 307 Hayes St, San Francisco, CA
- **Pickup at**: 2016-07-27T15:01:43Z
- **Fare**: 7.0
- **Driver**: Suki
- **Vehicle**: uberX
```

---

### `shipment` (695 cards)

_Typical extractors:_ `shipping`

- **Confidence distribution:** >=0.8: 695 (100%), 0.5–0.8: 0 (0%), <0.5: 0 (0%), n/a: 0

- **Share with ≥1 flag:** 578/695 (83.2%)
- **Flag counts (cards can have multiple):**
  - `heuristic:duplicate_suspect`: 578

#### Flagged examples (prioritize fixes)

**File:** `Transactions/Shipments/2018-11/hfa-shipment-e1dd3d02bd7c.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2018-11-19
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-fb143357dd71]]
source_id: hfa-shipment-e1dd3d02bd7c
summary: UPS 1ZA2E9136704615020
tracking_number: 1ZA2E9136704615020
type: shipment
uid: hfa-shipment-e1dd3d02bd7c
updated: 2018-11-19
```

Body (truncated):

```markdown
# Shipment UPS 1ZA2E9136704615020
```

---

**File:** `Transactions/Shipments/2025-12/hfa-shipment-f672de6444e3.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2025-12-04
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-a704a7cd3487]]
source_id: hfa-shipment-f672de6444e3
summary: FedEx 491406912334
tracking_number: 491406912334
type: shipment
uid: hfa-shipment-f672de6444e3
updated: 2025-12-04
```

Body (truncated):

```markdown
# Shipment FedEx 491406912334
```

---

**File:** `Transactions/Shipments/2019-11/hfa-shipment-d496d548337b.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2019-11-17
estimated_delivery: Time: 11:30 AM - 02:30 PM
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-47f87fe89d20]]
source_id: hfa-shipment-d496d548337b
summary: UPS 1Z7X3W68A892324204
tracking_number: 1Z7X3W68A892324204
type: shipment
uid: hfa-shipment-d496d548337b
updated: 2019-11-17
```

Body (truncated):

```markdown
# Shipment UPS 1Z7X3W68A892324204
```

---

**File:** `Transactions/Shipments/2019-03/hfa-shipment-13e5394882d1.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2019-03-22
estimated_delivery: Time: 12:30 PM - 03:30 PM
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-92c237afe3d6]]
source_id: hfa-shipment-13e5394882d1
summary: UPS 1ZE105X9P204417696
tracking_number: 1ZE105X9P204417696
type: shipment
uid: hfa-shipment-13e5394882d1
updated: 2019-03-22
```

Body (truncated):

```markdown
# Shipment UPS 1ZE105X9P204417696
```

---

**File:** `Transactions/Shipments/2021-10/hfa-shipment-c1e078e4f508.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2021-10-07
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-5ad128559bdc]]
source_id: hfa-shipment-c1e078e4f508
summary: UPS 1Z81F62A0392144361
tracking_number: 1Z81F62A0392144361
type: shipment
uid: hfa-shipment-c1e078e4f508
updated: 2021-10-07
```

Body (truncated):

```markdown
# Shipment UPS 1Z81F62A0392144361
```

---

**File:** `Transactions/Shipments/2024-01/hfa-shipment-42c3cbd15f63.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2024-01-23
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-67214a1c9ed5]]
source_id: hfa-shipment-42c3cbd15f63
summary: UPS 1ZX07Y270374347903
tracking_number: 1ZX07Y270374347903
type: shipment
uid: hfa-shipment-42c3cbd15f63
updated: 2024-01-23
```

Body (truncated):

```markdown
# Shipment UPS 1ZX07Y270374347903
```

---

**File:** `Transactions/Shipments/2020-11/hfa-shipment-3ea01934c114.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2020-11-20
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-b0257932aec4]]
source_id: hfa-shipment-3ea01934c114
summary: UPS 1ZX289556690922848
tracking_number: 1ZX289556690922848
type: shipment
uid: hfa-shipment-3ea01934c114
updated: 2020-11-20
```

Body (truncated):

```markdown
# Shipment UPS 1ZX289556690922848
```

---

**File:** `Transactions/Shipments/2022-01/hfa-shipment-56495dbfb7cd.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: UPS
created: 2022-01-30
delivered_at: Saturday 01/29/2022 5:32 PM
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-a2b071c1d612]]
source_id: hfa-shipment-56495dbfb7cd
summary: UPS 1Z691VR30340139599
tracking_number: 1Z691VR30340139599
type: shipment
uid: hfa-shipment-56495dbfb7cd
updated: 2022-01-30
```

Body (truncated):

```markdown
# Shipment UPS 1Z691VR30340139599
```

---

**File:** `Transactions/Shipments/2024-06/hfa-shipment-81d503b8ffc9.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2024-06-20
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-dd6022db6161]]
source_id: hfa-shipment-81d503b8ffc9
summary: FedEx 776708038902
tracking_number: 776708038902
type: shipment
uid: hfa-shipment-81d503b8ffc9
updated: 2024-06-20
```

Body (truncated):

```markdown
# Shipment FedEx 776708038902
```

---

**File:** `Transactions/Shipments/2024-06/hfa-shipment-4fdd2de693cd.md`
**Flags:** `heuristic:duplicate_suspect`

```yaml
carrier: FedEx
created: 2024-06-14
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-550932e06b29]]
source_id: hfa-shipment-4fdd2de693cd
summary: FedEx 747336045230
tracking_number: 747336045230
type: shipment
uid: hfa-shipment-4fdd2de693cd
updated: 2024-06-14
```

Body (truncated):

```markdown
# Shipment FedEx 747336045230
```

---

#### Clean examples (quality bar)

**File:** `Transactions/Shipments/2019-10/hfa-shipment-cf1f246d262e.md`

```yaml
carrier: FedEx
created: 2019-10-23
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-6a7a9f0ef593]]
source_id: hfa-shipment-cf1f246d262e
summary: FedEx 31587792584301
tracking_number: 31587792584301
type: shipment
uid: hfa-shipment-cf1f246d262e
updated: 2019-10-23
```

Body (truncated):

```markdown
# Shipment FedEx 31587792584301
```

---

**File:** `Transactions/Shipments/2020-11/hfa-shipment-2d6098c67e8f.md`

```yaml
carrier: FedEx
created: 2020-11-23
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-b45bab04aaf6]]
source_id: hfa-shipment-2d6098c67e8f
summary: FedEx 39351994690301
tracking_number: 39351994690301
type: shipment
uid: hfa-shipment-2d6098c67e8f
updated: 2020-11-23
```

Body (truncated):

```markdown
# Shipment FedEx 39351994690301
```

---

**File:** `Transactions/Shipments/2020-12/hfa-shipment-376d3a67fe24.md`

```yaml
carrier: FedEx
created: 2020-12-03
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-fe6bd0f4528f]]
source_id: hfa-shipment-376d3a67fe24
summary: FedEx 39751169383301
tracking_number: 39751169383301
type: shipment
uid: hfa-shipment-376d3a67fe24
updated: 2020-12-03
```

Body (truncated):

```markdown
# Shipment FedEx 39751169383301
```

---

**File:** `Transactions/Shipments/2021-01/hfa-shipment-ff1e9d749acc.md`

```yaml
carrier: FedEx
created: 2021-01-02
extraction_confidence: 1.0
source: ["email_extraction"]
source_email: [[hfa-email-message-d9677c528827]]
source_id: hfa-shipment-ff1e9d749acc
summary: FedEx 41308445903301
tracking_number: 41308445903301
type: shipment
uid: hfa-shipment-ff1e9d749acc
updated: 2021-01-02
```

Body (truncated):

```markdown
# Shipment FedEx 41308445903301
```

---

**File:** `Transactions/Shipments/2024-06/hfa-shipment-755b397b806d.md`

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

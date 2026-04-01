# PPA v2 Vision

---

## The Core Thesis

The PPA today is a **search engine over digital artifacts**. The goal is to make it a **knowledge system** — one that understands your life well enough to answer questions instantly, build context automatically, and get smarter over time. The interface stays simple (MCP tools for chatbots), but the intelligence moves inside the archive itself.

The build is sequenced so that **every expensive operation — rebuild, embedding, enrichment — happens exactly once**, after all upstream changes that would invalidate it are in place.

---

## Principles

These decision rules govern the entire vision. When executing any phase, defer to these when facing ambiguous choices.

1. **Schema before data, data before index, index before embeddings, embeddings before enrichment, enrichment before aggregation, aggregation before agent.** Each layer's output is consumed by the next. Nothing gets thrown away.

2. **Every card represents a proven action, booking, request, or communication made by the archive owner.** Promotional materials, marketing emails, and passive notifications are not worth extracting into derived cards. The archive is about what you _did_, not what was _advertised to you_.

3. **Extractors must be idempotent.** Running `ppa extract-emails` twice against the same vault produces identical output. UIDs for derived cards are deterministic functions of their source card's UID and key fields.

4. **The test corpus must be relationally complete.** Every wikilink resolves. Every person reference has a PersonCard. Every thread has its messages. Zero orphans. This is the standard against which all correctness is measured.

5. **Rebuilds are expensive and should be avoidable.** CI/CD should validate changes against the test corpus so that deploying to production is a confidence operation, not a debugging session. The goal is a gitflow where work tested against the mock DB produces the verification needed to take it to production.

6. **Make the archive smarter, not the consumer.** Every chatbot, voice assistant, or agent that connects to the PPA should get the benefit of its accumulated understanding without being taught how the archive works.

7. **Derived card bodies carry the highest-fidelity human-readable representation of the transaction.** A meal_order body should read like an itemized receipt — what was ordered, customizations, prices — so that prompts like "what kind of carrots do I usually buy?" or "where did I get that amazing delivery banh mi?" work against the full text. If itemized data isn't available, the body is a summary at whatever fidelity the source email provides.

8. **Provenance is preserved for every field.** Deterministically-extracted fields are tagged `deterministic`. LLM-enriched fields are tagged `llm`. Derived cards write provenance blocks following the existing `<!-- provenance ... -->` convention.

9. **Operational logging on long jobs.** Any PPA command or agent-driven pipeline expected to run more than a few minutes must emit structured progress (phase, counts or %, elapsed and ETA in **`M:SS`**, throughput where meaningful) via the `ppa.*` loggers, with optional **`ppa --log-file PATH <subcommand>`** for retained, tail-friendly artifacts under `ppa/logs/`. See **`.cursor/rules/ppa-long-running-jobs.mdc`** (workspace rule, always on) and Phase 0’s **Operational logging** subsection for `slice-seed`, rebuild, benchmark, and CI targets.

---

## Complete Card Type Inventory

### Existing types (22) — these stay as-is

| Category  | Types                                                        |
| --------- | ------------------------------------------------------------ |
| People    | `person`                                                     |
| Finance   | `finance`                                                    |
| Health    | `medical_record`, `vaccination`                              |
| Email     | `email_thread`, `email_message`, `email_attachment`          |
| iMessage  | `imessage_thread`, `imessage_message`, `imessage_attachment` |
| Beeper    | `beeper_thread`, `beeper_message`, `beeper_attachment`       |
| Calendar  | `calendar_event`                                             |
| Media     | `media_asset`                                                |
| Documents | `document`                                                   |
| Meetings  | `meeting_transcript`                                         |
| Code      | `git_repository`, `git_commit`, `git_thread`, `git_message`  |

### New derived types (11) — extracted from emails and other sources

| Type            | Source Senders / Data                                                                                                    | Key Fields Beyond BaseCard                                                                                                                                                                                 | Body Content                                                                                                                                     | Estimated Volume                        | Why It Needs Its Own Type                                                                                                                                                                                                                                                                                                                                  |
| --------------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `meal_order`    | DoorDash, UberEats, Postmates, Caviar, Grubhub                                                                           | `service`, `restaurant`, `items` (list of {name, qty, price, customizations}), `subtotal`, `total`, `tip`, `delivery_fee`, `tax`, `mode` (pickup/delivery), `delivery_address`, `source_email`             | Itemized receipt: each item with name, customizations, quantity, price. When items aren't parseable, a summary with restaurant + total.          | ~1,000–1,500                            | Items, restaurant, tip/fee breakdown are unique to food delivery. Enables "what do I eat most," "where did I get that banh mi," "spending by restaurant."                                                                                                                                                                                                  |
| `grocery_order` | Instacart, Amazon Fresh, FreshDirect                                                                                     | `service`, `store`, `items` (list of {name, qty, price, unit}), `subtotal`, `total`, `delivery_fee`, `delivery_address`, `source_email`                                                                    | Itemized grocery list with quantities and prices. "2x Organic Baby Carrots — $3.99" level detail.                                                | ~200–500                                | Distinct from meal_order — items are groceries not prepared food. Enables "what kind of carrots do I usually buy" and household provisioning analysis.                                                                                                                                                                                                     |
| `ride`          | Uber, Lyft, Scoot, Lime, Bird, CitiBike                                                                                  | `service`, `ride_type` (car/scooter/bike), `pickup_location`, `dropoff_location`, `pickup_at`, `dropoff_at`, `fare`, `tip`, `distance_miles`, `duration_minutes`, `driver_name`, `vehicle`, `source_email` | Route summary: "Uber from 123 Main St, Brooklyn to JFK Terminal 4 — 18.3 mi, 42 min — $54.20"                                                    | ~500–2,000                              | Origin/destination, distance, duration. Enables location inference, commute patterns, travel reconstruction.                                                                                                                                                                                                                                               |
| `flight`        | United, Delta, JetBlue, Hawaiian, American, Surf Air, booking aggregators                                                | `airline`, `confirmation_code`, `origin_airport`, `destination_airport`, `departure_at`, `arrival_at`, `fare_class`, `seat`, `fare_amount`, `booking_source`, `passengers`, `source_email`                 | Flight itinerary: "United UA 1234 — SFO → JFK — Dec 15 8:00am → 4:30pm — Economy Plus 12C"                                                       | ~50–100                                 | Route, airports, fare class, seat. Enables "where have I traveled" and trip reconstruction.                                                                                                                                                                                                                                                                |
| `accommodation` | Booking.com, Expedia, Hotels.com, Airbnb, 1Hotels                                                                        | `property_name`, `property_type` (hotel/airbnb/rental), `address`, `check_in`, `check_out`, `confirmation_code`, `nightly_rate`, `total_cost`, `guests`, `booking_source`, `source_email`                  | Stay summary: "Airbnb — Silverlake Bungalow, 1234 Sunset Blvd, LA — Dec 28 → Jan 5 — $180/night — Confirmation ABC123"                           | ~30–50                                  | Check-in/out dates, property info, nightly rate. Enables "where have I stayed" and trip cost reconstruction.                                                                                                                                                                                                                                               |
| `car_rental`    | National, Hertz, Emerald Club                                                                                            | `company`, `pickup_location`, `dropoff_location`, `pickup_at`, `dropoff_at`, `vehicle_class`, `confirmation_code`, `total_cost`, `source_email`                                                            | Rental summary: "National Car Rental — LAX pickup Dec 28, return Jan 5 — Midsize — $342.00"                                                      | ~10–30                                  | Pickup/dropoff locations and dates, vehicle class. Distinct from a ride (multi-day, self-driven).                                                                                                                                                                                                                                                          |
| `purchase`      | Amazon, eBay, Etsy, Costco, Wayfair, RH, Crate & Barrel, Shopbop, Target, MrPorter, Chewy, Drizly                        | `vendor`, `items` (list of {name, qty, price}), `subtotal`, `total`, `tax`, `shipping_cost`, `shipping_address`, `order_number`, `payment_method`, `source_email`                                          | Itemized order: "Amazon Order #112-1234567-1234567 — 1x Philips Hue Starter Kit $129.99, 1x USB-C Cable $12.99 — Ship to: 123 Main St, Brooklyn" | ~500–2,000                              | Items, vendor, shipping address, order number. Item-level detail that FinanceCard doesn't carry.                                                                                                                                                                                                                                                           |
| `shipment`      | UPS, FedEx, USPS, Amazon shipping notifications                                                                          | `carrier`, `tracking_number`, `shipped_at`, `estimated_delivery`, `delivered_at`, `origin`, `destination`, `linked_purchase` (wikilink to purchase card), `source_email`                                   | Tracking summary: "UPS 1Z999AA10123456784 — Shipped Dec 20, Est. delivery Dec 23 — From: Amazon Fulfillment, To: 123 Main St Brooklyn"           | ~500–2,000 (roughly 1:1 with purchases) | Tracking lifecycle. Links to purchase cards. Enables "when did X arrive."                                                                                                                                                                                                                                                                                  |
| `subscription`  | Spotify, Netflix, Apple, SaaS tools (Notion, Figma, Calendly, Otter), NYT, The Information, Stratechery, gym memberships | `service_name`, `plan_name`, `price`, `currency`, `billing_cycle` (monthly/annual), `event_type` (started/renewed/cancelled/paused/upgraded), `event_at`, `source_email`                                   | Lifecycle event: "Spotify Premium — $10.99/month — Renewed Jan 15, 2026" or "NYT Digital — Cancelled Dec 1, 2024"                                | ~50–100 lifecycle events                | One card per lifecycle event (not one card per service). Each event links to its source email and to other events for the same service via `service_name`. Enables "what am I subscribed to" (query latest event per service), "when did I cancel NYT" (filter by event_type), and full subscription history. Follows the "one card per action" principle. |
| `event_ticket`  | Ticketmaster, Eventbrite, Dice, venue-specific senders, sports teams                                                     | `event_name`, `venue`, `venue_address`, `event_at`, `section`, `seat`, `price`, `quantity`, `barcode_url`, `confirmation_code`, `source_email`                                                             | Ticket: "Radiohead at Madison Square Garden — Oct 15, 2024 8:00pm — Section 112, Row F, Seat 8 — $125.00"                                        | ~20–50                                  | Venue, seat, event name. Distinct from calendar_event (which is time blocks, not ticketed admission). Enables "what concerts have I been to."                                                                                                                                                                                                              |
| `payroll`       | Gusto, ADP, Paychex, Justworks, direct deposit notifications                                                             | `employer`, `pay_date`, `pay_period_start`, `pay_period_end`, `gross_amount`, `net_amount`, `deductions_json` (list of {name, amount}), `currency`, `source_email`                                         | Pay stub summary: "Endaoment — Pay period Jan 1–15, 2026 — Gross: $X,XXX — Net: $X,XXX — Federal: $XXX, State: $XXX, 401k: $XXX"                 | ~100–200                                | Gross/net/deductions is structurally different from FinanceCard (expenses, not income). Enables "what was my income in Q3" and compensation history.                                                                                                                                                                                                       |

### New entity types (2) — auto-generated from extraction and cross-referencing

| Type           | Key Fields                                                                                                                                                                                     | Body Content                                                                                                                        | Estimated Volume | How It's Populated                                                                                                                                                                                                                                              |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `place`        | `name`, `address`, `city`, `state`, `country`, `latitude`, `longitude`, `place_type` (restaurant/hospital/office/airport/residence/venue/store), `first_seen`, `last_seen`                     | Description of the place with all known facts: address, type, frequency of visits, linked card types.                               | ~200–500 unique  | Created by entity resolution when extractors encounter locations. "Brooklyn Hero Shop" across 20 meal_orders → 1 PlaceCard. "Cedars-Sinai" across hundreds of medical records → 1 PlaceCard. Uber pickup/dropoff addresses → PlaceCards for frequent locations. |
| `organization` | `name`, `org_type` (service/employer/medical/financial/government/retail/media), `domain`, `relationship` (customer/employee/patient/member/subscriber), `first_seen`, `last_seen`, `websites` | Description of the org and its relationship: what service it provides, how long you've been a customer/employee, linked card types. | ~100–300 unique  | Created by entity resolution from sender domains and card metadata. "DoorDash" from 150 emails → 1 OrgCard with `relationship: customer`. "Endaoment" from work emails → 1 OrgCard with `relationship: employer`.                                               |

### New system types (2) — agent infrastructure

| Type          | Key Fields                                                                                                                                                                     | Purpose                                                                                                                                                                                                                           |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knowledge`   | `domain` (relationships/food/health/travel/work/finance), `standing_query`, `depends_on_types` (list), `refresh_interval_days`, `freshness_date`, `input_watermark`            | Cached answers to recurring questions. "Who are my top 20 contacts?" doesn't need a hybrid search every time — the knowledge card has the pre-computed answer, refreshed when its inputs change.                                  |
| `observation` | `domain`, `observation_type` (pattern/anomaly/inference/milestone), `confidence` (0.0–1.0), `evidence_uids` (list of card UIDs backing the claim), `valid_from`, `valid_until` | Agent-generated inferences. "Moved from SF to NYC around September 2022" — backed by: restaurant cities shifted, Uber pickups shifted, calendar locations shifted. Stored as a card so the agent doesn't re-derive it every time. |

**Total: 37 card types** (22 existing + 11 derived + 2 entity + 2 system).

### Types that DON'T get their own card (and why)

**Decision rule:** A sender category gets its own derived type if and only if (a) it has structured data beyond what `email_message` already captures, (b) the structured data enables queries that email_message search cannot answer, and (c) the email represents a proven action or transaction — not a promotion, ad, or passive notification.

| Category                                                         | Decision                                                           | Reasoning                                                                                         |
| ---------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| Bank alerts (Chase, Wells Fargo, Amex)                           | → `finance` with `transaction_type: alert`                         | Same schema, just a subtype. Amount/currency/counterparty fields already work.                    |
| Crypto transactions (Coinbase, Etherscan)                        | → `finance` with `source: coinbase`                                | Amount/currency/counterparty fields already work.                                                 |
| Restaurant reservations (OpenTable, Resy)                        | → `calendar_event`                                                 | A reservation IS a calendar event — you're blocking time at a location.                           |
| DocuSign/contract signatures                                     | → `document` with `document_type: contract`                        | Signing metadata (parties, dates) fits existing DocumentCard fields.                              |
| Insurance documents                                              | → `document` or `medical_record`                                   | Not enough distinct structure for its own type.                                                   |
| Social media notifications (Twitter, Reddit, Facebook, LinkedIn) | Stay as `email_message`                                            | No structured transactional data. Passive notifications, not actions.                             |
| GitHub notifications                                             | Stay as `email_message` (already backed by `git_*` types from API) | The `git_*` card types capture the real data from the API. Email notifications are just pointers. |
| Google Workspace alerts (Drive, Docs, Cloud)                     | Stay as `email_message`                                            | System notifications, not transactions.                                                           |
| Marketing / newsletters / promotional emails                     | Stay as `email_message`                                            | Not transactions. The archive is about what you did, not what was advertised to you.              |
| Real estate listing emails (Redfin, Zillow, StreetEasy, Compass) | Stay as `email_message`                                            | Browsing activity, not transactions. If a purchase/lease closes, that's a `document`.             |
| News digests (NYT, WaPo, The Information, Substack)              | Stay as `email_message`                                            | Reading material, not transactions. Subscription _billing_ is captured via `subscription`.        |
| Loyalty program updates (Amex Platinum, airline mileage)         | Stay as `email_message`                                            | Status notifications, not transactions.                                                           |

---

## Cursor execution plans (PPA v2)

Detailed, step-by-step **execution plans** for each phase live under **`~/.cursor/plans/`** (same filenames on any machine with Cursor). Open in the editor or link from here:

| Phase                    | Execution plan                                                                                                           |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| 0 — Test infrastructure  | [`phase_0_execution_plan_61b73684.plan.md`](file:///Users/rheeger/.cursor/plans/phase_0_execution_plan_61b73684.plan.md) |
| 1 — Schema & data model  | [`phase_1_execution_plan_f2f5802d.plan.md`](file:///Users/rheeger/.cursor/plans/phase_1_execution_plan_f2f5802d.plan.md) |
| 2 — Extractors           | [`phase_2_execution_plan_50a42c00.plan.md`](file:///Users/rheeger/.cursor/plans/phase_2_execution_plan_50a42c00.plan.md) |
| 3 — Full extraction      | [`phase_3_execution_plan_49b4bd6d.plan.md`](file:///Users/rheeger/.cursor/plans/phase_3_execution_plan_49b4bd6d.plan.md) |
| 4 — ONE full rebuild     | [`phase_4_execution_plan_3156f3e2.plan.md`](file:///Users/rheeger/.cursor/plans/phase_4_execution_plan_3156f3e2.plan.md) |
| 5 — Embedding pass       | [`phase_5_embedding_pass_17a0e872.plan.md`](file:///Users/rheeger/.cursor/plans/phase_5_embedding_pass_17a0e872.plan.md) |
| 6 — LLM enrichment       | [`phase_6_llm_enrichment_f286b0bd.plan.md`](file:///Users/rheeger/.cursor/plans/phase_6_llm_enrichment_f286b0bd.plan.md) |
| 7 — Knowledge cache      | [`phase_7_execution_plan_b4b2c2ef.plan.md`](file:///Users/rheeger/.cursor/plans/phase_7_execution_plan_b4b2c2ef.plan.md) |
| 8 — Maintenance & tools  | [`phase_8_execution_plan_a16ec5dc.plan.md`](file:///Users/rheeger/.cursor/plans/phase_8_execution_plan_a16ec5dc.plan.md) |
| 9 — Production on Arnold | [`phase_9_execution_plan_794d5d32.plan.md`](file:///Users/rheeger/.cursor/plans/phase_9_execution_plan_794d5d32.plan.md) |

Each phase section below includes a direct **Execution plan** link in its heading block.

---

## Phase 0: Test Infrastructure Foundation

**Execution plan:** [`phase_0_execution_plan_61b73684.plan.md`](file:///Users/rheeger/.cursor/plans/phase_0_execution_plan_61b73684.plan.md)

**What it is:** A two-tier test infrastructure — minimal synthetic fixtures for unit tests, and a **stratified real slice** of the seed vault for integration and behavioral tests — that is **relationally complete** (every wikilink resolves, every person reference has a PersonCard, every edge fires), covers every existing card type, and includes known-good query/answer pairs that validate behavior, not just structure.

**Vault scan cache (Phase 0 enhancement):** PPA stores a SQLite vault scan cache under `<vault>/_meta/vault-scan-cache.sqlite3` so repeated `slice-seed`, `rebuild-indexes`, benchmark cleaning metrics, and seed-link passes avoid re-reading millions of notes. See **`docs/SLICE_TESTING.md`** (Vault scan cache section) and global CLI flag **`--no-cache`**.

**Why it exists:** The current `build_benchmark_sample` slices the real vault and produces a graph with orphaned wikilinks, missing person cards, and broken thread/message relationships. `_orphan_metrics` measures the damage but doesn't fix it. You can't verify rebuild correctness, test incremental caching, or benchmark search precision against broken data. More fundamentally, a test harness that only checks structure ("zero orphans") without checking behavior ("this query returns these cards in this order") gives false confidence.

### Two-tier testing model

**Tier 1 — Synthetic fixtures (unit tests): `tests/fixtures/`**

Minimal, hand-crafted card fixtures for testing code paths that don't need realistic data:

- **Schema validation:** One fixture per card type verifying Pydantic serialization/deserialization, required fields, type literals.
- **Edge rule wiring:** Small fixture graphs (5-10 cards) testing that `derived_from`, `located_at`, `provided_by`, and other `DeclEdgeRule` entries produce the expected edges.
- **Edge cases:** Malformed dates (missing timezone, date-only vs. datetime), missing required fields, unusually long bodies, Unicode/encoding edge cases, duplicate UIDs, cards with no people/orgs.
- **Mutation scenarios:** The `verify-incremental` test generates modified copies of fixtures (changed summaries, added people, modified bodies, new cards, deleted cards) to verify that incremental rebuild produces identical results to a fresh full rebuild. This is synthetic by definition — it's testing the rebuild code path, not data quality.

Fixtures are version-controlled, deterministic, and extend naturally when new card types are added in Phase 1 — each new type gets its own fixture file following the established pattern.

**Tier 2 — Stratified seed slice (integration/behavioral tests): `archive_mcp/test_slice.py`**

A real-data test corpus sliced from the production seed vault, treated like a **fork of a blockchain at a specific block height**. This is the primary test corpus — it validates behavior against real data and provides the basis for performance benchmarking.

**Slice strategy — stratified transitive closure:**

1. **Type-stratified seeding.** For each of the 22 existing card types, select seed cards targeting ≥5% of that type's total count in the seed (minimum 5 cards per type, even for rare types like `vaccination` or `meeting_transcript`). Seeds are spread across the full date range.
2. **Transitive closure.** For each seed card, recursively follow all references: threads pull their messages, messages pull their threads and PersonCards, etc. Continue until the graph is closed — every reference resolves.
3. **Cluster cap.** If a single seed's transitive closure exceeds 200 cards (highly-connected hub), drop that seed and pick an alternative of the same type. This prevents a single email thread with 50 participants from pulling in the entire corpus.
4. **The result** is a structurally complete slice — roughly 5-8% of total cards, with guaranteed coverage of every card type, and **zero orphans**.

**Snapshot fork model:**

The slice is generated once against a specific state of the seed vault and version-controlled as a fixed snapshot. The slice configuration is stored in `tests/slice_config.json`:

```json
{
  "vault_commit": "abc123def",
  "snapshot_date": "2026-03-28",
  "seed_uids_by_type": {
    "email_message": ["hfa-email-message-...", ...],
    "person": ["hfa-person-...", ...],
    ...
  },
  "cluster_cap": 200,
  "min_cards_per_type": 5,
  "target_percent": 5
}
```

CI always runs against this fixed snapshot. The slice does not drift when new emails are synced to the seed vault. To update the snapshot (because the seed vault has grown significantly or test coverage needs to change), explicitly "re-fork" — update `slice_config.json`, regenerate the slice, verify all assertions still pass, and commit the new config. This is a deliberate action, not an automatic one.

**Snapshot distribution for CI:** A 5% slice of ~1.85M files is ~92K files / ~460MB — too large to commit to the repo. When a snapshot is generated (or re-forked), the `ppa slice-seed` command also produces a compressed Docker image (`ppa-test-slice:<snapshot_date>`) containing the slice vault files. This image is pushed to the container registry alongside the `slice_config.json` commit. CI pulls this image as a service container, mounts the vault, and runs tests against it. The image is immutable and tagged to the snapshot date — no drift, no rebuild, no vault access required on CI runners. Re-forking the snapshot rebuilds and pushes a new image.

The stratified slicer replaces `build_benchmark_sample` for correctness and behavioral testing. The old stratified sampler can remain for pure throughput benchmarks where orphans don't matter.

### Known query/answer pairs — the behavioral contract

The slice includes a **test manifest** (`tests/slice_manifest.json`) that defines expected outcomes. This is the specification of "correct behavior" — not just "did it build without errors" but "does it return the right results for known queries."

**Full-text search (FTS) queries:**

| Query                                                          | Expected Result Type       | Expected Minimum Hits | Validation                                                 |
| -------------------------------------------------------------- | -------------------------- | --------------------- | ---------------------------------------------------------- |
| A specific person's full name (from a PersonCard in the slice) | `person`, `email_message`  | ≥3                    | The PersonCard ranks in top 3; related emails appear       |
| A specific email subject line (verbatim from the slice)        | `email_message`            | ≥1                    | Exact match card ranks #1                                  |
| A medical provider name (from a medical_record in the slice)   | `medical_record`, `person` | ≥1                    | The medical record and associated doctor PersonCard appear |
| A finance counterparty (from a finance card in the slice)      | `finance`                  | ≥1                    | The transaction card appears                               |
| A calendar event title (from the slice)                        | `calendar_event`           | ≥1                    | The event card ranks #1                                    |

**Temporal neighborhood queries:**

| Timestamp                                                                                      | Expected Cards                                                                         | Validation                                                                  |
| ---------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| The `activity_at` of a known email_message in the slice                                        | Neighboring messages in the same thread, plus temporally adjacent cards of other types | Cards within ±24h of the timestamp appear; same-thread messages are present |
| A date known to have multiple card types (e.g., a day with both an email and a calendar event) | Cards of multiple types                                                                | At least 2 distinct card types in results                                   |
| The earliest `activity_at` in the slice                                                        | The first N cards chronologically                                                      | `(activity_at, uid)` ordering is correct for the earliest cards             |

**Graph traversal queries:**

| Starting Card           | Traversal                                                  | Expected Result                                                                   |
| ----------------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------------- |
| An `email_thread` card  | Follow thread → messages                                   | All `email_message` cards in the thread appear                                    |
| An `email_message` card | Follow message → people                                    | All PersonCards referenced in `people:` frontmatter appear                        |
| A PersonCard            | Reverse traversal — find all cards referencing this person | Email messages, threads, and any other cards with this person in `people:` appear |

**Structural invariants (verified on every run):**

- Zero orphaned wikilinks — every `[[...]]` reference resolves to a card in the slice
- Zero orphaned person references — every name in a card's `people:` field has a corresponding PersonCard
- Edge count by rule > 0 for every active `DeclEdgeRule`
- Card count by type matches the manifest exactly
- Every card has a non-empty `summary`
- Every card has a parseable `activity_at` timestamp

**Post-Phase 1 expansion:** When the 15 new card types are added, the manifest expands to include queries like "restaurant name → meal_order," "airline confirmation code → flight," "tracking number → shipment," and graph traversals through `derived_from` / `located_at` / `provided_by` edges. The manifest format is designed to be extended — each new type adds entries to the existing query tables.

### Postgres environment parity

The test infrastructure runs against a Postgres instance that mirrors production exactly:

- **Image:** `pgvector/pgvector:pg17` (matching the existing `docker-compose.pgvector.yml`)
- **pgvector extension version:** Pinned in `docker-compose.test.yml` — must match the version on Arnold's VM
- **Configuration parity with Arnold:** The test Docker container applies the same Postgres GUCs (Postgres configuration parameters) as Arnold's production VM. These are codified in a `postgres-test.conf` mounted into the container:

```
shared_buffers = '256MB'
work_mem = '64MB'
maintenance_work_mem = '256MB'
effective_cache_size = '512MB'
```

The actual values are derived from Arnold's current VM allocation (inspect via `SHOW shared_buffers;` etc. on the Arnold Postgres instance and codify here). The goal: query planner behavior in tests matches query planner behavior in production. If an index scan vs. sequential scan decision differs between test and production Postgres configs, we want to discover that in testing, not in deployment.

- **CI runner:** The same `docker-compose.test.yml` runs in CI (GitHub Actions service container). No divergence between local dev, CI, and production Postgres behavior.

### Verification output — structured reports, not just pass/fail

When assertions fail, the developer needs actionable context, not just "FAILED." Every verification run produces a structured report (`test-report.json` + human-readable `test-report.md`):

**Structural health report:**

- Card counts by type: expected vs. actual, with diff
- Orphan report: which UIDs are orphaned, what references them, which cards contain the broken wikilinks
- Edge counts by rule: expected vs. actual
- Cards missing required fields (no `summary`, no `activity_at`, no `people:`)

**Behavioral test report:**

- Each query/answer pair: query text, expected results, actual results, pass/fail, diff highlighting (missing expected cards, unexpected cards in results, rank order differences)
- FTS precision/recall per query
- Temporal neighborhood: expected cards vs. actual cards, with `(activity_at, uid)` ordering shown

**Performance report:**

- Wall-clock time by phase (scan, classify, materialize, load, commit, finalize)
- Peak RSS
- Rows/second throughput
- Comparison to previous run (if available) with regression flags

This report is the primary output of the test infrastructure. It's what a developer looks at after a test run and says "yes, this is right" or "this broke, and here's exactly where."

### Performance benchmarking — multi-size with superlinear awareness

Performance benchmarking runs the slice at multiple sizes (1% and 5% of seed) to produce data points for extrapolation. The key insight: not all operations scale linearly. The benchmark harness explicitly categorizes operations by scaling behavior so extrapolation is honest.

**Scaling categories:**

| Scaling                                                           | Operations                                                                                                                                                                         | Extrapolation                                 |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| **Linear** — safe to multiply                                     | Card scanning, frontmatter parsing, projection writes, checkpoint resume, ingestion log writes                                                                                     | 5% time × 20 ≈ full time                      |
| **O(N log N)** — slightly superlinear                             | GIN index rebuild (FTS), B-tree index creation                                                                                                                                     | 5% time × 22-25 ≈ full time                   |
| **Superlinear / quadratic risk** — extrapolation is a lower bound | Entity resolution (clustering across N entities), embedding nearest-neighbor dedup, seed-link analysis (pairwise similarity), edge materialization for highly-connected card types | Must benchmark at 2+ sizes and plot the curve |
| **External bottleneck** — wall-clock dominated by API rate limits | Embedding API calls (rate-limited), geocoding (1 req/sec Nominatim), LLM enrichment (token budget)                                                                                 | Extrapolate from rate limit, not CPU time     |

**Benchmark protocol:**

1. Generate the 1% slice → full rebuild → record times by phase
2. Generate the 5% slice → full rebuild → record times by phase
3. For each operation, compare the ratio: if 5× more data produces 5× more time, it's linear. If it produces 7× or 10× more time, it's superlinear and the extrapolation needs adjustment.
4. **Worker count sweep:** Run the 5% slice rebuild at 1, 2, 4, and 8 workers. This serves two purposes: (a) correctness — verify that all worker counts produce identical output (same row-level diff as the single-worker baseline), and (b) performance — identify the optimal `DEFAULT_REBUILD_WORKERS` setting and detect concurrency bottlenecks (lock contention, connection pool exhaustion, diminishing returns).
5. Store results as JSON per run for regression tracking. Flag any operation whose time increased more than linearly between the 1% and 5% runs — these are the operations that will balloon at full scale.

**Baseline targets (derived from production history):**

- The initial seed rebuild took approximately 2 hours for the full corpus. At 5%, this should be ~6-8 minutes. If it's 20+ minutes at 5%, something is wrong.
- Embedding pass took approximately 2 hours for the full corpus. At 5%, this should be proportional to API rate limits, not CPU.
- Noop rebuild (no changes detected) should complete in < 10 seconds regardless of corpus size.

### `ppa health-check` — reusable across test and production

A single command that runs structural and behavioral assertions against **any** index — test slice, production, or remote:

```bash
ppa health-check                          # against local index
ppa health-check --dsn $ARNOLD_DSN        # against Arnold's index
```

**Checks:**

- Zero orphaned wikilinks
- Card count by type > 0 for all expected types
- Edge count by rule > 0 for all active `DeclEdgeRule` entries
- Quality score distribution by type (median, p10, p90) — flags types where median quality is below a configured threshold
- Sample FTS queries return non-empty results
- Composite B-tree index on `(activity_at, uid)` is present and temporal neighbor queries use it
- No duplicate UIDs

**Output:** Same structured report format as the test verification output. When run against the test slice, the expected values come from the manifest. When run against production, the expected values are proportional ranges (e.g., "card count for `email_message` should be between 400K and 500K" rather than an exact number).

This is how you detect production divergence after Phase 4's real rebuild: run `ppa health-check` against production and compare its report to the test slice report. Same structure, same checks, different scale.

### Extensibility contract for Phase 1

The test infrastructure defines a clear contract that Phase 1 must satisfy when adding 15 new card types. For each new type, Phase 1 must provide:

1. **A synthetic fixture** in `tests/fixtures/` — at least one valid card of the new type, exercising all type-specific fields.
2. **Schema validation test** — Pydantic round-trip (serialize → deserialize → assert identical).
3. **Edge rule test fixtures** — small graph demonstrating that the type's `DeclEdgeRule` entries produce expected edges.
4. **Quality score test** — a "rich" and a "sparse" fixture for the type, asserting that the quality formula produces meaningfully different scores.
5. **Expansion of `slice_manifest.json`** — new query/answer pairs for FTS (type-specific field values as search terms), new graph traversal entries (following new edge rules), new temporal neighborhood expectations.
6. **Updated slice config** — once extractors have run (Phase 3), re-fork the slice to include the new derived card types, ensuring the slice covers all 37 types.

This contract is documented in Phase 0's output so that Phase 1 developers know the standard before they start.

### CLI integration

- `ppa slice-seed --config tests/slice_config.json --output /tmp/test-slice`
- `ppa health-check [--dsn DSN] [--report-format json|md]`
- `ppa benchmark --slice-percent 1 --slice-percent 5 --output /tmp/bench-results/`

### Makefile targets

- `test-unit` — run synthetic fixture tests (fast, no Postgres, every push)
- `test-slice` — generate slice from seed + rebuild + run full behavioral assertion suite against Postgres
- `test-slice-verify` — rebuild slice + verify all query/answer pairs + produce structured report
- `benchmark-1pct` / `benchmark-5pct` — generate 1% / 5% slice, rebuild, report wall-clock time + throughput + scaling analysis
- `verify-incremental` — using synthetic fixtures: full rebuild, mutate 5%, incremental rebuild, assert identical to fresh full rebuild
- `health-check` — run `ppa health-check` against local index

### Operational logging (required for Phase 0 work)

All stratified slice generation, rebuilds, benchmarks, and CI jobs that run longer than a few minutes **must** follow `.cursor/rules/ppa-long-running-jobs.mdc`:

- **`slice-seed`:** `ppa --log-file logs/….log slice-seed …` (global `--log-file` **before** `slice-seed`). Logs include immediate `start`, walk `total_notes`, read-pass progress with **`eta_remaining`** (`M:SS`), copy progress with `%` and ETA. Smoke / fast feedback: `tests/slice_config.smoke.json`, `make test-slice-smoke` (writes `logs/ppa-slice-smoke.log`), `make test-slice-verify-smoke` for rebuild + health-check on that output.
- **`rebuild-indexes` / `benchmark` / `embed-pending` / `migrate`:** Use `--log-file` the same way; set `--progress-every` for noisy progress; never rely on stdout for status (stdout is JSON or MCP).
- **Agents / humans:** Prefer smoke slice + file log before full 5% slice; retain logs for postmortems and CI artifact upload when applicable.

Structured reports (`test-report.json` / `test-report.md`) remain the behavioral contract; stderr + file logs are the **live** visibility layer.

### CI integration

- **Every push:** `test-unit` (synthetic fixtures, no Postgres, seconds)
- **Every push:** `test-slice-verify` against the version-controlled slice snapshot (requires Postgres service container, minutes)
- **Nightly:** `benchmark-5pct` with regression detection (flag if any operation's time increased >15% vs. last run)
- **Weekly:** `benchmark-1pct` + `benchmark-5pct` together for scaling curve analysis

CI uses a **pgvector Docker service container** configured via `docker-compose.test.yml` with production-parity Postgres settings. Fully isolated, no external dependencies.

### Rebuild cache verification & repair

The rebuild system has three known gaps that must be fixed before the Phase 4 full rebuild. Fixing them here — alongside the test infrastructure — means the one expensive rebuild is crash-safe, operationally predictable, and verifiable.

**Known gap 1: `PPA_REBUILD_RESUME` is dead code.** Defined in `index_config.py` (line 216-217), documented in `PPA_RUNTIME_CONTRACT.md`, but `get_rebuild_resume()` is never called from `loader.py`. The `rebuild_checkpoint` table writes progress during full rebuilds (the `_save_rebuild_checkpoint` call at loader.py ~684-698) but nothing reads it back. If a multi-hour rebuild crashes at 80%, you start over from zero.

**Known gap 2: `content_hash` is computed but ignored by incremental classification.** The classifier (`_classify_manifest_rebuild_delta` in scanner.py ~178-237) uses mtime/size + frontmatter_hash + slug + people_json/orgs_json. A body-only change that preserves mtime and size (common with `rsync --size-only`, `git checkout`, or vault copies across filesystems) would be missed. `content_hash` is the authoritative signal — it SHA-256s the full frontmatter + body — but it's unused in classification.

**Known gap 3: No integration test proves incremental == full.** `test_rebuild_manifest.py` tests the classification logic in isolation (noop, full escalation, duplicate UID, person change, fingerprint stability). But no test runs both paths against Postgres and diffs the output.

**What to build:**

**a) Checkpoint resume.** Wire `get_rebuild_resume()` into `rebuild_with_metrics` in `loader.py`. On startup, if `rebuild_checkpoint.status == 'in_progress'` and `run_id` matches, skip cards whose `rel_path` is lexically `<=` `last_committed_rel_path`. This makes the Phase 4 full rebuild crash-safe.

**`run_id` composition:** The `run_id` must incorporate schema version constants — not just the vault manifest hash. Use `sha256(vault_manifest_hash + INDEX_SCHEMA_VERSION + CHUNK_SCHEMA_VERSION + PROJECTION_REGISTRY_VERSION)`. This ensures that deploying new code with a schema version bump automatically invalidates stale checkpoints. Without this, a crash-resume after a code update would produce a half-old-schema, half-new-schema index.

**b) `content_hash` as opt-in verification.** At ~1.85M vault files, computing SHA-256 on every file during every incremental rebuild adds 30-60 seconds of I/O on SSD (potentially minutes on the encrypted share) — incompatible with the "noop rebuild < 10 seconds" target. The `content_hash` fallback is gated behind `PPA_REBUILD_VERIFY_HASH=1`:

- **Default (off):** Incremental classification uses mtime/size + frontmatter_hash (fast path). This is correct for all normal editing workflows where file modifications update mtime.
- **Opt-in (on):** After the mtime/size check passes, compute `content_hash` from disk and compare to stored value. If they differ, mark the card as changed. Use this after operations known to preserve mtime: `rsync --size-only`, `git checkout`, cross-filesystem vault copies.
- **Documentation:** The CLI help for `ppa rebuild-indexes` and the runtime contract document which operations warrant `PPA_REBUILD_VERIFY_HASH=1`.

**c) Soften person-card-forces-full escalation.** Currently (scanner.py ~231-232), any PersonCard in `materialize_uids` triggers a full rebuild because person references spread everywhere. Instead: query `card_people` for all UIDs referencing the changed person, add those to `materialize_uids`, and rebuild them incrementally. Only escalate to full if affected UIDs exceed **5,000 cards** (absolute threshold, not percentage). At ~1.84M total cards, 5,000 is <0.3% of the corpus — well within incremental's efficient range. This keeps incremental viable for the common case of fixing a person's name or adding an email alias.

**d) Incremental == full integration test.** Using synthetic fixtures for mutation:

1. Full rebuild → snapshot deterministic table columns (see exclusion list below)
2. Mutate 5% of cards (change summaries, add people, modify bodies)
3. Add 2% new cards, delete 1%
4. Run incremental rebuild → snapshot
5. Run fresh full rebuild against the same mutated corpus → snapshot
6. Assert incremental snapshot == full snapshot (row-level diff)
7. Run seed slice behavioral tests — verify all query/answer pairs from `slice_manifest.json` still pass after rebuild cache code changes

**Table comparison scope and column exclusions:** The diff compares `cards`, `card_people`, `card_orgs`, `card_sources`, `edges`, `chunks`, and all typed projection tables. Excluded columns (non-deterministic across runs):

- Any column of type `TIMESTAMPTZ` that records wall-clock time (`logged_at`, `created_at`, `updated_at`, `queued_at`, `completed_at`, etc.)
- `note_manifest.mtime` and `note_manifest.size` (filesystem-dependent)
- `rebuild_checkpoint.*` (run metadata, not data)
- `ingestion_log.id` (sequence-generated)
- `embeddings.embedding` (floating-point, not compared here — embedding correctness is Phase 5's concern)

This exclusion rule is forward-compatible: any new table follows the same principle — compare content columns, exclude wall-clock timestamps and sequence IDs.

**e) Checkpoint resume test.** Send SIGTERM to the rebuild process after it has committed at least 50% of cards (monitor via `rebuild_checkpoint.last_committed_rel_path` progress). Resume with the same `run_id`. Assert the final output matches a clean full rebuild. SIGKILL / power-failure scenarios are out of scope — the goal is graceful crash recovery, not filesystem corruption resilience.

**f) Migration infrastructure validation.** The Phase 1 DDL changes (e.g., `activity_at` TEXT → TIMESTAMPTZ, new columns, new tables) will be handled via **explicit migrations through `MigrationRunner`** (`migrate.py`). Phase 0 validates that the migration infrastructure works:

- Verify `MigrationRunner` correctly reads, applies, and tracks migrations in the `schema_migrations` table
- Write and apply a **sample no-op migration** against the seed slice to prove the end-to-end flow (create migration file → runner detects it → applies it → records it → is idempotent on re-run)
- The actual Phase 1 migration files are written during Phase 1, not here — Phase 0 proves the machinery, Phase 1 uses it

Each DDL change in Phase 1 gets its own numbered migration file in `archive_mcp/migrations/`. The Phase 4 rebuild applies pending migrations before materializing, so the schema is correct before any cards are processed.

### Hard reset / rollback path

At any point during v2 development, the system can be fully reset to pre-v2 state:

- **Vault (markdown files):** The vault is version-controlled. Phase 3's extractor-written derived cards can be reverted with `git checkout` on the vault directories. Phases 0, 1, 2 don't modify the vault at all.
- **Postgres index:** The index is fully derived from the vault + code. To reset: `ppa rebuild-indexes --force-full` with the pre-v2 code against the pre-v2 vault. The rebuild drops and recreates all tables from scratch.
- **Schema migrations:** `MigrationRunner` tracks applied migrations in `schema_migrations`. Rolling back code to pre-v2 and running `--force-full` drops all tables (including `schema_migrations`) and rebuilds from the old DDL.

The invariant: **the vault is the source of truth, and the Postgres index is always derivable from vault + code.** No v2 change breaks this invariant. A full rebuild with old code against the original vault produces the original index.

**Files touched:** New files: `archive_mcp/test_slice.py`, `archive_mcp/commands/health_check.py`, `tests/slice_config.json`, `tests/slice_manifest.json`, `docker-compose.test.yml`, `postgres-test.conf`. New fixture directory: `tests/fixtures/` (one fixture per card type). Modified: `archive_mcp/__main__.py` (new CLI commands), `archive_mcp/loader.py` (checkpoint resume), `archive_mcp/scanner.py` (content_hash opt-in, person escalation threshold), `archive_mcp/index_config.py` (run_id composition), `Makefile` (new targets), `tests/` (new test modules including `test_rebuild_incremental.py`, `test_rebuild_resume.py`).

**Why this is Phase 0:** Everything downstream depends on two things: (1) a test environment that validates behavior, not just structure, and (2) a rebuild system that is crash-safe and correct. Schema changes, new card types, extractor output, search precision — all need both. Build them together as a single foundation.

**Definition of Done:**

- `ppa slice-seed --config tests/slice_config.json` produces a vault with zero orphans, zero broken wikilinks, and ≥5 cards of every existing type
- `slice_config.json` and `slice_manifest.json` are version-controlled and the slice is reproducible from them
- Full rebuild against the slice succeeds; all query/answer pairs in the manifest pass
- Behavioral test report is human-readable and shows expected vs. actual for every query
- `verify-incremental` Makefile target passes (incremental == full) using synthetic fixtures, with deterministic column comparison
- Checkpoint resume test passes: SIGTERM at 50%, resume, identical output
- `content_hash` verification test passes: modify body without changing mtime, `PPA_REBUILD_VERIFY_HASH=1` rebuild detects the change
- Person card edit triggers incremental rebuild of referencing cards (not full escalation) when affected count < 5,000
- `ppa health-check` runs against the slice index and produces a clean report
- `benchmark-5pct` completes and produces a scaling report with per-phase timings
- CI pipeline runs `test-unit` and `test-slice-verify` on every push
- `docker-compose.test.yml` matches Arnold's Postgres configuration (version, GUCs)
- Migration infrastructure validated: sample migration applies, is idempotent, and is tracked in `schema_migrations`
- Extensibility contract for Phase 1 is documented in the test infrastructure README
- Hard reset path documented and verified: `--force-full` with pre-v2 code produces original index

---

## Phase 1: Schema & Data Model

**Execution plan:** [`phase_1_execution_plan_f2f5802d.plan.md`](file:///Users/rheeger/.cursor/plans/phase_1_execution_plan_f2f5802d.plan.md)

**What it is:** Every change that affects the shape of data in the vault and Postgres. All code, zero rebuilds.

**Why everything goes in one phase:** Each of these changes would individually require a rebuild. By batching them, the rebuild cost is paid once. Version constants (`MANIFEST_SCHEMA_VERSION`, `INDEX_SCHEMA_VERSION`, `CHUNK_SCHEMA_VERSION`, `PROJECTION_REGISTRY_VERSION`) get bumped so the first rebuild after these changes correctly detects that the entire corpus needs reprocessing.

**Logging:** Schema work is mostly short-lived; any **`ppa migrate`** run that could take more than a few minutes (large DB) should use **`ppa --log-file logs/migrate.log migrate`** (global flag before `migrate`) and structured `ppa.*` output per `.cursor/rules/ppa-long-running-jobs.mdc`.

### 1a) Temporal spine

The temporal spine is what makes "what was happening at this moment" a native query instead of a manual stitching exercise. Today, `activity_at` is a `TEXT` column with inconsistent resolution — some cards have `2025-12-27`, others have fields like `sent_at: "2025-12-27T20:14:00-08:00"` that get flattened to the date string during indexing. All the temporal precision in the source data is lost.

**Changes:**

- **`activity_at` becomes `TIMESTAMPTZ`.** Cards that only have date-level precision get midnight in the archive owner's configured timezone (`PPA_DEFAULT_TIMEZONE`, default `UTC`). Set `PPA_DEFAULT_TIMEZONE=America/Los_Angeles` in config so that a card dated `2025-12-27` sorts on the right calendar day relative to Pacific Time events. Cards with full datetime+timezone precision are parsed and normalized to UTC.

- **Migration from TEXT:** The Phase 1 migration for `activity_at` must handle conversion of ~1.84M existing rows. The migration: (1) add a new `activity_at_tz TIMESTAMPTZ` column, (2) populate it by parsing existing TEXT values using the `card_activity_at()` priority cascade with the configured default timezone for date-only values, (3) drop the old `activity_at` column and rename `activity_at_tz` to `activity_at`. Empty/null TEXT values → `NULL TIMESTAMPTZ`. The `_filter_clauses` function in `index_query.py` must be updated from `LEFT(activity_at, 10)` string comparison to proper `TIMESTAMPTZ` range queries (`activity_at >= $start AND activity_at < $end`).

- **Add `activity_end_at TIMESTAMPTZ`** for interval events. Without this, interval events are point events and you lose the "during" semantics — "were we on that flight at noon?" can't be answered.

  **Per-type `activity_end_at` field mapping** (extends the existing `card_activity_at()` pattern in `features.py`):

  | Card Type            | `activity_end_at` Source Field |
  | -------------------- | ------------------------------ |
  | `flight`             | `arrival_at`                   |
  | `accommodation`      | `check_out`                    |
  | `car_rental`         | `dropoff_at`                   |
  | `calendar_event`     | `end_at`                       |
  | `meeting_transcript` | `end_at`                       |
  | `ride`               | `dropoff_at`                   |
  | All other types      | `NULL` (point events)          |

  Implement as `card_activity_end_at(card_type, frontmatter)` in `features.py`, called from `materializer._materialize_row`.

- **Extend `card_activity_at()` in `features.py`** with new type-specific fields. The current priority cascade is: `last_message_at → sent_at → start_at → captured_at → committed_at → occurred_at → updated → created → first_message_at`. Add the following fields (inserted in priority order after `occurred_at`): `departure_at` (flights), `pickup_at` (rides), `check_in` (accommodation), `pay_date` (payroll), `shipped_at` (shipments). Also update `TIMELINE_FIELDS` to include the new timestamp columns.

- **Composite B-tree index on `(activity_at, uid)`** replaces the need for a separate `event_seq` column. This is the industry-standard approach (keyset pagination) for ordered timeline access at scale. At ~1.84M rows, neighbor lookups are ~3-4 cached B-tree page reads — microseconds, indistinguishable from O(1) in practice. Critically, the composite index is **zero-maintenance**: Postgres updates it automatically on every insert. No full-table UPDATE is ever needed, making card insertion (from refetches, new extractions, or new data sources) a lightweight operation regardless of corpus size.

  **Why not `event_seq`:** A dense integer sequence requires a full-table `UPDATE ... row_number() OVER (ORDER BY activity_at, uid)` on every rebuild — O(N) on ~1.84M rows. Every new card insertion would either require reassigning the entire sequence or complex gap-management logic. The composite index provides the same query performance with zero write overhead.

- **Add B-tree index on `activity_end_at`** for interval overlap queries.

- **New query in `index_query.py`:** `temporal_neighbors(timestamp, direction='both', limit=20, type_filter=None, source_filter=None, people_filter=None)` — returns cards near a timestamp using keyset pagination on `(activity_at, uid)`, **plus interval overlap** for events active at the queried time.

  The query has three legs:

  ```sql
  -- Forward: next N cards after timestamp T
  (SELECT * FROM cards WHERE activity_at >= $T ORDER BY activity_at, uid LIMIT $N)
  UNION ALL
  -- Backward: previous N cards before timestamp T
  (SELECT * FROM cards WHERE activity_at < $T ORDER BY activity_at DESC, uid DESC LIMIT $N)
  UNION ALL
  -- During: interval events active at timestamp T
  (SELECT * FROM cards
   WHERE activity_at <= $T
   AND activity_end_at >= $T
   AND activity_end_at IS NOT NULL)
  ```

  Results are deduplicated (a flight whose departure is near T AND T falls within its interval appears once, not twice) and merged into a single timeline ordered by `activity_at, uid`. All three legs are index-backed.

- Wire as MCP tool `archive_temporal_neighbors` and CLI command `temporal-neighbors`.

**Files touched:** `archive_mcp/schema_ddl.py` (ALTER/CREATE cards table — `activity_at TIMESTAMPTZ`, `activity_end_at TIMESTAMPTZ`, composite index, drop `event_seq`), `archive_mcp/features.py` (extend `card_activity_at()` cascade, add `card_activity_end_at()`, update `TIMELINE_FIELDS`), `archive_mcp/materializer.py` (activity_at/end_at population with timezone handling), `archive_mcp/index_query.py` (temporal_neighbors function, `_filter_clauses` migration to TIMESTAMPTZ range queries), `archive_mcp/server.py` (new MCP tool), `archive_mcp/__main__.py` (new CLI command). **Migration file:** `archive_mcp/migrations/` (activity_at TEXT → TIMESTAMPTZ data migration).

### 1b) All 15 new card type schemas

Every type listed in the inventory: `meal_order`, `grocery_order`, `ride`, `flight`, `accommodation`, `car_rental`, `purchase`, `shipment`, `subscription`, `event_ticket`, `payroll`, `place`, `organization`, `knowledge`, `observation`. Each as a Pydantic class extending `BaseCard` with `type: Literal["..."]` and type-specific fields as described in the inventory.

Register all 15 in `CARD_TYPES`, bringing the total from 22 to 37.

**Files touched:** `hfa/schema.py` (15 new Pydantic classes + CARD_TYPES registration).

### 1c) Vault directory conventions for new types

New types need directory assignments in the vault. Update `ppa-init-vault.sh` to create these:

- `Transactions/MealOrders/` — meal_order cards, organized by `YYYY-MM/`
- `Transactions/Groceries/` — grocery_order cards, organized by `YYYY-MM/`
- `Transactions/Rides/` — ride cards, organized by `YYYY-MM/`
- `Transactions/Flights/` — flight cards, organized by `YYYY-MM/`
- `Transactions/Accommodations/` — accommodation cards, organized by `YYYY-MM/`
- `Transactions/CarRentals/` — car_rental cards, organized by `YYYY-MM/`
- `Transactions/Purchases/` — purchase cards, organized by `YYYY-MM/`
- `Transactions/Shipments/` — shipment cards, organized by `YYYY-MM/`
- `Transactions/Subscriptions/` — subscription lifecycle event cards, organized by `YYYY-MM/`
- `Transactions/EventTickets/` — event_ticket cards, organized by `YYYY-MM/`
- `Transactions/Payroll/` — payroll cards, organized by `YYYY-MM/`
- `Entities/Places/` — place cards
- `Entities/Organizations/` — organization cards
- `Knowledge/` — knowledge cards
- `Agent/` — observation cards and agent working memory

**Files touched:** `scripts/ppa-init-vault.sh`, `hfa/vault.py` (if directory-skipping logic needs updates).

### 1d) Projection definitions and edge rules

Each new type gets a `CardTypeRegistration` in `card_registry.py` with:

- Typed projection table with appropriate indexed columns
- `person_edge_type` where applicable
- `DeclEdgeRule` entries:
  - `derived_from` — every derived card → source email_message via `source_email` field
  - `located_at` — meal_order → place, ride → pickup_place + dropoff_place, accommodation → place, event_ticket → venue place
  - `provided_by` — meal_order → org, ride → org, flight → org, purchase → org, subscription → org
  - `ships_for` — shipment → purchase
  - `observed_from` — observation → evidence cards (multi=True)
- `chunk_builder_name` and `chunk_types` for embedding

**`search_text` is automatic.** The existing `_build_search_text` in `materializer.py` dumps all frontmatter string values + body text into `cards.search_text`. New card types with fields like `restaurant`, `airline`, `vendor` in frontmatter are automatically searchable. The rich body text produced by Phase 2 extractors (formatted receipts, route summaries, itineraries) is also included. No per-type `search_text` customization is needed in `CardTypeRegistration` — the generic mechanism handles it.

**Relationship to existing seed-link system:** Declarative edges (from `DeclEdgeRule`) handle known structural relationships — a meal_order is always `derived_from` its source email and `located_at` a restaurant. Seed-links handle **discovered** relationships — semantic similarity, co-occurrence, contextual connections that can't be expressed as rules. Both populate the `edges` table. The Phase 6 enrichment pass uses seed-links to find connections that declarative rules can't express.

**Quality score formula:** Each `CardTypeRegistration` defines a `quality_critical_fields` list — the type-specific fields whose population matters most. A `meal_order` with no `items` is quality 0.3; with items it's 0.8+. A `ride` with no `pickup_location` is 0.4; with full route data it's 0.9+. The generic quality formula (summary, people, orgs, timestamp precision) is supplemented by per-type critical fields, each worth a configurable weight.

**Files touched:** `archive_mcp/card_registry.py` (15 new registrations), `archive_mcp/projections/registry.py` (new projection table definitions).

### 1e) Infrastructure tables and columns for future phases

These tables and columns are consumed by Phases 6-8 (enrichment, knowledge, agent) but are created in Phase 1 to batch all DDL changes into a single rebuild cost. They will be empty until their consuming phase activates them, except for `ingestion_log` which the loader populates starting at Phase 4.

**Ingestion ledger** — a stream the agent can tail to know what's new, eliminating full-scan maintenance cycles:

```sql
CREATE TABLE {schema}.ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    card_uid TEXT NOT NULL,
    action TEXT NOT NULL,           -- 'created', 'updated', 'deleted'
    source_adapter TEXT NOT NULL,   -- 'gmail-messages', 'extract-emails', 'entity-resolution', etc.
    batch_id TEXT NOT NULL DEFAULT '',
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_ingestion_log_logged_at ON {schema}.ingestion_log(logged_at);
CREATE INDEX idx_ingestion_log_card_uid ON {schema}.ingestion_log(card_uid);
```

**Card quality metadata** — new columns on the `cards` table:

- `quality_score DOUBLE PRECISION DEFAULT 0.0` — computed deterministically during materialization. Per-type formula using `quality_critical_fields` from the card type registration plus universal factors (summary length, people/orgs populated, timestamp precision, body length, edges).
- `quality_flags TEXT[] DEFAULT '{}'` — what's missing: `no_people`, `no_summary`, `vague_timestamp`, `sparse_body`, `no_orgs`, `no_edges`, `no_tags`, `missing_items` (for order types), `missing_route` (for ride/flight), etc.
- `enrichment_version INTEGER DEFAULT 0`
- `enrichment_status TEXT DEFAULT 'none'` — `none` / `queued` / `in_progress` / `complete` / `skipped`
- `last_enriched_at TIMESTAMPTZ`

**Enrichment queue** — workers claim tasks via `SELECT ... FOR UPDATE SKIP LOCKED` (single-instance, no cross-instance competition):

```sql
CREATE TABLE {schema}.enrichment_queue (
    id BIGSERIAL PRIMARY KEY,
    card_uid TEXT NOT NULL,
    task_type TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    status TEXT NOT NULL DEFAULT 'pending',
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT DEFAULT '',
    attempts INTEGER DEFAULT 0
);
CREATE INDEX idx_eq_status_priority ON {schema}.enrichment_queue(status, priority);
```

**Retrieval gaps** — logged when queries return insufficient results (consumed by Phase 8 agent layer):

```sql
CREATE TABLE {schema}.retrieval_gaps (
    id BIGSERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    gap_type TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    card_uid TEXT DEFAULT '',
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMPTZ
);
```

**Files touched:** `archive_mcp/schema_ddl.py` (all new table DDL + card quality columns), `archive_mcp/materializer.py` (quality scoring during materialization, ingestion_log emission), `archive_mcp/card_registry.py` (per-type `quality_critical_fields`).

### 1f) Knowledge card dependency tracking

- New function in `index_query.py`: `is_knowledge_stale(knowledge_uid)` — compares the card's `input_watermark` against the latest `ingestion_log.logged_at` for cards matching `depends_on_types`. Returns boolean.
- New MCP tool `archive_knowledge` — given a domain, returns the freshest non-stale knowledge card. Falls back to retrieval if none exists or all are stale.

**Files touched:** `archive_mcp/index_query.py`, `archive_mcp/server.py`, `archive_mcp/__main__.py`.

### 1g) Provenance for derived cards

Derived cards participate in the existing provenance system (`<!-- provenance ... -->`). All extracted fields are `deterministic` (parsed from email body). The `source_email` field provides the audit trail back to the raw email. If LLM enrichment later improves a derived card's summary, that field's provenance changes to `llm`.

**Files touched:** `hfa/provenance.py` (ensure derived card compatibility), extractor framework (Phase 2) writes provenance blocks.

### 1h) Expand test infrastructure for 37 types

Per the Phase 0 extensibility contract, each new card type must provide:

1. A synthetic fixture in `tests/fixtures/` — at least one valid card exercising all type-specific fields.
2. Schema validation test — Pydantic round-trip (serialize → deserialize → assert identical).
3. Edge rule test fixtures — small graph demonstrating that the type's `DeclEdgeRule` entries produce expected edges.
4. Quality score test — a "rich" and a "sparse" fixture asserting the quality formula produces meaningfully different scores.
5. Expansion of `slice_manifest.json` — new query/answer pairs for FTS (type-specific field values as search terms), new graph traversal entries (following `derived_from`/`located_at`/`provided_by` edges), temporal neighborhood expectations, quality score expectations per type, and knowledge card staleness checks.

After Phase 3 runs extractors and the seed vault contains real derived cards, re-fork the seed slice (`slice_config.json`) to include the new types, ensuring the slice covers all 37 types with ≥5 cards each.

**Files touched:** `tests/fixtures/` (15 new fixture files), `tests/slice_config.json`, `tests/slice_manifest.json`, test assertion modules.

### 1i) Version bumps

Bump `MANIFEST_SCHEMA_VERSION`, `INDEX_SCHEMA_VERSION`, `CHUNK_SCHEMA_VERSION`, `PROJECTION_REGISTRY_VERSION` so existing manifests correctly trigger full rebuild on first run after these changes.

**Files touched:** `archive_mcp/index_config.py` (version constants), `archive_mcp/schema_ddl.py` (meta table values).

**Definition of Done:**

_Phase 0 baseline (must remain green throughout Phase 1):_

- Full Phase 0 test suite passes: `test-unit`, `test-slice-verify`, `verify-incremental`, `health-check` — no regressions
- `ppa health-check` produces a clean report against the seed slice index after Phase 1 code changes

_Schema and types:_

- All 37 card types validate in Pydantic (unit tests with synthetic fixtures for each new type)
- All 15 new types have synthetic fixtures, edge rule tests, and quality score tests per the Phase 0 extensibility contract
- All new DDL tables and columns create successfully during rebuild against the seed slice
- `activity_at` TEXT → TIMESTAMPTZ migration applies cleanly against the seed slice, with no data loss (all existing rows have valid `TIMESTAMPTZ` values or NULL)
- `_filter_clauses` date range queries work correctly with `TIMESTAMPTZ` (tested against the seed slice with known date-range queries in `slice_manifest.json`)
- Version bumps trigger full rebuild when run against an old-schema index

_Temporal spine:_

- `temporal_neighbors` query returns expected results against the seed slice — specific query/answer pairs added to `slice_manifest.json` covering: forward neighbors, backward neighbors, and interval overlap ("during" events)
- `activity_end_at` is populated for interval event types (flight, accommodation, calendar_event, meeting_transcript, ride, car_rental) in seed slice fixtures
- `PPA_DEFAULT_TIMEZONE` config works: date-only cards resolve to midnight in configured timezone
- Composite B-tree index on `(activity_at, uid)` is present and used by temporal queries (verify via `EXPLAIN ANALYZE`)
- `archive_temporal_neighbors` MCP tool passes all temporal query/answer pairs in `slice_manifest.json` (forward, backward, and during legs)

_Quality and infrastructure:_

- Quality scores are type-aware (a meal_order without items scores lower than one with items)
- `archive_knowledge` MCP tool returns correct results for knowledge-domain query/answer pairs in `slice_manifest.json`
- All new types have vault directory conventions and `ppa-init-vault.sh` creates them

---

## Phase 2: Email Extractor Framework + Extractors

**Execution plan:** [`phase_2_execution_plan_50a42c00.plan.md`](file:///Users/rheeger/.cursor/plans/phase_2_execution_plan_50a42c00.plan.md)

**What it is:** The framework and extractors that transform ~461K raw email cards into structured derived cards. Built and validated incrementally — each extractor is developed, run against real data, and verified before moving to the next. Phase 2 and Phase 3 are not strictly sequential; they form a **per-extractor loop**: build extractor → run against vault → inspect output → fix → promote to vault → next extractor.

**Why it's separate from Phase 1:** Phase 1 defines the slots (schemas, projections, edge rules). Phase 2 builds the machines that fill those slots. Keeping them separate means you can validate schemas with synthetic fixtures and the seed slice before worrying about extractor correctness.

**Logging:** `extract-emails` and per-extractor runs must emit **matched/total, yield, errors, throughput, wall time** on stderr via `ppa.*` loggers. Full-vault or long runs: **`ppa --log-file logs/extract-….log extract-emails …`**.

### Critical constraint: email bodies are plaintext, not HTML

The Gmail adapter (`_extract_text_body` in `gmail_messages.py`) stores the `text/plain` MIME part when available. For HTML-only emails, it runs `_strip_html` — a regex-based tag stripper + `html.unescape` — and stores the result as plaintext. **Raw HTML is not preserved in the vault.**

This means:

- Extractors work with plaintext (or stripped-HTML-to-text) bodies, not DOM structures. No BeautifulSoup, no CSS selectors.
- Older emails (pre-2020) from most services had plaintext parts with structured line items — these are the most extractable.
- Newer HTML-only emails (DoorDash 2024+, modern Amazon) are stripped to text, which may lose structural information (table layouts become run-together text).
- **Summary-only fallback** is essential: when line items can't be parsed from the stripped text, emit the card with whatever is available (restaurant + total, vendor + order number). A meal_order with no `items` is still valuable — it answers "did I order from X on this date" even if it can't answer "what did I order."
- **Future improvement (not in v2 scope):** re-ingest emails with HTML preservation for a second extraction pass. This would require a Gmail adapter change to store HTML alongside plaintext.

### 2a) Framework: `archive_sync/extractors/`

The core abstraction:

```python
class EmailExtractor:
    sender_patterns: list[str]       # regex on from_email
    subject_patterns: list[str]      # regex on subject
    output_card_type: str            # e.g., "meal_order"
    template_versions: list[TemplateVersion]  # ordered newest-first

class TemplateVersion:
    date_range: tuple[str, str]      # approximate validity window
    parser: Callable[[dict, str], list[dict]]  # (frontmatter, body) -> list of card dicts
```

Supporting modules:

- `registry.py` — maps sender patterns to extractor classes. `match_extractor(from_email, subject) -> Optional[EmailExtractor]`. Should integrate with the existing `AUTOMATED_LOCAL_PREFIXES` / `AUTOMATED_DOMAINS` classification in `gmail_correspondents.py` to avoid duplicating sender classification logic.
- `runner.py` — scans email cards in vault, runs matched extractors, writes derived cards via `vault.write_card`. Records `source_email` field (wikilink to source email card) and `derived_from` edge. Writes provenance blocks.
- `entity_resolution.py` — post-pass that clusters place/org names from derived cards and creates/merges PlaceCard and OrganizationCard files.
- CLI: `ppa extract-emails [--sender doordash] [--dry-run] [--limit N] [--staging-dir DIR] [--workers N]`

**Idempotency:** Derived card UIDs are deterministic functions of their source. For a meal_order derived from email `hfa-email-message-abc123`, the UID is `hfa-meal-order-{sha256("hfa-email-message-abc123" + restaurant_name)[:12]}`. Running `ppa extract-emails` twice produces the same UIDs and overwrites cleanly. The runner checks for existing cards with matching UIDs and skips if `content_hash` matches (no change), or overwrites if the extractor logic has been updated.

**Body text rendering:** Each extractor produces a human-readable body for the derived card — the "receipt" view. For a meal_order, this is the itemized list with customizations, quantities, and prices, formatted as markdown. For a ride, it's the route with pickup/dropoff, distance, duration, and fare. For a flight, it's the full itinerary. This body becomes the card's markdown content below the frontmatter, feeds into `search_text`, and gets chunked for embeddings. The goal: a prompt like "where did I get that amazing delivery banh mi" should match against the body text of meal_order cards that contain "banh mi" in their item list.

**Template versioning:** Email templates change every 1-2 years. DoorDash emails from 2020-2021 have plaintext line items. From 2024+, they're HTML-only (stripped to text in the vault, losing item structure). The same extractor needs multiple parsers, tried in order from newest to oldest, with a summary-only fallback as the last resort. Template discovery happens by sampling emails from different years in the vault and identifying format transitions.

### Runner performance and parallelism

The runner must process ~461K email cards efficiently. This is not a casual operation — it's a production-scale batch job.

**Sender matching:** The runner pre-indexes email cards by `from_email` domain during the scan pass, then dispatches batches to matched extractors. This avoids O(emails × patterns) — instead, it's O(emails) for the index build + O(matched) for extraction. With ~30 sender patterns matching maybe 10-20% of emails, the extraction set is ~50K-90K emails.

**Parallelism:** The runner supports `--workers N` for parallel extraction:

- Workers claim batches of matched email UIDs (batch size configurable, default 500)
- Each worker reads email cards, runs the matched extractor, writes derived cards to the output directory
- No shared mutable state between workers — each email is processed independently
- Default worker count: `max(cpu_count, 4)`, matching the rebuild worker default

**Progress and metrics:** The runner reports in real-time:

- Matched emails / total emails scanned (coverage)
- Extracted cards / matched emails (yield rate per extractor)
- Errors / matched emails (error rate per extractor)
- Wall-clock time per extractor and overall
- Cards written per second (throughput)

**Benchmarking:** Run the full extraction pipeline against the 5% seed slice first. Measure wall-clock time, extrapolate to full vault. If projected full-vault time exceeds 30 minutes, investigate bottlenecks before running at scale. The extraction is file I/O bound (reading email cards, writing derived cards) — parallelism should provide near-linear speedup.

### 2b) Extractors — incremental build order

Extractors are built and validated incrementally, not as a monolithic batch. Each extractor follows the loop: **build → run against vault staging → inspect output → fix bugs → repeat → promote to vault**. This catches bugs early and prevents the same parsing mistake from propagating across 20 extractors.

**Build order by priority** (highest volume and value first):

| Tier  | Extractors                                                                             | Card Type                                     | Est. Volume                 | Rationale                                                                                                       |
| ----- | -------------------------------------------------------------------------------------- | --------------------------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------- |
| **1** | `doordash.py`, `uber_rides.py`, `amazon.py`                                            | meal_order, ride, purchase                    | ~1,000 + ~800 + ~1,500      | Highest volume, best-structured plaintext eras, immediate feedback                                              |
| **2** | `instacart.py`, `shipping.py`, `lyft.py`, `ubereats.py`                                | grocery_order, shipment, ride, meal_order     | ~300 + ~1,500 + ~500 + ~500 | Good volume, shipments link to purchases                                                                        |
| **3** | `united.py`, `airbnb.py`, `rental_cars.py`                                             | flight, accommodation, car_rental             | ~50 + ~30 + ~15             | Travel cluster — low volume but high value for trip reconstruction                                              |
| **4** | `postmates.py`, `caviar.py`, `grubhub.py`, `micromobility.py`                          | meal_order, ride                              | ~200 + ~100 + ~50           | Additional meal/ride coverage                                                                                   |
| **5** | `delta.py`, `jetblue.py`, `hawaiian.py`, `booking_aggregators.py`, `booking_hotels.py` | flight, accommodation                         | ~30 + ~10 + ~10 + ~20 + ~20 | Remaining travel coverage                                                                                       |
| **6** | `retail.py`, `subscription_lifecycle.py`, `tickets.py`, `payroll.py`                   | purchase, subscription, event_ticket, payroll | ~500 + ~100 + ~30 + ~150    | Lower priority — retail handles many merchants with high template variation; subscriptions are harder to detect |

Each tier is a natural checkpoint: after Tier 1, you have the highest-volume types working. After Tier 3, travel reconstruction is possible. After Tier 6, full coverage.

**Sender patterns and module scope** (per card type):

| Module                      | Card Type       | Sender Patterns                                          | Notes                                                                                                                                                                                                                                                                                                                                                 |
| --------------------------- | --------------- | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `doordash.py`               | `meal_order`    | `*@doordash.com`, `*@messages.doordash.com`              | 3 template eras                                                                                                                                                                                                                                                                                                                                       |
| `ubereats.py`               | `meal_order`    | `ubereats@uber.com`, `*@uber.com` (subject: "Uber Eats") | Disambiguate from ride receipts                                                                                                                                                                                                                                                                                                                       |
| `postmates.py`              | `meal_order`    | `*@postmates.com`, `*@app.postmates.com`                 |                                                                                                                                                                                                                                                                                                                                                       |
| `caviar.py`                 | `meal_order`    | `*@trycaviar.com`                                        |                                                                                                                                                                                                                                                                                                                                                       |
| `grubhub.py`                | `meal_order`    | `*@grubhub.com`, `*@a.grubhub.com`                       |                                                                                                                                                                                                                                                                                                                                                       |
| `instacart.py`              | `grocery_order` | `*@instacartemail.com`, `*@instacart.com`                |                                                                                                                                                                                                                                                                                                                                                       |
| `uber_rides.py`             | `ride`          | `*@uber.com` (subject: ride receipt, not Eats)           | Pickup/dropoff, fare, distance, duration                                                                                                                                                                                                                                                                                                              |
| `lyft.py`                   | `ride`          | `*@lyft.com` receipt patterns                            |                                                                                                                                                                                                                                                                                                                                                       |
| `micromobility.py`          | `ride`          | `*@limebike.com`, `*@ride.bird.co`, `*@scoot*.com`       | `ride_type: scooter` or `bike`                                                                                                                                                                                                                                                                                                                        |
| `united.py`                 | `flight`        | `*@united.com`                                           | Confirmation, route, dates                                                                                                                                                                                                                                                                                                                            |
| `delta.py`                  | `flight`        | `*@delta.com`, `*@o.delta.com`, `*@t.delta.com`          |                                                                                                                                                                                                                                                                                                                                                       |
| `jetblue.py`                | `flight`        | `*@email.jetblue.com`                                    |                                                                                                                                                                                                                                                                                                                                                       |
| `hawaiian.py`               | `flight`        | `*@hawaiianairlines.com`                                 |                                                                                                                                                                                                                                                                                                                                                       |
| `booking_aggregators.py`    | `flight`        | Expedia/Booking.com flight confirmations                 |                                                                                                                                                                                                                                                                                                                                                       |
| `airbnb.py`                 | `accommodation` | `*@airbnb.com`                                           |                                                                                                                                                                                                                                                                                                                                                       |
| `booking_hotels.py`         | `accommodation` | `*@booking.com`, `*@hotels.com`, `*@expedia*.com`        |                                                                                                                                                                                                                                                                                                                                                       |
| `rental_cars.py`            | `car_rental`    | `*@nationalcar.com`, `*@hertz.com`, `*@emeraldclub.com`  |                                                                                                                                                                                                                                                                                                                                                       |
| `amazon.py`                 | `purchase`      | `*@amazon.com` order confirmations                       | ~10 years of template variation                                                                                                                                                                                                                                                                                                                       |
| `retail.py`                 | `purchase`      | eBay, Etsy, Costco, Wayfair, RH, Chewy, Target, etc.     | Single module, many merchants — uses per-merchant pattern configs within one robust parser. Each merchant is a config entry (sender patterns, subject patterns, field extraction rules), not a separate module. New merchants are added by config, not by code. The module must handle massive template variation within a single merchant over time. |
| `shipping.py`               | `shipment`      | `*@ups.com`, Amazon shipping, FedEx, USPS                | Tracking number, carrier, dates                                                                                                                                                                                                                                                                                                                       |
| `subscription_lifecycle.py` | `subscription`  | Spotify, Netflix, Apple, NYT, SaaS tools                 | Detects lifecycle events (started/renewed/cancelled) from subject patterns. High template variation across services — evaluate yield after first pass.                                                                                                                                                                                                |
| `tickets.py`                | `event_ticket`  | Ticketmaster, Eventbrite, Dice, venue-specific           | Event name, venue, date, seat                                                                                                                                                                                                                                                                                                                         |
| `payroll.py`                | `payroll`       | `*@gusto.com`, `*@justworks.com`                         | Pay date, gross, net                                                                                                                                                                                                                                                                                                                                  |

### 2c) Entity resolution module

- **Place disambiguation:** Resolution uses `(normalized_name, city)` as the compound key, not just name alone. "Main Street Pizza" in Brooklyn and "Main Street Pizza" in SF are different PlaceCards. If a meal_order has no city, infer city from the delivery address. If no address is available, cluster by temporal proximity to other orders from the same name and use the most common city in that cluster. Limits of this approach are acknowledged — ambiguous names without address context may merge incorrectly. Manual override via tags or frontmatter edits is the escape hatch.
- **Geocoding (lat/lng) runs in Phase 4** (post-rebuild, pre-embedding). PlaceCards are created in Phase 2/3 with `name`, `address`, `city`, `state`, `country` populated from extraction. Geocoding to populate `latitude`/`longitude` runs immediately after the Phase 4 rebuild via Nominatim/OSM (free, ~3-8 minutes for ~200-500 PlaceCards). This means lat/lng is available in `search_text` before Phase 5's embedding pass.
- **Organization deduplication:** Domain-based: all `*@doordash.com` senders = 1 OrganizationCard named "DoorDash" with `relationship: customer`.
- **Person linkage:** Doctor names in medical records → existing PersonCards via `IdentityCache`.

### 2d) Tests

Test fixtures follow the Phase 0 convention: **real data from the seed vault**, not anonymized copies. Since the seed is already real production data and test fixtures are used in the same context as the seed slice, there's no additional PII exposure. Fixtures are stored alongside the seed slice snapshot.

- Each extractor gets unit tests with real email body fixtures from the seed — at least 2 fixtures per extractor covering different template eras.
- Test template versioning: same sender, different era, both parse correctly.
- Test idempotency: run extractor twice, assert identical output (same UIDs, same content_hash).
- Test summary-only fallback: provide an email where line-item parsing fails, assert the extractor still produces a valid card with available data.
- Test entity resolution: duplicate place names in same city merge; same name in different cities don't; ambiguous cases handled gracefully.
- Integration test: run extraction pipeline against the seed slice, verify derived cards have correct edges, correct card types, correct body text, correct provenance, and resolve to existing entities.

### Per-extractor development + execution loop

Each extractor follows this cycle (combining the old Phase 2 "build" and Phase 3 "run" into a single iterative loop):

1. **Build** the extractor module with template versions and parsers.
2. **Unit test** against seed fixtures (at least 2 eras).
3. **Run against staging:** `ppa extract-emails --sender <name> --staging-dir _staging/ --workers 4`
4. **Inspect staging output.** Spot-check 10+ cards. Verify item parsing, body text readability, timestamps, entity references, provenance.
5. **Check yield rate.** If < 10% of matched emails produce valid cards, investigate — is it a template issue, an HTML-stripping issue, or a sender pattern mismatch?
6. **Fix bugs, re-run.** Repeat until output quality is acceptable.
7. **Promote to vault:** Move files from `_staging/` to vault directories per Phase 1c conventions.
8. **Run entity resolution:** `ppa resolve-entities` for the newly promoted cards.
9. **Validate:** `ppa validate` reports zero errors.
10. **Move to next extractor.**

After all tiers are complete:

- Run `ppa extract-emails --dry-run` for a final coverage report across all extractors.
- Re-fork the seed slice (`slice_config.json`) to include new derived card types, ensuring the slice covers all 37 types.
- Run full Phase 0 test suite against the updated slice.

**Files touched:** New directory `archive_sync/extractors/` with `__init__.py`, `base.py`, `registry.py`, `runner.py`, `entity_resolution.py`, and one module per extractor. Modified: `archive_sync/handler.py` (new subcommands), `archive_mcp/__main__.py` (CLI commands).

**Definition of Done:**

_Phase 0 baseline (must remain green throughout):_

- Full Phase 0 test suite passes after all extractors are promoted to vault

_Extractor coverage:_

- All Tier 1-3 extractors (DoorDash, Uber rides, Amazon, Instacart, shipping, Lyft, UberEats, United, Airbnb, rental cars) are complete and promoted to vault — these represent the highest-volume and highest-value card types
- Tier 4-6 extractors are complete or explicitly deferred with documented reasoning (e.g., "subscription_lifecycle yield was 5% — defer to post-v2 iteration")
- Each completed extractor has ≥2 test fixtures from different template eras
- Idempotency test passes for all completed extractors

_Quality and performance:_

- Yield rate per extractor documented: matched emails → extracted cards, with explanation for low-yield extractors
- Summary-only fallback tested and working for extractors with HTML-stripping degradation
- Full extraction pipeline against the 5% seed slice completes in < 5 minutes with 4 workers
- `--dry-run` reports expected extraction counts per type matching volume estimates within 50%

_Entity resolution:_

- Entity resolution produces PlaceCards and OrgCards that pass manual spot-check
- "Brooklyn Hero Shop" across multiple fixture orders merges into 1 PlaceCard

_Vault integrity:_

- `ppa validate` reports zero errors after all promotions
- All derived cards have `source_email` wikilinks pointing to existing email cards
- Seed slice re-forked to include new derived card types, covering all 37 types with ≥5 cards each

---

## Phase 3: Full Extraction Run + Vault Promotion

**Execution plan:** [`phase_3_execution_plan_49b4bd6d.plan.md`](file:///Users/rheeger/.cursor/plans/phase_3_execution_plan_49b4bd6d.plan.md)

**What it is:** The final production-scale extraction run across the full vault, entity resolution, and validation. By the time this phase starts, all extractors have been individually developed, tested, and run against staging in the Phase 2 loop. Phase 3 is the batch run at full scale.

**Why it's still its own phase:** Individual extractors were run against staging during Phase 2, but the full vault run at scale — all extractors, all ~461K emails, with parallelism — is a distinct operation. Entity resolution across the full derived card set (not just per-extractor batches) may produce different clustering results. And the final vault state needs comprehensive validation before the Phase 4 rebuild.

**Logging:** Same as Phase 2; full-scale **`extract-emails`** and **`resolve-entities`** require **`--log-file`** artifacts for postmortems. Runner metrics (matched, extracted, yield, errors, wall-clock) must remain visible in logs.

**Process:**

1. **Full extraction:** `ppa extract-emails --staging-dir _staging/ --workers 8` — runs all extractors against all email cards in parallel. Monitor progress via runner metrics (matched/total, yield, errors, throughput).
2. **Inspect staging output at scale.** Spot-check per-type samples. Verify volume estimates:
   - meal_order: ~1,000-1,500 cards
   - purchase: ~500-2,000 cards
   - ride: ~500-2,000 cards
   - shipment: ~500-2,000 cards
   - Other types: per volume estimates in the Card Type Inventory
3. **Promote staging to vault:** Move files from `_staging/` to vault directories per Phase 1c conventions.
4. **Full entity resolution:** `ppa resolve-entities` across all derived cards — this may produce different clustering than per-extractor runs because cross-type entity references are now visible (e.g., a restaurant PlaceCard referenced by both meal_orders and ride dropoff locations).
5. **Final validation:** `ppa validate` against the full vault.

At the end of this phase, the vault has ~3K-7K new derived cards plus ~300-800 entity cards. None are in Postgres yet.

**Definition of Done:**

_Phase 0 baseline:_

- Full Phase 0 test suite passes (seed slice re-forked to include all 37 types)
- `ppa health-check` passes against the seed slice after re-fork

_Idempotency:_

- Full-scale extraction produces 0 new cards beyond what Phase 2's per-extractor loop already promoted — confirms no cross-extractor interactions or ordering dependencies

_Volume and quality:_

- `ppa extract-emails --dry-run` final report shows extraction counts within 50% of volume estimates per type
- Staging output spot-checked: at least 10 cards of each high-volume type manually reviewed
- Yield rates per extractor documented and acceptable (>10% for all Tier 1-3 extractors, or explicit deferral with reasoning)

_Vault integrity:_

- `ppa validate` reports zero errors after full promotion
- Entity resolution produces PlaceCards and OrgCards with no obvious false merges on spot-check
- All derived cards have `source_email` wikilinks pointing to existing email cards

_Performance:_

- Full extraction run completes within projected time (extrapolated from 5% benchmark × 20, adjusted for superlinear operations)
- Runner metrics logged: total matched, total extracted, yield per extractor, errors per extractor, wall-clock time

---

## Phase 4: ONE Full Rebuild

**Execution plan:** [`phase_4_execution_plan_3156f3e2.plan.md`](file:///Users/rheeger/.cursor/plans/phase_4_execution_plan_3156f3e2.plan.md)

**What it is:** The single expensive rebuild that processes the entire vault — all existing cards with the new schema, all new derived cards, all entity cards.

**Why now:** All schema changes are in (Phase 1). All new cards are written (Phase 3). The rebuild caching system is verified (Phase 0). This is the one time we pay the cost.

**Logging:** Run **`ppa --log-file logs/phase4-rebuild.log rebuild-indexes --force-full --workers N`** (and **`--progress-every`** as needed). Stderr shows rebuild steps `k/6`, materialize/load progress, checkpoint lines; correlate with **`rebuild_checkpoint`** in Postgres. See Phase 0 operational logging and `ppa-long-running-jobs.mdc`.

**Rollback:** Record the current git commit hash before starting. If the rebuild produces bad data, revert code to that commit and rebuild the old schema. The vault (markdown files) is unaffected by rebuilds — only the derived Postgres index changes.

**Pre-rebuild verification:**

- Run the full Phase 0 test suite against the new code: synthetic fixture unit tests (all 37 types pass schema/edge/quality tests) and seed slice behavioral tests (all query/answer pairs in `slice_manifest.json` pass, zero orphans, correct edge materialization, incremental == full).
- Run `ppa health-check` against the slice index — clean report, no regressions.
- If any fail, stop and fix. Don't burn hours on a rebuild against broken code.

**Performance expectations:**

- Before starting, review Phase 0 benchmark results (5% slice timing × 20, adjusted for superlinear operations from the scaling curve analysis). Log the predicted completion time.
- Use the optimal worker count determined by Phase 0's worker count sweep (likely 4 or 8): `ppa rebuild-indexes --force-full --workers N`
- **Hard cap: 5 hours.** If the rebuild hasn't completed within 5 hours, investigate — the checkpoint resume system ensures no progress is lost, but exceeding 5 hours signals a scaling problem that needs diagnosis before continuing.
- After completion, compare actual time vs. predicted time. If actual > 1.5× predicted, update Phase 0's scaling model — the extrapolation was inaccurate and needs recalibration.

**The rebuild:**

- `ppa rebuild-indexes --force-full --workers N` (N from Phase 0 benchmarks)
- All new DDL tables created (ingestion_log, enrichment_queue, retrieval_gaps, quality columns, new projections)
- All cards re-indexed with proper `TIMESTAMPTZ` resolution in `activity_at`
- Composite B-tree index on `(activity_at, uid)` created for temporal ordering
- New edge rules materialize (`derived_from`, `located_at`, `provided_by`, `ships_for`, `observed_from`)
- `search_text` includes new fields from derived card types (item names, routes, etc.)
- `quality_score` and `quality_flags` computed for every card
- `ingestion_log` populated with initial load entries
- `rebuild_checkpoint` tracks progress (and resume works if it crashes)

**Post-rebuild geocoding:**

Geocoding runs immediately after the rebuild — it's free (Nominatim/OSM), fast (~200-500 PlaceCards at 1 req/sec = 3-8 minutes), and zero-risk. Running it here means lat/lng is available in `search_text` before Phase 5's embedding pass, enabling location-aware semantic search from the start.

- `ppa enrich --task geocode` — populates `latitude`/`longitude` for PlaceCards with `address` or `city`/`state`
- Incremental rebuild of geocoded PlaceCards to update the index with new lat/lng values

**Post-rebuild verification:**

- `ppa index-status` — verify card counts match expectations (existing + new derived + new entities)
- `ppa validate` — no broken references
- `ppa temporal-neighbors --timestamp 2025-12-27T20:14:00-08:00` — returns Amelia birth cards in correct order
- `ppa query --type meal_order --limit 20` — returns DoorDash-derived cards with populated items
- `ppa query --type ride --limit 20` — returns Uber/Lyft-derived cards with routes
- `ppa query --type place --limit 20` — returns auto-generated PlaceCards with lat/lng populated
- Quality score distribution check — aggregate by type, verify derived cards with items score higher than those without

**Definition of Done:**

_Phase 0 baseline:_

- Full Phase 0 test suite passes — no regressions from pre-rebuild state
- `ppa health-check` produces a clean report against the production index

_Rebuild correctness:_

- `ppa index-status` card count matches vault file count within 0.1%
- Zero validation errors
- All temporal neighbor query/answer pairs in `slice_manifest.json` pass (forward, backward, and during legs)
- At least 5 new card types have populated projection tables with non-zero rows
- Quality score distribution by type matches expectations (derived cards with items score higher than those without)

_Performance:_

- Rebuild completes within 5-hour cap
- Actual rebuild time logged and compared to Phase 0 extrapolation

---

## Phase 5: ONE Full Embedding Pass

**Execution plan:** [`phase_5_embedding_pass_17a0e872.plan.md`](file:///Users/rheeger/.cursor/plans/phase_5_embedding_pass_17a0e872.plan.md)

**What it is:** Compute embeddings for every chunk in the corpus using OpenAI `text-embedding-3-small` (1536 dimensions, matching `DEFAULT_VECTOR_DIMENSION`). All cards now have their final `search_text` and summaries from the Phase 4 rebuild.

**Why now:** If you embed before the schema changes, the embeddings are computed against the old `search_text` (which didn't include item names, routes, restaurant names, etc.). If you embed before the extractors run, you miss thousands of cards entirely. Embedding after the rebuild means every chunk gets the best possible text, and you pay the API cost once.

**Logging:** **`embed-pending`** is rate-limit-bound — use **`ppa --log-file logs/embed-phase5.log embed-pending …`**; log chunks/sec, retries, and API errors on stderr (`ppa.*`).

**Pre-flight:**

- Run `ppa embedding-status` to get the pending chunk count. With ~1.84M existing cards + ~3K-7K derived cards, and richer `search_text` (new frontmatter fields + receipt-style body text), expect significantly more chunks than the pre-v2 corpus.
- The operation is **API-rate-limited**, not CPU-bound. Wall-clock time is dominated by OpenAI's rate limit. With `DEFAULT_EMBED_CONCURRENCY = 4` and `DEFAULT_EMBED_BATCH_SIZE = 32`, throughput is ~128 chunks per API call cycle. Adjust concurrency based on your API tier's rate limits.

**Process:**

- `ppa embed-pending` — processes all chunks without embeddings
- Track throughput: chunks/second, total time, API errors/retries
- The operation is **re-runnable**: `embed-pending` only processes chunks that don't already have embeddings. If it crashes or is interrupted, re-run it — it picks up where it left off. To fully redo embeddings (e.g., after a model change), drop the `embeddings` table contents and re-run.

**Post-embedding verification:**

- Run all semantic search query/answer pairs from `slice_manifest.json` — these should include pairs added during Phase 1 (type-specific field values as search terms) and Phase 2 (derived card body text as search targets)
- Specifically verify that new derived card types are findable via semantic search: a query containing meal item names should rank `meal_order` cards highly; a query containing a destination city should rank `flight` cards
- Run hybrid search (FTS + vector) queries from the manifest to verify fusion works correctly with the new card types

**Definition of Done:**

_Phase 0 baseline:_

- Full Phase 0 test suite passes — no regressions

_Embedding completeness:_

- `ppa embedding-status` reports 0 pending chunks
- All semantic search query/answer pairs in `slice_manifest.json` pass
- Hybrid search (FTS + vector) queries from the manifest return correct fused results
- At least 3 queries specifically targeting new derived card types (meal_order, ride, flight) return those types in the top 5 results

---

## Phase 6: LLM Enrichment

**Execution plan:** [`phase_6_llm_enrichment_f286b0bd.plan.md`](file:///Users/rheeger/.cursor/plans/phase_6_llm_enrichment_f286b0bd.plan.md)

**What it is:** The first LLM-based improvement pass across the corpus. Two parallel workstreams — LLM enrichment (summary improvement, entity extraction) and seed-link analysis — running against the fully-indexed, fully-embedded graph. Geocoding is already complete (moved to Phase 4 post-rebuild).

**Why now:** Enrichment needs embeddings (for semantic neighbor analysis in seed-links), needs the full edge graph (to understand relationships), and needs all cards present (to avoid enriching cards that will later get new edges or context). Running earlier would produce lower-quality enrichment against incomplete data, and you'd have to re-run it.

**Logging:** Enrichment and seed-link backfills must log **queue depth, workers, batch progress, spend/budget counters, and errors** to `ppa.*` stderr; overnight or full-corpus runs use **`--log-file`**.

### LLM enrichment configuration

- **Model:** Configurable via `PPA_ENRICHMENT_MODEL`, default `openai:gpt-4o-mini`. Start with GPT-4o-mini — it's cost-effective for summary improvement and basic entity extraction. If quality is insufficient for specific task types, escalate to GPT-4o for those tasks via a per-task-type model override.
- **Budget:** `PPA_ENRICHMENT_TOKEN_BUDGET` — maximum **$200** for the first pass. At GPT-4o-mini rates (~$0.15/1M input + $0.60/1M output), this covers roughly 500K-700K cards at ~1K input + ~200 output tokens per card — far more than the likely enrichment queue. The budget is a safety cap, not a target.
- **Parallelism:** The enrichment job runs with `--workers N` (default 4). Workers claim tasks from the enrichment queue via `SELECT ... FOR UPDATE SKIP LOCKED`. Each worker makes concurrent LLM API calls within its batch. Seed-link analysis runs as a separate parallel workstream (CPU-bound, no API calls).

### Run-scoped provenance and rollback

Every enrichment run is tagged with a `run_id` (timestamp + model + config hash). Provenance for LLM-modified fields records not just `llm` but the specific `run_id`:

```
<!-- provenance summary: llm, run_id: enrich-20260401-gpt4omini-a3f2 -->
```

This enables:

- **Run isolation:** Query all cards modified by a specific run (`SELECT * FROM cards WHERE enrichment_run_id = '...'`)
- **Run revert:** If a run produced bad results (hallucinated entities, degraded summaries), revert all vault files modified by that run via `git diff` filtered by run_id. The enrichment system records which files were modified per run.
- **Model comparison:** Run enrichment on a sample with GPT-4o-mini, then the same sample with GPT-4o, compare quality improvement per dollar.

### Process

**Workstream 1 — LLM enrichment (budget-gated):**

1. **Quality scan and prioritization.** Query cards from the enrichment queue, prioritized by type weight:

   | Priority Tier   | Card Types                                                                   | Rationale                                                                                                                                                                           |
   | --------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
   | **1 — Highest** | `meal_order`, `purchase`, `grocery_order`, `ride`, `flight`, `accommodation` | Derived cards with structured data — enrichment adds missing entities (restaurants → PlaceCards, airlines → OrgCards) and improves summaries that feed knowledge cache aggregations |
   | **2 — High**    | `person`, `place`, `organization`                                            | Entity cards — richer descriptions improve entity-based retrieval and graph traversal                                                                                               |
   | **3 — Medium**  | `email_message`, `calendar_event`, `meeting_transcript`                      | High-volume types where summary improvement has the biggest search impact (generic subjects like "Re: Re: Thursday")                                                                |
   | **4 — Lower**   | `finance`, `medical_record`, `document`, `subscription`, `payroll`           | Structured enough already; lower enrichment ROI                                                                                                                                     |
   | **5 — Lowest**  | `email_thread`, `*_attachment`, `git_*`, `knowledge`, `observation`          | Metadata or system cards; minimal enrichment value                                                                                                                                  |

   Within each tier, order by recency (recent cards are more likely to be queried).

2. **Pilot batch (quality confirmation).** Before exhausting the budget, run enrichment on a small pilot batch (~100 cards from Tier 1). Generate the enrichment report (see below). Review the report — confirm that summaries are better, entities are correct, no hallucinations. **Only after pilot quality is confirmed, proceed to exhaust the budget.**

3. **Summary improvement.** For cards with `quality_score < 0.5` or sparse summaries (< 20 characters, or generic subjects): generate better summaries using body text + context from thread neighbors (for email_messages) or structured fields (for derived cards).

4. **Entity extraction.** From body text, find person/place/org mentions that aren't in frontmatter. Add them to `people:`, `orgs:`. Create new entity cards (PersonCard, PlaceCard, OrgCard) if the entity doesn't already exist. Focus on Tier 1-2 card types where entity extraction adds the most knowledge graph connectivity.

5. **Budget exhaustion.** After pilot quality is confirmed, work through the enrichment queue in priority order until the $200 budget is exhausted or all queued cards are processed. Track spending per run in real time.

6. **Re-score quality.** After enrichment, recompute `quality_score` for affected cards. `enrichment_version` increments, `enrichment_status` → `complete`, `enrichment_run_id` set.

**Workstream 2 — Seed-link analysis (runs in parallel, no LLM cost):**

- Run against the fully-connected graph with embeddings. Better edge candidates than running on partial data.
- This is CPU/memory-bound (pairwise embedding comparison), not API-bound. Runs concurrently with LLM enrichment.

**Post-enrichment:**

1. **Incremental re-embedding.** Cards whose summaries or `search_text` changed get re-embedded. This is a small incremental pass (likely a few thousand cards), not a full re-embed.

2. **Incremental rebuild.** Run incremental rebuild to update the index with enriched card data (new people, orgs, updated summaries).

### Enrichment report

Every enrichment run produces a human-readable report (`enrichment-report-{run_id}.md`) showing concrete before/after examples:

**Summary improvements (10 examples):**

```
Card: hfa-email-message-abc123
Before: "Re: Re: Re: Thursday"
After:  "Planning dinner at Cafe Grumpy with Sarah for Thursday 7pm"
Quality: 0.2 → 0.7
```

**Entity extraction (10 examples):**

```
Card: hfa-meal-order-def456
Added: people: ["Sarah Chen"], orgs: ["Brooklyn Hero Shop"]
New PlaceCard created: hfa-place-brooklyn-hero-shop
Quality: 0.6 → 0.85
```

**Aggregate metrics:**

- Cards enriched: N (by tier)
- Budget spent: $X of $200
- Average quality_score change: before → after
- New entities created: N PersonCards, N PlaceCards, N OrgCards
- Seed-link candidates produced: N

**Retrieval impact (before/after):**

- Run all query/answer pairs from `slice_manifest.json` before and after enrichment
- Report precision/recall changes per query category (FTS, temporal, graph, semantic)
- Flag any queries where precision decreased (potential hallucination or bad entity extraction)

### Definition of Done

_Phase 0 baseline:_

- Full Phase 0 test suite passes — no regressions
- `ppa health-check` clean after enrichment pass

_Quality confirmation:_

- Pilot batch reviewed: summary improvements are accurate, entity extractions are correct, no hallucinations
- Enrichment report generated with before/after examples for manual review
- Retrieval impact measured: `slice_manifest.json` query precision does not decrease after enrichment (enrichment should help, not hurt)

_Enrichment results:_

- Budget exhausted or all queued cards processed (whichever comes first)
- Average quality_score for enriched cards increases by ≥0.1
- Seed-link analysis produces new edge candidates
- Incremental re-embedding completes for changed cards
- All enriched cards have run-scoped provenance tags

_Operational:_

- Enrichment run_id recorded; rollback path verified (can identify and revert all files modified by a specific run)
- Budget tracking accurate: actual spend within 10% of reported spend

---

## Phase 7: Knowledge Cache Population

**Execution plan:** [`phase_7_execution_plan_b4b2c2ef.plan.md`](file:///Users/rheeger/.cursor/plans/phase_7_execution_plan_b4b2c2ef.plan.md)

**What it is:** Build a structured understanding of the archive owner's life — preferences, habits, patterns, relationships, and context — organized by domain, continuously maintained, and instantly queryable by any agent or MCP consumer. This is not just "cached SQL aggregations." It's a **living profile** derived from the archive that understands the owner from a human perspective.

**Why now:** Knowledge cards aggregate over the full corpus. The aggregations are best after enrichment has improved card quality — better summaries, more complete entity links, richer metadata.

**Logging:** `refresh-knowledge` / domain rebuilds over large facets must log **domain, facet, rows processed, elapsed, ETA (`M:SS` where implemented)** and use **`--log-file`** for long runs.

**Design principles (inspired by the [endaoment-fabric](https://github.com/endaoment/endaoment-fabric) knowledge architecture):**

1. **Domain-organized, not query-organized.** Knowledge is structured by life domain (food, travel, relationships, etc.), not by individual questions. Each domain has a summary card plus facet cards that cover different aspects.
2. **Knowledge vs. execution.** Knowledge cards are durable facts and patterns. They describe what the archive knows — agents and tools decide how to act on it.
3. **Quantitative + qualitative.** "Top 10 restaurants" is quantitative. "Prefers spicy food and Southeast Asian cuisine" is qualitative. Both are valuable. Quantitative comes from SQL aggregations. Qualitative comes from pattern inference (some via LLM, some algorithmic).
4. **Exhaustive within each domain.** The goal is that an agent consulting the knowledge base about "food" gets a comprehensive picture — not just a top-10 list, but preferences, habits, spending, dietary patterns, and how they've changed over time.

### Knowledge domain model

Each domain produces 1 summary card + N facet cards in `Knowledge/<domain>/`. Facet cards are independently refreshable — a new meal_order only stales the food facets, not the travel facets.

**Relationships domain:**

| Facet                     | Standing Query                                    | Method                                                                                  | Depends On                                            |
| ------------------------- | ------------------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `top-contacts`            | "Who are the 20 people I communicate with most?"  | SQL: GROUP BY person, COUNT messages, rank                                              | `email_message`, `imessage_message`, `beeper_message` |
| `family`                  | "Who are my family members?"                      | SQL: Filter PersonCards by `relationship_type` or tags                                  | `person`                                              |
| `close-friends`           | "Who are my closest friends?"                     | SQL: Top contacts excluding work-domain senders, weighted by recency + frequency        | `email_message`, `imessage_message`, `person`         |
| `professional-network`    | "Who do I work with most?"                        | SQL: Top contacts from work-domain senders                                              | `email_message`, `person`                             |
| `relationship-changes`    | "Who have I started/stopped talking to recently?" | SQL: Compare 90-day vs. prior-90-day message counts, flag >2× changes                   | `email_message`, `imessage_message`                   |
| `key-contacts-by-context` | "Who do I talk to about what?"                    | SQL: For top 20 contacts, most common email thread subjects/topics, weighted by recency | `email_message`, `email_thread`, `person`             |

**Food & dining domain:**

| Facet                 | Standing Query                                    | Method                                                                                       | Depends On                    |
| --------------------- | ------------------------------------------------- | -------------------------------------------------------------------------------------------- | ----------------------------- |
| `top-restaurants`     | "What restaurants do I order from most?"          | SQL: GROUP BY restaurant, COUNT, rank                                                        | `meal_order`                  |
| `top-items`           | "What are my most-ordered items?"                 | SQL: Flatten items, GROUP BY name, COUNT                                                     | `meal_order`                  |
| `monthly-spend`       | "How much do I spend on food delivery per month?" | SQL: GROUP BY month, SUM total                                                               | `meal_order`, `grocery_order` |
| `cuisine-preferences` | "What kinds of food do I prefer?"                 | SQL: GROUP BY restaurant with inferred cuisine type (from restaurant names/categories), rank | `meal_order`, `place`         |
| `ordering-patterns`   | "When and how do I order food?"                   | SQL: GROUP BY day-of-week + hour, delivery vs. pickup ratio, weekday vs. weekend             | `meal_order`                  |
| `grocery-habits`      | "What groceries do I buy regularly?"              | SQL: Flatten items, GROUP BY name, COUNT; filter for items ordered >3 times                  | `grocery_order`               |

**Travel domain:**

| Facet                 | Standing Query                               | Method                                                                                                                                  | Depends On                                                        |
| --------------------- | -------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `destinations`        | "Where have I traveled in the last 2 years?" | SQL: Unique destinations from flights + accommodations, sorted by date                                                                  | `flight`, `accommodation`                                         |
| `travel-frequency`    | "How often do I travel?"                     | SQL: COUNT trips per quarter, average trip duration                                                                                     | `flight`, `accommodation`                                         |
| `preferred-airlines`  | "Which airlines do I fly most?"              | SQL: GROUP BY airline, COUNT, rank                                                                                                      | `flight`                                                          |
| `preferred-hotels`    | "Where do I usually stay?"                   | SQL: GROUP BY property_name/chain, COUNT                                                                                                | `accommodation`                                                   |
| `common-routes`       | "What routes do I fly most?"                 | SQL: GROUP BY (origin, destination) pair, COUNT                                                                                         | `flight`                                                          |
| `seat-preferences`    | "What seats/class do I usually book?"        | SQL: GROUP BY fare_class, COUNT; most common seat position pattern                                                                      | `flight`                                                          |
| `trip-reconstruction` | "What did a specific trip look like?"        | SQL: For a given date range, join flights + accommodations + rides + calendar_events + meal_orders to reconstruct a full trip itinerary | `flight`, `accommodation`, `ride`, `calendar_event`, `meal_order` |

**Health domain:**

| Facet           | Standing Query                       | Method                                                            | Depends On                 |
| --------------- | ------------------------------------ | ----------------------------------------------------------------- | -------------------------- |
| `providers`     | "Who are my healthcare providers?"   | SQL: Distinct providers from medical_record, with last visit date | `medical_record`, `person` |
| `vaccinations`  | "What vaccinations do I have?"       | SQL: List vaccinations with dates                                 | `vaccination`              |
| `visit-history` | "When did I last see each provider?" | SQL: Latest medical_record per provider                           | `medical_record`           |

**Privacy constraint:** Health knowledge cards use **structured aggregation only** — no LLM synthesis. Medical record content is not sent to any external API. The health domain produces factual lists (providers, dates, vaccination records), not narrative summaries.

**Work domain:**

| Facet                | Standing Query                         | Method                                                                          | Depends On                |
| -------------------- | -------------------------------------- | ------------------------------------------------------------------------------- | ------------------------- |
| `current-role`       | "What is my current job and employer?" | SQL: From own PersonCard fields + most frequent recent work email sender domain | `person`, `email_message` |
| `work-contacts`      | "Who do I work with most right now?"   | SQL: Top contacts from work-domain senders in last 90 days                      | `email_message`, `person` |
| `work-communication` | "How much do I communicate for work?"  | SQL: COUNT work-domain messages per week, trend over time                       | `email_message`           |

**Finance domain:**

| Facet                  | Standing Query                                  | Method                                                                      | Depends On                       |
| ---------------------- | ----------------------------------------------- | --------------------------------------------------------------------------- | -------------------------------- |
| `active-subscriptions` | "What subscriptions am I currently paying for?" | SQL: Latest lifecycle event per service where event_type != 'cancelled'     | `subscription`                   |
| `monthly-spending`     | "What are my major spending categories?"        | SQL: GROUP BY vendor/category from purchases + meals + rides, SUM per month | `purchase`, `meal_order`, `ride` |
| `income`               | "What is my compensation pattern?"              | SQL: Latest payroll records, gross/net trends                               | `payroll`                        |

**Transport domain:**

| Facet                | Standing Query                            | Method                                                                   | Depends On |
| -------------------- | ----------------------------------------- | ------------------------------------------------------------------------ | ---------- |
| `monthly-ride-spend` | "How much do I spend on rides per month?" | SQL: GROUP BY month, SUM fare                                            | `ride`     |
| `ride-patterns`      | "How do I get around?"                    | SQL: Ride frequency by type (car/scooter/bike), time-of-day distribution | `ride`     |
| `preferred-services` | "Which ride services do I use most?"      | SQL: GROUP BY service, COUNT                                             | `ride`     |
| `common-routes`      | "Where do I go most by ride?"             | SQL: GROUP BY (pickup, dropoff) pair, COUNT                              | `ride`     |

**Home & lifestyle domain:**

| Facet                  | Standing Query               | Method                                                                                                                             | Depends On         |
| ---------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `current-residence`    | "Where do I currently live?" | SQL: Most frequent shipping address from `purchase` cards in the last 6 months, falling back to most frequent ride pickup location | `purchase`, `ride` |
| `residence-history`    | "Where have I lived?"        | SQL: Dominant shipping address per 6-month window, ordered by date                                                                 | `purchase`, `ride` |
| `shopping-preferences` | "Where do I shop most?"      | SQL: GROUP BY vendor from purchases, COUNT, rank                                                                                   | `purchase`         |

**Entertainment domain:**

| Facet             | Standing Query                   | Method                                                            | Depends On     |
| ----------------- | -------------------------------- | ----------------------------------------------------------------- | -------------- |
| `events-attended` | "What events have I been to?"    | SQL: List event_tickets sorted by date, with venue and event name | `event_ticket` |
| `favorite-venues` | "Where do I go for events most?" | SQL: GROUP BY venue, COUNT                                        | `event_ticket` |

**Personal domain — the "Executive's Bible":**

Inspired by the onboarding questionnaire a high-end executive assistant fills out in their first weeks. The goal: the knowledge base should understand you as well as a great personal assistant would — your rhythm, your preferences, your key dates, your VIPs. All of this is deducible from the card stack.

| Facet                 | Standing Query                           | Method                                                                                                                                                                                                                                    | Depends On                                            |
| --------------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `daily-rhythm`        | "What is my daily schedule pattern?"     | Algorithmic: First/last email or message timestamp per day, bucketed by hour-of-day. Identifies peak activity windows, typical start/end of day, and weekend activity level.                                                              | `email_message`, `imessage_message`                   |
| `communication-style` | "How do I communicate?"                  | SQL: Message volume by platform (email vs iMessage vs beeper), average email body length distribution, response time patterns (median time between receiving and replying)                                                                | `email_message`, `imessage_message`, `beeper_message` |
| `key-dates`           | "What are the important personal dates?" | SQL + algorithmic: Recurring calendar events with personal tags (birthdays, anniversaries). Cross-reference with purchase patterns around those dates (gift purchases near family birthdays).                                             | `calendar_event`, `purchase`, `person`                |
| `service-providers`   | "Who are my regular service providers?"  | SQL: Distinct providers from medical_record (doctors, dentists), recurring appointment patterns from calendar_event (therapist, trainer, accountant), frequent non-work contacts by category                                              | `medical_record`, `calendar_event`, `person`          |
| `vip-contacts`        | "Who do I respond to fastest?"           | SQL: For top 50 contacts by volume, compute median response time (time between their email arriving and my reply). Rank by fastest response — these are the VIPs.                                                                         | `email_message`                                       |
| `travel-preferences`  | "What are my travel booking patterns?"   | SQL: Preferred fare class distribution from flights (economy vs business vs first), preferred seat position, typical booking lead time (days between booking email and departure), preferred travel times (morning vs evening departures) | `flight`, `accommodation`                             |
| `gifting-patterns`    | "When and how do I buy gifts?"           | Algorithmic: Purchases from gift-likely vendors (flowers, wine, specialty retail) within ±7 days of known family birthdays/anniversaries from `key-dates`. Identifies who I buy for and typical spend.                                    | `purchase`, `calendar_event`, `person`                |
| `dietary-signals`     | "What dietary patterns are visible?"     | SQL: From meal_order items, GROUP BY common keywords (vegetarian, vegan, gluten-free, spicy, etc.) — identifies recurring dietary preferences. From grocery_order items, identify staple purchases.                                       | `meal_order`, `grocery_order`                         |

**Total: 9 domains, ~46 facets.** Each facet is a KnowledgeCard with independent staleness tracking.

### Computation methods

Most facets are **pure SQL aggregations** — no LLM calls needed. The knowledge base builds understanding through exhaustive structured queries, not generative AI.

| Method                                                                                                  | Facets                                                            | LLM Cost |
| ------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | -------- |
| Pure SQL (GROUP BY, COUNT, SUM, RANK, window functions)                                                 | ~34 of 46                                                         | $0       |
| Algorithmic (address clustering, pattern detection, temporal cross-referencing, response-time analysis) | ~10 of 46                                                         | $0       |
| LLM synthesis (narrative summary from structured data)                                                  | ~2 of 46 (deferred — only if SQL aggregations prove insufficient) | Minimal  |

LLM synthesis is explicitly deferred. Start with SQL-only facets. If specific facets need richer narrative (e.g., a cross-domain "Executive's Bible" summary), add LLM synthesis as a Phase 8 agent capability, not a Phase 7 concern.

### Card format

Knowledge cards use **structured JSON in frontmatter** (`knowledge_data` field) plus a **human-readable markdown body** that renders the same data. This preserves vault conventions (markdown bodies for all cards) while providing machine-readable structured data for MCP consumers.

```yaml
---
type: knowledge
domain: food
facet: top-restaurants
depends_on_types: [meal_order]
refresh_interval_hours: 24
input_watermark: "2026-03-28T12:00:00Z"
knowledge_data:
  - restaurant: "Brooklyn Hero Shop"
    count: 20
    total_spent: 845.60
    last_order: "2026-03-15"
  - restaurant: "Thai Villa"
    count: 15
    total_spent: 612.30
    last_order: "2026-03-22"
---
## Top Restaurants

1. **Brooklyn Hero Shop** — 20 orders, $845.60 total (last: Mar 15, 2026)
2. **Thai Villa** — 15 orders, $612.30 total (last: Mar 22, 2026)
...
```

The `archive_knowledge` MCP tool returns the `knowledge_data` field directly. The markdown body is for human inspection.

### Refresh and staleness

- **Staleness detection:** When new cards of a facet's `depends_on_types` appear in `ingestion_log` since the facet's `input_watermark`, the facet is stale.
- **Refresh interval:** Each facet has a `refresh_interval_hours` (default 24). Staleness detection fires, but refresh only happens if at least `refresh_interval_hours` have elapsed since last refresh. This prevents "new meal_order every day → food facets refresh every day" when the data barely changed.
- **`ppa refresh-knowledge [--domain food] [--force]`** — refreshes stale facets. `--force` ignores the interval. `--domain` limits to one domain. Without flags, refreshes all stale facets that have exceeded their refresh interval.
- **Maintenance integration:** `ppa maintain` (Phase 8b) calls `refresh-knowledge` as part of its cycle.

### Process

1. **Define facet templates.** Create KnowledgeCard templates for each of the ~35 facets, with `domain`, `facet`, `depends_on_types`, `refresh_interval_hours`, and the SQL/algorithm for computation.
2. **Implement refresh logic.** `ppa refresh-knowledge` runs the computation for each stale facet, populates `knowledge_data` and renders the markdown body.
3. **Initial population.** Run `ppa refresh-knowledge --force` to compute all facets for the first time.
4. **Verify.** Check that each domain's facets produce sensible results against the production data.

**Files touched:** New `archive_mcp/commands/knowledge.py` (refresh logic), `archive_mcp/knowledge/` directory with per-domain facet definitions and SQL queries. Knowledge card templates in `Knowledge/` vault directory (one subdirectory per domain). Modified: `archive_mcp/__main__.py` (CLI command), `archive_mcp/server.py` (if `archive_knowledge` tool needs updates for domain/facet routing).

### Definition of Done

_Phase 0 baseline:_

- Full Phase 0 test suite passes — no regressions

_Knowledge coverage:_

- All 9 domains populated with facet cards (relationships, food, travel, health, work, finance, transport, home, entertainment, personal)
- At least 38 of ~46 facets produce non-empty knowledge cards (some facets may be empty if the underlying data is sparse — e.g., `event_ticket` with few events)
- `archive_knowledge --domain <domain>` returns correct facet data for all 9 domains — verified against `slice_manifest.json` knowledge-domain query/answer pairs
- Each facet's `knowledge_data` is valid structured JSON and the markdown body renders it correctly

_Staleness and refresh:_

- Staleness detection works: add a mock card of a dependent type, confirm the corresponding facet flags as stale
- `refresh_interval_hours` respected: a facet flagged stale within its interval is not refreshed until the interval expires
- `ppa refresh-knowledge --force` recomputes all facets and updates `input_watermark`
- `ppa refresh-knowledge --domain food` refreshes only food domain facets

_Quality:_

- Facet results spot-checked against known data (e.g., "top restaurants" matches what you actually order from)
- Health domain produces only structured aggregations (no LLM synthesis, no external API calls with health data)

---

## Phase 8: Maintenance Automation + Tool Enhancements

**Execution plan:** [`phase_8_execution_plan_a16ec5dc.plan.md`](file:///Users/rheeger/.cursor/plans/phase_8_execution_plan_a16ec5dc.plan.md)

**What it is:** Two things: (1) bare-minimum maintenance automation that keeps the system current after deployment, and (2) enhanced MCP tool responses with confidence signaling and gap detection so consuming agents get the most out of the retrieval surface.

**What it is NOT:** Phase 8 does not build a query agent (`archive_ask`), discovery/pattern detection, or agent working memory. The PPA is a **retrieval engine, not a conversational agent.** Consuming agents (Claude, GPT-4o, OpenClaw agents, voice assistants) do the reasoning — the PPA retrieves, ranks, and cites. This separation ensures the PPA never interferes with the consuming agent's interpretation.

**Deferred to post-v2:** Discovery mode (pattern detection, observation cards, agent working memory). These are valuable but not critical for v2 — Phase 7's knowledge facets already capture patterns via SQL. Discovery can be added once v2 is deployed and operational patterns are understood from real usage.

**Logging:** `ppa maintain` must log each step (ledger tail, extract, resolve, rebuild, refresh) with **per-step duration and errors** to stderr; cron should append to **`/var/log/ppa-maintain.log`** (or `ppa --log-file` when invoked manually). Implements the same structured rules as other long jobs.

### Model provider interface

Maintenance uses LLM calls for enrichment tasks (summary improvement, entity extraction from Phase 6's pipeline). The model configuration uses a provider abstraction:

**Config format:** `PPA_ENRICHMENT_MODEL=provider:model` where provider is:

| Provider   | Format           | Example              | Notes                                                                  |
| ---------- | ---------------- | -------------------- | ---------------------------------------------------------------------- |
| `openai`   | `openai:<model>` | `openai:gpt-4o-mini` | Cloud API, requires `OPENAI_API_KEY`                                   |
| `ollama`   | `ollama:<model>` | `ollama:llama3.2:3b` | Local, free, private. Requires Ollama running on device.               |
| `openclaw` | `openclaw`       | `openclaw`           | Future: delegates model selection to OpenClaw based on user preference |

**Provider interface** (implemented in `archive_mcp/providers/`):

```python
class ModelProvider:
    def generate(self, prompt: str, max_tokens: int = 1024) -> str: ...
    def is_available(self) -> bool: ...
    def estimated_cost_per_1k_tokens(self) -> float: ...
```

Implementations: `OpenAIProvider`, `OllamaProvider`. `OpenClawProvider` is a future stub. The provider is resolved once at startup from `PPA_ENRICHMENT_MODEL` and shared across all LLM-consuming operations (enrichment, maintenance).

**Fallback cascade:** If the configured provider is unavailable (API down, Ollama not running), maintenance logs the failure and skips LLM-dependent tasks. Non-LLM tasks (extraction, entity resolution, knowledge refresh via SQL) continue normally.

### 8a) Enhanced MCP tool responses

Each existing retrieval tool gains two new response fields:

**Confidence signaling:** Every retrieval tool includes a `confidence` field (high/medium/low) in its response:

- `high` — knowledge cache hit (fresh facet), exact match, or >10 relevant results
- `medium` — partial matches, stale knowledge, 3-10 results
- `low` — <3 results, no knowledge cache, query hit a known gap pattern

**Gap detection:** When any retrieval tool returns sparse results (<3 cards, or no results for a query that _should_ have results based on known card types), it logs an entry in `retrieval_gaps`:

- `query_text`: the original query
- `gap_type`: `no_results`, `sparse_results`, `stale_knowledge`, `type_mismatch`
- `card_uid`: if the gap relates to a specific card

This happens transparently — the tool still returns whatever results it has. The gap log is for the maintenance cycle to act on.

**Agent prompt guide:** The MCP server's `instructions` field (already used in `server.py`) is updated with a routing guide for consuming agents:

```
When answering questions about the archive owner's life:

1. For factual lookups ("who is X", "what is Y"):
   → archive_person or archive_read

2. For cached knowledge ("what restaurants do I order from", "where do I live"):
   → archive_knowledge with the relevant domain

3. For temporal questions ("what was I doing on Dec 27", "last Tuesday"):
   → archive_temporal_neighbors with parsed timestamp

4. For recall ("where did I get that banh mi", "that flight to NYC"):
   → archive_hybrid_search with the query

5. For analytics ("how much do I spend on rides per month"):
   → archive_knowledge with finance/transport domain, or archive_query with type_filter

6. For exploration ("tell me about my relationship with Sarah"):
   → archive_person for the PersonCard, then archive_graph for connected cards

Always prefer archive_knowledge first — it returns pre-computed answers instantly.
Fall back to archive_hybrid_search for queries that don't match a knowledge domain.
Check the confidence field in responses — low confidence means the archive may not have enough data.
```

**Files touched:** Modified: `archive_mcp/index_query.py` (confidence computation per query type), `archive_mcp/server.py` (tool response enhancements, updated instructions), `archive_mcp/schema_ddl.py` (retrieval_gaps table already created in Phase 1e).

### 8b) Maintenance automation

A single CLI command that sequences existing operations to keep the system current. This is glue code over infrastructure built in earlier phases — not new capability.

`ppa maintain` — a single idempotent invocation, safe to run from cron:

1. **Tail ingestion ledger.** Read `ingestion_log` for entries since the last maintenance watermark (stored in `meta` table as `last_maintenance_at`). If no new entries, report "nothing to do" and exit.
2. **Auto-extraction.** For new `email_message` cards from known sender patterns (matched against the extractor registry from Phase 2) → run `ppa extract-emails` for matched senders only.
3. **Entity resolution.** For new derived cards produced by step 2 → run `ppa resolve-entities`.
4. **Incremental rebuild.** Run `ppa rebuild-indexes` (incremental, not full) to index newly extracted cards.
5. **Refresh knowledge.** Run `ppa refresh-knowledge` for stale facets only.
6. **Coverage report.** Output to stdout:
   - New cards ingested since last maintenance
   - Cards extracted / entities resolved in this cycle
   - Knowledge facets refreshed
   - Enrichment queue depth
   - Retrieval gaps logged since last maintenance
   - Errors encountered (with task details)
7. **Update watermark.** Set `last_maintenance_at` in `meta`.

**Error handling:** If any step fails, log the error and continue to the next step. The cycle is not atomic — partial progress is better than no progress. Failed steps are reported in the coverage report. Steps 2-5 are independently idempotent — safe to re-run.

**Scheduling on Arnold:**

```cron
# Run maintenance daily at 3am
0 3 * * * cd /srv/ppa && ppa maintain >> /var/log/ppa-maintain.log 2>&1
```

**Files touched:** New `archive_mcp/commands/maintain.py` (sequences existing commands). Modified: `archive_mcp/__main__.py` (new CLI command).

### Definition of Done

_Phase 0 baseline:_

- Full Phase 0 test suite passes — no regressions
- `ppa health-check` clean after Phase 8 changes

_Tool enhancements:_

- All retrieval tools include `confidence` field in responses
- Gap detection logs entries in `retrieval_gaps` when results are sparse (verified: run a query with no expected results, confirm gap is logged)
- MCP server instructions updated with agent prompt guide
- Consuming agent (tested via Claude or similar) correctly routes queries to appropriate tools using the prompt guide

_Maintenance:_

- `ppa maintain` runs end-to-end: tails ledger, extracts new emails, resolves entities, rebuilds incrementally, refreshes knowledge, produces coverage report
- Maintenance is idempotent: running twice in a row produces the same result (second run reports "nothing to do")
- Partial failure handling: if extraction fails for one sender, remaining steps still execute
- Coverage report includes all specified metrics

_Model provider:_

- `PPA_ENRICHMENT_MODEL=openai:gpt-4o-mini` works for maintenance enrichment tasks
- `PPA_ENRICHMENT_MODEL=ollama:<model>` works when Ollama is running locally
- Provider unavailability is handled gracefully (LLM tasks skipped, non-LLM tasks continue)

---

## Phase 9: Production Deployment on Arnold

**Execution plan:** [`phase_9_execution_plan_794d5d32.plan.md`](file:///Users/rheeger/.cursor/plans/phase_9_execution_plan_794d5d32.plan.md)

**What it is:** Deploy v2 to Arnold and rebuild the production index directly on the VM. No pgdump/transfer needed — Arnold has the vault (encrypted share) and Postgres already running. The seed stays local as the test corpus and fallback.

**Why it's a separate phase:** All prior phases develop and test locally against the seed vault. This phase runs the v2 code against the full production vault on Arnold for the first time.

**Logging:** On Arnold, run **`rebuild-indexes`**, **`embed-pending`**, and **`migrate`** with **`ppa --log-file`** under e.g. **`/var/log/ppa/`** or `/srv/ppa/logs/` so SSH disconnects do not lose visibility; tail the same files you use for Phase 4/5 locally.

**Current Arnold architecture** (from hey-arnold):

- **Vault:** Encrypted LUKS volume at `/srv/hfa-secure/vault`, unlocked via 1Password + passkey-gate, mounted via systemd chain (`mnt-user.mount` → `ppa-unlock.service` → `ppa-mount.service`)
- **Postgres:** `pgvector/pgvector:pg17` Docker container, data at `/srv/hfa-secure/postgres`, port bound to `127.0.0.1`, stock defaults (no GUC tuning)
- **PPA code:** Git worktree at `/home/arnold/openclaw/ppa`, pip editable install, deployed via `make deploy-*` or `ppa-sync` (rsync)
- **MCP server:** systemd service `ppa-mcp.service` running `python -m archive_mcp serve`
- **Health check:** `scripts/ppa-health.sh` checks mount, vault file count, Docker PG container, backups

### Deployment model — rebuild on Arnold, not pgdump/transfer

The v2 deployment eliminates the pgdump → scp → restore pipeline. Arnold rebuilds its own index:

1. **Deploy v2 code to Arnold:** `make deploy-workspace` (git pull) + `ppa-install` (pip install -e .)
2. **Pre-flight checks on Arnold:**
   - `ppa health-check` against the existing (pre-v2) index — confirms the system is in a known-good state before changes
   - Disk space check: estimate v2 index size from Phase 4 metrics, confirm `/srv/hfa-secure/postgres` has sufficient room
   - Postgres version match: `pg17` on Arnold matches test infrastructure
3. **Run v2 migrations on Arnold:** Pending migrations from Phase 1 apply via `MigrationRunner`
4. **Run full rebuild on Arnold:** `ppa rebuild-indexes --force-full --workers N` against `/srv/hfa-secure/vault`
   - Same 5-hour cap as Phase 4 — checkpoint resume provides crash safety
   - Monitor via `rebuild_checkpoint` progress
5. **Post-rebuild geocoding:** `ppa enrich --task geocode` (same as Phase 4)
6. **Run embedding pass on Arnold:** `ppa embed-pending` — uses Arnold's OpenAI API access (or OpenClaw model config for future)
7. **Post-deployment verification:**
   - `ppa health-check` against the new index — clean report
   - `ppa index-status` — card count matches vault file count
   - Test a sample of manifest queries via the MCP server
8. **Set up maintenance cron:**

   ```cron
   # Run maintenance daily at 3am
   0 3 * * * cd /srv/ppa && ppa maintain >> /var/log/ppa-maintain.log 2>&1
   ```

9. **Restart MCP server:** `systemctl restart ppa-mcp`

**The seed stays as fallback:** During the v2 transition, keep the local seed dump available. If Arnold's rebuild fails or produces bad data, restore the pre-v2 index from the seed dump (`scp-restore-seed-arnold.sh`). Once the v2 production index is confirmed working, the seed becomes the test corpus and disaster-recovery backup, not the primary deployment source.

**Post-v2 steady state (no more pgdump/transfer):**

| Scenario                     | What happens on Arnold                                                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Code change (no schema bump) | Git pull → pip install → restart ppa-mcp. `ppa maintain` handles incremental work.                                                  |
| Code change (schema bump)    | Git pull → pip install → run migrations → `ppa rebuild-indexes --force-full` → restart ppa-mcp                                      |
| New data synced to vault     | Vault files appear on encrypted share → `ppa maintain` (daily cron) extracts, resolves, rebuilds incrementally, refreshes knowledge |
| Disaster recovery            | Restore from seed dump or latest backup                                                                                             |

### Postgres tuning for v2

Arnold currently runs pg17 with **stock defaults** (no GUC tuning). The v2 index is larger — more card types, more projections, more embeddings. Add basic tuning to Arnold's Postgres Docker config to match the Phase 0 test infrastructure:

```
shared_buffers = '256MB'
work_mem = '64MB'
maintenance_work_mem = '256MB'
effective_cache_size = '512MB'
```

These values should be derived from Arnold's VM memory allocation (inspect current allocation, allocate ~25% to shared_buffers). Apply via a `postgres.conf` mounted into the Docker container. **Update both Arnold's config and the Phase 0 test Docker** (`docker-compose.test.yml` / `postgres-test.conf`) so they match.

### Model configuration on Arnold

Arnold's enrichment and maintenance tasks use the model provider from Phase 8. For v2 deployment, configure via OpenClaw's model preferences:

- `PPA_ENRICHMENT_MODEL=openclaw` — delegates model selection to OpenClaw based on user preference
- Fallback: `PPA_ENRICHMENT_MODEL=openai:gpt-4o-mini` if OpenClaw model routing isn't available yet
- The model config lives in Arnold's environment (`.env` or systemd service override), not in the PPA codebase

### Encrypted vault integration

The orthanc hfa-secure encrypted share is the canonical production vault. Document and verify:

- Mount/unmount workflow (`ppa-backup-encrypt.sh` already exists, systemd chain handles boot-time unlock)
- Rebuild performance against encrypted share vs. local SSD — expect some I/O overhead from LUKS but within acceptable range given the 5-hour cap
- Backup schedule: ensure the existing backup system covers the v2 index (larger Postgres data directory)

### Remote MCP latency targets

The MCP server on Arnold serves over SSH tunnel. Latency targets for the v2 index:

| Query Type                    | Target Latency | Bottleneck                           |
| ----------------------------- | -------------- | ------------------------------------ |
| `archive_knowledge`           | < 1s           | Single row read from knowledge facet |
| `archive_search` (FTS)        | < 2s           | GIN index scan                       |
| `archive_temporal_neighbors`  | < 2s           | B-tree index on `(activity_at, uid)` |
| `archive_hybrid_search`       | < 5s           | Vector similarity + FTS fusion       |
| `archive_query` (type filter) | < 2s           | Projection table scan                |

If any query exceeds its target, investigate: connection pooling (`PPA_STATEMENT_TIMEOUT_MS`), Postgres GUC tuning, index stats (`ANALYZE`), or SSH tunnel overhead.

### CI/CD integration

Phase 0's test infrastructure provides the confidence gate for deployment:

1. Push to main → CI runs `test-unit` + `test-slice-verify` → all pass
2. `ppa health-check` passes against local seed slice index
3. Deploy to Arnold (manual trigger or post-merge automation via `make deploy-*`)
4. Arnold runs rebuild + health-check
5. If health-check fails on Arnold → rollback to pre-v2 code + restore seed dump

**Files touched:** Modified: `hey-arnold/Makefile` (deploy targets for v2), `hey-arnold/config/systemd/ppa-mcp.service` (environment for model config), `hey-arnold/scripts/ppa-health.sh` (incorporate `ppa health-check`). New: `hey-arnold/config/postgres.conf` (GUC tuning), cron entry for `ppa maintain`.

### Definition of Done

_Phase 0 baseline:_

- Full Phase 0 test suite passes — no regressions
- `ppa health-check` clean against both local seed index and Arnold's production index

_Deployment:_

- v2 code deployed to Arnold via `make deploy-*` + `ppa-install`
- Full rebuild completes on Arnold within 5-hour cap
- Embedding pass completes on Arnold
- `ppa health-check` on Arnold produces clean report with all card types populated, all edge rules active, temporal queries working
- Remote MCP server meets latency targets for all query types

_Operational:_

- `ppa maintain` cron job running on Arnold (daily at 3am)
- Postgres GUCs tuned and matching Phase 0 test infrastructure
- Model config set via OpenClaw preference (or OpenAI fallback)
- Seed dump retained locally as fallback — documented restore procedure verified
- Encrypted vault backup schedule covers v2 index size

---

## New MCP Tools & CLI Commands Reference

| Name                           | Type      | Phase | Profile                      | Purpose                                                                                                         |
| ------------------------------ | --------- | ----- | ---------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `archive_temporal_neighbors`   | MCP tool  | 1a    | full, read-only              | Cards near a timestamp via `(activity_at, uid)` keyset pagination + interval overlap                            |
| `archive_knowledge`            | MCP tool  | 1i    | full, read-only              | Read/check knowledge cache by domain                                                                            |
| (all existing retrieval tools) | MCP tools | 8a    | full, read-only, remote-read | Enhanced with confidence signaling and gap detection. Agent prompt guide in MCP instructions describes routing. |
| `slice-seed`                   | CLI       | 0     | —                            | Stratified transitive-closure slice from seed vault                                                             |
| `health-check`                 | CLI       | 0     | —                            | Structural + behavioral health assertions against any index                                                     |
| `benchmark`                    | CLI       | 0     | —                            | Multi-size performance benchmarking with scaling analysis                                                       |
| `temporal-neighbors`           | CLI       | 1a    | —                            | CLI version of temporal neighbors query                                                                         |
| `extract-emails`               | CLI       | 2     | —                            | Run email extractors against vault                                                                              |
| `resolve-entities`             | CLI       | 2c    | —                            | Create/merge PlaceCard and OrgCard files                                                                        |
| `refresh-knowledge`            | CLI       | 7     | —                            | Recompute stale knowledge cards                                                                                 |
| `maintain`                     | CLI       | 8b    | —                            | Run maintenance cycle (extraction, entity resolution, incremental rebuild, knowledge refresh)                   |
| `deploy`                       | CLI       | 9     | —                            | Deploy index to remote target                                                                                   |

---

## Dependencies Between Phases

```
Phase 0 (test infrastructure + rebuild verification)
└── Phase 1 (schema & data model) — uses Phase 0 migration infrastructure
    ├── Phase 1h (expand test infra for 37 types) — follows Phase 0 extensibility contract
    └── Phase 2 (extractors) — depends on 1b schemas being defined
        └── Phase 3 (run extractors) — depends on 2 code being ready
            └── Phase 4 (ONE rebuild) — depends on 1 + 3 complete
                └── Phase 5 (ONE embedding) — depends on 4 complete
                    └── Phase 6 (enrichment) — depends on 5 complete
                        └── Phase 7 (knowledge cache) — depends on 6 complete
                            └── Phase 8 (maintenance + tool enhancements) — depends on 7 complete
                                └── Phase 9 (deployment) — depends on 8 complete
```

**Parallelization opportunities:**

- Phase 2 (extractor code) can be developed in parallel with Phase 1 (schema), as long as 1b (type schemas) is done first
- Within Phase 2, individual extractors are independent and can be developed in parallel
- Phase 8a (tool enhancements) and 8b (maintenance) can be developed in parallel

**Critical path:** Phase 0 → Phase 1 → Phase 3 → Phase 4 → Phase 5 → Phase 6. Phases 7, 8, 9 build on 6 but have less time pressure since the archive is already usable after Phase 5.

---

## Risks

| Risk                                                                                                                                                                                                      | Impact                                                                       | Mitigation                                                                                                                                                                                                         | Decision Point                                                                                                                      |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Extractor yield rates** — what % of emails from a given sender actually produce parseable derived cards? Some eras may have HTML-only bodies with no extractable text.                                  | Lower-than-expected derived card counts. Some types may have very few cards. | Summary-only fallback in extractors (emit card with whatever data is available). Audit yield rates during Phase 3 staging.                                                                                         | Phase 3: if yield < 10% for a sender, consider dropping that extractor or investing in HTML parsing.                                |
| **Entity resolution precision** — fuzzy matching may produce false merges (two different "Main Street Pizza" locations merged) or false splits (same restaurant with slightly different name not merged). | Incorrect PlaceCards, misleading analytics.                                  | Use `(name, city)` compound key. Manual override via tags. Audit entity resolution output during Phase 3.                                                                                                          | Phase 3: spot-check entity resolution output. If error rate > 5%, invest in better disambiguation (geocoding, temporal clustering). |
| **Rebuild time growth** — 37 card types with more projections and edges means the full rebuild takes longer.                                                                                              | Phase 4 rebuild takes longer than expected.                                  | Benchmark against 1% and 5% seed slices to extrapolate full rebuild time. Use scaling curve analysis (Phase 0) to identify superlinear operations early. Checkpoint resume (Phase 0) provides crash safety.        | Phase 4: if projected time > 8 hours, consider parallelizing the materialization loop.                                              |
| **Embedding cost** — more cards with richer `search_text` means more chunks and higher API cost for the full embedding pass.                                                                              | Phase 5 costs more than budgeted.                                            | Estimate chunk count from Phase 4 rebuild metrics before starting. Consider embedding only high-quality cards first (quality_score > 0.3).                                                                         | Phase 5: review chunk count and projected cost before running.                                                                      |
| **LLM enrichment ROI** — summary improvement may not meaningfully improve retrieval quality for the token cost.                                                                                           | Phase 6 spends tokens without measurable benefit.                            | Budget-gate enrichment. Measure quality score improvement per 1K tokens spent. Stop if ROI drops below threshold.                                                                                                  | Phase 6: after first 1K cards, evaluate quality score improvement vs. cost.                                                         |
| **Seed slice completeness** — the stratified transitive-closure slicer may produce a corpus that's too large (if a few highly-connected cards pull in thousands of references) or miss rare types.        | Test corpus doesn't represent production well enough.                        | Cap cluster size (max 200 cards per seed). Reject seeds that exceed cap and pick alternatives of the same type. Guarantee ≥5 cards per type via stratified seeding.                                                | Phase 0: validate slice size, type coverage, and structure after first run.                                                         |
| **Arnold disk space** — the v2 index with 37 types, more projections, and embeddings may exceed Arnold's current disk allocation.                                                                         | Deployment fails in Phase 9.                                                 | Estimate v2 index size from Phase 4 rebuild. Plan Docker volume expansion before Phase 9.                                                                                                                          | Phase 9: pre-flight disk check before deploy.                                                                                       |
| **Template versioning maintenance** — email senders change templates regularly. Extractors need ongoing maintenance.                                                                                      | Derived cards stop being produced for newer emails.                          | Template versioning architecture (Phase 2) makes adding new parsers easy. `ppa maintain` (Phase 8b) runs extraction on new emails automatically; yield rate drops will be visible in maintenance coverage reports. | Ongoing: monitor extraction yield per sender per month via maintenance reports.                                                     |

---

## Continuous: Performance & Correctness Regression Suite

Running against the seed slice, synthetic fixtures, and production index in CI. Not a phase — this runs perpetually after Phase 0.

**a) Rebuild benchmarks:**

- Full rebuild against seed slice (every push), with per-phase timing
- Incremental rebuild correctness using synthetic fixtures (mutate 5%, assert == full)
- Noop rebuild time (should be < 10 seconds regardless of corpus size)
- Checkpoint resume (kill mid-rebuild, resume, assert identical output)
- Multi-size scaling analysis (1% and 5% slices, compare scaling curves for superlinear detection)

**b) Search precision:**

- Known query/answer pairs from `slice_manifest.json`
- FTS precision/recall against real seed data
- Vector search precision/recall (with `hash` provider for speed, `openai` behind a flag for realism)
- Hybrid fusion precision/recall
- Temporal neighborhood precision (given timestamp, expected cards in expected order)

**c) Concurrency:**

- Concurrent reads during rebuild — no deadlocks, reads return consistent results
- Concurrent MCP tool calls — no connection pool exhaustion
- Concurrent embedding calls — rate limiting works

**d) Extractor precision:**

- Each extractor has test fixtures from real emails
- Parsing precision (did it extract the right items, prices, dates?)
- Entity resolution precision (did duplicates merge? did distinct entities stay separate?)

**e) Regression tracking:**

- Benchmark results stored as JSON artifacts per CI run
- Alert on regressions above a threshold (rebuild time +10%, search precision -5%)
- Makefile target: `benchmark-all`

**Current test baseline:** ~294 tests. Expected growth: ~70 new tests in Phase 0 (test infra + rebuild verification), ~30 in Phase 1, ~60 in Phase 2 (extractors), ~10 each in Phases 3-9. Target: ~500+ tests by Phase 9 completion.

---

## Post-v2 Operational Model

After all v2 phases are complete, **regular operations are fully incremental. Full rebuilds are not part of the operational cadence.**

### Regular operations — all incremental

| Operation                                           | Mechanism                                                                                     | Rebuild Type |
| --------------------------------------------------- | --------------------------------------------------------------------------------------------- | :----------: |
| Gmail sync brings in new emails                     | New vault files → scanner detects → processes only new files                                  | Incremental  |
| Running extractors on new emails                    | `ppa extract-emails` writes derived cards → scanner detects                                   | Incremental  |
| Entity resolution on new derived cards              | New PlaceCards/OrgCards → scanner detects                                                     | Incremental  |
| Editing a person card                               | Scanner detects change → rebuilds person + up to 5,000 referencing cards                      | Incremental  |
| LLM enrichment improves summaries                   | Modified frontmatter_hash → scanner detects                                                   | Incremental  |
| Knowledge card refresh                              | SQL aggregation + vault write + incremental index                                             | Incremental  |
| `ppa maintain` cycle                                | Tails ingestion_log, extracts, resolves entities, rebuilds incrementally, refreshes knowledge | Incremental  |
| Data source refetch (e.g., re-sync a year of Gmail) | Changed/new cards detected by mtime/size                                                      | Incremental  |

The composite B-tree index on `(activity_at, uid)` is maintained automatically by Postgres on every insert — no post-load pass, no reassignment, no maintenance. New cards are immediately queryable via `temporal_neighbors` without any additional work.

### Adding new data sources

**If the card type already exists** (one of the 37): Write the adapter that produces vault files, run it, incremental rebuild picks them up. No full rebuild. No schema changes. No version bumps.

**If it's a truly new card type** (#38+): Use the Phase 0 migration infrastructure to add the projection table, add the Pydantic class and `CardTypeRegistration`, write the adapter, run it. Incremental rebuild processes the new cards. **No full rebuild required** as long as the new type is additive (doesn't change how existing types are processed). Do not bump version constants — existing cards don't need reprocessing.

### What actually requires a full rebuild

| Scenario                                                                                                  | Why                                                      | Expected Frequency                                       |
| --------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| Changing materialization logic for existing types (quality formula, edge rules, search_text construction) | Existing cards need reprocessing with new logic          | Rare — only for significant code-level changes           |
| Changing the embedding model                                                                              | All embeddings need regeneration (re-embed, not rebuild) | Very rare — model upgrades every 1-2 years               |
| Corruption recovery                                                                                       | Index has drifted from vault due to a bug                | Exceptional — Phase 0's verify-incremental prevents this |

**The invariant:** The vault is the source of truth, and the Postgres index is always derivable from vault + code. A full rebuild is always available as a reset button (`ppa rebuild-indexes --force-full`), but it should never be needed during normal operations.

---

## Summary Table

| Phase | What                                                                                                          | Touches Vault | Touches Postgres | Rebuild Cost  | Embed Cost  | LLM Cost |
| ----- | ------------------------------------------------------------------------------------------------------------- | :-----------: | :--------------: | :-----------: | :---------: | :------: |
| 0     | Test infrastructure + rebuild verification (seed slice, fixtures, health-check, cache fixes, migration infra) |   test only   |    test only     |   test only   |      0      |    0     |
| 1     | Schema + data model (37 types, temporal spine, infra tables)                                                  |       0       |        0         |       0       |      0      |    0     |
| 2     | Extractor framework + all extractors                                                                          |       0       |        0         |       0       |      0      |    0     |
| 3     | Run extractors (vault writes)                                                                                 |    **yes**    |        0         |       0       |      0      |    0     |
| 4     | **ONE full rebuild**                                                                                          |       0       |     **yes**      |    **1x**     |      0      |    0     |
| 5     | **ONE full embedding pass**                                                                                   |       0       |     **yes**      |       0       |   **1x**    |    0     |
| 6     | LLM enrichment                                                                                                |  incremental  |   incremental    |  incremental  | incremental |  **1x**  |
| 7     | Knowledge cache                                                                                               |  incremental  |   incremental    |       0       |      0      | partial  |
| 8     | Maintenance automation + tool enhancements (confidence, gap detection, agent prompt guide)                    |  incremental  |   incremental    |       0       |      0      |    0     |
| 9     | Secure deployment & remote access                                                                             |       0       |      remote      | **1x remote** |      0      |    0     |

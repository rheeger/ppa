# Card type contracts

An archive record is called a card. Its type gives shared meaning to fields across services: a purchase has an identity, amount, and source reference regardless of which supported receipt produced it. These contracts let importers and indexes preserve that meaning as new sources join the archive.

`archive_vault/schema.py` owns the models. `archive_vault/card_contracts.py` declares path families, chunk and edge profiles, external IDs, relationships, and projection targets. `archive_cli/card_registry.py` and `archive_cli/projections/` use those contracts in the derived index.

## Contract rules

1. Register a model and its canonical contract together.
2. Declare one path family, chunk profile, edge profile, and typed projection for each type.
3. Preserve deterministic fields and field provenance through import and processing.
4. Update the derived registration and tests in the same change as the card contract.

See the [implementation playbook](PLAYBOOK.md#adding-a-new-card-type) for the full workflow.

## Current inventory

These 36 registrations are copied from `archive_vault/card_contracts.py`. They define storage and indexing support; the records available in an archive depend on its imports and processing. The `knowledge` and `observation` types alone do not establish that PPA maintains a personal profile.

| Card type | Path family | Chunk profile | Edge profile | Typed projection | External ID fields | Relationship fields |
| --- | --- | --- | --- | --- | --- | --- |
| `person` | `People` | `person` | `person` | `people` | `source_id, linkedin, linkedin_url, github, twitter, instagram, telegram, discord` | `reports_to, people, orgs` |
| `finance` | `Finance` | `default` | `default` | `finance_records` | `source_id` | `people, orgs, counterparties, source_email` |
| `medical_record` | `Medical` | `default` | `default` | `medical_records` | `source_id, encounter_source_id` | `people, orgs` |
| `vaccination` | `Vaccinations` | `default` | `default` | `vaccinations` | `source_id` | `people, orgs` |
| `email_thread` | `EmailThreads` | `email_thread` | `email_thread` | `email_threads` | `source_id, gmail_thread_id` | `people, orgs, messages, calendar_events` |
| `email_message` | `Email` | `email_message` | `email_message` | `email_messages` | `source_id, gmail_message_id, gmail_history_id, message_id_header, invite_ical_uid, invite_event_id_hint` | `people, orgs, thread, attachments, calendar_events` |
| `email_attachment` | `EmailAttachments` | `default` | `default` | `email_attachments` | `source_id, attachment_id, content_id` | `people, orgs, message, thread` |
| `imessage_thread` | `IMessageThreads` | `imessage_thread` | `imessage_thread` | `imessage_threads` | `source_id, imessage_chat_id` | `people, orgs, messages` |
| `imessage_message` | `IMessage` | `default` | `imessage_message` | `imessage_messages` | `source_id, imessage_message_id, linked_message_event_id, reply_to_event_id` | `people, orgs, thread` |
| `imessage_attachment` | `IMessageAttachments` | `default` | `default` | `imessage_attachments` | `source_id, attachment_id` | `people, orgs, message, thread` |
| `beeper_thread` | `BeeperThreads` | `default` | `default` | `beeper_threads` | `source_id, beeper_room_id` | `people, orgs, messages` |
| `beeper_message` | `Beeper` | `default` | `default` | `beeper_messages` | `source_id, beeper_event_id, linked_message_event_id, reply_to_event_id` | `people, orgs, thread` |
| `beeper_attachment` | `BeeperAttachments` | `default` | `default` | `beeper_attachments` | `source_id, attachment_id, src_url` | `people, orgs, message, thread` |
| `calendar_event` | `Calendar` | `calendar_event` | `calendar_event` | `calendar_events` | `source_id, calendar_id, event_id, event_etag, ical_uid, invite_ical_uid, invite_event_id_hint, event_id_hint` | `people, orgs, source_messages, source_threads, meeting_transcripts` |
| `media_asset` | `Photos` | `default` | `default` | `media_assets` | `source_id, photos_asset_id` | `people, orgs` |
| `document` | `Documents` | `document` | `default` | `documents` | `source_id, content_sha, extracted_text_sha` | `people, orgs, authors, counterparties` |
| `meeting_transcript` | `MeetingTranscripts` | `meeting_transcript` | `meeting_transcript` | `meeting_transcripts` | `source_id, otter_meeting_id, otter_conversation_id, event_id_hint` | `people, orgs, calendar_events` |
| `git_repository` | `GitRepos` | `git_repository` | `git_repository` | `git_repositories` | `source_id, repository_id, repository_name_with_owner` | `people, orgs` |
| `git_commit` | `GitCommits` | `git_commit` | `git_commit` | `git_commits` | `source_id, commit_sha` | `people, orgs, parent_shas, repository` |
| `git_thread` | `GitThreads` | `git_thread` | `git_thread` | `git_threads` | `source_id, github_thread_id, number, associated_pr_numbers` | `people, orgs, messages, repository` |
| `git_message` | `GitMessages` | `git_message` | `git_message` | `git_messages` | `source_id, github_message_id, review_commit_sha, original_commit_sha` | `people, orgs, thread` |
| `meal_order` | `Transactions/MealOrders` | `default` | `meal_order` | `meal_orders` | `source_id` | `people, orgs, source_email` |
| `grocery_order` | `Transactions/Groceries` | `default` | `grocery_order` | `grocery_orders` | `source_id` | `people, orgs, source_email` |
| `ride` | `Transactions/Rides` | `default` | `ride` | `rides` | `source_id` | `people, orgs, source_email` |
| `flight` | `Transactions/Flights` | `default` | `flight` | `flights` | `source_id, confirmation_code` | `people, orgs, source_email` |
| `accommodation` | `Transactions/Accommodations` | `default` | `accommodation` | `accommodations` | `source_id, confirmation_code` | `people, orgs, source_email` |
| `car_rental` | `Transactions/CarRentals` | `default` | `car_rental` | `car_rentals` | `source_id, confirmation_code` | `people, orgs, source_email` |
| `purchase` | `Transactions/Purchases` | `default` | `purchase` | `purchases` | `source_id, order_number` | `people, orgs, source_email` |
| `shipment` | `Transactions/Shipments` | `default` | `shipment` | `shipments` | `source_id, tracking_number` | `people, orgs, source_email, linked_purchase` |
| `subscription` | `Transactions/Subscriptions` | `default` | `subscription` | `subscriptions` | `source_id` | `people, orgs, source_email` |
| `event_ticket` | `Transactions/EventTickets` | `default` | `event_ticket` | `event_tickets` | `source_id, confirmation_code` | `people, orgs, source_email` |
| `payroll` | `Transactions/Payroll` | `default` | `payroll` | `payroll_records` | `source_id` | `people, orgs, source_email` |
| `place` | `Entities/Places` | `default` | `default` | `places` | `source_id` | `people, orgs` |
| `organization` | `Entities/Organizations` | `default` | `default` | `organizations` | `source_id, domain` | `people, orgs` |
| `knowledge` | `Knowledge` | `default` | `default` | `knowledge_cards` | `source_id` | `people, orgs` |
| `observation` | `Agent` | `default` | `observation` | `observations` | `source_id` | `people, orgs, evidence_uids` |

## Adapter declarations

An ingest adapter declares its emitted card types, owned deterministic fields, identity keys, external IDs, relationships, and cursor support. Keep these declarations explicit so another contributor can understand how the source joins the catalog without reverse-engineering the parser.

The [connector SDK](CONNECTOR_SDK.md) adds account-scoped identity and replay checks. The [test extensibility contract](../archive_tests/EXTENSIBILITY_CONTRACT.md) describes fixture coverage for new types.

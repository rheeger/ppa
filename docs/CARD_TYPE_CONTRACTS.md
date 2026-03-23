# Card Type Contracts

This document is the canonical contract index for card types consumed by `archive-mcp`.

## Contract Rules

- `hfa` owns the canonical card schema.
- Each card type has one declared path family.
- Each card type has one declared chunk profile and one edge profile.
- Each card type has one typed projection target in the derived layer.
- New card types must update both the canonical contract registry and the derived projection registry in the same change.

## Current Inventory

| Card Type             | Path Family           | Chunk Profile        | Edge Profile         | Typed Projection       | Key External IDs                                                                   | Key Relationship Fields                                                      |
| --------------------- | --------------------- | -------------------- | -------------------- | ---------------------- | ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `person`              | `People`              | `person`             | `person`             | `people`               | `source_id`, `linkedin`, `github`, `twitter`, `instagram`, `telegram`, `discord`   | `reports_to`, `people`, `orgs`                                               |
| `finance`             | `Finance`             | `default`            | `default`            | `finance_records`      | `source_id`                                                                        | `people`, `orgs`, `counterparties`                                           |
| `medical_record`      | `Medical`             | `default`            | `default`            | `medical_records`      | `source_id`, `encounter_source_id`                                                 | `people`, `orgs`                                                             |
| `vaccination`         | `Vaccinations`        | `default`            | `default`            | `vaccinations`         | `source_id`                                                                        | `people`, `orgs`                                                             |
| `email_thread`        | `EmailThreads`        | `email_thread`       | `email_thread`       | `email_threads`        | `source_id`, `gmail_thread_id`                                                     | `people`, `orgs`, `messages`, `calendar_events`                              |
| `email_message`       | `Email`               | `email_message`      | `email_message`      | `email_messages`       | `source_id`, `gmail_message_id`, `message_id_header`                               | `people`, `orgs`, `thread`, `attachments`, `calendar_events`                 |
| `email_attachment`    | `EmailAttachments`    | `default`            | `default`            | `email_attachments`    | `source_id`, `attachment_id`, `content_id`                                         | `message`, `thread`, `people`, `orgs`                                        |
| `imessage_thread`     | `IMessageThreads`     | `imessage_thread`    | `imessage_thread`    | `imessage_threads`     | `source_id`, `imessage_chat_id`                                                    | `people`, `orgs`, `messages`                                                 |
| `imessage_message`    | `IMessage`            | `default`            | `imessage_message`   | `imessage_messages`    | `source_id`, `imessage_message_id`, `linked_message_event_id`, `reply_to_event_id` | `thread`, `people`, `orgs`                                                   |
| `imessage_attachment` | `IMessageAttachments` | `default`            | `default`            | `imessage_attachments` | `source_id`, `attachment_id`                                                       | `message`, `thread`, `people`, `orgs`                                        |
| `beeper_thread`       | `BeeperThreads`       | `default`            | `default`            | `beeper_threads`       | `source_id`, `beeper_room_id`                                                      | `people`, `orgs`, `messages`                                                 |
| `beeper_message`      | `Beeper`              | `default`            | `default`            | `beeper_messages`      | `source_id`, `beeper_event_id`, `linked_message_event_id`, `reply_to_event_id`     | `thread`, `people`, `orgs`                                                   |
| `beeper_attachment`   | `BeeperAttachments`   | `default`            | `default`            | `beeper_attachments`   | `source_id`, `attachment_id`, `src_url`                                            | `message`, `thread`, `people`, `orgs`                                        |
| `calendar_event`      | `Calendar`            | `calendar_event`     | `calendar_event`     | `calendar_events`      | `source_id`, `calendar_id`, `event_id`, `event_etag`, `ical_uid`                   | `people`, `orgs`, `source_messages`, `source_threads`, `meeting_transcripts` |
| `media_asset`         | `Photos`              | `default`            | `default`            | `media_assets`         | `source_id`, `photos_asset_id`                                                     | `people`, `orgs`                                                             |
| `document`            | `Documents`           | `document`           | `default`            | `documents`            | `source_id`, `content_sha`, `extracted_text_sha`                                   | `people`, `orgs`, `authors`, `counterparties`                                |
| `meeting_transcript`  | `MeetingTranscripts`  | `meeting_transcript` | `meeting_transcript` | `meeting_transcripts`  | `source_id`, `otter_meeting_id`, `otter_conversation_id`, `event_id_hint`          | `people`, `orgs`, `calendar_events`                                          |
| `git_repository`      | `GitRepos`            | `git_repository`     | `git_repository`     | `git_repositories`     | `source_id`, `repository_id`, `repository_name_with_owner`                         | `people`, `orgs`                                                             |
| `git_commit`          | `GitCommits`          | `git_commit`         | `git_commit`         | `git_commits`          | `source_id`, `commit_sha`                                                          | `people`, `orgs`, `parent_shas`, `repository`                                |
| `git_thread`          | `GitThreads`          | `git_thread`         | `git_thread`         | `git_threads`          | `source_id`, `github_thread_id`, `number`, `associated_pr_numbers`                 | `people`, `orgs`, `messages`, `repository`                                   |
| `git_message`         | `GitMessages`         | `git_message`        | `git_message`        | `git_messages`         | `source_id`, `github_message_id`, `review_commit_sha`, `original_commit_sha`       | `people`, `orgs`, `thread`                                                   |

## Adapter Contract Reminder

Every ingest adapter must declare:

- emitted card types
- deterministic fields owned
- identity keys
- external ID fields
- relationship fields
- whether it supports incremental cursor checkpoints

The adapter contract is explicit so `archive-mcp` does not have to rediscover canonical intent from adapter implementation details.

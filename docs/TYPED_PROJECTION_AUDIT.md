# Typed Projection Audit

This is the implementation inventory for the current typed projection rollout.

## Coverage Summary

All current canonical card types in `hfa.schema.CARD_TYPES` now have a registered typed projection.

| Card Type             | Typed Table            | Column Family Summary                                                     |
| --------------------- | ---------------------- | ------------------------------------------------------------------------- |
| `person`              | `people`               | names, aliases, contact methods, org/person linkage, social handles       |
| `finance`             | `finance_records`      | amount/currency, date range, counterparties, communication metadata       |
| `medical_record`      | `medical_records`      | clinical coding, encounter metadata, value fields, details JSON           |
| `vaccination`         | `vaccinations`         | immunization metadata, provider/facility, lot/series fields, details JSON |
| `email_thread`        | `email_threads`        | subject, participants, message counts, thread linkage                     |
| `email_message`       | `email_messages`       | sender/recipient fields, thread linkage, attachment and calendar linkage  |
| `email_attachment`    | `email_attachments`    | attachment metadata and parent linkage                                    |
| `imessage_thread`     | `imessage_threads`     | thread identity, participant metadata, conversation shape                 |
| `imessage_message`    | `imessage_messages`    | sender state, reply/reaction metadata, thread linkage                     |
| `imessage_attachment` | `imessage_attachments` | attachment metadata and parent linkage                                    |
| `beeper_thread`       | `beeper_threads`       | bridge/service identity and participant metadata                          |
| `beeper_message`      | `beeper_messages`      | sender state, reply/reaction metadata, thread linkage                     |
| `beeper_attachment`   | `beeper_attachments`   | attachment metadata and parent linkage                                    |
| `calendar_event`      | `calendar_events`      | event identity, organizer/attendees, time fields, source linkage          |
| `media_asset`         | `media_assets`         | media identity, capture metadata, labels, dimensions, place metadata      |
| `document`            | `documents`            | library/file metadata, extraction metadata, entities and quality flags    |
| `meeting_transcript`  | `meeting_transcripts`  | transcript identity, participant/speaker metadata, Otter linkage          |
| `git_repository`      | `git_repositories`     | repo identity, visibility, topics/languages, counts                       |
| `git_commit`          | `git_commits`          | commit identity, author, parent/branch linkage                            |
| `git_thread`          | `git_threads`          | issue/PR identity, state, assignees/labels, branch metadata               |
| `git_message`         | `git_messages`         | review/comment identity, actor metadata, diff context                     |

## Shared Structural Fields

Every typed table also includes:

- `card_uid`
- `rel_path`
- `card_type`
- `summary`
- `created`
- `updated`
- `primary_source`
- `source_id`
- `activity_at`
- `external_ids_json`
- `relationships_json`
- `typed_projection_version`
- `canonical_ready`
- `migration_notes`

## Explainability Contract

Each typed projection is expected to support:

- projection inventory entry
- projection status entry
- projection explain output keyed by `card_uid`

Projection explain should answer:

- which typed table this card materialized into
- whether canonical data was fully ready
- which canonical fields fed each major typed column group
- whether migration notes remain

# Recap a day

Use this for "how was my day", "what did I do yesterday", or any recap
of one local day. Follow the ranking in [SKILL.md](SKILL.md). A longer
span is [reconstruct-a-story.md](reconstruct-a-story.md).

Calendar is the plan. Messages are what happened. If they disagree, the
messages win.

## Sequence

1. Planned: `archive_query` `type_filter=calendar_event` for the local
   day. Read those cards. Label them planned. Do not narrate them as
   attended.
2. Lived, one type per call, `limit` at least 40:
   `imessage_message`, `beeper_message`, `email_message`,
   `meeting_transcript`, `ride`, `flight`. Query **messages**, not
   thread cards.
3. Date-only `start_date` / `end_date` is a UTC calendar day. For an
   America/New_York recap, query that UTC date and the next one, then
   keep rows whose local `sent_at` falls on the day.
4. Skip `git_commit` and `git_message` unless the user asked about code.
5. Sort remaining hits by `sent_at` in local time. Read bodies with
   `archive_read_many` for every 1:1 that moved, and for any line that
   breaks the plan. If the day is a cancellation, also read that thread
   from the evening before.
6. Name the people on those messages. Follow the parent thread UID when
   a single line is not enough.
7. Write the day as: where they were, who they talked to, what they
   cancelled or missed, then (separately) what had been on the calendar.

## Stop tests

- You did not tell a working-block story when a message said they were
  on the road or at a funeral.
- You did not fill a thin message query with calendar or git.
- You did not hybrid-search biography words without the date.
- Times are local, not UTC.

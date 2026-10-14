# Reconstruct a story

Use this for "what happened", a relationship, a trip, a fight, a hire,
"what do we know about X", or any span that is not a single local day
(that is [recap-a-day.md](recap-a-day.md)). Follow the ranking in
[SKILL.md](SKILL.md). Do not start with untyped `archive_evidence`.

## Sequence

1. If a person is in the question, finish
   [identify-person.md](identify-person.md), then
   [census-channels.md](census-channels.md). Include Beeper, iMessage,
   email, calendar, and any movement types (`ride`, `flight`).
2. Name the window in local time. Query lived message types in that
   window, one type per call, `limit` at least 40. Skip `git_commit`
   unless the question is about code.
3. Read thread cards for the 1:1s that moved (`message_count`,
   `first_message_at`, `last_message_at`). Thread cards are the map.
   Bodies are samples.
4. `archive_read_many` on every line that would change the story, and on
   enough of the largest 1:1 to see the arc. Follow parent / attachment /
   duplicate UIDs. If the span looks like a cancellation, read the
   evening before.
5. Check plan against actual: `calendar_event` and invites versus the
   messages. Do not narrate a working block as attended.
6. Write from events. Name who was there. Keep conflicts. Cite cards
   you opened.

## Stop tests

- Channel `matched_total`s are visible before the narrative.
- The last email is not the last update unless iMessage and Beeper
  totals are in hand (or those channels are empty after a real census).
- A calendar block is not attendance.
- You did not collapse two people who share an alias or a household
  phone.
- A thin lived query was named as a thin index, not filled from git.

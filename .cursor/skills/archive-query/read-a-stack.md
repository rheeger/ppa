# Read a stack

Use this for a dated pile: a thread, a trip, a month, an attachment set.
Follow the ranking in [SKILL.md](SKILL.md). A mixed dated dump ranks git
and calendar over texts.

## Sequence

1. Query **one type per call**. For lived time that is
   `imessage_message` and `beeper_message`, not `archive_timeline` and
   not untyped `archive_evidence`. Raise `limit` when you need more
   than the default.
2. `archive_evidence` is a compact dated list (uid, date, type, title,
   parent/duplicate/attachment pointers) **inside** a `type_filter` or
   `people_filter`. `narrative=true` outlines whatever ranked. It is
   not the story.
3. Do not add biography nouns (`life`, `school`, `work`, `family`) to
   `query=` when `people_filter` is already set. Those words turn a
   person's iMessage corpus into lexical junk.
4. Recency-sorted `archive_query` without a type is the latest group
   chatter, often git. Add `type_filter`.
5. Follow parent / attachment / duplicate UIDs on demand. Do not flatten
   them into one quote.
6. Read bodies with `archive_read` / `archive_read_many` only for UIDs
   you will use.
7. A personal day is [recap-a-day.md](recap-a-day.md). A span that is
   not one local day is [reconstruct-a-story.md](reconstruct-a-story.md).

## Stop tests

- Every claim you will speak has a card you opened, not a snippet.
- Thread cards (`message_count`, date span) are the map. Bodies are
  samples.
- Conflicting cards stay conflicting.
- You did not treat a git-heavy timeline as the lived stack.

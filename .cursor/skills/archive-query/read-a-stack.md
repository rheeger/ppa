# Read a stack

Use this for a dated pile: a thread, a trip, a month, an attachment set.

## Sequence

1. Prefer `archive_evidence` for a compact dated list (uid, date, type, title,
   parent/duplicate/attachment pointers). Raise `limit` when you need more
   than the default.
2. Do not add biography nouns (`life`, `school`, `work`, `family`) to `query=`
   when `people_filter` is already set. Those words turn a person's iMessage
   corpus into lexical junk.
3. Recency-sorted `archive_query` without a type is the latest group chatter.
   Add `type_filter` or use evidence.
4. Follow parent / attachment / duplicate UIDs on demand. Do not flatten them
   into one quote.
5. Read bodies with `archive_read` / `archive_read_many` only for UIDs you
   will use.
6. Set `narrative=true` on evidence when you want a short dated outline that
   still cites UIDs.

## Stop tests

- Every claim you will speak has a card you opened, not a snippet.
- Thread cards (`message_count`, date span) are the map. Bodies are samples.
- Conflicting cards stay conflicting.

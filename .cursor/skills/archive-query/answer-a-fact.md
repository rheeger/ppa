# Answer a fact

Use this when the user asked one thing that a card can settle. Follow
the ranking in [SKILL.md](SKILL.md). Attendance, "did it happen", and
"where were they" are lived-message facts, not calendar facts.

## Sequence

1. If you already have the UID or path, `archive_read`. That is the
   trust boundary.
2. If the fact is about a person, confirm them
   ([identify-person.md](identify-person.md)) before `people_filter`.
3. If you have a type, person, or source, `archive_query`. One type per
   call. For "was I there" / "did the meeting happen", query
   `imessage_message` and `beeper_message` in the window, then read
   `calendar_event` as the plan.
4. Exact phrase → `archive_search_json`. Vague →
   `archive_hybrid_search_json`. Drop hits from other years.
5. Read the card you will cite. Titles, chunks, and embeddings are not
   quotes.
6. If the first phrasing misses, change the filter, switch tools, or
   raise `limit`. Do not invent. A thin lived query is a thin index,
   not a no.

## Stop tests

- The fact is on a card you opened.
- A calendar block is not proof they attended.
- A lexical hit from another year is not this fact.
- `archive_person`'s first hit is not a cite when `summary` is not the
  needle (see [identify-person.md](identify-person.md)).
- High confidence means exact-identifier retrieval quality, not "the
  vault is complete."
- Empty, timeout, or `ok=false` is fail closed.

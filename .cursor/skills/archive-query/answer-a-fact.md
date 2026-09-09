# Answer a fact

Use this when the user asked one thing that a card can settle.

## Sequence

1. If you already have the UID or path, `archive_read`. That is the trust
   boundary.
2. If you have a type, person, or source, `archive_query`. One type per call.
3. Exact phrase → `archive_search_json`. Vague → `archive_hybrid_search_json`.
4. Read the card you will cite. Titles, chunks, and embeddings are not quotes.
5. If the first phrasing misses, change the filter, switch tools, or raise
   `limit`. Do not invent.

## Stop tests

- The fact is on a card you opened.
- `archive_person`'s first hit is not a cite when `summary` is not the needle
  (see [identify-person.md](identify-person.md)).
- High confidence means exact-identifier retrieval quality, not "the vault is
  complete."
- Empty, timeout, or `ok=false` is fail closed.

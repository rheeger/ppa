---
name: archive-query
description: >-
  Investigate the living PPA archive. Use for who someone is, what happened,
  a day or trip, a thread, a fact, or any archive_person / archive_query /
  archive_evidence / archive_read retrieval. Ranks lived messages over calendar
  and git, censuses channels, and reconstructs events from cards.
---

# Archive query

`archive-local` MCP tools only. If those tools are missing, say so in one
sentence. Do not curl Tailscale, do not spawn `archive_cli`, do not read
runbooks to answer a question.

Cards are truth. Search hits, embeddings, and chunks are navigation.
`people_filter` is a name or slug, never an email. `archive_person`
`ambiguous` means list candidates, do not pick a winner.

## How to reconstruct

A question is almost never one card type. One tool call returns whatever
that index ranks. Git commits and all-day calendar blocks outrank texts.
That ranking is not the event. You assemble the event from cards you open.

### Rank by how close the card is to what happened

1. Lived: `imessage_message`, `beeper_message` (Discord, Slack, Telegram),
   `meeting_transcript`, `ride`, `flight`. Query **messages**, not thread
   cards. Threads date to the thread, not the day.
2. Sent: `email_message` bodies. Not `email_thread`, not attachment dumps.
3. Planned: `calendar_event`, invites, `.ics` files. A block on the calendar
   is the plan. Do not narrate it as attended.
4. Derived: search snippets, embeddings, `archive_knowledge`, person aliases.
   These find cards. They are not quotes.
5. Byproduct: `git_commit`, `git_message`, `git_thread` unless the question
   is about code.

If a lived message and a calendar block disagree, the message wins.

### Census before you narrate

For anyone or any span you will describe, get `matched_total` per channel.
A page of 40 with `truncated` is not the corpus. Include Beeper. Empty
iMessage does not mean they did not text.

### Deduce from events

1. Name the people and the local-time window. Date-only `start_date` /
   `end_date` is a UTC day. For an America/New_York span, query that UTC
   date and the next, then keep rows whose local `sent_at` falls inside
   the window.
2. If a person is in the question, confirm them
   ([identify-person.md](identify-person.md)), then
   [census-channels.md](census-channels.md).
3. Query **one lived type per call**. Raise `limit`. Sort by `sent_at` /
   `activity_at` in local time. Skip git unless the question is code.
4. Read bodies (`archive_read` / `archive_read_many`) for every 1:1 that
   moved, and for any line that would change the story (late, driving,
   cancelled, "won't make it", a death, a flight). Follow parent thread,
   attachment, and duplicate UIDs when one line is not enough. If the
   span looks like a cancellation, read that thread from the evening
   before.
5. Check the plan against those bodies. Write where they were, who they
   talked to, what they cancelled. Then, separately, what had been
   planned. Keep conflicts. Cite cards you opened.

### Thin index is not an empty life

If lived query or search returns none or two rows against a busy day or
person, say the search index is thin and stop. Do not fill the hole with
calendar or git. `archive_read` by UID can succeed when query misses.
That is an unpublished card, not proof nothing happened.

Do not hybrid-search biography words (`funeral`, `drive`, `family`,
`life`) without a date. That recalls other years.

Empty, timeout, or `ok=false` is fail closed. High confidence is
identifier-match quality, not vault completeness.

## Tools

- Exact phrase → `archive_search_json`
- Vague when lexical is weak → `archive_hybrid_search_json` or
  `archive_vector_search`
- Known type / person / source → `archive_query` (one type per call)
- Totals → `archive_analytics` `workflow=query`; read `matched_total`
- Compact dated list **inside a type** → `archive_evidence` (raise `limit`)
- Around one timestamp you already have → `archive_temporal_neighbors`
- Mixed `archive_timeline` / untyped evidence → git-and-calendar ranking,
  not a recap
- Candidate person → `archive_person`, then read the card
- Edges from a known UID → `archive_graph`
- Bodies → `archive_read` / `archive_read_many`

Defaults are starting points. Raise `limit`. Follow parent /
attachment / duplicate UIDs.

## Jobs

Every job follows the ranking above. Open the matching file, then retrieve.

1. Identify a person → [identify-person.md](identify-person.md)
2. Census channels → [census-channels.md](census-channels.md)
3. Read a stack → [read-a-stack.md](read-a-stack.md)
4. Answer a fact → [answer-a-fact.md](answer-a-fact.md)
5. Reconstruct a story → [reconstruct-a-story.md](reconstruct-a-story.md)
6. Recap a day → [recap-a-day.md](recap-a-day.md)

"Who is X" is 1, then 2, then 5. "What happened", a trip, or a
relationship is 5 (and 1–2 if a person is in it). A local day is 6. A
single settleable claim is 4.

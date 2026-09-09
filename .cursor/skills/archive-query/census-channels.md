# Census their channels

Do this before any "who is" write-up. A recency page is not the corpus.

## Sequence

1. Finish [identify-person.md](identify-person.md) so `people_filter` points at
   the real person.
2. Run `archive_analytics` `workflow=query` once per type:
   `imessage_thread`, `imessage_message`, `email_message`, `calendar_event`.
3. Read `matched_total` on each response. A listing of 40 with `truncated` is
   a page, not the total.
4. If iMessage (or Beeper) threads exist, read the thread cards first:
   `message_count`, `first_message_at`, `last_message_at`, `thread_summary`.
5. Sample sender-confirmed bodies after the thread map, not before.
6. Search the person's email and phone as `query=` in a second pass. The
   people index and the handle on the thread can disagree.

## Stop tests

- You printed `matched_total` for each channel you will talk about.
- You did not call the last email the last update while iMessage totals were
  unread.
- `volume_without_relevance` plus a high `matched_total` means split the type
  and drop extra query words. It does not mean abandon the channel.
- Phones and emails on the person card are second needles (`query=`), not
  `people_filter`.

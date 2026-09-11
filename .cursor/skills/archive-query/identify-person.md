# Identify a person

`archive_person` returns a candidate card. It is not identity.

## Sequence

1. Call `archive_person` with the display name, an email, or a phone. The
   tool accepts all three. `people_filter` on search and query is still a
   name or slug only.
2. Read the card. Confirm `summary` equals the needle, or that an email or
   phone on the card already appeared on a message you read.
3. If the needle appears only in `aliases`, the name was stolen. Do not use
   that card. Invitation From-lines (Paperless Post, Evite, Punchbowl) write
   host names and event titles onto the mailbox owner.
4. Search the name and read every `type=person` hit. Prefer the card whose
   `summary` is the needle.
5. Look up emails and phones from a grounded message (`query=`, never
   `people_filter`). Name lookup can miss the real card. Email lookup found
   Lisa Messinger after name lookup returned Susan Wolfe.
6. If the tool says `ambiguous`, list the candidate UIDs. Do not pick a winner.
7. A thin card (one email, "N emails seen") is a contact stub. It is not the
   biography. Continue to [census-channels.md](census-channels.md).

## Stop tests

- `summary` matches the needle, or you have an email/phone join you already
  saw on a real message.
- You have not treated an alias-only hit as the person.
- `archive_graph` from the person UID can be empty while thousands of cards
  already list `[[that-slug]]`. Graph is edges, not the people index.
- `type_filter=person` plus the same `people_filter` can be empty. That is not
  "no such person."

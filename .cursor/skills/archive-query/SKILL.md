---
name: archive-query
description: >-
  Routes living-archive questions into five retrieval jobs and loads the matching
  recipe. Use when asking who someone is, what the vault knows, reconstructing a
  story, reading a card stack, citing a fact, or using archive_person / archive_query
  / archive_evidence / archive_hybrid_search / people_filter against PPA.
---

# Archive query jobs

Cards are truth. Search hits are navigation. Warm MCP only. Never spawn a new
`archive_cli` process to ask a living-archive question.

Pick one job. Read that file before you retrieve. Do not improvise from the
tool list alone.

1. Identify a person → [identify-person.md](identify-person.md)
2. Census their channels → [census-channels.md](census-channels.md)
3. Read a stack → [read-a-stack.md](read-a-stack.md)
4. Answer a fact → [answer-a-fact.md](answer-a-fact.md)
5. Reconstruct a story → [reconstruct-a-story.md](reconstruct-a-story.md)

A "who is X" or profile write-up is job 1, then job 2, then job 5. A single
dated question is job 3 or 4.

Types use underscores (`email_message`, `imessage_thread`). `people_filter` is
a name or slug, never an email. Put emails in `query=` or pass them to
`archive_person` (name, slug, email, or phone). If `archive_person` returns
`ambiguous`, list the candidates. Do not pick a winner.

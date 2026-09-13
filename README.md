# PPA - Personal Private Archives

PPA is the evidence layer you own, and that any agent can use.

It stores the records your life or organization already creates: email, messages, calendar events, files, photos, health records, financial exports, code history, receipts, travel, and meetings. Those records become typed Markdown cards on disk. You keep the files. You pick the agent. The archive does not move when the agent does.

A cloud agent (Cursor, Claude, Muse, Grok, Hermes, Instinct, OpenClaw, or the next one) can write and reason. It cannot remember your last tetanus shot, which hotel was on the December trip, or what you and Sarah actually said in 2019, unless that evidence lives somewhere you control. Provider memory is their product. It stays in their cloud, in their format, for as long as they keep the account. PPA is your archive.

When an agent is connected to PPA, it looks up your cards, reads them, and cites them. You can open the same file and check the cite. When you switch agents, the next one gets the same tools and the same evidence. You do not re-teach it your life.

PPA finds cards, ranks them, and shows why they matched. It does not invent a biography, and it does not replace the cards with a generated answer.

## Why an agent needs this

Without an archive you own, an agent can only use what you typed into that product, what that product stored for itself, or what it guesses from the public web. That is why it forgets last month, invents a subscription, or merges two people who share a household phone.

With PPA, the same agent can:

- Open the vaccination card, the receipt, or the thread, and cite the card identifier.
- Filter by type, so a flight is a `flight` and a DoorDash receipt is a `meal_order`.
- Follow a charge to the purchase it paid for, or a flight to the hotel on the same trip.
- Say when the archive does not have enough data, instead of filling the gap with a guess.

The model is still the model. The agent becomes more capable because the evidence is typed, linked, local, and yours. Every agent you attach can use that layer. None of them become the source of truth.

## What that feels like

You ask in the client you already use. The agent calls PPA over MCP (the Model Context Protocol). You see identifiers you can open as Markdown. If two cards disagree, you see both. If a person lookup is ambiguous, the agent lists candidates instead of picking a household winner.

These are the kinds of questions that become answerable, and what you get back:

- "Where did I get that delivery banh mi?" looks across food orders, receipts, finance records, and messages, not one inbox and not the agent's chat history.
- "What was I doing around December 27?" pulls calendar, travel, photos, purchases, rides, and conversations from the same window.
- "Which flight, hotel, and rental car were part of this trip?" follows booking codes, airports, cities, and dates, not titles that happen to sound alike.
- "Which purchase matches this credit-card charge?" reconciles merchant, amount, and date.
- "Tell me about my relationship with Sarah" combines her person card, threads, calendar events, photos, and related cards. Two people who share a household phone stay separate until you can tell them apart.
- "What subscriptions am I paying for?" and "What did that trip cost?" read every matching card the archive has, and say when coverage or freshness is incomplete. They do not invent a current subscription or convert currencies.

A folder indexer finds files. A notes app finds notes you wrote. A cloud agent finds what it was allowed to remember. PPA is for records that already live in products that were never meant to work together, turned into something you or any agent can query on purpose, and that you can take with you.

Worked examples of the same "without the archive / with the archive" contrast live in [What changes when an agent has PPA](archive_docs/ARCHIVE_SUPERPOWER_DEMO.md).

## What you get

### One archive across the systems you already use

PPA imports from services, exports, and local databases you already have, so an agent is not limited to the last prompt you typed:

- Communication and meetings come from Gmail (including people found in your mail), iMessage, Beeper, and Otter.ai. Those sources can stay live.
- Calendar and contacts come from Google Calendar, Google Contacts, and Apple Contacts (Contacts.app). A hand-dropped VCF file is still an export import, not a nightly updater. Apple Contacts needs Automation permission for Contacts, the same class of macOS grant as iMessage Full Disk Access.
- People directories come from LinkedIn exports, Notion people or staff CSVs, and people files you provide.
- Files and code come from file libraries and GitHub.
- Photos can be imported from Apple Photos. Health and medical records can be imported from Apple Health exports, clinical and EHR files, FHIR JSON, CCD/XML, PDFs, and Epic EHI TSVs. Live refresh for Photos and Apple Health is parked. The cards you already imported stay queryable.
- Finance records come from Copilot transaction CSVs.

Re-running an import updates the same cards instead of duplicating history. You do not re-type your life into each new agent.

The same software can represent a person, a household, a company, or another organizational identity. Two archives on one machine stay isolated, so a work archive and a personal archive do not bleed into each other, and an agent attached to one cannot see the other.

### Cards that name what actually happened

Each card is a Markdown file with a type. A flight is a `flight`. A DoorDash receipt becomes a `meal_order`. An iMessage conversation is a thread plus its messages. You and an agent can filter by type instead of hoping a keyword hits the right blob of text, or hoping the agent's memory still has last Tuesday.

PPA models cards for:

- People, places, and organizations (`person`, `place`, `organization`)
- Email, iMessage, and Beeper threads, messages, and attachments
- Calendar events, photos and other media, documents, and meeting transcripts
- Finance records, medical records, and vaccinations
- Git repositories, commits, threads, and messages
- Derived transactions: meals, groceries, rides, flights, stays, car rentals, purchases, shipments, subscriptions, event tickets, and payroll

A card should represent something that happened, a booking, a request, a transaction, a message, or a person or place. Marketing mail and passive notifications do not become cards unless they contain structured evidence of a real action.

Every meaningful field records which step wrote it (an import, an enrichment pass, or a person). An agent can prefer the card's own fields over a generated summary, exact values over inferred ones, and the card body over a search snippet or a chat recollection.

### Enrichment that fills gaps without becoming the record

Sparse imports get richer before you or an agent search them:

- Email extractors classify threads, skip noise, and write typed cards for receipts, travel, purchases, subscriptions, rides, and payroll.
- Thread enrichment adds summaries and the names of people and organizations mentioned in email, iMessage, and Beeper conversations.
- Finance enrichment classifies who you paid (or who paid you) and links charges to people, organizations, purchases, meals, and subscriptions.
- Document enrichment extracts text from supported files and adds summaries, dates, and names.

Language models help with that work. Their outputs are checked against a schema, tagged with where they came from, cached, and safe to resume. The Markdown card stays the record. The enriching model is not the archive, and neither is the agent that later reads it.

### Links that match how things actually connect

Search by similarity is useful when you only remember the gist. That is also what a cloud agent does when it has no structure. Questions about a specific charge, trip, or meeting need links:

- Finance reconciliation links a charge to the purchase, meal, or subscription it paid for.
- Trip clustering groups flights, stays, and rental cars from airports, cities, dates, and booking codes.
- Meeting linking connects a calendar event to the transcript and the email thread about that meeting.
- Shipment linking connects a tracking notice to the purchase it belongs to.
- Identity, communication, calendar, and media links connect a person to their messages, events, and photos.

That is why an agent can answer "which hotel was on this trip?" by following a real link, not by hoping two titles sound alike in its context window. Confirmation codes, source emails, amounts, dates, tracking numbers, routes, calendar IDs, and shared participants beat similar-looking titles.

When PPA walks from one card to a related card, the link has a type (paid-for, part-of-trip, same-meeting). Do not treat every related card as equally evidenced. Guessed "maybe related" links stay off unless you turn that path on.

### Several ways to look things up

You and an agent share the same lookup tools. A person uses the `ppa` command. An agent uses the matching MCP tools in Cursor, Claude Desktop, Codex, OpenClaw, or any other MCP client. The meanings are the same. Switching clients is a config paste, not a migration of your life.

| When you or the agent want to                                             | Use                                                                           |
| ------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Open the actual card and cite it                                          | `ppa read` / `archive_read` (or `archive_read_many` for a batch)              |
| Filter by type, source, person, organization, or date                     | `ppa query` / `archive_query`                                                 |
| Find exact words                                                          | `ppa search` / `archive_search`                                               |
| Find the meaning when you forgot the phrasing                             | `archive_vector_search`                                                       |
| Combine words, meaning, and related cards                                 | `ppa hybrid-search` / `archive_hybrid_search`                                 |
| Get a compact dated stack, with parent and attachment pointers            | `archive_evidence`                                                            |
| Walk chronology, or ask what else happened around a time                  | `archive_timeline`, `ppa temporal-neighbors` / `archive_temporal_neighbors`   |
| Identify someone by name, email, or phone                                 | `ppa person` / `archive_person`                                               |
| See cards linked to one you already have                                  | `ppa graph` / `archive_graph`                                                 |
| Ask about subscriptions, trip cost, or what changed since a point in time | `ppa analytics` / `archive_analytics`                                         |
| See why a result ranked where it did                                      | `archive_retrieval_explain`                                                   |
| Check whether the archive is in a state you should trust                  | `ppa status`, `ppa health`, `archive_status_json`, `archive_embedding_status` |

Search hits, snippets, embeddings, and an agent's paraphrase are navigation. The Markdown card is the cite. After a hybrid hit in a long email or message thread, PPA can include the message before and after so the short answer is not stranded. Sources you have denied do not leak through those related cards.

`archive_person` returns `unique`, `ambiguous`, or `unresolved`. It will not pick a household winner. A name that only appears as someone else's alias is not that person.

`archive_knowledge` exists, but today it falls back to ordinary search. It is not a living profile of you, and it is not a substitute for reading cards.

When results are thin or surprising, PPA records a lookup gap so maintenance can show where the archive needs more data, better extraction, or better linking. That is how the data layer stays honest for every agent you attach later.

### Answers that stay honest

PPA does the lookup. You or the agent do the reasoning. That split is the point. The agent can write. The archive holds the evidence.

Analytics say when coverage or freshness is incomplete. Lookup only sees a finished, published search index, so a half-written rebuild cannot become the live one. If two cards disagree, you see both. A rebuilt index is not a license to invent a missing card.

New mail and new imports land through `ppa maintain`. That command records what it built and publishes only the cards this run actually produced, so what you or an agent can ask matches what just arrived.

### Files you own, on a machine you control

The archive is a folder of Markdown. PPA calls that folder the vault. That is what you back up, copy, and keep when you change agents, laptops, or providers. The search index and the Postgres database are built from those files. If the index disappears, rebuild it from the vault.

Embeddings and enrichment can use a local provider or a cloud API, depending on how you configure the instance. API keys stay in the client environment. Generated MCP config does not print them. A read-only tool profile lets an agent ask questions without changing the archive.

```text
Gmail, calendar, photos, exports
  -> Markdown cards you own
  -> search index (rebuilt from those cards)
  -> CLI and MCP tools
  -> you, or any agent you attach
```

Keeping the files separate from the index, and the archive separate from the agent, is what makes this safe. The archive can grow, the index can be rebuilt, embeddings can be regenerated, and you can change from Muse to Grok to OpenClaw, without surrendering the record.

## How you use it

A person can ask from the command line, which is the same lookup an agent will run:

```bash
ppa search "banh mi"
ppa hybrid-search "that flight to NYC"
ppa query --type meal_order
ppa temporal-neighbors "2025-12-27T18:00:00Z"
ppa person "Sarah"
ppa analytics subscriptions
ppa analytics trip-costs
ppa graph "People/sarah.md"
ppa read "hfa-email-message-..."
ppa status
ppa health
ppa maintain
```

`ppa status` and `ppa readiness` describe the archive you are talking to right now. A new archive reports its own health. It does not copy a passing grade from another archive on the same machine. `ppa analytics` reports the cards you actually have, not every real-world event.

To give any MCP agent the same tools:

```bash
ppa serve
ppa mcp-config
```

Paste the generated config into Cursor, Claude Desktop, Codex, OpenClaw, or another MCP client. Details are in [MCP setup](archive_docs/MCP_SETUP.md). Agents should read the card before treating a search hit as a fact. Recipes for identifying a person, counting their channels, reading a dated stack, citing a fact, and reconstructing a story live in `.cursor/skills/archive-query/` and [Agent usage](archive_docs/AGENT_USAGE.md).

Admin commands such as `rebuild-indexes`, embedding backfills, and rebuilds of related-card links are in the [runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md). Restrict those tools with `PPA_MCP_TOOL_PROFILE` so a client that should only ask questions cannot change the index.

## Getting started

You need Python 3.10 or newer, and Postgres with the pgvector extension (used to store embeddings in the warehouse). Live lookup uses the Rust search index built from the vault, not Postgres full-text search. For meaning-based search, set an embeddings provider. `hash` is enough to test locally without an API. `openai` or a compatible API is what you want for quality.

Supported search needs the native `archive_crate` library. Until that library is built, lookup tools will not run. A plain `pip install -e .` is not enough.

Clean install (hashed wheels, Python 3.12):

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install --require-hashes -r requirements/runtime-py312.lock
python archive_scripts/build_release.py
pip install --no-deps dist/ppa-*.whl
ppa setup --non-interactive --from spec.json --apply
```

Developer checkout (you must build the native library):

```bash
git clone https://github.com/rheeger/ppa.git
cd ppa
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
# build archive_crate with maturin against this interpreter (see the install runbook)
```

See [install-independent-archive](archive_docs/runbooks/install-independent-archive.md).

Start local Postgres and build the index:

```bash
cp .env.pgvector.example .env.pgvector
make pg-up
make bootstrap-postgres
make rebuild-indexes
make embed-pending
```

Then run `ppa serve` as above. Minimum environment:

```bash
export PPA_INDEX_DSN="postgresql://archive:archive@127.0.0.1:5432/archive"
export PPA_INDEX_SCHEMA="archive_seed"
export PPA_PATH="/path/to/vault"
export PPA_EMBEDDING_PROVIDER="openai"
export PPA_EMBEDDING_MODEL="text-embedding-3-small"
```

Remote Postgres over SSH is supported:

```bash
ppa serve --tunnel user@host
```

An agent on another machine can use HTTP MCP against the host that owns the vault. The files stay on that host. Details: [MCP setup](archive_docs/MCP_SETUP.md), [runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md), and [example MCP config](archive_docs/examples/ppa.mcp-example.json).

## Keeping the archive current

Day to day, you run incremental work so every attached agent sees new evidence without a full rebuild:

1. New imports write vault cards.
2. Extractors turn new source material into meals, flights, purchases, and the rest.
3. The same person, place, or organization appearing in more than one source is merged into one card.
4. Incremental rebuilds update search without reprocessing the whole vault.
5. `ppa maintain` records that work and publishes only the cards this run built, so lookup sees what just arrived.

A full rebuild is the reset button. The normal operating model is incremental.

Large archives stay usable because walking the vault, building the index, and answering lookups run through a Rust layer. Health checks validate vault structure, embeddings, related-card links, and known question/answer pairs, so a broken lookup shows up as a failed check instead of a confident wrong answer in chat.

## Production notes

- Set `PPA_FORBID_REBUILD=1` around a real production database unless a rebuild is intentional.
- Prefer a local build, dump, restore, or a written playbook for production index changes.
- Use a read-only or remote-read MCP tool profile for agents that should never mutate the index.
- Keep the vault backed up separately from Postgres. The Markdown folder is the record. The index can be rebuilt.
- Run `ppa health`, `archive_status_json`, and embedding status checks before trusting lookup after imports or maintenance.

Security, backup, and operations: [security model](archive_docs/SECURITY_MODEL.md), [backup and restore](archive_docs/PPA_BACKUP_AND_RESTORE.md), and [runbooks](archive_docs/runbooks/).

## Tests

```bash
.venv/bin/python -m pytest archive_tests/
```

The suite covers card schema, adapters, index behavior, MCP and CLI tools, graph and linker behavior, destructive-operation safeguards, migrations, and live warehouse integration when available.

## Documentation

- [Current status](archive_docs/STATUS.md) (what is true today)
- [Product capability matrix](archive_docs/PRODUCT_CAPABILITY_MATRIX.md)
- [Archive superpowers](archive_docs/ARCHIVE_SUPERPOWER_DEMO.md) (with-archive vs without-archive)
- [Architecture](archive_docs/ARCHITECTURE.md)
- [Indexing](archive_docs/INDEXING.md)
- [Agent usage](archive_docs/AGENT_USAGE.md)
- [MCP setup](archive_docs/MCP_SETUP.md)
- [Contributor playbook](archive_docs/PLAYBOOK.md)
- [Runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md)
- [Card type contracts](archive_docs/CARD_TYPE_CONTRACTS.md)
- [Retrieval contract](archive_docs/RETRIEVAL_CONTRACT.md)
- [Linker architecture](archive_docs/LINKER_ARCHITECTURE.md)
- [Contributing linkers](archive_docs/CONTRIBUTING_LINKERS.md)

## Contributing

PRs are welcome. Run the focused tests for the area you touch, and run `pytest` for changes to card schemas, adapters, index materialization, retrieval, MCP, migrations, linkers, or operational safety. If CLI, environment, or MCP semantics change, update [the runtime contract](archive_docs/PPA_RUNTIME_CONTRACT.md). That file is the automation handshake. How to add a card type, adapter, extractor, or linker is in the [contributor playbook](archive_docs/PLAYBOOK.md).

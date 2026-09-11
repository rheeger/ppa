# PPA status

**As of 2026-09-09.** Branch `fix/maintain-living-loop`. Living seed serving generation `1788997214192`.

This page is the current product state. When a vision file, rebase, or older report conflicts with this page or [PRODUCT_CAPABILITY_MATRIX.md](PRODUCT_CAPABILITY_MATRIX.md), this page wins. Vision files stay historical intent. There is no v2.75.

`production_proven` is still **false**. Isolated acceptance is not a long soak.

## What PPA is now

A private evidence archive. Cards in a Markdown vault are the truth. A Rust serving index answers query. Postgres is a derived warehouse. Agents retrieve through MCP. They do not get a living profile or a chatbot that answers for them.

v2.5-done is this machine’s canonical seed as the living high-signal corpus. The work after that close-out (PRs 24–33, including the ten-plan hardening in [PR 29](https://github.com/rheeger/ppa/pull/29)) made that corpus usable at seed scale.

## What you can ask

- Find a receipt, flight, thread, or charge with lexical, hybrid, or vector search that returns instead of timing out.
- Open the card before you treat a hit as a fact.
- Identify a person by name, slug, email, or phone. Shared household phones stay `ambiguous`. An alias-only hit is not the person.
- Ask what subscriptions you pay for, what a trip cost, or what changed since a checkpoint through `ppa analytics` / `archive_analytics`. Those workflows return facts, coverage, and freshness. They do not advise. A later cancel is not a current subscription. Currencies do not convert.
- Ask from a remote client (Arnold on this tailnet) over HTTP MCP. The vault and index stay on the machine that owns them.

## Ten-plan product hardening (PR 29)

This is the retrieval and engine product of late v2.5. It is why PPA is not “a folder of Markdown plus search.”

| Plan                       | What a person using the archive gets                                                                                                                                                                                                            |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P01 Retrieval fidelity     | Short answers inside long email and message threads are findable (conversation bursts). Hybrid search fuses lexical and vector ranks (RRF). Semantic search uses trained IVF centroids, not a placeholder. Ranking can prefer fresher evidence. |
| P02 Publication            | Query reads a complete serving generation. A half-written publish cannot become `ACTIVE`. Changes are journaled.                                                                                                                                |
| P03 Processor receipts     | `ppa maintain` publishes the cards this run actually built. Failed revisions stay off the served watermark.                                                                                                                                     |
| P04 Acceptance             | Isolated product gate on an integrated SHA. Not a user feature. Not a production rollout.                                                                                                                                                       |
| P05 Access                 | Denied people and sources do not leak through graph hops or neighbor context.                                                                                                                                                                   |
| P06 Engine ports           | CLI and MCP share one typed runtime. Burst attach and connector persist use the same path.                                                                                                                                                      |
| P07 Recovery               | Corrections and restores keep a versioned story. A rebuilt index is not a license to invent missing cards.                                                                                                                                      |
| P08 Connectors             | Account-isolated ingest and replay. One account’s mail does not become another archive’s evidence.                                                                                                                                              |
| P09 Independent instances  | Two archives on one machine stay isolated through restart. Saved scopes are reusable filters, not household ACLs. `ppa setup` can bind a fixture root.                                                                                          |
| P10 Evidence and analytics | Full-set typed query with honest completeness. Neighbor context (one message before and after). Bounded graph. Subscriptions, trip costs, and changes-since.                                                                                    |

Model rerank stays optional and off. Million-vector train at 1536-d on an 8 GB cap was blocked, not waived. The living seed trains with a higher host cap.

## After PR 29 (PRs 30–33)

- One phone / email / handle canon so the same person is findable across iMessage and email.
- `archive_person` resolves phone and email, not only slug.
- Person lookup returns `unique` / `ambiguous` / `unresolved`. It does not pick a household winner.
- Embeddings are keyed by chunk text. Rematerialize reuses paid vectors instead of orphaning them.

## Ops on this machine

| Fact                                      | Status                                                                                                                                                                                                                                         |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Living vault                              | `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127`, schema `ppa`                                                                                                                                                                   |
| Query                                     | Rust serving index. Postgres is warehouse-only.                                                                                                                                                                                                |
| Arnold                                    | Remote HTTP MCP client of Ginger. Not the home of the corpus. Do not copy the seed there.                                                                                                                                                      |
| Nightly `ppa maintain`                    | Apply loop no longer tails the whole ingestion ledger. A follow-up apply rematerialized 2,516 dirty cards (not 1.39 million) and published generation `1788997214192`. LaunchAgent `com.rheeger.ppa.maintain-nightly` stays **unloaded**. Receipt: [reports/maintain-living-loop.md](reports/maintain-living-loop.md). |
| HTTP MCP                                  | `com.rheeger.ppa.mcp-http` on the current Tailscale address, read-only profile. Pin the `ppa-http-mcp` worktree to current main and bounce after a deploy.                                                                                     |
| Photos, Apple Health, `--catch-up`        | Parked                                                                                                                                                                                                                                         |
| Knowledge cache / 46-facet living profile | Empty. `archive_knowledge` falls back to search.                                                                                                                                                                                               |
| New archives                              | Fail closed on their own instance. They do not inherit `local_seed_living_corpus`.                                                                                                                                                             |

Ask living-archive questions through the already-running MCP. Do not cold-open `archive_cli` once per question.

## What “proven” is not, on this increment

This seed already uses real accounts and paid OpenAI embeddings. That is done. Do not treat it as unfinished.

These are **v3**, not a gate for this machine:

- Install the way a stranger would (packaged wheels, no checkout).
- Prove Linux or any second platform.
- Flip `production_proven` to true after a formal sign-off.

The apply loop is dirty-only on this seed. Gmail from April through July 2026 is still thin on disk (April folder missing). A Gmail-only uncapped catch-up is walking the mailbox to fill that gap. Nightly stays unloaded. See [reports/maintain-living-loop.md](reports/maintain-living-loop.md).

## What is not done

- Maintain living loop: rematerialize scope is fixed and proven on this seed. Gmail April–July 2026 backfill is in progress (uncapped `gmail-messages` catch-up). Nightly stays unloaded.
- v3: stranger-ready install, Docker as the product, vault encryption UX, Linux wheels, `production_proven=true`.
- v4: native app, OAuth proxy, billing, signed connector feed.

## Where to read next

1. [README.md](../README.md) for humans.
2. This page for “what is true today.”
3. [PRODUCT_CAPABILITY_MATRIX.md](PRODUCT_CAPABILITY_MATRIX.md) for shipped / partial / empty / deferred, with a code path on every row.
4. `.cursor/skills/archive-query/` and [AGENT_USAGE.md](AGENT_USAGE.md) for how agents retrieve.
5. [v2.5vision.md](vision/v2.5vision.md) for the living-seed thesis and the ten-plan close. Older HEAD notes in that file stop at `5980464`.
6. [Maintain living loop](plans/maintain-living-loop.md) for the next ops increment.

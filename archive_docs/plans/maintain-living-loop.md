# Maintain living loop

**Status:** implementation-ready. This document does not run maintain, load nightly, bounce HTTP MCP, or touch the living seed.  
**Owner increment:** late v2.5 ops. Not v3. Not `production_proven`.  
**Branch when work starts:** a new `fix/maintain-living-loop` (or similar). Do not stack this on stranger-install work.  
**Living status:** [STATUS.md](../STATUS.md).

## What this is, in one page

You already have a real archive: real mail, real messages, real OpenAI embeddings, search that returns. That part is done.

What is not done is the daily loop a person expects:

> Overnight (or when I say so), pull what is new from the accounts I already connected, turn it into cards, enrich and embed only what changed, and make search see it. Do not re-pay for text that did not change. Tell me what happened.

`ppa maintain` is supposed to be that loop. Today the pieces exist, but the default command does not run them, the nightly job is off on purpose, and a rematerialize recently reminted embedding keys and orphaned paid vectors. Until that loop is trustworthy, unattended nightly stays off.

Search picking up a new generation does **not** require restarting Arnold. Publish flips `ACTIVE`. The already-running MCP remaps. Bounce HTTP only when you deploy new code.

### What you should see when this plan is done

1. One command, with apply, does the whole loop on this machine’s connected sources.
2. New mail, calendar, messages, Otter, files, Beeper, and GitHub that those updaters can see become cards.
3. New or changed cards get extracted, enriched, rematerialized, and embedded. Unchanged text reuses the vectors you already paid for.
4. The serving index publishes a complete generation. The next search on the warm MCP can see the new cards.
5. The report says, in plain numbers: pulled, written, embedded, reused, published generation, still pending, failed sources. Not a pile of skipped-step flags.
6. A supervised run on the living seed has done that once without orphaning embeddings or publishing a thin index. Only then do we talk about turning nightly back on.

### What this plan is not

- A new installer, Linux wheels, or “proven for the next person.” That is v3.
- Photos, Apple Health, or a full-mailbox catch-up walk.
- A full re-embed of the ~4M live vectors.
- Loading `com.rheeger.ppa.maintain-nightly` as the first step.
- Bouncing HTTP MCP as part of maintain.
- A second pipeline beside `ppa maintain`. Nightly, when it returns, stays a thin wrapper.

## Destination

`ppa maintain --apply` (name may stay the current flag pair until slice A lands) is the only user-facing loop for “bring the living archive up to date.”

It must, in order:

1. Pull incremental updates from every connected, non-parked source.
2. Run dirty-only processors: extract, enrich, rematerialize, embed, incremental links.
3. Attach leftover embeddings by chunk text when rematerialize rewrites rows but the text did not change.
4. Publish one complete serving generation for the eligible checkpoint this run built.
5. Write a human report that a person can read once.

Success is behavior you can see: a message that arrived yesterday is a card today, and search finds it, without a 4M re-embed and without a thin `ACTIVE`.

## Owner map (extend these)

| Concern             | Existing owner                                                                 | This plan changes                                                                        |
| ------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| User command        | `archive_cli/__main__.py` `maintain`, `archive_cli/commands/maintain.py`       | Default apply loop; fewer flags; one report                                              |
| Source pull         | `archive_sync/source_updaters/` (`default_maintain_source_keys`, runner)       | Default keys on the apply loop; parked stay parked; honest per-source failure            |
| Dirty processors    | `archive_sync/processors/`                                                     | Keep dirty-only. Prove extract / enrich / rematerialize / embed / link on one path       |
| Embed + reuse       | `archive_cli/embedder.py` `reuse_embeddings_by_content`, processor `embedding` | Required after rematerialize. Content hash stays the key. Schema version is not identity |
| Publish             | `archive_engine/publication.py`, `maintain._publish_serving_index`             | Publish the eligible checkpoint. Incomplete export cannot become `ACTIVE`                |
| Nightly wrapper     | `archive_scripts/ppa-maintain-nightly.py`                                      | Last. Same command, not a second pipeline. Stays unloaded until slice H says so          |
| Query after publish | Warm MCP `get_serving_handle`                                                  | No bounce. Remap on new `ACTIVE`                                                         |

## What not to invent

- A second nightly script that calls `embed-pending` and `rebuild-indexes` by hand.
- `PPA_EMBED_DEFER_VECTOR_INDEX=1` on incremental embed.
- 100k `ANY()` allowlists (Docker `/dev/shm` is 64MB).
- Full rebuild, `--allow-full-embedding`, `--catch-up`, Photos, Apple Health.
- Re-embedding the live 4M corpus to “make sure.”
- Cold `ppa person/query/search` against the living index.
- Killing HTTP MCP as part of this loop.
- `launchctl load` of nightly before slice H.
- A new `production_proven=true` claim.

## Locked product decisions

| Area                                                         | Decision                                                                   |
| ------------------------------------------------------------ | -------------------------------------------------------------------------- |
| Stranger install / Linux / formal proven-for-the-next-person | v3. Out of this plan.                                                      |
| Real mailbox + OpenAI on this seed                           | Already done. Do not redo as a gate.                                       |
| Nightly LaunchAgent                                          | Stays unloaded until a supervised living-seed run proves attach + publish. |
| HTTP MCP bounce                                              | Code deploy only. Not maintain.                                            |
| Chunk identity                                               | Text hash. Not schema version. PR 33 stays.                                |
| Thin `ACTIVE`                                                | Never. Incomplete publish fail-closes.                                     |
| Parked sources                                               | Photos, Apple Health, `--catch-up` stay parked.                            |

## Grounded problems

| ID  | What a person notices                                                                        | Required close                                                                                                                              | Slice |
| --- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ----- |
| M01 | Bare `ppa maintain` does not pull mail or refresh search. You need four flags and a wrapper. | One apply command runs pull → process → publish. Dry-run stays safe.                                                                        | A     |
| M02 | Nightly is off because rematerialize reminted keys and search lost the fat index.            | After rematerialize, reuse leftover vectors by content hash, embed only leftovers, then publish. Prove reuse count > 0 on a fixture remint. | D, E  |
| M03 | You cannot tell if last night worked.                                                        | One report: pulled, written, enriched, embedded, reused, generation id, pending, failed sources.                                            | F     |
| M04 | A failed Gmail walk can look like success.                                                   | Per-source status. Partial failure is visible. `--strict` still exists. Default report does not say “ok” if a live source failed.           | B, F  |
| M05 | Enrichment silently skips when the model env is unset.                                       | Apply loop requires the configured enrichment/embed providers or fails with a one-line reason.                                              | A, C  |
| M06 | Search can stay on an old generation after cards were written.                               | Publish is part of the apply loop. Skip only when nothing changed. Publish failure fails the run.                                           | E     |
| M07 | Unattended 2am is still the thing that can hang or republish 25GB.                           | Supervised living-seed run first (slice H). Nightly wrapper last, same command.                                                             | H     |

PRs 25–28, 33 already own pieces of M02/M06. This plan proves they work together as the user loop. Do not re-implement those PRs.

## Slices

### A — One apply command

**Depends on:** nothing.  
**Does:** Define the user contract. `ppa maintain --apply` (or keep today’s flag pair and make `--apply` imply source + processor apply). Dry-run prints the same steps with no writes. Help text describes the loop in the language above, not “tail ingestion ledger.”  
**Does not:** Change updater or embed internals.  
**Proof:** CLI help + a fixture dry-run that lists pull, process, publish.

### B — Pull what is new

**Depends on:** A.  
**Does:** The apply loop calls the existing source-updater runner with `default_maintain_source_keys` plus this machine’s Google account expansion. Parked sources stay out. Cursors move only after durable writes.  
**Does not:** Full-mailbox walk. New adapters.  
**Proof:** Isolated fixture updater writes cards and dirty UIDs. A failed source is a named error, not a quiet skip.

### C — Extract and enrich dirty cards

**Depends on:** B.  
**Does:** Processor DAG on those dirty UIDs: typed extract, thread enrichment when the provider is up. No broad LLM unless `--allow-broad-llm`.  
**Does not:** Re-enrich the whole vault.  
**Proof:** Fixture dirty email becomes a derived card and/or enrichment row. Missing provider fails the apply loop with a readable reason (M05).

### D — Rematerialize and reuse embeddings

**Depends on:** C.  
**Does:** Incremental rematerialize on dirty UIDs. Then content-hash attach of leftover embeddings (`reuse_embeddings_by_content`). Then `embed_pending` only for keys still missing. Schema version must not change the key.  
**Does not:** Full embed. Defer-vector-index on incremental.  
**Proof:** Fixture rematerialize that changes schema metadata but not text reuses every vector. A second run embeds 0. Living-seed supervised run (H) must show reuse, not a 4M backlog.

### E — Publish a complete generation

**Depends on:** D.  
**Does:** `publication.publish(eligible_checkpoint)` from this run’s UIDs. Incomplete export cannot become `ACTIVE`. Skip only when the run wrote nothing.  
**Does not:** Force-full serving rebuild. HTTP bounce.  
**Proof:** Isolated publish; query on that process sees the new UID. Warm-handle remap test (generation id changes, no process restart).

### F — Human report

**Depends on:** A. Can land in parallel with B–E as the report shape, then fill counts.  
**Does:** JSON + a short human summary: sources, cards in, extracted, enriched, embedded, reused, published generation, pending, errors.  
**Does not:** A second status product.  
**Proof:** Fixture report contains those fields. A failed source makes the human summary say so.

### G — Isolated loop proof

**Depends on:** A–F.  
**Does:** One acceptance scenario: fixture source write → extract/enrich → rematerialize → reuse/embed → publish → search/read the new UID. Hash embeddings are fine here.  
**Does not:** Touch the living seed.  
**Proof:** `--suite` scenario or focused pytest, `--require-integration`.

### H — Supervised living-seed run, then nightly decision

**Depends on:** G, and an explicit go from you.  
**Does:** One `ppa --log-file logs/ppa-maintain-supervised-YYYYMMDD.log maintain --apply` (final flags per A) against the living seed. Watch the log. Confirm: no thin `ACTIVE`, reuse or zero unexpected pending, warm MCP search finds a card that arrived in the pull.  
**Does not:** `launchctl load` unless you say so after that receipt.  
**Proof:** Redacted receipt appended to `archive_docs/reports/maintain-living-loop.md` (create when H starts). Nightly wrapper change, if any, is a follow-up commit after that receipt.

## G0 checklist (before any code)

1. `git status --short` and `git rev-parse HEAD`. Tree clean or only this plan.
2. Confirm nightly LaunchAgent is still unloaded. Do not load it.
3. Do not query the living archive with a new `archive_cli` process.
4. Do not start `rebuild-indexes`, `embed-pending`, or a second `serve --http`.
5. Create `logs/plans/maintain-living-loop/` when implementation starts, not now.
6. Read `archive_cli/commands/maintain.py`, `archive_sync/processors/runner.py`, `archive_cli/embedder.py` (`reuse_embeddings_by_content`), `archive_engine/publication.py`.

## Stop conditions

Stop and ask if:

- A run would re-embed the live corpus or publish a generation with far fewer embeddings than `ACTIVE`.
- Attach leftover would toast-insert/delete millions of rows.
- Nightly would be loaded to “test it.”
- HTTP MCP would be killed as part of a maintain test.
- Photos, Health, or `--catch-up` look “helpful.”
- Someone wants `production_proven=true` because maintain worked once.

## Definition of done

- Slices A–G merged with isolated proof.
- Slice H run once on the living seed, receipt written, you decide about nightly.
- STATUS.md updated: maintain loop shipped or “supervised only.”
- `production_proven` still false. That flag is a v3 claim.

## Suggested commit subjects

```text
maintain A: one apply command for the living loop
maintain B: pull connected sources on apply
maintain C: dirty extract and enrich on apply
maintain D: rematerialize then reuse embeddings by content
maintain E: publish complete generation from this run
maintain F: human maintain report
maintain G: isolated living-loop acceptance
maintain H: supervised seed receipt (after owner go)
```

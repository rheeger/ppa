# Maintain living loop — supervised receipt

**Started:** 2026-09-09 17:54 local. Slice H. Nightly still unloaded. HTTP MCP not bounced.  
**Stopped:** 2026-09-09 18:46 local. SIGTERM during processor classify, before rematerialize, embed, or publish.  
**Branch:** `fix/maintain-living-loop`.  
**Command:** `ppa --log-file logs/ppa-maintain-supervised-20260909.log maintain --apply`  
**Vault:** living seed `hf-archives-seed-20260307-235127`, schema `ppa`.  
**Log:** `logs/ppa-maintain-supervised-20260909.log`

## Before

- Serving generation: `1788920983071` (ready, dirty records 0)
- Chunks: 4,374,692
- Embedded: 4,853,894 (`text-embedding-3-small` v1)
- Pending: 23
- HTTP MCP: `com.rheeger.ppa.mcp-http` running
- Nightly LaunchAgent: unloaded

## Pull (completed, all 9 sources success)

| Source | Created | Merged | Dirty |
| ------ | ------- | ------ | ----- |
| gmail-messages | 116 | 0 | 116 |
| calendar-events | 65 | 0 | 65 |
| otter-transcripts | 0 | 0 | 0 |
| gmail-correspondents | 3 | 40 | 43 |
| imessage:local | 0 | 120 | 120 |
| file-libraries:documents | 12 | 356 | 368 |
| beeper:local | 158 | 0 | 158 |
| contacts:google | 0 | 594 | 583 |
| github-history:local | 853 | 36 | 889 |

No Photos, no Apple Health source, no `--catch-up`. File libraries was documents only (about 35 minutes). Vault-cache rebuild after the pull was incremental: 1,841 notes rebuilt, 1,397,997 unchanged.

File hygiene: junk purge 0. Duplicate links 83 (80 more dirty UIDs). Leftover serving-index dirty after hygiene: 2,405.

Example card written this pull and not published: `hfa-email-message-c264bc54d171`.

## Process (stopped)

The apply loop then planned processors against **1,391,923** cards.

```
processor plan built inputs=1391923 dirty=1389586 stale=5880259
skipped=200293 materialize_uids=1389586
processor execute classify start items=6080552
```

That is a full rematerialize of the living vault, not the ~2,400 dirty UIDs from this pull.

Likely cause: `ppa.meta` has no `last_maintenance_at` row. `_tail_ingestion_log` then reads the whole warehouse log (`ppa.ingestion_log` has 2,773,424 rows) and unions those UIDs into `scheduler_uids`. Isolated G never had this log.

The run was stopped during classify. There is no rematerialize, no `embed_pending`, no publish in the log.

## After

- Serving generation: still `1788920983071` (warm MCP `archive_status_json`, ready)
- Chunks: still 4,374,692
- New cards are on disk. Search still serves the generation from before this run.
- Pending embeddings: not re-checked after stop (was 23 before; no embed ran)
- Failed sources: none
- HTTP MCP: still running. Not bounced.
- Nightly LaunchAgent: still unloaded
- `production_proven`: still false

Warm MCP search for a card from this pull was not done. Publish never ran.

## Result

H was incomplete. Pull worked. Process tried to rematerialize the whole vault. Nightly stays off.

## Follow-up (same day)

The apply loop no longer unions the warehouse `ingestion_log` into processor dirty UIDs when `last_maintenance_at` is empty, and `--apply` never uses that ledger as the rematerialize set.

A later `maintain --apply --source-updater gmail-messages:rheeger@gmail.com --catch-up` rematerialized `scheduled=2516` (not 1.39 million) and published generation `1788997214192` (delta, parent `1788920983071`). Result: ok.

Gmail on the published index before that run:

| Month | Served Gmail |
| ----- | ------------ |
| 2026-02 | 1770 |
| 2026-03 | 783 |
| 2026-04 | 0 (no `Email/2026-04` on disk) |
| 2026-05 | 3 |
| 2026-06 | 11 |
| 2026-07 | 4 |
| 2026-08 | 114 |
| 2026-09 (1–10) | 645 |

Catch-up was also capped at 100 messages by the adapter default, so one run could not fill April–July. Catch-up now uncaps threads, messages, and attachments unless `--max-items` is set. An uncapped Gmail-only walk is running (`logs/ppa-maintain-gmail-catchup-uncapped-20260909.log`).

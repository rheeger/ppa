# Scale slice ladder proof

Run: `20260824-scale`
Engine: `PPA_ENGINE=rust` (`archive_crate` rebuilt into the 3.12 venv)
Role: `PPA_ARCHIVE_INSTANCE_ROLE=slice`
Interpreter: `/Users/rheeger/Code/rheeger/ppa-wt-track-a/.venv/bin/python`

No full-seed copy. No canonical-seed apply. 5pct vault was missing; per operator, skipped 5pct and jumped to 10pct (timed).

## Ladder

| Slice | Cache | Census dry-run | Hygiene apply | Hygiene rollback |
| ----- | ----- | -------------- | ------------- | ---------------- |
| 1pct (existing) | rust hit, 138007 notes, **2.16s** | **2.49s**, 9011 threads | **~1.5s**, 9011 threads / 43804 cards | **~0.5s**, 43804 restored |
| 5pct | skipped — vault missing | — | — | — |
| 10pct (generated this run) | rust hit, 539299 notes, **4.45s** | **15.96s**, 75288 threads | **8.47s**, 75288 threads / 243598 cards | **0.50s**, 243598 restored |

### 1pct (`archive_test_slice`)

- Vault: `.slices/1pct` (already present; not regenerated)
- Census `decision_run_id`: `gate-dd58ff8237de`
- Corpus counts: active 3475, quarantine 3552, suppressed 1984
- Classification sources: stage0 4260, missing 4751, new LLM 0
- Apply/rollback instance: `slice:archive_test_slice@127.0.0.1:49624/archive@1pct`
- Markdown deleted: no

### 10pct (`archive_test_slice_10pct`)

- Generated this run from seed cache (not a full-seed copy). `slice-seed` wall **7:41** / `real 463.57s`
  - cache load 18.1s (seed tier-2 hit, 1,882,463 notes)
  - closure 539,299 cards (75,288 email_thread / 168,310 email_message)
  - copy 4:47 @ ~1900 files/s
  - output tier-2 cache build **0:54** rust, 2709 MB
- Census `decision_run_id`: `gate-1e6c315ba34a`
- Corpus counts: active 25840, quarantine 33327, suppressed 16121
- Classification sources: stage0 34496, missing 40792, new LLM 0
- Apply/rollback instance: `slice:archive_test_slice_10pct@127.0.0.1:49624/archive@10pct`
- Markdown deleted: no
- No walk / rglob / per-row apply warnings in this run’s logs

## Offline Gmail + Calendar (no live API)

| Vault | Presence threads | Message hashes | Attachment hashes | Calendar rows | Invite ical (msg/thread) | Events | Wall |
| ----- | ---------------- | -------------- | ----------------- | ------------- | ------------------------ | ------ | ---- |
| 1pct | 9011 | 34793 | 18197 | 44219 | 931 / 848 | 374 | ~16s |
| 10pct | 75288 | 168310 | 59073 | 246568 | 5503 / 5272 | 2758 | **32.23s** (presence 13.2s, quick 9.3s, calendar dump 7.9s, index 0.66s) |

## Dirty extract (1pct, 8 UIDs)

- Allowlist resolved via vault-cache IN query
- `iter_parsed_notes_for_card_types` calls: **0**
- scanned=8 matched=0 extracted=0 (dry-run; none matched a registered extractor)
- elapsed **0.332s**

## Scale commits proven

- `0c53d7e` bulk corpus apply writes (COPY)
- `f339580` census from rust cache iter
- `0bdbd48` gmail presence via vault cache
- `9139c58` calendar indexes from cache
- `35901b0` dirty-only extract scan

## Stop conditions

- No hang, no markdown-walk log line, no per-row apply at 10pct
- Did not run full seed

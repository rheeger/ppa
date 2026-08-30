# Calendar 5b re-proof (Track C)

**Verdict:** live staging apply **PASS** (plus fixture proof)

## What was wrong

Gate 5b minted with `CALENDAR_READONLY_SCOPES` → OAuth HTTP 400 `invalid_scope`.
Local refresh grant has `calendar`, not `calendar.readonly`.

`0a600e4` switched the adapter to `services=["calendar"]`. Remaining holes closed here:

- mint retries once **without** `scope` on HTTP 400 / `invalid_scope` (not `invalid_grant`)
- removed the dead/harmful readonly fallback
- `Token refresh failed` / `invalid_scope` classify as **blocked**
- `_gws` no longer dies if mint fails before HTTP fallback

## Fixture proof

`pytest` 27 passed:

- `archive_tests/archive_auth/test_google_cli_token_manager.py`
- `archive_tests/archive_sync/test_calendar_events_adapter.py`
- `archive_tests/source_updaters/test_source_updater_execution.py`

Includes real `CalendarEventsAdapter` apply with `_list_events` stubbed (no live OAuth).

## Live staging apply

Prior `/tmp/ppa-seed-staging/hf-archives-seed-staging` was gone. Recreated a **minimal** vault at that path (not a canonical-seed copy). Schema/DSN **not** attached (vault-only). Role `seed-staging`.

| Step | Result |
| --- | --- |
| Mint `services=["calendar"]` | ok (non-interactive refresh) |
| Dry-run `--max-items 3` | success · observed=3 · dirty=3 · cursor unchanged |
| Apply `--max-items 3` | success · run `su-ab5714fd0e60` · promoted=3 · 3 cards · cursor advanced (`last_sync`, `page_token`, `emitted_events=3`) |

Canonical seed `/Users/rheeger/Archive/seed/hf-archives-seed-20260307-235127` **not written**.
`_meta/sync-state.json` SHA `ab2157c1…508d6010` unchanged before/after.

## Caveats

- Live proof used a **fresh minimal** staging vault, not the Jul 12 seed clone (that tree is gone).
- No `archive_seed_staging` Postgres writes this run (DSN unset).
- Arnold not contacted (`arnoldlib` absent locally).
- Uncapped Calendar apply not run.

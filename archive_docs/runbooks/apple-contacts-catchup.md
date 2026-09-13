# Apple Contacts living-vault catch-up

Nightly `contacts:apple` only sees contacts that changed since the last cursor. The first apply against the living seed is a one-shot catch-up. Later nights stay incremental.

Do this only on the living vault. One writer. Detach every apply with `setsid` and `--log-file` as in [`.cursor/skills/long-running-jobs/SKILL.md`](../../.cursor/skills/long-running-jobs/SKILL.md). Do not start a second maintain, rebuild, or identity-repair against the same vault. Do not kill the warm MCP. Do not ask living-archive questions with a new `archive_cli` process.

`contacts:apple` dumps unified Contacts.app people through `osascript`. The Python/`osascript` process, including the launchd GUI agent, needs Automation permission for Contacts. That is the same class of macOS grant as iMessage Full Disk Access. A permission failure is `blocked`. It does not fall through to a stale Downloads VCF.

Hand-dropped VCF stays `contacts:vcf` and is not executable.

## Sequence

1. Unit and adapter tests green. Vault scan cache present. No other writer. Contacts Automation granted.
2. Baseline census and discoverability snapshot:

```bash
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-baseline.log \
  identity-repair census --output logs/apple-contacts-baseline-census.json
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-baseline.log \
  identity-repair discoverability --output logs/apple-contacts-baseline-discoverability.json
```

3. Dry-run Apple only. Do not pull Gmail, iMessage, or Beeper.

```bash
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-dry.log \
  maintain --source-updater contacts:apple
```

Read created / merged / review / skipped. Stop if creates look like companies, one-token nicknames, or a second card for people who already exist from an old VCF seed.

4. Human review of `_meta/identity-proposals.json`. Accept, reject, or leave open. Safe exact matches are not in this pile. Do not auto-apply the queue.

5. Apply Apple, detached:

```bash
setsid .venv/bin/python -m archive_cli --log-file logs/apple-contacts-apply.log \
  maintain --apply --source-updater contacts:apple \
  </dev/null >logs/apple-contacts-apply.stdio 2>&1 &
```

Confirm PPID 1. Watch the log. Auto-merge is exact canon email or phone plus a safe name only.

6. Apply accepted reviews:

```bash
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-reviews.log \
  identity-repair apply-reviews
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-reviews.log \
  identity-repair apply-reviews --apply
```

Rejected and open rows stay untouched.

7. Historical identity repair, dry-run then detached apply:

```bash
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-repair-dry.log \
  identity-repair canonicalize
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-repair-dry.log \
  identity-repair merge
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-repair-dry.log \
  identity-repair resolve-people
```

Then the same three commands with `--apply`, each detached, one writer at a time.

8. After census and discoverability snapshot. Compare `missing_people`, empty `people` lists by comms type, identity-map phone/email keys, match histogram from the Apple updater report, and review-queue counts. That pair of JSON files is the discoverability score.

```bash
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-after.log \
  identity-repair census --output logs/apple-contacts-after-census.json
.venv/bin/python -m archive_cli --log-file logs/apple-contacts-after.log \
  identity-repair discoverability --output logs/apple-contacts-after-discoverability.json
```

9. Dirty processors on rewritten UIDs through the normal `ppa maintain --apply` processor path. No Gmail `--catch-up`. No Photos.

10. Verify on the already-running MCP. `archive_person` for a few people who live in Apple Contacts. Confirm `source` includes `contacts.apple` and phones landed. Query an iMessage or Beeper thread for one of them and confirm a single `people` wikilink.

11. Hand off to nightly. `contacts:apple` sits before iMessage and Beeper. Later nights skip unchanged Apple UIDs and keep queuing close matches for review.

## Done when

- Matched Apple contacts are on person cards with `contacts.apple` in `source`.
- The identity map has those phones and emails (canon plus alias forms).
- Eligible stubs are redirected. Fuzzy, close, and household-phone pairs stay in identity-proposals until a human accepts.
- Historical threads whose handles now resolve have one canonical `people:` link. Handles stay on the thread.
- `contacts.apple` has a cursor, so the next nightly is a hash skip.

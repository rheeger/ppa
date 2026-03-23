# archive-sync

Archive import layer for HFA.

## Contract

Every adapter follows the shared `hfa` ingest contract:

- `fetch()` extracts raw source rows
- `to_card()` returns `(card, provenance, body)`
- `ingest()` in `adapters/base.py` owns identity resolution, merge/create/conflict flow, atomic writes, and cursor updates

Adapters should not write markdown directly.

## Commands

- `python skills/archive-sync/handler.py contacts`
- `python skills/archive-sync/handler.py contacts-import`
- `python skills/archive-sync/handler.py linkedin --csv-path ~/Downloads/Connections.csv`
- `python skills/archive-sync/handler.py notion-people --csv-path ~/Downloads/notion-people.csv`
- `python skills/archive-sync/handler.py copilot-finance --csv-path ~/Downloads/copilot-transactions.csv`
- `python skills/archive-sync/handler.py gmail-correspondents --account-email rheeger@gmail.com --max-messages 5000`
- `python scripts/hfa-imessage-snapshot.py --output-dir ~/Archive/imessage-snapshots/latest`
- `python skills/archive-sync/handler.py imessage --snapshot-dir ~/Archive/imessage-snapshots/latest`
- `python skills/archive-sync/handler.py photos --source-label apple-photos --quick-update`

All commands support `--dry-run`.

## Canonical Import Order

1. Apple / VCF contacts
2. Google contacts
3. LinkedIn
4. Notion people
5. Gmail correspondents

This order seeds identity from stronger sources before weaker ones.

## Rules

- Keep person emails and phones as arrays.
- Use `deterministic_provenance()` or equivalent field-level provenance for every non-empty adapter field.
- Read filters and thresholds from `PPAConfig`.
- Use `_meta/own-emails.json` to exclude self aliases from Gmail correspondent extraction.
- The Photos adapter is read-only and uses `osxphotos`; private people and label extraction should stay optional.
- When adding a new adapter, follow `docs/hfa/PLAYBOOK.md`.
- After imports, rebuild the derived `archive-mcp` index if agents or tools rely on indexed retrieval.
- Treat the vault as canonical and any derived search/index layer as rebuildable.

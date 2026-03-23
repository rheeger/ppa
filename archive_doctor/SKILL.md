# archive-doctor

Maintenance layer for HFA vault quality.

## Commands

- `python skills/archive-doctor/handler.py dedup-sweep`
- `python skills/archive-doctor/handler.py validate`
- `python skills/archive-doctor/handler.py stats`
- `python skills/archive-doctor/handler.py purge-source --source <source>`

## When To Run It

- After imports: run `dedup-sweep`, `validate`, and `stats`
- After imports that affect agent retrieval: rebuild the derived `archive-mcp` index after doctor checks pass
- After schema or provenance changes: run `validate`
- Before wiping or re-importing a source: use `purge-source`
- For routine health checks: run `stats`

## Command Intent

- `dedup-sweep`: find likely duplicate people using identity buckets and weighted scoring
- `validate`: write a vault-wide validation report covering schema and provenance
- `stats`: print vault health and quality summaries
- `purge-source`: remove cards from one source and clean sync/identity state

Use `docs/hfa/ARCHITECTURE.md`, `docs/hfa/INDEXING.md`, and `docs/hfa/PLAYBOOK.md` for the system model, index lifecycle, and extension rules.

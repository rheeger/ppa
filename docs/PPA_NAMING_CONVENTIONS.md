# PPA Naming Conventions

> **Status**: Locked as of Phase 2 (2026-03-23).
> PPA code reads `PPA_*` env vars exclusively. Integration layers (e.g. the hey-arnold Makefile) translate their own variable names to `PPA_*` when invoking PPA subprocesses.

## Product Name

**PPA** (Personal Private Archives) — a semantic memory engine that indexes a personal digital life. Markdown vault as canonical truth, Postgres as derived index, MCP as query interface.

## Engine vs. Instance

| Layer    | Name                           | Scope                                                                  |
| -------- | ------------------------------ | ---------------------------------------------------------------------- |
| Engine   | PPA                            | Universal protocol, MCP server, index, projections, retrieval pipeline |
| Instance | HFA (Heeger-Friedman Archives) | One family's vault, seed material, instance configuration              |

## Repo and Package Names

| Artifact              | Canonical        | Current (transitional)                   |
| --------------------- | ---------------- | ---------------------------------------- |
| MCP server repo       | `ppa`            | `ppa` (split complete)                   |
| Source sync module    | `ppa-sync`       | `archive_sync` (in `ppa/`)              |
| Vault repair module   | `ppa-doctor`     | `archive_doctor` (in `ppa/`)            |
| Shared schema library | `ppa-core`       | `hfa` (in `ppa/`)                       |
| Arnold integration    | `hey-arnold`     | `hey-arnold` (thin consumer after split) |

## Python Import Paths

Frozen during the transition. Renamed to `ppa` namespace after the split is confirmed stable.

| Package          | Role                                 | Future name  |
| ---------------- | ------------------------------------ | ------------ |
| `archive_mcp`    | MCP server, index, retrieval         | `ppa`        |
| `hfa`            | Shared schema, vault I/O, provenance | `ppa_core`   |
| `archive_sync`   | Source adapters                      | `ppa_sync`   |
| `archive_doctor` | Vault validation and repair          | `ppa_doctor` |

## Environment Variables

Canonical prefix: `PPA_`. No aliases in PPA code. Integration layers (e.g. the hey-arnold Makefile) are responsible for translation.

See [PPA_RUNTIME_CONTRACT.md](PPA_RUNTIME_CONTRACT.md) for the full env contract.

## Service and Timer Names

Canonical prefix: `ppa-`. Systemd unit renames are complete.

| Canonical                   | Status    |
| --------------------------- | --------- |
| `ppa-mcp.service`           | Deployed  |
| `ppa-postgres.service`      | Deployed  |
| `ppa-health-audit.service`  | Planned   |
| `ppa-index-refresh.service` | Planned   |
| `ppa-embed-pending.service` | Planned   |
| `ppa-sync@.service`         | Planned   |
| `ppa-sync@.timer`           | Planned   |

Instance timer examples: `ppa-sync@gmail-messages.timer`, `ppa-sync@calendar-events.timer`.

## Job and State Naming

| Concern          | Pattern                                      |
| ---------------- | -------------------------------------------- |
| Job family       | `ppa-sync-`                                  |
| State root       | `state/ppa/`                                 |
| Per-source state | `state/ppa/sources/<source_id>.json`         |
| Run ledger       | `state/ppa/runs/<job_name>/<timestamp>.json` |
| Lock files       | `state/ppa/locks/<job_name>.lock`            |

## Documentation Naming

| Scope             | Pattern              |
| ----------------- | -------------------- |
| Architecture docs | `PPA_*.md`           |
| Runbooks          | `ppa-*.md`           |
| Arnold-only docs  | `arnold-ppa-*.md`    |
| Migration docs    | `ppa-migration-*.md` |

## Make Targets

Canonical prefix: `ppa-`. Rename from `hfa-archive-*` is complete.

Safe targets (no Python invocation on Arnold):

- `make ppa-health`
- `make ppa-pg-backup`
- `make ppa-mcp-status`

## CLI Commands

The canonical CLI entrypoint remains `python -m archive_mcp` during transition, evolving to `ppa` after the split.

| Command      | Action                       |
| ------------ | ---------------------------- |
| `ppa search` | Semantic/lexical search      |
| `ppa trace`  | Graph edge traversal         |
| `ppa recall` | Read a specific card by UID  |
| `ppa status` | Health and embedding backlog |

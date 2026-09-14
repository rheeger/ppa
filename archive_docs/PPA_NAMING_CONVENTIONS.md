# PPA names and terminology

PPA means Personal Private Archives. It is the software in this repository. An archive instance is the collection and configuration that a person or organization operates.

Use these names consistently so a reader can tell which software, records, or running instance a document describes.

## Product terms

| Term | Meaning |
| --- | --- |
| PPA | The archive software, including import, processing, query, and maintenance |
| Archive instance | One root, archive identity, configuration, warehouse schema, and serving state |
| Vault | Canonical Markdown cards and associated files |
| Card | A typed record with structured fields, optional body, and field provenance |
| Catalog | Searchable fields, passages, and relationships prepared for queries across sources |
| Serving index | The published Rust index used for live retrieval |
| Warehouse | Postgres data derived from the vault for materialization, embedding work, and supported analytics |
| Connector or adapter | Code that brings source records into an archive |
| Extractor | Code that derives a typed record from source material, such as a flight from an email |
| Linker | Code that proposes relationships between records |

HFA means Heeger-Friedman Archives, the maintainer's archive instance. Older reports also mention Arnold and Ginger, names of machines in that deployment. None is a required host name, path, or instance identity for PPA.

## Packages and entrypoints

Current packages are `archive_vault`, `archive_sync`, `archive_cli`, `archive_engine`, `archive_crate`, `archive_doctor`, and `archive_auth`. Use those names when pointing to code. Earlier `hfa` and `archive_mcp` package names in historical plans are not current import paths.

The command line is `ppa`, with `python -m archive_cli` as the module entrypoint. MCP tools use the `archive_` prefix. Examples include `ppa read` / `archive_read`, `ppa graph` / `archive_graph`, and `ppa analytics` / `archive_analytics`.

## Identifiers and configuration

Existing card UIDs use the `hfa-` prefix. Preserve those stable IDs even though the product is called PPA. Card types use underscores, such as `email_message` and `meal_order`.

New product configuration uses `PPA_*` environment variables and the instance configuration file. Historical launchers may translate older variable names. The [runtime contract](PPA_RUNTIME_CONTRACT.md) defines current precedence and supported settings.

## Documentation

Use the [documentation index](README.md) to place new guides. A general guide should work with an independent archive. Name any host or account assumption in an operational runbook, and date reports that describe a particular run.

Keep existing document paths stable when rewriting them so earlier links still work. Do not rename a code package, command, or UID to match a prose edit.

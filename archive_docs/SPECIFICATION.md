# PPA specification

PPA keeps imported service records in a local archive and makes them queryable together. Source references let a client check the evidence behind a result, while shared fields and relationships connect history across providers.

This specification defines the current engine's capabilities and limits. PPA remains in development; a general release and supported onboarding flow are not available yet.

## Sources

The archive accepts service connections, local libraries, and exports. An initial import can recover history that predates PPA. Incremental sources can then refresh changed records; export sources need a new export to add later activity.

| Source | Records | Update method |
| --- | --- | --- |
| Gmail | Messages, threads, attachments, correspondents | Incremental |
| Google Calendar | Events and participants | Incremental |
| Google Contacts, Apple Contacts | People and contact details | Incremental; VCF import also available |
| iMessage, Beeper | Messages, threads, attachments | Incremental from the configured local source |
| Otter | Meeting transcripts | Incremental |
| File libraries | Documents and extracted text | Incremental from configured directories |
| GitHub | Repositories, commits, issues, pull requests, comments | Incremental |
| Copilot finance | Transactions | CSV import |
| LinkedIn, Notion people exports | People and contact details | Export import |
| Apple Photos | Photo metadata and labels | Library import; ongoing refresh inactive |
| Apple Health | Health records | Export import; ongoing refresh inactive |
| Clinical records | Medical and vaccination records | FHIR, CCD, Epic EHI, and supported document imports |

Email extraction also produces typed purchases, deliveries, rides, flights, accommodation, and rental-car records from supported provider formats. Coverage includes Amazon, DoorDash, Uber, Instacart, Lyft, United, Airbnb, shipping, and rental-car emails. Coverage depends on the imported material and recognized formats.

## Archive records

Each record is a Markdown file with a stable identifier, record type, source, relevant dates, content, and relationships. Structured fields let clients query records without parsing each provider's original format. Field provenance records where a value came from. Associated attachments are retained where the import supports them.

The schema defines 36 record types across people, communication, events, documents, media, health, finance, travel, and code. Repeated imports use stable identities to update existing records. Provider and account identity keep unrelated source objects separate.

Relationships connect records through identifiers and corroborating fields, such as contact details, booking codes, amounts, and dates. Person lookup reports unresolved or ambiguous identities. Inferred relationships remain distinguishable from source-reported facts, and model enrichment cannot overwrite protected deterministic fields.

## Queries

The command line and Model Context Protocol (MCP) expose the same archive operations across imported sources. An agent can search across services, follow relationships, and combine records in a timeline or calculation without knowing each provider's search interface.

| Operation | Behavior |
| --- | --- |
| Read | Open a stored record by identifier or path, individually or in a batch |
| Search | Find keywords, semantic matches, or combined results; semantic search requires configured embeddings |
| Filter | Select records by type, source, person, organization, date, and supported typed fields |
| Resolve a person | Look up a name, email, or phone and report identity ambiguity |
| Follow relationships | Retrieve linked records within depth and result limits |
| Reconstruct context | Build timelines and retrieve neighboring messages or passages |
| Calculate | Count or sum the full eligible set of stored records; reconcile trip charges and refunds |
| Inspect subscription history | Report the last observed billing, renewal, or cancellation events and any conflicts |
| Inspect changes | Retrieve recorded archive changes since a checkpoint |

Results identify their source records. Analytical results report completeness, coverage, and freshness so a client can explain which records an answer includes. Trip costs separate currencies, preserve refunds, and distinguish supported charges from booking estimates. A complete result covers eligible stored records; activity that was never imported remains outside that coverage.

PPA retrieves evidence and performs defined calculations. The agent interprets the results and writes the answer.

## Updates and control

Maintenance processes changed records and publishes a complete search index. A failed publication leaves the previous index available. Nightly refresh requires a configured schedule and working source access; freshness is reported separately for each source.

Queries use the stored archive, so imported records remain available when the original account closes or the service shuts down. Later imports still require available source data. The owner can retain, copy, and move the readable records independently of PPA. Search indexes are rebuildable; recovery also needs the records and saved correction decisions.

The owner controls the archive files, connected sources, and client access. Retrieval restrictions apply to reads, search, counts, and related records. Separate archive instances keep distinct storage and identities.

Local storage does not imply local processing. Remote embedding or enrichment providers receive the text sent to them, and cloud agents receive returned records. PPA provides controls for supported provider requests and retrieval access. It does not provide built-in encryption at rest or isolate arbitrary code running under the owner's operating-system account.

The [architecture](ARCHITECTURE.md) describes the storage, processing, and retrieval technologies. Detailed engine contracts are indexed under [technical references](README.md#technical-references).

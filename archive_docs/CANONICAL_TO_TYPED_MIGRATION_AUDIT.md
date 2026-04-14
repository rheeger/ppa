# Canonical To Typed Migration Audit

This audit records expected rebuild readiness for the current canonical corpus as it materializes into the new typed projection layer.

## Readiness Status Legend

- `expected_clean_rebuild`: current canonical schema appears sufficient for direct rebuild into the typed table
- `needs_full_seed_verification`: schema support exists, but full-seed rebuild verification is still required to validate field completeness and source variance
- `likely_partial_canonical_coverage`: typed shape exists, but some optional source families may leave sparse columns until canonical imports are richer

## Current Assessment

| Card Type             | Typed Table            | Current Readiness                   | Notes                                                                                             |
| --------------------- | ---------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------- |
| `person`              | `people`               | `needs_full_seed_verification`      | multiple person-producing connectors exist; identity and social field density will vary by source |
| `finance`             | `finance_records`      | `needs_full_seed_verification`      | structure exists, but counterparty/entity richness depends on source adapter output               |
| `medical_record`      | `medical_records`      | `expected_clean_rebuild`            | canonical medical cards already carry strong deterministic structure                              |
| `vaccination`         | `vaccinations`         | `expected_clean_rebuild`            | canonical vaccination cards already carry strong deterministic structure                          |
| `email_thread`        | `email_threads`        | `needs_full_seed_verification`      | rebuild should work, but thread/message linkage density depends on import completeness            |
| `email_message`       | `email_messages`       | `needs_full_seed_verification`      | rebuild should work; attachment and invite-related fields may be sparse for some sources          |
| `email_attachment`    | `email_attachments`    | `needs_full_seed_verification`      | attachment metadata should materialize, but some historical archives may be incomplete            |
| `imessage_thread`     | `imessage_threads`     | `needs_full_seed_verification`      | structure exists, but participant metadata can vary by export quality                             |
| `imessage_message`    | `imessage_messages`    | `needs_full_seed_verification`      | reply/reaction fields may be sparse depending on source extraction depth                          |
| `imessage_attachment` | `imessage_attachments` | `likely_partial_canonical_coverage` | exported-path and metadata completeness may vary across snapshots                                 |
| `beeper_thread`       | `beeper_threads`       | `needs_full_seed_verification`      | bridge metadata should materialize, but real-world source variance still needs validation         |
| `beeper_message`      | `beeper_messages`      | `needs_full_seed_verification`      | reply/reaction richness depends on bridge/source fidelity                                         |
| `beeper_attachment`   | `beeper_attachments`   | `likely_partial_canonical_coverage` | cached-path and media-shape coverage may vary by source                                           |
| `calendar_event`      | `calendar_events`      | `expected_clean_rebuild`            | canonical event schema is already strong and relationally rich                                    |
| `media_asset`         | `media_assets`         | `needs_full_seed_verification`      | metadata completeness depends on Photos/private meta availability                                 |
| `document`            | `documents`            | `needs_full_seed_verification`      | extraction-dependent fields may be sparse where OCR or parsing was incomplete                     |
| `meeting_transcript`  | `meeting_transcripts`  | `needs_full_seed_verification`      | transcript/event linkage should rebuild, but participant metadata density may vary                |
| `git_repository`      | `git_repositories`     | `expected_clean_rebuild`            | repository identity fields are stable                                                             |
| `git_commit`          | `git_commits`          | `needs_full_seed_verification`      | branch and PR linkage fields may vary by extraction depth                                         |
| `git_thread`          | `git_threads`          | `needs_full_seed_verification`      | issue vs PR thread variance needs full-seed verification                                          |
| `git_message`         | `git_messages`         | `likely_partial_canonical_coverage` | review-comment metadata may be sparse for some message classes                                    |

## Required Post-Implementation Validation

The implementation is not fully closed until these are done on a real rebuilt schema:

1. run a clean bootstrap and rebuild into a fresh schema
2. inspect `projection_status()` output for every typed table
3. verify `canonical_ready_ratio` and `migration_notes` on full-seed data
4. record any newly discovered blockers back into this audit

## Amelia Epic EHI structured medical import

The `hey-arnold-hfa` archive-sync `medical-records` adapter (`--ehi-tables-dir-path`) now ingests patient-anchored Epic EHI TSV domains beyond medication orders:

- **Medications**: `ORDER_MED.tsv` + `CLARITY_MEDICATION.tsv`, enriched with `ORDER_MEDINFO.tsv` where present.
- **Immunizations**: `IMM_ADMIN.tsv` joined via `DOCS_RCVD.tsv` to `PAT_ID` (primary rich rows); `PAT_IMMUNIZATIONS.tsv` + `IMMUNE.tsv` + `IMMUNE_HISTORY.tsv` for registry linkage; vaccine **orders** from `ORDER_MED` are deduped when an `IMM_ADMIN` row matches the same calendar day and vaccine semantics.
- **Problems**: `PAT_PROBLEM_LIST.tsv` + `PROBLEM_LIST.tsv` + `PAT_ENC_DX.tsv` + `CLARITY_EDG.tsv` for diagnosis display text when `DX_LINK_PROB_ID` is populated.
- **Encounters**: `PAT_ENC.tsv` + department names from `CLARITY_DEP.tsv`; optional **ADT timeline** from `CLARITY_ADT.tsv` (`record_subtype=adt_event`).
- **Labs / results**: `ORDER_RESULTS.tsv` scoped through `ORDER_PROC.tsv` for the same patient (`record_type=observation`), skippable with `--ehi-no-order-results` for very large exports.

Cursor key for this path is `medical-records:epic-ehi` (distinct from FHIR One Medical imports). Full-seed rebuild verification for typed `medical_records` / `vaccinations` rows sourced from Epic EHI remains `needs_full_seed_verification` until a production seed run is recorded here.

## Expected Migration Behavior

- no legacy conversion step should mutate canonical markdown just to satisfy typed tables
- rebuild is the primary migration path
- if canonical structure is missing for a field, the typed row should surface that through `canonical_ready` and `migration_notes`
- only if rebuild exposes a genuine canonical data gap should follow-up canonical remediation be planned

# Processor execution validation (P03-D)

One `maintain` path now journals source work, runs the processor DAG, and
publishes only the eligible checkpoint through
`archive_engine.publication.publish(eligible_checkpoint, context)`.
The cards a later query uses are the ones this run built.

## Path

1. Source updaters persist cards and emit journal records. Cursors move only
   after durable persist (`commit_cursor_after_persisted`). Dry-run does not.
2. Legacy ingestion-log / leftover DIRTY UIDs are extra dirty inputs to the
   scheduler. Dirty emails without a corpus decision default to
   `processor_decision=typed_extraction` so one maintain command still creates
   derived cards (the old auto-extract path). There is no second extract /
   entity-resolution / rebuild chain after the DAG.
3. Processor receipts are the count source (`cards_extracted`,
   `entities_resolved`, `cards_rebuilt`).
4. Processor writes invalidate the vault scan cache so derived cards are
   visible to the next materialize/query (`mark_vault_written` + incremental
   rebuild). `publish()` then receives the contiguous eligible checkpoint.
   Failed required revisions and pending gaps are excluded. A failed publish
   does not advance the served watermark.
5. Query (warehouse / native serving / MCP) reads the published generation.

## Watermarks

These are three different numbers. Do not collapse them into
`last_maintenance_at`.

| Name | Meaning |
| --- | --- |
| `journal_watermark` | Contiguous committed journal prefix |
| `materialized_watermark` | `warehouse` consumer cursor |
| `published_watermark` | `publication` consumer / `PublicationReceipt.acked_watermark` |
| per-source `source_cursors` | Adapter catch-up only; not globally served freshness |

A no-op maintain keeps the last good generation. Dry-run leaves all three
watermarks and every source cursor untouched.

## CLI compatibility

| Entry | Execution model | Dry-run |
| --- | --- | --- |
| `ppa maintain` | sources → journal → processors → `publish()` | read-only |
| `ppa processors run --apply` | DAG, then the same `publish()` helper | plan only, no publish |
| `ppa source-updaters run --apply` | persist + cursor after persist; exposes `source_cursors` separately from `served_freshness` | no persist, no cursor move |

## Evidence

Acceptance `p03.maintain_revision_pipeline` (run `maintain-20260906T053916090084Z`):

| Watermark | Value |
| --- | --- |
| journal | 18 |
| materialized (`warehouse` cursor) | 17 |
| published (`PublicationReceipt.acked_watermark`) | 18 |

Dry-run left journal and published unchanged. Failed required revisions stay
out of the eligible checkpoint. Canonical / warehouse / native / MCP all
returned the P03-D purchase (`hfa-purchase-0e9d4cd7ae7b`) and the P03-C
purchase (`hfa-purchase-3e8ce8f2d53a`). Receipt counts: extracted=2,
resolved=11, rebuilt=13.

Pytest: 109 passed (`test_maintain.py`, `test_maintain_incremental.py`,
`test_maintain_revision_pipeline.py`, `archive_tests/source_updaters/`).
Acceptance suite `p03`: 4 passed.

This unblocks P07-C (revision receipts as compiler output) and P08-B
(connector batches at the durable source boundary).

# Publication validation (P02-D)

Writer routes now either emit a journaled `ChangeRecord` or call
`request_reconciliation` with a bounded UID map. P03 publishes through
`archive_engine.publication.publish(eligible_checkpoint, context) -> PublicationReceipt`.
`commands/maintain.py` is unchanged.

## Writer → journal → consumer → generation

| Route | Emission | Consumer | Reaches `publish()` |
| --- | --- | --- | --- |
| Source / adapter `write_card` | `create` / `update` | publication | yes |
| Manual file edit | `request_reconciliation(uid_to_rel)` → `update`/`delete` | publication | yes |
| Enrichment vault write | `write_card` + `mutation_context(source=card_enrichment)` | publication | yes |
| Warehouse materialization | `acknowledge_materialized` | warehouse (independent) | yes |
| Embedding-only completion | `OPERATION_EMBED` | publication / vectors | yes |
| Batch embed ingest | bounded `request_reconciliation` | publication | yes |
| Hygiene vault-remove | `delete_card` (`OPERATION_DELETE`) | publication | yes |
| Rebuild | warehouse ack + DIRTY/`legacy_dirty` | warehouse + publication | yes |

Unknown dirty UIDs are not skipped: empty-UID hygiene unlinks request
reconciliation; embed without card UIDs does the same.

## Receipt example

```python
from archive_engine.publication import publish

receipt = publish(
    eligible_checkpoint,
    {
        "vault": vault,
        "store": store,          # or "snapshot": ServingSnapshot(...)
        "mode": "incremental",
        "parent_generation": active,
    },
)
# receipt.generation_id, acked_watermark, unresolved_gaps, ok, error
```

## Small change vs compaction

Incremental publish writes a delta segment (dirty UIDs + tombstones + new
vectors). Compaction is `mode=compact` / `force_compact=True` and rebuilds a
self-contained generation. Delta bytes stay below compact bytes for the P02
fixture (3 live cards).

## Legacy DIRTY / rollback

`mark_serving_index_dirty` still appends `DIRTY` and imports `legacy_dirty`
when the UID is new to the journal. Removing migration `009` drops only the
warehouse cursor table. Do not delete committed mutation rows to undo a card;
write a new delete or update.

Recovery point: durable committed mutations are not lost. An interrupted
uncommitted source fetch may be replayed from its previous source cursor.

## Named consumers

Publication ack never advances warehouse, vectors, graph, seed-link,
enrichment, or claims. Those workers can acknowledge independently.

## Scale note

Journal emission is per completed UID/batch, not a vault walk. Reconciliation
is bounded to prepared rows plus a caller-supplied UID map. Publication still
uses the Rust generation builder and COPY-backed warehouse load.

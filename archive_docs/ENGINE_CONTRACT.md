# Engine contract (P06-A)

This is the frozen shared-record and exact-read contract. It does not claim
query, publication, processor, or recovery behavior. P06-B moves broader
composition behind these types; P07-A owns `archive_engine/recovery_manifest.py`
as a sibling module and is not claimed here.

Query quality here means the same typed card, the same revision, and the same
relationship fields whether the caller is `ppa read` or MCP `archive_read`.

## Import the frozen types

P01, P03, and P08 must import these records. Do not redefine them.

```python
from archive_engine.contracts import (
    AccessContext,
    ArchiveIdentity,
    ChangeBatch,
    ChangeRecord,
    ChunkEvidenceRef,
    EmbeddingSpec,
    EvidenceEnvelope,
    OutputReceipt,
    RunEvidence,
    ServingEdge,
    ServingManifest,
    dump_contract,
    load_contract,
)
from archive_engine.service import ArchiveEngineService
```

Package re-exports the same names from `archive_engine`.

`ChangeBatch.consumer_name` is required. `OutputReceipt.outputs` is the
revision-specific UID+revision list later invalidation uses. `ServingEdge`
`method` / `confidence` / `evidence_uids` are unknown when absent — absence is
not "everything is trusted." `EvidenceEnvelope` carries method, corpus state,
provenance, and `evidence_kind` (`source_reported` | `derived` |
`proposed_link` | `unknown`).

## Exact read

```text
CLI `ppa read` / MCP `archive_read`
    → DefaultArchiveStore.read
    → ArchiveEngineService.read_exact
         → UidPathLookup (injected)
         → CanonicalReader.read_contained
              → archive_vault.paths.resolve_contained_path
```

There is one contained-path helper. Denied or escaping paths stay a stable
not-found envelope (`found=false`, empty content), matching P05-A.

`AccessContext` is always present on the service. Legacy CLI/MCP construct the
configured trusted-local context (`principal=local-operator`,
`profile=trusted-local`, `deny=false`). MCP tool-profile gating remains a
transport check in `archive_cli/server.py` until P05-B.

## Identity

`ArchiveIdentity.archive_id` is persistent and explicit. The factory will use
`PPA_ARCHIVE_ID` when set; otherwise it hashes the resolved canonical root plus
schema binding. It never uses a display name or directory stem as the id.
`schema_binding` is `warehouse:{schema}+index_schema_v{N}`. P09 provisions
durable instance identity.

## Isolation

Each `ArchiveEngineService` holds its own identity, access, lookup, and reader.
Two archive contexts in one process do not share those objects or card bytes.
The factory does not keep a process-global engine.

## Ports (declared, not all implemented)

| Port | P06-A |
| --- | --- |
| `UidPathLookup` | wired (serving, then index) |
| `CanonicalReader` | wired (contained vault read) |
| `AffectedContextResolver` | declared; absence means `reconciliation_pending`, not success |
| `WarehouseSnapshot` | declared for bounded cursor lifetime; unused until later slices |

Core `archive_engine` modules must not import `archive_cli.commands` or
`archive_cli.server`.

## Serialization

Durable/interprocess JSON uses `dump_contract` / `load_contract`:

```json
{ "contract": "ArchiveIdentity", "version": 1, "payload": { ... } }
```

Unknown contract names and versions are rejected. `ChunkEvidenceRef.version`
must be `1`. Missing optional burst/`source_spans` fields mean unavailable, not
empty proof (`span_unavailable`, `message_refs_available`). Invalid span
offsets fail closed.

`RunEvidence.environment` / `versions` reject known secret keys.

## Notes for P01 / P03 / P08

- Import `EmbeddingSpec` everywhere a vector/cache/generation identity crosses
  a boundary. Mixed model/dimension/metric/normalization spaces must not share
  a ranking.
- P01-B freezes the Python/JSON/Rust round trip of `ChunkEvidenceRef` and
  serving artifacts. Do not invent a competing evidence dict.
- P02 owns `ChangeRecord` / `ChangeBatch` storage. Each consumer keeps its own
  cursor; acknowledgement is monotonic and bounded.
- P03 fills `OutputReceipt`. Completion is revision-specific and lists actual
  output UID+revision pairs.
- P08 may add connector adapters without importing CLI/MCP transports.
- Do not start consumer work against a local copy of these dataclasses.

P06-B will publish the facade→service path map in this file after
correctness/privacy slices land.

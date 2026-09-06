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
configured context via `archive_engine.access.resolve_access_context`. P05-B
applies that context before ranking on every retrieval surface.

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

## P06-B instance-scoped runtime

CLI and MCP share one `ArchiveRuntime` per store:

```text
resolve_store() / resolve_runtime()
    → DefaultArchiveStore  (compatibility facade)
    → ArchiveRuntime
         exact_read   ArchiveEngineService
         retrieval    archive_engine.adapters.retrieval.RetrievalAdapter
         warehouse    archive_engine.adapters.warehouse.WarehouseAdapter
         providers    archive_engine.adapters.providers.EmbeddingProviderAdapter
```

### Facade → service path map

| Old call | New seam | Removal condition |
| --- | --- | --- |
| `DefaultArchiveStore.read` | `runtime.read` → `ArchiveEngineService` | After P06-C import guards; owner P06 |
| `DefaultArchiveStore.search/query/graph` | `runtime.retrieval` | After P10 filtered query lands; owner P06-D |
| `DefaultArchiveStore.rebuild/bootstrap` | `runtime.warehouse` | After maintain no longer needs the store facade; owner P03/P06 |
| `store.index._connect` from commands | **not migrated** — stays in maintain/admin | Warehouse adapter may open a snapshot internally; public API has no `_connect` |
| `archive_cli.ppa_engine.ppa_engine` | `archive_engine.execution_mode.ppa_engine` | CLI re-export remains; remove after vault/cache callers switch (P09) |
| `get_serving_handle` process singleton | vault-keyed `_HANDLES` + `close_serving_handles` | Keep facade; runtime.close unpins this vault |

Serving vs index is selected **once** in `engine_factory.build_runtime` (serving factory present or not). Retrieval adapters do not `isinstance` a warehouse type. Query ranking, publication, corrections, and access predicates are unchanged sibling behavior.

Two `ArchiveRuntime` objects with the same UID and different canonical roots return their own cards. Native serving handles are keyed by vault and closed on `runtime.close()` / exception exit.

### Notes for P05-C / P09-B / P10-A

- **P05-C:** attach egress/provider hooks to `runtime.providers`. Do not read env inside retrieval adapters.
- **P09-B:** `build_runtime` / `resolve_archive_identity` is the explicit-config factory. Saved scopes must bind `AccessContext` on the runtime, not a second store.
- **P10-A:** typed query/analytics should call `runtime.retrieval` / `runtime.query` with the instance `AccessContext`. Do not add a second query path on `DefaultArchiveStore`.

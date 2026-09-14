# Engine contract

A user should get the same stored record whether they query PPA from a terminal or an agent. Shared engine records and a contained exact-read path keep identity, revision, relationships, and access consistent across clients.

This contract covers shared types, exact reads, and runtime integration. Publication, processing, and recovery have their own contracts in the [documentation index](README.md).

## Shared types

Retrieval, processors, and connectors must import these records rather than redefine them.

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
`method` / `confidence` / `evidence_uids` remain unknown when absent; missing metadata does not grant trust. `EvidenceEnvelope` carries method, corpus state,
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
not-found envelope (`found=false`, empty content).

`AccessContext` is always present on the service. Legacy CLI/MCP construct the
configured context via `archive_engine.access.resolve_access_context`. Retrieval applies that context before ranking or returning records.

## Identity

`ArchiveIdentity.archive_id` is persistent and explicit. The factory will use
`PPA_ARCHIVE_ID` when set; otherwise it hashes the resolved canonical root plus
schema binding. It never uses a display name or directory stem as the id.
`schema_binding` is `warehouse:{schema}+index_schema_v{N}`. Instance configuration carries the archive identity across client connections.

## Isolation

Each `ArchiveEngineService` holds its own identity, access, lookup, and reader.
Two archive contexts in one process do not share those objects or card bytes.
The factory does not keep a process-global engine.

## Read and context interfaces

| Interface | Role |
| --- | --- |
| `UidPathLookup` | Resolve a record identifier through the serving index or warehouse lookup |
| `CanonicalReader` | Read a file within the vault |
| `AffectedContextResolver` | Identify context affected by a change; absence means `reconciliation_pending` |
| `WarehouseSnapshot` | Declare a bounded lifetime for warehouse cursors |

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

## Requirements for retrieval, processors, and connectors

- Import `EmbeddingSpec` everywhere a vector/cache/generation identity crosses
  a boundary. Mixed model/dimension/metric/normalization spaces must not share
  a ranking.
- Preserve the Python/JSON/Rust round trip of `ChunkEvidenceRef` and serving artifacts. Use the shared evidence types.
- The change journal stores `ChangeRecord` / `ChangeBatch`. Each consumer keeps its own
  cursor; acknowledgement is monotonic and bounded.
- Processors populate `OutputReceipt`. Completion is revision-specific and lists actual
  output UID+revision pairs.
- Connector adapters must remain independent of CLI/MCP transports.
- Do not start consumer work against a local copy of these dataclasses.

## Runtime for each archive instance

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

### Store calls and runtime interfaces

| Store call | Runtime interface | Compatibility requirement |
| --- | --- | --- |
| `DefaultArchiveStore.read` | `runtime.read` → `ArchiveEngineService` | Keep existing exact-read callers working |
| `DefaultArchiveStore.search/query/graph` | `runtime.retrieval` | Use the shared retrieval adapter |
| `DefaultArchiveStore.rebuild/bootstrap` | `runtime.warehouse` | Keep warehouse work behind the runtime adapter |
| `store.index._connect` from commands | not migrated; remains in maintain/admin | Warehouse adapter may open a snapshot internally; public API has no `_connect` |
| `archive_cli.ppa_engine.ppa_engine` | `archive_engine.execution_mode.ppa_engine` | Keep the CLI re-export while vault/cache callers use it |
| `get_serving_handle` process singleton | vault-keyed `_HANDLES` + `close_serving_handles` | Keep facade; runtime.close unpins this vault |

Serving vs index is selected **once** in `engine_factory.build_runtime` (serving factory present or not). Retrieval adapters do not `isinstance` a warehouse type. Query ranking, publication, corrections, and access predicates are unchanged sibling behavior.

Two `ArchiveRuntime` objects with the same UID and different canonical roots return their own cards. Native serving handles are keyed by vault and closed on `runtime.close()` / exception exit.

### Provider, configuration, and query integration

- Attach egress/provider hooks to `runtime.providers`. Do not read env inside retrieval adapters.
- Use `build_runtime` / `resolve_archive_identity` as the explicit-config factory. Saved scopes must bind `AccessContext` on the runtime, not a second store.
- Typed query/analytics should call `runtime.retrieval` / `runtime.query` with the instance `AccessContext`. Do not add a second query path on `DefaultArchiveStore`.

## Dependency and registry contracts

Card-type tests check agreement between the model, card contract, projection registry, and native materializer. Import checks reject core engine dependencies on CLI commands or MCP with `forbidden engine import`.

### Dependency direction

```text
archive_vault          → schema / I/O / card contracts   (no archive_cli)
archive_engine core    → vault + contracts/ports only    (no archive_cli)
archive_engine.adapters→ may wrap index/serving objects  (no commands/server)
archive_cli factory    → composes adapters + runtime
CLI / MCP              → DefaultArchiveStore.runtime
```

`archive_tests/test_engine_boundaries.py` AST-walks these rules. A planted `from archive_cli.commands…` in a temp engine tree raises `EngineBoundaryError` naming the file and module.

### Registry authority

| Layer | Source |
| --- | --- |
| Schema / ownership | `archive_vault.schema.CARD_TYPES` + `DETERMINISTIC_ONLY` / `LLM_ELIGIBLE` |
| Contract profiles | `archive_vault.card_contracts.CARD_TYPE_SPECS` |
| Projection / edges | `archive_cli.card_registry.CARD_TYPE_REGISTRATIONS` |
| Native materializer | `archive_crate/materializer_registry.json` via `archive_scripts/export_materializer_registry.py` |

`materializer_registry_payload()` / `dump_registry_json()` are the only export shape. `validate_card_type_specs()` and `validate_card_type_registrations()` require coverage plus table-name alignment. Handwritten extras that are not Pydantic fields are listed in `CONTRACT_FIELDS_NOT_ON_MODEL` and `HANDWRITTEN_PROJECTION_SOURCES`. A new unexplained field or a stale checked-in JSON fails with `stale materializer registry` / `undocumented …`.

Regenerate after a real registration change:

```text
python archive_scripts/export_materializer_registry.py
```

Do not hand-edit the JSON.

### Public compatibility

| Surface | Names that must keep working |
| --- | --- |
| `archive_engine` | `AccessContext`, `ArchiveIdentity`, `ArchiveRuntime`, `ArchiveEngineService`, `dump_contract`, `load_contract` |
| `archive_cli.ppa_engine` | `ppa_engine` (re-export of `archive_engine.execution_mode`) |
| CLI | `ppa read`, `ppa search`, `ppa query` |
| MCP | `archive_read`, `archive_search`, `archive_query` |
| Resolve | `resolve_store`, `resolve_runtime` |

Package discovery includes `archive_engine*` (`pyproject.toml`). The [packaging checks](runbooks/install-independent-archive.md) verify installed-wheel execution outside the checkout.

### Remaining compatibility adapters

| Adapter | Why it stays | Removal |
| --- | --- | --- |
| `DefaultArchiveStore` | CLI/MCP facade over `ArchiveRuntime` | After callers use `resolve_runtime` |
| `archive_cli.ppa_engine` | Old import path | After vault/cache-only callers switch |
| `store.index._connect` in maintain | Admin/warehouse private conn | Stays inside warehouse adapter snapshot |
| `archive_engine.publication` → `archive_cli.index_config` / `serving_index` | Publication builds the serving generation | After the publication adapter owns the native write |
| `archive_vault.identity_resolver` → `archive_cli.vault_cache` | Person resolution cache | After the cache moves under vault/engine |

A synthetic supported `person` card must still travel write → materialize → `runtime.read` / `runtime.query` with the registered `people` projection.

## Connectors and conversation context

Connectors can receive records before the conversation-context resolver is available. The runtime keeps affected context pending, then schedules it when the resolver attaches. Source parsing and conversation segmentation remain in their respective modules.

```text
connector persist
    → ChangeRecord / OutputReceipt
    → pending scopes (if BurstKeyResolver is absent)
    → ArchiveRuntime.drain_pending_scopes()  → attach_resolver
    → EngineBurstBridge (burst_key_for / resolve_burst_affected)
    → publication generation (serving handle)
    → DefaultArchiveStore.search/query/evidence  → runtime.retrieval
    → installed CLI / MCP via resolve_store()
```

### Resolver availability

| Arrival order | Freshness before attach | After `drain_pending_scopes` |
| --- | --- | --- |
| Connector then bursts | `unknown`, pending scopes persist | scopes `scheduled`; burst keys invalidated |
| Bursts then connector | resolver already on `execute_connector` | `invalidated` immediately; no pending wait |
| Resolver capability actually unavailable (`attach_bursts=False`) | `unknown` | stays `unknown` |

Unknown freshness is allowed only when the burst capability is missing. A late resolver cannot leave freshness unknown.

### Retrieval and context paths

| Call | Interface | Dependency to avoid |
| --- | --- | --- |
| `DefaultArchiveStore.search/query/evidence/graph/timeline` | `runtime.retrieval` | `index.search` / `serving.search` branches on the store |
| `DefaultArchiveStore.read` | `runtime.read` | concrete warehouse lookup in commands |
| MCP `archive_search` / `archive_read` / `archive_query` / `archive_evidence` | `_delegated_store()` → same facade | a second `ArchiveRuntime` |
| Connector burst keys | `execute_connector(..., burst_resolver=)` | engine importing `archive_cli.conversation_bursts` |
| Pending drain | `runtime.drain_pending_scopes` → injected `attach_resolver` | core engine importing connector replay |

`EngineBurstBridge` lives in `archive_cli.engine_factory`. It forwards to the conversation-burst implementation and uses its segmentation rules.

Installed CLI/MCP checks use `install_isolated` outside the checkout. Record the `archive_crate` path and wheel hashes so another developer can identify the tested build. These fixture checks report `production_proven=false`.

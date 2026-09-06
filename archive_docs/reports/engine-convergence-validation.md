# Engine convergence validation (P06-D)

`production_proven=false`. Isolated fixtures only. No seed vault. No inherited `PPA_TEST_PG_DSN`.

## Feature-arrival matrix

| Order | Fixture | Freshness | Pending scopes |
| --- | --- | --- | --- |
| Connector before bursts | `sample.fixture` persist, then `runtime.drain_pending_scopes()` | `unknown` until attach | persist, then `scheduled` once |
| Bursts before connector | `execute_connector(..., burst_resolver=EngineBurstBridge())` | `invalidated` immediately | none required |
| Capability unavailable | `build_runtime(attach_bursts=False)` | stays `unknown` | drain is a no-op |

Unknown freshness is reserved for a missing burst capability. A late resolver cannot leave scopes unscheduled.

## Event → revision → chunk → generation → citation

1. Connector persist writes a canonical card and a `ChangeRecord` / `OutputReceipt` with `after_revision`.
2. `EngineBurstBridge.burst_keys_for` / `resolve_burst_affected` (P01 algorithm) emits current burst keys and retires obsolete ones.
3. Publication generation is `runtime.retrieval.generation()` from the serving handle.
4. CLI/MCP context query (`store.search` / `store.evidence` / `ppa read`) cites the current reply. Unrelated thread keys stay searchable.

Append does not retire earlier keys. Edit/delete retire only the changed burst. Duplicate keeps keys and re-cites the current replacement.

## Restart

Pending scopes survive process restart as `_meta/connector-pending-scopes.json`. A new `ArchiveRuntime` with the burst bridge attached drains them idempotently (`scheduled=true` on the second pass).

## Denied evidence

Restricted `AccessContext(allowed_sources=("gmail",))` cannot surface adjacent medical rows or the `P06D_DENIED_SYNTHETIC_SENTINEL` provider token through `store.search` or `store.evidence`.

## Installed artifact identity

Acceptance installs candidate wheels with `archive_tests.acceptance.p09_install_support.install_isolated` outside the checkout (`PYTHONPATH` unset). Record:

- `extension_path` — `archive_crate.__file__` inside the fresh venv
- `wheel_hashes` — SHA-256 of root + native wheels
- matching `ppa read` and installed-module `read` on the baseline person card

Host note: rustc 1.85.1 via rustup is required to build the crate graph. `requires-python` stays `>=3.10`.

## Facade path map

| Call | Seam |
| --- | --- |
| search / query | `runtime.search` / `runtime.query` |
| evidence / graph / timeline | `runtime.retrieval` |
| read | `runtime.read` |
| MCP search/read/query/evidence | `_delegated_store()` |
| burst keys | `execute_connector(..., burst_resolver=)` |
| pending drain | `runtime.drain_pending_scopes` → `attach_resolver` |

Compatibility-removal inventory: no late patch restores `DefaultArchiveStore.search` → `index.search`, `evidence` → `serving.search`, or engine imports of `archive_cli.commands` / `archive_cli.server`.

## Unresolved seams

None opened by this slice. Algorithm failures (ranking, connector lifecycle, publication) stay with P01 / P08 / P02. P04-D is not started.

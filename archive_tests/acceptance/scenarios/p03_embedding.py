"""P03-B acceptance: dirty embed selects the dirty card's chunks only."""

from __future__ import annotations

import time
from typing import Any

from psycopg import connect
from psycopg.rows import dict_row

from archive_tests.acceptance.environment import IsolatedRuntime, inspect_warehouse_card
from archive_tests.acceptance.fixtures import init_vault
from archive_tests.acceptance.registry import Scenario, register
from archive_vault.provenance import ProvenanceEntry
from archive_vault.schema import PersonCard
from archive_vault.vault import write_card

DIRTY_UID = "hfa-person-p03bdirty01"
BACKLOG_UID = "hfa-person-p03bbacklg1"
DIRTY_BODY = (
    "P03-B dirty card body. " + ("selected multi-chunk text. " * 80)
)
BACKLOG_BODY = (
    "P03-B older backlog card body. " + ("unrelated backlog text. " * 80)
)


class ScenarioAssertionError(AssertionError):
    """P03-B product assertion failed."""


def _write_person(vault, *, uid: str, name: str, rel: str, body: str) -> None:
    card = PersonCard(
        uid=uid,
        type="person",
        source=["acceptance.p03b"],
        source_id=f"{uid}@example.test",
        created="2026-09-06",
        updated="2026-09-06",
        summary=name,
        first_name=name.split()[0],
        last_name=name.split()[-1],
        emails=[f"{uid}@example.test"],
        tags=["p03-acceptance", "synthetic"],
    )
    prov = {
        field: ProvenanceEntry("acceptance.p03b", "2026-09-06", "deterministic")
        for field in ("summary", "first_name", "last_name", "emails", "tags")
    }
    write_card(vault, rel, card, body=body, provenance=prov)


def _chunk_keys(dsn: str, schema: str, uid: str) -> list[str]:
    with connect(dsn, row_factory=dict_row) as conn:
        rows = conn.execute(
            f"""
            SELECT chunk_key FROM {schema}.chunks
            WHERE card_uid = %s
            ORDER BY rel_path, chunk_type, chunk_index
            """,
            (uid,),
        ).fetchall()
    return [str(row["chunk_key"]) for row in rows]


def _embedded_keys(dsn: str, schema: str, *, model: str, uid: str | None = None) -> set[str]:
    with connect(dsn, row_factory=dict_row) as conn:
        if uid is None:
            rows = conn.execute(
                f"""
                SELECT e.chunk_key
                FROM {schema}.embeddings e
                WHERE e.embedding_model = %s AND e.embedding_version = 1
                """,
                (model,),
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT e.chunk_key
                FROM {schema}.embeddings e
                JOIN {schema}.chunks c ON c.chunk_key = e.chunk_key
                WHERE e.embedding_model = %s AND e.embedding_version = 1 AND c.card_uid = %s
                """,
                (model, uid),
            ).fetchall()
    return {str(row["chunk_key"]) for row in rows}


class CountingHash:
    name = "hash"

    def __init__(self, inner):
        self.inner = inner
        self.model = inner.model
        self.dimension = inner.dimension
        self.calls = 0

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        return self.inner.embed_texts(texts)


class BoomProvider:
    name = "hash"

    def __init__(self, *, model: str, dimension: int):
        self.model = model
        self.dimension = dimension
        self.calls = 0

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.calls += 1
        raise RuntimeError("acceptance provider failure")


def run_p03_embedding(runtime: IsolatedRuntime) -> dict[str, Any]:
    started = time.monotonic()
    init_vault(runtime.vault, owned_root=runtime.root)
    _write_person(
        runtime.vault,
        uid=BACKLOG_UID,
        name="P03 Backlog Card",
        rel="People/p03-backlog.md",
        body=BACKLOG_BODY,
    )
    _write_person(
        runtime.vault,
        uid=DIRTY_UID,
        name="P03 Dirty Card",
        rel="People/p03-dirty.md",
        body=DIRTY_BODY,
    )

    from archive_cli.embedding_provider import HashEmbeddingProvider
    from archive_cli.store import DefaultArchiveStore
    from archive_sync.processors.batch import ProcessorPlanItem
    from archive_sync.processors.constants import (
        CORPUS_ACTIVE,
        INPUT_STATUS_COMPLETE,
        INPUT_STATUS_FAILED,
        PROCESSOR_EMBEDDING,
        PROCESSOR_MATERIALIZATION,
    )
    from archive_sync.processors.runner import ExecuteContext, _execute_embedding, run_processors
    from archive_sync.processors.staleness import ProcessorInputSnapshot
    from archive_sync.processors.state_store import ProcessorStateStore

    store = DefaultArchiveStore(vault=runtime.vault)
    store.bootstrap()
    state = ProcessorStateStore(None, meta_path=runtime.vault / "_meta" / "processors.json")

    def _snap(uid: str) -> ProcessorInputSnapshot:
        return ProcessorInputSnapshot(
            input_uid=uid,
            card_type="person",
            corpus_state=CORPUS_ACTIVE,
            field_values={
                "body_sha": uid,
                "frontmatter_hash": uid,
                "chunk_hash": uid,
                "source_hash": uid,
                "target_hash": uid,
                "corpus_state": CORPUS_ACTIVE,
            },
            source_dirty=True,
            upstream_complete=True,
        )

    mat = run_processors(
        inputs=[_snap(BACKLOG_UID), _snap(DIRTY_UID)],
        vault_path=str(runtime.vault),
        store=store,
        state_store=state,
        processor_keys=[PROCESSOR_MATERIALIZATION],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03b-materialize",
    )
    if inspect_warehouse_card(runtime.dsn, runtime.schema, DIRTY_UID) is None:
        raise ScenarioAssertionError("dirty card missing after materialize")
    if inspect_warehouse_card(runtime.dsn, runtime.schema, BACKLOG_UID) is None:
        raise ScenarioAssertionError("backlog card missing after materialize")

    dirty_keys = _chunk_keys(runtime.dsn, runtime.schema, DIRTY_UID)
    backlog_keys = _chunk_keys(runtime.dsn, runtime.schema, BACKLOG_UID)
    if len(dirty_keys) < 2:
        raise ScenarioAssertionError(f"expected multi-chunk dirty card, got {dirty_keys}")
    if not backlog_keys:
        raise ScenarioAssertionError("backlog card produced no chunks")

    model = "archive-hash-dev"
    inner = HashEmbeddingProvider(model=model, dimension=8)
    counting = CountingHash(inner)
    store.provider_factory = lambda model="": counting

    embed_state = ProcessorStateStore(None, meta_path=runtime.vault / "_meta" / "processors-embed.json")
    result = run_processors(
        inputs=[_snap(DIRTY_UID)],
        vault_path=str(runtime.vault),
        store=store,
        state_store=embed_state,
        processor_keys=[PROCESSOR_EMBEDDING],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03b-embed",
    )
    embed_items = [item for item in result.item_results if item.processor_key == PROCESSOR_EMBEDDING]
    if not embed_items or embed_items[0].status != INPUT_STATUS_COMPLETE:
        raise ScenarioAssertionError(f"dirty card embed did not complete: {embed_items}")
    receipt = embed_items[0].receipt
    if receipt is None or set(receipt.chunk_keys) != set(dirty_keys):
        raise ScenarioAssertionError(f"receipt chunk keys {getattr(receipt, 'chunk_keys', None)} != {dirty_keys}")

    warehouse_dirty = _embedded_keys(runtime.dsn, runtime.schema, model=model, uid=DIRTY_UID)
    warehouse_backlog = _embedded_keys(runtime.dsn, runtime.schema, model=model, uid=BACKLOG_UID)
    if warehouse_dirty != set(dirty_keys):
        raise ScenarioAssertionError(f"warehouse dirty {warehouse_dirty} != selected {dirty_keys}")
    if warehouse_backlog:
        raise ScenarioAssertionError(f"unrelated backlog was embedded: {warehouse_backlog}")
    first_calls = counting.calls
    if first_calls < 1:
        raise ScenarioAssertionError("expected a provider call for first dirty embed")

    counting.calls = 0
    rerun = run_processors(
        inputs=[_snap(DIRTY_UID)],
        vault_path=str(runtime.vault),
        store=store,
        state_store=embed_state,
        processor_keys=[PROCESSOR_EMBEDDING],
        apply=True,
        dry_run=False,
        provider_available=True,
        run_id="p03b-embed-rerun",
    )
    # already_current may skip the executor; force a direct store call for cache proof
    cache = store.embed_pending(limit=0, uid_allowlist={DIRTY_UID})
    if counting.calls != 0:
        raise ScenarioAssertionError(f"compatible cache hit called provider {counting.calls} times")
    if set(cache.get("reused_chunk_keys") or []) != set(dirty_keys):
        raise ScenarioAssertionError(f"cache reuse {cache.get('reused_chunk_keys')} != {dirty_keys}")

    raised = False
    try:
        store.embed_pending(limit=1)
    except ValueError as exc:
        raised = "allowlist" in str(exc)
    if not raised:
        raise ScenarioAssertionError("dirty-path embed_pending without allowlist did not raise")

    boom = BoomProvider(model=model, dimension=8)
    store.provider_factory = lambda model="": boom
    fail_out = _execute_embedding(
        ExecuteContext(
            vault_path=str(runtime.vault),
            store=store,
            apply=True,
            dry_run=False,
            provider_available=True,
            run_id="p03b-fail",
        ),
        [
            ProcessorPlanItem(
                processor_key=PROCESSOR_EMBEDDING,
                input_uid=BACKLOG_UID,
                current_input_hash="rev-backlog",
                input_revision="rev-backlog",
            )
        ],
    )
    if not fail_out.results or fail_out.results[0].status != INPUT_STATUS_FAILED:
        raise ScenarioAssertionError(f"provider failure marked card complete: {fail_out.results}")
    if _embedded_keys(runtime.dsn, runtime.schema, model=model, uid=BACKLOG_UID):
        raise ScenarioAssertionError("failed backlog card still received embeddings")

    return {
        "id": "p03.embedding_dirty_selection",
        "status": "passed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "selected_chunk_keys": dirty_keys,
        "completed_chunk_keys": sorted(warehouse_dirty),
        "backlog_chunk_keys": backlog_keys,
        "backlog_embedded": sorted(warehouse_backlog),
        "provider_calls_first": first_calls,
        "provider_calls_cache": counting.calls,
        "materialize_status": mat.report.status,
        "rerun_status": rerun.report.status,
        "schema": runtime.schema,
    }


register(
    Scenario(
        id="p03.embedding_dirty_selection",
        suite="p03",
        product_guarantee=(
            "A multi-chunk dirty card is fully embedded despite older unrelated "
            "backlog; unselected cards are untouched"
        ),
        proof_tier="isolated_integration",
        fixture_seed=0,
        fixture_hash="",
        prerequisites=("docker", "rust_engine", "hash_embeddings"),
        expected_artifacts=("results.json", "evidence.json"),
        run=run_p03_embedding,
    )
)

"""P02-C: two publishers and a pinned reader across processes."""

from __future__ import annotations

import multiprocessing as mp
import os
from pathlib import Path

import pytest

from archive_cli.serving_index import prune_retired_serving_generations
from archive_engine.errors import PublisherBusyError
from archive_engine.publication import (
    PublisherLease,
    pin_generation,
    pinned_generations,
    publish_snapshot,
    read_active_generation,
    unpin_generation,
)
from archive_tests.test_publication_equivalence import (
    _base_state,
    _mutated_full,
    _publish_full,
    _snapshot,
)

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")


def _state_snapshot(state: dict[str, object], name: str) -> object:
    return _snapshot(
        cards=list(state["cards"]),  # type: ignore[arg-type]
        chunks=list(state["chunks"]),  # type: ignore[arg-type]
        edges=list(state["edges"]),  # type: ignore[arg-type]
        embeddings=list(state["embeddings"]),  # type: ignore[arg-type]
        name=name,
    )


def _hold_lease(root: str, ready: mp.synchronize.Event, release: mp.synchronize.Event) -> None:
    lease = PublisherLease(Path(root))
    lease.acquire()
    ready.set()
    release.wait(timeout=30)
    lease.release()


def _try_publish(root: str, result: mp.Queue) -> None:
    try:
        publish_snapshot(
            Path(root),
            _state_snapshot(_mutated_full(), "race"),
            generation_id="gen-race",
            mode="full",
        )
        result.put("ok")
    except PublisherBusyError:
        result.put("busy")
    except Exception as exc:  # pragma: no cover - diagnostic
        result.put(f"err:{type(exc).__name__}:{exc}")


def _abandon_lease(root: str, ready: mp.synchronize.Event) -> None:
    PublisherLease(Path(root)).acquire()
    ready.set()
    os._exit(0)


def _hold_pin(root: str, gid: str, ready: mp.synchronize.Event, release: mp.synchronize.Event) -> None:
    pin_generation(root, gid)
    ready.set()
    release.wait(timeout=60)
    unpin_generation(root, gid)


def test_second_publisher_is_busy(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    _publish_full(root, _base_state(), "gen-base")
    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    result = ctx.Queue()
    holder = ctx.Process(target=_hold_lease, args=(str(root), ready, release))
    challenger = ctx.Process(target=_try_publish, args=(str(root), result))
    holder.start()
    assert ready.wait(timeout=15)
    challenger.start()
    outcome = result.get(timeout=60)
    assert outcome == "busy"
    release.set()
    holder.join(timeout=10)
    challenger.join(timeout=10)
    assert holder.exitcode == 0
    assert challenger.exitcode == 0
    assert read_active_generation(root) == "gen-base"


def test_abandoned_lease_can_be_stolen(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    child = ctx.Process(target=_abandon_lease, args=(str(root), ready))
    child.start()
    assert ready.wait(timeout=15)
    child.join(timeout=10)
    assert child.exitcode == 0
    lease = PublisherLease(root)
    lease.acquire()
    try:
        payload = lease.lease_path.read_text(encoding="utf-8")
        assert str(os.getpid()) in payload
    finally:
        lease.release()


def test_pinned_reader_survives_prune(tmp_path: Path) -> None:
    root = tmp_path / "idx"
    _publish_full(root, _base_state(), "gen-base")
    ctx = mp.get_context("spawn")
    ready = ctx.Event()
    release = ctx.Event()
    reader = ctx.Process(target=_hold_pin, args=(str(root), "gen-base", ready, release))
    reader.start()
    assert ready.wait(timeout=15)
    assert "gen-base" in pinned_generations(root)
    _publish_full(root, _mutated_full(), "gen-next")
    removed = prune_retired_serving_generations(tmp_path, keep="gen-next", index_root=root)
    assert "gen-base" not in removed
    assert (root / "generations" / "gen-base").is_dir()
    release.set()
    reader.join(timeout=10)
    assert reader.exitcode == 0
    removed_after = prune_retired_serving_generations(tmp_path, keep="gen-next", index_root=root)
    assert "gen-base" in removed_after
    assert not (root / "generations" / "gen-base").exists()

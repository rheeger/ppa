from __future__ import annotations

import json
from unittest.mock import MagicMock

from archive_cli.serving_index import ack_dirty_uids, publish_serving_index
from archive_engine.contracts import EmbeddingSpec
from archive_engine.publication import PublisherLease, ServingSnapshot


def test_publish_acquires_lease_before_status(tmp_path, monkeypatch) -> None:
    order: list[str] = []
    vault = tmp_path / "vault"
    vault.mkdir()
    root = vault / "_meta" / "rust-search-index"
    root.mkdir(parents=True)

    orig_acquire = PublisherLease.acquire

    def _acquire(self):
        order.append("lease")
        return orig_acquire(self)

    monkeypatch.setattr(PublisherLease, "acquire", _acquire)

    def _status(_vault):
        order.append("status")
        return {"serving_index_generation": "", "serving_index_ready": False}

    monkeypatch.setattr("archive_cli.serving_index.serving_index_status", _status)
    monkeypatch.setattr("archive_cli.serving_index.get_serving_index_path", lambda _v: root)
    monkeypatch.setattr(
        "archive_cli.serving_index._export_warehouse_snapshot",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stop-after-lease")),
    )
    store = MagicMock()
    store.vault = vault
    try:
        publish_serving_index(store)
    except RuntimeError:
        pass
    assert order[0] == "lease"
    assert "status" in order


def test_unresolved_scan_rejections_block_default_publish(tmp_path, monkeypatch) -> None:
    from archive_engine.errors import IncompatibleStateError
    from archive_engine.journaled_state import SCAN_REJECTIONS_REL, SCAN_REJECTIONS_UID, persist_json_state
    from archive_engine.publication import publish_snapshot

    vault = tmp_path / "vault"
    vault.mkdir()
    persist_json_state(
        vault,
        uid=SCAN_REJECTIONS_UID,
        rel=SCAN_REJECTIONS_REL,
        payload={"rejections": [{"contained_path": "People/bad.md", "error_code": "schema_invalid"}], "count": 1},
        source="test",
    )
    root = tmp_path / "index"
    spec = EmbeddingSpec(
        provider_namespace="hash",
        model="t",
        model_revision="1",
        dimension=4,
        metric="ip",
        normalization="none",
        chunk_schema="v1",
    )
    snapshot = ServingSnapshot(
        snapshot_id="s",
        source_watermark=0,
        cards=(),
        chunks=(),
        edges=(),
        embeddings=(),
        embedding_spec=spec,
    )
    try:
        publish_snapshot(root, snapshot, vault=vault, acquire_lease=True)
        raise AssertionError("expected publication_blocked_unresolved_scan_rejections")
    except IncompatibleStateError as exc:
        assert "scan_rejection" in str(exc)


def test_successful_publish_does_not_truncate_dirty(tmp_path, monkeypatch) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    root = vault / "_meta" / "rust-search-index"
    root.mkdir(parents=True)
    (root / "DIRTY").write_text("keep-me\n", encoding="utf-8")
    monkeypatch.setattr(
        "archive_cli.serving_index.serving_index_status",
        lambda _v: {"serving_index_generation": "", "serving_index_ready": False},
    )
    monkeypatch.setattr("archive_cli.serving_index.get_serving_index_path", lambda _v: root)
    monkeypatch.setattr(
        "archive_cli.serving_index._export_warehouse_snapshot",
        lambda *a, **k: ServingSnapshot(
            snapshot_id="s",
            source_watermark=0,
            cards=(),
            chunks=(),
            edges=(),
            embeddings=(),
            embedding_spec=EmbeddingSpec(
                provider_namespace="hash",
                model="t",
                model_revision="1",
                dimension=4,
                metric="ip",
                normalization="none",
                chunk_schema="v1",
            ),
        ),
    )

    class _Crate:
        def serving_index_truncate_dirty(self, *_a, **_k):
            raise AssertionError("must not blanket-truncate DIRTY")

    monkeypatch.setattr("archive_cli.serving_index._crate", lambda: _Crate())
    monkeypatch.setattr(
        "archive_cli.serving_index.publish_snapshot",
        lambda *a, **k: type(
            "R",
            (),
            {
                "generation_id": "g1",
                "to_payload": lambda self: {},
                "cards": 0,
                "chunks": 0,
                "embeddings": 0,
                "mode": "full",
                "parent_generation": "",
                "snapshot_id": "s",
            },
        )(),
    )
    monkeypatch.setattr("archive_cli.serving_index.prune_retired_serving_generations", lambda *a, **k: [])
    monkeypatch.setattr("archive_cli.serving_index.close_serving_handles", lambda **k: None)

    class _Cache:
        def evict(self, **_k):
            return None

        def close(self):
            return None

    monkeypatch.setattr("archive_cli.serving_index.QueryEmbedCache", lambda *a, **k: _Cache())
    store = MagicMock()
    store.vault = vault
    publish_serving_index(store)
    assert (root / "DIRTY").read_text(encoding="utf-8") == "keep-me\n"


def test_ack_dirty_uids_drops_published_ids_only(tmp_path) -> None:
    vault = tmp_path / "vault"
    root = vault / "_meta" / "rust-search-index"
    root.mkdir(parents=True)
    (root / "DIRTY").write_text(
        json.dumps({"ts": "1", "reason": "p03d-create", "uids": ["hfa-person-a", "hfa-person-b"]})
        + "\n"
        + json.dumps({"ts": "2", "reason": "other", "uids": ["hfa-person-c"]})
        + "\nkeep-me\n",
        encoding="utf-8",
    )
    removed = ack_dirty_uids(vault, ["hfa-person-a"])
    assert removed == 1
    leftover = (root / "DIRTY").read_text(encoding="utf-8")
    assert "hfa-person-a" not in leftover
    assert "hfa-person-b" in leftover
    assert "hfa-person-c" in leftover
    assert "keep-me" in leftover

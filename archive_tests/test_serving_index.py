from __future__ import annotations

import json
import struct
import time
from pathlib import Path

import pytest

from archive_cli.errors import ServingIndexUnavailableError
from archive_cli.index_store import PostgresArchiveIndex
from archive_cli.store import DefaultArchiveStore
from archive_cli.serving_index import (
    get_serving_handle,
    mark_serving_index_dirty,
    serving_index_status,
    verify_serving_index,
)

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate  # noqa: E402


def _write_mini_export(work: Path, *, dim: int = 4) -> None:
    cards = [
        {
            "card_uid": "hfa-person-aaaabbbbcccc",
            "rel_path": "People/jane-smith.md",
            "summary": "Jane Smith",
            "type": "person",
            "slug": "jane-smith",
            "activity_at": "2026-03-06T00:00:00Z",
            "search_text": "Jane Smith Endaoment",
            "people": ["Jane Smith"],
            "sources": ["linkedin"],
            "orgs": ["Endaoment"],
            "corpus_state": "active",
            "aliases": ["jane"],
            "emails": [],
        },
        {
            "card_uid": "hfa-email-111122223333",
            "rel_path": "Emails/note.md",
            "summary": "Board update",
            "type": "email_message",
            "slug": "board-update",
            "activity_at": "2026-03-07T00:00:00Z",
            "search_text": "board update quarterly",
            "people": ["Jane Smith"],
            "sources": ["gmail"],
            "orgs": [],
            "corpus_state": "active",
            "aliases": [],
            "emails": [],
        },
    ]
    (work / "cards.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cards), encoding="utf-8")
    chunks = [
        {"chunk_key": "ck-jane", "card_uid": "hfa-person-aaaabbbbcccc", "chunk_type": "summary", "chunk_index": 0},
        {"chunk_key": "ck-email", "card_uid": "hfa-email-111122223333", "chunk_type": "body", "chunk_index": 0},
    ]
    (work / "chunks.jsonl").write_text("".join(json.dumps(c) + "\n" for c in chunks), encoding="utf-8")
    (work / "edges.jsonl").write_text(
        json.dumps(
            {
                "source_uid": "hfa-email-111122223333",
                "target_uid": "hfa-person-aaaabbbbcccc",
                "edge_type": "mentions",
                "field_name": "people",
                "trust": 1.0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    keys = ["ck-jane", "ck-email"]
    (work / "embedding_keys.txt").write_text("\n".join(keys), encoding="utf-8")
    jane = [1.0, 0.0, 0.0, 0.0]
    email = [0.0, 1.0, 0.0, 0.0]
    with (work / "embeddings.bin").open("wb") as fh:
        fh.write(struct.pack(f"<{dim}f", *jane))
        fh.write(struct.pack(f"<{dim}f", *email))


def _publish_mini(root: Path) -> str:
    gid = "gen-test"
    dest = root / "generations" / gid
    dest.mkdir(parents=True)
    work = dest / "_inbox"
    work.mkdir()
    _write_mini_export(work)
    archive_crate.serving_index_build(
        str(dest),
        str(work / "cards.jsonl"),
        str(work / "chunks.jsonl"),
        str(work / "embedding_keys.txt"),
        str(work / "embeddings.bin"),
        4,
        str(work / "edges.jsonl"),
    )
    archive_crate.serving_index_publish(str(root), gid)
    archive_crate.serving_index_truncate_dirty(str(root))
    return gid


def test_dirty_publish_and_status(tmp_path, monkeypatch) -> None:
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    mark_serving_index_dirty(tmp_path, "test", ["hfa-person-aaaabbbbcccc"])
    status = serving_index_status(tmp_path)
    assert status["serving_index_ready"] is False
    assert int(status["serving_index_dirty_records"]) >= 1
    gid = _publish_mini(root)
    status = serving_index_status(tmp_path)
    assert status["serving_index_generation"] == gid
    assert status["serving_index_ready"] is True


def test_search_query_hybrid_on_mini_index(tmp_path, monkeypatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    _publish_mini(root)
    handle = get_serving_handle(tmp_path)
    rows = handle.search("Jane", limit=5)
    assert any(r.get("card_uid") == "hfa-person-aaaabbbbcccc" for r in rows)
    listed = handle.query(type_filter="person", limit=5)
    assert listed and listed[0]["type"] == "person"
    vec_rows = handle.vector([1.0, 0.0, 0.0, 0.0], limit=5)
    assert vec_rows
    hybrid = handle.hybrid("Jane", [1.0, 0.0, 0.0, 0.0], limit=5)
    assert hybrid
    person = handle.person("Jane Smith")
    assert person.get("found") is True
    graph = handle.graph("People/jane-smith.md", hops=1)
    assert graph
    verified = verify_serving_index(tmp_path)
    assert verified["ok"] is True


def test_slo_clocks_mini_index(tmp_path, monkeypatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    _publish_mini(root)
    handle = get_serving_handle(tmp_path)
    t0 = time.monotonic()
    handle.search("Jane", limit=5)
    handle.query(limit=5)
    handle.hybrid("Jane", [1.0, 0.0, 0.0, 0.0], limit=5)
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    assert elapsed_ms < 250.0


def _card(
    uid: str,
    *,
    rel_path: str,
    summary: str,
    type_: str,
    activity_at: str,
    activity_end_at: str = "",
    people: list[str] | None = None,
    sources: list[str] | None = None,
) -> dict:
    return {
        "card_uid": uid,
        "rel_path": rel_path,
        "summary": summary,
        "type": type_,
        "slug": uid.replace("hfa-", ""),
        "activity_at": activity_at,
        "activity_end_at": activity_end_at,
        "search_text": summary,
        "people": people or [],
        "sources": sources or [],
        "orgs": [],
        "corpus_state": "active",
        "aliases": [],
        "emails": [],
    }


def _publish_temporal(root: Path) -> None:
    gid = "gen-temporal"
    dest = root / "generations" / gid
    dest.mkdir(parents=True)
    work = dest / "_inbox"
    work.mkdir()
    cards = [
        _card(
            "hfa-email-mar05",
            rel_path="Emails/mar05.md",
            summary="March fifth",
            type_="email_message",
            activity_at="2026-03-05T09:00:00Z",
            sources=["gmail"],
        ),
        _card(
            "hfa-person-aaaabbbbcccc",
            rel_path="People/jane-smith.md",
            summary="Jane Smith",
            type_="person",
            activity_at="2026-03-06T00:00:00Z",
            people=["Jane Smith"],
        ),
        _card(
            "hfa-email-111122223333",
            rel_path="Emails/note.md",
            summary="Board update",
            type_="email_message",
            activity_at="2026-03-07T00:00:00Z",
            people=["Jane Smith"],
            sources=["gmail"],
        ),
        _card(
            "hfa-flight-span",
            rel_path="Travel/flight.md",
            summary="SFO to JFK",
            type_="flight",
            activity_at="2026-03-08T16:00:00Z",
            activity_end_at="2026-03-12T06:00:00Z",
        ),
        _card(
            "hfa-email-mar11",
            rel_path="Emails/mar11.md",
            summary="March eleventh",
            type_="email_message",
            activity_at="2026-03-11T15:00:00Z",
            sources=["gmail"],
        ),
    ]
    (work / "cards.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cards), encoding="utf-8")
    (work / "chunks.jsonl").write_text("", encoding="utf-8")
    (work / "edges.jsonl").write_text("", encoding="utf-8")
    (work / "embedding_keys.txt").write_text("", encoding="utf-8")
    (work / "embeddings.bin").write_bytes(b"")
    archive_crate.serving_index_build(
        str(dest),
        str(work / "cards.jsonl"),
        str(work / "chunks.jsonl"),
        str(work / "embedding_keys.txt"),
        str(work / "embeddings.bin"),
        4,
        str(work / "edges.jsonl"),
    )
    archive_crate.serving_index_publish(str(root), gid)


def test_temporal_neighbors_keyset_and_interval(tmp_path, monkeypatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    _publish_temporal(root)
    handle = get_serving_handle(tmp_path)

    both = handle.temporal_neighbors("2026-03-10", direction="both", limit=4)
    assert both["ok"] is True
    uids = [row["uid"] for row in both["results"]]
    assert "hfa-flight-span" in uids
    assert both["results"][0]["leg"] == "during"
    assert both["results"][0]["uid"] == "hfa-flight-span"

    forward = handle.temporal_neighbors("2026-03-10T00:00:00Z", direction="forward", limit=2)
    assert [row["uid"] for row in forward["results"]] == ["hfa-email-mar11"]
    assert {row["leg"] for row in forward["results"]} == {"forward"}

    backward = handle.temporal_neighbors("2026-03-07T00:00:00Z", direction="backward", limit=2)
    assert [row["uid"] for row in backward["results"]] == ["hfa-email-111122223333", "hfa-person-aaaabbbbcccc"]

    filtered = handle.temporal_neighbors(
        "2026-03-10",
        direction="forward",
        limit=5,
        type_filter="email_message",
    )
    assert [row["uid"] for row in filtered["results"]] == ["hfa-email-mar11"]

    bad = handle.temporal_neighbors("not-a-timestamp")
    assert bad["ok"] is False
    assert bad["error"] == "invalid_timestamp"


def test_temporal_neighbors_skips_full_scan(tmp_path, monkeypatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    gid = "gen-many"
    dest = root / "generations" / gid
    dest.mkdir(parents=True)
    work = dest / "_inbox"
    work.mkdir()
    cards = [
        _card(
            f"hfa-note-{i:05d}",
            rel_path=f"Notes/n{i:05d}.md",
            summary=f"note {i}",
            type_="note",
            activity_at="2024-01-01T00:00:00Z" if i < 2500 else "2026-06-01T00:00:00Z",
        )
        for i in range(5000)
    ]
    cards.append(
        _card(
            "hfa-note-pivot",
            rel_path="Notes/pivot.md",
            summary="pivot",
            type_="note",
            activity_at="2025-06-15T12:00:00Z",
        )
    )
    (work / "cards.jsonl").write_text("".join(json.dumps(c) + "\n" for c in cards), encoding="utf-8")
    (work / "chunks.jsonl").write_text("", encoding="utf-8")
    (work / "edges.jsonl").write_text("", encoding="utf-8")
    (work / "embedding_keys.txt").write_text("", encoding="utf-8")
    (work / "embeddings.bin").write_bytes(b"")
    archive_crate.serving_index_build(
        str(dest),
        str(work / "cards.jsonl"),
        str(work / "chunks.jsonl"),
        str(work / "embedding_keys.txt"),
        str(work / "embeddings.bin"),
        4,
        str(work / "edges.jsonl"),
    )
    archive_crate.serving_index_publish(str(root), gid)
    handle = get_serving_handle(tmp_path)
    t0 = time.monotonic()
    result = handle.temporal_neighbors("2025-06-15T12:00:00Z", direction="both", limit=6)
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    assert result["ok"] is True
    assert result["results"][0]["uid"] == "hfa-note-pivot"
    assert elapsed_ms < 50.0


def test_warehouse_store_fails_closed_without_active(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(tmp_path / "missing-index"))
    idx = object.__new__(PostgresArchiveIndex)
    store = DefaultArchiveStore(vault=tmp_path, index=idx)
    with pytest.raises(ServingIndexUnavailableError):
        store.search("hello")

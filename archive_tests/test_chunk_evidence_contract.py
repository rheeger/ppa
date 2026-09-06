"""P01-B format freeze: ChunkEvidenceRef Python / JSON / Rust round trip."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from archive_engine.contracts import (
    CHUNK_EVIDENCE_REF_VERSION,
    ChunkEvidenceRef,
    MessageEvidenceRef,
    SourceSpan,
)
from archive_engine.errors import IncompatibleContractError
from archive_tests.acceptance.ann_corpus import populated_burst_evidence

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

import archive_crate  # noqa: E402


def _publish_with_evidence(root: Path, ref: ChunkEvidenceRef) -> str:
    gid = "gen-evidence"
    dest = root / "generations" / gid
    dest.mkdir(parents=True)
    work = dest / "_inbox"
    work.mkdir()
    card = {
        "card_uid": ref.card_uid,
        "rel_path": "Notes/evidence.md",
        "summary": "evidence",
        "type": "note",
        "slug": "evidence",
        "activity_at": "2026-09-06T00:00:00Z",
        "search_text": "evidence café",
        "people": [],
        "sources": [],
        "orgs": [],
        "corpus_state": "active",
    }
    chunk = {
        "chunk_key": ref.chunk_id,
        "card_uid": ref.card_uid,
        "chunk_type": "body",
        "chunk_index": 0,
        "evidence": ref.to_payload(),
    }
    (work / "cards.jsonl").write_text(json.dumps(card) + "\n", encoding="utf-8")
    (work / "chunks.jsonl").write_text(json.dumps(chunk) + "\n", encoding="utf-8")
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
    return gid


def test_multibyte_and_crlf_spans_round_trip() -> None:
    text = "café\r\nuse port 8432"
    encoded = text.encode("utf-8")
    start = encoded.index("café".encode("utf-8"))
    end = start + len("café".encode("utf-8"))
    span = SourceSpan(
        source_uid="hfa-email-p01bspan0001",
        source_message_id="m1",
        source_revision="sha256:span",
        representation="canonical_body_utf8",
        start_byte=start,
        end_byte=end,
    )
    assert encoded[span.start_byte:span.end_byte] == "café".encode("utf-8")
    ref = ChunkEvidenceRef(
        version=CHUNK_EVIDENCE_REF_VERSION,
        archive_id="p01b-freeze",
        card_uid="hfa-email-p01bspan0001",
        chunk_id="ck-span",
        chunk_schema_version="6",
        algorithm_version="p01b-freeze-1",
        evidence_kind="source_reported",
        lineage_complete=True,
        source_spans=(span,),
        span_unavailable=False,
        message_refs=(
            MessageEvidenceRef(message_id="m1", source_revision="sha256:span", spans=(span,)),
        ),
        message_refs_available=True,
    )
    loaded = ChunkEvidenceRef.from_payload(ref.to_payload())
    assert loaded == ref
    assert loaded.source_spans[0].representation == "canonical_body_utf8"


def test_legacy_unavailable_and_unknown_version() -> None:
    legacy = ChunkEvidenceRef.from_payload(
        {
            "version": 1,
            "archive_id": "aid",
            "card_uid": "uid",
            "chunk_id": "c1",
            "chunk_schema_version": "6",
            "algorithm_version": "p01b-freeze-1",
        }
    )
    assert legacy.span_unavailable is True
    assert legacy.message_refs_available is False
    assert legacy.evidence_kind == "unknown"
    with pytest.raises(IncompatibleContractError, match="unsupported ChunkEvidenceRef"):
        ChunkEvidenceRef.from_payload(
            {
                "version": 99,
                "archive_id": "aid",
                "card_uid": "uid",
                "chunk_id": "c1",
                "chunk_schema_version": "6",
                "algorithm_version": "x",
            }
        )


def test_rust_returns_populated_burst_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from archive_cli import serving_index as si

    si._HANDLE = None
    root = tmp_path / "rust-search-index"
    monkeypatch.setenv("PPA_SERVING_INDEX_PATH", str(root))
    ref = populated_burst_evidence("hfa-note-p01bburst001", "ck-burst")
    _publish_with_evidence(root, ref)
    handle = si.get_serving_handle(tmp_path)
    loaded = archive_crate.serving_index_chunk_evidence(handle._native, "ck-burst")
    assert loaded
    again = ChunkEvidenceRef.from_payload(loaded)
    assert again.card_uid == ref.card_uid
    assert again.parent_thread == "thread-p01b"
    assert again.span_unavailable is False
    assert again.source_spans[0].end_byte > again.source_spans[0].start_byte
    assert again.message_refs_available is True


def test_invalid_span_rejected_by_python() -> None:
    with pytest.raises(IncompatibleContractError, match="half-open"):
        SourceSpan.from_payload(
            {
                "source_uid": "u",
                "source_revision": "r",
                "start_byte": 8,
                "end_byte": 2,
            }
        )

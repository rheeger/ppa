"""Parity tests for archive_crate hashing vs archive_cli.vault_cache."""

import hashlib

import pytest

pytest.importorskip("archive_crate", reason="build with: cd archive_crate && maturin develop")

from archive_cli.vault_cache import _content_hash


def test_raw_content_sha256_matches_hashlib():
    import archive_crate

    content = "---\ntype: email_message\n---\nBody text\n"
    expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
    assert archive_crate.raw_content_sha256(content.encode("utf-8")) == expected


def test_content_hash_matches_vault_cache():
    import archive_crate

    fm = {"type": "email_message", "uid": "hfa-test-1", "summary": "Hello"}
    body = "Plain body\n"
    assert archive_crate.content_hash(fm, body) == _content_hash(fm, body)


def test_content_hash_nul_body_stripped():
    import archive_crate

    fm = {"type": "x"}
    body = "a\x00b"
    assert archive_crate.content_hash(fm, body) == _content_hash(fm, body)

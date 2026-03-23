from hfa.hasher import file_hash


def test_file_hash_returns_sha256_prefix(tmp_path):
    path = tmp_path / "sample.txt"
    path.write_text("hello", encoding="utf-8")
    assert file_hash(str(path)).startswith("sha256:")

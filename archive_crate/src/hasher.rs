//! Content hashing — matches `archive_cli.vault_cache` `_content_hash` and raw file SHA-256.

use crate::json_stable;
use pyo3::prelude::*;
use sha2::{Digest, Sha256};

/// SHA-256 hex digest of raw file bytes (same as `hashlib.sha256(content.encode("utf-8")).hexdigest()`).
#[pyfunction]
pub fn raw_content_sha256(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Logical content fingerprint: same algorithm as `archive_cli.vault_cache._content_hash` (Step 4a — pure Rust).
#[pyfunction]
pub fn content_hash(frontmatter: &Bound<'_, PyAny>, body: &str) -> PyResult<String> {
    let py = frontmatter.py();
    let v = json_stable::json_value_from_py_any(py, frontmatter)?;
    json_stable::content_hash_from_value(v, body).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })
}

//! Text chunking — native Rust (parity with `archive_cli.chunking`).

use pyo3::prelude::*;

use crate::json_stable;

/// Parity with ``archive_cli.chunk_builders._chunk_hash`` (tests / debugging).
#[pyfunction]
pub fn chunk_hash(chunk_type: String, content: String, source_fields: Vec<String>) -> String {
    crate::chunk::helpers::chunk_hash_current(&chunk_type, &content, &source_fields)
}

/// Matches ``archive_cli.chunking.render_chunks_for_card``.
#[pyfunction]
pub fn render_chunks_for_card(
    py: Python<'_>,
    frontmatter: &Bound<'_, PyAny>,
    body: &str,
) -> PyResult<PyObject> {
    let fm_val = json_stable::json_value_from_py_any(py, frontmatter)?;
    let records = crate::chunk::build_chunks(&fm_val, body);
    crate::chunk::chunk_records_to_py_list(py, &records)
}

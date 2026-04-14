//! Python delegation for Phase 2.9 steps 13–14, 18 (`archive_cli.crate_bridge`).

use pyo3::prelude::*;

/// Delegates to :func:`archive_cli.crate_bridge.rebuild_index` (full Postgres rebuild via Python loader).
#[pyfunction]
pub fn rebuild_index(py: Python<'_>) -> PyResult<PyObject> {
    py.import_bound("archive_cli.crate_bridge")?
        .getattr("rebuild_index")?
        .call0()?
        .extract::<PyObject>()
}

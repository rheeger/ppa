//! PyO3 progress callback bridge — operational logging (plan § Operational logging).

use pyo3::prelude::*;

/// Optional Python callable ``(phase: str, current: int, total: int, msg: str) -> None``.
#[pyclass]
pub struct ProgressCallback {
    callback: Option<PyObject>,
}

#[pymethods]
impl ProgressCallback {
    #[new]
    #[pyo3(signature = (callback=None))]
    fn new(callback: Option<PyObject>) -> Self {
        Self { callback }
    }

    fn emit(
        &self,
        py: Python<'_>,
        phase: String,
        current: u64,
        total: u64,
        msg: String,
    ) -> PyResult<()> {
        if let Some(ref cb) = self.callback {
            cb.call1(py, (phase, current, total, msg))?;
        }
        Ok(())
    }
}

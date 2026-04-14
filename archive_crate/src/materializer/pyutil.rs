//! Python truthiness / `or` semantics without `Python::eval` (PyO3 0.22).

use pyo3::prelude::*;

pub fn python_or(py: Python<'_>, a: &Bound<'_, PyAny>, b: &Bound<'_, PyAny>) -> PyResult<PyObject> {
    let builtins = py.import_bound("builtins")?;
    let truthy: bool = builtins.getattr("bool")?.call1((a,))?.extract()?;
    if truthy {
        Ok(a.clone().unbind())
    } else {
        Ok(b.clone().unbind())
    }
}

pub fn python_or3(
    py: Python<'_>,
    a: &Bound<'_, PyAny>,
    b: &Bound<'_, PyAny>,
    c: &Bound<'_, PyAny>,
) -> PyResult<PyObject> {
    let builtins = py.import_bound("builtins")?;
    let truthy_a: bool = builtins.getattr("bool")?.call1((a,))?.extract()?;
    if truthy_a {
        return Ok(a.clone().unbind());
    }
    let truthy_b: bool = builtins.getattr("bool")?.call1((b,))?.extract()?;
    if truthy_b {
        return Ok(b.clone().unbind());
    }
    Ok(c.clone().unbind())
}

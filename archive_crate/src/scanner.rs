//! Vault scan metadata — paths+fingerprint (step 7) and ``cards_by_type`` from vault-scan SQLite.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;

use crate::cache;

/// Same algorithm as ``archive_cli.scanner._vault_paths_and_fingerprint`` — stat each path, hash sorted lines.
///
/// Pass the same ``rel_paths`` you used from ``archive_vault.vault.iter_note_paths`` (or ``VaultScanCache.all_rel_paths()``) so the fingerprint matches Python.
#[pyfunction]
pub fn vault_paths_and_fingerprint(
    py: Python<'_>,
    vault_path: String,
    rel_paths: Vec<String>,
) -> PyResult<(PyObject, String)> {
    let vault = Path::new(&vault_path);
    let (stats, fp) = cache::compute_vault_fingerprint(vault, &rel_paths)
        .map_err(PyErr::new::<pyo3::exceptions::PyValueError, _>)?;
    let dict = PyDict::new_bound(py);
    for (k, (mt, sz)) in stats {
        let tup = PyTuple::new_bound(py, [mt, sz]);
        dict.set_item(k, tup)?;
    }
    Ok((dict.to_object(py), fp))
}

fn cards_by_type_from_cache_impl(
    py: Python<'_>,
    cache_path: String,
    types: Option<Vec<String>>,
) -> PyResult<PyObject> {
    let conn = Connection::open(&cache_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();

    let use_filter = matches!(&types, Some(ts) if !ts.is_empty());

    if use_filter {
        let ts = types.as_ref().unwrap();
        let ph = ts.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT card_type, rel_path FROM notes WHERE card_type IN ({ph}) ORDER BY rel_path"
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(ts.iter().map(|s| s.as_str())))
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
        while let Some(row) = rows
            .next()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?
        {
            let ct: String = row
                .get(0)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
            let rp: String = row
                .get(1)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
            map.entry(ct).or_default().push(rp);
        }
    } else {
        let mut stmt = conn
            .prepare(
                "SELECT card_type, rel_path FROM notes WHERE card_type != '' ORDER BY rel_path",
            )
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
        let mut rows = stmt
            .query([])
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
        while let Some(row) = rows
            .next()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?
        {
            let ct: String = row
                .get(0)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
            let rp: String = row
                .get(1)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(format!("{e}")))?;
            map.entry(ct).or_default().push(rp);
        }
    }

    let out = PyDict::new_bound(py);
    for (k, v) in map {
        out.set_item(k, v)?;
    }
    Ok(out.to_object(py))
}

/// Group ``rel_path`` by ``card_type`` from a tier ≥1 ``vault-scan-cache.sqlite3`` (same layout as Python).
///
/// ``types`` — when ``Some`` and non-empty, restrict to those card types; when ``None`` or empty, all non-empty types.
#[pyfunction]
#[pyo3(signature = (cache_path, types=None))]
pub fn cards_by_type_from_cache(
    py: Python<'_>,
    cache_path: String,
    types: Option<Vec<String>>,
) -> PyResult<PyObject> {
    cards_by_type_from_cache_impl(py, cache_path, types)
}

/// Plan name for ``cards_by_type_from_cache`` — same implementation (SQLite index from vault scan cache).
#[pyfunction]
#[pyo3(signature = (cache_path, types=None))]
pub fn cards_by_type(
    py: Python<'_>,
    cache_path: String,
    types: Option<Vec<String>>,
) -> PyResult<PyObject> {
    cards_by_type_from_cache_impl(py, cache_path, types)
}

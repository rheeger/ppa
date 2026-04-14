//! Step 15a — Rust cache-backed note iteration.
//!
//! Reads tier-2 SQLite rows and returns them to Python without per-note file I/O or YAML parsing.
//! GIL is released during the SQLite query + zlib decompress + JSON parse; reacquired only to
//! build the final Python list.

use flate2::read::ZlibDecoder;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rusqlite::Connection;
use std::io::Read;

/// A single note row from the tier-2 cache, fully decompressed and parsed in Rust.
struct CachedNote {
    rel_path: String,
    frontmatter_json: String,
    body: Option<String>,
}

fn zlib_decompress(data: &[u8]) -> Result<String, String> {
    let mut decoder = ZlibDecoder::new(data);
    let mut out = String::new();
    decoder
        .read_to_string(&mut out)
        .map_err(|e| format!("zlib decompress: {e}"))?;
    Ok(out)
}

fn query_notes_inner(
    cache_path: &str,
    types: &Option<Vec<String>>,
    prefix: &Option<String>,
    include_body: bool,
) -> Result<Vec<CachedNote>, String> {
    let conn =
        Connection::open(cache_path).map_err(|e| format!("open cache: {e}"))?;

    let body_col = if include_body {
        "body_compressed"
    } else {
        "NULL"
    };

    let mut conditions: Vec<String> = Vec::new();
    let mut param_values: Vec<String> = Vec::new();

    if let Some(ts) = types {
        if !ts.is_empty() {
            let placeholders: Vec<&str> = ts.iter().map(|_| "?").collect();
            conditions.push(format!("card_type IN ({})", placeholders.join(",")));
            param_values.extend(ts.iter().cloned());
        }
    }

    if let Some(pfx) = prefix {
        if !pfx.is_empty() {
            conditions.push("rel_path LIKE ?".to_string());
            let like_val = format!("{}%", pfx.replace('%', "\\%").replace('_', "\\_"));
            param_values.push(like_val);
        }
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    let sql = format!(
        "SELECT rel_path, frontmatter_json, {body_col} FROM notes{where_clause} ORDER BY rel_path"
    );

    let mut stmt = conn.prepare(&sql).map_err(|e| format!("prepare: {e}"))?;

    let param_refs: Vec<&dyn rusqlite::types::ToSql> = param_values
        .iter()
        .map(|s| s as &dyn rusqlite::types::ToSql)
        .collect();

    let mut rows = stmt
        .query(param_refs.as_slice())
        .map_err(|e| format!("query: {e}"))?;

    let mut out: Vec<CachedNote> = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("row: {e}"))? {
        let rel_path: String = row.get(0).map_err(|e| format!("col0: {e}"))?;
        let fm_json: String = row.get(1).map_err(|e| format!("col1: {e}"))?;

        let body = if include_body {
            let blob: Option<Vec<u8>> = row.get(2).map_err(|e| format!("col2: {e}"))?;
            match blob {
                Some(data) if !data.is_empty() => Some(zlib_decompress(&data)?),
                _ => Some(String::new()),
            }
        } else {
            None
        };

        out.push(CachedNote {
            rel_path,
            frontmatter_json: fm_json,
            body,
        });
    }
    Ok(out)
}

/// Read tier-2 cache rows and return `list[dict]` with keys `rel_path`, `frontmatter` (parsed
/// JSON dict), and `body` (decompressed string).  Type-filter and/or prefix-filter via SQL.
///
/// GIL released during SQLite read + zlib decompress.
#[pyfunction]
#[pyo3(signature = (cache_path, types=None, prefix=None))]
pub fn notes_from_cache(
    py: Python<'_>,
    cache_path: String,
    types: Option<Vec<String>>,
    prefix: Option<String>,
) -> PyResult<PyObject> {
    let rows = py
        .allow_threads(|| query_notes_inner(&cache_path, &types, &prefix, true))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let json_mod = py.import_bound("json")?;
    let json_loads = json_mod.getattr("loads")?;

    let list = PyList::empty_bound(py);
    for row in &rows {
        let d = PyDict::new_bound(py);
        d.set_item("rel_path", &row.rel_path)?;
        let fm_obj = json_loads.call1((&row.frontmatter_json,))?;
        d.set_item("frontmatter", fm_obj)?;
        if let Some(body) = &row.body {
            d.set_item("body", body)?;
        }
        list.append(d)?;
    }
    Ok(list.to_object(py))
}

/// Frontmatter-only read from tier-2 cache — no body decompression.
///
/// Returns `list[dict]` with keys `rel_path` and `frontmatter`.
#[pyfunction]
#[pyo3(signature = (cache_path, types=None, prefix=None))]
pub fn frontmatter_dicts_from_cache(
    py: Python<'_>,
    cache_path: String,
    types: Option<Vec<String>>,
    prefix: Option<String>,
) -> PyResult<PyObject> {
    let rows = py
        .allow_threads(|| query_notes_inner(&cache_path, &types, &prefix, false))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let json_mod = py.import_bound("json")?;
    let json_loads = json_mod.getattr("loads")?;

    let list = PyList::empty_bound(py);
    for row in &rows {
        let d = PyDict::new_bound(py);
        d.set_item("rel_path", &row.rel_path)?;
        let fm_obj = json_loads.call1((&row.frontmatter_json,))?;
        d.set_item("frontmatter", fm_obj)?;
        list.append(d)?;
    }
    Ok(list.to_object(py))
}

/// Return `list[str]` of `rel_path` values from cache, optionally filtered by card type(s) and/or
/// path prefix.
#[pyfunction]
#[pyo3(signature = (cache_path, types=None, prefix=None))]
pub fn note_paths_from_cache(
    py: Python<'_>,
    cache_path: String,
    types: Option<Vec<String>>,
    prefix: Option<String>,
) -> PyResult<PyObject> {
    let paths = py
        .allow_threads(|| -> Result<Vec<String>, String> {
            let conn = Connection::open(&cache_path)
                .map_err(|e| format!("open cache: {e}"))?;

            let mut conditions: Vec<String> = Vec::new();
            let mut param_values: Vec<String> = Vec::new();

            if let Some(ts) = &types {
                if !ts.is_empty() {
                    let ph: Vec<&str> = ts.iter().map(|_| "?").collect();
                    conditions.push(format!("card_type IN ({})", ph.join(",")));
                    param_values.extend(ts.iter().cloned());
                }
            }

            if let Some(pfx) = &prefix {
                if !pfx.is_empty() {
                    conditions.push("rel_path LIKE ?".to_string());
                    let like_val =
                        format!("{}%", pfx.replace('%', "\\%").replace('_', "\\_"));
                    param_values.push(like_val);
                }
            }

            let where_clause = if conditions.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", conditions.join(" AND "))
            };

            let sql = format!("SELECT rel_path FROM notes{where_clause} ORDER BY rel_path");

            let mut stmt = conn.prepare(&sql).map_err(|e| format!("prepare: {e}"))?;
            let param_refs: Vec<&dyn rusqlite::types::ToSql> = param_values
                .iter()
                .map(|s| s as &dyn rusqlite::types::ToSql)
                .collect();

            let mut rows = stmt
                .query(param_refs.as_slice())
                .map_err(|e| format!("query: {e}"))?;

            let mut out: Vec<String> = Vec::new();
            while let Some(row) = rows.next().map_err(|e| format!("row: {e}"))? {
                let rp: String = row.get(0).map_err(|e| format!("col0: {e}"))?;
                out.push(rp);
            }
            Ok(out)
        })
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let list = PyList::new_bound(py, &paths);
    Ok(list.to_object(py))
}

//! `_build_search_text` and `_content_hash` parity with `archive_cli.materializer`.

use crate::json_stable;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use regex::Regex;
use serde_json::Map;
use serde_json::Value;
use std::sync::OnceLock;

fn ws_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s+").expect("regex"))
}

fn clean_text(value: &str) -> String {
    let sanitized = value.replace('\0', "");
    let s = sanitized.trim();
    ws_re().replace_all(s, " ").trim().to_string()
}

fn iter_string_values(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = value.extract::<String>() {
        let cleaned = s.replace('\0', "").trim().to_string();
        return Ok(if cleaned.is_empty() {
            vec![]
        } else {
            vec![cleaned]
        });
    }
    if let Ok(seq) = value.downcast::<PyList>() {
        let mut out = Vec::new();
        for item in seq.iter() {
            out.extend(iter_string_values(py, &item)?);
        }
        return Ok(out);
    }
    if let Ok(d) = value.downcast::<PyDict>() {
        let mut out = Vec::new();
        for (_k, v) in d.iter() {
            out.extend(iter_string_values(py, &v)?);
        }
        return Ok(out);
    }
    Ok(vec![])
}

pub fn build_search_text_value(fm: &Map<String, Value>, body: &str) -> String {
    let mut parts: Vec<String> = Vec::new();
    for (k, v) in fm.iter() {
        if k == "uid" {
            continue;
        }
        for text in crate::materializer::fm_value::iter_string_values_json(v) {
            parts.push(clean_text(&text));
        }
    }
    let body_cleaned = body.replace('\0', "").trim().to_string();
    if !body_cleaned.is_empty() {
        parts.push(body_cleaned);
    }
    parts.join("\n")
}

#[pyfunction]
pub fn build_search_text(
    py: Python<'_>,
    frontmatter: &Bound<'_, PyDict>,
    body: &str,
) -> PyResult<String> {
    let mut parts: Vec<String> = Vec::new();
    for (k, v) in frontmatter.iter() {
        let key: String = k.extract()?;
        if key == "uid" {
            continue;
        }
        for text in iter_string_values(py, &v)? {
            parts.push(clean_text(&text));
        }
    }
    let body_cleaned = body.replace('\0', "").trim().to_string();
    if !body_cleaned.is_empty() {
        parts.push(body_cleaned);
    }
    Ok(parts.join("\n"))
}

pub fn content_hash_value(fm: Value, body: &str) -> Result<String, serde_json::Error> {
    json_stable::content_hash_from_value(fm, body)
}

/// Matches `archive_cli.vault_cache._content_hash` (pure Rust, same as `hasher::content_hash`).
#[pyfunction(name = "materialize_content_hash")]
pub fn content_hash(
    py: Python<'_>,
    frontmatter: &Bound<'_, PyAny>,
    body: &str,
) -> PyResult<String> {
    let v = json_stable::json_value_from_py_any(py, frontmatter)?;
    content_hash_value(v, body).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))
}

//! Frontmatter split + YAML parse — mirrors `archive_vault.yaml_parser.parse_frontmatter` (regex + ruamel YAML).

use pyo3::prelude::*;
use pyo3::types::PyDict;
use regex::Regex;
use serde_json::Value as JsonValue;

fn split_regex() -> Regex {
    Regex::new(r"(?s)^---\s*\n(.*?)\n---\s*\n?(.*)$").expect("valid regex")
}

/// Split file content into (frontmatter_yaml_text, body) when `---` fences exist; else `None`.
#[allow(dead_code)] // used by future cache + Rust unit tests
pub fn split_frontmatter_text(content: &str) -> Option<(String, String)> {
    let re = split_regex();
    let caps = re.captures(content)?;
    let fm = caps.get(1)?.as_str().to_string();
    let body = caps.get(2)?.as_str().to_string();
    Some((fm, body))
}

fn json_value_to_py_dict(py: Python<'_>, v: &JsonValue) -> PyResult<PyObject> {
    let json_str = serde_json::to_string(v).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("JSON encode: {e}"))
    })?;
    let json_mod = py.import_bound("json")?;
    let loads = json_mod.getattr("loads")?;
    let obj = loads.call1((json_str,))?;
    Ok(obj.unbind())
}

/// Parse YAML frontmatter + body; returns `(frontmatter_dict, body)` like `archive_vault.yaml_parser.parse_frontmatter`.
#[pyfunction]
pub fn parse_frontmatter(py: Python<'_>, content: &str) -> PyResult<(PyObject, String)> {
    let empty_dict = PyDict::new_bound(py);
    let re = split_regex();
    let Some(caps) = re.captures(content) else {
        return Ok((empty_dict.to_object(py), content.to_string()));
    };
    let fm_text = caps.get(1).unwrap().as_str();
    let body = caps.get(2).unwrap().as_str().to_string();
    if fm_text.trim().is_empty() {
        return Ok((empty_dict.to_object(py), body));
    }

    let v: JsonValue = serde_yaml::from_str(fm_text).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("YAML frontmatter: {e}"))
    })?;

    match v {
        JsonValue::Null => Ok((empty_dict.to_object(py), body)),
        JsonValue::Object(_) => {
            let obj = json_value_to_py_dict(py, &v)?;
            Ok((obj, body))
        }
        _ => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Frontmatter must parse to a mapping",
        )),
    }
}

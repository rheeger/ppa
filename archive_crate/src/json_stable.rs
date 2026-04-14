//! Canonical JSON for frontmatter hashing — matches `archive_cli.vault_cache` `_frontmatter_hash_stable` / `_content_hash`.
//!
//! Python uses `json.dumps(..., sort_keys=True, default=str)` with default separators `(", ", ": ")`, not serde_json's compact form.
//! String escaping must match Python `ensure_ascii=True` (`\uXXXX` for non-ASCII), not serde_json’s default UTF-8 in strings.

use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList, PyTuple};
use serde_json::{Map, Value as JsonValue};
use sha2::{Digest, Sha256};

/// JSON string literal matching Python 3 `json.dumps` with `ensure_ascii=True` (UTF-16 `\u` escapes for non-ASCII).
pub fn python_escape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    out.push('"');
    for ch in s.chars() {
        let code = ch as u32;
        if code < 0x20 {
            match ch {
                '\u{8}' => out.push_str("\\b"),
                '\t' => out.push_str("\\t"),
                '\n' => out.push_str("\\n"),
                '\u{c}' => out.push_str("\\f"),
                '\r' => out.push_str("\\r"),
                _ => out.push_str(&format!("\\u{:04x}", code)),
            }
        } else if code < 0x7f {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                c => out.push(c),
            }
        } else {
            let mut buf = [0u16; 2];
            for unit in ch.encode_utf16(&mut buf).iter().copied() {
                out.push_str(&format!("\\u{:04x}", unit));
            }
        }
    }
    out.push('"');
    out
}

/// Sort object keys recursively (for inputs that may not be ordered).
pub fn sort_json_value(v: JsonValue) -> JsonValue {
    match v {
        JsonValue::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            let mut out = Map::new();
            for k in keys {
                if let Some(child) = map.get(&k) {
                    out.insert(k, sort_json_value(child.clone()));
                }
            }
            JsonValue::Object(out)
        }
        JsonValue::Array(arr) => JsonValue::Array(arr.into_iter().map(sort_json_value).collect()),
        _ => v,
    }
}

/// Matches Python `json.dumps(x, sort_keys=True, default=str)` for `serde_json::Value` inputs.
/// Uses default separators `(", ", ": ")` and `ensure_ascii`-style string escaping via `serde_json::to_string` for scalars.
pub fn python_style_json_dumps_sorted(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "null".to_string(),
        JsonValue::Bool(b) => {
            if *b {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        JsonValue::Number(n) => serde_json::to_string(&JsonValue::Number(n.clone()))
            .expect("number JSON"),
        JsonValue::String(s) => python_escape_json_string(s),
        JsonValue::Array(arr) => {
            let inner: Vec<String> = arr.iter().map(python_style_json_dumps_sorted).collect();
            format!("[{}]", inner.join(", "))
        }
        JsonValue::Object(map) => {
            let mut keys: Vec<String> = map.keys().cloned().collect();
            keys.sort();
            let mut pairs = Vec::new();
            for k in keys {
                let val = map.get(&k).expect("key present");
                let key_s = python_escape_json_string(&k);
                let val_s = python_style_json_dumps_sorted(val);
                pairs.push(format!("{}: {}", key_s, val_s));
            }
            format!("{{{}}}", pairs.join(", "))
        }
    }
}

/// Same sanitization as `vault_cache._frontmatter_hash_stable`: double `json.dumps` with NUL escape strip between.
pub fn stable_json_string_from_value(v: JsonValue) -> Result<String, serde_json::Error> {
    let sorted = sort_json_value(v);
    let s1 = python_style_json_dumps_sorted(&sorted);
    let cleaned = s1.replace(r"\u0000", "");
    let v2: JsonValue = serde_json::from_str(&cleaned)?;
    let sorted2 = sort_json_value(v2);
    Ok(python_style_json_dumps_sorted(&sorted2))
}

/// Parse YAML frontmatter text (mapping) and return the same canonical JSON string as Python `ruamel` + `vault_cache` pipeline.
pub fn stable_json_from_frontmatter_yaml(yaml: &str) -> Result<String, String> {
    let fm = yaml.trim();
    if fm.is_empty() {
        return stable_json_string_from_value(JsonValue::Object(Map::new())).map_err(|e| e.to_string());
    }
    let v: JsonValue = serde_yaml::from_str(fm).map_err(|e| e.to_string())?;
    let v = match v {
        JsonValue::Null => JsonValue::Object(Map::new()),
        JsonValue::Object(_) => v,
        _ => {
            return Err("Frontmatter must parse to a mapping or null".to_string());
        }
    };
    stable_json_string_from_value(v).map_err(|e| e.to_string())
}

/// Build `serde_json::Value` from Python objects (dict/list/scalars). Unknown types use `str()` like `json.dumps(..., default=str)`.
pub fn json_value_from_py_any<'py>(py: Python<'py>, ob: &Bound<'py, PyAny>) -> PyResult<JsonValue> {
    if ob.is_none() {
        return Ok(JsonValue::Null);
    }
    if ob.is_instance_of::<PyBool>() {
        let b: bool = ob.extract()?;
        return Ok(JsonValue::Bool(b));
    }
    if let Ok(i) = ob.extract::<i64>() {
        return Ok(JsonValue::Number(i.into()));
    }
    if let Ok(u) = ob.extract::<u64>() {
        return Ok(JsonValue::Number(u.into()));
    }
    if let Ok(f) = ob.extract::<f64>() {
        return Ok(
            serde_json::Number::from_f64(f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
        );
    }
    if let Ok(s) = ob.extract::<String>() {
        return Ok(JsonValue::String(s));
    }
    if let Ok(d) = ob.downcast::<PyDict>() {
        let mut map = Map::new();
        for (k, v) in d.iter() {
            let key: String = k.extract()?;
            map.insert(key, json_value_from_py_any(py, &v)?);
        }
        return Ok(JsonValue::Object(map));
    }
    if let Ok(list) = ob.downcast::<PyList>() {
        let mut out = Vec::new();
        for item in list.iter() {
            out.push(json_value_from_py_any(py, &item)?);
        }
        return Ok(JsonValue::Array(out));
    }
    if let Ok(tup) = ob.downcast::<PyTuple>() {
        let mut out = Vec::new();
        for item in tup.iter() {
            out.push(json_value_from_py_any(py, &item)?);
        }
        return Ok(JsonValue::Array(out));
    }
    let s = ob.str()?.to_string();
    Ok(JsonValue::String(s))
}

/// SHA-256 hex of UTF-8 bytes (same as `hashlib.sha256(s.encode()).hexdigest()`).
pub fn sha256_hex_utf8(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

/// `_frontmatter_hash_stable` — hash of canonical JSON only.
pub fn frontmatter_hash_stable_from_value(v: JsonValue) -> Result<String, serde_json::Error> {
    let payload = stable_json_string_from_value(v)?;
    Ok(sha256_hex_utf8(&payload))
}

/// `_content_hash` — canonical frontmatter JSON + newline + body (NUL stripped from body).
pub fn content_hash_from_value(v: JsonValue, body: &str) -> Result<String, serde_json::Error> {
    let fm = stable_json_string_from_value(v)?;
    let payload = format!("{}\n{}", fm, body.replace('\0', ""));
    Ok(sha256_hex_utf8(&payload))
}

/// Exposed for pytest parity matrix (Step 8a).
#[pyfunction]
pub fn stable_json_from_yaml_frontmatter(yaml: &str) -> PyResult<String> {
    stable_json_from_frontmatter_yaml(yaml).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_yaml_is_empty_object_roundtrip() {
        let s = stable_json_from_frontmatter_yaml("").unwrap();
        assert_eq!(s, "{}");
    }
}

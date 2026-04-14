//! Frontmatter access for chunk builders — `serde_json::Map` parity with Python dict access.

use serde_json::{Map, Value};

pub use crate::materializer::fm_value::fm_str_value;

/// Same as Python `coerce_string_list` for `serde_json::Value` (chunk builders).
pub fn coerce_string_list_json(value: Option<&Value>) -> Vec<String> {
    match value {
        None => Vec::new(),
        Some(v) => crate::materializer::fm_value::iter_string_values_json(v),
    }
}

/// Git commit stats: missing key → `"0"`, else Python `str(value)` semantics.
pub fn stat_str_json(fm: &Map<String, Value>, key: &str) -> String {
    match fm.get(key) {
        None => "0".to_string(),
        Some(v) => crate::materializer::fm_value::value_python_str(v),
    }
}

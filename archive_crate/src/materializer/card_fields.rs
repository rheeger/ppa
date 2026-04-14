//! Step 8b — Rust view of a validated card for materialization.
//!
//! Mirrors `archive_vault.schema.validate_card_permissive` **key filtering** (known Pydantic fields only).
//! Values are taken from frontmatter JSON; list/string coercion matches read-path usage in the materializer.

use std::collections::HashSet;
use std::sync::OnceLock;

use serde_json::{Map, Value};

include!("card_field_keys.rs");

fn known_keys_set() -> &'static HashSet<&'static str> {
    static S: OnceLock<HashSet<&'static str>> = OnceLock::new();
    S.get_or_init(|| KNOWN_CARD_FIELD_KEYS.iter().copied().collect())
}

/// Filter frontmatter to known card fields (same keys as permissive validation).
pub fn filter_known_card_fields(obj: Map<String, Value>) -> Map<String, Value> {
    let keys = known_keys_set();
    let mut out = Map::new();
    for (k, v) in obj {
        if keys.contains(k.as_str()) {
            out.insert(k, v);
        }
    }
    out
}

fn string_list_from_value(v: Option<&Value>) -> Vec<String> {
    let Some(v) = v else {
        return Vec::new();
    };
    match v {
        Value::Array(items) => items
            .iter()
            .filter_map(|x| match x {
                Value::String(s) => Some(s.trim().to_string()),
                Value::Number(n) => Some(n.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            })
            .filter(|s| !s.is_empty())
            .collect(),
        Value::String(s) => {
            let t = s.trim();
            if t.is_empty() {
                vec![]
            } else {
                vec![t.to_string()]
            }
        }
        Value::Null => vec![],
        _ => vec![],
    }
}

/// Card fields read by the materializer — replaces passing a Python `BaseCard` from `validate_card_permissive`.
#[derive(Debug, Clone)]
pub struct CardFields {
    pub card_type: String,
    pub uid: String,
    pub summary: String,
    pub source: Vec<String>,
    pub people: Vec<String>,
    pub orgs: Vec<String>,
    /// Key-filtered map (known Pydantic fields only). Used by future `serde_json::Value` projection path (Step 8c/8d).
    #[allow(dead_code)]
    pub filtered: Map<String, Value>,
}

impl CardFields {
    /// Build from full frontmatter JSON (`serde_json::Value::Object`).
    pub fn from_frontmatter_value(value: Value) -> Result<Self, String> {
        match value {
            Value::Object(m) => Self::from_object_map(m),
            _ => Err("frontmatter must be a JSON object".to_string()),
        }
    }

    fn from_object_map(obj: Map<String, Value>) -> Result<Self, String> {
        let filtered = filter_known_card_fields(obj);
        let card_type = filtered
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let uid = filtered
            .get("uid")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let summary = filtered
            .get("summary")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let source = string_list_from_value(filtered.get("source"));
        let people = string_list_from_value(filtered.get("people"));
        let orgs = string_list_from_value(filtered.get("orgs"));
        Ok(Self {
            card_type,
            uid,
            summary,
            source,
            people,
            orgs,
            filtered,
        })
    }
}

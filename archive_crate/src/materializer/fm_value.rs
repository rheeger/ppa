//! Frontmatter as `serde_json::Map` — Step 8c helpers mirroring Python dict access + `archive_cli.features`.

use serde_json::{Map, Value};

use crate::json_stable;

/// Same tuple as `archive_cli.features.RELATIONSHIP_FIELDS`.
pub const RELATIONSHIP_FIELDS: &[&str] = &[
    "people",
    "orgs",
    "reports_to",
    "messages",
    "attachments",
    "calendar_events",
    "meeting_transcripts",
    "source_messages",
    "source_threads",
    "thread",
    "message",
    "repository",
    "source_email",
    "linked_purchase",
    "evidence_uids",
];

pub fn sanitize_json_value(v: Value) -> Value {
    match v {
        Value::String(mut s) => {
            s.retain(|c| c != '\0');
            Value::String(s)
        }
        Value::Array(a) => Value::Array(a.into_iter().map(sanitize_json_value).collect()),
        Value::Object(o) => {
            let mut out = Map::new();
            for (k, val) in o {
                let k2 = k.replace('\0', "");
                out.insert(k2, sanitize_json_value(val));
            }
            Value::Object(out)
        }
        _ => v,
    }
}

/// `archive_cli.features.json_text` — `json.dumps(..., sort_keys=True)` on sanitized payload.
pub fn json_text_value(v: &Value) -> String {
    let payload = match v {
        Value::Object(_) | Value::Array(_) => sanitize_json_value(v.clone()),
        Value::Null => Value::Object(Map::new()),
        Value::String(s) if s.is_empty() => Value::Object(Map::new()),
        _ => sanitize_json_value(v.clone()),
    };
    let payload = if matches!(payload, Value::Null) {
        Value::Object(Map::new())
    } else {
        payload
    };
    json_stable::python_style_json_dumps_sorted(&payload)
}

/// Mirrors `archive_cli.materializer._iter_string_values`: only `str`, `list`, and `dict`
/// contribute (numbers/bools/null are ignored, not stringified).
pub fn iter_string_values_json(v: &Value) -> Vec<String> {
    match v {
        Value::String(s) => {
            let cleaned = s.replace('\0', "").trim().to_string();
            if cleaned.is_empty() {
                vec![]
            } else {
                vec![cleaned]
            }
        }
        Value::Array(a) => a.iter().flat_map(|x| iter_string_values_json(x)).collect(),
        Value::Object(o) => o.values().flat_map(|x| iter_string_values_json(x)).collect(),
        Value::Number(_) | Value::Bool(_) | Value::Null => vec![],
    }
}

/// `str(frontmatter.get(key, default))` for scalar-ish display — matches Python `str()` on JSON scalars.
pub fn fm_str_value(fm: &Map<String, Value>, key: &str) -> String {
    match fm.get(key) {
        None => String::new(),
        Some(v) => value_python_str(v),
    }
}

/// `str(v)` for JSON scalars — matches Python `str()` on typical frontmatter values.
pub fn value_python_str(v: &Value) -> String {
    match v {
        Value::Null => "None".to_string(),
        Value::Bool(true) => "True".to_string(),
        Value::Bool(false) => "False".to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

pub fn external_ids_by_provider_value(fm: &Map<String, Value>) -> Map<String, Value> {
    use crate::materializer::external_ids::{EXTERNAL_ID_FIELDS, EXTERNAL_ID_LIST_FIELDS};
    let mut grouped: Map<String, Value> = Map::new();
    for (field_name, provider) in EXTERNAL_ID_FIELDS {
        let v = fm.get(*field_name).unwrap_or(&Value::Null);
        if let Value::String(s) = v {
            let t = s.trim();
            if !t.is_empty() {
                push_provider_id(&mut grouped, provider, t);
            }
        }
    }
    for (field_name, provider) in EXTERNAL_ID_LIST_FIELDS {
        if let Some(v) = fm.get(*field_name) {
            for item in iter_string_values_json(v) {
                push_provider_id(&mut grouped, provider, &item);
            }
        }
    }
    grouped
}

fn push_provider_id(grouped: &mut Map<String, Value>, provider: &str, id: &str) {
    let entry = grouped
        .entry(provider.to_string())
        .or_insert_with(|| Value::Array(vec![]));
    if let Value::Array(arr) = entry {
        if !arr.iter().any(|x| x.as_str() == Some(id)) {
            arr.push(Value::String(id.to_string()));
        }
    }
}

pub fn relationship_payload_value(fm: &Map<String, Value>) -> Map<String, Value> {
    let mut payload = Map::new();
    for field_name in RELATIONSHIP_FIELDS {
        if let Some(v) = fm.get(*field_name) {
            let vals = iter_string_values_json(v);
            if !vals.is_empty() {
                payload.insert(
                    (*field_name).to_string(),
                    Value::Array(vals.into_iter().map(Value::String).collect()),
                );
            }
        }
    }
    payload
}

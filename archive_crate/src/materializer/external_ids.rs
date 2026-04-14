//! Mirrors `archive_cli.features.iter_external_ids` / `iter_string_values`.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::Map;
use serde_json::Value;

/// (field_name, provider) pairs — order matches `features.EXTERNAL_ID_FIELDS` iteration.
pub static EXTERNAL_ID_FIELDS: &[(&str, &str)] = &[
    ("source_id", "canonical"),
    ("gmail_thread_id", "gmail"),
    ("gmail_message_id", "gmail"),
    ("gmail_history_id", "gmail"),
    ("message_id_header", "email"),
    ("attachment_id", "attachment"),
    ("content_id", "attachment"),
    ("calendar_id", "calendar"),
    ("event_id", "calendar"),
    ("event_etag", "calendar"),
    ("ical_uid", "calendar"),
    ("invite_ical_uid", "calendar"),
    ("invite_event_id_hint", "calendar"),
    ("event_id_hint", "calendar"),
    ("imessage_chat_id", "imessage"),
    ("imessage_message_id", "imessage"),
    ("beeper_room_id", "beeper"),
    ("beeper_event_id", "beeper"),
    ("photos_asset_id", "photos"),
    ("otter_meeting_id", "otter"),
    ("otter_conversation_id", "otter"),
    ("repository_id", "github_repo"),
    ("repository_name_with_owner", "github_repo"),
    ("commit_sha", "github_commit"),
    ("github_thread_id", "github_thread"),
    ("github_message_id", "github_message"),
    ("number", "github_thread_number"),
    ("review_commit_sha", "github_commit"),
    ("original_commit_sha", "github_commit"),
    ("encounter_source_id", "medical_encounter"),
    ("confirmation_code", "booking"),
    ("tracking_number", "shipping"),
    ("order_number", "purchase"),
];

pub static EXTERNAL_ID_LIST_FIELDS: &[(&str, &str)] = &[
    ("invite_ical_uids", "calendar"),
    ("invite_event_id_hints", "calendar"),
    ("parent_shas", "github_commit"),
    ("associated_pr_numbers", "github_thread_number"),
    ("associated_pr_urls", "github_pr"),
];

pub fn iter_string_values_py(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = value.extract::<String>() {
        let t = s.trim();
        return Ok(if t.is_empty() {
            vec![]
        } else {
            vec![t.to_string()]
        });
    }
    if let Ok(seq) = value.downcast::<PyList>() {
        let mut out = Vec::new();
        for item in seq.iter() {
            out.extend(iter_string_values_py(py, &item)?);
        }
        return Ok(out);
    }
    if let Ok(d) = value.downcast::<PyDict>() {
        let mut out = Vec::new();
        for (_k, v) in d.iter() {
            out.extend(iter_string_values_py(py, &v)?);
        }
        return Ok(out);
    }
    Ok(vec![])
}

pub fn iter_external_ids_value(
    frontmatter: &Map<String, Value>,
) -> Vec<(String, String, String)> {
    let mut rows: Vec<(String, String, String)> = Vec::new();
    for (field_name, provider) in EXTERNAL_ID_FIELDS {
        let v = frontmatter.get(*field_name);
        if let Some(Value::String(s)) = v {
            let t = s.trim();
            if !t.is_empty() {
                rows.push((field_name.to_string(), provider.to_string(), t.to_string()));
            }
        }
    }
    for (field_name, provider) in EXTERNAL_ID_LIST_FIELDS {
        let Some(v) = frontmatter.get(*field_name) else {
            continue;
        };
        for item in crate::materializer::fm_value::iter_string_values_json(v) {
            rows.push((field_name.to_string(), provider.to_string(), item));
        }
    }
    rows
}

pub fn iter_external_ids(
    py: Python<'_>,
    frontmatter: &Bound<'_, PyDict>,
) -> PyResult<Vec<(String, String, String)>> {
    let mut rows: Vec<(String, String, String)> = Vec::new();
    for (field_name, provider) in EXTERNAL_ID_FIELDS {
        let v = frontmatter.get_item(*field_name)?;
        if v.is_none() {
            continue;
        }
        let v = v.unwrap();
        if let Ok(s) = v.extract::<String>() {
            let t = s.trim();
            if !t.is_empty() {
                rows.push((field_name.to_string(), provider.to_string(), t.to_string()));
            }
        }
    }
    for (field_name, provider) in EXTERNAL_ID_LIST_FIELDS {
        let v = frontmatter.get_item(*field_name)?;
        if v.is_none() {
            continue;
        }
        let v = v.unwrap();
        for item in iter_string_values_py(py, &v)? {
            rows.push((field_name.to_string(), provider.to_string(), item));
        }
    }
    Ok(rows)
}

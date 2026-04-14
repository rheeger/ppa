//! `card_activity_at` / `card_activity_end_at` — same field cascade as `archive_cli.features`.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Map;

use crate::materializer::time_parse::{optional_utc_to_py, parse_timestamp_to_utc_rust};

const ACTIVITY_CASCADE: &[&str] = &[
    "last_message_at",
    "sent_at",
    "start_at",
    "captured_at",
    "committed_at",
    "occurred_at",
    "departure_at",
    "pickup_at",
    "check_in",
    "pay_date",
    "shipped_at",
    "event_at",
    "updated",
    "created",
    "first_message_at",
];

fn str_or_empty(v: Option<&serde_json::Value>) -> String {
    match v {
        None => String::new(),
        Some(serde_json::Value::String(s)) => s.trim().to_string(),
        Some(serde_json::Value::Null) => String::new(),
        Some(x) => x.to_string().trim().to_string(),
    }
}

pub fn card_activity_at_value(fm: &Map<String, serde_json::Value>) -> String {
    for field in ACTIVITY_CASCADE {
        let v = str_or_empty(fm.get(*field));
        if !v.is_empty() {
            return v;
        }
    }
    String::new()
}

pub fn card_activity_end_at_value(
    card_type: &str,
    fm: &Map<String, serde_json::Value>,
) -> String {
    let field = match card_type {
        "flight" => Some("arrival_at"),
        "accommodation" => Some("check_out"),
        "car_rental" => Some("dropoff_at"),
        "calendar_event" | "meeting_transcript" => Some("end_at"),
        "ride" => Some("dropoff_at"),
        _ => None,
    };
    let Some(f) = field else {
        return String::new();
    };
    str_or_empty(fm.get(f))
}

/// Legacy PyDict path (tests / callers outside materializer).
pub fn card_activity_at(frontmatter: &Bound<'_, PyDict>) -> PyResult<String> {
    for field in ACTIVITY_CASCADE {
        let v = frontmatter.get_item(*field)?;
        if v.is_none() {
            continue;
        }
        let s: String = v.unwrap().extract()?;
        let t = s.trim();
        if !t.is_empty() {
            return Ok(t.to_string());
        }
    }
    Ok(String::new())
}

pub fn card_activity_end_at(card_type: &str, frontmatter: &Bound<'_, PyDict>) -> PyResult<String> {
    let field = match card_type {
        "flight" => Some("arrival_at"),
        "accommodation" => Some("check_out"),
        "car_rental" => Some("dropoff_at"),
        "calendar_event" | "meeting_transcript" => Some("end_at"),
        "ride" => Some("dropoff_at"),
        _ => None,
    };
    let Some(f) = field else {
        return Ok(String::new());
    };
    let v = frontmatter.get_item(f)?;
    if v.is_none() {
        return Ok(String::new());
    }
    let s: String = v.unwrap().extract()?;
    Ok(s.trim().to_string())
}

pub fn parse_timestamp_to_utc(py: Python<'_>, value: &str) -> PyResult<Option<PyObject>> {
    let dt = parse_timestamp_to_utc_rust(value);
    optional_utc_to_py(py, dt)
}

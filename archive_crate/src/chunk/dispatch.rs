//! Orchestrates chunk building — parity with `archive_cli.chunk_builders._build_chunks`.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use serde_json::{Map, Value as JsonValue};

use crate::chunk::accumulator::{ChunkAccumulator, ChunkRecord};
use crate::chunk::builders;
use crate::chunk::fm::fm_str_value;

/// Same mapping as `archive_cli.card_registry` `chunk_builder_name` (None → default path).
fn chunk_builder_name(card_type: &str) -> Option<&'static str> {
    match card_type {
        "person" => Some("person"),
        "email_thread" => Some("email_thread"),
        "email_message" => Some("email_message"),
        "imessage_thread" => Some("imessage_thread"),
        "calendar_event" => Some("calendar_event"),
        "meeting_transcript" => Some("meeting_transcript"),
        "document" => Some("document"),
        "git_repository" => Some("git_repository"),
        "git_commit" => Some("git_commit"),
        "git_thread" => Some("git_thread"),
        "git_message" => Some("git_message"),
        _ => None,
    }
}

/// Step 9c — chunk builders read `serde_json::Map` frontmatter (no `value_to_py_dict` bridge).
pub fn build_chunks(fm: &JsonValue, body: &str) -> Vec<ChunkRecord> {
    let empty = Map::new();
    let fm_map: &Map<String, JsonValue> = match fm {
        JsonValue::Object(m) => m,
        _ => &empty,
    };
    build_chunks_map(fm_map, body)
}

fn build_chunks_map(fm: &Map<String, JsonValue>, body: &str) -> Vec<ChunkRecord> {
    let limit = crate::chunk::config::get_chunk_char_limit();
    let card_type = fm_str_value(fm, "type");

    let mut acc = ChunkAccumulator::new(limit);

    if let Some(name) = chunk_builder_name(&card_type) {
        match name {
            "person" => builders::build_person_chunks(fm, body, &mut acc),
            "email_thread" => builders::build_email_thread_chunks(fm, body, &mut acc, limit),
            "email_message" => builders::build_email_message_chunks(fm, body, &mut acc),
            "imessage_thread" => builders::build_imessage_thread_chunks(fm, body, &mut acc, limit),
            "calendar_event" => builders::build_calendar_event_chunks(fm, body, &mut acc),
            "meeting_transcript" => {
                builders::build_meeting_transcript_chunks(fm, body, &mut acc, limit)
            }
            "document" => builders::build_document_chunks(fm, body, &mut acc),
            "git_repository" => builders::build_git_repository_chunks(fm, body, &mut acc),
            "git_commit" => builders::build_git_commit_chunks(fm, body, &mut acc),
            "git_thread" => builders::build_git_thread_chunks(fm, body, &mut acc),
            "git_message" => builders::build_git_message_chunks(fm, body, &mut acc),
            _ => builders::build_default_chunks(fm, body, &mut acc),
        }
    } else {
        builders::build_default_chunks(fm, body, &mut acc);
    }

    acc.chunks
}

/// Serialize chunk records to the same dict shape as `archive_cli.chunking.render_chunks_for_card`.
pub fn chunk_records_to_py_list(py: Python<'_>, records: &[ChunkRecord]) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    for r in records {
        let d = PyDict::new_bound(py);
        d.set_item("chunk_type", &r.chunk_type)?;
        d.set_item("chunk_index", r.chunk_index)?;
        let sf = PyList::empty_bound(py);
        for s in &r.source_fields {
            sf.append(s)?;
        }
        d.set_item("source_fields", sf)?;
        d.set_item("content", &r.content)?;
        d.set_item("content_hash", &r.content_hash)?;
        d.set_item("token_count", r.token_count)?;
        list.append(d)?;
    }
    Ok(list.to_object(py))
}

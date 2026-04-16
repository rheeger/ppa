//! Authoritative vault validator — reads `notes` from tier-2 SQLite cache and validates
//! card schema + provenance in parallel with rayon. Uses `frontmatter_json` and
//! `provenance_json` columns (no body decompression needed).
//!
//! Produces the same errors as the Python `validate_card_strict` + `validate_provenance`
//! path for the checks it implements (uid prefix, type, source, dates, provenance coverage,
//! deterministic-only protection).

use pyo3::prelude::*;
use pyo3::types::PyDict;
use rayon::prelude::*;
use regex::Regex;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

const VALID_CARD_TYPES: &[&str] = &[
    "person", "finance", "medical_record", "vaccination",
    "email_thread", "email_message", "email_attachment",
    "imessage_thread", "imessage_message", "imessage_attachment",
    "beeper_thread", "beeper_message", "beeper_attachment",
    "calendar_event", "media_asset", "document", "meeting_transcript",
    "git_repository", "git_commit", "git_thread", "git_message",
    "meal_order", "grocery_order", "ride", "flight",
    "accommodation", "car_rental", "purchase", "shipment",
    "subscription", "event_ticket", "payroll",
    "place", "organization", "knowledge", "observation",
];

fn provenance_exempt() -> &'static HashSet<&'static str> {
    static SET: OnceLock<HashSet<&str>> = OnceLock::new();
    SET.get_or_init(|| {
        [
            "uid", "type", "source", "source_id", "created", "updated",
            "people", "orgs", "source_email", "extraction_confidence",
        ].into_iter().collect()
    })
}

fn deterministic_only() -> &'static HashSet<&'static str> {
    static SET: OnceLock<HashSet<&str>> = OnceLock::new();
    SET.get_or_init(|| {
        [
            "uid", "type", "source", "source_id", "created", "updated",
            "emails", "phones", "birthday", "first_name", "last_name",
            "aliases", "company", "companies", "title", "titles",
            "linkedin", "linkedin_url", "linkedin_connected_on",
            "twitter", "github", "instagram", "telegram", "discord",
            "pronouns", "reports_to", "websites", "emails_seen_count",
            "amount", "currency",
            "gmail_thread_id", "gmail_message_id", "gmail_history_id",
            "imessage_chat_id", "imessage_message_id",
            "beeper_room_id", "beeper_event_id",
            "attachment_id", "account_email", "account_id",
            "thread", "message", "service", "protocol",
            "bridge_name", "thread_type", "thread_title", "thread_description",
            "participant_ids", "participant_names", "participant_identifiers",
            "counterpart_ids", "counterpart_names", "counterpart_identifiers",
            "message_type", "sender_id", "sender_name", "sender_identifier",
            "sender_person", "chat_identifier", "display_name",
            "participant_handles", "sender_handle",
            "is_from_me", "attachment_count", "is_group",
            "edited_at", "deleted_at",
            "associated_message_guid", "associated_message_type",
            "associated_message_emoji", "expressive_send_style_id",
            "balloon_bundle_id", "transfer_name", "uti",
            "original_path", "exported_path",
            "calendar_id", "event_id", "event_etag", "ical_uid",
            "direction", "from_name", "from_email",
            "to_emails", "cc_emails", "bcc_emails", "reply_to_emails",
            "participant_emails", "participants", "label_ids",
            "sent_at", "start_at", "end_at", "timezone",
            "subject", "snippet", "message_id_header", "in_reply_to",
            "references", "has_attachments", "attachments",
            "calendar_events", "messages", "message_count",
            "first_message_at", "last_message_at",
            "filename", "mime_type", "size_bytes", "content_id", "is_inline",
            "invite_ical_uid", "invite_event_id_hint", "invite_method",
            "invite_title", "invite_start_at", "invite_end_at",
            "invite_ical_uids", "invite_event_id_hints",
            "thread_body_sha", "message_body_sha", "attachment_metadata_sha",
            "linked_message_event_id", "reply_to_event_id", "reaction_key",
            "status", "organizer_email", "organizer_name",
            "attendee_emails", "conference_url",
            "source_messages", "source_threads", "meeting_transcripts",
            "all_day", "event_body_sha",
            "otter_meeting_id", "otter_conversation_id",
            "meeting_url", "transcript_url", "recording_url",
            "speaker_names", "speaker_emails",
            "host_name", "host_email", "language", "duration_seconds",
            "event_id_hint", "transcript_body_sha", "otter_updated_at",
            "photos_asset_id", "photos_source_label", "media_type",
            "captured_at", "modified_at", "keywords", "labels",
            "person_labels", "albums", "album_paths", "folders",
            "favorite", "hidden", "has_adjustments",
            "live_photo", "burst", "screenshot", "slow_mo", "time_lapse",
            "width", "height", "place_name", "place_city", "place_state",
            "place_country", "latitude", "longitude",
            "edited_path", "metadata_sha", "original_filename",
            "is_missing", "attachment_type", "src_url", "cached_path",
            "duration_ms", "is_voice_note", "is_gif", "is_sticker",
            "library_root", "relative_path", "extension",
            "content_sha", "file_created_at", "file_modified_at",
            "date_start", "date_end", "document_type", "document_date",
            "authors", "counterparties",
            "location", "sheet_names", "page_count",
            "text_source", "extracted_text_sha", "extraction_status",
            "quality_flags", "source_system", "source_format",
            "record_type", "record_subtype", "occurred_at", "recorded_at",
            "provider_name", "facility_name", "encounter_source_id",
            "code_system", "code", "code_display",
            "value_text", "value_numeric", "unit", "raw_source_ref", "details_json",
            "vaccine_name", "cvx_code", "manufacturer", "brand_name",
            "lot_number", "expiration_date", "administered_at", "performer_name",
            "github_repo_id", "github_thread_id", "github_message_id", "github_node_id",
            "name_with_owner", "owner_login", "owner_type",
            "html_url", "api_url", "ssh_url", "default_branch", "homepage_url",
            "visibility", "is_private", "is_fork", "is_archived",
            "parent_name_with_owner", "primary_language", "languages", "topics",
            "license_name", "created_at", "pushed_at",
            "commit_sha", "repository_name_with_owner", "repository",
            "parent_shas", "authored_at", "committed_at", "message_headline",
            "additions", "deletions", "changed_files",
            "author_login", "author_name", "author_email",
            "committer_login", "committer_name", "committer_email",
            "associated_pr_numbers", "associated_pr_urls",
            "number", "state", "is_draft", "merged_at", "closed_at",
            "assignees", "milestone", "base_ref", "head_ref",
            "participant_logins", "actor_login", "actor_name", "actor_email",
            "review_state", "review_commit_sha", "in_reply_to_message_id",
            "path", "position", "original_position", "original_commit_sha", "diff_hunk",
            "restaurant", "airline", "confirmation_code", "tracking_number",
            "service_name", "employer", "pickup_location", "dropoff_location",
            "fare", "tip", "nightly_rate", "total_cost", "order_number",
            "pay_date", "gross_amount", "net_amount", "deductions_json",
            "evidence_uids", "depends_on_types", "standing_query", "input_watermark",
            "observation_type", "confidence", "valid_from", "valid_until",
            "billing_cycle", "event_type", "event_at", "venue", "venue_address",
            "barcode_url", "property_name", "property_type",
            "fare_class", "fare_amount", "booking_source", "passengers",
            "vehicle_class", "shipping_cost", "shipping_address", "payment_method",
            "linked_purchase", "estimated_delivery", "delivered_at",
            "origin_airport", "destination_airport", "departure_at", "arrival_at",
            "pickup_at", "dropoff_at", "check_in", "check_out",
            "ride_type", "distance_miles", "duration_minutes", "driver_name", "vehicle",
            "carrier", "shipped_at", "pay_period_start", "pay_period_end",
            "place_type", "org_type", "relationship", "first_seen", "last_seen",
            "refresh_interval_days", "freshness_date", "mode",
            "delivery_address", "delivery_fee", "store", "items",
            "subtotal", "total", "tax", "seat", "quantity", "price", "plan_name",
            "guests", "event_name", "vendor", "name",
            "address", "city", "country", "domain",
        ].into_iter().collect()
    })
}

fn date_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\d{4}-\d{2}-\d{2}$").expect("date regex"))
}

fn is_empty_value(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Number(n) => n.as_f64().map_or(false, |f| f == 0.0),
        Value::Bool(b) => !b,
        Value::Object(o) => o.is_empty(),
    }
}

struct NoteRow {
    rel_path: String,
    frontmatter_json: String,
    provenance_json: String,
}

fn load_note_rows(cache_path: &str) -> Result<Vec<NoteRow>, String> {
    let conn = Connection::open(cache_path).map_err(|e| e.to_string())?;

    let has_prov_col = conn
        .prepare("SELECT provenance_json FROM notes LIMIT 0")
        .is_ok();

    if has_prov_col {
        let mut stmt = conn
            .prepare("SELECT rel_path, frontmatter_json, provenance_json FROM notes")
            .map_err(|e| e.to_string())?;
        let rows: Vec<NoteRow> = stmt
            .query_map([], |row| {
                Ok(NoteRow {
                    rel_path: row.get(0)?,
                    frontmatter_json: row.get(1)?,
                    provenance_json: row.get::<_, String>(2).unwrap_or_else(|_| "{}".to_string()),
                })
            })
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    } else {
        let mut stmt = conn
            .prepare("SELECT rel_path, frontmatter_json FROM notes")
            .map_err(|e| e.to_string())?;
        let rows: Vec<NoteRow> = stmt
            .query_map([], |row| {
                Ok(NoteRow {
                    rel_path: row.get(0)?,
                    frontmatter_json: row.get(1)?,
                    provenance_json: "{}".to_string(),
                })
            })
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }
}

fn validate_one(row: &NoteRow) -> Vec<String> {
    let mut errors: Vec<String> = Vec::new();
    let rel = &row.rel_path;

    let fm: Value = match serde_json::from_str(&row.frontmatter_json) {
        Ok(v) => v,
        Err(e) => {
            errors.push(format!("- {rel}: frontmatter JSON parse error: {e}"));
            return errors;
        }
    };

    let obj = match fm.as_object() {
        Some(o) => o,
        None => {
            errors.push(format!("- {rel}: frontmatter is not an object"));
            return errors;
        }
    };

    // uid
    let uid = obj.get("uid").and_then(|v| v.as_str()).unwrap_or("");
    if uid.is_empty() {
        errors.push(format!("- {rel}: missing uid"));
    } else if !uid.starts_with("hfa-") {
        errors.push(format!("- {rel}: uid must start with 'hfa-', got '{}'", &uid[..uid.len().min(20)]));
    }

    // type
    let card_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("");
    if card_type.is_empty() {
        errors.push(format!("- {rel}: missing type"));
    } else if !VALID_CARD_TYPES.contains(&card_type) {
        errors.push(format!("- {rel}: unknown card type '{card_type}'"));
    }

    // source
    let source = obj.get("source");
    match source {
        Some(Value::Array(arr)) => {
            if arr.is_empty() {
                errors.push(format!("- {rel}: source array is empty"));
            }
        }
        Some(Value::String(s)) if !s.is_empty() => {}
        _ => {
            errors.push(format!("- {rel}: missing or invalid source"));
        }
    }

    // created / updated date format
    for field in &["created", "updated"] {
        if let Some(Value::String(s)) = obj.get(*field) {
            if !date_re().is_match(s) {
                errors.push(format!("- {rel}: {field} must be YYYY-MM-DD, got '{s}'"));
            }
        }
    }

    // provenance validation — mirrors Python validate_provenance exactly
    let prov: HashMap<String, Value> = serde_json::from_str(&row.provenance_json).unwrap_or_default();
    let exempt = provenance_exempt();
    let det_only = deterministic_only();

    for (field_name, value) in obj.iter() {
        if exempt.contains(field_name.as_str()) {
            continue;
        }
        if is_empty_value(value) {
            continue;
        }
        match prov.get(field_name) {
            None => {
                errors.push(format!(
                    "- {rel}: Field '{field_name}' is missing provenance"
                ));
            }
            Some(entry) => {
                if det_only.contains(field_name.as_str()) {
                    let method = entry
                        .get("method")
                        .and_then(|m| m.as_str())
                        .unwrap_or("");
                    if method != "deterministic" {
                        errors.push(format!(
                            "- {rel}: Field '{field_name}' is deterministic-only but provenance method is '{method}'"
                        ));
                    }
                }
            }
        }
    }

    errors
}

#[pyfunction]
#[pyo3(signature = (cache_path))]
pub fn validate_vault_from_cache(py: Python<'_>, cache_path: String) -> PyResult<PyObject> {
    let rows = py
        .allow_threads(|| load_note_rows(&cache_path))
        .map_err(|e: String| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    let total = rows.len();

    let all_errors: Vec<Vec<String>> = py.allow_threads(|| {
        rows.par_iter().map(|row| validate_one(row)).collect()
    });

    let mut errors: Vec<String> = Vec::new();
    let mut valid = 0usize;
    for errs in &all_errors {
        if errs.is_empty() {
            valid += 1;
        } else {
            errors.extend(errs.iter().cloned());
        }
    }

    let out = PyDict::new_bound(py);
    out.set_item("total", total)?;
    out.set_item("valid", valid)?;
    out.set_item("error_count", errors.len())?;
    let error_list: Vec<&str> = errors.iter().map(|s| s.as_str()).collect();
    out.set_item("errors", error_list)?;
    Ok(out.to_object(py))
}

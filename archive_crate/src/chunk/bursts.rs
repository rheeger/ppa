//! Conversation burst segmentation. Parity with `archive_cli.conversation_bursts`.

use sha2::{Digest, Sha256};

use super::accumulator::ChunkAccumulator;
use super::config::{get_burst_chat_gap_seconds, get_burst_token_limit};
use super::constants::{BURST_ALGORITHM_VERSION, BURST_CHUNK_TYPE};
use super::helpers::{clean_text, encode_json_string_python_ensure_ascii, token_count};

const BOILERPLATE_PREFIXES: &[&str] = &[
    "liked a message",
    "loved a message",
    "emphasized a message",
    "reacted to",
    "tapback",
    "reacted ",
];

#[derive(Clone, Debug)]
pub struct ConversationMessage {
    pub message_id: String,
    pub timestamp: String,
    pub author: String,
    pub text: String,
    pub source_uid: String,
    pub revision: String,
}

fn json_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::Bool(v) => v.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
    }
}

fn clean_json(value: Option<&serde_json::Value>) -> String {
    match value {
        None | Some(serde_json::Value::Null) => String::new(),
        Some(v) => clean_text(&json_text(v)),
    }
}

fn first_clean(obj: &serde_json::Map<String, serde_json::Value>, keys: &[&str]) -> String {
    for key in keys {
        if let Some(v) = obj.get(*key) {
            if !v.is_null() {
                let cleaned = clean_text(&json_text(v));
                if !cleaned.is_empty() {
                    return cleaned;
                }
            }
        }
    }
    String::new()
}

fn message_from_json(item: &serde_json::Value) -> Option<ConversationMessage> {
    let obj = item.as_object()?;
    let text = first_clean(obj, &["text", "body", "content"]);
    let message_id = first_clean(obj, &["message_id", "uid", "id"]);
    if text.is_empty() || message_id.is_empty() {
        return None;
    }
    let timestamp = first_clean(obj, &["timestamp", "sent_at", "created"]);
    let author_raw = first_clean(obj, &["author", "from_name", "from"]);
    let author = if author_raw.is_empty() {
        "unknown".to_string()
    } else {
        author_raw
    };
    let source_uid = first_clean(obj, &["source_uid", "uid"]);
    let revision = first_clean(obj, &["revision", "source_revision", "content_hash"]);
    Some(ConversationMessage {
        message_id: message_id.clone(),
        timestamp,
        author,
        text,
        source_uid: if source_uid.is_empty() {
            message_id
        } else {
            source_uid
        },
        revision,
    })
}

fn messages_from_array(items: &[serde_json::Value]) -> Vec<ConversationMessage> {
    items.iter().filter_map(message_from_json).collect()
}

fn parse_json_messages(raw: &serde_json::Value) -> Vec<ConversationMessage> {
    match raw {
        serde_json::Value::String(s) => {
            let text = s.trim();
            if text.starts_with('[') || text.starts_with('{') {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(text) {
                    return parse_json_messages(&parsed);
                }
            }
            Vec::new()
        }
        serde_json::Value::Array(items) => messages_from_array(items),
        _ => Vec::new(),
    }
}

fn structured_messages(frontmatter: &serde_json::Value) -> Vec<ConversationMessage> {
    let Some(obj) = frontmatter.as_object() else {
        return Vec::new();
    };
    if let Some(raw) = obj.get("conversation_messages") {
        if !raw.is_null() {
            return parse_json_messages(raw);
        }
    }
    if let Some(raw) = obj.get("messages_json") {
        return parse_json_messages(raw);
    }
    Vec::new()
}

fn transcript_messages(body: &str) -> Vec<ConversationMessage> {
    const MARKER: &str = "## burst-transcript";
    let Some(start) = body.find(MARKER) else {
        return Vec::new();
    };
    let section = &body[start + MARKER.len()..];
    let mut messages = Vec::new();
    let mut current: Option<(String, String, String, String)> = None;
    let mut body_lines: Vec<String> = Vec::new();

    let flush = |current: &mut Option<(String, String, String, String)>,
                 body_lines: &mut Vec<String>,
                 messages: &mut Vec<ConversationMessage>| {
        let Some((ts, author, mid, rev)) = current.take() else {
            return;
        };
        let mut obj = serde_json::Map::new();
        obj.insert("timestamp".into(), serde_json::Value::String(ts));
        obj.insert("author".into(), serde_json::Value::String(author));
        obj.insert("message_id".into(), serde_json::Value::String(mid));
        obj.insert("revision".into(), serde_json::Value::String(rev));
        obj.insert(
            "text".into(),
            serde_json::Value::String(body_lines.join("\n")),
        );
        body_lines.clear();
        if let Some(parsed) = message_from_json(&serde_json::Value::Object(obj)) {
            messages.push(parsed);
        }
    };

    for line in section.lines() {
        if let Some(parsed) = parse_transcript_header(line.trim()) {
            flush(&mut current, &mut body_lines, &mut messages);
            current = Some(parsed);
            continue;
        }
        if current.is_some() {
            body_lines.push(line.to_string());
        }
    }
    flush(&mut current, &mut body_lines, &mut messages);
    messages
}

fn parse_transcript_header(line: &str) -> Option<(String, String, String, String)> {
    if !line.starts_with('[') || !line.ends_with(']') {
        return None;
    }
    let inner = &line[1..line.len() - 1];
    let parts: Vec<&str> = inner.split('|').collect();
    if parts.len() < 3 {
        return None;
    }
    let ts = parts[0].trim();
    let author = parts[1].trim();
    let mid_part = parts[2].trim();
    let mid = mid_part.strip_prefix("mid=")?;
    if ts.is_empty() || author.is_empty() || mid.is_empty() || ts.contains(']') || author.contains(']')
    {
        return None;
    }
    let rev = if parts.len() > 3 {
        parts[3].trim().strip_prefix("rev=").unwrap_or("").trim()
    } else {
        ""
    };
    Some((
        ts.to_string(),
        author.to_string(),
        mid.to_string(),
        rev.to_string(),
    ))
}

pub fn load_conversation_messages(
    frontmatter: &serde_json::Value,
    body: &str,
) -> Vec<ConversationMessage> {
    let structured = structured_messages(frontmatter);
    if !structured.is_empty() {
        return structured;
    }
    transcript_messages(body)
}

fn content_hash(text: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn burst_identity_json(message_ids: &[String], content_hashes: &[String]) -> String {
    let mut out = String::new();
    out.push('{');
    out.push_str("\"algorithm\": ");
    out.push_str(&encode_json_string_python_ensure_ascii(BURST_ALGORITHM_VERSION));
    out.push_str(", \"content_hashes\": [");
    for (i, h) in content_hashes.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&encode_json_string_python_ensure_ascii(h));
    }
    out.push_str("], \"message_ids\": [");
    for (i, mid) in message_ids.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&encode_json_string_python_ensure_ascii(mid));
    }
    out.push_str("]}");
    out
}

fn burst_key_for(message_ids: &[String], content_hashes: &[String]) -> String {
    let payload = burst_identity_json(message_ids, content_hashes);
    let mut hasher = Sha256::new();
    hasher.update(payload.as_bytes());
    format!("burst-{:x}", hasher.finalize())[..30].to_string()
}

fn is_boilerplate(text: &str) -> bool {
    let lowered = clean_text(text).to_lowercase();
    BOILERPLATE_PREFIXES
        .iter()
        .any(|prefix| lowered.starts_with(prefix))
}

fn parse_timestamp(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let mut text = clean_text(raw);
    if text.is_empty() {
        return None;
    }
    if text.ends_with('Z') {
        text = format!("{}+00:00", &text[..text.len() - 1]);
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&text) {
        return Some(dt.with_timezone(&chrono::Utc));
    }
    const NAIVE: &[&str] = &[
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];
    for fmt in NAIVE {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&text, fmt) {
            return Some(naive.and_utc());
        }
        if *fmt == "%Y-%m-%d" {
            if let Ok(date) = chrono::NaiveDate::parse_from_str(&text, fmt) {
                return Some(date.and_hms_opt(0, 0, 0)?.and_utc());
            }
        }
    }
    None
}

fn gap_seconds(left: &ConversationMessage, right: &ConversationMessage) -> Option<i64> {
    let start = parse_timestamp(&left.timestamp)?;
    let end = parse_timestamp(&right.timestamp)?;
    Some((end - start).num_seconds())
}

fn sort_messages(mut messages: Vec<ConversationMessage>) -> Vec<ConversationMessage> {
    messages.sort_by(|a, b| (&a.timestamp, &a.message_id).cmp(&(&b.timestamp, &b.message_id)));
    messages
}

fn pack_groups(
    groups: Vec<Vec<ConversationMessage>>,
    token_limit: usize,
) -> Vec<Vec<ConversationMessage>> {
    let mut packed = Vec::new();
    for group in groups {
        let mut current: Vec<ConversationMessage> = Vec::new();
        let mut current_tokens = 0i32;
        for message in group {
            let tokens = token_count(&message.text);
            if !current.is_empty() && current_tokens + tokens > token_limit as i32 {
                packed.push(std::mem::take(&mut current));
                current = vec![message];
                current_tokens = tokens;
                continue;
            }
            current_tokens += tokens;
            current.push(message);
        }
        if !current.is_empty() {
            packed.push(current);
        }
    }
    packed
}

#[derive(Clone, Debug)]
pub struct ConversationBurst {
    pub burst_key: String,
    pub sequence: i64,
    pub message_ids: Vec<String>,
    pub source_revisions: Vec<String>,
    pub text: String,
    pub embed_eligible: bool,
}

pub fn conversation_channel(card_type: &str) -> &'static str {
    if card_type.starts_with("email") {
        "email"
    } else if card_type.starts_with("imessage") {
        "imessage"
    } else if card_type.starts_with("beeper") {
        "beeper"
    } else {
        "chat"
    }
}

pub fn conversation_card_type(card_type: &str) -> bool {
    matches!(
        card_type,
        "email_thread" | "imessage_thread" | "beeper_thread"
    )
}

pub fn segment_conversation_bursts(
    messages: Vec<ConversationMessage>,
    channel: &str,
) -> Vec<ConversationBurst> {
    let ordered = sort_messages(messages);
    if ordered.is_empty() {
        return Vec::new();
    }
    let token_limit = get_burst_token_limit();
    let gap_limit = get_burst_chat_gap_seconds();
    let groups = if channel == "email" {
        ordered.into_iter().map(|item| vec![item]).collect()
    } else {
        let mut groups: Vec<Vec<ConversationMessage>> = Vec::new();
        let mut current: Vec<ConversationMessage> = Vec::new();
        for message in ordered {
            if current.is_empty() {
                current = vec![message];
                continue;
            }
            let previous = current.last().unwrap();
            let author_changed = previous.author.to_lowercase() != message.author.to_lowercase();
            let gap = gap_seconds(previous, &message);
            if author_changed || gap.is_some_and(|g| g > gap_limit) {
                groups.push(std::mem::take(&mut current));
                current = vec![message];
                continue;
            }
            current.push(message);
        }
        if !current.is_empty() {
            groups.push(current);
        }
        groups
    };
    pack_groups(groups, token_limit)
        .into_iter()
        .enumerate()
        .map(|(sequence, group)| {
            let hashes: Vec<String> = group.iter().map(|item| content_hash(&item.text)).collect();
            let ids: Vec<String> = group.iter().map(|item| item.message_id.clone()).collect();
            ConversationBurst {
                burst_key: burst_key_for(&ids, &hashes),
                sequence: sequence as i64,
                message_ids: ids,
                source_revisions: group
                    .iter()
                    .filter(|item| !item.revision.is_empty())
                    .map(|item| item.revision.clone())
                    .collect(),
                text: group
                    .iter()
                    .map(|item| item.text.clone())
                    .collect::<Vec<_>>()
                    .join("\n"),
                embed_eligible: group.iter().any(|item| !is_boilerplate(&item.text)),
            }
        })
        .collect()
}

fn participant_list(frontmatter: &serde_json::Value) -> Vec<String> {
    let raw = frontmatter
        .get("participants")
        .or_else(|| frontmatter.get("participant_handles"));
    match raw {
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| clean_text(&json_text(item)))
            .filter(|item| !item.is_empty())
            .collect(),
        Some(v) => {
            let cleaned = clean_text(&json_text(v));
            if cleaned.is_empty() {
                Vec::new()
            } else {
                vec![cleaned]
            }
        }
        None => Vec::new(),
    }
}

pub fn burst_prefix(frontmatter: &serde_json::Value, channel: &str) -> String {
    let source = match frontmatter.get("source") {
        Some(serde_json::Value::Array(items)) => {
            if items.is_empty() {
                channel.to_string()
            } else {
                let cleaned = clean_text(&json_text(&items[0]));
                if cleaned.is_empty() {
                    channel.to_string()
                } else {
                    cleaned
                }
            }
        }
        Some(v) => {
            let cleaned = clean_text(&json_text(v));
            if cleaned.is_empty() {
                channel.to_string()
            } else {
                cleaned
            }
        }
        None => channel.to_string(),
    };
    let subject = first_clean(
        frontmatter.as_object().unwrap_or(&serde_json::Map::new()),
        &["subject", "display_name", "summary"],
    );
    let participants = participant_list(frontmatter).join(", ");
    let lines = [
        format!("channel: {channel}"),
        format!("subject: {subject}"),
        format!("participants: {participants}"),
        format!("source: {source}"),
    ];
    format!("{}\n\n", lines.join("\n").trim_end())
}

pub struct BurstChunkRecord {
    pub content: String,
    pub burst_key: String,
    pub burst_sequence: i64,
    pub message_ids: Vec<String>,
    pub parent_thread: String,
    pub algorithm_version: String,
    pub embed_eligible: bool,
    pub source_revisions: Vec<String>,
}

pub fn burst_chunk_records(
    frontmatter: &serde_json::Value,
    body: &str,
    card_type: &str,
) -> Vec<BurstChunkRecord> {
    if !conversation_card_type(card_type) {
        return Vec::new();
    }
    let channel = conversation_channel(card_type);
    let bursts = segment_conversation_bursts(load_conversation_messages(frontmatter, body), channel);
    if bursts.is_empty() {
        return Vec::new();
    }
    let parent = clean_json(frontmatter.get("uid"));
    let prefix = burst_prefix(frontmatter, channel);
    bursts
        .into_iter()
        .map(|burst| BurstChunkRecord {
            content: format!("{}{}", prefix, burst.text),
            burst_key: burst.burst_key,
            burst_sequence: burst.sequence,
            message_ids: burst.message_ids,
            parent_thread: parent.clone(),
            algorithm_version: BURST_ALGORITHM_VERSION.to_string(),
            embed_eligible: burst.embed_eligible,
            source_revisions: burst.source_revisions,
        })
        .collect()
}

pub fn append_burst_chunks(
    acc: &mut ChunkAccumulator,
    frontmatter: &serde_json::Value,
    body: &str,
    card_type: &str,
) {
    for burst in burst_chunk_records(frontmatter, body, card_type) {
        acc.append_burst(
            BURST_CHUNK_TYPE,
            &burst.content,
            &["body".to_string(), "messages".to_string()],
            burst.burst_key,
            burst.burst_sequence,
            burst.message_ids,
            burst.parent_thread,
            burst.algorithm_version,
            burst.embed_eligible,
            burst.source_revisions,
        );
    }
}

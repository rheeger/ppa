use serde::{Deserialize, Serialize};

use super::schema::{CHUNK_EVIDENCE_REF_VERSION, UNKNOWN};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SourceSpan {
    #[serde(default)]
    pub source_uid: String,
    #[serde(default)]
    pub source_message_id: String,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default = "canonical_repr")]
    pub representation: String,
    pub start_byte: i64,
    pub end_byte: i64,
}

fn canonical_repr() -> String {
    "canonical_body_utf8".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MessageEvidenceRef {
    #[serde(default)]
    pub message_id: String,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default)]
    pub spans: Vec<SourceSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChunkEvidenceRef {
    pub version: u32,
    pub archive_id: String,
    pub card_uid: String,
    pub chunk_id: String,
    pub chunk_schema_version: String,
    pub algorithm_version: String,
    #[serde(default = "unknown")]
    pub evidence_kind: String,
    #[serde(default)]
    pub lineage_complete: bool,
    #[serde(default)]
    pub parent_thread: String,
    #[serde(default)]
    pub source_revisions: Vec<String>,
    #[serde(default)]
    pub source_spans: Vec<SourceSpan>,
    #[serde(default = "default_true")]
    pub span_unavailable: bool,
    #[serde(default)]
    pub message_refs: Vec<MessageEvidenceRef>,
    #[serde(default)]
    pub message_refs_available: bool,
}

fn unknown() -> String {
    UNKNOWN.to_string()
}
fn default_true() -> bool {
    true
}

impl ChunkEvidenceRef {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != CHUNK_EVIDENCE_REF_VERSION {
            return Err(format!(
                "unsupported ChunkEvidenceRef version: {} (frozen={CHUNK_EVIDENCE_REF_VERSION})",
                self.version
            ));
        }
        if self.archive_id.is_empty() || self.card_uid.is_empty() || self.chunk_id.is_empty() {
            return Err("ChunkEvidenceRef missing archive_id/card_uid/chunk_id".into());
        }
        if !self.span_unavailable {
            for span in &self.source_spans {
                if span.start_byte < 0 || span.end_byte < span.start_byte {
                    return Err("source span offsets must be half-open and non-negative".into());
                }
            }
        }
        Ok(())
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut obj = serde_json::json!({
            "version": self.version,
            "archive_id": self.archive_id,
            "card_uid": self.card_uid,
            "chunk_id": self.chunk_id,
            "chunk_schema_version": self.chunk_schema_version,
            "algorithm_version": self.algorithm_version,
            "evidence_kind": self.evidence_kind,
            "lineage_complete": self.lineage_complete,
            "parent_thread": self.parent_thread,
            "source_revisions": self.source_revisions,
            "span_unavailable": self.span_unavailable,
            "message_refs_available": self.message_refs_available,
        });
        if let Some(map) = obj.as_object_mut() {
            if !self.span_unavailable {
                map.insert(
                    "source_spans".into(),
                    serde_json::to_value(&self.source_spans).unwrap_or(serde_json::json!([])),
                );
            }
            if self.message_refs_available {
                map.insert(
                    "message_refs".into(),
                    serde_json::to_value(&self.message_refs).unwrap_or(serde_json::json!([])),
                );
            }
        }
        obj
    }
}

use std::collections::{HashMap, HashSet};

use crate::chunk::helpers::{chunk_hash_current, split_text_chunks, token_count};

#[derive(Debug, Clone, Default)]
pub struct BurstExtras {
    pub burst_key: String,
    pub burst_sequence: i64,
    pub message_ids: Vec<String>,
    pub parent_thread: String,
    pub algorithm_version: String,
    pub embed_eligible: bool,
    pub source_revisions: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ChunkRecord {
    pub chunk_type: String,
    pub chunk_index: i32,
    pub source_fields: Vec<String>,
    pub content: String,
    pub content_hash: String,
    pub token_count: i32,
    pub burst: Option<BurstExtras>,
}

pub struct ChunkAccumulator {
    limit: usize,
    pub chunks: Vec<ChunkRecord>,
    seen: HashSet<(String, String)>,
    chunk_type_counts: HashMap<String, usize>,
}

impl ChunkAccumulator {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            chunks: Vec::new(),
            seen: HashSet::new(),
            chunk_type_counts: HashMap::new(),
        }
    }

    /// Matches `append_chunks` in `archive_cli/chunk_builders.py` exactly.
    pub fn append_chunks(&mut self, chunk_type: &str, content: &str, source_fields: &[&str]) {
        self.append_record(chunk_type, content, source_fields, None);
    }

    pub fn append_burst(
        &mut self,
        chunk_type: &str,
        content: &str,
        source_fields: &[String],
        burst_key: String,
        burst_sequence: i64,
        message_ids: Vec<String>,
        parent_thread: String,
        algorithm_version: String,
        embed_eligible: bool,
        source_revisions: Vec<String>,
    ) {
        let extras = BurstExtras {
            burst_key,
            burst_sequence,
            message_ids,
            parent_thread,
            algorithm_version,
            embed_eligible,
            source_revisions,
        };
        let fields: Vec<&str> = source_fields.iter().map(String::as_str).collect();
        self.append_record(chunk_type, content, &fields, Some(extras));
    }

    fn append_record(
        &mut self,
        chunk_type: &str,
        content: &str,
        source_fields: &[&str],
        extras: Option<BurstExtras>,
    ) {
        let start_index = *self.chunk_type_counts.get(chunk_type).unwrap_or(&0);
        let pieces = split_text_chunks(content, self.limit);
        for (offset, piece) in pieces.iter().enumerate() {
            let key = (chunk_type.to_string(), piece.clone());
            if self.seen.contains(&key) {
                continue;
            }
            self.seen.insert(key);
            let index = start_index + offset;
            let sf: Vec<String> = source_fields.iter().map(|s| (*s).to_string()).collect();
            let h = chunk_hash_current(chunk_type, piece, &sf);
            let tc = token_count(piece);
            self.chunks.push(ChunkRecord {
                chunk_type: chunk_type.to_string(),
                chunk_index: index as i32,
                source_fields: sf,
                content: piece.clone(),
                content_hash: h,
                token_count: tc,
                burst: extras.clone(),
            });
        }
        self.chunk_type_counts
            .insert(chunk_type.to_string(), start_index + pieces.len());
    }
}

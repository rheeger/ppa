use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use super::evidence::ChunkEvidenceRef;
use super::generation;
use super::graph::EdgeRow;
use super::metadata::CardMeta;
use super::schema::{LAYOUT_FILE, LAYOUT_VERSION};

pub const MAX_CHAIN_WALK: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationLayout {
    #[serde(default = "default_layout_version")]
    pub layout_version: u32,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub parent_generation: String,
    #[serde(default)]
    pub base_generation: String,
    #[serde(default)]
    pub snapshot_id: String,
    #[serde(default)]
    pub source_watermark: i64,
    #[serde(default)]
    pub tombstone_uids: Vec<String>,
    #[serde(default)]
    pub tombstone_chunk_keys: Vec<String>,
    #[serde(default)]
    pub replaced_uids: Vec<String>,
    #[serde(default)]
    pub embedding_spec: Option<serde_json::Value>,
}

fn default_layout_version() -> u32 {
    LAYOUT_VERSION
}

fn default_mode() -> String {
    "full".to_string()
}

impl Default for GenerationLayout {
    fn default() -> Self {
        Self {
            layout_version: LAYOUT_VERSION,
            mode: default_mode(),
            parent_generation: String::new(),
            base_generation: String::new(),
            snapshot_id: String::new(),
            source_watermark: 0,
            tombstone_uids: Vec::new(),
            tombstone_chunk_keys: Vec::new(),
            replaced_uids: Vec::new(),
            embedding_spec: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiveChunk {
    pub chunk_key: String,
    pub card_uid: String,
    pub chunk_type: String,
    pub chunk_index: i32,
    pub evidence: Option<serde_json::Value>,
}

#[derive(Debug)]
pub struct ResolvedLive {
    pub cards: HashMap<String, CardMeta>,
    pub chunks: HashMap<String, LiveChunk>,
    pub edges: HashMap<(String, String, String, String), EdgeRow>,
    pub live_uids: HashSet<String>,
    pub live_chunk_keys: HashSet<String>,
    pub tombstone_uids: HashSet<String>,
    pub chain: Vec<PathBuf>,
    pub chain_ids: Vec<String>,
    pub embedding_spec: Option<serde_json::Value>,
}

pub fn load_layout(dir: &Path) -> PyResult<GenerationLayout> {
    let path = dir.join(LAYOUT_FILE);
    if !path.exists() {
        return Ok(GenerationLayout::default());
    }
    let raw = fs::read_to_string(&path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("layout.json: {e}")))?;
    let layout: GenerationLayout = serde_json::from_str(&raw)
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("layout.json: {e}")))?;
    if layout.layout_version > LAYOUT_VERSION {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "serving_index_layout_unsupported: found {}, need {}",
            layout.layout_version, LAYOUT_VERSION
        )));
    }
    Ok(layout)
}

pub fn walk_chain(index_root: &Path, active_gid: &str) -> PyResult<Vec<(String, PathBuf, GenerationLayout)>> {
    let mut newest_first: Vec<(String, PathBuf, GenerationLayout)> = Vec::new();
    let mut seen = HashSet::new();
    let mut gid = active_gid.trim().to_string();
    if gid.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "serving_index_chain_empty",
        ));
    }
    for _ in 0..MAX_CHAIN_WALK {
        if !seen.insert(gid.clone()) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "serving_index_chain_cycle generation={gid}"
            )));
        }
        let dir = generation::generation_dir(index_root, &gid);
        if !dir.is_dir() {
            return Err(pyo3::exceptions::PyFileNotFoundError::new_err(format!(
                "serving_index_parent_missing generation={gid}"
            )));
        }
        let layout = load_layout(&dir)?;
        let parent = layout.parent_generation.trim().to_string();
        newest_first.push((gid.clone(), dir, layout));
        if parent.is_empty() {
            newest_first.reverse();
            return Ok(newest_first);
        }
        gid = parent;
    }
    Err(pyo3::exceptions::PyValueError::new_err(
        "serving_index_chain_unbounded",
    ))
}

fn iter_jsonl(path: &Path) -> PyResult<Vec<serde_json::Value>> {
    let mut out = Vec::new();
    if !path.exists() {
        return Ok(out);
    }
    let f = fs::File::open(path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(&line)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        out.push(value);
    }
    Ok(out)
}

fn parse_card(value: &serde_json::Value) -> Option<CardMeta> {
    serde_json::from_value(value.clone()).ok()
}

fn parse_chunk(value: &serde_json::Value) -> PyResult<Option<LiveChunk>> {
    let Some(chunk_key) = value.get("chunk_key").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    if chunk_key.is_empty() {
        return Ok(None);
    }
    let card_uid = value
        .get("card_uid")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let evidence = if let Some(raw) = value.get("evidence") {
        let parsed: ChunkEvidenceRef = serde_json::from_value(raw.clone())
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("unsupported ChunkEvidenceRef: {e}")))?;
        if let Err(msg) = parsed.validate() {
            return Err(pyo3::exceptions::PyValueError::new_err(msg));
        }
        Some(parsed.to_json())
    } else {
        None
    };
    Ok(Some(LiveChunk {
        chunk_key: chunk_key.to_string(),
        card_uid,
        chunk_type: value
            .get("chunk_type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        chunk_index: value.get("chunk_index").and_then(|v| v.as_i64()).unwrap_or(0) as i32,
        evidence,
    }))
}

fn parse_edge(value: &serde_json::Value) -> Option<EdgeRow> {
    serde_json::from_value(value.clone()).ok()
}

fn edge_key(edge: &EdgeRow) -> (String, String, String, String) {
    (
        edge.source_uid.clone(),
        edge.target_uid.clone(),
        edge.edge_type.clone(),
        edge.field_name.clone(),
    )
}

fn drop_incident(
    edges: &mut HashMap<(String, String, String, String), EdgeRow>,
    uid: &str,
) {
    edges.retain(|(src, tgt, _, _), _| src != uid && tgt != uid);
}

fn spec_space(spec: &serde_json::Value) -> Option<(String, String, String, String, String, i64)> {
    let obj = spec.as_object()?;
    let dim = obj.get("dimension").and_then(|v| v.as_i64()).unwrap_or(0);
    if dim <= 0 {
        return None;
    }
    Some((
        obj.get("provider_namespace")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        obj.get("model").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        obj.get("model_revision")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        obj.get("metric").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        obj.get("normalization")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        dim,
    ))
}

pub fn check_spec_compat(
    current: Option<&serde_json::Value>,
    incoming: Option<&serde_json::Value>,
) -> PyResult<Option<serde_json::Value>> {
    match (current, incoming) {
        (None, None) => Ok(None),
        (Some(cur), None) => Ok(Some(cur.clone())),
        (None, Some(inc)) => Ok(Some(inc.clone())),
        (Some(cur), Some(inc)) => {
            let left = spec_space(cur);
            let right = spec_space(inc);
            if left.is_some() && right.is_some() && left != right {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "incompatible EmbeddingSpec for this serving generation",
                ));
            }
            if right.is_some() {
                Ok(Some(inc.clone()))
            } else {
                Ok(Some(cur.clone()))
            }
        }
    }
}

pub fn resolve_live(index_root: &Path, active_gid: &str) -> PyResult<ResolvedLive> {
    let chain = walk_chain(index_root, active_gid)?;
    let mut cards: HashMap<String, CardMeta> = HashMap::new();
    let mut chunks: HashMap<String, LiveChunk> = HashMap::new();
    let mut edges: HashMap<(String, String, String, String), EdgeRow> = HashMap::new();
    let mut tombstoned: HashSet<String> = HashSet::new();
    let mut embedding_spec: Option<serde_json::Value> = None;
    let mut chain_dirs = Vec::new();
    let mut chain_ids = Vec::new();

    for (gid, dir, layout) in &chain {
        chain_dirs.push(dir.clone());
        chain_ids.push(gid.clone());
        embedding_spec = check_spec_compat(embedding_spec.as_ref(), layout.embedding_spec.as_ref())?;

        for uid in &layout.tombstone_uids {
            let uid = uid.trim();
            if uid.is_empty() {
                continue;
            }
            tombstoned.insert(uid.to_string());
            cards.remove(uid);
            chunks.retain(|_, chunk| chunk.card_uid != uid);
            drop_incident(&mut edges, uid);
        }
        for uid in &layout.replaced_uids {
            let uid = uid.trim();
            if uid.is_empty() || tombstoned.contains(uid) {
                continue;
            }
            cards.remove(uid);
            chunks.retain(|_, chunk| chunk.card_uid != uid);
            drop_incident(&mut edges, uid);
        }
        for key in &layout.tombstone_chunk_keys {
            let key = key.trim();
            if !key.is_empty() {
                chunks.remove(key);
            }
        }

        for value in iter_jsonl(&dir.join("cards.jsonl"))? {
            if let Some(card) = parse_card(&value) {
                if card.card_uid.is_empty() || tombstoned.contains(&card.card_uid) {
                    continue;
                }
                cards.insert(card.card_uid.clone(), card);
            }
        }
        for value in iter_jsonl(&dir.join("chunks.jsonl"))? {
            if let Some(chunk) = parse_chunk(&value)? {
                if tombstoned.contains(&chunk.card_uid) {
                    continue;
                }
                chunks.insert(chunk.chunk_key.clone(), chunk);
            }
        }
        for value in iter_jsonl(&dir.join("edges.jsonl"))? {
            if let Some(edge) = parse_edge(&value) {
                if edge.source_uid.is_empty() || edge.target_uid.is_empty() {
                    continue;
                }
                if tombstoned.contains(&edge.source_uid) || tombstoned.contains(&edge.target_uid) {
                    continue;
                }
                edges.insert(edge_key(&edge), edge);
            }
        }
    }

    let live_uids: HashSet<String> = cards.keys().cloned().collect();
    let live_chunk_keys: HashSet<String> = chunks.keys().cloned().collect();
    Ok(ResolvedLive {
        cards,
        chunks,
        edges,
        live_uids,
        live_chunk_keys,
        tombstone_uids: tombstoned,
        chain: chain_dirs,
        chain_ids,
        embedding_spec,
    })
}

impl ResolvedLive {
    pub fn chunk_to_card(&self) -> HashMap<String, (String, String, i32)> {
        self.chunks
            .iter()
            .map(|(key, chunk)| {
                (
                    key.clone(),
                    (chunk.card_uid.clone(), chunk.chunk_type.clone(), chunk.chunk_index),
                )
            })
            .collect()
    }

    pub fn chunk_evidence(&self) -> HashMap<String, serde_json::Value> {
        self.chunks
            .iter()
            .filter_map(|(key, chunk)| chunk.evidence.clone().map(|ev| (key.clone(), ev)))
            .collect()
    }

    pub fn to_json(&self) -> serde_json::Value {
        let mut live_uids: Vec<String> = self.live_uids.iter().cloned().collect();
        live_uids.sort();
        let mut live_chunk_keys: Vec<String> = self.live_chunk_keys.iter().cloned().collect();
        live_chunk_keys.sort();
        let mut tombstones: Vec<String> = self.tombstone_uids.iter().cloned().collect();
        tombstones.sort();
        let mut live_chunks: Vec<serde_json::Value> = self
            .chunks
            .values()
            .map(|chunk| {
                serde_json::json!({
                    "chunk_key": chunk.chunk_key,
                    "card_uid": chunk.card_uid,
                    "chunk_type": chunk.chunk_type,
                    "chunk_index": chunk.chunk_index,
                    "evidence": chunk.evidence,
                })
            })
            .collect();
        live_chunks.sort_by(|a, b| {
            a["chunk_key"]
                .as_str()
                .unwrap_or("")
                .cmp(b["chunk_key"].as_str().unwrap_or(""))
        });
        let mut edges: Vec<serde_json::Value> = self
            .edges
            .values()
            .map(|edge| {
                serde_json::json!({
                    "source_uid": edge.source_uid,
                    "target_uid": edge.target_uid,
                    "edge_type": edge.edge_type,
                    "field_name": edge.field_name,
                    "method": edge.method,
                    "confidence": edge.confidence,
                    "evidence_uids": edge.evidence_uids,
                })
            })
            .collect();
        edges.sort_by(|a, b| {
            let left = (
                a["source_uid"].as_str().unwrap_or(""),
                a["target_uid"].as_str().unwrap_or(""),
                a["edge_type"].as_str().unwrap_or(""),
                a["field_name"].as_str().unwrap_or(""),
            );
            let right = (
                b["source_uid"].as_str().unwrap_or(""),
                b["target_uid"].as_str().unwrap_or(""),
                b["edge_type"].as_str().unwrap_or(""),
                b["field_name"].as_str().unwrap_or(""),
            );
            left.cmp(&right)
        });
        serde_json::json!({
            "live_uids": live_uids,
            "live_chunk_keys": live_chunk_keys,
            "live_chunks": live_chunks,
            "edges": edges,
            "tombstone_uids": tombstones,
            "chain": self.chain_ids,
            "embedding_spec": self.embedding_spec,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_gen(root: &Path, gid: &str, layout: &str, cards: &str, chunks: &str, edges: &str) {
        let dir = root.join("generations").join(gid);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(LAYOUT_FILE), layout).unwrap();
        fs::write(dir.join("cards.jsonl"), cards).unwrap();
        fs::write(dir.join("chunks.jsonl"), chunks).unwrap();
        fs::write(dir.join("edges.jsonl"), edges).unwrap();
        fs::write(dir.join("manifest.json"), r#"{"serving_index_format_version":2}"#).unwrap();
    }

    #[test]
    fn tombstone_hides_parent_card_chunks_and_edges() {
        let root = std::env::temp_dir().join(format!("ppa-seg-tomb-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        write_gen(
            &root,
            "base",
            r#"{"layout_version":1,"mode":"full"}"#,
            r#"{"card_uid":"keep","summary":"Keep"}
{"card_uid":"gone","summary":"Gone"}
"#,
            r#"{"chunk_key":"ck-keep","card_uid":"keep","chunk_type":"body","chunk_index":0}
{"chunk_key":"ck-gone","card_uid":"gone","chunk_type":"body","chunk_index":0}
"#,
            r#"{"source_uid":"keep","target_uid":"gone","edge_type":"mentions","field_name":"body"}
"#,
        );
        write_gen(
            &root,
            "delta",
            r#"{"layout_version":1,"mode":"delta","parent_generation":"base","tombstone_uids":["gone"]}"#,
            "",
            "",
            "",
        );
        let live = resolve_live(&root, "delta").unwrap();
        assert!(live.live_uids.contains("keep"));
        assert!(!live.live_uids.contains("gone"));
        assert!(live.live_chunk_keys.contains("ck-keep"));
        assert!(!live.live_chunk_keys.contains("ck-gone"));
        assert!(live.edges.is_empty());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn replacement_by_uid_drops_old_chunks() {
        let root = std::env::temp_dir().join(format!("ppa-seg-repl-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        write_gen(
            &root,
            "base",
            r#"{"layout_version":1,"mode":"full"}"#,
            r#"{"card_uid":"msg","summary":"old"}
"#,
            r#"{"chunk_key":"ck-old","card_uid":"msg","chunk_type":"body","chunk_index":0}
"#,
            "",
        );
        write_gen(
            &root,
            "delta",
            r#"{"layout_version":1,"mode":"delta","parent_generation":"base","replaced_uids":["msg"],"tombstone_chunk_keys":["ck-old"]}"#,
            r#"{"card_uid":"msg","summary":"new"}
"#,
            r#"{"chunk_key":"ck-new","card_uid":"msg","chunk_type":"body","chunk_index":0}
"#,
            "",
        );
        let live = resolve_live(&root, "delta").unwrap();
        assert_eq!(live.cards["msg"].summary, "new");
        assert_eq!(live.live_chunk_keys, HashSet::from(["ck-new".to_string()]));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn cycle_fails_closed() {
        let root = std::env::temp_dir().join(format!("ppa-seg-cycle-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        write_gen(
            &root,
            "a",
            r#"{"layout_version":1,"mode":"delta","parent_generation":"b"}"#,
            "",
            "",
            "",
        );
        write_gen(
            &root,
            "b",
            r#"{"layout_version":1,"mode":"delta","parent_generation":"a"}"#,
            "",
            "",
            "",
        );
        assert!(resolve_live(&root, "a").is_err());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn mixed_vector_space_fails_closed() {
        let root = std::env::temp_dir().join(format!("ppa-seg-spec-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        write_gen(
            &root,
            "base",
            r#"{"layout_version":1,"mode":"full","embedding_spec":{"provider_namespace":"hash","model":"a","model_revision":"1","dimension":4,"metric":"cosine","normalization":"l2","chunk_schema":"6"}}"#,
            "",
            "",
            "",
        );
        write_gen(
            &root,
            "delta",
            r#"{"layout_version":1,"mode":"delta","parent_generation":"base","embedding_spec":{"provider_namespace":"hash","model":"b","model_revision":"1","dimension":8,"metric":"cosine","normalization":"l2","chunk_schema":"6"}}"#,
            "",
            "",
            "",
        );
        assert!(resolve_live(&root, "delta").is_err());
        let _ = fs::remove_dir_all(&root);
    }
}

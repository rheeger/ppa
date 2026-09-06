use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use pyo3::prelude::*;
use serde::Deserialize;

use super::schema::UNKNOWN;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct EdgeRow {
    #[serde(default)]
    pub source_uid: String,
    #[serde(default)]
    pub target_uid: String,
    #[serde(default)]
    pub edge_type: String,
    #[serde(default)]
    pub field_name: String,
    #[serde(default)]
    pub trust: Option<f64>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub method: String,
    #[serde(default)]
    pub evidence_uids: Vec<String>,
    #[serde(default)]
    pub direction: String,
}

#[derive(Debug, Clone)]
pub struct StoredEdge {
    pub neighbor_uid: String,
    pub edge_type: String,
    pub field_name: String,
    pub method: String,
    pub confidence: Option<f64>,
    pub evidence_uids: Vec<String>,
    pub trust: Option<f64>,
    pub direction: String,
    pub source_uid: String,
    pub target_uid: String,
}

impl StoredEdge {
    pub fn to_json(&self, path: String) -> serde_json::Value {
        let mut obj = serde_json::json!({
            "path": path,
            "edge_type": self.edge_type,
            "field_name": self.field_name,
            "method": if self.method.is_empty() { UNKNOWN } else { self.method.as_str() },
            "evidence_uids": self.evidence_uids,
            "direction": if self.direction.is_empty() { "forward" } else { self.direction.as_str() },
            "source_uid": self.source_uid,
            "target_uid": self.target_uid,
        });
        if let Some(map) = obj.as_object_mut() {
            if let Some(confidence) = self.confidence {
                map.insert("confidence".into(), serde_json::json!(confidence));
            }
            if let Some(trust) = self.trust.or(self.confidence) {
                map.insert("trust".into(), serde_json::json!(trust));
            }
        }
        obj
    }
}

fn effective_trust(edge: &EdgeRow) -> Option<f64> {
    edge.confidence.or(edge.trust)
}

fn edge_method(edge: &EdgeRow) -> String {
    let method = edge.method.trim();
    if method.is_empty() {
        UNKNOWN.to_string()
    } else {
        method.to_string()
    }
}

#[derive(Debug, Default, Clone)]
pub struct GraphStore {
    adj: HashMap<String, Vec<StoredEdge>>,
}

impl GraphStore {
    pub fn push_edge(&mut self, edge: EdgeRow) {
        if edge.source_uid.is_empty() || edge.target_uid.is_empty() {
            return;
        }
        let trust = effective_trust(&edge);
        let method = edge_method(&edge);
        let direction = if edge.direction.trim().is_empty() {
            "forward".to_string()
        } else {
            edge.direction.clone()
        };
        let evidence = edge.evidence_uids.clone();
        self.adj.entry(edge.source_uid.clone()).or_default().push(StoredEdge {
            neighbor_uid: edge.target_uid.clone(),
            edge_type: edge.edge_type.clone(),
            field_name: edge.field_name.clone(),
            method: method.clone(),
            confidence: edge.confidence.or(trust),
            evidence_uids: evidence.clone(),
            trust,
            direction: direction.clone(),
            source_uid: edge.source_uid.clone(),
            target_uid: edge.target_uid.clone(),
        });
        self.adj.entry(edge.target_uid.clone()).or_default().push(StoredEdge {
            neighbor_uid: edge.source_uid.clone(),
            edge_type: edge.edge_type.clone(),
            field_name: edge.field_name.clone(),
            method,
            confidence: edge.confidence.or(trust),
            evidence_uids: evidence,
            trust,
            direction,
            source_uid: edge.source_uid,
            target_uid: edge.target_uid,
        });
    }

    pub fn from_edges(edges: impl IntoIterator<Item = EdgeRow>) -> Self {
        let mut store = GraphStore::default();
        for edge in edges {
            store.push_edge(edge);
        }
        store
    }

    pub fn load(dir: &Path) -> PyResult<Self> {
        let path = dir.join("edges.jsonl");
        let mut store = GraphStore::default();
        if !path.exists() {
            return Ok(store);
        }
        let f = fs::File::open(&path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("edges.jsonl: {e}")))?;
        for line in BufReader::new(f).lines() {
            let line = line.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            if line.trim().is_empty() {
                continue;
            }
            let edge: EdgeRow = serde_json::from_str(&line)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            store.push_edge(edge);
        }
        Ok(store)
    }

    pub fn neighbor_trust<F>(&self, anchors: &[String], allow: F) -> HashMap<String, f64>
    where
        F: Fn(&str) -> bool,
    {
        let anchor_set: HashSet<&str> = anchors.iter().map(|s| s.as_str()).collect();
        let mut out: HashMap<String, f64> = HashMap::new();
        for a in anchors {
            if !allow(a) {
                continue;
            }
            if let Some(nbrs) = self.adj.get(a) {
                for edge in nbrs {
                    if anchor_set.contains(edge.neighbor_uid.as_str()) {
                        continue;
                    }
                    if !allow(&edge.neighbor_uid) {
                        continue;
                    }
                    let Some(trust) = edge.trust.or(edge.confidence) else {
                        continue;
                    };
                    let e = out.entry(edge.neighbor_uid.clone()).or_insert(0.0);
                    if trust > *e {
                        *e = trust;
                    }
                }
            }
        }
        out
    }

    pub fn hops(&self, start: &str, hops: usize) -> HashMap<String, Vec<StoredEdge>> {
        self.hops_where(start, hops, |_| true)
    }

    pub fn hops_where<F>(&self, start: &str, hops: usize, allow: F) -> HashMap<String, Vec<StoredEdge>>
    where
        F: Fn(&str) -> bool,
    {
        let mut graph: HashMap<String, Vec<StoredEdge>> = HashMap::new();
        if !allow(start) {
            return graph;
        }
        let mut seen = HashSet::new();
        let mut q = VecDeque::new();
        q.push_back((start.to_string(), 0usize));
        seen.insert(start.to_string());
        while let Some((node, depth)) = q.pop_front() {
            if depth >= hops {
                continue;
            }
            let mut targets = Vec::new();
            if let Some(nbrs) = self.adj.get(&node) {
                for edge in nbrs {
                    if !allow(&edge.neighbor_uid) {
                        continue;
                    }
                    targets.push(edge.clone());
                    if seen.insert(edge.neighbor_uid.clone()) {
                        q.push_back((edge.neighbor_uid.clone(), depth + 1));
                    }
                }
            }
            graph.insert(node, targets);
        }
        graph
    }

    pub fn pointers(&self, uids: &[String]) -> HashMap<String, HashMap<String, Vec<String>>> {
        let wanted: HashSet<&str> = uids.iter().map(|s| s.as_str()).collect();
        let fields = ["attachments", "duplicates", "message", "thread", "source_email", "parent"];
        let mut out: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
        for uid in uids {
            let mut slot = HashMap::new();
            for f in fields {
                slot.insert(f.to_string(), Vec::new());
            }
            out.insert(uid.clone(), slot);
        }
        for uid in uids {
            if let Some(nbrs) = self.adj.get(uid) {
                for edge in nbrs {
                    if !wanted.contains(edge.neighbor_uid.as_str()) && !fields.contains(&edge.field_name.as_str()) {
                        // still record if field matches
                    }
                    if let Some(slot) = out.get_mut(uid) {
                        if let Some(list) = slot.get_mut(&edge.field_name) {
                            if !list.contains(&edge.neighbor_uid) {
                                list.push(edge.neighbor_uid.clone());
                            }
                        }
                    }
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_trust_does_not_upgrade() {
        let (_dir, store) = write_edges(
            "missing-trust",
            r#"{"source_uid":"a","target_uid":"b","edge_type":"possible_same_person","field_name":"","method":"inferred"}
"#,
        );
        assert_eq!(store.neighbor_trust(&["a".into()], |_| true), HashMap::new());
        let hops = store.hops("a", 1);
        let edge = &hops["a"][0];
        assert_eq!(edge.method, "inferred");
        assert!(edge.trust.is_none());
        assert!(edge.confidence.is_none());
    }

    #[test]
    fn hops_where_does_not_traverse_denied() {
        let (_dir, store) = write_edges(
            "deny-hop",
            r#"{"source_uid":"a","target_uid":"b","edge_type":"wikilink","field_name":"body"}
{"source_uid":"b","target_uid":"c","edge_type":"wikilink","field_name":"body"}
"#,
        );
        let hops = store.hops_where("a", 2, |uid| uid != "b");
        assert!(!hops.contains_key("b"));
        assert!(!hops.contains_key("c"));
        assert!(hops.get("a").map(|edges| edges.is_empty()).unwrap_or(true));
    }

    #[test]
    fn warehouse_confidence_derives_trust() {
        let (_dir, store) = write_edges(
            "warehouse-confidence",
            r#"{"source_uid":"a","target_uid":"b","edge_type":"wikilink","field_name":"body","confidence":1.0,"method":"unknown"}
"#,
        );
        assert_eq!(store.neighbor_trust(&["a".into()], |_| true).get("b").copied(), Some(1.0));
    }

    fn write_edges(name: &str, contents: &str) -> (std::path::PathBuf, GraphStore) {
        let dir = std::env::temp_dir().join(format!("ppa-graph-{}-{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("edges.jsonl"), contents).unwrap();
        let store = GraphStore::load(&dir).unwrap();
        (dir, store)
    }
}

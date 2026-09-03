use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use pyo3::prelude::*;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct EdgeRow {
    #[serde(default)]
    source_uid: String,
    #[serde(default)]
    target_uid: String,
    #[serde(default)]
    edge_type: String,
    #[serde(default)]
    field_name: String,
    #[serde(default = "default_trust")]
    trust: f64,
}

fn default_trust() -> f64 {
    1.0
}

#[derive(Debug, Default, Clone)]
pub struct GraphStore {
    adj: HashMap<String, Vec<(String, f64, String, String)>>,
}

impl GraphStore {
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
            if edge.source_uid.is_empty() || edge.target_uid.is_empty() {
                continue;
            }
            store.adj.entry(edge.source_uid.clone()).or_default().push((
                edge.target_uid.clone(),
                edge.trust,
                edge.edge_type.clone(),
                edge.field_name.clone(),
            ));
            store.adj.entry(edge.target_uid).or_default().push((
                edge.source_uid,
                edge.trust,
                edge.edge_type,
                edge.field_name,
            ));
        }
        Ok(store)
    }

    pub fn neighbor_trust(&self, anchors: &[String]) -> HashMap<String, f64> {
        let anchor_set: HashSet<&str> = anchors.iter().map(|s| s.as_str()).collect();
        let mut out: HashMap<String, f64> = HashMap::new();
        for a in anchors {
            if let Some(nbrs) = self.adj.get(a) {
                for (uid, trust, _, _) in nbrs {
                    if anchor_set.contains(uid.as_str()) {
                        continue;
                    }
                    let e = out.entry(uid.clone()).or_insert(0.0);
                    if *trust > *e {
                        *e = *trust;
                    }
                }
            }
        }
        out
    }

    pub fn hops(&self, start: &str, hops: usize) -> HashMap<String, Vec<(String, String)>> {
        let mut graph: HashMap<String, Vec<(String, String)>> = HashMap::new();
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
                for (uid, _trust, edge_type, _field) in nbrs {
                    targets.push((uid.clone(), edge_type.clone()));
                    if seen.insert(uid.clone()) {
                        q.push_back((uid.clone(), depth + 1));
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
                for (other, _t, _et, field) in nbrs {
                    if !wanted.contains(other.as_str()) && !fields.contains(&field.as_str()) {
                        // still record if field matches
                    }
                    if let Some(slot) = out.get_mut(uid) {
                        if let Some(list) = slot.get_mut(field) {
                            if !list.contains(other) {
                                list.push(other.clone());
                            }
                        }
                    }
                }
            }
        }
        out
    }
}

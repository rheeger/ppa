use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

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

#[derive(Debug, Clone)]
pub struct GraphBudget {
    pub max_depth: usize,
    pub max_nodes: usize,
    pub max_edges: usize,
    pub max_elapsed_ms: u64,
    pub allowed_relation_types: HashSet<String>,
}

impl GraphBudget {
    pub fn public(hops: usize) -> Self {
        Self {
            max_depth: hops.clamp(1, 2),
            max_nodes: 256,
            max_edges: 512,
            max_elapsed_ms: 250,
            allowed_relation_types: HashSet::new(),
        }
    }

    pub fn compat(hops: usize) -> Self {
        Self {
            max_depth: hops.max(1),
            max_nodes: 10_000,
            max_edges: 20_000,
            max_elapsed_ms: 0,
            allowed_relation_types: HashSet::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BoundedGraph {
    pub graph: HashMap<String, Vec<StoredEdge>>,
    pub truncated: bool,
    pub truncation_reason: String,
    pub nodes_visited: usize,
    pub edges_emitted: usize,
    pub frontier: Vec<String>,
    pub depth: usize,
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
        self.hops_bounded(start, &GraphBudget::compat(hops), allow).graph
    }

    pub fn hops_bounded<F>(&self, start: &str, budget: &GraphBudget, allow: F) -> BoundedGraph
    where
        F: Fn(&str) -> bool,
    {
        let started = Instant::now();
        let mut out = BoundedGraph {
            depth: budget.max_depth,
            ..BoundedGraph::default()
        };
        if !allow(start) {
            return out;
        }
        let mut seen = HashSet::new();
        let mut q = VecDeque::new();
        q.push_back((start.to_string(), 0usize));
        seen.insert(start.to_string());
        out.nodes_visited = 1;
        while let Some((node, depth)) = q.pop_front() {
            if budget.max_elapsed_ms > 0 && started.elapsed().as_millis() as u64 >= budget.max_elapsed_ms {
                out.truncated = true;
                out.truncation_reason = "elapsed".into();
                out.frontier.push(node);
                while let Some((pending, _)) = q.pop_front() {
                    out.frontier.push(pending);
                }
                break;
            }
            if depth >= budget.max_depth {
                continue;
            }
            let mut targets = Vec::new();
            if let Some(nbrs) = self.adj.get(&node) {
                let mut sorted: Vec<&StoredEdge> = nbrs.iter().collect();
                sorted.sort_by(|left, right| {
                    left.neighbor_uid
                        .cmp(&right.neighbor_uid)
                        .then(left.edge_type.cmp(&right.edge_type))
                        .then(left.field_name.cmp(&right.field_name))
                });
                for edge in sorted {
                    if !allow(&edge.neighbor_uid) {
                        continue;
                    }
                    if !budget.allowed_relation_types.is_empty()
                        && !budget.allowed_relation_types.contains(&edge.edge_type)
                    {
                        continue;
                    }
                    if out.edges_emitted >= budget.max_edges {
                        out.truncated = true;
                        out.truncation_reason = "max_edges".into();
                        if !seen.contains(&edge.neighbor_uid) {
                            out.frontier.push(edge.neighbor_uid.clone());
                        }
                        continue;
                    }
                    if !seen.contains(&edge.neighbor_uid) && out.nodes_visited >= budget.max_nodes {
                        out.truncated = true;
                        if out.truncation_reason.is_empty() {
                            out.truncation_reason = "max_nodes".into();
                        }
                        out.frontier.push(edge.neighbor_uid.clone());
                        continue;
                    }
                    targets.push(edge.clone());
                    out.edges_emitted += 1;
                    if seen.insert(edge.neighbor_uid.clone()) {
                        q.push_back((edge.neighbor_uid.clone(), depth + 1));
                        out.nodes_visited += 1;
                    }
                }
            }
            out.graph.insert(node, targets);
            if out.truncated && (out.truncation_reason == "max_edges" || out.truncation_reason == "max_nodes") {
                while let Some((pending, _)) = q.pop_front() {
                    out.frontier.push(pending);
                }
                break;
            }
        }
        out.frontier.sort();
        out.frontier.dedup();
        out
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
    fn hops_bounded_truncates_high_degree_hub() {
        let mut contents = String::new();
        for index in 0..80 {
            contents.push_str(&format!(
                r#"{{"source_uid":"hub","target_uid":"n{index:03}","edge_type":"wikilink","field_name":"body","method":"source_reported","evidence_uids":["ev-{index:03}"]}}"#
            ));
            contents.push('\n');
        }
        let (_dir, store) = write_edges("hub-cap", &contents);
        let budget = GraphBudget {
            max_depth: 1,
            max_nodes: 100,
            max_edges: 32,
            max_elapsed_ms: 0,
            allowed_relation_types: HashSet::new(),
        };
        let bounded = store.hops_bounded("hub", &budget, |_| true);
        assert!(bounded.truncated);
        assert_eq!(bounded.truncation_reason, "max_edges");
        assert!(bounded.edges_emitted <= 32);
        assert!(bounded.nodes_visited <= 100);
        let hub = &bounded.graph["hub"];
        assert_eq!(hub.len(), bounded.edges_emitted);
        assert!(!hub[0].evidence_uids.is_empty());
        assert_eq!(hub[0].method, "source_reported");
        assert!(bounded.frontier.len() >= 1);
    }

    #[test]
    fn hops_bounded_skips_denied_and_keeps_stable_order() {
        let (_dir, store) = write_edges(
            "deny-budget",
            r#"{"source_uid":"hub","target_uid":"denied","edge_type":"wikilink","field_name":"body"}
{"source_uid":"denied","target_uid":"secret","edge_type":"wikilink","field_name":"body"}
{"source_uid":"hub","target_uid":"ok-b","edge_type":"wikilink","field_name":"body"}
{"source_uid":"hub","target_uid":"ok-a","edge_type":"wikilink","field_name":"body"}
"#,
        );
        let budget = GraphBudget {
            max_depth: 2,
            max_nodes: 16,
            max_edges: 16,
            max_elapsed_ms: 0,
            allowed_relation_types: HashSet::new(),
        };
        let bounded = store.hops_bounded("hub", &budget, |uid| uid != "denied" && uid != "secret");
        assert!(!bounded.graph.contains_key("denied"));
        assert!(!bounded.graph.contains_key("secret"));
        let hub = &bounded.graph["hub"];
        let neighbors: Vec<&str> = hub.iter().map(|edge| edge.neighbor_uid.as_str()).collect();
        assert_eq!(neighbors, vec!["ok-a", "ok-b"]);
        assert!(!neighbors.contains(&"denied"));
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

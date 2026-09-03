use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;

use chrono::{Duration, NaiveDate};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};

use crate::materializer::time_parse::parse_timestamp_to_utc_rust;

#[derive(Debug, Clone)]
pub struct ActivityEntry {
    pub at_ms: i64,
    pub end_ms: Option<i64>,
    pub uid: String,
}

pub struct NeighborHit<'a> {
    pub card: &'a CardMeta,
    pub leg: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CardMeta {
    pub card_uid: String,
    #[serde(default)]
    pub rel_path: String,
    #[serde(default)]
    pub summary: String,
    #[serde(default)]
    pub r#type: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub activity_at: String,
    #[serde(default)]
    pub activity_end_at: String,
    #[serde(default)]
    pub sources: Vec<String>,
    #[serde(default)]
    pub people: Vec<String>,
    #[serde(default)]
    pub orgs: Vec<String>,
    #[serde(default)]
    pub corpus_state: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub emails: Vec<String>,
    #[serde(default)]
    pub search_text: String,
}

#[derive(Debug, Default)]
pub struct MetadataStore {
    pub by_uid: HashMap<String, CardMeta>,
    pub by_slug: HashMap<String, String>,
    pub by_path: HashMap<String, String>,
    /// Cards with a parseable `activity_at`, sorted by `(at_ms, uid)`.
    pub by_activity: Vec<ActivityEntry>,
    /// Indexes into `by_activity` for cards that have an interval end.
    pub intervals: Vec<usize>,
}

fn activity_ms(raw: &str) -> Option<i64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    parse_timestamp_to_utc_rust(s)
        .or_else(|| parse_timestamp_to_utc_rust(&s.replace(' ', "T")))
        .map(|dt| dt.timestamp_millis())
}

fn is_date_only(raw: &str) -> bool {
    let s = raw.trim();
    s.len() == 10 && NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}

impl MetadataStore {
    pub fn load(dir: &Path) -> PyResult<Self> {
        let path = dir.join("cards.jsonl");
        let mut store = MetadataStore::default();
        if !path.exists() {
            return Ok(store);
        }
        let f = fs::File::open(&path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("cards.jsonl: {e}")))?;
        for line in BufReader::new(f).lines() {
            let line = line.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            if line.trim().is_empty() {
                continue;
            }
            let card: CardMeta = serde_json::from_str(&line)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            if !card.slug.is_empty() {
                store.by_slug.insert(card.slug.to_lowercase(), card.card_uid.clone());
            }
            if !card.rel_path.is_empty() {
                store.by_path.insert(card.rel_path.clone(), card.card_uid.clone());
            }
            for alias in &card.aliases {
                store.by_slug.insert(alias.to_lowercase(), card.card_uid.clone());
            }
            store.by_uid.insert(card.card_uid.clone(), card);
        }
        store.rebuild_activity_index();
        Ok(store)
    }

    fn rebuild_activity_index(&mut self) {
        let mut entries: Vec<ActivityEntry> = self
            .by_uid
            .values()
            .filter_map(|card| {
                let at_ms = activity_ms(&card.activity_at)?;
                Some(ActivityEntry {
                    at_ms,
                    end_ms: activity_ms(&card.activity_end_at),
                    uid: card.card_uid.clone(),
                })
            })
            .collect();
        entries.sort_by(|a, b| a.at_ms.cmp(&b.at_ms).then_with(|| a.uid.cmp(&b.uid)));
        self.intervals = entries
            .iter()
            .enumerate()
            .filter_map(|(i, e)| e.end_ms.map(|_| i))
            .collect();
        self.by_activity = entries;
    }

    pub fn matches_filters(
        &self,
        card: &CardMeta,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
        org_filter: &str,
        start_date: &str,
        end_date: &str,
    ) -> bool {
        if !type_filter.is_empty() && card.r#type != type_filter {
            return false;
        }
        if !source_filter.is_empty()
            && !card
                .sources
                .iter()
                .any(|s| s == source_filter || s.contains(source_filter))
        {
            return false;
        }
        if !people_filter.is_empty() {
            let needle = people_filter.to_lowercase();
            if !card
                .people
                .iter()
                .any(|p| p.to_lowercase() == needle || p.to_lowercase().contains(&needle))
            {
                return false;
            }
        }
        if !org_filter.is_empty()
            && !card
                .orgs
                .iter()
                .any(|o| o == org_filter || o.to_lowercase().contains(&org_filter.to_lowercase()))
        {
            return false;
        }
        let act = card.activity_at.get(..10).unwrap_or("");
        if !start_date.is_empty() && act < start_date.get(..10).unwrap_or(start_date) {
            return false;
        }
        if !end_date.is_empty() && act > end_date.get(..10).unwrap_or(end_date) {
            return false;
        }
        true
    }

    fn matches_neighbor_filters(
        &self,
        card: &CardMeta,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
    ) -> bool {
        self.matches_filters(card, type_filter, source_filter, people_filter, "", "", "")
    }

    /// First index with `at_ms >= ts_ms`.
    fn first_at_or_after(&self, ts_ms: i64) -> usize {
        self.by_activity.partition_point(|e| e.at_ms < ts_ms)
    }

    /// First index with `at_ms > ts_ms` (exclusive upper bound for `<= ts_ms`).
    fn first_after(&self, ts_ms: i64) -> usize {
        self.by_activity.partition_point(|e| e.at_ms <= ts_ms)
    }

    pub fn timeline_range(
        &self,
        start_date: &str,
        end_date: &str,
        limit: usize,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
    ) -> Vec<&CardMeta> {
        let start_key = start_date.get(..10).unwrap_or(start_date);
        let end_key = end_date.get(..10).unwrap_or(end_date);
        let start_idx = if start_key.is_empty() {
            0
        } else if let Some(ms) = activity_ms(start_key) {
            self.first_at_or_after(ms)
        } else {
            0
        };
        let mut out = Vec::new();
        for entry in self.by_activity.get(start_idx..).unwrap_or(&[]) {
            if let Some(card) = self.by_uid.get(&entry.uid) {
                let act = card.activity_at.get(..10).unwrap_or("");
                if !end_key.is_empty() && act > end_key {
                    break;
                }
                if self.matches_neighbor_filters(card, type_filter, source_filter, people_filter)
                    && (start_key.is_empty() || act >= start_key)
                {
                    out.push(card);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        out
    }

    /// Keyset scan around `timestamp`. `None` means the timestamp could not be parsed.
    pub fn temporal_neighbors(
        &self,
        timestamp: &str,
        direction: &str,
        limit: usize,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
    ) -> Option<Vec<NeighborHit<'_>>> {
        let ts_ms = activity_ms(timestamp)?;
        let (window_start, window_end) = if is_date_only(timestamp) {
            let end = parse_timestamp_to_utc_rust(timestamp.trim())
                .and_then(|dt| dt.checked_add_signed(Duration::days(1)))
                .map(|dt| dt.timestamp_millis() - 1)
                .unwrap_or(ts_ms);
            (ts_ms, end)
        } else {
            (ts_ms, ts_ms)
        };
        let per_leg = limit.max(1);
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<NeighborHit<'_>> = Vec::new();

        let want_forward = direction == "forward" || direction == "both";
        let want_backward = direction == "backward" || direction == "both";

        if direction != "forward" && direction != "backward" {
            self.collect_during(
                window_start,
                window_end,
                per_leg,
                type_filter,
                source_filter,
                people_filter,
                &mut seen,
                &mut out,
            );
        }

        if want_backward {
            self.collect_backward(
                window_end,
                per_leg,
                type_filter,
                source_filter,
                people_filter,
                &mut seen,
                &mut out,
            );
        }
        if want_forward {
            self.collect_forward(
                window_start,
                per_leg,
                type_filter,
                source_filter,
                people_filter,
                &mut seen,
                &mut out,
            );
        }

        out.truncate(limit.max(1));
        Some(out)
    }

    fn collect_during<'a>(
        &'a self,
        window_start: i64,
        window_end: i64,
        limit: usize,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
        seen: &mut HashSet<String>,
        out: &mut Vec<NeighborHit<'a>>,
    ) {
        let mut added = 0usize;
        let lo = self.first_at_or_after(window_start);
        let hi = self.first_after(window_end);
        for entry in self.by_activity.get(lo..hi).unwrap_or(&[]) {
            if added >= limit {
                break;
            }
            if !seen.insert(entry.uid.clone()) {
                continue;
            }
            if let Some(card) = self.by_uid.get(&entry.uid) {
                if self.matches_neighbor_filters(card, type_filter, source_filter, people_filter) {
                    out.push(NeighborHit { card, leg: "during" });
                    added += 1;
                }
            }
        }
        if added >= limit || self.intervals.is_empty() {
            return;
        }
        for &idx in &self.intervals {
            if added >= limit {
                break;
            }
            let Some(entry) = self.by_activity.get(idx) else {
                continue;
            };
            let Some(end_ms) = entry.end_ms else {
                continue;
            };
            if entry.at_ms > window_end || end_ms < window_start {
                continue;
            }
            if !seen.insert(entry.uid.clone()) {
                continue;
            }
            if let Some(card) = self.by_uid.get(&entry.uid) {
                if self.matches_neighbor_filters(card, type_filter, source_filter, people_filter) {
                    out.push(NeighborHit { card, leg: "during" });
                    added += 1;
                }
            }
        }
    }

    fn collect_forward<'a>(
        &'a self,
        ts_ms: i64,
        limit: usize,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
        seen: &mut HashSet<String>,
        out: &mut Vec<NeighborHit<'a>>,
    ) {
        let mut added = 0usize;
        let start = self.first_at_or_after(ts_ms);
        for entry in self.by_activity.get(start..).unwrap_or(&[]) {
            if added >= limit {
                break;
            }
            if !seen.insert(entry.uid.clone()) {
                continue;
            }
            if let Some(card) = self.by_uid.get(&entry.uid) {
                if self.matches_neighbor_filters(card, type_filter, source_filter, people_filter) {
                    out.push(NeighborHit {
                        card,
                        leg: "forward",
                    });
                    added += 1;
                }
            }
        }
    }

    fn collect_backward<'a>(
        &'a self,
        ts_ms: i64,
        limit: usize,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
        seen: &mut HashSet<String>,
        out: &mut Vec<NeighborHit<'a>>,
    ) {
        let mut added = 0usize;
        let end = self.first_after(ts_ms);
        for entry in self.by_activity[..end].iter().rev() {
            if added >= limit {
                break;
            }
            if !seen.insert(entry.uid.clone()) {
                continue;
            }
            if let Some(card) = self.by_uid.get(&entry.uid) {
                if self.matches_neighbor_filters(card, type_filter, source_filter, people_filter) {
                    out.push(NeighborHit {
                        card,
                        leg: "backward",
                    });
                    added += 1;
                }
            }
        }
    }
}

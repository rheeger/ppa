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
    pub phones: Vec<String>,
    #[serde(default)]
    pub external_ids: Vec<String>,
    #[serde(default)]
    pub search_text: String,
    #[serde(default)]
    pub source_revision: String,
    #[serde(default)]
    pub retrieval_weight: Option<f64>,
    #[serde(default)]
    pub provenance_summary: String,
    #[serde(default)]
    pub domains: Vec<String>,
    #[serde(default)]
    pub required_sources: Vec<String>,
    #[serde(default)]
    pub accounts: Vec<String>,
    #[serde(default)]
    pub lineage_complete: Option<bool>,
}

#[derive(Debug, Clone, Default)]
pub struct AccessPolicy {
    pub deny: bool,
    pub restricted: bool,
    pub allowed_sources: Vec<String>,
    pub allowed_domains: Vec<String>,
}

fn norm_label(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn source_allowed(allowed: &[String], source: &str) -> bool {
    let needle = norm_label(source);
    if needle.is_empty() {
        return false;
    }
    allowed.iter().any(|item| {
        let allow = norm_label(item);
        !allow.is_empty()
            && (needle == allow || needle.starts_with(&format!("{allow}:")) || allow.starts_with(&format!("{needle}:")))
    })
}

fn classify_domain(card: &CardMeta) -> String {
    if !card.domains.is_empty() {
        return norm_label(&card.domains[0]);
    }
    let kind = card.r#type.replace('-', "_").to_ascii_lowercase();
    match kind.as_str() {
        "medical_record" | "vaccination" | "health_metric" => "medical".into(),
        "finance" | "purchase" | "meal_order" | "grocery_order" | "ride" | "flight" | "subscription"
        | "invoice" | "receipt" | "bank_transaction" | "tax_document" => "finance".into(),
        "email_message" | "email_thread" | "email_attachment" | "imessage_message" | "imessage_thread"
        | "imessage_attachment" | "beeper_message" | "beeper_thread" | "beeper_attachment" | "sms" => {
            "communication".into()
        }
        "calendar_event" | "meeting_transcript" => "calendar".into(),
        "person" | "organization" | "place" => "identity".into(),
        "git_repository" | "git_commit" | "git_thread" | "git_message" => "code".into(),
        "media_asset" | "document" => "media".into(),
        _ => {
            let joined = card
                .sources
                .iter()
                .map(|s| norm_label(s))
                .collect::<Vec<_>>()
                .join(" ");
            if ["medical", "health", "hospital", "clinic", "ehr"]
                .iter()
                .any(|m| joined.contains(m))
            {
                "medical".into()
            } else if ["bank", "finance", "stripe", "plaid", "tax", "payroll"]
                .iter()
                .any(|m| joined.contains(m))
            {
                "finance".into()
            } else if kind.is_empty() {
                "unknown".into()
            } else {
                "general".into()
            }
        }
    }
}

impl AccessPolicy {
    pub fn unrestricted() -> Self {
        Self::default()
    }

    pub fn permits(&self, card: &CardMeta) -> bool {
        if self.deny {
            return false;
        }
        if !self.restricted {
            return true;
        }
        let required: &[String] = if !card.required_sources.is_empty() {
            &card.required_sources
        } else {
            &card.sources
        };
        if required.is_empty() {
            return false;
        }
        if !self.allowed_sources.is_empty() {
            for source in required {
                if !source_allowed(&self.allowed_sources, source) {
                    return false;
                }
            }
        }
        if card.lineage_complete == Some(false) {
            return false;
        }
        if !self.allowed_domains.is_empty() {
            let domain = classify_domain(card);
            if domain.is_empty() || domain == "unknown" {
                return false;
            }
            if !self.allowed_domains.iter().any(|d| norm_label(d) == domain) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Default, Clone)]
pub struct ChunkAdjacency {
    by_card_type: HashMap<(String, String), Vec<(i32, String)>>,
}

impl ChunkAdjacency {
    pub fn insert(&mut self, card_uid: String, chunk_type: String, chunk_index: i32, chunk_key: String) {
        self.by_card_type
            .entry((card_uid, chunk_type))
            .or_default()
            .push((chunk_index, chunk_key));
    }

    pub fn finalize(&mut self) {
        for slot in self.by_card_type.values_mut() {
            slot.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        }
    }

    pub fn neighbors(&self, card_uid: &str, chunk_type: &str, chunk_index: i32) -> (Option<String>, Option<String>) {
        let Some(slot) = self.by_card_type.get(&(card_uid.to_string(), chunk_type.to_string())) else {
            return (None, None);
        };
        let Some(position) = slot.iter().position(|(index, _)| *index == chunk_index) else {
            return (None, None);
        };
        let preceding = position.checked_sub(1).map(|index| slot[index].1.clone());
        let following = slot.get(position + 1).map(|item| item.1.clone());
        (preceding, following)
    }
}

#[derive(Debug, Default)]
pub struct MetadataStore {
    pub by_uid: HashMap<String, CardMeta>,
    pub by_slug: HashMap<String, String>,
    pub by_path: HashMap<String, String>,
    pub by_email: HashMap<String, Vec<String>>,
    pub by_phone: HashMap<String, Vec<String>>,
    pub by_external_id: HashMap<String, String>,
    /// Cards with a parseable `activity_at`, sorted by `(at_ms, uid)`.
    pub by_activity: Vec<ActivityEntry>,
    /// Indexes into `by_activity` for cards that have an interval end.
    pub intervals: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct PreparedPeopleFilter {
    pub needle: String,
    pub uids: HashSet<String>,
    pub status: &'static str,
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
    fn index_card(&mut self, card: CardMeta) {
        if !card.slug.is_empty() {
            self.by_slug.insert(card.slug.to_lowercase(), card.card_uid.clone());
        }
        if !card.rel_path.is_empty() {
            self.by_path.insert(card.rel_path.clone(), card.card_uid.clone());
        }
        for alias in &card.aliases {
            self.by_slug.insert(alias.to_lowercase(), card.card_uid.clone());
        }
        for email in &card.emails {
            let key = email.to_lowercase();
            if !key.is_empty() {
                let slot = self.by_email.entry(key).or_default();
                if !slot.contains(&card.card_uid) {
                    slot.push(card.card_uid.clone());
                }
            }
        }
        for phone in &card.phones {
            for form in crate::canon::phone_alias_forms(phone) {
                let slot = self.by_phone.entry(form).or_default();
                if !slot.contains(&card.card_uid) {
                    slot.push(card.card_uid.clone());
                }
            }
        }
        for ext in &card.external_ids {
            let key = ext.trim().to_string();
            if !key.is_empty() {
                self.by_external_id.insert(key.clone(), card.card_uid.clone());
                self.by_external_id.insert(key.to_lowercase(), card.card_uid.clone());
            }
        }
        self.by_uid.insert(card.card_uid.clone(), card);
    }

    fn resolve_people_filter_uids(&self, people_filter: &str) -> HashSet<String> {
        let parsed = crate::canon::parse_wikilink(people_filter);
        let parsed_l = parsed.to_lowercase();
        let slug = crate::canon::normalize_slug(&parsed);
        let email = crate::canon::normalize_email(&parsed);
        let mut uids = HashSet::new();
        if let Some(card) = self.by_uid.get(&parsed) {
            if card.r#type == "person" {
                uids.insert(card.card_uid.clone());
            }
        }
        if let Some(uid) = self.by_slug.get(&parsed_l).or_else(|| self.by_slug.get(&slug)) {
            uids.insert(uid.clone());
        }
        if parsed.contains('@') {
            if let Some(found) = self.by_email.get(&email) {
                uids.extend(found.iter().cloned());
            }
        }
        for form in crate::canon::phone_alias_forms(&parsed) {
            if let Some(found) = self.by_phone.get(&form) {
                uids.extend(found.iter().cloned());
            }
        }
        if uids.is_empty() {
            for card in self.by_uid.values() {
                if card.r#type != "person" {
                    continue;
                }
                if card.summary.to_lowercase() == parsed_l
                    || card.aliases.iter().any(|alias| alias.to_lowercase() == parsed_l)
                {
                    uids.insert(card.card_uid.clone());
                }
            }
        }
        uids
    }

    pub fn prepare_people_filter(&self, people_filter: &str) -> PreparedPeopleFilter {
        let needle = people_filter.trim().to_string();
        if needle.is_empty() {
            return PreparedPeopleFilter {
                needle,
                uids: HashSet::new(),
                status: "none",
            };
        }
        let uids = self.resolve_people_filter_uids(&needle);
        let status = match uids.len() {
            0 => "unresolved",
            1 => "unique",
            _ => "ambiguous",
        };
        PreparedPeopleFilter { needle, uids, status }
    }

    pub fn people_ok(&self, card: &CardMeta, people: &PreparedPeopleFilter) -> bool {
        if people.status == "none" {
            return true;
        }
        if people.uids.is_empty() {
            return false;
        }
        card.people.iter().any(|person| people.uids.contains(person))
    }

    pub fn person_resolution(&self, needle: &str) -> PreparedPeopleFilter {
        let prepared = self.prepare_people_filter(needle);
        if prepared.status != "none" {
            return prepared;
        }
        PreparedPeopleFilter {
            needle: needle.to_string(),
            uids: HashSet::new(),
            status: "unresolved",
        }
    }

    pub fn eligible_prepared(
        &self,
        card: &CardMeta,
        policy: &AccessPolicy,
        type_filter: &str,
        source_filter: &str,
        people: &PreparedPeopleFilter,
        org_filter: &str,
        start_date: &str,
        end_date: &str,
    ) -> bool {
        if !policy.permits(card) {
            return false;
        }
        if Self::is_suppressed(card) {
            return false;
        }
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
        if !self.people_ok(card, people) {
            return false;
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
        if !end_date.is_empty() && !act.is_empty() && act > end_date.get(..10).unwrap_or(end_date) {
            return false;
        }
        true
    }

    pub fn from_cards(cards: impl IntoIterator<Item = CardMeta>) -> Self {
        let mut store = MetadataStore::default();
        for card in cards {
            store.index_card(card);
        }
        store.rebuild_activity_index();
        store
    }

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
            store.index_card(card);
        }
        store.rebuild_activity_index();
        Ok(store)
    }

    pub fn is_suppressed(card: &CardMeta) -> bool {
        card.corpus_state == "suppressed"
    }

    pub fn exact_identifier(&self, query: &str) -> Option<&CardMeta> {
        let q = query.trim();
        if q.is_empty() {
            return None;
        }
        if let Some(card) = self.by_uid.get(q) {
            return Some(card);
        }
        let lower = q.to_lowercase();
        if let Some(uid) = self.by_slug.get(&lower) {
            return self.by_uid.get(uid);
        }
        if let Some(found) = self.by_email.get(&lower) {
            if found.len() == 1 {
                return self.by_uid.get(&found[0]);
            }
            return None;
        }
        for form in crate::canon::phone_alias_forms(q) {
            if let Some(found) = self.by_phone.get(&form) {
                if found.len() == 1 {
                    return self.by_uid.get(&found[0]);
                }
                return None;
            }
        }
        if let Some(uid) = self
            .by_external_id
            .get(q)
            .or_else(|| self.by_external_id.get(&lower))
        {
            return self.by_uid.get(uid);
        }
        if let Some(uid) = self.by_path.get(q) {
            return self.by_uid.get(uid);
        }
        None
    }

    pub fn resolve_person_card(&self, needle: &str) -> Option<&CardMeta> {
        let needle = needle.trim();
        if needle.is_empty() {
            return None;
        }
        if let Some(card) = self.exact_identifier(needle) {
            if card.r#type == "person" && !Self::is_suppressed(card) {
                return Some(card);
            }
        }
        let mut persons: Vec<&CardMeta> = self
            .resolve_people_filter_uids(needle)
            .into_iter()
            .filter_map(|uid| self.by_uid.get(&uid))
            .filter(|card| card.r#type == "person" && !Self::is_suppressed(card))
            .collect();
        if persons.len() == 1 {
            return Some(persons.remove(0));
        }
        None
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

    pub fn eligible(
        &self,
        card: &CardMeta,
        policy: &AccessPolicy,
        type_filter: &str,
        source_filter: &str,
        people_filter: &str,
        org_filter: &str,
        start_date: &str,
        end_date: &str,
    ) -> bool {
        if !policy.permits(card) {
            return false;
        }
        self.matches_filters(
            card,
            type_filter,
            source_filter,
            people_filter,
            org_filter,
            start_date,
            end_date,
        )
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
        if Self::is_suppressed(card) {
            return false;
        }
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
        if !self.people_ok(card, &self.prepare_people_filter(people_filter)) {
            return false;
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
        policy: &AccessPolicy,
    ) -> Vec<&CardMeta> {
        let people = self.prepare_people_filter(people_filter);
        if people.status == "ambiguous" || people.status == "unresolved" {
            return Vec::new();
        }
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
                if self.eligible_prepared(card, policy, type_filter, source_filter, &people, "", "", "")
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
        policy: &AccessPolicy,
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
        let people = self.prepare_people_filter(people_filter);
        if people.status == "ambiguous" || people.status == "unresolved" {
            return Some(Vec::new());
        }
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
                &people,
                policy,
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
                &people,
                policy,
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
                &people,
                policy,
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
        people: &PreparedPeopleFilter,
        policy: &AccessPolicy,
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
                if self.eligible_prepared(card, policy, type_filter, source_filter, people, "", "", "") {
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
                if self.eligible_prepared(card, policy, type_filter, source_filter, people, "", "", "") {
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
        people: &PreparedPeopleFilter,
        policy: &AccessPolicy,
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
                if self.eligible_prepared(card, policy, type_filter, source_filter, people, "", "", "") {
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
        people: &PreparedPeopleFilter,
        policy: &AccessPolicy,
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
                if self.eligible_prepared(card, policy, type_filter, source_filter, people, "", "", "") {
                    out.push(NeighborHit {
                        card,
                        leg: "backward",
                    });
                    added += 1;
                }
            }
        }
    }

    pub fn typed_field_value(card: &CardMeta, field: &str) -> Option<TypedFieldValue> {
        match field {
            "uid" | "card_uid" => Some(TypedFieldValue::Text(card.card_uid.clone())),
            "type" | "card_type" => Some(TypedFieldValue::Text(card.r#type.clone())),
            "source" | "sources" => Some(TypedFieldValue::List(card.sources.clone())),
            "people" => Some(TypedFieldValue::List(card.people.clone())),
            "org" | "orgs" | "organization" => Some(TypedFieldValue::List(card.orgs.clone())),
            "activity_at" => Some(TypedFieldValue::Text(card.activity_at.clone())),
            "corpus_state" => Some(TypedFieldValue::Text(card.corpus_state.clone())),
            "summary" => Some(TypedFieldValue::Text(card.summary.clone())),
            "slug" => Some(TypedFieldValue::Text(card.slug.clone())),
            "emails" => Some(TypedFieldValue::List(card.emails.clone())),
            "domains" => Some(TypedFieldValue::List(card.domains.clone())),
            _ => None,
        }
    }

    fn people_predicate_matches(&self, card: &CardMeta, needle: &str) -> bool {
        self.people_ok(card, &self.prepare_people_filter(needle))
    }

    fn predicate_string(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.as_str().unwrap_or("").to_string(),
        }
    }

    pub fn matches_typed_predicate(&self, card: &CardMeta, pred: &TypedPredicate) -> Result<bool, String> {
        match pred.op.as_str() {
            "and" => {
                for child in &pred.predicates {
                    if !self.matches_typed_predicate(card, child)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            "or" => {
                if pred.predicates.is_empty() {
                    return Ok(false);
                }
                for child in &pred.predicates {
                    if self.matches_typed_predicate(card, child)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            "exists" => {
                let value = Self::typed_field_value(card, &pred.field)
                    .ok_or_else(|| format!("unknown field: {}", pred.field))?;
                Ok(!value.is_empty())
            }
            "in" => {
                if pred.field == "people" {
                    let items = pred.value.as_array().cloned().unwrap_or_default();
                    return Ok(items.iter().any(|item| {
                        self.people_predicate_matches(card, &Self::predicate_string(item))
                    }));
                }
                let value = Self::typed_field_value(card, &pred.field)
                    .ok_or_else(|| format!("unknown field: {}", pred.field))?;
                let items = pred.value.as_array().cloned().unwrap_or_default();
                Ok(items.iter().any(|item| value.matches_eq(item)))
            }
            "eq" | "lt" | "lte" | "gt" | "gte" => {
                if pred.field == "people" && pred.op == "eq" {
                    return Ok(self.people_predicate_matches(card, &Self::predicate_string(&pred.value)));
                }
                let value = Self::typed_field_value(card, &pred.field)
                    .ok_or_else(|| format!("unknown field: {}", pred.field))?;
                Ok(value.compare(pred.op.as_str(), &pred.value))
            }
            other => Err(format!("unsupported predicate operator: {other}")),
        }
    }

    pub fn typed_query_page<'a>(
        &'a self,
        policy: &AccessPolicy,
        pred: Option<&TypedPredicate>,
        order_field: &str,
        order_direction: &str,
        after_uid: &str,
        after_value: &str,
        after_null: bool,
        page_size: usize,
    ) -> Result<TypedQueryPage<'a>, String> {
        if !matches!(
            order_field,
            "uid" | "type" | "activity_at" | "summary" | "slug" | "corpus_state"
        ) {
            return Err(format!("order field {order_field} is not sortable"));
        }
        if let Some(node) = pred {
            validate_typed_predicate(node)?;
        }
        let desc = order_direction == "desc";
        let mut eligible: Vec<&CardMeta> = self
            .by_uid
            .values()
            .filter(|card| {
                if !policy.permits(card) || Self::is_suppressed(card) {
                    return false;
                }
                match pred {
                    None => true,
                    Some(node) => self.matches_typed_predicate(card, node).unwrap_or(false),
                }
            })
            .collect();
        eligible.sort_by(|a, b| typed_order_cmp(a, b, order_field, desc));
        let start = if after_uid.is_empty() {
            0
        } else {
            eligible
                .iter()
                .position(|card| typed_after(*card, order_field, desc, after_value, after_uid, after_null))
                .unwrap_or(eligible.len())
        };
        let remaining = eligible.len().saturating_sub(start);
        let take = page_size.min(remaining);
        let rows = eligible[start..start + take].to_vec();
        let next = if remaining > page_size {
            rows.last().map(|card| {
                let value = order_value(card, order_field);
                TypedAfter {
                    uid: card.card_uid.clone(),
                    order_value: value.clone(),
                    order_null: value.is_empty(),
                }
            })
        } else {
            None
        };
        Ok(TypedQueryPage {
            rows,
            matched_total: eligible.len(),
            next,
            truncated: false,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TypedPredicate {
    pub op: String,
    #[serde(default)]
    pub field: String,
    #[serde(default)]
    pub value: serde_json::Value,
    #[serde(default)]
    pub predicates: Vec<TypedPredicate>,
}

#[derive(Debug, Clone)]
pub enum TypedFieldValue {
    Text(String),
    List(Vec<String>),
}

impl TypedFieldValue {
    fn is_empty(&self) -> bool {
        match self {
            TypedFieldValue::Text(value) => value.trim().is_empty(),
            TypedFieldValue::List(values) => values.iter().all(|item| item.trim().is_empty()),
        }
    }

    fn matches_eq(&self, other: &serde_json::Value) -> bool {
        self.compare("eq", other)
    }

    fn compare(&self, op: &str, other: &serde_json::Value) -> bool {
        let right = match other {
            serde_json::Value::String(value) => value.clone(),
            serde_json::Value::Number(num) => num.to_string(),
            serde_json::Value::Bool(flag) => flag.to_string(),
            serde_json::Value::Null => String::new(),
            _ => return false,
        };
        match self {
            TypedFieldValue::List(values) => {
                if op != "eq" {
                    return false;
                }
                let needle = right.to_ascii_lowercase();
                values.iter().any(|item| {
                    let hay = item.to_ascii_lowercase();
                    hay == needle || hay.contains(&needle)
                })
            }
            TypedFieldValue::Text(left) => {
                if op == "eq" {
                    return left.eq_ignore_ascii_case(&right);
                }
                let left_key = if right.len() == 10 { left.get(..10).unwrap_or(left) } else { left.as_str() };
                match op {
                    "lt" => left_key < right.as_str(),
                    "lte" => left_key <= right.as_str(),
                    "gt" => left_key > right.as_str(),
                    "gte" => left_key >= right.as_str(),
                    _ => false,
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TypedAfter {
    pub uid: String,
    pub order_value: String,
    pub order_null: bool,
}

#[derive(Debug, Clone)]
pub struct TypedQueryPage<'a> {
    pub rows: Vec<&'a CardMeta>,
    pub matched_total: usize,
    pub next: Option<TypedAfter>,
    pub truncated: bool,
}

fn validate_typed_predicate(pred: &TypedPredicate) -> Result<(), String> {
    match pred.op.as_str() {
        "and" | "or" => {
            if pred.predicates.is_empty() {
                return Err(format!("{} requires child predicates", pred.op));
            }
            for child in &pred.predicates {
                validate_typed_predicate(child)?;
            }
            Ok(())
        }
        "eq" | "in" | "lt" | "lte" | "gt" | "gte" | "exists" => {
            if pred.field.is_empty() {
                return Err("predicate field is required".into());
            }
            if MetadataStore::typed_field_value(&CardMeta::default(), &pred.field).is_none() {
                return Err(format!("unknown field: {}", pred.field));
            }
            if matches!(pred.op.as_str(), "lt" | "lte" | "gt" | "gte")
                && matches!(pred.field.as_str(), "source" | "sources" | "people" | "org" | "orgs" | "emails" | "domains")
            {
                return Err(format!("operator {} is not valid for list field {}", pred.op, pred.field));
            }
            Ok(())
        }
        other => Err(format!("unsupported predicate operator: {other}")),
    }
}

fn order_value(card: &CardMeta, field: &str) -> String {
    match MetadataStore::typed_field_value(card, field) {
        Some(TypedFieldValue::Text(value)) => value,
        Some(TypedFieldValue::List(values)) => values.first().cloned().unwrap_or_default(),
        None => String::new(),
    }
}

fn typed_order_cmp(a: &CardMeta, b: &CardMeta, field: &str, desc: bool) -> std::cmp::Ordering {
    let av = order_value(a, field);
    let bv = order_value(b, field);
    let a_null = av.is_empty();
    let b_null = bv.is_empty();
    let null_ord = a_null.cmp(&b_null);
    if null_ord != std::cmp::Ordering::Equal {
        return null_ord;
    }
    let value_ord = if desc { bv.cmp(&av) } else { av.cmp(&bv) };
    if value_ord != std::cmp::Ordering::Equal {
        return value_ord;
    }
    a.card_uid.cmp(&b.card_uid)
}

fn typed_after(card: &CardMeta, field: &str, desc: bool, last_value: &str, last_uid: &str, last_null: bool) -> bool {
    let value = order_value(card, field);
    let null = value.is_empty();
    if desc {
        if last_null && !null {
            return false;
        }
        if null && !last_null {
            return true;
        }
        if null && last_null {
            return card.card_uid.as_str() > last_uid;
        }
        if value == last_value {
            return card.card_uid.as_str() > last_uid;
        }
        return value.as_str() < last_value;
    }
    if last_null && !null {
        return true;
    }
    if null && !last_null {
        return false;
    }
    if null && last_null {
        return card.card_uid.as_str() > last_uid;
    }
    if value == last_value {
        return card.card_uid.as_str() > last_uid;
    }
    value.as_str() > last_value
}

#[cfg(test)]
mod access_tests {
    use super::*;

    fn card(uid: &str, sources: &[&str], kind: &str) -> CardMeta {
        CardMeta {
            card_uid: uid.into(),
            r#type: kind.into(),
            sources: sources.iter().map(|s| (*s).to_string()).collect(),
            corpus_state: "active".into(),
            ..CardMeta::default()
        }
    }

    #[test]
    fn unrestricted_permits_unknown_lineage() {
        let policy = AccessPolicy::unrestricted();
        assert!(policy.permits(&card("u1", &[], "person")));
    }

    #[test]
    fn adjacent_chunks_follow_sorted_index() {
        let mut adj = ChunkAdjacency::default();
        adj.insert("card".into(), "body".into(), 2, "ck-2".into());
        adj.insert("card".into(), "body".into(), 0, "ck-0".into());
        adj.insert("card".into(), "body".into(), 1, "ck-1".into());
        adj.finalize();
        assert_eq!(adj.neighbors("card", "body", 1), (Some("ck-0".into()), Some("ck-2".into())));
        assert_eq!(adj.neighbors("card", "body", 0), (None, Some("ck-1".into())));
    }

    #[test]
    fn restricted_denies_unknown_and_mixed_source() {
        let policy = AccessPolicy {
            restricted: true,
            allowed_sources: vec!["gmail".into()],
            ..AccessPolicy::default()
        };
        assert!(!policy.permits(&card("u1", &[], "person")));
        assert!(policy.permits(&card("u2", &["gmail"], "email_message")));
        assert!(!policy.permits(&card("u3", &["gmail", "medical"], "purchase")));
    }

    #[test]
    fn missing_lineage_is_deny_when_restricted() {
        let policy = AccessPolicy {
            restricted: true,
            allowed_sources: vec!["gmail".into()],
            ..AccessPolicy::default()
        };
        let mut derived = card("u4", &["gmail"], "purchase");
        derived.lineage_complete = Some(false);
        assert!(!policy.permits(&derived));
    }

    fn person(uid: &str, summary: &str) -> CardMeta {
        CardMeta {
            card_uid: uid.into(),
            r#type: "person".into(),
            summary: summary.into(),
            slug: summary.to_lowercase().replace(' ', "-"),
            sources: vec!["test".into()],
            corpus_state: "active".into(),
            ..CardMeta::default()
        }
    }

    #[test]
    fn people_filter_resolves_name_to_person_uid() {
        let sam = CardMeta {
            phones: vec!["+19147153533".into()],
            emails: vec!["sampanken@gmail.com".into()],
            ..person("hfa-person-54fc3b19aeda", "Sam Panken")
        };
        let thread = CardMeta {
            card_uid: "hfa-imessage-thread-45b3a963c99a".into(),
            r#type: "imessage_thread".into(),
            people: vec!["hfa-person-54fc3b19aeda".into()],
            sources: vec!["imessage.thread".into()],
            corpus_state: "active".into(),
            ..CardMeta::default()
        };
        let store = MetadataStore::from_cards([sam, thread.clone()]);
        assert!(store.matches_filters(&thread, "imessage_thread", "", "Sam Panken", "", "", ""));
        assert!(store.matches_filters(&thread, "imessage_thread", "", "sam-panken", "", "", ""));
        assert!(store.matches_filters(&thread, "imessage_thread", "", "9147153533", "", "", ""));
        let pred = TypedPredicate {
            op: "eq".into(),
            field: "people".into(),
            value: serde_json::json!("Sam Panken"),
            ..TypedPredicate::default()
        };
        assert!(store.matches_typed_predicate(&thread, &pred).unwrap());
        assert_eq!(
            store.exact_identifier("9147153533").map(|card| card.card_uid.as_str()),
            Some("hfa-person-54fc3b19aeda")
        );
        assert_eq!(
            store.resolve_person_card("9147153533").map(|card| card.card_uid.as_str()),
            Some("hfa-person-54fc3b19aeda")
        );
        assert_eq!(
            store
                .resolve_person_card("+19147153533")
                .map(|card| card.card_uid.as_str()),
            Some("hfa-person-54fc3b19aeda")
        );
        assert_eq!(
            store
                .resolve_person_card("sampanken@gmail.com")
                .map(|card| card.card_uid.as_str()),
            Some("hfa-person-54fc3b19aeda")
        );
        assert_eq!(
            store.resolve_person_card("Sam Panken").map(|card| card.card_uid.as_str()),
            Some("hfa-person-54fc3b19aeda")
        );
        let alice_card = CardMeta {
            emails: vec!["shared@example.com".into()],
            ..person("hfa-person-alice", "Alice Smith")
        };
        let alex_card = CardMeta {
            emails: vec!["shared@example.com".into()],
            ..person("hfa-person-alex", "Alex Rivera")
        };
        let collided = MetadataStore::from_cards([alice_card, alex_card]);
        assert!(collided.resolve_person_card("shared@example.com").is_none());
        let prepared = collided.prepare_people_filter("shared@example.com");
        assert_eq!(prepared.status, "ambiguous");
        assert_eq!(prepared.uids.len(), 2);
    }

    #[test]
    fn typed_query_pages_all_eligible_exactly_once() {
        let store = MetadataStore::from_cards([
            person("hfa-person-a", "Alex Rivera"),
            person("hfa-person-b", "Alex Rivera"),
            person("hfa-person-c", "Jordan Hale"),
            CardMeta {
                card_uid: "hfa-person-sup".into(),
                r#type: "person".into(),
                summary: "Alex Rivera".into(),
                sources: vec!["test".into()],
                corpus_state: "suppressed".into(),
                ..CardMeta::default()
            },
        ]);
        let pred = TypedPredicate {
            op: "and".into(),
            predicates: vec![
                TypedPredicate {
                    op: "eq".into(),
                    field: "type".into(),
                    value: serde_json::json!("person"),
                    ..TypedPredicate::default()
                },
                TypedPredicate {
                    op: "eq".into(),
                    field: "summary".into(),
                    value: serde_json::json!("Alex Rivera"),
                    ..TypedPredicate::default()
                },
            ],
            ..TypedPredicate::default()
        };
        let first = store
            .typed_query_page(&AccessPolicy::unrestricted(), Some(&pred), "uid", "asc", "", "", false, 1)
            .unwrap();
        assert_eq!(first.matched_total, 2);
        assert_eq!(first.rows.len(), 1);
        assert_eq!(first.rows[0].card_uid, "hfa-person-a");
        let after = first.next.expect("next page");
        let second = store
            .typed_query_page(
                &AccessPolicy::unrestricted(),
                Some(&pred),
                "uid",
                "asc",
                &after.uid,
                &after.order_value,
                after.order_null,
                1,
            )
            .unwrap();
        assert_eq!(second.rows[0].card_uid, "hfa-person-b");
        assert!(second.next.is_none());
        let seen: Vec<_> = first
            .rows
            .iter()
            .chain(second.rows.iter())
            .map(|card| card.card_uid.as_str())
            .collect();
        assert_eq!(seen, vec!["hfa-person-a", "hfa-person-b"]);
    }

    #[test]
    fn typed_query_rejects_unknown_field() {
        let store = MetadataStore::from_cards([person("hfa-person-a", "Alex")]);
        let pred = TypedPredicate {
            op: "eq".into(),
            field: "not_a_field".into(),
            value: serde_json::json!("x"),
            ..TypedPredicate::default()
        };
        let err = store
            .typed_query_page(&AccessPolicy::unrestricted(), Some(&pred), "uid", "asc", "", "", false, 10)
            .unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
    }

    #[test]
    fn restricted_policy_applied_before_typed_count() {
        let store = MetadataStore::from_cards([
            person("hfa-person-a", "Alex Rivera"),
            CardMeta {
                card_uid: "hfa-person-denied".into(),
                r#type: "person".into(),
                summary: "Alex Rivera".into(),
                sources: vec!["other".into()],
                corpus_state: "active".into(),
                ..CardMeta::default()
            },
        ]);
        let policy = AccessPolicy {
            restricted: true,
            allowed_sources: vec!["test".into()],
            ..AccessPolicy::default()
        };
        let pred = TypedPredicate {
            op: "eq".into(),
            field: "summary".into(),
            value: serde_json::json!("Alex Rivera"),
            ..TypedPredicate::default()
        };
        let page = store
            .typed_query_page(&policy, Some(&pred), "uid", "asc", "", "", false, 10)
            .unwrap();
        assert_eq!(page.matched_total, 1);
        assert_eq!(page.rows[0].card_uid, "hfa-person-a");
    }
}

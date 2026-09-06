use std::fs::File;
use std::io::Write;
use std::path::Path;

use pyo3::prelude::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::schema::{DEFAULT_NPROBE, MAX_NLIST, UNASSIGNED_LIST, VECTOR_IMPL};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TrainConfig {
    #[serde(default)]
    pub nlist: Option<usize>,
    #[serde(default = "default_nprobe")]
    pub nprobe: usize,
    #[serde(default = "default_sample")]
    pub train_sample: usize,
    #[serde(default = "default_iters")]
    pub train_iters: usize,
    #[serde(default = "default_seed")]
    pub seed: u64,
    #[serde(default = "default_budget")]
    pub candidate_budget: usize,
    #[serde(default = "default_memory")]
    pub memory_mb: usize,
    #[serde(default)]
    pub embedding_spec: Option<serde_json::Value>,
}

fn default_nprobe() -> usize {
    DEFAULT_NPROBE
}
fn default_sample() -> usize {
    100_000
}
fn default_iters() -> usize {
    25
}
fn default_seed() -> u64 {
    20260906
}
fn default_budget() -> usize {
    4096
}
fn default_memory() -> usize {
    8192
}

impl Default for TrainConfig {
    fn default() -> Self {
        Self {
            nlist: None,
            nprobe: default_nprobe(),
            train_sample: default_sample(),
            train_iters: default_iters(),
            seed: default_seed(),
            candidate_budget: default_budget(),
            memory_mb: default_memory(),
            embedding_spec: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IvfMeta {
    pub vector_impl: String,
    pub n: usize,
    pub dim: usize,
    pub nlist: usize,
    pub nprobe: usize,
    pub seed: u64,
    pub train_sample: usize,
    pub train_iters: usize,
    pub candidate_budget: usize,
    pub valid_count: usize,
    pub skipped_invalid: usize,
    pub skipped_zero: usize,
    pub checksum: String,
    #[serde(default)]
    pub embedding_spec: Option<serde_json::Value>,
}

pub fn default_nlist(n: usize) -> usize {
    if n < 1 {
        return 1;
    }
    ((n as f64).sqrt() as usize).clamp(1, MAX_NLIST)
}

pub fn default_nprobe_for(nlist: usize) -> usize {
    nlist.min(DEFAULT_NPROBE).max(1)
}

pub fn is_finite_vec(vec: &[f32]) -> bool {
    vec.iter().all(|v| v.is_finite())
}

pub fn is_zero_vec(vec: &[f32]) -> bool {
    vec.iter().all(|v| *v == 0.0)
}

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

pub fn closest_centroid(vec: &[f32], centroids: &[f32], dim: usize) -> usize {
    let nlist = if dim == 0 { 0 } else { centroids.len() / dim };
    let mut best_i = 0usize;
    let mut best = f32::NEG_INFINITY;
    for i in 0..nlist {
        let c = &centroids[i * dim..(i + 1) * dim];
        let score = cosine(vec, c);
        if score > best || (score == best && i < best_i) {
            best = score;
            best_i = i;
        }
    }
    best_i
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
}

fn shuffle(items: &mut [usize], seed: u64) {
    let mut rng = SplitMix64(seed ^ 0xA5A5A5A5A5A5A5A5);
    for i in (1..items.len()).rev() {
        let j = (rng.next() as usize) % (i + 1);
        items.swap(i, j);
    }
}

fn l2_normalize(slot: &mut [f32]) {
    let norm = slot.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in slot.iter_mut() {
            *v /= norm;
        }
    }
}

pub fn train_ivf(vectors: &[f32], n: usize, dim: usize, cfg: &TrainConfig) -> PyResult<(Vec<f32>, Vec<u32>, IvfMeta)> {
    if dim == 0 || n == 0 {
        return Ok((
            Vec::new(),
            Vec::new(),
            IvfMeta {
                vector_impl: VECTOR_IMPL.to_string(),
                n,
                dim,
                nlist: 0,
                nprobe: 1,
                seed: cfg.seed,
                train_sample: cfg.train_sample,
                train_iters: cfg.train_iters,
                candidate_budget: cfg.candidate_budget,
                valid_count: 0,
                skipped_invalid: 0,
                skipped_zero: 0,
                checksum: sha256_hex(b""),
                embedding_spec: cfg.embedding_spec.clone(),
            },
        ));
    }
    if vectors.len() != n * dim {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "embedding length {} != n*dim {}",
            vectors.len(),
            n * dim
        )));
    }
    let bytes_needed = (n + cfg.nlist.unwrap_or(default_nlist(n))) * dim * 4;
    if bytes_needed / (1024 * 1024) > cfg.memory_mb {
        return Err(pyo3::exceptions::PyMemoryError::new_err(format!(
            "serving_index_train_exceeds_memory need_mb={} cap_mb={}",
            bytes_needed / (1024 * 1024),
            cfg.memory_mb
        )));
    }
    let mut valid = Vec::new();
    let mut skipped_invalid = 0usize;
    let mut skipped_zero = 0usize;
    for i in 0..n {
        let vec = &vectors[i * dim..(i + 1) * dim];
        if !is_finite_vec(vec) {
            skipped_invalid += 1;
            continue;
        }
        if is_zero_vec(vec) {
            skipped_zero += 1;
            continue;
        }
        valid.push(i);
    }
    if valid.is_empty() {
        let assign = vec![UNASSIGNED_LIST; n];
        let meta = finish_meta(n, dim, 0, 1, cfg, 0, skipped_invalid, skipped_zero, &[], &assign);
        return Ok((Vec::new(), assign, meta));
    }
    let nlist = cfg
        .nlist
        .unwrap_or_else(|| default_nlist(n))
        .clamp(1, MAX_NLIST.min(valid.len()));
    let nprobe = cfg.nprobe.max(1).min(nlist);
    let mut sample = valid.clone();
    shuffle(&mut sample, cfg.seed);
    sample.truncate(cfg.train_sample.min(sample.len()));
    let mut centroids = vec![0.0f32; nlist * dim];
    for c in 0..nlist {
        let src = sample[c % sample.len()];
        centroids[c * dim..(c + 1) * dim].copy_from_slice(&vectors[src * dim..(src + 1) * dim]);
        l2_normalize(&mut centroids[c * dim..(c + 1) * dim]);
    }
    for iter in 0..cfg.train_iters {
        let mut sums = vec![0.0f32; nlist * dim];
        let mut counts = vec![0u32; nlist];
        let assigned: Vec<usize> = sample
            .par_iter()
            .map(|&idx| closest_centroid(&vectors[idx * dim..(idx + 1) * dim], &centroids, dim))
            .collect();
        for (sample_i, &c) in assigned.iter().enumerate() {
            let idx = sample[sample_i];
            let src = &vectors[idx * dim..(idx + 1) * dim];
            for d in 0..dim {
                sums[c * dim + d] += src[d];
            }
            counts[c] += 1;
        }
        for c in 0..nlist {
            if counts[c] == 0 {
                let pick = sample[(cfg.seed as usize).wrapping_add(iter).wrapping_add(c) % sample.len()];
                centroids[c * dim..(c + 1) * dim].copy_from_slice(&vectors[pick * dim..(pick + 1) * dim]);
                l2_normalize(&mut centroids[c * dim..(c + 1) * dim]);
                continue;
            }
            for d in 0..dim {
                centroids[c * dim + d] = sums[c * dim + d] / counts[c] as f32;
            }
            l2_normalize(&mut centroids[c * dim..(c + 1) * dim]);
        }
    }
    let mut assign = vec![UNASSIGNED_LIST; n];
    let closest: Vec<(usize, usize)> = valid
        .par_iter()
        .map(|&idx| (idx, closest_centroid(&vectors[idx * dim..(idx + 1) * dim], &centroids, dim)))
        .collect();
    for (idx, list) in closest {
        assign[idx] = list as u32;
    }
    fill_empty_lists(&mut assign, nlist, &valid);
    let meta = finish_meta(
        n,
        dim,
        nlist,
        nprobe,
        cfg,
        valid.len(),
        skipped_invalid,
        skipped_zero,
        &centroids,
        &assign,
    );
    Ok((centroids, assign, meta))
}

fn fill_empty_lists(assign: &mut [u32], nlist: usize, valid: &[usize]) {
    if nlist == 0 || valid.len() < nlist {
        return;
    }
    let mut lists: Vec<Vec<usize>> = vec![Vec::new(); nlist];
    for &idx in valid {
        let list = assign[idx] as usize;
        if list < nlist {
            lists[list].push(idx);
        }
    }
    for c in 0..nlist {
        if !lists[c].is_empty() {
            continue;
        }
        let Some((src, _)) = lists
            .iter()
            .enumerate()
            .max_by_key(|(_, members)| members.len())
        else {
            continue;
        };
        if lists[src].len() <= 1 {
            continue;
        }
        let moved = lists[src].pop().unwrap();
        assign[moved] = c as u32;
        lists[c].push(moved);
    }
}

fn finish_meta(
    n: usize,
    dim: usize,
    nlist: usize,
    nprobe: usize,
    cfg: &TrainConfig,
    valid_count: usize,
    skipped_invalid: usize,
    skipped_zero: usize,
    centroids: &[f32],
    assign: &[u32],
) -> IvfMeta {
    let mut hasher = Sha256::new();
    for v in centroids {
        hasher.update(v.to_le_bytes());
    }
    for a in assign {
        hasher.update(a.to_le_bytes());
    }
    IvfMeta {
        vector_impl: VECTOR_IMPL.to_string(),
        n,
        dim,
        nlist,
        nprobe,
        seed: cfg.seed,
        train_sample: cfg.train_sample,
        train_iters: cfg.train_iters,
        candidate_budget: cfg.candidate_budget,
        valid_count,
        skipped_invalid,
        skipped_zero,
        checksum: format!("{:x}", hasher.finalize()),
        embedding_spec: cfg.embedding_spec.clone(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub fn write_ivf_artifacts(dir: &Path, centroids: &[f32], assign: &[u32], meta: &IvfMeta) -> PyResult<()> {
    let mut cbytes = Vec::with_capacity(centroids.len() * 4);
    for v in centroids {
        cbytes.extend_from_slice(&v.to_le_bytes());
    }
    std::fs::write(dir.join("ivf_centroids.bin"), cbytes)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let mut abytes = Vec::with_capacity(assign.len() * 4);
    for a in assign {
        abytes.extend_from_slice(&a.to_le_bytes());
    }
    let mut af = File::create(dir.join("ivf_assign.bin"))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    af.write_all(&abytes)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    std::fs::write(
        dir.join("ivf_meta.json"),
        serde_json::to_string_pretty(meta).unwrap(),
    )
    .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    if let Some(spec) = &meta.embedding_spec {
        std::fs::write(
            dir.join("embedding_spec.json"),
            serde_json::to_string_pretty(spec).unwrap(),
        )
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    }
    Ok(())
}

pub fn train_from_embeddings_bin(dir: &Path, dim: usize, cfg: &TrainConfig) -> PyResult<IvfMeta> {
    let keys_path = dir.join("embedding_keys.txt");
    let bin_path = dir.join("embeddings.bin");
    if !keys_path.exists() || !bin_path.exists() {
        let meta = IvfMeta {
            vector_impl: VECTOR_IMPL.to_string(),
            n: 0,
            dim,
            nlist: 0,
            nprobe: 1,
            seed: cfg.seed,
            train_sample: cfg.train_sample,
            train_iters: cfg.train_iters,
            candidate_budget: cfg.candidate_budget,
            valid_count: 0,
            skipped_invalid: 0,
            skipped_zero: 0,
            checksum: sha256_hex(b""),
            embedding_spec: cfg.embedding_spec.clone(),
        };
        write_ivf_artifacts(dir, &[], &[], &meta)?;
        return Ok(meta);
    }
    let keys = std::fs::read_to_string(&keys_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let n = keys.lines().filter(|l| !l.trim().is_empty()).count();
    let raw = std::fs::read(&bin_path).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    if n == 0 {
        let meta = train_ivf(&[], 0, dim, cfg)?.2;
        write_ivf_artifacts(dir, &[], &[], &meta)?;
        return Ok(meta);
    }
    if raw.len() != n * dim * 4 {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "embeddings.bin length {} != n*dim*4 {}",
            raw.len(),
            n * dim * 4
        )));
    }
    let mut vectors = vec![0.0f32; n * dim];
    for (i, chunk) in raw.chunks_exact(4).enumerate() {
        vectors[i] = f32::from_le_bytes(chunk.try_into().unwrap());
    }
    let (centroids, assign, meta) = train_ivf(&vectors, n, dim, cfg)?;
    write_ivf_artifacts(dir, &centroids, &assign, &meta)?;
    Ok(meta)
}

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use memmap2::Mmap;
use pyo3::prelude::*;
use rayon::prelude::*;

use super::schema::{SERVING_INDEX_FORMAT_VERSION, UNASSIGNED_LIST, VECTOR_IMPL};
use super::vector_train::{
    cosine, default_nlist, is_finite_vec, is_zero_vec, train_from_embeddings_bin, TrainConfig, IvfMeta,
};

pub struct KnnHit {
    pub key: String,
    pub score: f32,
}

pub struct KnnReport {
    pub hits: Vec<KnnHit>,
    pub nlist: usize,
    pub nprobe: usize,
    pub lists_probed: usize,
    pub candidates_scored: usize,
    pub scanned_all: bool,
    pub skipped_invalid: usize,
    pub skipped_zero: usize,
    pub truncated: bool,
}

pub struct IvfMmapAnn {
    dim: usize,
    keys: Vec<String>,
    mmap: Option<Mmap>,
    nlist: usize,
    nprobe: usize,
    candidate_budget: usize,
    lists: Vec<Vec<usize>>,
    centroids: Vec<f32>,
    valid_count: usize,
    embedding_spec: Option<serde_json::Value>,
    meta: IvfMeta,
}

impl IvfMmapAnn {
    pub fn open(dir: &Path) -> PyResult<Self> {
        let keys_path = dir.join("embedding_keys.txt");
        let vec_path = dir.join("embeddings.bin");
        let keys = if keys_path.exists() {
            BufReader::new(
                File::open(&keys_path)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("embedding_keys: {e}")))?,
            )
            .lines()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?
            .into_iter()
            .filter(|l| !l.trim().is_empty())
            .collect()
        } else {
            Vec::new()
        };
        if keys.is_empty() {
            let meta = IvfMeta {
                vector_impl: VECTOR_IMPL.to_string(),
                n: 0,
                dim: 0,
                nlist: 0,
                nprobe: 1,
                seed: 0,
                train_sample: 0,
                train_iters: 0,
                candidate_budget: 4096,
                valid_count: 0,
                skipped_invalid: 0,
                skipped_zero: 0,
                checksum: String::new(),
                embedding_spec: None,
            };
            return Ok(Self {
                dim: 0,
                keys,
                mmap: None,
                nlist: 0,
                nprobe: 1,
                candidate_budget: 4096,
                lists: Vec::new(),
                centroids: Vec::new(),
                valid_count: 0,
                embedding_spec: None,
                meta,
            });
        }
        if !dir.join("ivf_centroids.bin").exists() || !dir.join("ivf_meta.json").exists() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "serving_index_format_unsupported: missing ivf_centroids_v2 artifacts (need format {SERVING_INDEX_FORMAT_VERSION}). Rebuild the serving index."
            )));
        }
        let meta: IvfMeta = serde_json::from_str(
            &std::fs::read_to_string(dir.join("ivf_meta.json"))
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?,
        )
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("ivf_meta.json: {e}")))?;
        if meta.vector_impl != VECTOR_IMPL {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "serving_index_format_unsupported: vector_impl={} expected={VECTOR_IMPL}. Rebuild the serving index.",
                meta.vector_impl
            )));
        }
        let file = File::open(&vec_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("embeddings.bin: {e}")))?;
        let mmap = unsafe { Mmap::map(&file) }
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("mmap embeddings: {e}")))?;
        let n = keys.len();
        let dim = meta.dim;
        if dim == 0 || mmap.len() != n * dim * 4 {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "serving_index_corrupt: embeddings.bin len {} != n*dim*4 {}",
                mmap.len(),
                n * dim * 4
            )));
        }
        let raw_c = std::fs::read(dir.join("ivf_centroids.bin"))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        if raw_c.len() != meta.nlist * dim * 4 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "serving_index_corrupt: ivf_centroids.bin size mismatch",
            ));
        }
        let centroids: Vec<f32> = raw_c
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        let raw_a = std::fs::read(dir.join("ivf_assign.bin"))
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        if raw_a.len() != n * 4 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "serving_index_corrupt: ivf_assign.bin size mismatch",
            ));
        }
        let mut lists = vec![Vec::new(); meta.nlist.max(1)];
        for (i, chunk) in raw_a.chunks_exact(4).enumerate() {
            let list = u32::from_le_bytes(chunk.try_into().unwrap());
            if list == UNASSIGNED_LIST {
                continue;
            }
            if (list as usize) < lists.len() && i < n {
                lists[list as usize].push(i);
            }
        }
        Ok(Self {
            dim,
            keys,
            mmap: Some(mmap),
            nlist: meta.nlist,
            nprobe: meta.nprobe.max(1).min(meta.nlist.max(1)),
            candidate_budget: meta.candidate_budget.max(1),
            lists,
            centroids,
            valid_count: meta.valid_count,
            embedding_spec: meta.embedding_spec.clone(),
            meta,
        })
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn nlist(&self) -> usize {
        self.nlist
    }

    pub fn nprobe(&self) -> usize {
        self.nprobe
    }

    pub fn candidate_budget(&self) -> usize {
        self.candidate_budget
    }

    pub fn embedding_spec(&self) -> Option<&serde_json::Value> {
        self.embedding_spec.as_ref()
    }

    pub fn meta(&self) -> &IvfMeta {
        &self.meta
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    fn read_vec(&self, idx: usize) -> Vec<f32> {
        match &self.mmap {
            Some(mmap) => read_f32_vec(mmap, self.dim, idx),
            None => vec![0.0; self.dim],
        }
    }

    pub fn knn(&self, query: &[f32], k: usize) -> Vec<(String, f32)> {
        self.knn_report(query, k, None, self.nprobe, self.candidate_budget)
            .hits
            .into_iter()
            .map(|h| (h.key, h.score))
            .collect()
    }

    pub fn knn_report(
        &self,
        query: &[f32],
        k: usize,
        eligible: Option<&HashSet<usize>>,
        nprobe: usize,
        budget: usize,
    ) -> KnnReport {
        let empty = KnnReport {
            hits: Vec::new(),
            nlist: self.nlist,
            nprobe: nprobe.min(self.nlist.max(1)),
            lists_probed: 0,
            candidates_scored: 0,
            scanned_all: false,
            skipped_invalid: 0,
            skipped_zero: 0,
            truncated: false,
        };
        if self.keys.is_empty() || self.dim == 0 || k == 0 {
            return empty;
        }
        if query.len() != self.dim || !is_finite_vec(query) {
            return empty;
        }
        if is_zero_vec(query) {
            return empty;
        }
        if let Some(set) = eligible {
            if !set.is_empty() && set.len() <= budget {
                return self.exact_over(query, k, eligible, budget);
            }
        }
        self.ivf_probe(query, k, eligible, nprobe, budget)
    }

    fn exact_over(
        &self,
        query: &[f32],
        k: usize,
        eligible: Option<&HashSet<usize>>,
        budget: usize,
    ) -> KnnReport {
        let mut skipped_invalid = 0usize;
        let mut skipped_zero = 0usize;
        let mut candidates: Vec<usize> = Vec::new();
        if let Some(set) = eligible {
            candidates.extend(set.iter().copied().filter(|i| *i < self.keys.len()));
        } else {
            candidates.extend(0..self.keys.len());
        }
        candidates.truncate(budget);
        let truncated = eligible.map(|s| s.len() > budget).unwrap_or(self.keys.len() > budget);
        let scored = self.score_candidates(query, &candidates, &mut skipped_invalid, &mut skipped_zero);
        let scanned_all = !truncated && eligible.map(|s| s.len()).unwrap_or(self.valid_count) == scored.len() + skipped_zero + skipped_invalid;
        KnnReport {
            hits: top_k(scored, &self.keys, k),
            nlist: self.nlist,
            nprobe: 0,
            lists_probed: 0,
            candidates_scored: candidates.len(),
            scanned_all,
            skipped_invalid,
            skipped_zero,
            truncated,
        }
    }

    fn ivf_probe(
        &self,
        query: &[f32],
        k: usize,
        eligible: Option<&HashSet<usize>>,
        nprobe: usize,
        budget: usize,
    ) -> KnnReport {
        if self.nlist == 0 || self.centroids.is_empty() {
            return self.exact_over(query, k, eligible, budget);
        }
        let probe_n = nprobe.max(1).min(self.nlist);
        let mut list_scores: Vec<(f32, usize)> = (0..self.nlist)
            .map(|li| {
                if self.lists.get(li).map(|l| l.is_empty()).unwrap_or(true) {
                    return (f32::NEG_INFINITY, li);
                }
                let c = &self.centroids[li * self.dim..(li + 1) * self.dim];
                (cosine(query, c), li)
            })
            .collect();
        list_scores.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
        });
        let probe: Vec<usize> = list_scores.into_iter().take(probe_n).map(|(_, i)| i).collect();
        let mut candidates = Vec::new();
        for li in &probe {
            for &idx in &self.lists[*li] {
                if let Some(set) = eligible {
                    if !set.contains(&idx) {
                        continue;
                    }
                }
                candidates.push(idx);
                if candidates.len() >= budget {
                    break;
                }
            }
            if candidates.len() >= budget {
                break;
            }
        }
        let truncated = candidates.len() >= budget;
        let mut skipped_invalid = 0usize;
        let mut skipped_zero = 0usize;
        let scored = self.score_candidates(query, &candidates, &mut skipped_invalid, &mut skipped_zero);
        let scanned_all = probe_n >= self.nlist && !truncated;
        KnnReport {
            hits: top_k(scored, &self.keys, k),
            nlist: self.nlist,
            nprobe: probe_n,
            lists_probed: probe.len(),
            candidates_scored: candidates.len(),
            scanned_all,
            skipped_invalid,
            skipped_zero,
            truncated,
        }
    }

    fn score_candidates(
        &self,
        query: &[f32],
        candidates: &[usize],
        skipped_invalid: &mut usize,
        skipped_zero: &mut usize,
    ) -> Vec<(f32, usize)> {
        let rows: Vec<Option<(f32, usize)>> = candidates
            .par_iter()
            .map(|&idx| {
                let vec = match &self.mmap {
                    Some(mmap) => read_f32_vec(mmap, self.dim, idx),
                    None => return None,
                };
                if !is_finite_vec(&vec) {
                    return Some((f32::NAN, idx));
                }
                if is_zero_vec(&vec) {
                    return Some((f32::NEG_INFINITY, idx));
                }
                Some((cosine(query, &vec), idx))
            })
            .collect();
        let mut scored = Vec::new();
        for row in rows {
            match row {
                Some((score, _idx)) if score.is_nan() => *skipped_invalid += 1,
                Some((score, _idx)) if score == f32::NEG_INFINITY => *skipped_zero += 1,
                Some((score, idx)) => scored.push((score, idx)),
                None => {}
            }
        }
        scored
    }
}

fn top_k(mut scored: Vec<(f32, usize)>, keys: &[String], k: usize) -> Vec<KnnHit> {
    scored.sort_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| keys[a.1].cmp(&keys[b.1]))
    });
    scored
        .into_iter()
        .take(k)
        .map(|(score, idx)| KnnHit {
            key: keys[idx].clone(),
            score,
        })
        .collect()
}

fn read_f32_vec(mmap: &Mmap, dim: usize, idx: usize) -> Vec<f32> {
    let start = idx * dim * 4;
    let mut out = vec![0.0f32; dim];
    for (i, slot) in out.iter_mut().enumerate() {
        let off = start + i * 4;
        if off + 4 <= mmap.len() {
            *slot = f32::from_le_bytes(mmap[off..off + 4].try_into().unwrap());
        }
    }
    out
}

pub fn write_trained_ivf(dir: &Path, dim: usize, cfg: &TrainConfig) -> PyResult<IvfMeta> {
    train_from_embeddings_bin(dir, dim, cfg)
}

pub fn write_embedding_files(dir: &Path, keys: &[String], vectors: &[f32], dim: usize) -> PyResult<()> {
    std::fs::write(dir.join("embedding_keys.txt"), keys.join("\n"))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let mut bytes = Vec::with_capacity(vectors.len() * 4);
    for v in vectors {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    std::fs::write(dir.join("embeddings.bin"), bytes)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    let _ = dim;
    write_trained_ivf(dir, dim, &TrainConfig::default())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serving_index::vector_train::{train_ivf, TrainConfig};

    fn adversary() -> (Vec<f32>, Vec<String>, Vec<f32>) {
        let n = 1089usize;
        let dim = 8usize;
        let nlist = default_nlist(n);
        assert_eq!(nlist, 33);
        let true_index = 32 + 33;
        let mut vectors = vec![0.0f32; n * dim];
        for i in 0..n {
            let slot = &mut vectors[i * dim..(i + 1) * dim];
            if i == true_index {
                slot[0] = 1.0;
            } else if i == 32 {
                slot[0] = -1.0;
            } else if i < 32 {
                slot[0] = 0.5;
                slot[1] = 0.5;
            } else {
                slot[2] = 1.0;
                slot[3] = ((i * 17 + 20260906) % 11) as f32 * 0.01;
            }
        }
        let keys: Vec<String> = (0..n).map(|i| format!("ann-p04b-{i:04}")).collect();
        let query = {
            let mut q = vec![0.0f32; dim];
            q[0] = 1.0;
            q
        };
        (vectors, keys, query)
    }

    #[test]
    fn trained_ivf_beats_modulo_adversary() {
        let (vectors, keys, query) = adversary();
        let n = keys.len();
        let dim = 8;
        let cfg = TrainConfig {
            nlist: Some(33),
            nprobe: 32,
            ..TrainConfig::default()
        };
        let (centroids, assign, meta) = train_ivf(&vectors, n, dim, &cfg).unwrap();
        assert_eq!(meta.nlist, 33);
        assert_eq!(meta.nprobe, 32);
        let true_index = 65usize;
        let true_list = assign[true_index] as usize;
        let mut list_scores: Vec<(f32, usize)> = (0..33)
            .map(|li| (cosine(&query, &centroids[li * dim..(li + 1) * dim]), li))
            .collect();
        list_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
        let probed: Vec<usize> = list_scores.into_iter().take(32).map(|(_, i)| i).collect();
        assert!(
            probed.contains(&true_list),
            "true neighbor list {true_list} not among 32 probed centroids"
        );
        assert_eq!(assign.iter().filter(|a| **a != UNASSIGNED_LIST).count(), n);
        let mut occupied = vec![0usize; 33];
        for a in &assign {
            occupied[*a as usize] += 1;
        }
        assert!(occupied.iter().all(|c| *c > 0), "empty IVF list would force a full scan");
    }

    #[test]
    fn invalid_vectors_are_not_assigned() {
        let dim = 2;
        let vectors = [1.0f32, 0.0, f32::NAN, 1.0, 0.0, 0.0];
        let cfg = TrainConfig {
            nlist: Some(1),
            nprobe: 1,
            ..TrainConfig::default()
        };
        let (_c, assign, meta) = train_ivf(&vectors, 3, dim, &cfg).unwrap();
        assert_eq!(assign[1], UNASSIGNED_LIST);
        assert_eq!(assign[2], UNASSIGNED_LIST);
        assert_eq!(meta.skipped_invalid, 1);
        assert_eq!(meta.skipped_zero, 1);
        assert_eq!(meta.valid_count, 1);
    }
}

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use memmap2::Mmap;
use pyo3::prelude::*;
use rayon::prelude::*;

pub trait VectorAnn {
    fn knn(&self, query: &[f32], k: usize) -> Vec<(String, f32)>;
}

pub struct IvfMmapAnn {
    dim: usize,
    keys: Vec<String>,
    mmap: Mmap,
    nlist: usize,
    lists: Vec<Vec<usize>>,
}

impl IvfMmapAnn {
    pub fn open(dir: &Path) -> PyResult<Self> {
        let keys_path = dir.join("embedding_keys.txt");
        let vec_path = dir.join("embeddings.bin");
        let assign_path = dir.join("ivf_assign.bin");
        let keys = if keys_path.exists() {
            BufReader::new(
                File::open(&keys_path)
                    .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("embedding_keys: {e}")))?,
            )
            .lines()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?
        } else {
            Vec::new()
        };
        let file = File::open(&vec_path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("embeddings.bin: {e}"))
        })?;
        let mmap = unsafe { Mmap::map(&file) }
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(format!("mmap embeddings: {e}")))?;
        let dim = if keys.is_empty() {
            0
        } else {
            mmap.len() / (keys.len() * 4)
        };
        let n = keys.len();
        let nlist = ((n as f64).sqrt() as usize).clamp(1, 4096);
        let lists = if assign_path.exists() {
            let raw = std::fs::read(&assign_path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let mut lists = vec![Vec::new(); nlist];
            for (i, chunk) in raw.chunks_exact(4).enumerate() {
                let list = u32::from_le_bytes(chunk.try_into().unwrap()) as usize;
                if list < lists.len() && i < n {
                    lists[list].push(i);
                }
            }
            lists
        } else {
            let mut lists = vec![Vec::new(); nlist];
            for i in 0..n {
                lists[i % nlist].push(i);
            }
            lists
        };
        Ok(Self {
            dim,
            keys,
            mmap,
            nlist,
            lists,
        })
    }

}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
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

impl VectorAnn for IvfMmapAnn {
    fn knn(&self, query: &[f32], k: usize) -> Vec<(String, f32)> {
        if self.keys.is_empty() || self.dim == 0 || k == 0 {
            return Vec::new();
        }
        let nprobe = (self.nlist.min(32)).max(1);
        // Probe lists whose first member is closest to the query (cheap IVF).
        let mut list_scores: Vec<(f32, usize)> = (0..self.nlist)
            .map(|li| {
                let first = self.lists.get(li).and_then(|v| v.first()).copied();
                let score = first
                    .map(|idx| cosine(query, &read_f32_vec(&self.mmap, self.dim, idx)))
                    .unwrap_or(0.0);
                (score, li)
            })
            .collect();
        list_scores.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        let probe: Vec<usize> = list_scores.into_iter().take(nprobe).map(|(_, i)| i).collect();
        let candidates: Vec<usize> = probe.iter().flat_map(|li| self.lists[*li].iter().copied()).collect();
        let mut scored: Vec<(f32, usize)> = candidates
            .par_iter()
            .map(|&idx| (cosine(query, &read_f32_vec(&self.mmap, self.dim, idx)), idx))
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored
            .into_iter()
            .take(k)
            .map(|(sim, idx)| (self.keys[idx].clone(), sim))
            .collect()
    }
}

pub fn write_ivf_assignments(dir: &Path, n: usize) -> PyResult<()> {
    if n == 0 {
        return Ok(());
    }
    let nlist = ((n as f64).sqrt() as usize).clamp(1, 4096);
    let mut raw = Vec::with_capacity(n * 4);
    for i in 0..n {
        raw.extend_from_slice(&((i % nlist) as u32).to_le_bytes());
    }
    let mut f = File::create(dir.join("ivf_assign.bin"))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    f.write_all(&raw)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    Ok(())
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
    write_ivf_assignments(dir, keys.len())
}

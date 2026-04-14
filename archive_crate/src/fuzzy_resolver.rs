//! Fuzzy name scoring — Step 14 (token-sort ratio via strsim, matches rapidfuzz-style pipeline).

use pyo3::prelude::*;

/// Token-sort both strings, then normalized Levenshtein × 100 (same pattern as plan §14b).
pub(crate) fn token_sort_ratio_inner(a: &str, b: &str) -> f64 {
    let mut ta: Vec<&str> = a.split_whitespace().collect();
    let mut tb: Vec<&str> = b.split_whitespace().collect();
    ta.sort_unstable();
    tb.sort_unstable();
    let sa = ta.join(" ");
    let sb = tb.join(" ");
    strsim::normalized_levenshtein(&sa, &sb) * 100.0
}

/// Token-sort both strings, then normalized Levenshtein × 100 (same pattern as plan §14b).
#[pyfunction]
pub fn token_sort_ratio(a: &str, b: &str) -> f64 {
    token_sort_ratio_inner(a, b)
}

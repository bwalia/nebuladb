//! Distance metrics for dense float vectors.
//!
//! All metrics here are **distances**: smaller = more similar. For cosine we
//! return `1 - cos(a, b)` so the ordering matches L2 and the same
//! min-heap code paths work unchanged.
//!
//! The functions are deliberately plain `f32` loops. LLVM auto-vectorizes
//! them on x86-64 (AVX2) and aarch64 (NEON) for dimension multiples of 8,
//! which covers 384/768/1536 — the dims we target. A hand-rolled SIMD
//! layer can be swapped in later behind the same signatures.

use serde::{Deserialize, Serialize};

/// Distance metric selection. Stored with the index so search and insert
/// always agree, and so loading an index from disk can't silently switch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Metric {
    /// `1 - cos(a, b)`. Magnitude-invariant; the typical choice for
    /// modern embedding models which emit normalized-ish outputs.
    Cosine,
    /// Squared euclidean, `sum((a-b)^2)`. Monotonic with L2, and skipping
    /// the sqrt is a free speedup because we only compare distances.
    L2Sq,
    /// Negative dot product, so smaller = more similar.
    NegDot,
}

impl Metric {
    /// Compute the distance between two equal-length slices.
    ///
    /// # Panics
    /// Panics if `a.len() != b.len()`. The HNSW index never calls this
    /// with mismatched lengths, so an assertion here catches programmer
    /// errors instead of returning a `Result` everywhere in the hot loop.
    #[inline]
    pub fn distance(self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len());
        match self {
            Metric::Cosine => cosine_distance(a, b),
            Metric::L2Sq => euclidean_sq(a, b),
            Metric::NegDot => -dot(a, b),
        }
    }
}

#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut s = 0.0f32;
    for i in 0..a.len() {
        s += a[i] * b[i];
    }
    s
}

#[inline]
pub fn euclidean_sq(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut s = 0.0f32;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

#[inline]
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    // Zero-norm vectors are ill-defined for cosine; treat as maximally
    // distant rather than propagating NaN through the index.
    let denom = (na.sqrt()) * (nb.sqrt());
    if denom == 0.0 {
        return 1.0;
    }
    1.0 - (dot / denom)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_identical_is_zero() {
        let a = vec![1.0_f32, 2.0, 3.0];
        assert!(cosine_distance(&a, &a).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_is_one() {
        let a = [1.0_f32, 0.0];
        let b = [0.0_f32, 1.0];
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_zero_norm_saturates() {
        let a = [0.0_f32, 0.0];
        let b = [1.0_f32, 0.0];
        assert_eq!(cosine_distance(&a, &b), 1.0);
    }

    #[test]
    fn l2sq_matches_manual() {
        let a = [1.0_f32, 2.0, 3.0];
        let b = [4.0_f32, 0.0, -1.0];
        // (−3)^2 + 2^2 + 4^2 = 9 + 4 + 16 = 29
        assert!((euclidean_sq(&a, &b) - 29.0).abs() < 1e-6);
    }

    #[test]
    fn neg_dot_orders_correctly() {
        let q = [1.0_f32, 0.0];
        let near = [0.9_f32, 0.1];
        let far = [-1.0_f32, 0.0];
        assert!(Metric::NegDot.distance(&q, &near) < Metric::NegDot.distance(&q, &far));
    }
}

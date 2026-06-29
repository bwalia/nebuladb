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

    /// Distance between an f32 query and an int8-quantized stored vector,
    /// fusing dequantization into the metric loop so the hot search path
    /// never allocates a temporary f32 buffer per candidate. `code` is the
    /// stored vector quantized via [`quantize`]; `scale` is its companion
    /// per-vector scale (so the dequantized value is `code[i] * scale`).
    ///
    /// # Panics
    /// Panics if `q.len() != code.len()` (same contract as [`distance`]).
    #[inline]
    pub fn distance_code(self, q: &[f32], code: &[i8], scale: f32) -> f32 {
        debug_assert_eq!(q.len(), code.len());
        match self {
            Metric::Cosine => cosine_distance_code(q, code, scale),
            Metric::L2Sq => euclidean_sq_code(q, code, scale),
            Metric::NegDot => -dot_code(q, code, scale),
        }
    }
}

/// Symmetric per-vector int8 quantization. Returns the quantized codes
/// and the scale `s` such that `value[i] ≈ codes[i] * s`. The scale is
/// `max(|value|) / 127`, so the largest-magnitude component maps to
/// ±127 and the int8 range is used fully. An all-zero vector yields a
/// zero scale and all-zero codes (dequantizes back to zeros).
///
/// This is the storage format for the HNSW arena: ~4x smaller than the
/// f32 it replaces, at the cost of a small, bounded rounding error that
/// shows up as a minor recall delta — see the recall tests in `hnsw`.
pub fn quantize(value: &[f32]) -> (Vec<i8>, f32) {
    let max_abs = value.iter().fold(0.0f32, |m, &x| m.max(x.abs()));
    if max_abs == 0.0 {
        return (vec![0i8; value.len()], 0.0);
    }
    let scale = max_abs / 127.0;
    let inv = 1.0 / scale;
    let codes = value
        .iter()
        .map(|&x| {
            // round-half-away-from-zero, then clamp into i8 range. The
            // clamp guards the largest component which rounds to exactly
            // ±127 and any FP drift that would push it to ±128.
            (x * inv).round().clamp(-127.0, 127.0) as i8
        })
        .collect();
    (codes, scale)
}

/// Dequantize `code`/`scale` (see [`quantize`]) into `out`. `out.len()`
/// must equal `code.len()`.
#[inline]
pub fn dequantize_into(code: &[i8], scale: f32, out: &mut [f32]) {
    debug_assert_eq!(code.len(), out.len());
    for (o, &c) in out.iter_mut().zip(code.iter()) {
        *o = c as f32 * scale;
    }
}

#[inline]
fn dot_code(q: &[f32], code: &[i8], scale: f32) -> f32 {
    debug_assert_eq!(q.len(), code.len());
    let mut s = 0.0f32;
    for i in 0..q.len() {
        s += q[i] * code[i] as f32;
    }
    s * scale
}

#[inline]
fn euclidean_sq_code(q: &[f32], code: &[i8], scale: f32) -> f32 {
    debug_assert_eq!(q.len(), code.len());
    let mut s = 0.0f32;
    for i in 0..q.len() {
        let d = q[i] - code[i] as f32 * scale;
        s += d * d;
    }
    s
}

#[inline]
fn cosine_distance_code(q: &[f32], code: &[i8], scale: f32) -> f32 {
    debug_assert_eq!(q.len(), code.len());
    let mut dot = 0.0f32;
    let mut nq = 0.0f32;
    let mut nc = 0.0f32; // squared norm of the *codes*; scale factors out
    for i in 0..q.len() {
        let c = code[i] as f32;
        dot += q[i] * c;
        nq += q[i] * q[i];
        nc += c * c;
    }
    // |stored| = scale * sqrt(nc); denom = |q| * |stored|. The `scale`
    // on both the dot (scale*dot) and the stored norm cancels, so we
    // fold it once into the denominator.
    let denom = nq.sqrt() * nc.sqrt() * scale;
    if denom == 0.0 {
        return 1.0;
    }
    1.0 - (dot * scale / denom)
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

    #[test]
    fn quantize_round_trips_within_tolerance() {
        let v = [0.5_f32, -1.0, 0.25, 0.0, 0.9, -0.33];
        let (codes, scale) = quantize(&v);
        let mut back = vec![0.0f32; v.len()];
        dequantize_into(&codes, scale, &mut back);
        // Max error is half a quantization step = scale/2.
        for (a, b) in v.iter().zip(back.iter()) {
            assert!((a - b).abs() <= scale / 2.0 + 1e-6, "{a} vs {b}");
        }
    }

    #[test]
    fn quantize_zero_vector_is_all_zero() {
        let (codes, scale) = quantize(&[0.0_f32, 0.0, 0.0]);
        assert_eq!(scale, 0.0);
        assert!(codes.iter().all(|&c| c == 0));
    }

    #[test]
    fn quantize_uses_full_int8_range_for_max_component() {
        let (codes, _) = quantize(&[0.1_f32, -2.0, 0.5]);
        // The largest-magnitude component maps to ±127.
        assert_eq!(codes[1], -127);
    }

    #[test]
    fn code_distance_matches_dequantized_distance() {
        // distance_code must equal computing the metric against the
        // explicitly dequantized vector — that equivalence is what lets
        // search skip the per-candidate temporary buffer.
        let stored = [0.4_f32, -0.8, 0.2, 0.6, -0.1];
        let query = [0.3_f32, -0.7, 0.25, 0.55, 0.05];
        let (codes, scale) = quantize(&stored);
        let mut deq = vec![0.0f32; stored.len()];
        dequantize_into(&codes, scale, &mut deq);
        for m in [Metric::Cosine, Metric::L2Sq, Metric::NegDot] {
            let fused = m.distance_code(&query, &codes, scale);
            let explicit = m.distance(&query, &deq);
            assert!((fused - explicit).abs() < 1e-5, "{m:?}: {fused} vs {explicit}");
        }
    }

    #[test]
    fn code_distance_zero_norm_saturates_cosine() {
        let query = [1.0_f32, 0.0];
        let (codes, scale) = quantize(&[0.0_f32, 0.0]);
        assert_eq!(Metric::Cosine.distance_code(&query, &codes, scale), 1.0);
    }
}

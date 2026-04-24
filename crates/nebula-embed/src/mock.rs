//! Deterministic offline embedder.
//!
//! Produces the same vector for the same `(model, text)` on every run
//! and every platform. That makes it usable both as a test fixture and
//! as a real default in dev/CI where we don't want to call out to an
//! external API.
//!
//! The vectors aren't semantically meaningful — this is a hash, not a
//! language model. But they are L2-normalized and well-distributed, so
//! ANN indexes built on top of them behave numerically like the real
//! thing (no zero vectors, no pathological clusters).

use async_trait::async_trait;

use crate::{EmbedError, Embedder, Result};

#[derive(Debug, Clone)]
pub struct MockEmbedder {
    dim: usize,
    model: String,
}

impl MockEmbedder {
    pub fn new(dim: usize) -> Self {
        assert!(dim > 0, "dim must be > 0");
        Self {
            dim,
            model: format!("mock-{dim}"),
        }
    }
}

impl Default for MockEmbedder {
    fn default() -> Self {
        // 384 matches small sentence-transformers models; a realistic
        // default for "swap in a real provider later without resizing".
        Self::new(384)
    }
}

#[async_trait]
impl Embedder for MockEmbedder {
    fn dim(&self) -> usize {
        self.dim
    }

    fn model(&self) -> &str {
        &self.model
    }

    async fn embed(&self, inputs: &[String]) -> Result<Vec<Vec<f32>>> {
        if inputs.is_empty() {
            return Err(EmbedError::EmptyBatch);
        }
        Ok(inputs.iter().map(|s| embed_one(s, self.dim)).collect())
    }
}

/// Hash-seeded PRNG → raw floats → L2-normalize.
///
/// Using FNV-1a + xorshift keeps this dependency-free and
/// byte-identical across platforms, which is the point of a "mock". We
/// deliberately don't use `rand` here so a future change to `rand`'s
/// sampling doesn't invalidate saved test fixtures.
fn embed_one(text: &str, dim: usize) -> Vec<f32> {
    let mut seed = fnv1a_64(text.as_bytes());
    let mut out = Vec::with_capacity(dim);
    for _ in 0..dim {
        seed = xorshift64(seed);
        // Map u64 → f32 in (-1, 1). Using the top 24 bits keeps the
        // float bit pattern well-distributed.
        let bits = (seed >> 40) as u32;
        let u = (bits as f32) / (1u32 << 24) as f32; // [0, 1)
        out.push(u * 2.0 - 1.0);
    }
    l2_normalize_in_place(&mut out);
    out
}

fn fnv1a_64(data: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for b in data {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    // Mix in a non-zero constant so empty strings still seed a usable
    // state for xorshift (xorshift is degenerate at zero).
    if h == 0 {
        h = 0x9e3779b97f4a7c15;
    }
    h
}

fn xorshift64(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

fn l2_normalize_in_place(v: &mut [f32]) {
    let mut n = 0.0f32;
    for x in v.iter() {
        n += x * x;
    }
    let n = n.sqrt();
    if n > 0.0 {
        for x in v.iter_mut() {
            *x /= n;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn is_deterministic() {
        let e = MockEmbedder::new(64);
        let a = e.embed_one("zero trust architecture").await.unwrap();
        let b = e.embed_one("zero trust architecture").await.unwrap();
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn different_inputs_differ() {
        let e = MockEmbedder::new(64);
        let a = e.embed_one("alpha").await.unwrap();
        let b = e.embed_one("beta").await.unwrap();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn output_is_unit_length() {
        let e = MockEmbedder::new(128);
        let v = e.embed_one("hello").await.unwrap();
        let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-4, "not unit length: {n}");
    }

    #[tokio::test]
    async fn empty_batch_errors() {
        let e = MockEmbedder::new(8);
        assert!(matches!(e.embed(&[]).await, Err(EmbedError::EmptyBatch)));
    }
}

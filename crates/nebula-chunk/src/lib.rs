//! Document chunking for embedding pipelines.
//!
//! # Why a crate
//!
//! Chunking lives at the intersection of tokenizer behavior (every
//! embedding model has a context window in *tokens*), text layout
//! (sentence / paragraph boundaries), and encoding (UTF-8 char
//! boundaries). A naive `&text[start..end]` is a three-way footgun
//! on real inputs. Isolating this into a trait lets us swap strategies
//! per-bucket later without touching the index.
//!
//! # Strategies
//!
//! - [`FixedSizeChunker`]: deterministic window over *Unicode scalar
//!   values*, with overlap. No allocations per chunk beyond the
//!   returned `String`. Works on any input. Default.
//! - [`SentenceChunker`]: splits on sentence terminators (`.`, `!`,
//!   `?`, newline), then greedily packs sentences into windows up to
//!   `max_chars`. Falls back to fixed-size for sentences longer than
//!   the window (e.g. code blocks).
//!
//! # Units
//!
//! Sizes are in **Unicode scalar values** (`char` count), not bytes
//! and not tokens. Char count is a reasonable proxy for token count
//! (~4 chars per token in English); a later change can swap to a real
//! tokenizer behind the same trait.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("invalid config: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, ChunkError>;

/// One output unit. `index` is the 0-based ordinal within its parent
/// document so the index layer can reconstruct order. `char_start` is
/// useful for later features like citation offsets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    pub index: usize,
    pub char_start: usize,
    pub text: String,
}

pub trait Chunker: Send + Sync {
    fn chunk(&self, text: &str) -> Vec<Chunk>;
}

#[derive(Debug, Clone)]
pub struct FixedSizeChunker {
    pub chunk_chars: usize,
    pub overlap_chars: usize,
}

impl FixedSizeChunker {
    pub fn new(chunk_chars: usize, overlap_chars: usize) -> Result<Self> {
        if chunk_chars == 0 {
            return Err(ChunkError::InvalidConfig("chunk_chars must be > 0".into()));
        }
        if overlap_chars >= chunk_chars {
            // An overlap >= window makes the stride zero or negative and
            // would loop forever. Reject at construction.
            return Err(ChunkError::InvalidConfig(
                "overlap_chars must be < chunk_chars".into(),
            ));
        }
        Ok(Self {
            chunk_chars,
            overlap_chars,
        })
    }
}

impl Default for FixedSizeChunker {
    /// 500-char window with 50-char overlap — a common default for
    /// retrieval over prose. Tune per workload.
    fn default() -> Self {
        Self::new(500, 50).unwrap()
    }
}

impl Chunker for FixedSizeChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        if text.is_empty() {
            return Vec::new();
        }
        // Collect into `Vec<char>` once so we can slice by char index in
        // O(1) and never split inside a multi-byte UTF-8 sequence.
        // For very large inputs this uses 4× memory briefly; if that
        // becomes a problem we switch to a char-indices iterator that
        // tracks byte offsets instead.
        let chars: Vec<char> = text.chars().collect();
        let stride = self.chunk_chars - self.overlap_chars;
        let mut out = Vec::new();
        let mut start = 0usize;
        let mut idx = 0usize;
        while start < chars.len() {
            let end = (start + self.chunk_chars).min(chars.len());
            let slice: String = chars[start..end].iter().collect();
            out.push(Chunk {
                index: idx,
                char_start: start,
                text: slice,
            });
            idx += 1;
            if end == chars.len() {
                break;
            }
            start += stride;
        }
        out
    }
}

/// Sentence-greedy chunker. For inputs with a sensible sentence
/// structure this produces chunks that start and end at clause
/// boundaries, which materially improves retrieval quality for
/// question-answering workloads. For code / logs / single-token inputs
/// it degenerates to the fallback, which is fine.
#[derive(Debug, Clone)]
pub struct SentenceChunker {
    pub max_chars: usize,
    pub overlap_chars: usize,
    fallback: FixedSizeChunker,
}

impl SentenceChunker {
    pub fn new(max_chars: usize, overlap_chars: usize) -> Result<Self> {
        Ok(Self {
            max_chars,
            overlap_chars,
            fallback: FixedSizeChunker::new(max_chars, overlap_chars)?,
        })
    }
}

impl Chunker for SentenceChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        let sentences = split_sentences(text);
        if sentences.is_empty() {
            return Vec::new();
        }

        let mut out = Vec::new();
        let mut current = String::new();
        let mut current_start = 0usize;
        let mut idx = 0usize;

        for (sent_start, sent) in sentences {
            let sent_chars = sent.chars().count();

            // Sentence longer than the window: flush what we have, then
            // fall back to fixed-size for the monster sentence so we
            // never produce a single chunk larger than `max_chars`.
            if sent_chars > self.max_chars {
                if !current.is_empty() {
                    out.push(Chunk {
                        index: idx,
                        char_start: current_start,
                        text: std::mem::take(&mut current),
                    });
                    idx += 1;
                }
                for sub in self.fallback.chunk(&sent) {
                    out.push(Chunk {
                        index: idx,
                        char_start: sent_start + sub.char_start,
                        text: sub.text,
                    });
                    idx += 1;
                }
                continue;
            }

            let cur_chars = current.chars().count();
            if cur_chars + sent_chars > self.max_chars {
                // Flush current, carry overlap.
                if !current.is_empty() {
                    out.push(Chunk {
                        index: idx,
                        char_start: current_start,
                        text: current.clone(),
                    });
                    idx += 1;
                }
                // Seed the next chunk with the tail of the last one so
                // sentences that span the boundary remain retrievable.
                // Cap the tail so `tail + next_sentence` still fits in
                // the window — otherwise a greedy overlap can push the
                // new chunk over `max_chars`.
                let max_tail = self.max_chars.saturating_sub(sent_chars);
                let overlap = self.overlap_chars.min(max_tail);
                let tail_rev: String = current.chars().rev().take(overlap).collect();
                let tail: String = tail_rev.chars().rev().collect();
                let tail_len = tail.chars().count();
                current = tail;
                current_start = sent_start.saturating_sub(tail_len);
                current.push_str(&sent);
            } else {
                if current.is_empty() {
                    current_start = sent_start;
                }
                current.push_str(&sent);
            }
        }
        if !current.is_empty() {
            out.push(Chunk {
                index: idx,
                char_start: current_start,
                text: current,
            });
        }
        out
    }
}

/// Split on common sentence terminators while preserving the trailing
/// punctuation on the sentence it belongs to. Returns `(char_start,
/// sentence_text)` pairs — `char_start` is the offset in the original
/// string measured in Unicode scalars.
///
/// This is intentionally dumb (no abbreviation handling, no locales).
/// A real deployment swaps in `unicode-segmentation` or `icu`.
fn split_sentences(text: &str) -> Vec<(usize, String)> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut buf_start = 0usize;
    for (char_idx, c) in text.chars().enumerate() {
        if buf.is_empty() {
            buf_start = char_idx;
        }
        buf.push(c);
        if matches!(c, '.' | '!' | '?' | '\n') {
            out.push((buf_start, std::mem::take(&mut buf)));
        }
    }
    if !buf.is_empty() {
        out.push((buf_start, buf));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_empty_input_yields_no_chunks() {
        let c = FixedSizeChunker::default();
        assert!(c.chunk("").is_empty());
    }

    #[test]
    fn fixed_short_input_yields_one_chunk() {
        let c = FixedSizeChunker::new(100, 10).unwrap();
        let out = c.chunk("hello world");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].text, "hello world");
        assert_eq!(out[0].char_start, 0);
    }

    #[test]
    fn fixed_overlap_produces_expected_chunks() {
        let c = FixedSizeChunker::new(5, 2).unwrap();
        // 10 chars, window 5, stride 3 → starts at 0, 3, 6. At 6 the
        // window is "ghij" (4 chars, clamped to end-of-input) and we
        // stop because `end == len`. Three chunks total, contiguous
        // coverage, overlap honored on all but the last.
        let out = c.chunk("abcdefghij");
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].text, "abcde");
        assert_eq!(out[1].text, "defgh");
        assert_eq!(out[2].text, "ghij");
    }

    #[test]
    fn fixed_utf8_boundary_safe() {
        // Each emoji is a single scalar but 4 bytes. A byte-sliced
        // implementation would panic; a scalar-sliced one is fine.
        let c = FixedSizeChunker::new(2, 0).unwrap();
        let out = c.chunk("🙂🙃🙂🙃");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].text, "🙂🙃");
        assert_eq!(out[1].text, "🙂🙃");
    }

    #[test]
    fn fixed_rejects_bad_config() {
        assert!(FixedSizeChunker::new(0, 0).is_err());
        assert!(FixedSizeChunker::new(10, 10).is_err());
        assert!(FixedSizeChunker::new(10, 11).is_err());
    }

    #[test]
    fn sentence_chunker_groups_sentences() {
        let c = SentenceChunker::new(60, 5).unwrap();
        let input = "First sentence. Second sentence! Third? Fourth.";
        let out = c.chunk(input);
        // Everything fits in 60 chars, so we expect a single chunk.
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].text, input);
    }

    #[test]
    fn sentence_chunker_splits_at_window() {
        let c = SentenceChunker::new(20, 3).unwrap();
        let input = "alpha beta gamma. delta epsilon zeta.";
        let out = c.chunk(input);
        assert!(out.len() >= 2);
        // Each chunk is within the limit.
        assert!(out.iter().all(|ch| ch.text.chars().count() <= 20));
    }

    #[test]
    fn sentence_chunker_handles_oversize_sentence() {
        let c = SentenceChunker::new(10, 2).unwrap();
        // No terminators → one giant "sentence" → fallback path.
        let input = "abcdefghijklmnopqrstuvwxyz";
        let out = c.chunk(input);
        assert!(out.len() >= 3);
        assert!(out.iter().all(|ch| ch.text.chars().count() <= 10));
    }

    #[test]
    fn indices_are_dense_and_monotone() {
        let c = FixedSizeChunker::new(5, 1).unwrap();
        let out = c.chunk("aaaaabbbbbccccc");
        for (i, ch) in out.iter().enumerate() {
            assert_eq!(ch.index, i);
        }
    }
}

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

/// Document kind, used to auto-select a chunking strategy (design 0008
/// §8). The ingestion worker detects this from the file extension /
/// content; the server's default path uses [`DocType::Text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocType {
    /// Markdown / docs — split on headings so each chunk is a section.
    Markdown,
    /// HTML — treated like Markdown after the worker strips tags; we
    /// still heading-split on the residual structure.
    Html,
    /// Source code — split on top-level definitions (fn/class/etc.).
    Code,
    /// Plain prose — sentence-greedy packing.
    Text,
    /// Structured rows (CSV/JSON-lines) — fixed-size is the safe choice;
    /// row-aware splitting is a later refinement.
    Structured,
}

/// Pick a best-practice chunker for a document kind (design 0008 §8).
///
/// The parameters (`target_chars`, `overlap_chars`) bound chunk size
/// uniformly across strategies so retrieval behaves consistently
/// regardless of which one fires. Returns a boxed trait object because
/// the concrete type varies by `doc_type` and callers store one
/// `Arc<dyn Chunker>` per ingest.
pub fn select_strategy(
    doc_type: DocType,
    target_chars: usize,
    overlap_chars: usize,
) -> Result<Box<dyn Chunker>> {
    Ok(match doc_type {
        DocType::Markdown | DocType::Html => {
            Box::new(HeadingAwareChunker::new(target_chars, overlap_chars)?)
        }
        DocType::Code => Box::new(CodeAwareChunker::new(target_chars, overlap_chars)?),
        DocType::Text => Box::new(SentenceChunker::new(target_chars, overlap_chars)?),
        DocType::Structured => Box::new(FixedSizeChunker::new(target_chars, overlap_chars)?),
    })
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

/// Heading-aware chunker for Markdown / docs. Splits the document into
/// sections at Markdown ATX headings (`#`, `##`, …), keeping the heading
/// line attached to its section so each chunk carries its own title —
/// which materially helps retrieval ("what does section X say?"). A
/// section larger than `max_chars` is sub-split by the sentence chunker
/// so no single chunk exceeds the window. Text before the first heading
/// becomes its own leading section.
#[derive(Debug, Clone)]
pub struct HeadingAwareChunker {
    max_chars: usize,
    fallback: SentenceChunker,
}

impl HeadingAwareChunker {
    pub fn new(max_chars: usize, overlap_chars: usize) -> Result<Self> {
        Ok(Self {
            max_chars,
            fallback: SentenceChunker::new(max_chars, overlap_chars)?,
        })
    }
}

impl Chunker for HeadingAwareChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        if text.is_empty() {
            return Vec::new();
        }
        let sections = split_headings(text);
        emit_sections(sections, self.max_chars, &self.fallback)
    }
}

/// Code-aware chunker. Splits source at top-level definition boundaries
/// — lines that begin (no leading whitespace) with a definition keyword
/// common across mainstream languages (`fn`, `def`, `class`, `func`,
/// `impl`, `struct`, `enum`, `trait`, `interface`, `type`, `pub`,
/// `async`, `export`, `function`). Each function/class stays in one
/// chunk where it fits the window, so a retrieved chunk is a coherent
/// unit of code. Oversized definitions fall back to fixed-size slicing
/// (prose sentence rules don't apply to code).
#[derive(Debug, Clone)]
pub struct CodeAwareChunker {
    max_chars: usize,
    fallback: FixedSizeChunker,
}

impl CodeAwareChunker {
    pub fn new(max_chars: usize, overlap_chars: usize) -> Result<Self> {
        Ok(Self {
            max_chars,
            fallback: FixedSizeChunker::new(max_chars, overlap_chars)?,
        })
    }
}

impl Chunker for CodeAwareChunker {
    fn chunk(&self, text: &str) -> Vec<Chunk> {
        if text.is_empty() {
            return Vec::new();
        }
        let sections = split_code_definitions(text);
        emit_sections(sections, self.max_chars, &self.fallback)
    }
}

/// Shared section→chunk emission for the structure-aware chunkers. Each
/// `(char_start, section_text)` becomes one chunk if it fits `max_chars`,
/// otherwise it's sub-split by `fallback` with char offsets rebased onto
/// the original document. Chunk indices are dense and monotone across
/// the whole document regardless of how many sub-splits occur.
fn emit_sections(
    sections: Vec<(usize, String)>,
    max_chars: usize,
    fallback: &dyn Chunker,
) -> Vec<Chunk> {
    let mut out = Vec::new();
    let mut idx = 0usize;
    for (start, body) in sections {
        if body.trim().is_empty() {
            continue;
        }
        if body.chars().count() <= max_chars {
            out.push(Chunk {
                index: idx,
                char_start: start,
                text: body,
            });
            idx += 1;
        } else {
            for sub in fallback.chunk(&body) {
                out.push(Chunk {
                    index: idx,
                    char_start: start + sub.char_start,
                    text: sub.text,
                });
                idx += 1;
            }
        }
    }
    out
}

/// Split Markdown into `(char_start, section)` at ATX headings. A
/// heading line (optional leading spaces, then 1–6 `#` followed by a
/// space) starts a new section and is kept as that section's first line.
fn split_headings(text: &str) -> Vec<(usize, String)> {
    let mut sections = Vec::new();
    let mut current = String::new();
    let mut current_start = 0usize;
    let mut char_pos = 0usize;

    for line in text.split_inclusive('\n') {
        let is_heading = is_markdown_heading(line);
        if is_heading && !current.is_empty() {
            sections.push((current_start, std::mem::take(&mut current)));
            current_start = char_pos;
        } else if current.is_empty() {
            current_start = char_pos;
        }
        current.push_str(line);
        char_pos += line.chars().count();
    }
    if !current.is_empty() {
        sections.push((current_start, current));
    }
    sections
}

fn is_markdown_heading(line: &str) -> bool {
    let t = line.trim_start();
    let hashes = t.chars().take_while(|c| *c == '#').count();
    (1..=6).contains(&hashes) && t.chars().nth(hashes) == Some(' ')
}

/// Split source code into `(char_start, section)` at top-level
/// definition lines (a definition keyword with no leading indentation).
/// The boundary line begins a new section. Code before the first such
/// line (imports, license header) forms a leading section.
fn split_code_definitions(text: &str) -> Vec<(usize, String)> {
    let mut sections = Vec::new();
    let mut current = String::new();
    let mut current_start = 0usize;
    let mut char_pos = 0usize;

    for line in text.split_inclusive('\n') {
        let is_def = is_top_level_definition(line);
        if is_def && !current.is_empty() {
            sections.push((current_start, std::mem::take(&mut current)));
            current_start = char_pos;
        } else if current.is_empty() {
            current_start = char_pos;
        }
        current.push_str(line);
        char_pos += line.chars().count();
    }
    if !current.is_empty() {
        sections.push((current_start, current));
    }
    sections
}

const DEF_KEYWORDS: &[&str] = &[
    "fn", "pub", "async", "def", "class", "func", "impl", "struct", "enum",
    "trait", "interface", "type", "export", "function", "module",
];

fn is_top_level_definition(line: &str) -> bool {
    // Top-level = no leading whitespace. The first whitespace-delimited
    // token must be a definition keyword.
    if line.starts_with([' ', '\t']) {
        return false;
    }
    let Some(first) = line.split_whitespace().next() else {
        return false;
    };
    DEF_KEYWORDS.contains(&first)
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

    // ---------- heading-aware ----------

    #[test]
    fn heading_chunker_splits_on_headings() {
        let c = HeadingAwareChunker::new(500, 50).unwrap();
        let md = "# Title\nintro line\n\n## Section A\nbody a\n\n## Section B\nbody b\n";
        let out = c.chunk(md);
        assert_eq!(out.len(), 3);
        assert!(out[0].text.starts_with("# Title"));
        assert!(out[1].text.starts_with("## Section A"));
        assert!(out[2].text.starts_with("## Section B"));
        // Heading line stays attached to its section.
        assert!(out[1].text.contains("body a"));
    }

    #[test]
    fn heading_chunker_keeps_preamble_before_first_heading() {
        let c = HeadingAwareChunker::new(500, 50).unwrap();
        let md = "preamble text with no heading\n# First\nbody\n";
        let out = c.chunk(md);
        assert_eq!(out.len(), 2);
        assert!(out[0].text.contains("preamble"));
        assert!(out[1].text.starts_with("# First"));
    }

    #[test]
    fn heading_chunker_subsplits_oversize_section() {
        let c = HeadingAwareChunker::new(20, 3).unwrap();
        let md = "# H\nalpha beta gamma. delta epsilon zeta. eta theta iota.\n";
        let out = c.chunk(md);
        assert!(out.len() >= 2);
        assert!(out.iter().all(|ch| ch.text.chars().count() <= 20));
        // Indices remain dense across the sub-split.
        for (i, ch) in out.iter().enumerate() {
            assert_eq!(ch.index, i);
        }
    }

    #[test]
    fn heading_chunker_char_start_points_into_original() {
        let c = HeadingAwareChunker::new(500, 50).unwrap();
        let md = "# A\nbody a\n## B\nbody b\n";
        let chars: Vec<char> = md.chars().collect();
        let out = c.chunk(md);
        for ch in &out {
            let slice: String = chars[ch.char_start..ch.char_start + ch.text.chars().count()]
                .iter()
                .collect();
            assert_eq!(slice, ch.text, "char_start must index the original text");
        }
    }

    #[test]
    fn heading_chunker_no_headings_is_one_section() {
        let c = HeadingAwareChunker::new(500, 50).unwrap();
        let out = c.chunk("just some prose with no headings at all");
        assert_eq!(out.len(), 1);
    }

    // ---------- code-aware ----------

    #[test]
    fn code_chunker_splits_on_top_level_defs() {
        let c = CodeAwareChunker::new(500, 50).unwrap();
        let src = "use std::io;\n\nfn alpha() {\n    do_a();\n}\n\nfn beta() {\n    do_b();\n}\n";
        let out = c.chunk(src);
        // leading `use` section + two fns.
        assert_eq!(out.len(), 3);
        assert!(out[0].text.contains("use std::io"));
        assert!(out[1].text.starts_with("fn alpha"));
        assert!(out[2].text.starts_with("fn beta"));
    }

    #[test]
    fn code_chunker_ignores_indented_keywords() {
        // A `fn` nested inside another block is not a top-level boundary.
        let c = CodeAwareChunker::new(500, 50).unwrap();
        let src = "fn outer() {\n    fn inner() {}\n    inner();\n}\n";
        let out = c.chunk(src);
        assert_eq!(out.len(), 1, "indented fn must not start a new section");
    }

    #[test]
    fn code_chunker_subsplits_oversize_def() {
        let c = CodeAwareChunker::new(15, 3).unwrap();
        let src = "fn big() { aaaaaaaaaaaaaaaaaaaaaaaaaaaa }\n";
        let out = c.chunk(src);
        assert!(out.len() >= 2);
        assert!(out.iter().all(|ch| ch.text.chars().count() <= 15));
    }

    #[test]
    fn code_chunker_recognizes_multiple_languages() {
        let c = CodeAwareChunker::new(500, 50).unwrap();
        let src = "class Foo:\n    pass\n\ndef bar():\n    return 1\n\nexport function baz() {}\n";
        let out = c.chunk(src);
        assert_eq!(out.len(), 3);
    }

    // ---------- select_strategy ----------

    #[test]
    fn select_strategy_picks_expected_chunker_behaviour() {
        // Markdown → heading split.
        let md = select_strategy(DocType::Markdown, 500, 50).unwrap();
        let out = md.chunk("# A\nx\n## B\ny\n");
        assert_eq!(out.len(), 2);

        // Code → definition split.
        let code = select_strategy(DocType::Code, 500, 50).unwrap();
        let out = code.chunk("fn a() {}\nfn b() {}\n");
        assert_eq!(out.len(), 2);

        // Text → sentence packing (single chunk when it fits).
        let text = select_strategy(DocType::Text, 500, 50).unwrap();
        let out = text.chunk("One sentence. Two sentence.");
        assert_eq!(out.len(), 1);

        // Structured → fixed-size.
        let structured = select_strategy(DocType::Structured, 500, 50).unwrap();
        assert_eq!(structured.chunk("a,b,c\n1,2,3\n").len(), 1);
    }

    #[test]
    fn select_strategy_propagates_bad_config() {
        assert!(select_strategy(DocType::Text, 0, 0).is_err());
        assert!(select_strategy(DocType::Code, 10, 10).is_err());
    }

    #[test]
    fn doctype_serde_roundtrip() {
        let j = serde_json::to_string(&DocType::Markdown).unwrap();
        assert_eq!(j, "\"markdown\"");
        let back: DocType = serde_json::from_str("\"code\"").unwrap();
        assert_eq!(back, DocType::Code);
    }
}

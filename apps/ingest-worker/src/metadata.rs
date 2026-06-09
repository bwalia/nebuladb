//! Lightweight, local metadata extraction (design 0008 §8).
//!
//! Generates `summary`/`keywords`/`doc_type` without an LLM or any
//! model — keyword frequency + a leading-text summary. This keeps the
//! default ingest path offline and dependency-free. An LLM-backed
//! enrichment (calling the server's configured `LlmClient`) is a future
//! addition; the JSON shape here is forward-compatible with it.

use std::collections::HashMap;

use nebula_chunk::DocType;
use serde_json::json;

/// Common English stopwords excluded from keyword extraction. Small on
/// purpose — this is a heuristic, not an NLP pipeline.
const STOPWORDS: &[&str] = &[
    "the", "a", "an", "and", "or", "but", "of", "to", "in", "on", "for", "with", "is", "are",
    "was", "were", "be", "been", "as", "at", "by", "it", "this", "that", "these", "those", "from",
    "we", "you", "they", "i", "he", "she", "his", "her", "its", "our", "their", "not", "no", "do",
    "does", "did", "can", "will", "would", "should", "if", "then", "else", "so", "than", "too",
];

/// Build a metadata JSON object from extracted text. Always includes
/// `doc_type`, `keywords`, `summary`, and `char_count`.
pub fn build(text: &str, doc_type: DocType, max_keywords: usize) -> serde_json::Value {
    json!({
        "doc_type": doc_type,
        "summary": summarize(text, 280),
        "keywords": keywords(text, max_keywords),
        "char_count": text.chars().count(),
    })
}

/// First ~`max_chars` of meaningful text, trimmed at a word boundary.
/// Good enough as a card preview; an LLM summary can overwrite this key.
fn summarize(text: &str, max_chars: usize) -> String {
    let normalized: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        return normalized;
    }
    let truncated: String = normalized.chars().take(max_chars).collect();
    // Trim back to the last space so we don't cut a word in half.
    match truncated.rsplit_once(' ') {
        Some((head, _)) => format!("{head}…"),
        None => format!("{truncated}…"),
    }
}

/// Top-N keywords by frequency, excluding stopwords and short tokens.
fn keywords(text: &str, n: usize) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for raw in text.split(|c: char| !c.is_alphanumeric()) {
        let tok = raw.to_lowercase();
        if tok.chars().count() < 3 || STOPWORDS.contains(&tok.as_str()) {
            continue;
        }
        *counts.entry(tok).or_insert(0) += 1;
    }
    let mut pairs: Vec<(String, usize)> = counts.into_iter().collect();
    // Descending frequency; alphabetical tie-break for determinism.
    pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    pairs.into_iter().take(n).map(|(k, _)| k).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keywords_exclude_stopwords_and_rank_by_frequency() {
        let text = "kubernetes kubernetes cluster the the the and pod";
        let kw = keywords(text, 5);
        // "the"/"and" are stopwords → excluded; "kubernetes" most frequent.
        assert_eq!(kw[0], "kubernetes");
        assert!(kw.contains(&"cluster".to_string()));
        assert!(kw.contains(&"pod".to_string()));
        assert!(!kw.contains(&"the".to_string()));
    }

    #[test]
    fn summary_truncates_at_word_boundary() {
        let text = "one two three four five six seven eight nine ten";
        let s = summarize(text, 12);
        assert!(s.ends_with('…'));
        assert!(s.chars().count() <= 13);
        // Did not cut mid-word.
        assert!(!s.trim_end_matches('…').ends_with(|c: char| c.is_alphanumeric() && false));
    }

    #[test]
    fn summary_short_text_unchanged() {
        assert_eq!(summarize("short text", 100), "short text");
    }

    #[test]
    fn build_includes_expected_keys() {
        let m = build("kubernetes cluster pod scaling guide", DocType::Markdown, 5);
        assert_eq!(m["doc_type"], "markdown");
        assert!(m["summary"].is_string());
        assert!(m["keywords"].is_array());
        assert!(m["char_count"].as_u64().unwrap() > 0);
    }
}

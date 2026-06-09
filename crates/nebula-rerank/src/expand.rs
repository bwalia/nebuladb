//! Query expansion (design 0008 §7).
//!
//! Broadens a query before retrieval so recall improves: a search for
//! `VPN` should also match documents that only say "Virtual Private
//! Network". The default is a synonym/abbreviation map — cheap,
//! deterministic, no model. An LLM-backed expander is a future impl
//! behind the same trait (it would reuse `nebula_llm::LlmClient`, so it
//! lives in a layer that already depends on that crate).

use std::collections::HashMap;

use async_trait::async_trait;

/// Expands a query into one or more search strings. The first element of
/// the returned vec is always the original query, so a caller can choose
/// to weight it highest. Implementations must be cheap to clone.
#[async_trait]
pub trait QueryExpander: Send + Sync {
    /// Return the original query plus any expansions. Never empty —
    /// at minimum it contains the input query unchanged.
    async fn expand(&self, query: &str) -> Vec<String>;
}

/// No expansion: returns the query unchanged. The default, so "no
/// expansion stage" needs no `Option` branching at the call site.
#[derive(Debug, Default, Clone)]
pub struct NoopQueryExpander;

#[async_trait]
impl QueryExpander for NoopQueryExpander {
    async fn expand(&self, query: &str) -> Vec<String> {
        vec![query.to_string()]
    }
}

/// Dictionary-driven expander. Holds a case-insensitive map from a term
/// to its synonyms/expansions. For each whole-word token in the query
/// that has an entry, it appends a query variant with that token
/// substituted. Keeps things deterministic and dependency-free; the
/// map is supplied at construction (defaults cover a few common tech
/// abbreviations so it's useful out of the box).
#[derive(Debug, Clone)]
pub struct MapQueryExpander {
    /// Lowercased term → list of expansions (each already lowercased).
    synonyms: HashMap<String, Vec<String>>,
    /// Cap on how many expanded variants to emit, to bound fan-out on
    /// queries with several expandable terms.
    max_variants: usize,
}

impl MapQueryExpander {
    pub fn new(synonyms: HashMap<String, Vec<String>>, max_variants: usize) -> Self {
        Self {
            synonyms: synonyms
                .into_iter()
                .map(|(k, v)| (k.to_lowercase(), v.into_iter().map(|s| s.to_lowercase()).collect()))
                .collect(),
            max_variants,
        }
    }

    /// A small starter dictionary of common infrastructure abbreviations
    /// (the design's `VPN → Virtual Private Network` example, plus a
    /// few peers). Operators replace this with a domain dictionary.
    pub fn with_defaults() -> Self {
        let mut m: HashMap<String, Vec<String>> = HashMap::new();
        m.insert("vpn".into(), vec!["virtual private network".into(), "remote access".into()]);
        m.insert("k8s".into(), vec!["kubernetes".into()]);
        m.insert("dns".into(), vec!["domain name system".into()]);
        m.insert("tls".into(), vec!["ssl".into(), "transport layer security".into()]);
        m.insert("db".into(), vec!["database".into()]);
        Self::new(m, 8)
    }
}

#[async_trait]
impl QueryExpander for MapQueryExpander {
    async fn expand(&self, query: &str) -> Vec<String> {
        let mut out = vec![query.to_string()];

        // Whole-word, case-insensitive token scan. We rebuild the query
        // per matching expansion by substituting just that token,
        // preserving the rest verbatim.
        let tokens: Vec<&str> = query.split_whitespace().collect();
        'outer: for (i, tok) in tokens.iter().enumerate() {
            let key = tok.trim_matches(|c: char| !c.is_alphanumeric()).to_lowercase();
            let Some(expansions) = self.synonyms.get(&key) else {
                continue;
            };
            for exp in expansions {
                let mut rebuilt = tokens.clone();
                rebuilt[i] = exp;
                let variant = rebuilt.join(" ");
                if !out.contains(&variant) {
                    out.push(variant);
                }
                if out.len() >= self.max_variants {
                    break 'outer;
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn noop_returns_query_unchanged() {
        let e = NoopQueryExpander;
        assert_eq!(e.expand("hello world").await, vec!["hello world"]);
    }

    #[tokio::test]
    async fn map_expands_known_abbreviation() {
        let e = MapQueryExpander::with_defaults();
        let out = e.expand("how to configure VPN access").await;
        // Original is always first.
        assert_eq!(out[0], "how to configure VPN access");
        // The VPN token gets substituted in at least one variant.
        assert!(out.iter().any(|v| v.contains("virtual private network")));
        assert!(out.iter().any(|v| v.contains("remote access")));
    }

    #[tokio::test]
    async fn map_leaves_unknown_terms_alone() {
        let e = MapQueryExpander::with_defaults();
        let out = e.expand("quarterly revenue report").await;
        assert_eq!(out, vec!["quarterly revenue report"]);
    }

    #[tokio::test]
    async fn map_is_case_insensitive_and_handles_punctuation() {
        let e = MapQueryExpander::with_defaults();
        let out = e.expand("restart k8s!").await;
        assert!(out.iter().any(|v| v.contains("kubernetes")));
    }

    #[tokio::test]
    async fn map_respects_max_variants() {
        let mut m: HashMap<String, Vec<String>> = HashMap::new();
        m.insert("a".into(), vec!["a1".into(), "a2".into(), "a3".into()]);
        let e = MapQueryExpander::new(m, 2);
        let out = e.expand("a").await;
        // original + capped at max_variants total.
        assert_eq!(out.len(), 2);
    }
}

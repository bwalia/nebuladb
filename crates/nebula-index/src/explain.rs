//! EXPLAIN / EXPLAIN ANALYZE output shared by every query surface.
//!
//! SQL, `/ai/search` and RAG all answer "how did this query arrive at
//! its result?" with one [`Explain`]: the stages that ran (rows in →
//! out, time, stage-specific attributes), warnings worth acting on, and
//! a per-result breakdown of the score. The showcase renders it; SQL
//! `EXPLAIN [ANALYZE]` also returns [`Explain::text`] as `QUERY PLAN`
//! rows so psql users get the familiar Postgres shape.
//!
//! The index fills in the retrieval stages (embed, HNSW, bucket filter,
//! BM25, fusion); callers append their own (SQL filters/sort/limit,
//! RAG expansion/rerank/prompt/LLM) and then call [`Explain::finish`].

use serde::Serialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExplainKind {
    Sql,
    Search,
    Rag,
}

/// One executed (or, for plan-only EXPLAIN, planned) step.
#[derive(Debug, Clone, Serialize)]
pub struct Stage {
    /// Stable id: "embed", "hnsw", "bucket_filter", "bm25", "fuse",
    /// "filter", "sort", "limit", "project", ...
    pub name: String,
    /// Short human title.
    pub label: String,
    /// One plain-English sentence: what the stage did and why.
    pub detail: String,
    pub rows_in: Option<usize>,
    pub rows_out: Option<usize>,
    pub took_us: u64,
    pub attrs: Map<String, Value>,
}

impl Stage {
    pub fn new(name: &str, label: &str, detail: impl Into<String>) -> Self {
        Self {
            name: name.to_string(),
            label: label.to_string(),
            detail: detail.into(),
            rows_in: None,
            rows_out: None,
            took_us: 0,
            attrs: Map::new(),
        }
    }

    pub fn rows(mut self, rows_in: Option<usize>, rows_out: Option<usize>) -> Self {
        self.rows_in = rows_in;
        self.rows_out = rows_out;
        self
    }

    pub fn took(mut self, took: std::time::Duration) -> Self {
        self.took_us = took.as_micros() as u64;
        self
    }

    pub fn attr(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.attrs.insert(key.to_string(), value.into());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NoteLevel {
    Info,
    Warn,
}

#[derive(Debug, Clone, Serialize)]
pub struct Note {
    pub level: NoteLevel,
    pub message: String,
}

impl Note {
    pub fn info(message: impl Into<String>) -> Self {
        Self { level: NoteLevel::Info, message: message.into() }
    }
    pub fn warn(message: impl Into<String>) -> Self {
        Self { level: NoteLevel::Warn, message: message.into() }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreKind {
    /// Vector distance — lower is better.
    Distance,
    /// Hybrid fused score — higher is better.
    Fused,
    /// Raw BM25 — higher is better.
    Bm25,
}

#[derive(Debug, Clone, Serialize)]
pub struct VectorPart {
    /// Cosine distance, `1 - cos`.
    pub distance: f32,
    /// `1 / (1 + distance)` — the higher-is-better mapping hybrid fuses.
    pub similarity: f32,
    pub normalized: Option<f32>,
    /// Fusion weight applied to `normalized` (hybrid only).
    pub weight: Option<f32>,
    pub weighted: Option<f32>,
    pub rank: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TermPart {
    pub term: String,
    pub tf: u32,
    pub df: u32,
    pub idf: f32,
    pub contribution: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct Bm25Part {
    pub score: f32,
    pub normalized: Option<f32>,
    /// Fusion weight applied to `normalized` (hybrid only).
    pub weight: Option<f32>,
    pub weighted: Option<f32>,
    pub rank: Option<usize>,
    pub terms: Vec<TermPart>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilterCheck {
    pub predicate: String,
    pub actual: Value,
    pub passed: bool,
}

/// Why one result is where it is.
#[derive(Debug, Clone, Serialize)]
pub struct HitExplain {
    pub id: String,
    pub rank: usize,
    pub final_score: f32,
    pub score_kind: ScoreKind,
    pub vector: Option<VectorPart>,
    pub bm25: Option<Bm25Part>,
    pub filters: Option<Vec<FilterCheck>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Explain {
    pub kind: ExplainKind,
    /// `true` = the query ran (ANALYZE); `false` = plan-only EXPLAIN.
    pub analyzed: bool,
    pub summary: String,
    pub total_us: u64,
    pub stages: Vec<Stage>,
    pub notes: Vec<Note>,
    pub hits: Vec<HitExplain>,
    /// SQL: the typed plan tree.
    pub plan: Option<Value>,
    /// RAG: the exact prompt sent to the LLM.
    pub prompt: Option<String>,
    /// Postgres-style text rendering, one line per entry.
    pub text: Vec<String>,
}

impl Explain {
    pub fn new(kind: ExplainKind, analyzed: bool) -> Self {
        Self {
            kind,
            analyzed,
            summary: String::new(),
            total_us: 0,
            stages: Vec::new(),
            notes: Vec::new(),
            hits: Vec::new(),
            plan: None,
            prompt: None,
            text: Vec::new(),
        }
    }

    /// Fold a retrieval trace from the index into this explain.
    pub fn absorb(&mut self, trace: SearchTrace) {
        self.stages.extend(trace.stages);
        self.notes.extend(trace.notes);
        self.hits = trace.hits;
    }

    /// Set the summary and total time, and render [`Self::text`].
    pub fn finish(&mut self, summary: impl Into<String>, total: std::time::Duration) {
        self.summary = summary.into();
        self.total_us = total.as_micros() as u64;
        self.text = self.render_text();
    }

    /// Postgres `EXPLAIN ANALYZE`-style lines. Stages print outermost
    /// (last) first with each earlier stage nested beneath, the way a
    /// Postgres plan reads: the root produced the result, its children
    /// fed it.
    fn render_text(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (depth, st) in self.stages.iter().rev().enumerate() {
            let pad = if depth == 0 { String::new() } else { format!("{}->  ", "      ".repeat(depth - 1)) };
            let mut head = format!("{pad}{}", st.label);
            if self.analyzed {
                head.push_str(&format!("  (time={} ", fmt_us(st.took_us)));
                head.push_str(&match (st.rows_in, st.rows_out) {
                    (Some(i), Some(o)) => format!("rows={i}->{o})"),
                    (None, Some(o)) => format!("rows={o})"),
                    _ => "rows=?)".to_string(),
                });
            } else if let Some(o) = st.rows_out {
                head.push_str(&format!("  (rows={o})"));
            }
            out.push(head);
            let body_pad = " ".repeat(pad.len() + 2);
            out.push(format!("{body_pad}{}", st.detail));
        }
        for n in &self.notes {
            let tag = match n.level {
                NoteLevel::Info => "Note",
                NoteLevel::Warn => "Warning",
            };
            out.push(format!("{tag}: {}", n.message));
        }
        if self.analyzed {
            out.push(format!("Execution Time: {}", fmt_us(self.total_us)));
        }
        out
    }
}

/// RAG prompt assembly. `system` / `user` are the prompt's two parts;
/// the rendered text is kept on [`Explain::prompt`] by the caller.
pub fn prompt_stage(system: Option<&str>, user: &str, snippets: usize, took: std::time::Duration) -> Stage {
    let chars = system.map_or(0, str::len) + user.len();
    Stage::new(
        "prompt",
        "Build prompt",
        format!(
            "Put the {snippets} retrieved chunk{} into the prompt as numbered context ahead of the \
             question — {chars} characters (~{} tokens) in all.",
            if snippets == 1 { "" } else { "s" },
            chars / 4
        ),
    )
    .rows(Some(snippets), Some(1))
    .took(took)
    .attr("context_chunks", snippets)
    .attr("chars", chars)
    .attr("approx_tokens", chars / 4)
}

/// Full prompt text as sent: system line, then the user message.
pub fn prompt_text(system: Option<&str>, user: &str) -> String {
    match system {
        Some(s) => format!("[system]\n{s}\n\n[user]\n{user}"),
        None => format!("[user]\n{user}"),
    }
}

/// LLM generation, with time to first token.
pub fn llm_stage(
    model: &str,
    first_token: Option<std::time::Duration>,
    total: std::time::Duration,
    answer_chars: usize,
) -> Stage {
    let ttft = first_token.map_or("no tokens".to_string(), |d| fmt_us(d.as_micros() as u64));
    Stage::new(
        "llm",
        "Generate answer (LLM)",
        format!(
            "'{model}' streamed a {answer_chars}-character answer; first token after {ttft}, \
             finished after {}.",
            fmt_us(total.as_micros() as u64)
        ),
    )
    .rows(Some(1), Some(1))
    .took(total)
    .attr("model", model)
    .attr("answer_chars", answer_chars)
    .attr("time_to_first_token_us", first_token.map(|d| d.as_micros() as u64))
}

/// What the index contributes to an [`Explain`].
#[derive(Debug, Clone, Default)]
pub struct SearchTrace {
    pub stages: Vec<Stage>,
    pub notes: Vec<Note>,
    pub hits: Vec<HitExplain>,
}

pub(crate) fn fmt_us(us: u64) -> String {
    if us >= 1_000_000 {
        format!("{:.2}s", us as f64 / 1e6)
    } else if us >= 1_000 {
        format!("{:.2}ms", us as f64 / 1e3)
    } else {
        format!("{us}µs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_nests_earlier_stages_under_the_last() {
        let mut e = Explain::new(ExplainKind::Sql, true);
        e.stages.push(Stage::new("hnsw", "Vector search (HNSW)", "walked").rows(None, Some(32)));
        e.stages.push(Stage::new("filter", "Filter", "city = 'London'").rows(Some(32), Some(3)));
        e.notes.push(Note::warn("starved"));
        e.finish("done", std::time::Duration::from_micros(1500));
        assert_eq!(e.text[0], "Filter  (time=0µs rows=32->3)");
        assert!(e.text[2].starts_with("->  Vector search (HNSW)"), "{:?}", e.text);
        assert!(e.text.contains(&"Warning: starved".to_string()));
        assert_eq!(e.text.last().unwrap(), "Execution Time: 1.50ms");
    }
}

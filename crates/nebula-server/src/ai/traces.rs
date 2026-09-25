//! In-memory AI request traces for the observability UI.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex as PlMutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

fn next_trace_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!(
        "tr_{}_{}",
        now_ms(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTraceEvent {
    pub name: String,
    pub at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTrace {
    pub id: String,
    pub started_at_ms: u64,
    pub provider: String,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    pub events: Vec<AiTraceEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_latency_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_used: Option<String>,
    /// Redacted by default in API responses when `redact` is true.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_preview: Option<String>,
}

impl AiTrace {
    pub fn new(provider: &str, model: &str) -> Self {
        Self {
            id: next_trace_id(),
            started_at_ms: now_ms(),
            provider: provider.into(),
            model: model.into(),
            task: None,
            events: Vec::new(),
            prompt_tokens: None,
            completion_tokens: None,
            total_latency_ms: None,
            error: None,
            fallback_used: None,
            prompt_preview: None,
        }
    }

    pub fn push(&mut self, name: impl Into<String>, detail: Option<serde_json::Value>) {
        self.events.push(AiTraceEvent {
            name: name.into(),
            at_ms: now_ms(),
            detail,
        });
    }

    pub fn finish(&mut self) {
        self.total_latency_ms = Some(now_ms().saturating_sub(self.started_at_ms));
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct TraceStore {
    inner: PlMutex<VecDeque<AiTrace>>,
    capacity: usize,
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new(200)
    }
}

impl TraceStore {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: PlMutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    pub fn record(&self, mut trace: AiTrace) {
        trace.finish();
        let mut q = self.inner.lock();
        if q.len() >= self.capacity {
            q.pop_front();
        }
        q.push_back(trace);
    }

    pub fn list(&self, limit: usize) -> Vec<AiTrace> {
        let q = self.inner.lock();
        q.iter().rev().take(limit).cloned().collect()
    }

    pub fn get(&self, id: &str) -> Option<AiTrace> {
        self.inner.lock().iter().find(|t| t.id == id).cloned()
    }
}

// Silence unused Mutex import if we switch — keep parking_lot only.
#[allow(dead_code)]
type _StdMutex<T> = Mutex<T>;

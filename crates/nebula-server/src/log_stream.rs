//! Live log streaming + small recent-lines ring buffer.
//!
//! The server ships a hand-rolled `tracing::Subscriber` rather than
//! depending on `tracing-subscriber` — see the top of `main.rs` for
//! the rationale. This module upgrades that noop into a capturing
//! subscriber that:
//!
//! 1. Serializes each `tracing::Event` into a [`LogEvent`].
//! 2. Pushes the event through a [`tokio::sync::broadcast`] so SSE
//!    subscribers get it live.
//! 3. Keeps the last N events in a ring so a late connecter sees
//!    recent context (the kubectl-logs experience — you connect
//!    *after* something went wrong and still see the trailing
//!    lines that explain it).
//!
//! # What we do NOT capture
//!
//! No span attributes, no fields other than `message`, no parent
//! chain. That's deliberate: the consumer is an operator eyeballing
//! live output, not a structured-logs pipeline. If a richer shape
//! becomes necessary later, adding it is additive (new optional
//! fields on [`LogEvent`]).
//!
//! # Performance
//!
//! `enabled()` returns `true` for everything. A release build with
//! spammy DEBUG logs will push a lot of events through this;
//! tracing's macro-level filter (`RUST_LOG`) isn't respected here
//! because we don't want to pull in `tracing-subscriber::EnvFilter`.
//! If this becomes a problem, `LogBus::set_min_level` below gives
//! the operator a runtime knob (defaulting to INFO).

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::{field::Visit, span, Event, Metadata, Subscriber};

/// Severity, matching `tracing::Level` but serializable and
/// comparable with a cheap u8. We don't re-export `tracing::Level`
/// on the wire because its `Display` is non-stable (`"INFO"` not
/// `"info"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl LogLevel {
    pub fn from_tracing(level: &tracing::Level) -> Self {
        match *level {
            tracing::Level::TRACE => Self::Trace,
            tracing::Level::DEBUG => Self::Debug,
            tracing::Level::INFO => Self::Info,
            tracing::Level::WARN => Self::Warn,
            tracing::Level::ERROR => Self::Error,
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "trace" => Some(Self::Trace),
            "debug" => Some(Self::Debug),
            "info" => Some(Self::Info),
            "warn" | "warning" => Some(Self::Warn),
            "error" => Some(Self::Error),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        self as u8
    }

    fn from_u8(b: u8) -> Self {
        match b {
            0 => Self::Trace,
            1 => Self::Debug,
            2 => Self::Info,
            3 => Self::Warn,
            _ => Self::Error,
        }
    }
}

/// One log line on the wire.
#[derive(Clone, Debug, Serialize)]
pub struct LogEvent {
    /// Unix millis. Monotonic-ish per process but not across
    /// nodes — the UI shows the relative gap, not absolute time.
    pub ts_ms: u64,
    pub level: LogLevel,
    /// `module_path` when available, else empty. Lets operators
    /// filter to a subsystem (`nebula_grpc`) without grepping
    /// free-text messages.
    pub target: String,
    /// The rendered `message` field. Other fields are ignored —
    /// see the module docs.
    pub message: String,
}

/// Broadcast channel capacity. 1024 ≈ a couple hundred KB of text;
/// a slow SSE consumer tolerates a few seconds of bursty writes
/// before dropping with `Lagged`.
const BROADCAST_CAPACITY: usize = 1024;

/// Ring-buffer size for late connecters. Small enough that boot
/// plus the first minute fits; large enough that a reconnect
/// after a transient network blip doesn't leave the operator
/// staring at a blank terminal. Tuneable via `LogBus::new` if
/// that ever matters.
const DEFAULT_RING_SIZE: usize = 256;

pub struct LogBus {
    tx: broadcast::Sender<LogEvent>,
    /// Last N events, most-recent at the end. Protected by a
    /// small `parking_lot::Mutex` — lock hold time is O(1) (push
    /// + optional pop) so contention is negligible.
    ring: Mutex<std::collections::VecDeque<LogEvent>>,
    ring_cap: usize,
    /// Events below this level are discarded at the subscriber
    /// layer, never reaching the broadcast or the ring. Stored
    /// as an AtomicU8 so it's lock-free on the hot path.
    min_level: AtomicU8,
}

impl std::fmt::Debug for LogBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogBus")
            .field("ring_cap", &self.ring_cap)
            .field("min_level", &self.min_level())
            .finish()
    }
}

impl Default for LogBus {
    fn default() -> Self {
        Self::new(DEFAULT_RING_SIZE, LogLevel::Info)
    }
}

impl LogBus {
    pub fn new(ring_cap: usize, min_level: LogLevel) -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            tx,
            ring: Mutex::new(std::collections::VecDeque::with_capacity(ring_cap)),
            ring_cap,
            min_level: AtomicU8::new(min_level.to_u8()),
        }
    }

    pub fn min_level(&self) -> LogLevel {
        LogLevel::from_u8(self.min_level.load(Ordering::Relaxed))
    }

    pub fn set_min_level(&self, level: LogLevel) {
        self.min_level.store(level.to_u8(), Ordering::Relaxed);
    }

    /// Subscribe to the live stream. The returned receiver starts
    /// at "now" — historical events come from [`snapshot`].
    pub fn subscribe(&self) -> broadcast::Receiver<LogEvent> {
        self.tx.subscribe()
    }

    /// Snapshot of the ring buffer — the last N events that
    /// passed `min_level`. Cheap to copy; used once per SSE
    /// connection to prime the client before the live stream.
    pub fn snapshot(&self) -> Vec<LogEvent> {
        self.ring.lock().iter().cloned().collect()
    }

    /// Test-only: inject an event as if the subscriber had captured
    /// it. Lets integration tests exercise the SSE endpoint without
    /// depending on `tracing::info!` being wired up.
    pub fn push_for_test(&self, e: LogEvent) {
        self.push(e);
    }

    /// Called by the capturing subscriber. Silently drops events
    /// below `min_level`.
    fn push(&self, e: LogEvent) {
        if e.level < self.min_level() {
            return;
        }
        {
            let mut ring = self.ring.lock();
            if ring.len() == self.ring_cap {
                ring.pop_front();
            }
            ring.push_back(e.clone());
        }
        let _ = self.tx.send(e);
    }
}

/// `tracing::Subscriber` that pushes every event into a [`LogBus`].
/// Replaces the noop subscriber the server used before.
///
/// We implement a minimal subscriber rather than using
/// `tracing-subscriber` because the binary deliberately avoids
/// that dep (see `main.rs`). The trait surface is small; spans
/// are accepted (to keep child `tracing::info_span!` calls from
/// panicking) but carry no state.
pub struct CapturingSubscriber {
    bus: Arc<LogBus>,
}

impl CapturingSubscriber {
    pub fn new(bus: Arc<LogBus>) -> Self {
        Self { bus }
    }
}

impl Subscriber for CapturingSubscriber {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        // Accept everything at the tracing layer; level filtering
        // happens inside LogBus::push so a runtime change via
        // set_min_level takes effect without rewiring.
        let _ = metadata;
        true
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        // We don't use span state; any non-zero id satisfies the
        // contract. The same id reused for every span is fine — we
        // never read it back.
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        let meta = event.metadata();
        let level = LogLevel::from_tracing(meta.level());
        // Fast-path reject: don't pay the field-visit cost if the
        // event will be dropped downstream anyway.
        if level < self.bus.min_level() {
            return;
        }
        let mut visitor = MessageVisitor {
            buf: String::new(),
        };
        event.record(&mut visitor);
        let message = if visitor.buf.is_empty() {
            // Structured-only events with no `message` field do
            // happen. Better to surface "(no message)" than drop
            // the event entirely — the target still tells the
            // operator what module fired.
            "(no message)".to_string()
        } else {
            visitor.buf
        };
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        self.bus.push(LogEvent {
            ts_ms,
            level,
            target: meta.target().to_string(),
            message,
        });
    }

    fn enter(&self, _: &span::Id) {}
    fn exit(&self, _: &span::Id) {}
}

/// Visit that pulls out just the `message` field. Other fields
/// are dropped (see module docs). `message` is the field `info!`,
/// `warn!` etc. synthesize from their format string.
struct MessageVisitor {
    buf: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            // `{:?}` on `&str` adds quotes; `Display` isn't
            // guaranteed. Strip surrounding quotes if the Debug
            // output starts/ends with them for the common case.
            let rendered = format!("{value:?}");
            let cleaned = rendered
                .strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(&rendered);
            let _ = self.buf.write_str(cleaned);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_evicts_oldest_at_capacity() {
        let bus = LogBus::new(3, LogLevel::Trace);
        for i in 0..5 {
            bus.push(LogEvent {
                ts_ms: i,
                level: LogLevel::Info,
                target: "t".into(),
                message: format!("m{i}"),
            });
        }
        let snap = bus.snapshot();
        assert_eq!(snap.len(), 3);
        // Oldest two evicted; we keep m2, m3, m4.
        assert_eq!(snap[0].message, "m2");
        assert_eq!(snap[2].message, "m4");
    }

    #[test]
    fn min_level_filters_at_push() {
        let bus = LogBus::new(16, LogLevel::Warn);
        bus.push(LogEvent {
            ts_ms: 1,
            level: LogLevel::Info,
            target: "t".into(),
            message: "info-line".into(),
        });
        bus.push(LogEvent {
            ts_ms: 2,
            level: LogLevel::Error,
            target: "t".into(),
            message: "error-line".into(),
        });
        let snap = bus.snapshot();
        assert_eq!(snap.len(), 1, "INFO should be filtered below Warn");
        assert_eq!(snap[0].message, "error-line");
    }

    #[test]
    fn set_min_level_applies_live() {
        let bus = LogBus::new(16, LogLevel::Info);
        bus.push(LogEvent {
            ts_ms: 1,
            level: LogLevel::Debug,
            target: "t".into(),
            message: "dropped".into(),
        });
        bus.set_min_level(LogLevel::Debug);
        bus.push(LogEvent {
            ts_ms: 2,
            level: LogLevel::Debug,
            target: "t".into(),
            message: "kept".into(),
        });
        let snap = bus.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].message, "kept");
    }

    #[test]
    fn parse_level() {
        assert_eq!(LogLevel::parse("info"), Some(LogLevel::Info));
        assert_eq!(LogLevel::parse("WARN"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("warning"), Some(LogLevel::Warn));
        assert_eq!(LogLevel::parse("nope"), None);
    }
}

//! Workload classes + class-aware admission control (design 0010 §6).
//!
//! Every API request is classified by what it costs and what it's
//! worth, and each class gets a concurrency budget (max in-flight)
//! derived from the worker count and the current operating mode.
//! Under CPU pressure the cheap-to-refuse classes (LLM-bound RAG,
//! then search) shrink first; `Critical` (mutations, replication,
//! admin control plane) is never refused on resource grounds — the
//! flat rate limiter still handles abuse.
//!
//! The budgets are backstops, sized so a healthy node never touches
//! them: their job is to stop a flood of expensive analytics-class
//! requests from starving writes when the CPU is already saturated,
//! converting "everything is slow" into "cheap-to-refuse requests get
//! a clean 429 + Retry-After while transactional traffic flows".
//!
//! In-flight tracking is a per-class atomic counter with an RAII
//! permit — no semaphore wait, no queueing. If the class is over
//! budget the request is refused immediately; queueing under
//! saturation only converts overload into latency and memory growth.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::{header, Method, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;

use nebula_resource::OperatingMode;

/// Workload classes, ordered by importance (design 0010 §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadClass {
    /// Mutations, replication control, admin/ops actions. Never
    /// admission-refused — refusing a write to save CPU inverts the
    /// priority order the whole design exists to protect.
    Critical,
    /// Interactive SQL and direct document reads.
    High,
    /// Document/vector search — interactive but re-issuable.
    Medium,
    /// LLM-bound RAG answers — the most expensive request the server
    /// can serve and the cheapest to ask the client to retry.
    Low,
}

impl WorkloadClass {
    pub fn as_str(self) -> &'static str {
        match self {
            WorkloadClass::Critical => "critical",
            WorkloadClass::High => "high",
            WorkloadClass::Medium => "medium",
            WorkloadClass::Low => "low",
        }
    }
}

/// Classify a request from its method + nest-stripped path. Anything
/// unrecognized is `Critical` — misclassifying an unknown route as
/// sheddable would silently break whatever ships next; the safe
/// default is "don't shed".
pub fn classify(method: &Method, path: &str) -> WorkloadClass {
    let p = path.strip_prefix("/api/v1").unwrap_or(path);
    // LLM-bound answer paths: the expensive tail.
    if p == "/ai/rag" || p == "/rag/answer" {
        return WorkloadClass::Low;
    }
    // Search: interactive but re-issuable.
    if p == "/ai/search" || p == "/vector/search" {
        return WorkloadClass::Medium;
    }
    // SQL is SELECT-only (POST is just the body carrier) and direct
    // doc GETs are point reads.
    if p == "/query" || p == "/query/explain" {
        return WorkloadClass::High;
    }
    if (method == Method::GET || method == Method::HEAD) && p.starts_with("/bucket/") {
        return WorkloadClass::High;
    }
    // Mutations, admin, replication, everything else.
    WorkloadClass::Critical
}

/// Per-class concurrency budgets. `None` = unlimited.
#[derive(Debug, Clone, Copy)]
struct Budgets {
    high: i64,
    medium: i64,
    low: i64,
}

/// Derive budgets from the worker count and the operating mode.
/// Multipliers are deliberately generous — these fire under floods,
/// not under normal load. Under CPU (or worse) pressure the Low class
/// shrinks ~8x and Medium ~4x, freeing workers for Critical/High.
fn budgets_for(workers: usize, mode: OperatingMode) -> Budgets {
    let w = workers.max(2) as i64;
    match mode {
        OperatingMode::Normal => Budgets {
            high: 64 * w,
            medium: 32 * w,
            low: 16 * w,
        },
        // Any pressure mode: shed the expensive classes first. Memory
        // and disk pressure also imply the node needs headroom — the
        // shrunken budgets cost nothing when traffic is light.
        _ => Budgets {
            high: 32 * w,
            medium: 8 * w,
            low: 2 * w,
        },
    }
}

/// Class-aware admission controller. One per server; cheap atomics on
/// the request path (one classify + one CAS-free fetch_add per
/// admitted request).
pub struct AdmissionController {
    workers: usize,
    resource: Arc<nebula_resource::ResourceManager>,
    in_flight_high: AtomicI64,
    in_flight_medium: AtomicI64,
    in_flight_low: AtomicI64,
    rejected_high: AtomicU64,
    rejected_medium: AtomicU64,
    rejected_low: AtomicU64,
}

impl AdmissionController {
    pub fn new(workers: usize, resource: Arc<nebula_resource::ResourceManager>) -> Self {
        Self {
            workers,
            resource,
            in_flight_high: AtomicI64::new(0),
            in_flight_medium: AtomicI64::new(0),
            in_flight_low: AtomicI64::new(0),
            rejected_high: AtomicU64::new(0),
            rejected_medium: AtomicU64::new(0),
            rejected_low: AtomicU64::new(0),
        }
    }

    fn slot(&self, class: WorkloadClass) -> Option<(&AtomicI64, &AtomicU64, i64)> {
        let b = budgets_for(self.workers, self.resource.mode());
        match class {
            WorkloadClass::Critical => None,
            WorkloadClass::High => Some((&self.in_flight_high, &self.rejected_high, b.high)),
            WorkloadClass::Medium => {
                Some((&self.in_flight_medium, &self.rejected_medium, b.medium))
            }
            WorkloadClass::Low => Some((&self.in_flight_low, &self.rejected_low, b.low)),
        }
    }

    /// Try to admit a request of `class`.
    pub fn try_admit(self: &Arc<Self>, class: WorkloadClass) -> AdmitDecision {
        let Some((counter, rejected, limit)) = self.slot(class) else {
            return AdmitDecision::Unlimited;
        };
        // fetch_add first, undo on over-budget: two relaxed RMWs in
        // the refusal path, one in the admit path. Brief overshoot by
        // concurrent racers is bounded by the racer count and
        // harmless for a backstop.
        let prev = counter.fetch_add(1, Ordering::Relaxed);
        if prev >= limit {
            counter.fetch_sub(1, Ordering::Relaxed);
            rejected.fetch_add(1, Ordering::Relaxed);
            return AdmitDecision::Refused;
        }
        AdmitDecision::Admitted(Permit {
            controller: Arc::clone(self),
            class,
        })
    }

    fn release(&self, class: WorkloadClass) {
        if let Some((counter, _, _)) = self.slot(class) {
            counter.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Snapshot for `/metrics` and `/admin/reliability`:
    /// `(class, in_flight, budget, rejected_total)` per shed-able class.
    pub fn snapshot(&self) -> Vec<ClassStats> {
        let b = budgets_for(self.workers, self.resource.mode());
        vec![
            ClassStats {
                class: WorkloadClass::High,
                in_flight: self.in_flight_high.load(Ordering::Relaxed).max(0) as u64,
                budget: b.high as u64,
                rejected_total: self.rejected_high.load(Ordering::Relaxed),
            },
            ClassStats {
                class: WorkloadClass::Medium,
                in_flight: self.in_flight_medium.load(Ordering::Relaxed).max(0) as u64,
                budget: b.medium as u64,
                rejected_total: self.rejected_medium.load(Ordering::Relaxed),
            },
            ClassStats {
                class: WorkloadClass::Low,
                in_flight: self.in_flight_low.load(Ordering::Relaxed).max(0) as u64,
                budget: b.low as u64,
                rejected_total: self.rejected_low.load(Ordering::Relaxed),
            },
        ]
    }
}

#[derive(Debug, Serialize)]
pub struct ClassStats {
    pub class: WorkloadClass,
    pub in_flight: u64,
    pub budget: u64,
    pub rejected_total: u64,
}

/// Outcome of an admission attempt. `Unlimited` = Critical class, no
/// permit needed; `Admitted` carries the RAII permit; `Refused` = 429.
pub enum AdmitDecision {
    Unlimited,
    Admitted(Permit),
    Refused,
}

impl AdmitDecision {
    /// True unless the request was refused.
    pub fn is_admitted(&self) -> bool {
        !matches!(self, AdmitDecision::Refused)
    }
}

/// RAII admission permit — releases the class slot on drop, which the
/// middleware ties to the response completing (including panics and
/// client disconnects, since the future is dropped either way).
pub struct Permit {
    controller: Arc<AdmissionController>,
    class: WorkloadClass,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.controller.release(self.class);
    }
}

/// Admission middleware. Sits inside the rate limiter (abuse control
/// stays outermost) and outside auth — a saturated class refuses
/// before burning auth CPU. `None` controller (tests/dev) admits all.
pub async fn admission_control(
    State(state): State<crate::state::AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(controller) = state.admission.as_ref() else {
        return next.run(req).await;
    };
    let class = classify(req.method(), req.uri().path());
    match controller.try_admit(class) {
        // The decision (holding the permit, when one was issued)
        // lives across the downstream call and releases on drop.
        d @ (AdmitDecision::Unlimited | AdmitDecision::Admitted(_)) => {
            let _permit = d;
            next.run(req).await
        }
        AdmitDecision::Refused => {
            tracing::debug!(
                class = class.as_str(),
                "admission refused: class over budget"
            );
            (
                StatusCode::TOO_MANY_REQUESTS,
                [
                    (header::CONTENT_TYPE, "application/json"),
                    (header::RETRY_AFTER, "2"),
                ],
                format!(
                    r#"{{"error":{{"code":"class_saturated","message":"too many concurrent {} requests; retry shortly"}}}}"#,
                    class.as_str()
                ),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_resource::{ResourceManager, ResourceSample, Thresholds};

    fn manager() -> Arc<ResourceManager> {
        Arc::new(ResourceManager::new(Thresholds::default()))
    }

    /// Drive a manager into CpuPressure with debounced saturated samples.
    fn cpu_pressured() -> Arc<ResourceManager> {
        let m = manager();
        let hot = ResourceSample {
            cpu_ratio: Some(0.99),
            ..Default::default()
        };
        m.observe(hot);
        m.observe(hot);
        assert_eq!(m.mode(), nebula_resource::OperatingMode::CpuPressure);
        m
    }

    #[test]
    fn classify_maps_the_api_surface() {
        use WorkloadClass::*;
        let post = Method::POST;
        let get = Method::GET;
        assert_eq!(classify(&post, "/api/v1/bucket/b/doc"), Critical);
        assert_eq!(classify(&post, "/api/v1/admin/promote"), Critical);
        assert_eq!(classify(&get, "/api/v1/bucket/b/doc/x"), High);
        assert_eq!(classify(&post, "/api/v1/query"), High);
        assert_eq!(classify(&post, "/query/explain"), High);
        assert_eq!(classify(&post, "/api/v1/ai/search"), Medium);
        assert_eq!(classify(&post, "/vector/search"), Medium);
        assert_eq!(classify(&post, "/api/v1/ai/rag"), Low);
        assert_eq!(classify(&post, "/rag/answer"), Low);
        // Unknown routes default to Critical (never shed by accident).
        assert_eq!(classify(&post, "/api/v1/some/new/route"), Critical);
    }

    #[test]
    fn critical_is_never_refused() {
        let c = Arc::new(AdmissionController::new(2, manager()));
        for _ in 0..100_000 {
            assert!(matches!(
                c.try_admit(WorkloadClass::Critical),
                AdmitDecision::Unlimited
            ));
        }
    }

    #[test]
    fn low_class_hits_budget_and_permits_release() {
        let c = Arc::new(AdmissionController::new(2, manager()));
        // Normal-mode Low budget = 16 * 2 = 32.
        let permits: Vec<_> = (0..32).map(|_| c.try_admit(WorkloadClass::Low)).collect();
        assert!(permits.iter().all(|d| d.is_admitted()));
        assert!(
            !c.try_admit(WorkloadClass::Low).is_admitted(),
            "33rd must refuse"
        );
        assert_eq!(c.snapshot()[2].rejected_total, 1);
        drop(permits);
        assert!(
            c.try_admit(WorkloadClass::Low).is_admitted(),
            "slots freed on drop"
        );
    }

    #[test]
    fn cpu_pressure_shrinks_low_budget() {
        let c = Arc::new(AdmissionController::new(2, cpu_pressured()));
        // Pressured Low budget = 2 * 2 = 4.
        let _permits: Vec<_> = (0..4).map(|_| c.try_admit(WorkloadClass::Low)).collect();
        assert!(_permits.iter().all(|d| d.is_admitted()));
        assert!(!c.try_admit(WorkloadClass::Low).is_admitted());
        // High shrinks less aggressively: 32 * 2 = 64 still available.
        let stats = c.snapshot();
        assert_eq!(stats[0].budget, 64);
        assert_eq!(stats[2].budget, 4);
    }

    #[test]
    fn budgets_recover_with_mode() {
        let m = cpu_pressured();
        let c = Arc::new(AdmissionController::new(2, Arc::clone(&m)));
        assert_eq!(c.snapshot()[2].budget, 4);
        let idle = ResourceSample {
            cpu_ratio: Some(0.10),
            ..Default::default()
        };
        m.observe(idle);
        m.observe(idle);
        assert_eq!(c.snapshot()[2].budget, 32, "budget restored on recovery");
    }
}

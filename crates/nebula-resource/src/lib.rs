//! Resource manager and operating-mode state machine (design 0010).
//!
//! The manager turns raw OS readings (cgroup memory, statvfs disk,
//! process CPU time) into per-resource [`PressureLevel`]s and one
//! authoritative [`OperatingMode`], with hysteresis and debounce so a
//! single anomalous reading can never flip the node's behavior.
//!
//! Split of responsibilities:
//! - [`probe::Prober`] does the blocking OS reads (call from
//!   `spawn_blocking`).
//! - [`derive_levels`] / [`derive_mode`] are pure functions over a
//!   sample — the whole policy is unit-testable without an OS.
//! - [`ResourceManager`] owns the debounced state and publishes it
//!   through atomics so hot-path consumers (the write gate reads mode
//!   on every mutation) pay one `Relaxed` load, never a lock.

pub mod probe;

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;

pub use probe::{Prober, ResourceSample};

/// Pressure on one resource dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PressureLevel {
    Ok = 0,
    High = 1,
    Critical = 2,
}

/// The node's operating mode, ordered by severity: when several
/// pressures apply the most severe wins (design 0010 §3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatingMode {
    Normal = 0,
    CpuPressure = 1,
    MemoryPressure = 2,
    DiskPressure = 3,
    /// Mutations are refused (503) — a half-written WAL frame on a
    /// full volume is an incident; a refused write is a retry.
    DiskCritical = 4,
}

impl OperatingMode {
    pub fn as_str(self) -> &'static str {
        match self {
            OperatingMode::Normal => "normal",
            OperatingMode::CpuPressure => "cpu_pressure",
            OperatingMode::MemoryPressure => "memory_pressure",
            OperatingMode::DiskPressure => "disk_pressure",
            OperatingMode::DiskCritical => "disk_critical",
        }
    }

    fn from_u8(v: u8) -> Self {
        match v {
            1 => OperatingMode::CpuPressure,
            2 => OperatingMode::MemoryPressure,
            3 => OperatingMode::DiskPressure,
            4 => OperatingMode::DiskCritical,
            _ => OperatingMode::Normal,
        }
    }
}

/// Watermarks. Every pair is (enter, exit) with enter > exit so a
/// level engages at the high mark and only releases below the low
/// mark — the gap is the hysteresis band that prevents flapping when
/// a reading hovers at a threshold.
#[derive(Debug, Clone)]
pub struct Thresholds {
    /// Memory used/limit ratio to enter High. Matches the existing
    /// NebulaMemoryNearCap alert at 85%.
    pub memory_high_enter: f64,
    pub memory_high_exit: f64,
    /// Matches NebulaMemoryCritical at 93%.
    pub memory_critical_enter: f64,
    pub memory_critical_exit: f64,
    /// Disk free/total ratio to enter High (i.e. free fraction AT or
    /// BELOW this engages).
    pub disk_high_enter: f64,
    pub disk_high_exit: f64,
    pub disk_critical_enter: f64,
    pub disk_critical_exit: f64,
    /// Absolute free-bytes floor that also forces disk Critical: the
    /// node must always keep room for one more WAL segment plus a
    /// snapshot rename. Guards small volumes where 3% is huge and big
    /// volumes where 3% masks a nearly-full disk.
    pub disk_critical_free_bytes: u64,
    /// Process CPU ratio (of all cores) to enter High.
    pub cpu_high_enter: f64,
    pub cpu_high_exit: f64,
    /// Consecutive samples a NEW level must hold before the manager
    /// commits the transition.
    pub debounce_samples: u32,
}

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            memory_high_enter: 0.85,
            memory_high_exit: 0.78,
            memory_critical_enter: 0.93,
            memory_critical_exit: 0.88,
            disk_high_enter: 0.10,
            disk_high_exit: 0.15,
            disk_critical_enter: 0.03,
            disk_critical_exit: 0.06,
            // 2 x the default 64 MiB WAL segment.
            disk_critical_free_bytes: 128 * 1024 * 1024,
            cpu_high_enter: 0.90,
            cpu_high_exit: 0.75,
            debounce_samples: 2,
        }
    }
}

/// Per-resource pressure derived from one sample. Pure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PressureLevels {
    pub memory: Option<PressureLevel>,
    pub disk: Option<PressureLevel>,
    pub cpu: Option<PressureLevel>,
}

/// Classify one reading against (enter, exit) watermarks, carrying the
/// previous level for hysteresis. `higher_is_worse` is true for
/// used-fraction style readings (memory, cpu) and false for
/// free-fraction readings (disk).
fn classify(
    value: f64,
    prev: PressureLevel,
    enter_high: f64,
    exit_high: f64,
    enter_critical: Option<f64>,
    exit_critical: Option<f64>,
    higher_is_worse: bool,
) -> PressureLevel {
    // Normalize so "worse" is always numerically higher.
    let v = if higher_is_worse { value } else { -value };
    let flip = |t: f64| if higher_is_worse { t } else { -t };

    // Critical engages at the enter mark, or holds inside the
    // (exit, enter) hysteresis band when it was already engaged.
    let crit = match (enter_critical, exit_critical) {
        (Some(en), Some(ex)) => v >= flip(en) || (prev == PressureLevel::Critical && v >= flip(ex)),
        _ => false,
    };
    if crit {
        return PressureLevel::Critical;
    }
    if v >= flip(enter_high) {
        return PressureLevel::High;
    }
    if prev >= PressureLevel::High && v >= flip(exit_high) {
        return PressureLevel::High; // inside the high hysteresis band
    }
    PressureLevel::Ok
}

/// Derive per-resource levels from one sample. `prev` supplies the
/// hysteresis memory. Missing readings yield `None` (no signal), never
/// a pressure level.
pub fn derive_levels(s: &ResourceSample, prev: &PressureLevels, t: &Thresholds) -> PressureLevels {
    let memory = match (s.memory_used, s.memory_limit) {
        (Some(used), Some(limit)) if limit > 0 => Some(classify(
            used as f64 / limit as f64,
            prev.memory.unwrap_or(PressureLevel::Ok),
            t.memory_high_enter,
            t.memory_high_exit,
            Some(t.memory_critical_enter),
            Some(t.memory_critical_exit),
            true,
        )),
        _ => None,
    };
    let disk = match (s.disk_free, s.disk_total) {
        (Some(free), Some(total)) if total > 0 => {
            let mut level = classify(
                free as f64 / total as f64,
                prev.disk.unwrap_or(PressureLevel::Ok),
                t.disk_high_enter,
                t.disk_high_exit,
                Some(t.disk_critical_enter),
                Some(t.disk_critical_exit),
                false,
            );
            // Absolute floor overrides the fraction: nearly out of
            // bytes is critical no matter how big the volume is.
            if free <= t.disk_critical_free_bytes {
                level = PressureLevel::Critical;
            }
            Some(level)
        }
        _ => None,
    };
    let cpu = s.cpu_ratio.map(|r| {
        classify(
            r,
            prev.cpu.unwrap_or(PressureLevel::Ok),
            t.cpu_high_enter,
            t.cpu_high_exit,
            None,
            None,
            true,
        )
    });
    PressureLevels { memory, disk, cpu }
}

/// Collapse per-resource levels into the single mode. Severity order
/// per design 0010 §3; CPU has no critical rung (a saturated CPU still
/// makes progress — it deprioritizes, never gates).
pub fn derive_mode(levels: &PressureLevels) -> OperatingMode {
    match levels.disk {
        Some(PressureLevel::Critical) => return OperatingMode::DiskCritical,
        Some(PressureLevel::High) => return OperatingMode::DiskPressure,
        _ => {}
    }
    if levels.memory.is_some_and(|l| l >= PressureLevel::High) {
        return OperatingMode::MemoryPressure;
    }
    if levels.cpu.is_some_and(|l| l >= PressureLevel::High) {
        return OperatingMode::CpuPressure;
    }
    OperatingMode::Normal
}

/// One committed mode transition, kept in a bounded ring for
/// `/admin/reliability`.
#[derive(Debug, Clone, Serialize)]
pub struct ModeTransition {
    pub at_ms: u64,
    pub from: OperatingMode,
    pub to: OperatingMode,
    /// Human-readable cause, e.g. `disk free 2.1% (min 3%)`.
    pub cause: String,
}

const TRANSITION_RING_CAP: usize = 64;

/// Debounced state behind the mutex; only the sampler touches it.
struct ManagerInner {
    levels: PressureLevels,
    last_sample: ResourceSample,
    /// Candidate mode observed on recent samples and how many
    /// consecutive samples it has held.
    candidate: OperatingMode,
    candidate_streak: u32,
    transitions: Vec<ModeTransition>,
}

/// A component that adapts to operating-mode changes (design 0010
/// §6): shrink a cache, pause a background job, reduce concurrency.
/// Fired synchronously from the sampler thread on every COMMITTED
/// transition (post debounce), so implementations must be quick and
/// non-blocking — flip an atomic, resize under a short lock; heavy
/// work belongs on the component's own schedule.
pub trait Actuator: Send + Sync {
    /// Short name for the transition log.
    fn name(&self) -> &str;
    fn on_mode_change(&self, from: OperatingMode, to: OperatingMode);
}

/// Shared resource manager. Cheap to clone via `Arc`; readers use the
/// lock-free accessors, only [`ResourceManager::observe`] (the 5s
/// sampler) takes the mutex.
pub struct ResourceManager {
    thresholds: Thresholds,
    mode: AtomicU8,
    transitions_total: AtomicU64,
    inner: Mutex<ManagerInner>,
    /// Registered actuators, notified on committed transitions.
    /// Registration happens once at boot before the sampler starts;
    /// the mutex is uncontended after that.
    actuators: Mutex<Vec<std::sync::Arc<dyn Actuator>>>,
}

impl ResourceManager {
    pub fn new(thresholds: Thresholds) -> Self {
        Self {
            thresholds,
            mode: AtomicU8::new(OperatingMode::Normal as u8),
            transitions_total: AtomicU64::new(0),
            inner: Mutex::new(ManagerInner {
                levels: PressureLevels::default(),
                last_sample: ResourceSample::default(),
                candidate: OperatingMode::Normal,
                candidate_streak: 0,
                transitions: Vec::new(),
            }),
            actuators: Mutex::new(Vec::new()),
        }
    }

    /// Register an adaptive component. If the manager is already in a
    /// degraded mode the actuator is immediately synced to it, so
    /// boot-order races can't leave a component tuned for `Normal`
    /// while the node is under pressure.
    pub fn register_actuator(&self, actuator: std::sync::Arc<dyn Actuator>) {
        let mode = self.mode();
        if mode != OperatingMode::Normal {
            actuator.on_mode_change(OperatingMode::Normal, mode);
        }
        self.actuators.lock().push(actuator);
    }

    /// Current mode. One relaxed atomic load — safe on every hot path.
    pub fn mode(&self) -> OperatingMode {
        OperatingMode::from_u8(self.mode.load(Ordering::Relaxed))
    }

    /// True when mutations must be refused (design 0010 §3).
    pub fn writes_gated(&self) -> bool {
        self.mode() == OperatingMode::DiskCritical
    }

    pub fn transitions_total(&self) -> u64 {
        self.transitions_total.load(Ordering::Relaxed)
    }

    /// Feed one sample through hysteresis + debounce, committing a mode
    /// change only after the new mode holds for `debounce_samples`
    /// consecutive observations. Returns the (possibly unchanged) mode.
    pub fn observe(&self, sample: ResourceSample) -> OperatingMode {
        let mut inner = self.inner.lock();
        inner.levels = derive_levels(&sample, &inner.levels, &self.thresholds);
        inner.last_sample = sample;
        let target = derive_mode(&inner.levels);
        let current = self.mode();

        if target == current {
            // Back to agreement — drop any half-built streak.
            inner.candidate = current;
            inner.candidate_streak = 0;
            return current;
        }
        if target == inner.candidate {
            inner.candidate_streak += 1;
        } else {
            inner.candidate = target;
            inner.candidate_streak = 1;
        }
        if inner.candidate_streak < self.thresholds.debounce_samples {
            return current;
        }

        // Commit the transition.
        let cause = describe_cause(&inner.levels, &inner.last_sample, target);
        self.mode.store(target as u8, Ordering::Relaxed);
        self.transitions_total.fetch_add(1, Ordering::Relaxed);
        if target > current {
            tracing::warn!(from = current.as_str(), to = target.as_str(), %cause, "operating mode degraded");
        } else {
            tracing::info!(from = current.as_str(), to = target.as_str(), %cause, "operating mode recovered");
        }
        if inner.transitions.len() >= TRANSITION_RING_CAP {
            inner.transitions.remove(0);
        }
        inner.transitions.push(ModeTransition {
            at_ms: now_ms(),
            from: current,
            to: target,
            cause,
        });
        inner.candidate_streak = 0;
        // Notify actuators outside the state mutex (they may log or
        // take their own locks) but still on the sampler's thread —
        // transitions are rare and actuators are required to be quick.
        drop(inner);
        for a in self.actuators.lock().iter() {
            tracing::debug!(
                actuator = a.name(),
                from = current.as_str(),
                to = target.as_str(),
                "actuating"
            );
            a.on_mode_change(current, target);
        }
        target
    }

    /// Snapshot for `/admin/reliability` and `/metrics`: current
    /// levels, last raw sample, recent transitions.
    pub fn snapshot(&self) -> ReliabilitySnapshot {
        let inner = self.inner.lock();
        ReliabilitySnapshot {
            mode: self.mode(),
            levels: inner.levels,
            sample: inner.last_sample,
            transitions_total: self.transitions_total(),
            recent_transitions: inner.transitions.clone(),
        }
    }
}

/// Point-in-time view of the manager for observability endpoints.
#[derive(Debug, Clone, Serialize)]
pub struct ReliabilitySnapshot {
    pub mode: OperatingMode,
    pub levels: PressureLevels,
    #[serde(serialize_with = "serialize_sample")]
    pub sample: ResourceSample,
    pub transitions_total: u64,
    pub recent_transitions: Vec<ModeTransition>,
}

fn serialize_sample<S: serde::Serializer>(s: &ResourceSample, ser: S) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeStruct;
    let mut st = ser.serialize_struct("ResourceSample", 5)?;
    st.serialize_field("memory_used_bytes", &s.memory_used)?;
    st.serialize_field("memory_limit_bytes", &s.memory_limit)?;
    st.serialize_field("disk_free_bytes", &s.disk_free)?;
    st.serialize_field("disk_total_bytes", &s.disk_total)?;
    st.serialize_field("cpu_ratio", &s.cpu_ratio)?;
    st.end()
}

fn describe_cause(levels: &PressureLevels, s: &ResourceSample, target: OperatingMode) -> String {
    let pct = |num: u64, den: u64| 100.0 * num as f64 / den.max(1) as f64;
    match target {
        OperatingMode::DiskCritical | OperatingMode::DiskPressure => {
            match (s.disk_free, s.disk_total) {
                (Some(f), Some(t)) => format!("disk free {:.1}% ({} bytes)", pct(f, t), f),
                _ => "disk pressure".to_string(),
            }
        }
        OperatingMode::MemoryPressure => match (s.memory_used, s.memory_limit) {
            (Some(u), Some(l)) => format!("memory {:.1}% of cgroup limit", pct(u, l)),
            _ => "memory pressure".to_string(),
        },
        OperatingMode::CpuPressure => match s.cpu_ratio {
            Some(r) => format!("cpu {:.0}% sustained", r * 100.0),
            None => "cpu pressure".to_string(),
        },
        OperatingMode::Normal => {
            // Recovery cause: report what cleared.
            let _ = levels;
            "all resources below exit watermarks".to_string()
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        mem_pct: Option<f64>,
        disk_free_pct: Option<f64>,
        cpu: Option<f64>,
    ) -> ResourceSample {
        // Large enough that fraction watermarks, not the 128 MiB
        // absolute disk floor, decide the outcome in these tests.
        const LIMIT: u64 = 100_000_000_000;
        ResourceSample {
            memory_used: mem_pct.map(|p| (p * LIMIT as f64) as u64),
            memory_limit: mem_pct.map(|_| LIMIT),
            disk_free: disk_free_pct.map(|p| (p * LIMIT as f64) as u64),
            disk_total: disk_free_pct.map(|_| LIMIT),
            cpu_ratio: cpu,
        }
    }

    fn mgr() -> ResourceManager {
        // debounce=2 (the default) so tests exercise it explicitly.
        ResourceManager::new(Thresholds::default())
    }

    #[test]
    fn stays_normal_with_no_signals() {
        let m = mgr();
        assert_eq!(m.observe(ResourceSample::default()), OperatingMode::Normal);
        assert_eq!(m.observe(ResourceSample::default()), OperatingMode::Normal);
        assert!(!m.writes_gated());
    }

    #[test]
    fn debounce_requires_consecutive_samples() {
        let m = mgr();
        // One critical reading is NOT enough to gate writes.
        assert_eq!(
            m.observe(sample(None, Some(0.01), None)),
            OperatingMode::Normal
        );
        // An OK reading in between resets the streak.
        assert_eq!(
            m.observe(sample(None, Some(0.50), None)),
            OperatingMode::Normal
        );
        assert_eq!(
            m.observe(sample(None, Some(0.01), None)),
            OperatingMode::Normal
        );
        // Second consecutive critical commits.
        assert_eq!(
            m.observe(sample(None, Some(0.01), None)),
            OperatingMode::DiskCritical
        );
        assert!(m.writes_gated());
        assert_eq!(m.transitions_total(), 1);
    }

    #[test]
    fn hysteresis_holds_mode_inside_the_band() {
        let m = mgr();
        // Enter memory pressure at >= 85%.
        m.observe(sample(Some(0.86), None, None));
        assert_eq!(
            m.observe(sample(Some(0.86), None, None)),
            OperatingMode::MemoryPressure
        );
        // 80% is below enter (85%) but above exit (78%): still pressured.
        m.observe(sample(Some(0.80), None, None));
        assert_eq!(
            m.observe(sample(Some(0.80), None, None)),
            OperatingMode::MemoryPressure
        );
        // Below the exit watermark releases (after debounce).
        m.observe(sample(Some(0.70), None, None));
        assert_eq!(
            m.observe(sample(Some(0.70), None, None)),
            OperatingMode::Normal
        );
        assert_eq!(m.transitions_total(), 2);
    }

    #[test]
    fn severity_ordering_disk_beats_memory_beats_cpu() {
        let both = sample(Some(0.95), Some(0.05), Some(0.99));
        let levels = derive_levels(&both, &PressureLevels::default(), &Thresholds::default());
        assert_eq!(derive_mode(&levels), OperatingMode::DiskPressure);

        let mem_cpu = sample(Some(0.95), None, Some(0.99));
        let levels = derive_levels(&mem_cpu, &PressureLevels::default(), &Thresholds::default());
        assert_eq!(derive_mode(&levels), OperatingMode::MemoryPressure);

        let cpu_only = sample(None, None, Some(0.99));
        let levels = derive_levels(
            &cpu_only,
            &PressureLevels::default(),
            &Thresholds::default(),
        );
        assert_eq!(derive_mode(&levels), OperatingMode::CpuPressure);
    }

    #[test]
    fn absolute_disk_floor_forces_critical_on_large_volumes() {
        // 4 TiB volume at 5% free would be fraction-High, but only
        // 100 MiB actually free must still be Critical.
        let s = ResourceSample {
            disk_free: Some(100 * 1024 * 1024),
            disk_total: Some(4 * 1024 * 1024 * 1024 * 1024),
            ..Default::default()
        };
        let levels = derive_levels(&s, &PressureLevels::default(), &Thresholds::default());
        assert_eq!(levels.disk, Some(PressureLevel::Critical));
    }

    #[test]
    fn recovery_transition_is_recorded() {
        let m = mgr();
        m.observe(sample(None, Some(0.01), None));
        m.observe(sample(None, Some(0.01), None));
        assert!(m.writes_gated());
        // Free space recovers well above the exit watermark (6%).
        m.observe(sample(None, Some(0.50), None));
        m.observe(sample(None, Some(0.50), None));
        assert!(!m.writes_gated());
        let snap = m.snapshot();
        assert_eq!(snap.recent_transitions.len(), 2);
        assert_eq!(snap.recent_transitions[1].to, OperatingMode::Normal);
        assert!(snap.recent_transitions[0].cause.contains("disk free"));
    }

    #[test]
    fn actuators_fire_on_committed_transitions_only() {
        use std::sync::atomic::{AtomicU32, Ordering as O};
        struct Counting {
            fired: AtomicU32,
            last_to: Mutex<Option<OperatingMode>>,
        }
        impl Actuator for Counting {
            fn name(&self) -> &str {
                "counting"
            }
            fn on_mode_change(&self, _from: OperatingMode, to: OperatingMode) {
                self.fired.fetch_add(1, O::SeqCst);
                *self.last_to.lock() = Some(to);
            }
        }
        let m = mgr();
        let a = std::sync::Arc::new(Counting {
            fired: AtomicU32::new(0),
            last_to: Mutex::new(None),
        });
        m.register_actuator(a.clone());

        // One critical sample: debounced, no transition, no actuation.
        m.observe(sample(None, Some(0.01), None));
        assert_eq!(a.fired.load(O::SeqCst), 0);
        // Second commits: exactly one actuation.
        m.observe(sample(None, Some(0.01), None));
        assert_eq!(a.fired.load(O::SeqCst), 1);
        assert_eq!(*a.last_to.lock(), Some(OperatingMode::DiskCritical));
        // Steady state: no repeat firing.
        m.observe(sample(None, Some(0.01), None));
        assert_eq!(a.fired.load(O::SeqCst), 1);
        // Recovery commits a second actuation back to Normal.
        m.observe(sample(None, Some(0.50), None));
        m.observe(sample(None, Some(0.50), None));
        assert_eq!(a.fired.load(O::SeqCst), 2);
        assert_eq!(*a.last_to.lock(), Some(OperatingMode::Normal));
    }

    #[test]
    fn late_registration_syncs_to_current_mode() {
        use std::sync::atomic::{AtomicU32, Ordering as O};
        struct Counting(AtomicU32);
        impl Actuator for Counting {
            fn name(&self) -> &str {
                "late"
            }
            fn on_mode_change(&self, _from: OperatingMode, _to: OperatingMode) {
                self.0.fetch_add(1, O::SeqCst);
            }
        }
        let m = mgr();
        m.observe(sample(None, Some(0.01), None));
        m.observe(sample(None, Some(0.01), None));
        assert!(m.writes_gated());
        let a = std::sync::Arc::new(Counting(AtomicU32::new(0)));
        m.register_actuator(a.clone());
        assert_eq!(
            a.0.load(O::SeqCst),
            1,
            "must sync to the live degraded mode"
        );
    }

    #[test]
    fn missing_readings_never_create_pressure() {
        // memory_used without a limit (unlimited cgroup) is no signal.
        let s = ResourceSample {
            memory_used: Some(u64::MAX / 2),
            memory_limit: None,
            ..Default::default()
        };
        let levels = derive_levels(&s, &PressureLevels::default(), &Thresholds::default());
        assert_eq!(levels.memory, None);
        assert_eq!(derive_mode(&levels), OperatingMode::Normal);
    }
}

//! Memory profile: how hard the server works to keep its resident
//! memory small (`NEBULA_MEMORY_PROFILE`, design 0014).
//!
//! | Profile | Allocator | Index trim |
//! |---|---|---|
//! | `small` (default) | jemalloc returns freed pages to the OS after ~1s, via a background thread so an idle node shrinks too | once after boot recovery, then every 15 min |
//! | `fast` | pages kept ~10s for reuse (fewer page faults under heavy churn) | off |
//!
//! The trim ([`TextIndex::trim_memory`]) releases spare capacity in
//! the HNSW adjacency lists and BM25 posting lists — ~480 B/doc on prod
//! data — in small lock-released batches, so it runs on a serving node
//! without stalling requests and never changes search results.
//!
//! Overrides: `NEBULA_MEMORY_TRIM_SECS` (0 = periodic trim off),
//! `NEBULA_MEMORY_TRIM_BATCH` (nodes/terms per lock hold).

use std::sync::Arc;
use std::time::Duration;

use nebula_index::TextIndex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryProfile {
    Small,
    Fast,
}

impl MemoryProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Fast => "fast",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryConfig {
    pub profile: MemoryProfile,
    /// jemalloc `dirty_decay_ms`: how long freed pages stay mapped.
    pub dirty_decay_ms: isize,
    /// jemalloc `muzzy_decay_ms`. 0 skips the `MADV_FREE` stage, whose
    /// pages still count against the cgroup until memory pressure.
    pub muzzy_decay_ms: isize,
    /// Trim once after boot recovery, before serving.
    pub trim_on_boot: bool,
    /// Periodic trim interval; `None` = off.
    pub trim_interval: Option<Duration>,
    /// HNSW nodes / BM25 terms per write-lock hold.
    pub trim_batch: usize,
}

impl MemoryConfig {
    pub fn for_profile(profile: MemoryProfile) -> Self {
        match profile {
            MemoryProfile::Small => Self {
                profile,
                dirty_decay_ms: 1_000,
                muzzy_decay_ms: 0,
                trim_on_boot: true,
                trim_interval: Some(Duration::from_secs(900)),
                trim_batch: 20_000,
            },
            MemoryProfile::Fast => Self {
                profile,
                dirty_decay_ms: 10_000,
                muzzy_decay_ms: 10_000,
                trim_on_boot: false,
                trim_interval: None,
                trim_batch: 20_000,
            },
        }
    }

    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Parse from any key lookup, so tests don't touch process env.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let profile = match get("NEBULA_MEMORY_PROFILE").as_deref().map(str::trim) {
            None | Some("") | Some("small") => MemoryProfile::Small,
            Some("fast") => MemoryProfile::Fast,
            Some(other) => {
                tracing::warn!(value = other, "unknown NEBULA_MEMORY_PROFILE; using small");
                MemoryProfile::Small
            }
        };
        let mut cfg = Self::for_profile(profile);
        if let Some(secs) = get("NEBULA_MEMORY_TRIM_SECS").and_then(|v| v.trim().parse::<u64>().ok()) {
            cfg.trim_interval = (secs > 0).then(|| Duration::from_secs(secs));
        }
        if let Some(n) = get("NEBULA_MEMORY_TRIM_BATCH").and_then(|v| v.trim().parse::<usize>().ok()) {
            cfg.trim_batch = n.max(1);
        }
        cfg
    }
}

/// Apply the profile's jemalloc settings to the running allocator.
/// Failures are logged, not fatal: the server works either way.
pub fn apply_allocator(cfg: &MemoryConfig) {
    if let Err(e) = jemalloc::configure(cfg.dirty_decay_ms, cfg.muzzy_decay_ms) {
        tracing::warn!(error = %e, "memory profile: allocator settings not applied");
    }
}

/// One trim pass, then hand the freed pages straight back to the OS.
/// Blocking: call from `spawn_blocking`.
pub fn trim_now(index: &TextIndex, batch: usize) {
    let stats = index.trim_memory(batch);
    jemalloc::purge();
    tracing::info!(
        graph_nodes = stats.graph_nodes,
        posting_lists = stats.posting_lists,
        took_ms = stats.took.as_millis() as u64,
        "memory profile: index trimmed"
    );
}

/// Periodic trim task, if the config enables one.
pub fn spawn_trimmer(index: Arc<TextIndex>, cfg: &MemoryConfig) -> Option<tokio::task::JoinHandle<()>> {
    let period = cfg.trim_interval?;
    let batch = cfg.trim_batch;
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.tick().await; // first tick is immediate; boot trim covers it
        loop {
            tick.tick().await;
            let idx = Arc::clone(&index);
            if let Err(e) = tokio::task::spawn_blocking(move || trim_now(&idx, batch)).await {
                tracing::warn!("memory trim task panicked: {e}");
            }
        }
    }))
}

#[cfg(not(target_env = "msvc"))]
mod jemalloc {
    use std::ffi::{CStr, CString};
    use std::os::raw::c_void;

    use tikv_jemalloc_sys::mallctl;

    /// `MALLCTL_ARENAS_ALL` from jemalloc.h.
    const ARENAS_ALL: u32 = 4096;

    unsafe fn write<T>(name: &CStr, mut value: T) -> Result<(), String> {
        let rc = mallctl(
            name.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut value as *mut T as *mut c_void,
            std::mem::size_of::<T>(),
        );
        if rc == 0 {
            Ok(())
        } else {
            Err(format!("{}: errno {rc}", name.to_string_lossy()))
        }
    }

    pub(super) unsafe fn read<T: Default>(name: &CStr) -> Result<T, String> {
        let mut value = T::default();
        let mut len = std::mem::size_of::<T>();
        let rc = mallctl(
            name.as_ptr(),
            &mut value as *mut T as *mut c_void,
            &mut len,
            std::ptr::null_mut(),
            0,
        );
        if rc == 0 {
            Ok(value)
        } else {
            Err(format!("{}: errno {rc}", name.to_string_lossy()))
        }
    }

    pub(super) fn name(s: &str) -> CString {
        CString::new(s).expect("mallctl name has no NUL")
    }

    pub(super) fn configure(dirty_ms: isize, muzzy_ms: isize) -> Result<(), String> {
        // SAFETY: each name is a documented jemalloc control and each
        // value has the documented type (bool, ssize_t, unsigned).
        unsafe {
            // Without the background thread, decay only runs on
            // allocator activity, so an idle node never shrinks.
            // Linux/FreeBSD only (prod); elsewhere decay still applies
            // on the next allocator call.
            if let Err(e) = write(&name("background_thread"), true) {
                tracing::debug!(error = %e, "jemalloc background thread unavailable");
            }
            // Defaults for arenas created later...
            write(&name("arenas.dirty_decay_ms"), dirty_ms)?;
            write(&name("arenas.muzzy_decay_ms"), muzzy_ms)?;
            // ...and every arena that already exists.
            let n: u32 = read(&name("arenas.narenas"))?;
            for i in 0..n {
                // Arenas not yet initialized reject the write; they'll
                // pick up the defaults above when created.
                let _ = write(&name(&format!("arena.{i}.dirty_decay_ms")), dirty_ms);
                let _ = write(&name(&format!("arena.{i}.muzzy_decay_ms")), muzzy_ms);
            }
        }
        Ok(())
    }

    /// Release all unused dirty pages now instead of waiting for decay.
    pub(super) fn purge() {
        let n = name(&format!("arena.{ARENAS_ALL}.purge"));
        // SAFETY: `purge` takes no input or output.
        unsafe {
            mallctl(n.as_ptr(), std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut(), 0);
        }
    }
}

#[cfg(target_env = "msvc")]
mod jemalloc {
    pub(super) fn configure(_: isize, _: isize) -> Result<(), String> {
        Err("jemalloc is not used on msvc".into())
    }
    pub(super) fn purge() {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(pairs: &[(&str, &str)]) -> MemoryConfig {
        let map: std::collections::HashMap<_, _> = pairs.iter().copied().collect();
        MemoryConfig::from_lookup(|k| map.get(k).map(|v| v.to_string()))
    }

    #[test]
    fn small_is_the_default() {
        assert_eq!(cfg(&[]), MemoryConfig::for_profile(MemoryProfile::Small));
        assert_eq!(cfg(&[("NEBULA_MEMORY_PROFILE", "")]).profile, MemoryProfile::Small);
        assert_eq!(cfg(&[("NEBULA_MEMORY_PROFILE", "bogus")]).profile, MemoryProfile::Small);
    }

    #[test]
    fn fast_turns_trimming_off() {
        let c = cfg(&[("NEBULA_MEMORY_PROFILE", "fast")]);
        assert_eq!(c.profile, MemoryProfile::Fast);
        assert!(!c.trim_on_boot);
        assert_eq!(c.trim_interval, None);
    }

    #[test]
    fn overrides_apply_on_top_of_the_profile() {
        let c = cfg(&[("NEBULA_MEMORY_TRIM_SECS", "0"), ("NEBULA_MEMORY_TRIM_BATCH", "500")]);
        assert_eq!(c.trim_interval, None);
        assert_eq!(c.trim_batch, 500);
        let c = cfg(&[("NEBULA_MEMORY_PROFILE", "fast"), ("NEBULA_MEMORY_TRIM_SECS", "60")]);
        assert_eq!(c.trim_interval, Some(Duration::from_secs(60)));
    }

    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn allocator_settings_take_effect() {
        jemalloc::configure(1_234, 0).unwrap();
        // SAFETY: documented ssize_t / bool controls.
        let dirty: isize = unsafe { jemalloc::read(&jemalloc::name("arenas.dirty_decay_ms")) }.unwrap();
        assert_eq!(dirty, 1_234);
        #[cfg(target_os = "linux")]
        assert!(unsafe { jemalloc::read::<bool>(&jemalloc::name("background_thread")) }.unwrap());
        jemalloc::purge();
    }
}

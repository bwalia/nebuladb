//! OS-level resource probes.
//!
//! Deliberately dependency-light: direct cgroup file reads (same
//! sources the `/metrics` memory gauges have always used), one
//! `statvfs` call for disk, and process CPU-time deltas. Every probe
//! degrades to `None` rather than erroring — a node that can't read a
//! source (macOS dev laptop, no cgroup, in-memory index) simply has no
//! pressure signal for that resource and stays `Ok` there.

use std::path::{Path, PathBuf};
use std::time::Instant;

/// One point-in-time reading of everything the manager watches.
/// `None` fields mean "source unavailable", never "zero".
#[derive(Debug, Clone, Copy, Default)]
pub struct ResourceSample {
    /// Resident bytes charged to this cgroup.
    pub memory_used: Option<u64>,
    /// cgroup hard limit; `None` when unlimited (`max`) or no cgroup.
    pub memory_limit: Option<u64>,
    /// Free bytes on the data volume (available to unprivileged writes).
    pub disk_free: Option<u64>,
    /// Total bytes on the data volume.
    pub disk_total: Option<u64>,
    /// Process CPU utilization over the last sampling window, as a
    /// fraction of ALL cores (1.0 = every core saturated by us).
    pub cpu_ratio: Option<f64>,
}

/// Stateful prober: CPU utilization is a delta between consecutive
/// calls, so the prober remembers the previous reading.
pub struct Prober {
    data_dir: Option<PathBuf>,
    last_cpu: Option<(Instant, f64)>,
    cores: f64,
}

impl Prober {
    /// `data_dir = None` (in-memory index) disables the disk probe.
    pub fn new(data_dir: Option<PathBuf>) -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get() as f64)
            .unwrap_or(1.0);
        Self {
            data_dir,
            last_cpu: None,
            cores,
        }
    }

    /// Take one sample. Does blocking file/syscall I/O — call from
    /// `spawn_blocking`, mirroring the durability-metrics sampler.
    pub fn sample(&mut self) -> ResourceSample {
        let (memory_used, memory_limit) = read_cgroup_memory();
        let (disk_free, disk_total) = match &self.data_dir {
            Some(dir) => read_disk(dir),
            None => (None, None),
        };
        let cpu_ratio = self.sample_cpu();
        ResourceSample {
            memory_used,
            memory_limit,
            disk_free,
            disk_total,
            cpu_ratio,
        }
    }

    fn sample_cpu(&mut self) -> Option<f64> {
        let now = Instant::now();
        let cpu_secs = process_cpu_seconds()?;
        let ratio = match self.last_cpu {
            Some((prev_at, prev_secs)) => {
                let wall = now.duration_since(prev_at).as_secs_f64();
                if wall <= 0.0 {
                    None
                } else {
                    Some(((cpu_secs - prev_secs).max(0.0) / wall / self.cores).min(1.0))
                }
            }
            // First call has no window to measure over.
            None => None,
        };
        self.last_cpu = Some((now, cpu_secs));
        ratio
    }
}

/// cgroup v2 first, v1 fallback — the same sources the `/metrics`
/// memory gauges read. A v2 `memory.max` of the literal `max` means
/// unlimited and yields `None`.
fn read_cgroup_memory() -> (Option<u64>, Option<u64>) {
    let read_u64 =
        |p: &str| -> Option<u64> { std::fs::read_to_string(p).ok()?.trim().parse::<u64>().ok() };
    let current = read_u64("/sys/fs/cgroup/memory.current")
        .or_else(|| read_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes"));
    let limit = read_u64("/sys/fs/cgroup/memory.max")
        .or_else(|| read_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
        // cgroup v1 reports "no limit" as a page-rounded i64::MAX;
        // treat anything absurdly large as unlimited.
        .filter(|&l| l < (1u64 << 60));
    (current, limit)
}

/// Free/total bytes on the filesystem holding `dir`, via statvfs.
/// Uses `f_bavail` (unprivileged-available) — the WAL runs as a
/// non-root user, so root-reserved blocks are not really free.
#[cfg(unix)]
fn read_disk(dir: &Path) -> (Option<u64>, Option<u64>) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let Ok(cpath) = CString::new(dir.as_os_str().as_bytes()) else {
        return (None, None);
    };
    // SAFETY: statvfs writes into the zeroed struct on success and we
    // only read it after checking the return code.
    unsafe {
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(cpath.as_ptr(), &mut st) != 0 {
            return (None, None);
        }
        let frsize = st.f_frsize as u64;
        (
            Some(st.f_bavail as u64 * frsize),
            Some(st.f_blocks as u64 * frsize),
        )
    }
}

#[cfg(not(unix))]
fn read_disk(_dir: &Path) -> (Option<u64>, Option<u64>) {
    (None, None)
}

/// Total user+system CPU seconds this process has consumed.
#[cfg(unix)]
fn process_cpu_seconds() -> Option<f64> {
    // SAFETY: getrusage fills the zeroed struct on success.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return None;
        }
        let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        Some(tv(ru.ru_utime) + tv(ru.ru_stime))
    }
}

#[cfg(not(unix))]
fn process_cpu_seconds() -> Option<f64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_probe_reads_the_data_dir_filesystem() {
        let (free, total) = read_disk(Path::new("/"));
        // Any Unix machine running the tests has a root filesystem
        // with nonzero total and free <= total.
        let (free, total) = (free.unwrap(), total.unwrap());
        assert!(total > 0);
        assert!(free <= total);
    }

    #[test]
    fn cpu_probe_returns_ratio_on_second_sample() {
        let mut p = Prober::new(None);
        assert!(p.sample().cpu_ratio.is_none(), "no window on first call");
        // Burn a little CPU so the delta is nonzero-ish; we only
        // assert the ratio is in range, not its magnitude.
        let mut x = 0u64;
        for i in 0..1_000_000u64 {
            x = x.wrapping_add(i);
        }
        std::hint::black_box(x);
        let ratio = p.sample().cpu_ratio.expect("second sample has a window");
        assert!((0.0..=1.0).contains(&ratio), "ratio out of range: {ratio}");
    }

    #[test]
    fn missing_data_dir_disables_disk_probe() {
        let mut p = Prober::new(None);
        let s = p.sample();
        assert!(s.disk_free.is_none());
        assert!(s.disk_total.is_none());
    }
}

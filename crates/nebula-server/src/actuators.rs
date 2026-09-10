//! Concrete actuators (design 0010 §6) — the components the resource
//! manager retunes on committed mode transitions.
//!
//! Kept in one module so the full list of "what changes under
//! pressure" is readable in one place:
//!
//! - [`EmbedCacheActuator`]: shrinks the in-process embedding LRU 4x
//!   under memory/disk pressure (each 1536-dim entry is ~6 KB; a
//!   10k-entry cache is ~60 MB of reclaimable headroom), restores it
//!   on recovery.
//! - The embedding worker and snapshot scheduler read the mode
//!   directly each tick (they're already periodic; a poll is simpler
//!   and race-free vs. actuating their intervals from outside).

use std::sync::Arc;

use nebula_cache::CachingEmbedder;
use nebula_resource::{Actuator, OperatingMode};

/// Shrinks/restores the in-process embedding cache with memory
/// pressure. CPU pressure leaves the cache alone — it costs memory,
/// not CPU, and a warm cache *saves* CPU.
pub struct EmbedCacheActuator {
    cache: Arc<CachingEmbedder>,
    /// Divisor applied under pressure (full/4 by default).
    shrink_divisor: usize,
}

impl EmbedCacheActuator {
    pub fn new(cache: Arc<CachingEmbedder>) -> Self {
        Self {
            cache,
            shrink_divisor: 4,
        }
    }
}

impl Actuator for EmbedCacheActuator {
    fn name(&self) -> &str {
        "embed-cache"
    }

    fn on_mode_change(&self, _from: OperatingMode, to: OperatingMode) {
        let full = self.cache.capacity();
        match to {
            OperatingMode::MemoryPressure
            | OperatingMode::DiskPressure
            | OperatingMode::DiskCritical => {
                let target = (full / self.shrink_divisor).max(1);
                if self.cache.current_capacity() != target {
                    self.cache.set_capacity(target);
                    tracing::info!(
                        from = full,
                        to = target,
                        "embed cache shrunk under resource pressure"
                    );
                }
            }
            OperatingMode::Normal | OperatingMode::CpuPressure => {
                if self.cache.current_capacity() != full {
                    self.cache.set_capacity(full);
                    tracing::info!(to = full, "embed cache restored to full capacity");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebula_embed::{Embedder, MockEmbedder};

    fn cache_with_entries(cap: usize, n: usize) -> Arc<CachingEmbedder> {
        let cache = Arc::new(CachingEmbedder::new(Arc::new(MockEmbedder::new(4)), cap));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            for i in 0..n {
                let _ = cache.embed_one(&format!("text-{i}")).await.unwrap();
            }
        });
        assert_eq!(cache.len(), n);
        cache
    }

    #[test]
    fn shrinks_under_memory_pressure_and_restores() {
        let cache = cache_with_entries(100, 80);
        let a = EmbedCacheActuator::new(Arc::clone(&cache));
        a.on_mode_change(OperatingMode::Normal, OperatingMode::MemoryPressure);
        assert_eq!(cache.current_capacity(), 25);
        assert!(cache.len() <= 25, "entries evicted down to the new cap");
        a.on_mode_change(OperatingMode::MemoryPressure, OperatingMode::Normal);
        assert_eq!(cache.current_capacity(), 100);
    }

    #[test]
    fn cpu_pressure_leaves_cache_alone() {
        let cache = cache_with_entries(100, 80);
        let a = EmbedCacheActuator::new(Arc::clone(&cache));
        a.on_mode_change(OperatingMode::Normal, OperatingMode::CpuPressure);
        assert_eq!(cache.current_capacity(), 100);
        assert_eq!(cache.len(), 80);
    }
}

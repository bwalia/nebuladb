//! Hierarchical Navigable Small World graph (Malkov & Yashunin, 2016).
//!
//! # Design
//!
//! An HNSW index is a multi-layer graph. Layer 0 contains every node; each
//! upper layer contains a random subset, sampled from a geometric
//! distribution with parameter `1/M`. Search walks the top layer greedily
//! to find a near entry point, then refines level-by-level with a beam
//! search (`ef`) at each layer, and finally returns the `ef`-nearest at
//! layer 0.
//!
//! ## Concurrency
//!
//! The index is wrapped in a single [`RwLock`]: writes (insert/delete) take
//! an exclusive lock, reads (search) take a shared lock and can run in
//! parallel. This is simpler than the per-node locking used in some Rust
//! HNSW crates and is sufficient for the vertical-slice workload in this
//! codebase. The public API does not leak the lock, so tightening to
//! per-node locks later is a non-breaking change.
//!
//! ## Deletions
//!
//! Deletions are soft (tombstones). A tombstoned node stays in the graph
//! so connectivity is preserved for traversal, but it is filtered from
//! the final result set. A periodic rebuild would reclaim the space; that
//! is intentionally left out of this crate.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use ahash::{AHashMap, AHashSet};
use parking_lot::RwLock;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize, Serializer};

use nebula_core::{Id, NebulaError, Result};

use crate::distance::{dequantize_into, quantize, Metric};

/// Hard cap on graph height. 16 layers covers >10^20 nodes at M=16, so
/// this is effectively "unreachable in practice" and mainly exists so the
/// neighbor-list allocation per node is bounded.
const MAX_LEVEL: u8 = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Target number of bi-directional links per node on upper layers.
    /// Typical production values: 12–48. 16 is a good default.
    pub m: usize,
    /// Max links at layer 0. The paper recommends `2 * M`.
    pub m_max0: usize,
    /// Size of the dynamic candidate list during insert. Larger = better
    /// recall at insert-time but slower build. Typical: 100–400.
    pub ef_construction: usize,
    /// Default `ef` for search if the caller does not override it.
    pub ef_search: usize,
    /// Level-normalization constant. Set to `1 / ln(M)` for the standard
    /// geometric level distribution. Exposed so tests can pin it.
    pub ml: f32,
    /// Seed for the level-sampling RNG. Pinning this makes index builds
    /// deterministic, which is invaluable for reproducing recall issues.
    pub seed: u64,
}

impl HnswConfig {
    pub fn with_m(m: usize) -> Self {
        Self {
            m,
            m_max0: m * 2,
            ef_construction: 200,
            ef_search: 64,
            ml: 1.0 / (m as f32).ln(),
            seed: 0xC0FFEE,
        }
    }
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self::with_m(16)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SearchResult {
    pub id: Id,
    /// Distance under the index's metric. For cosine this is `1 - cos`,
    /// for L2 it is squared (no sqrt), for dot it is negated. Callers
    /// that want a "score" can flip sign / apply sqrt as needed.
    pub distance: f32,
}

/// Internal candidate used by the priority queues during search. We
/// compare by distance only; two nodes tied on distance are ordered by
/// id for total-order determinism (important when results are truncated).
#[derive(Debug, Clone, Copy)]
struct Candidate {
    distance: f32,
    node: u32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance && self.node == other.node
    }
}
impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    /// Max-heap ordering by distance (largest at top). `BinaryHeap` is a
    /// max-heap; to use it as a min-heap we wrap in `std::cmp::Reverse`.
    fn cmp(&self, other: &Self) -> Ordering {
        // NaN in distances would violate total order. Treat NaN as
        // "larger than anything" so it gets evicted first.
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Greater)
            .then_with(|| self.node.cmp(&other.node))
    }
}

struct Inner {
    /// Flat int8-quantized vector arena; node `n`'s codes occupy
    /// `codes[n*dim..(n+1)*dim]`, with its companion scale in
    /// `scales[n]` (see [`crate::distance::quantize`]). Storing int8
    /// rather than f32 is a ~4x cut in resident vector RAM — the single
    /// largest allocation in the process — at the cost of a small,
    /// bounded recall delta. A single contiguous allocation is
    /// cache-friendlier than `Vec<Vec<i8>>` and amortizes reallocation.
    /// The on-disk snapshot is still f32 (the arena is rebuilt by
    /// quantizing on restore), so the persistent format is unchanged.
    codes: Vec<i8>,
    /// Per-node quantization scale; `scales[n]` pairs with the codes at
    /// `codes[n*dim..(n+1)*dim]`.
    scales: Vec<f32>,
    /// Level assigned to each node at insert time.
    node_levels: Vec<u8>,
    /// `neighbors[node][level]` is the adjacency list at that layer.
    /// Upper layers are sparse, so inner `Vec`s are empty for levels
    /// above the node's own level.
    neighbors: Vec<Vec<Vec<u32>>>,
    /// External ↔ internal id mappings. External IDs are what the caller
    /// sees; internal `u32` node indices are what the graph uses.
    external: Vec<Id>,
    by_external: AHashMap<Id, u32>,
    /// Soft-deleted internal nodes. Still walked during traversal but
    /// filtered from the final result set.
    tombstones: AHashSet<u32>,
    /// Top of the graph. `None` only before the first insert.
    entry: Option<(u32, u8)>,
    rng: ChaCha8Rng,
}

pub struct Hnsw {
    dim: usize,
    metric: Metric,
    config: HnswConfig,
    inner: RwLock<Inner>,
}

/// Plain-data view of a graph — what gets persisted and restored.
/// Keep this in sync with `Inner` on every state-shape change; tests
/// below catch the common cases (missing field, size mismatch).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HnswSnapshot {
    pub dim: usize,
    pub metric: Metric,
    pub config: HnswConfig,
    /// Flat f32 arena, node-major (`vectors[n*dim..(n+1)*dim]`). The
    /// live index now stores int8 codes; this is the dequantized view,
    /// kept f32 so the on-disk snapshot format is unchanged. Restore
    /// re-quantizes it back into the arena.
    pub vectors: Vec<f32>,
    pub node_levels: Vec<u8>,
    pub neighbors: Vec<Vec<Vec<u32>>>,
    /// External IDs as raw `u64`s — bincode is happy with these,
    /// and we reconstruct the `AHashMap` index on load.
    pub external: Vec<u64>,
    pub tombstones: Vec<u32>,
    pub entry: Option<(u32, u8)>,
}

/// Borrowed mirror of `HnswSnapshot` used by `serialize_snapshot_into`.
/// Field order + types match exactly so bincode emits the same bytes
/// as serializing an owned `HnswSnapshot`.
#[derive(Serialize)]
struct HnswSnapshotView<'a> {
    dim: usize,
    metric: Metric,
    config: &'a HnswConfig,
    vectors: DequantizedArenaView<'a>,
    node_levels: &'a [u8],
    neighbors: &'a [Vec<Vec<u32>>],
    external: &'a [Id],
    tombstones: TombstoneSetView<'a>,
    entry: Option<(u32, u8)>,
}

/// Serializes the int8 arena as the f32 sequence bincode would emit for
/// a `Vec<f32>`, dequantizing element-by-element so the snapshot stays
/// f32-on-disk without ever materializing a full f32 copy of the arena
/// (which would defeat the point of the streaming writer). Per node `n`,
/// emits `codes[n*dim..(n+1)*dim]` each multiplied by `scales[n]`.
struct DequantizedArenaView<'a> {
    codes: &'a [i8],
    scales: &'a [f32],
    dim: usize,
}

impl<'a> Serialize for DequantizedArenaView<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.codes.len()))?;
        for (node, &scale) in self.scales.iter().enumerate() {
            for &c in &self.codes[node * self.dim..(node + 1) * self.dim] {
                seq.serialize_element(&(c as f32 * scale))?;
            }
        }
        seq.end()
    }
}

/// Serializes an `AHashSet<u32>` as a bincode sequence — same wire
/// bytes as `Vec<u32>` for the same elements. Iteration order is
/// hash-defined, which matches the owned path (`to_snapshot` collected
/// via the same iterator).
struct TombstoneSetView<'a>(&'a AHashSet<u32>);

impl<'a> Serialize for TombstoneSetView<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for &t in self.0 {
            seq.serialize_element(&t)?;
        }
        seq.end()
    }
}

impl Hnsw {
    pub fn new(dim: usize, metric: Metric, config: HnswConfig) -> Result<Self> {
        if dim == 0 {
            return Err(NebulaError::InvalidConfig("dim must be > 0".into()));
        }
        if config.m < 2 {
            return Err(NebulaError::InvalidConfig("m must be >= 2".into()));
        }
        if config.ef_construction < config.m {
            return Err(NebulaError::InvalidConfig(
                "ef_construction must be >= m".into(),
            ));
        }
        let rng = ChaCha8Rng::seed_from_u64(config.seed);
        Ok(Self {
            dim,
            metric,
            config,
            inner: RwLock::new(Inner {
                codes: Vec::new(),
                scales: Vec::new(),
                node_levels: Vec::new(),
                neighbors: Vec::new(),
                external: Vec::new(),
                by_external: AHashMap::new(),
                tombstones: AHashSet::new(),
                entry: None,
                rng,
            }),
        })
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// Number of inserted nodes including tombstones. See [`Self::len_live`]
    /// for the tombstone-excluded count.
    pub fn len(&self) -> usize {
        self.inner.read().external.len()
    }

    pub fn len_live(&self) -> usize {
        let g = self.inner.read();
        g.external.len() - g.tombstones.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len_live() == 0
    }

    /// Bincode-stream this index's state into `writer`, producing the
    /// same bytes that `bincode::serialize(&self.to_snapshot())` would,
    /// but without ever materializing the intermediate `HnswSnapshot`
    /// (which clones `vectors`/`node_levels`/`neighbors` and roughly
    /// doubles peak memory). Holds the read lock for the whole stream,
    /// so writers wait — disk I/O bound, no network — but reads remain
    /// concurrent.
    ///
    /// On-disk wire format is preserved: a `HnswSnapshotView` mirrors
    /// `HnswSnapshot` field-for-field, and bincode's format depends
    /// only on field order + element types + sequence layout — not on
    /// struct or field names.
    pub fn serialize_snapshot_into<W: std::io::Write>(
        &self,
        writer: W,
    ) -> bincode::Result<()> {
        let g = self.inner.read();
        let view = HnswSnapshotView {
            dim: self.dim,
            metric: self.metric,
            config: &self.config,
            vectors: DequantizedArenaView {
                codes: &g.codes,
                scales: &g.scales,
                dim: self.dim,
            },
            node_levels: &g.node_levels,
            neighbors: &g.neighbors,
            // `external` in the persisted snapshot is `Vec<u64>`, but
            // `Id(u64)` is a serde-newtype that bincode emits as just
            // the inner `u64`. Element bytes are identical.
            external: &g.external,
            tombstones: TombstoneSetView(&g.tombstones),
            entry: g.entry,
        };
        bincode::serialize_into(writer, &view)
    }

    /// Capture the whole graph state as a plain-data `HnswSnapshot`.
    /// The RNG is re-seeded from `config.seed` on restore, which
    /// keeps level sampling deterministic at the cost of never
    /// reproducing the *exact* same sequence of future levels that
    /// the live graph would have picked. That's fine — levels are
    /// only used for new inserts and the restored graph is
    /// statistically indistinguishable.
    pub fn to_snapshot(&self) -> HnswSnapshot {
        let g = self.inner.read();
        // Dequantize the int8 arena back to f32 for the on-disk format.
        let mut vectors = vec![0.0f32; g.codes.len()];
        for (node, &scale) in g.scales.iter().enumerate() {
            let r = node * self.dim..(node + 1) * self.dim;
            dequantize_into(&g.codes[r.clone()], scale, &mut vectors[r]);
        }
        HnswSnapshot {
            dim: self.dim,
            metric: self.metric,
            config: self.config.clone(),
            vectors,
            node_levels: g.node_levels.clone(),
            neighbors: g.neighbors.clone(),
            external: g.external.iter().map(|id| id.0).collect(),
            tombstones: g.tombstones.iter().copied().collect(),
            entry: g.entry,
        }
    }

    /// Rebuild a `Hnsw` from a snapshot. Rejects mismatched dim or
    /// metric — those are load-bearing invariants for every node
    /// currently in the graph; loading a mismatched snapshot would
    /// silently produce wrong results.
    pub fn restore_from_snapshot(snap: HnswSnapshot) -> Result<Self> {
        if snap.dim == 0 {
            return Err(NebulaError::InvalidConfig("snapshot dim must be > 0".into()));
        }
        if snap.vectors.len() != snap.external.len() * snap.dim {
            return Err(NebulaError::InvalidConfig(format!(
                "snapshot inconsistent: {} vector floats for {} nodes × dim {}",
                snap.vectors.len(),
                snap.external.len(),
                snap.dim
            )));
        }
        let rng = ChaCha8Rng::seed_from_u64(snap.config.seed);
        let mut by_external = AHashMap::with_capacity(snap.external.len());
        for (i, id) in snap.external.iter().enumerate() {
            by_external.insert(Id(*id), i as u32);
        }
        // Re-quantize the f32-on-disk vectors into the int8 arena.
        let n_nodes = snap.external.len();
        let mut codes = Vec::with_capacity(snap.vectors.len());
        let mut scales = Vec::with_capacity(n_nodes);
        for node in 0..n_nodes {
            let (c, s) = quantize(&snap.vectors[node * snap.dim..(node + 1) * snap.dim]);
            codes.extend_from_slice(&c);
            scales.push(s);
        }
        Ok(Self {
            dim: snap.dim,
            metric: snap.metric,
            config: snap.config,
            inner: RwLock::new(Inner {
                codes,
                scales,
                node_levels: snap.node_levels,
                neighbors: snap.neighbors,
                external: snap.external.into_iter().map(Id).collect(),
                by_external,
                tombstones: snap.tombstones.into_iter().collect(),
                entry: snap.entry,
                rng,
            }),
        })
    }

    /// Insert a vector under the given external id. Fails if the id is
    /// already present (use [`Self::delete`] first to replace).
    pub fn insert(&self, id: Id, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dim {
            return Err(NebulaError::DimensionMismatch {
                expected: self.dim,
                actual: vector.len(),
            });
        }
        let mut g = self.inner.write();
        if g.by_external.contains_key(&id) {
            return Err(NebulaError::InvalidConfig(format!("duplicate id: {id}")));
        }

        let node = g.external.len() as u32;
        let level = sample_level(&mut g.rng, self.config.ml);

        // Commit node identity before any graph surgery so a panic mid-
        // insert does not leave dangling half-registered IDs. The vector
        // is quantized to int8 here; the original f32 query is kept in
        // `query` below for the graph walk so build-time distances stay
        // full-precision (only stored vectors are quantized).
        let (codes, scale) = quantize(vector);
        g.codes.extend_from_slice(&codes);
        g.scales.push(scale);
        g.node_levels.push(level);
        g.neighbors
            .push((0..=level).map(|_| Vec::new()).collect());
        g.external.push(id);
        g.by_external.insert(id, node);

        // First node: it IS the entry point, no graph walk needed.
        let Some((mut ep, ep_level)) = g.entry else {
            g.entry = Some((node, level));
            return Ok(());
        };

        // Phase 1: greedy descent from the top of the graph down to
        // `level + 1`. At each upper layer we only need the single best
        // entry to seed the next layer's search.
        let query = vector.to_vec();
        if ep_level > level {
            for l in ((level + 1)..=ep_level).rev() {
                ep = self.greedy_step(&g, &query, ep, l);
            }
        }

        // Phase 2: at levels `min(level, ep_level)..=0` run a full
        // `ef_construction` beam search, pick neighbors via the
        // diversification heuristic, and wire both directions.
        let start_level = level.min(ep_level);
        let mut entry_points = vec![ep];
        for l in (0..=start_level).rev() {
            let neighbors_pool = self.search_layer(&g, &query, &entry_points, self.config.ef_construction, l);
            let m = if l == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            let selected =
                select_neighbors_heuristic(&g, &query, &neighbors_pool, m, self.dim, self.metric);

            // Write outbound edges for the new node first.
            g.neighbors[node as usize][l as usize] = selected.clone();

            // Then add reverse edges and prune each neighbor if it now
            // exceeds its level's cap. Pruning runs the same heuristic
            // over the neighbor's own neighborhood to keep diversity.
            for &nb in &selected {
                let nb_list = &mut g.neighbors[nb as usize][l as usize];
                if !nb_list.contains(&node) {
                    nb_list.push(node);
                }
                let cap = if l == 0 {
                    self.config.m_max0
                } else {
                    self.config.m
                };
                if nb_list.len() > cap {
                    // Dequantize the pivot once into an f32 buffer; all
                    // distances against it then fuse the other nodes'
                    // codes via `distance_code` (no per-candidate buffer).
                    let (nb_code, nb_scale) = code_at(&g, nb as usize, self.dim);
                    let mut nb_vec = vec![0.0f32; self.dim];
                    dequantize_into(nb_code, nb_scale, &mut nb_vec);
                    let pool: Vec<Candidate> = g.neighbors[nb as usize][l as usize]
                        .iter()
                        .map(|&x| {
                            let (xc, xs) = code_at(&g, x as usize, self.dim);
                            Candidate {
                                node: x,
                                distance: self.metric.distance_code(&nb_vec, xc, xs),
                            }
                        })
                        .collect();
                    let pruned =
                        select_neighbors_heuristic(&g, &nb_vec, &pool, cap, self.dim, self.metric);
                    g.neighbors[nb as usize][l as usize] = pruned;
                }
            }

            // Seed the next (lower) layer's search with this layer's
            // result pool — cheaper and higher-recall than restarting.
            entry_points = neighbors_pool.iter().map(|c| c.node).collect();
        }

        // Raise the entry point if we just created a node above the
        // current top layer.
        if level > ep_level {
            g.entry = Some((node, level));
        }

        Ok(())
    }

    /// Soft-delete. The node stays reachable in the graph for traversal
    /// correctness, but it will not appear in search results.
    pub fn delete(&self, id: Id) -> Result<()> {
        let mut g = self.inner.write();
        let node = *g.by_external.get(&id).ok_or(NebulaError::NotFound(id))?;
        g.tombstones.insert(node);
        Ok(())
    }

    /// Return a copy of the vector associated with `id`, if present.
    /// Used by the bucket export path so a rebalance target can ingest
    /// raw vectors without re-running the embedder. Skips tombstoned
    /// nodes since the caller is about to delete the source.
    /// Returns the dequantized f32 vector; the int8 round-trip means it
    /// is close to but not bit-identical to the originally inserted
    /// vector (within one quantization step per component). The export
    /// path is tolerant of this — recall on the rebalanced target is
    /// unaffected within the same bound as live search.
    pub fn get_vector(&self, id: Id) -> Option<Vec<f32>> {
        let g = self.inner.read();
        let node = *g.by_external.get(&id)? as usize;
        if g.tombstones.contains(&(node as u32)) {
            return None;
        }
        let (code, scale) = code_at(&g, node, self.dim);
        let mut out = vec![0.0f32; self.dim];
        dequantize_into(code, scale, &mut out);
        Some(out)
    }

    /// k-NN search. `ef` overrides the configured `ef_search`; pass
    /// `None` to use the default. `ef` is clamped to `>= k`.
    pub fn search(&self, query: &[f32], k: usize, ef: Option<usize>) -> Result<Vec<SearchResult>> {
        if query.len() != self.dim {
            return Err(NebulaError::DimensionMismatch {
                expected: self.dim,
                actual: query.len(),
            });
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        let g = self.inner.read();
        let Some((mut ep, ep_level)) = g.entry else {
            return Ok(Vec::new());
        };
        let ef = ef.unwrap_or(self.config.ef_search).max(k);

        // Greedy descent from the top to layer 1.
        for l in (1..=ep_level).rev() {
            ep = self.greedy_step(&g, query, ep, l);
        }

        // Beam search at layer 0, then filter tombstones and truncate.
        let pool = self.search_layer(&g, query, &[ep], ef, 0);

        let mut results: Vec<SearchResult> = pool
            .into_iter()
            .filter(|c| !g.tombstones.contains(&c.node))
            .map(|c| SearchResult {
                id: g.external[c.node as usize],
                distance: c.distance,
            })
            .collect();
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Greater)
        });
        results.truncate(k);
        Ok(results)
    }

    /// Walk one upper layer from `entry`, hopping to whichever neighbor
    /// is closer to `query`, until no neighbor improves. Classic greedy
    /// descent used between layers where we don't need ef>1.
    fn greedy_step(&self, g: &Inner, query: &[f32], entry: u32, level: u8) -> u32 {
        let mut current = entry;
        let (c_code, c_scale) = code_at(g, current as usize, self.dim);
        let mut best = self.metric.distance_code(query, c_code, c_scale);
        loop {
            let mut improved = false;
            for &nb in &g.neighbors[current as usize][level as usize] {
                let (nb_code, nb_scale) = code_at(g, nb as usize, self.dim);
                let d = self.metric.distance_code(query, nb_code, nb_scale);
                if d < best {
                    best = d;
                    current = nb;
                    improved = true;
                }
            }
            if !improved {
                return current;
            }
        }
    }

    /// Beam search within a single layer. Returns the `ef` closest
    /// candidates found, unsorted (callers sort when they need an order).
    fn search_layer(
        &self,
        g: &Inner,
        query: &[f32],
        entries: &[u32],
        ef: usize,
        level: u8,
    ) -> Vec<Candidate> {
        // `visited`: nodes we've already distance-computed.
        // `candidates`: frontier, min-heap keyed by distance (Reverse).
        // `results`: best-`ef`-so-far, max-heap so we can evict the worst.
        let mut visited: AHashSet<u32> = AHashSet::with_capacity(ef * 4);
        let mut candidates: BinaryHeap<std::cmp::Reverse<Candidate>> = BinaryHeap::new();
        let mut results: BinaryHeap<Candidate> = BinaryHeap::new();

        for &e in entries {
            if visited.insert(e) {
                let (e_code, e_scale) = code_at(g, e as usize, self.dim);
                let d = self.metric.distance_code(query, e_code, e_scale);
                let c = Candidate {
                    distance: d,
                    node: e,
                };
                candidates.push(std::cmp::Reverse(c));
                results.push(c);
                if results.len() > ef {
                    results.pop();
                }
            }
        }

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            // Termination: the closest remaining frontier point is
            // already worse than our current k-th best. Nothing deeper
            // in the frontier can beat what's in `results` because each
            // hop can only add distance (monotone search).
            let worst = results.peek().map(|x| x.distance).unwrap_or(f32::INFINITY);
            if c.distance > worst && results.len() >= ef {
                break;
            }
            for &nb in &g.neighbors[c.node as usize][level as usize] {
                if !visited.insert(nb) {
                    continue;
                }
                let (nb_code, nb_scale) = code_at(g, nb as usize, self.dim);
                let d = self.metric.distance_code(query, nb_code, nb_scale);
                let worst_now = results.peek().map(|x| x.distance).unwrap_or(f32::INFINITY);
                if results.len() < ef || d < worst_now {
                    let cand = Candidate {
                        distance: d,
                        node: nb,
                    };
                    candidates.push(std::cmp::Reverse(cand));
                    results.push(cand);
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }
        results.into_vec()
    }
}

/// Pick at most `m` neighbors from `pool` using Algorithm 4 of the HNSW
/// paper. The key property: a candidate `e` is only kept if it is closer
/// to the query than to any already-kept neighbor. This produces a
/// diverse neighborhood, which empirically dominates "just pick M
/// closest" for recall on clustered data.
///
/// `keep_pruned_connections` is implicitly true: if the heuristic picks
/// fewer than `m`, we top up with the next-closest discarded points
/// rather than leaving the neighbor list short.
fn select_neighbors_heuristic(
    inner: &Inner,
    _query: &[f32],
    pool: &[Candidate],
    m: usize,
    dim: usize,
    metric: Metric,
) -> Vec<u32> {
    // Sort ascending by distance to query; we iterate closest-first.
    let mut sorted: Vec<Candidate> = pool.to_vec();
    sorted.sort_by(|a, b| {
        a.distance
            .partial_cmp(&b.distance)
            .unwrap_or(Ordering::Greater)
    });

    let mut selected: Vec<u32> = Vec::with_capacity(m);
    // Dequantized f32 copies of the selected neighbors, so the diversity
    // check can fuse each candidate's codes against them via
    // `distance_code`. Owning the f32 (rather than borrowing the arena)
    // also sidesteps a borrow conflict on `inner`.
    let mut selected_vecs: Vec<Vec<f32>> = Vec::with_capacity(m);
    let mut discarded: Vec<Candidate> = Vec::new();

    for c in sorted {
        if selected.len() >= m {
            discarded.push(c);
            continue;
        }
        let (c_code, c_scale) = code_at(inner, c.node as usize, dim);
        // Algorithm 4: keep `c` iff it is closer to the query than to
        // any already-selected neighbor, under the same metric used for
        // the pool distances.
        let is_diverse = selected_vecs
            .iter()
            .all(|sv| c.distance < metric.distance_code(sv, c_code, c_scale));
        if is_diverse {
            let mut cv = vec![0.0f32; dim];
            dequantize_into(c_code, c_scale, &mut cv);
            selected.push(c.node);
            selected_vecs.push(cv);
        } else {
            discarded.push(c);
        }
    }

    // Top up if the heuristic was too strict. Without this, clusters
    // produce very short neighbor lists which hurt recall.
    if selected.len() < m {
        for c in discarded {
            if selected.len() >= m {
                break;
            }
            selected.push(c.node);
        }
    }
    selected
}

/// Borrow node `n`'s quantized codes and its companion scale from the
/// arena. Pair with [`Metric::distance_code`] to compute a distance
/// without dequantizing into a temporary buffer.
#[inline]
fn code_at(g: &Inner, n: usize, dim: usize) -> (&[i8], f32) {
    (&g.codes[n * dim..(n + 1) * dim], g.scales[n])
}

/// Geometric level sampling: `floor(-ln(U) * mL)`, capped at MAX_LEVEL.
/// With `mL = 1/ln(M)`, the expected fraction of nodes at level ≥ k is
/// `M^-k`, which is what makes the upper layers small.
fn sample_level(rng: &mut ChaCha8Rng, ml: f32) -> u8 {
    let r: f32 = rng.gen_range(f32::MIN_POSITIVE..1.0);
    let lvl = (-r.ln() * ml).floor();
    lvl.clamp(0.0, MAX_LEVEL as f32) as u8
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    fn random_vec(rng: &mut StdRng, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect()
    }

    #[test]
    fn empty_index_returns_nothing() {
        let h = Hnsw::new(8, Metric::L2Sq, HnswConfig::default()).unwrap();
        assert!(h.is_empty());
        let q = vec![0.0; 8];
        assert!(h.search(&q, 10, None).unwrap().is_empty());
    }

    #[test]
    fn single_insert_finds_self() {
        let h = Hnsw::new(4, Metric::L2Sq, HnswConfig::default()).unwrap();
        h.insert(Id(1), &[1.0, 2.0, 3.0, 4.0]).unwrap();
        let r = h.search(&[1.0, 2.0, 3.0, 4.0], 1, None).unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].id, Id(1));
        // Not exactly zero: the stored vector is int8-quantized while the
        // query stays f32, so self-distance reflects one quantization
        // step per component. max_abs=4 → scale=4/127, and L2Sq sums
        // dim*(scale/2)^2 ≈ 1e-3 in the worst case. A loose bound here
        // still distinguishes "found itself" from "found a wrong node".
        assert!(r[0].distance < 1e-2, "self-distance too large: {}", r[0].distance);
    }

    #[test]
    fn dimension_mismatch_errors() {
        let h = Hnsw::new(4, Metric::L2Sq, HnswConfig::default()).unwrap();
        let err = h.insert(Id(1), &[1.0, 2.0]).unwrap_err();
        assert!(matches!(err, NebulaError::DimensionMismatch { .. }));
    }

    #[test]
    fn duplicate_id_errors() {
        let h = Hnsw::new(2, Metric::L2Sq, HnswConfig::default()).unwrap();
        h.insert(Id(1), &[0.0, 0.0]).unwrap();
        assert!(h.insert(Id(1), &[1.0, 1.0]).is_err());
    }

    #[test]
    fn tombstoned_node_is_excluded() {
        let h = Hnsw::new(2, Metric::L2Sq, HnswConfig::default()).unwrap();
        h.insert(Id(1), &[0.0, 0.0]).unwrap();
        h.insert(Id(2), &[1.0, 0.0]).unwrap();
        h.delete(Id(1)).unwrap();
        let r = h.search(&[0.0, 0.0], 5, None).unwrap();
        assert!(r.iter().all(|x| x.id != Id(1)));
    }

    /// Recall sanity check: build a 2k-point index and compare the top-10
    /// result against brute-force ground truth. On random uniform data
    /// with M=16, ef=64, we expect high recall — we assert >= 0.9, which
    /// is well below what healthy implementations hit (~0.98+) but above
    /// what a broken graph would produce (<0.3).
    #[test]
    fn recall_vs_brute_force() {
        let dim = 32;
        let n = 2_000;
        let k = 10;
        let mut rng = StdRng::seed_from_u64(42);

        let data: Vec<Vec<f32>> = (0..n).map(|_| random_vec(&mut rng, dim)).collect();

        let h = Hnsw::new(dim, Metric::L2Sq, HnswConfig::with_m(16)).unwrap();
        for (i, v) in data.iter().enumerate() {
            h.insert(Id(i as u64), v).unwrap();
        }

        let mut hits = 0usize;
        let trials = 50;
        for _ in 0..trials {
            let q = random_vec(&mut rng, dim);

            let mut truth: Vec<(f32, usize)> = data
                .iter()
                .enumerate()
                .map(|(i, v)| (crate::distance::euclidean_sq(&q, v), i))
                .collect();
            truth.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
            let truth_ids: AHashSet<u64> = truth.iter().take(k).map(|x| x.1 as u64).collect();

            let got = h.search(&q, k, Some(64)).unwrap();
            for r in got {
                if truth_ids.contains(&r.id.0) {
                    hits += 1;
                }
            }
        }
        let recall = hits as f32 / (k * trials) as f32;
        assert!(
            recall >= 0.9,
            "recall too low: {recall} — graph is likely broken"
        );
    }

    #[test]
    fn cosine_metric_works() {
        let h = Hnsw::new(3, Metric::Cosine, HnswConfig::default()).unwrap();
        h.insert(Id(1), &[1.0, 0.0, 0.0]).unwrap();
        h.insert(Id(2), &[0.0, 1.0, 0.0]).unwrap();
        h.insert(Id(3), &[0.9, 0.1, 0.0]).unwrap();
        let r = h.search(&[1.0, 0.0, 0.0], 2, None).unwrap();
        assert_eq!(r[0].id, Id(1));
        assert_eq!(r[1].id, Id(3));
    }

    #[test]
    fn snapshot_round_trip_preserves_search() {
        // Build a small graph, snapshot it, restore, confirm
        // searches agree on the result set. Exact score parity
        // isn't required by the API but we get it here because
        // the vector bytes are the same.
        let h = Hnsw::new(8, Metric::L2Sq, HnswConfig::with_m(4)).unwrap();
        for i in 0..20u64 {
            let mut v = vec![0.0f32; 8];
            v[(i as usize) % 8] = 1.0 + (i as f32) * 0.1;
            h.insert(Id(i), &v).unwrap();
        }
        // Tombstone one to ensure that state survives too.
        h.delete(Id(7)).unwrap();

        let snap = h.to_snapshot();
        let h2 = Hnsw::restore_from_snapshot(snap).unwrap();

        // Deleted id must stay excluded after restore.
        let q = [1.0f32, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let r1 = h.search(&q, 5, None).unwrap();
        let r2 = h2.search(&q, 5, None).unwrap();
        let ids1: Vec<u64> = r1.iter().map(|x| x.id.0).collect();
        let ids2: Vec<u64> = r2.iter().map(|x| x.id.0).collect();
        assert_eq!(ids1, ids2, "restored graph gave different hits");
        assert!(!ids2.contains(&7), "tombstone was lost");
    }

    #[test]
    fn snapshot_roundtrip_via_bincode() {
        // The actual server path goes through bincode for on-disk
        // storage; verify that's wire-stable.
        let h = Hnsw::new(4, Metric::Cosine, HnswConfig::default()).unwrap();
        for i in 0..5u64 {
            h.insert(Id(i), &[i as f32, 1.0, 0.0, 0.0]).unwrap();
        }
        let snap = h.to_snapshot();
        let bytes = bincode::serialize(&snap).unwrap();
        let back: HnswSnapshot = bincode::deserialize(&bytes).unwrap();
        let h2 = Hnsw::restore_from_snapshot(back).unwrap();
        assert_eq!(h2.len(), 5);
    }

    #[test]
    fn streaming_serialize_matches_owned_serialize() {
        // serialize_snapshot_into must produce byte-identical output
        // to bincode::serialize(&to_snapshot()). If it doesn't, every
        // existing snapshot on disk becomes unreadable on the next
        // deploy — a far worse failure than the OOM the streaming
        // path is here to fix.
        let h = Hnsw::new(6, Metric::Cosine, HnswConfig::default()).unwrap();
        for i in 0..30u64 {
            let mut v = vec![0.0f32; 6];
            v[(i as usize) % 6] = (i as f32) + 0.5;
            h.insert(Id(i), &v).unwrap();
        }
        // Mix in tombstones; the borrowed view iterates the AHashSet
        // and must yield the same bytes.
        h.delete(Id(3)).unwrap();
        h.delete(Id(17)).unwrap();

        let owned_bytes = bincode::serialize(&h.to_snapshot()).unwrap();
        let mut streamed_bytes = Vec::with_capacity(owned_bytes.len());
        h.serialize_snapshot_into(&mut streamed_bytes).unwrap();

        // AHashSet iteration order isn't stable across binaries but
        // it IS stable within one process — `to_snapshot` and
        // `serialize_snapshot_into` both iterate the same set in the
        // same order in this run.
        assert_eq!(
            streamed_bytes, owned_bytes,
            "streamed snapshot bytes diverged from the owned path"
        );
    }
}

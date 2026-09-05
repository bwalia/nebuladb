/**
 * Deterministic demo simulator.
 *
 * NebulaDB exposes real endpoints for stats, durability, audit, slow
 * queries, buckets, replication and cluster nodes. It does *not*
 * expose multi-region XDCR topology, a failover drill or a swap
 * rebalance controller — those are the capabilities this module
 * stands in for, behind the `Sourced`/`simulated` provenance wrapper
 * so the UI can badge them (build brief section 34).
 *
 * Everything here is a pure function of (seed, tick). No `Math.random`,
 * no `Date.now` inside the state machines — the caller supplies the
 * tick. That makes a demo run reproducible: same seed, same story.
 */
import { rng, randInt, pick, type Sourced, simulated } from "./provenance";

// ---------------------------------------------------------------- topology

export type NodeHealth = "healthy" | "degraded" | "down" | "joining";
export type NodeRole = "primary" | "replica";

export interface SimNode {
  id: string;
  region: string;
  role: NodeRole;
  health: NodeHealth;
  shards: number[];
  cpuPct: number;
  memPct: number;
  storageGb: number;
  qps: number;
}

export interface SimRegion {
  name: string;
  label: string;
  nodes: SimNode[];
  /** XDCR lag to the peer region, milliseconds. */
  lagMs: number;
  throughputDocsSec: number;
  conflicts: number;
}

export interface SimCluster {
  regions: SimRegion[];
  shardCount: number;
  replicationFactor: number;
}

const REGION_DEFS = [
  { name: "eu-west", label: "Europe (eu-west)" },
  { name: "us-east", label: "America (us-east)" },
] as const;

/**
 * Build the baseline topology. Six nodes across two regions with a
 * replication factor of 3 — the shape the brief's multi-region
 * diagram asks for (section 21).
 */
export function buildCluster(seed = 20260905): SimCluster {
  const r = rng(seed);
  const shardCount = 12;
  const regions: SimRegion[] = REGION_DEFS.map((def, ri) => {
    const nodes: SimNode[] = Array.from({ length: 3 }, (_, ni) => {
      const idx = ri * 3 + ni + 1;
      const shards = Array.from({ length: shardCount })
        .map((_, s) => s)
        .filter((s) => s % 3 === ni);
      return {
        id: `nebula-${def.name}-${ni + 1}`,
        region: def.name,
        // One primary per region; the rest carry replicas.
        role: ni === 0 ? "primary" : "replica",
        health: "healthy" as NodeHealth,
        shards,
        cpuPct: randInt(r, 18, 46),
        memPct: randInt(r, 34, 62),
        storageGb: randInt(r, 120, 380),
        qps: randInt(r, 400, 1800) + idx * 7,
      };
    });
    return {
      name: def.name,
      label: def.label,
      nodes,
      lagMs: randInt(r, 40, 180),
      throughputDocsSec: randInt(r, 900, 2400),
      conflicts: randInt(r, 0, 3),
    };
  });
  return { regions, shardCount, replicationFactor: 3 };
}

/**
 * Advance the live-looking counters by one tick. Bounded random walk
 * so numbers breathe without drifting into nonsense.
 */
export function tickCluster(c: SimCluster, tick: number): SimCluster {
  const r = rng(0x9e3779b9 ^ tick);
  const walk = (v: number, lo: number, hi: number, amp: number) =>
    Math.max(lo, Math.min(hi, Math.round(v + (r() - 0.5) * amp)));
  return {
    ...c,
    regions: c.regions.map((reg) => ({
      ...reg,
      lagMs: walk(reg.lagMs, 25, 400, 40),
      throughputDocsSec: walk(reg.throughputDocsSec, 400, 3200, 260),
      nodes: reg.nodes.map((n) =>
        n.health === "down"
          ? { ...n, cpuPct: 0, memPct: 0, qps: 0 }
          : {
              ...n,
              cpuPct: walk(n.cpuPct, 8, 92, 9),
              memPct: walk(n.memPct, 20, 88, 5),
              qps: walk(n.qps, 120, 3000, 180),
            }
      ),
    })),
  };
}

// ---------------------------------------------------------------- failover

export const FAILOVER_PHASES = [
  "healthy",
  "failure",
  "detection",
  "promotion",
  "routing",
  "recovery",
] as const;
export type FailoverPhase = (typeof FAILOVER_PHASES)[number];

export interface FailoverState {
  phase: FailoverPhase;
  /** ms elapsed inside the current phase. */
  elapsedMs: number;
  targetNodeId: string | null;
  promotedNodeId: string | null;
  log: Array<{ atMs: number; phase: FailoverPhase; message: string }>;
}

/** Wall-clock budget per phase — tuned so a drill reads in ~12s. */
const FAILOVER_BUDGET_MS: Record<FailoverPhase, number> = {
  healthy: Infinity,
  failure: 900,
  detection: 2600,
  promotion: 3200,
  routing: 2200,
  recovery: 3400,
};

export const initialFailover = (): FailoverState => ({
  phase: "healthy",
  elapsedMs: 0,
  targetNodeId: null,
  promotedNodeId: null,
  log: [],
});

export function startFailover(nodeId: string): FailoverState {
  return {
    phase: "failure",
    elapsedMs: 0,
    targetNodeId: nodeId,
    promotedNodeId: null,
    log: [{ atMs: 0, phase: "failure", message: `${nodeId} stopped responding to heartbeats` }],
  };
}

/**
 * Drive the failover state machine forward. Pure: given the same
 * state and dt you always get the same next state.
 */
export function tickFailover(
  s: FailoverState,
  dtMs: number,
  cluster: SimCluster
): FailoverState {
  if (s.phase === "healthy") return s;
  const elapsed = s.elapsedMs + dtMs;
  const budget = FAILOVER_BUDGET_MS[s.phase];
  if (elapsed < budget) return { ...s, elapsedMs: elapsed };

  const i = FAILOVER_PHASES.indexOf(s.phase);
  const next = FAILOVER_PHASES[Math.min(i + 1, FAILOVER_PHASES.length - 1)];
  const totalMs = s.log.reduce((m, l) => Math.max(m, l.atMs), 0) + budget;

  // On promotion, pick the healthiest replica in the same region.
  let promoted = s.promotedNodeId;
  if (next === "promotion" && !promoted && s.targetNodeId) {
    const region = cluster.regions.find((r) =>
      r.nodes.some((n) => n.id === s.targetNodeId)
    );
    const candidate = region?.nodes.find(
      (n) => n.id !== s.targetNodeId && n.role === "replica" && n.health === "healthy"
    );
    promoted = candidate?.id ?? null;
  }

  const message: Record<FailoverPhase, string> = {
    healthy: "cluster healthy",
    failure: `${s.targetNodeId} stopped responding to heartbeats`,
    detection: `quorum agreed ${s.targetNodeId} is unreachable (3 missed heartbeats)`,
    promotion: promoted
      ? `promoting replica ${promoted} to primary for its shard range`
      : "no eligible replica — shard range is read-only until recovery",
    routing: "routing table updated; clients redirected on next request",
    recovery: `${s.targetNodeId} rejoined as replica and is catching up from WAL`,
  };

  return {
    phase: next,
    elapsedMs: 0,
    targetNodeId: s.targetNodeId,
    promotedNodeId: promoted,
    log: [...s.log, { atMs: totalMs, phase: next, message: message[next] }],
  };
}

// --------------------------------------------------------------- rebalance

export const REBALANCE_PHASES = [
  "idle",
  "snapshot",
  "cdc",
  "catchup",
  "verify",
  "cutover",
  "done",
] as const;
export type RebalancePhase = (typeof REBALANCE_PHASES)[number];

export interface RebalanceState {
  phase: RebalancePhase;
  paused: boolean;
  /** 0..1 within the current phase. */
  progress: number;
  bytesMoved: number;
  totalBytes: number;
  cdcLagMs: number;
  throughputMbSec: number;
  shardsMoved: number;
  totalShards: number;
  log: Array<{ phase: RebalancePhase; message: string }>;
}

const REBALANCE_RATE: Record<RebalancePhase, number> = {
  idle: 0,
  snapshot: 0.22,
  cdc: 0.3,
  catchup: 0.26,
  verify: 0.4,
  cutover: 0.6,
  done: 0,
};

export const initialRebalance = (totalShards = 12): RebalanceState => ({
  phase: "idle",
  paused: false,
  progress: 0,
  bytesMoved: 0,
  totalBytes: 48 * 1024 * 1024 * 1024,
  cdcLagMs: 0,
  throughputMbSec: 0,
  shardsMoved: 0,
  totalShards,
  log: [],
});

export function startRebalance(s: RebalanceState): RebalanceState {
  return {
    ...initialRebalance(s.totalShards),
    phase: "snapshot",
    log: [{ phase: "snapshot", message: "streaming base snapshot to the replacement node" }],
  };
}

export function tickRebalance(s: RebalanceState, dtMs: number, tick: number): RebalanceState {
  if (s.phase === "idle" || s.phase === "done" || s.paused) return s;
  const r = rng(0x85ebca6b ^ tick);
  const step = (REBALANCE_RATE[s.phase] * dtMs) / 1000;
  const progress = s.progress + step;

  const throughput = 40 + r() * 90;
  const bytesMoved = Math.min(
    s.totalBytes,
    s.bytesMoved + throughput * 1024 * 1024 * (dtMs / 1000)
  );
  const cdcLagMs =
    s.phase === "cdc" || s.phase === "catchup"
      ? Math.max(8, Math.round(600 * (1 - Math.min(1, progress)) + r() * 60))
      : 0;

  if (progress < 1) {
    return { ...s, progress, bytesMoved, cdcLagMs, throughputMbSec: throughput };
  }

  const i = REBALANCE_PHASES.indexOf(s.phase);
  const next = REBALANCE_PHASES[Math.min(i + 1, REBALANCE_PHASES.length - 1)];
  const message: Record<RebalancePhase, string> = {
    idle: "",
    snapshot: "base snapshot complete",
    cdc: "CDC stream attached; applying changes since snapshot LSN",
    catchup: "replica caught up to within one WAL segment",
    verify: "consistency verified — checksums match across all moved shards",
    cutover: "cutover complete; old node drained and removed from the routing table",
    done: "swap rebalance finished with zero dropped requests",
  };
  return {
    ...s,
    phase: next,
    progress: 0,
    bytesMoved,
    cdcLagMs: 0,
    throughputMbSec: throughput,
    shardsMoved: next === "done" ? s.totalShards : Math.min(s.totalShards, s.shardsMoved + 2),
    log: [...s.log, { phase: next, message: message[next] }],
  };
}

// ------------------------------------------------------------------- stats

export interface ExecStats {
  documents: Sourced<number>;
  vectors: Sourced<number>;
  embeddings: Sourced<number>;
  qps: Sourced<number>;
  vectorQps: Sourced<number>;
  p95Ms: Sourced<number>;
  activeAgents: Sourced<number>;
  ragQueries: Sourced<number>;
  replicationLagMs: Sourced<number>;
  clusterNodes: Sourced<number>;
  regions: Sourced<number>;
  storageGb: Sourced<number>;
  backupStatus: Sourced<string>;
}

/** Simulated slice of the executive dashboard (the parts with no endpoint). */
export function simExecSlice(cluster: SimCluster, tick: number) {
  const r = rng(0xc2b2ae35 ^ tick);
  const nodes = cluster.regions.flatMap((x) => x.nodes);
  const up = nodes.filter((n) => n.health !== "down");
  return {
    qps: simulated(up.reduce((a, n) => a + n.qps, 0), "cluster simulator"),
    vectorQps: simulated(
      Math.round(up.reduce((a, n) => a + n.qps, 0) * (0.28 + r() * 0.08)),
      "cluster simulator"
    ),
    p95Ms: simulated(Math.round(9 + r() * 14), "cluster simulator"),
    activeAgents: simulated(randInt(r, 3, 11), "agent simulator"),
    replicationLagMs: simulated(
      Math.round(cluster.regions.reduce((a, x) => a + x.lagMs, 0) / cluster.regions.length),
      "XDCR simulator"
    ),
    clusterNodes: simulated(nodes.length, "cluster simulator"),
    regions: simulated(cluster.regions.length, "cluster simulator"),
    storageGb: simulated(nodes.reduce((a, n) => a + n.storageGb, 0), "cluster simulator"),
    backupStatus: simulated(pick(r, ["healthy", "healthy", "healthy"]), "backup simulator"),
  };
}

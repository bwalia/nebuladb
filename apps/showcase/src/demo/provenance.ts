/**
 * Provenance — the honesty layer.
 *
 * The showcase mixes two kinds of numbers: values read from a live
 * NebulaDB, and values produced by the deterministic demo simulator
 * for capabilities the server does not expose yet (multi-region
 * topology, failover drills, swap rebalance).
 *
 * Section 34 of the build brief is explicit: never present simulated
 * metrics as real production metrics. So every value that reaches a
 * chart or a stat tile is wrapped in `Sourced<T>`, and the badge
 * components in `components/Provenance.tsx` render its origin. There
 * is deliberately no way to unwrap a value without seeing its source.
 */

export type Origin =
  /** Read from the NebulaDB REST API on this cluster. */
  | "live"
  /** Produced by the deterministic simulator in `simulation.ts`. */
  | "simulated"
  /** Derived from live values (e.g. a ratio of two live counters). */
  | "derived";

export interface Sourced<T> {
  value: T;
  origin: Origin;
  /** Endpoint for `live`/`derived`, scenario name for `simulated`. */
  from: string;
}

export const live = <T,>(value: T, from: string): Sourced<T> => ({
  value,
  origin: "live",
  from,
});

export const simulated = <T,>(value: T, from: string): Sourced<T> => ({
  value,
  origin: "simulated",
  from,
});

export const derived = <T,>(value: T, from: string): Sourced<T> => ({
  value,
  origin: "derived",
  from,
});

/** True when any input is simulated — use to badge an aggregate tile. */
export function anySimulated(...xs: Array<Sourced<unknown>>): boolean {
  return xs.some((x) => x.origin === "simulated");
}

/**
 * mulberry32 — a tiny deterministic PRNG.
 *
 * Determinism matters for a demo: the same seed must produce the same
 * cluster, the same failover timeline and the same rebalance byte
 * counts on every machine, so a presenter can rehearse a run and get
 * it again live. `Math.random()` would make the demo unrepeatable.
 */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Deterministic integer in [lo, hi]. */
export function randInt(r: () => number, lo: number, hi: number): number {
  return lo + Math.floor(r() * (hi - lo + 1));
}

/** Deterministic pick. */
export function pick<T>(r: () => number, xs: readonly T[]): T {
  return xs[Math.floor(r() * xs.length)];
}

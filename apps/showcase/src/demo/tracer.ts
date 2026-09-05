/**
 * Request tracer — the data behind "Show me what's happening"
 * (build brief section 37).
 *
 * `api.ts` pushes every real HTTP call through here, so the
 * Under-the-Hood panel shows the actual request, the actual response
 * and the actual timing rather than a hand-written illustration. A
 * technical audience will check, and a fabricated trace would be
 * spotted immediately.
 *
 * Ring buffer, newest first, capped so a long demo session cannot
 * grow unbounded.
 */

export interface TraceEntry {
  id: number;
  ts: number;
  method: string;
  path: string;
  status: number;
  tookMs: number;
  requestBody?: unknown;
  responseBody?: unknown;
  error?: string;
}

const MAX = 60;
let seq = 0;
let entries: TraceEntry[] = [];
const listeners = new Set<() => void>();

export function record(e: Omit<TraceEntry, "id" | "ts">): TraceEntry {
  const entry: TraceEntry = { ...e, id: ++seq, ts: Date.now() };
  entries = [entry, ...entries].slice(0, MAX);
  listeners.forEach((l) => l());
  return entry;
}

export const getTraces = (): TraceEntry[] => entries;

/** Most recent trace whose path contains `needle`. */
export function latestFor(needle: string): TraceEntry | undefined {
  return entries.find((e) => e.path.includes(needle));
}

export function clearTraces(): void {
  entries = [];
  listeners.forEach((l) => l());
}

export function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

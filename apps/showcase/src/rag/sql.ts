/**
 * Small helpers the RAG apps share for talking SQL to NebulaDB.
 *
 * NebulaDB SQL needs exactly one `semantic_match(...)` per query, then
 * residual metadata filters (`=`, `IN`, `<`/`<=`/`>`/`>=`/`<>`,
 * `array_contains`). Apps build those strings from user input, so every
 * literal goes through `lit()`.
 */
import { api, type SqlRow } from "../api";

/** Quote a value as a SQL literal. Strings are single-quoted with `''` escaping. */
export function lit(v: string | number | boolean): string {
  if (typeof v === "number") {
    if (!Number.isFinite(v)) throw new Error(`non-finite SQL literal: ${v}`);
    return String(v);
  }
  if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
  return `'${v.replace(/'/g, "''")}'`;
}

/** The semantic_match query text: flattened to one line and capped — the
 *  embedder only needs the gist, and a 5k-char literal makes SQL unreadable. */
export function semanticText(s: string, max = 600): string {
  const flat = s.replace(/\s+/g, " ").trim();
  return flat.length > max ? flat.slice(0, max) : flat;
}

export interface SqlRun {
  sql: string;
  rows: SqlRow[];
  tookMs: number;
}

export async function runSql(sql: string): Promise<SqlRun> {
  const started = performance.now();
  const r = await api.sql(sql);
  return { sql, rows: r.rows, tookMs: Math.round(performance.now() - started) };
}

/**
 * Cosine distance (0 = identical, 2 = opposite) → 0..1 similarity.
 * The server's `score` is a distance under the index metric.
 */
export function similarity(distance: number): number {
  return Math.max(0, Math.min(1, 1 - distance));
}

/**
 * Rescale values to 0..1 across one result set. Raw cosine similarity
 * between short docs bunches in a narrow band (e.g. 0.55–0.72); a
 * min-max spread makes the facet comparable with the deterministic ones.
 * A single value (or all-equal set) maps to 1.
 */
export function minMax(values: number[]): (v: number) => number {
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  if (!Number.isFinite(lo) || hi - lo < 1e-9) return () => 1;
  return (v) => (v - lo) / (hi - lo);
}

/** Read a metadata field from a SQL row (`SELECT *` puts metadata keys in `fields`). */
export function field<T = unknown>(row: SqlRow, key: string): T | undefined {
  const f = row.fields as Record<string, unknown>;
  if (key in f) return f[key] as T;
  const meta = f.metadata as Record<string, unknown> | undefined;
  return meta?.[key] as T | undefined;
}

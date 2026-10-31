/**
 * Small pieces every Source Supplies tab shares: the demo-data note,
 * multi-bucket seeding, scenario chips, GBP / date formatting and a
 * helper to read a SQL row's metadata.
 */
import { useEffect, useState } from "react";
import type { SqlRow } from "../api";
import { ensureSeeded } from "../rag/seed";
import { sseStream } from "../sse";
import type { ContextDoc } from "../rag/GroundedChat";
import { SEEDS, type SeedKey } from "./data";

export function DemoNote({ extra }: { extra?: string }) {
  return (
    <div className="rounded-md border border-amber-300/60 bg-amber-50 px-3 py-1.5 text-xs text-amber-900 dark:border-warn/30 dark:bg-warn/10 dark:text-muted">
      <span className="font-semibold">Demo data — not real Source Supplies records.</span> Products, prices, stock,
      customers, orders and safety sheets are made up. Prices exclude VAT.{extra ? ` ${extra}` : ""}
    </div>
  );
}

export interface SeedState {
  ready: boolean;
  failed: boolean;
  msg: string;
  /** Docs written by this open (0 when every bucket was already current). */
  written: number;
}

/** Seed the listed buckets (idempotent: a second open writes 0 docs). */
export function useSeeds(keys: SeedKey[]): SeedState {
  const id = keys.join(",");
  const [state, setState] = useState<SeedState>({ ready: false, failed: false, msg: "Checking demo data…", written: 0 });
  useEffect(() => {
    let cancelled = false;
    (async () => {
      let written = 0;
      let total = 0;
      try {
        for (const k of id.split(",") as SeedKey[]) {
          const s = SEEDS[k];
          const r = await ensureSeeded(s.bucket, s.docs, s.version, (d, t) => {
            if (!cancelled) setState((p) => ({ ...p, msg: `Seeding ${s.noun} ${d}/${t}…` }));
          });
          written += r.written;
          total += r.total;
        }
        if (cancelled) return;
        setState({
          ready: true,
          failed: false,
          written,
          msg: written > 0 ? `Seeded ${written} demo docs.` : `${total} demo docs ready (0 written).`,
        });
      } catch (e) {
        if (!cancelled) setState({ ready: false, failed: true, written, msg: `Seeding failed: ${(e as Error).message}` });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [id]);
  return state;
}

export function SeedChip({ seed }: { seed: SeedState }) {
  return (
    <span className="metachip">
      <span className={`dot ${seed.ready ? "dot-ok" : seed.failed ? "dot-bad" : "dot-warn animate-pulseok"}`} />
      {seed.msg}
    </span>
  );
}

export function Chips<T extends { label: string }>({
  items,
  active,
  onPick,
  label = "Try",
}: {
  items: T[];
  active?: string;
  onPick: (item: T) => void;
  label?: string;
}) {
  return (
    <div className="flex flex-wrap gap-1.5 mb-3">
      <span className="eyebrow self-center mr-1">{label}</span>
      {items.map((s) => (
        <button
          key={s.label}
          type="button"
          onClick={() => onPick(s)}
          className={`rounded-full border px-2.5 py-1 text-xs transition-colors ${
            active === s.label
              ? "border-accent text-accent bg-accent/10"
              : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent/60"
          }`}
        >
          {s.label}
        </button>
      ))}
    </div>
  );
}

const GBP = new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" });
export function gbp(n: number): string {
  return GBP.format(n);
}

/** Small amounts (cost per use) need more than 2 decimals to be readable. */
export function gbpFine(n: number): string {
  if (n >= 1) return GBP.format(n);
  return `£${n.toFixed(n < 0.1 ? 3 : 2)}`;
}

export function kg(n: number): string {
  return `${n >= 100 ? Math.round(n) : n.toFixed(1)} kg CO2e`;
}

export function ukDate(iso: string): string {
  const d = new Date(`${iso.slice(0, 10)}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
}

export function ukMonth(ym: string): string {
  const d = new Date(`${ym}-01T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return ym;
  return d.toLocaleDateString("en-GB", { month: "short", year: "numeric", timeZone: "UTC" });
}

/** A row's metadata flattened (`SELECT *` nests it under `fields.metadata`). */
export function metaOf(row: SqlRow): Record<string, unknown> {
  const { metadata, ...top } = row.fields as Record<string, unknown>;
  return { ...(metadata as Record<string, unknown> | undefined), ...top };
}

export function str(v: unknown): string {
  return v === undefined || v === null ? "" : String(v);
}

export function num(v: unknown, d = 0): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

export function compact(s: string, n = 1400): string {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

/**
 * One grounded, non-interactive answer (for batch drafting, e.g. one per
 * tender question). Same endpoint and limits as GroundedChat.
 */
export async function draftAnswer(
  query: string,
  context: ContextDoc[],
  system: string,
  signal?: AbortSignal
): Promise<string> {
  let out = "";
  for await (const frame of sseStream(
    "/api/v1/ai/rag",
    {
      query,
      stream: true,
      system,
      context: context.slice(0, 50).map((d) => ({ id: d.id, text: d.text, bucket: d.bucket, metadata: d.metadata ?? {} })),
    },
    signal
  )) {
    if (frame.event === "answer_delta") out += frame.data;
    else if (frame.event === "error") throw new Error(frame.data);
    else if (frame.event === "done") break;
  }
  return out.trim();
}

/** Lower-case content words, for deterministic overlap checks that still work with the mock embedder. */
const STOP = new Set(
  "a an and are as at be by can do does for from has have how i if in into is it its me my of on or our should so that the their them then there these this to us was we what when where which who why will with you your".split(
    " "
  )
);
export function words(s: string): string[] {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9\s-]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length > 2 && !STOP.has(w));
}

/** Share of the query's content words that appear in `text` (0..1). */
export function overlap(query: string, text: string): number {
  const q = [...new Set(words(query))];
  if (q.length === 0) return 0;
  const t = new Set(words(text));
  return q.filter((w) => t.has(w) || t.has(w.replace(/s$/, ""))).length / q.length;
}

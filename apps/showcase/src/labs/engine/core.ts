/**
 * Pure helpers for the Labs (no React, no network) so they run under
 * `node --test`: BM25, retrieval metrics, URL state, SQL building, the
 * numbers-in-answer grounding check, citation parsing and PCA.
 */
import type { FilterField, LabId, Pack, PackDoc } from "../types.ts";

// ---------------------------------------------------------------- tokens

/** Unicode-aware tokens: letters/digits of any script, lower-cased. Keeps codes like "eng2041" and "e_rate_limit". */
export function tokenize(text: string): string[] {
  return (text.toLowerCase().normalize("NFC").match(/[\p{L}\p{M}\p{N}_]+(?:[-'][\p{L}\p{M}\p{N}_]+)*/gu) ?? []).filter(
    (t) => t.length > 1 || /\p{N}/u.test(t)
  );
}

const STOP = new Set(
  "the a an and or of to in on for with is are was were be been it this that what which who how do does did can could should would my our your their at by from as about into than then so if not no".split(
    " "
  )
);

// ---------------------------------------------------------------- BM25

export interface Bm25Hit {
  id: string;
  score: number;
  /** term → contribution, for the "why did it rank" bar. */
  terms: Record<string, number>;
}

export class Bm25 {
  private docs: { id: string; tf: Map<string, number>; len: number }[] = [];
  private df = new Map<string, number>();
  private avgLen = 0;
  private k1 = 1.2;
  private b = 0.75;
  constructor(docs: { id: string; text: string }[]) {
    for (const d of docs) {
      const toks = tokenize(d.text);
      const tf = new Map<string, number>();
      for (const t of toks) tf.set(t, (tf.get(t) ?? 0) + 1);
      for (const t of tf.keys()) this.df.set(t, (this.df.get(t) ?? 0) + 1);
      this.docs.push({ id: d.id, tf, len: toks.length });
    }
    this.avgLen = this.docs.reduce((s, d) => s + d.len, 0) / Math.max(1, this.docs.length);
  }
  search(query: string, k = 10): Bm25Hit[] {
    const n = this.docs.length;
    const qt = [...new Set(tokenize(query).filter((t) => !STOP.has(t)))];
    const out: Bm25Hit[] = [];
    for (const d of this.docs) {
      let score = 0;
      const terms: Record<string, number> = {};
      for (const t of qt) {
        const f = d.tf.get(t);
        if (!f) continue;
        const df = this.df.get(t) ?? 0;
        const idf = Math.log(1 + (n - df + 0.5) / (df + 0.5));
        const c = (idf * f * (this.k1 + 1)) / (f + this.k1 * (1 - this.b + (this.b * d.len) / this.avgLen));
        terms[t] = c;
        score += c;
      }
      if (score > 0) out.push({ id: d.id, score, terms });
    }
    return out.sort((a, b) => b.score - a.score).slice(0, k);
  }
}

// ---------------------------------------------------------------- metrics

export function hitAt(ranked: string[], expect: string[], k: number): number {
  return ranked.slice(0, k).some((id) => expect.includes(id)) ? 1 : 0;
}

export function reciprocalRank(ranked: string[], expect: string[]): number {
  const i = ranked.findIndex((id) => expect.includes(id));
  return i < 0 ? 0 : 1 / (i + 1);
}

export function recallAt(ranked: string[], expect: string[], k: number): number {
  if (!expect.length) return 0;
  const top = new Set(ranked.slice(0, k));
  return expect.filter((e) => top.has(e)).length / expect.length;
}

export interface Score {
  n: number;
  hit1: number;
  hit5: number;
  mrr: number;
  recall10: number;
}

export function score(runs: { ranked: string[]; expect: string[] }[]): Score {
  const n = runs.length || 1;
  const sum = (f: (r: { ranked: string[]; expect: string[] }) => number) => runs.reduce((s, r) => s + f(r), 0) / n;
  return {
    n: runs.length,
    hit1: sum((r) => hitAt(r.ranked, r.expect, 1)),
    hit5: sum((r) => hitAt(r.ranked, r.expect, 5)),
    mrr: sum((r) => reciprocalRank(r.ranked, r.expect)),
    recall10: sum((r) => recallAt(r.ranked, r.expect, 10)),
  };
}

export function percentile(xs: number[], p: number): number {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.max(0, Math.ceil((p / 100) * s.length) - 1))];
}

/** Overlap of two top-k id lists (agreement with a reference run). */
export function overlapAt(a: string[], ref: string[], k: number): number {
  const r = new Set(ref.slice(0, k));
  if (!r.size) return 0;
  return a.slice(0, k).filter((x) => r.has(x)).length / r.size;
}

// ---------------------------------------------------------------- URL state

export interface LabState {
  lab: LabId | null;
  pack: string;
  q?: string;
  models?: string[];
  mode?: string;
  k?: number;
  ef?: number;
  persona?: string;
  f?: Record<string, string>;
}

/** `#/labs/<lab>?pack=…&q=…` ⇄ LabState. Unknown params are ignored. */
export function parseLabHash(hash: string, defaultPack = "campus"): LabState | null {
  const m = hash.match(/^#\/labs(?:\/([a-z]+))?(?:\?(.*))?$/);
  if (!m) return null;
  const p = new URLSearchParams(m[2] ?? "");
  const num = (v: string | null) => (v && /^\d+$/.test(v) ? Number(v) : undefined);
  const f: Record<string, string> = {};
  for (const [k, v] of p) if (k.startsWith("f.")) f[k.slice(2)] = v;
  return {
    lab: (m[1] as LabId) || null,
    pack: p.get("pack") || defaultPack,
    q: p.get("q") ?? undefined,
    models: p.get("models")?.split(",").filter(Boolean),
    mode: p.get("mode") ?? undefined,
    k: num(p.get("k")),
    ef: num(p.get("ef")),
    persona: p.get("persona") ?? undefined,
    f: Object.keys(f).length ? f : undefined,
  };
}

export function labHash(s: LabState): string {
  const p = new URLSearchParams();
  p.set("pack", s.pack);
  if (s.q) p.set("q", s.q);
  if (s.models?.length) p.set("models", s.models.join(","));
  if (s.mode) p.set("mode", s.mode);
  if (s.k !== undefined) p.set("k", String(s.k));
  if (s.ef !== undefined) p.set("ef", String(s.ef));
  if (s.persona) p.set("persona", s.persona);
  for (const [k, v] of Object.entries(s.f ?? {})) if (v) p.set(`f.${k}`, v);
  return `#/labs${s.lab ? `/${s.lab}` : ""}?${p.toString()}`;
}

// ---------------------------------------------------------------- SQL

/** SQL string literal with '' escaping. */
export function lit(v: string): string {
  return `'${v.replace(/'/g, "''")}'`;
}

/** One predicate for a filter value, or null when unset. */
export function predicate(f: FilterField, value: string | undefined): string | null {
  if (value === undefined || value === "") return null;
  switch (f.op) {
    case "eq":
      return `${f.field} = ${lit(value)}`;
    case "contains":
      return `array_contains(${f.field}, ${lit(value)})`;
    case "max":
      return /^-?\d+(\.\d+)?$/.test(value) ? `${f.field} <= ${value}` : null;
    case "bool":
      return value === "true" ? `${f.field} = TRUE` : value === "false" ? `${f.field} = FALSE` : null;
  }
}

export function filterSql(pack: Pack, q: string, values: Record<string, string>, limit: number, extra: Record<string, string> = {}): string {
  const preds = pack.filters.map((f) => predicate(f, values[f.field])).filter((x): x is string => !!x);
  for (const [k, v] of Object.entries(extra)) preds.push(`${k} = ${lit(v)}`);
  return [`SELECT * FROM ${pack.bucket}`, ` WHERE semantic_match(text, ${lit(q)})`, ...preds.map((p) => `   AND ${p}`), ` LIMIT ${limit}`].join("\n");
}

/**
 * NebulaDB SQL rules the labs rely on (one semantic_match, the pack's
 * bucket, a LIMIT, no OR / JOIN / subquery / CTE, explicit columns with
 * GROUP BY). Returns problems; empty means OK to run.
 */
export function checkSql(sql: string, bucket: string): string[] {
  const errs: string[] = [];
  const noStrings = sql.replace(/'([^']|'')*'/g, "''");
  if (!/^\s*SELECT\b/i.test(sql)) errs.push("must be a SELECT");
  if ((noStrings.match(/semantic_match\s*\(/gi) ?? []).length !== 1) errs.push("needs exactly one semantic_match(text, '…')");
  if (!new RegExp(`\\bFROM\\s+${bucket}\\b`, "i").test(noStrings)) errs.push(`must select FROM ${bucket}`);
  if (/\bOR\b/i.test(noStrings)) errs.push("OR is not supported");
  if (/\bJOIN\b|\(\s*SELECT\b|\bWITH\b/i.test(noStrings)) errs.push("no JOIN / subquery / CTE");
  if (!/\bLIMIT\s+\d+/i.test(noStrings)) errs.push("needs a LIMIT");
  if (/\bGROUP BY\b/i.test(noStrings) && /SELECT\s+\*/i.test(noStrings)) errs.push("SELECT * is not allowed with GROUP BY");
  if (!/\bGROUP BY\b/i.test(noStrings) && /\b(COUNT|SUM|AVG|MIN|MAX)\s*\(/i.test(noStrings)) errs.push("aggregates need a GROUP BY (group by the filtered column to get one row)");
  if (/;\s*\S/.test(noStrings) || /\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE)\b/i.test(noStrings)) errs.push("read-only single statement only");
  return errs;
}

/** First JSON object in a model reply (models like to wrap JSON in prose or fences). */
export function extractJson(text: string): unknown {
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  const body = fenced ? fenced[1] : text;
  const start = body.indexOf("{");
  if (start < 0) throw new Error("no JSON object in the reply");
  let depth = 0;
  let inStr = false;
  for (let i = start; i < body.length; i++) {
    const c = body[i];
    if (inStr) {
      if (c === "\\") i++;
      else if (c === '"') inStr = false;
    } else if (c === '"') inStr = true;
    else if (c === "{") depth++;
    else if (c === "}" && --depth === 0) return JSON.parse(body.slice(start, i + 1));
  }
  throw new Error("unterminated JSON object in the reply");
}

/** Does a doc pass these filter values? (client-side ground truth for the Filters lab) */
export function docPasses(pack: Pack, doc: PackDoc, values: Record<string, string>, extra: Record<string, string> = {}): boolean {
  for (const f of pack.filters) {
    const v = values[f.field];
    if (v === undefined || v === "") continue;
    const m = doc.metadata[f.field];
    if (f.op === "eq" && String(m) !== v) return false;
    if (f.op === "contains" && !(Array.isArray(m) && m.includes(v))) return false;
    if (f.op === "max" && !(typeof m === "number" && m <= Number(v))) return false;
    if (f.op === "bool" && String(m) !== v) return false;
  }
  for (const [k, v] of Object.entries(extra)) if (String(doc.metadata[k]) !== v) return false;
  return true;
}

// ---------------------------------------------------------------- grounding

/** Numbers in a text, normalised (commas, currency, % stripped). Citation markers [n] are excluded. */
export function numbersIn(text: string): string[] {
  const clean = text.replace(/\[\d+(?:\s*,\s*\d+)*\]/g, " ");
  // A leading "-" counts only when it isn't glued to a word (ids like cn-3110-02).
  return (clean.match(/(?:(?<!\w)-)?(?<!\w)\d[\d,]*(?:\.\d+)?/g) ?? []).map((n) => n.replace(/,/g, "")).map((n) => (n.includes(".") ? String(Number(n)) : n));
}

/**
 * Every number in `answer` must appear in the evidence (rows/chunks) or in
 * the question. Returns the numbers that don't — those are unsupported.
 * Single digits 0–9 are ignored (list numbering, "step 2").
 */
export function unsupportedNumbers(answer: string, evidence: string[], question = ""): string[] {
  const have = new Set([...evidence.flatMap(numbersIn), ...numbersIn(question)]);
  // Also accept values rounded to 0, 1 or 2 decimals.
  for (const n of [...have]) {
    const x = Number(n);
    if (Number.isFinite(x)) for (const d of [0, 1, 2]) have.add(String(Number(x.toFixed(d))));
  }
  return [...new Set(numbersIn(answer).filter((n) => !/^-?\d$/.test(n) && !have.has(n) && !have.has(String(Number(n)))))];
}

export interface Claim {
  text: string;
  cites: number[];
}

/** Split an answer into sentences with their [n] citations. */
export function claims(answer: string): Claim[] {
  const parts = answer
    .replace(/\n+/g, " \n ")
    .split(/(?<=[.!?])\s+|\n/)
    .map((s) => s.trim())
    .filter((s) => s.length > 2);
  return parts.map((text) => ({
    text,
    cites: [...text.matchAll(/\[(\d+(?:\s*,\s*\d+)*)\]/g)].flatMap((m) => m[1].split(",").map((x) => Number(x.trim()))),
  }));
}

// ---------------------------------------------------------------- RAG prompt

export function ragPrompt(question: string, chunks: { id: string; text: string }[], domain: string): { system: string; user: string } {
  const system = [
    "Answer ONLY from the numbered sources below. After every sentence that uses a source, cite it like [1] or [2,3].",
    "If the sources do not contain the answer, reply exactly: \"Not in the documents.\" Do not use outside knowledge.",
    "Quote numbers exactly as they appear in the sources.",
    domain,
  ].join(" ");
  const ctx = chunks.map((c, i) => `[${i + 1}] (${c.id}) ${c.text}`).join("\n");
  return { system, user: `Sources:\n${ctx}\n\nQuestion: ${question}` };
}

// ---------------------------------------------------------------- PCA

export interface PcaFit {
  mean: number[];
  comps: number[][];
}

/** Project one vector onto a fitted PCA basis. */
export function project(v: number[], fit: PcaFit): number[] {
  return fit.comps.map((p) => v.reduce((s, x, j) => s + (x - fit.mean[j]) * p[j], 0));
}

/** Project row vectors to `dims` principal components (power iteration with deflation). */
export function pca(rows: number[][], dims = 2, iters = 60): number[][] {
  const fit = pcaFit(rows, dims, iters);
  return rows.map((r) => project(r, fit));
}

export function pcaFit(rows: number[][], dims = 2, iters = 60): PcaFit {
  const n = rows.length;
  if (!n) return { mean: [], comps: [] };
  const d = rows[0].length;
  const mean = new Array(d).fill(0);
  for (const r of rows) for (let j = 0; j < d; j++) mean[j] += r[j] / n;
  const X = rows.map((r) => r.map((v, j) => v - mean[j]));
  const comps: number[][] = [];
  for (let c = 0; c < dims; c++) {
    let v = Array.from({ length: d }, (_, j) => Math.sin(j * 12.9898 + c * 78.233));
    for (let it = 0; it < iters; it++) {
      // w = Xᵀ X v
      const xv = X.map((r) => r.reduce((s, x, j) => s + x * v[j], 0));
      const w = new Array(d).fill(0);
      X.forEach((r, i) => {
        for (let j = 0; j < d; j++) w[j] += r[j] * xv[i];
      });
      for (const p of comps) {
        const dot = w.reduce((s, x, j) => s + x * p[j], 0);
        for (let j = 0; j < d; j++) w[j] -= dot * p[j];
      }
      const norm = Math.hypot(...w) || 1;
      v = w.map((x) => x / norm);
    }
    comps.push(v);
  }
  return { mean, comps };
}

export function cosine(a: number[], b: number[]): number {
  let dot = 0,
    na = 0,
    nb = 0;
  for (let i = 0; i < a.length; i++) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  return dot / (Math.sqrt(na * nb) || 1);
}

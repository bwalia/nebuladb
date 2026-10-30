/**
 * Config-driven shell for the industry use-case demos.
 *
 * Every demo is the same pipeline — seed a versioned corpus, pick a
 * scenario, run one `semantic_match` + metadata filters, then either
 * hand the shortlist to a grounded chat (RAG) or to a deterministic
 * `Insight` panel (vector-only: cache hit/miss, novelty, theme mix).
 * A spec supplies the domain; this file owns the mechanics.
 */
import { useCallback, useEffect, useMemo, useRef, useState, type ComponentType } from "react";
import { ApiError, type SqlRow } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded, type SeedDoc } from "../rag/seed";
import { field, lit, minMax, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { ResultsMap, type MapHit } from "../components/ResultsMap";

export interface UseCaseHit {
  id: string;
  title: string;
  sub: string;
  /** Raw 0..1 cosine similarity. */
  sim: number;
  /** Similarity min-max spread across this result set. */
  rel: number;
  text: string;
  meta: Record<string, unknown>;
}

/** A metadata filter rendered as a control and compiled to one SQL clause. */
export type FilterSpec =
  | { key: string; label: string; kind: "select"; options: readonly string[] }
  | { key: string; label: string; kind: "max" | "min"; step?: number; prefix?: string };

export type FilterValues = Record<string, string>;

export interface Scenario {
  label: string;
  q: string;
  filters?: FilterValues;
}

export interface UseCaseSpec {
  bucket: string;
  seedVersion: string;
  docs: SeedDoc[];
  /** Plural noun for seed status, e.g. "guidelines". */
  noun: string;
  title: string;
  subtitle: string;
  queryLabel: string;
  placeholder: string;
  scenarios: Scenario[];
  filters?: FilterSpec[];
  limit?: number;
  /** Metadata keys for the hit's heading and subline. */
  idKey: string;
  titleKey: string;
  subKeys: string[];
  resultsTitle: string;
  /** RAG mode: grounded chat over the top hits. */
  chat?: {
    title: string;
    subtitle: string;
    system: string;
    suggestions: string[];
    placeholder: string;
    topK?: number;
  };
  /** Vector-only mode: a deterministic panel over the hits. */
  Insight?: ComponentType<{ hits: UseCaseHit[]; query: string }>;
  insightTitle?: string;
  insightSubtitle?: string;
  /** Reference-architecture blueprint id (components/RagArchitecture). */
  blueprint?: string;
  /** Plot the hits on a map: metadata keys for coordinates, plus a short chip label. */
  map?: { latKey: string; lngKey: string; label: (h: UseCaseHit) => string };
}

function compact(s: string, n = 1400) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

export function buildSql(spec: UseCaseSpec, query: string, values: FilterValues): string {
  let sql = `SELECT * FROM ${spec.bucket}\nWHERE semantic_match(text, ${lit(semanticText(query))})`;
  for (const f of spec.filters ?? []) {
    const v = values[f.key];
    if (!v) continue;
    if (f.kind === "select") sql += `\n  AND ${f.key} = ${lit(v)}`;
    else {
      const n = Number(v);
      if (Number.isFinite(n)) sql += `\n  AND ${f.key} ${f.kind === "max" ? "<=" : ">="} ${lit(n)}`;
    }
  }
  return sql + `\nLIMIT ${spec.limit ?? 12}`;
}

function toHit(spec: UseCaseSpec, row: SqlRow): Omit<UseCaseHit, "rel"> {
  // `SELECT *` may put metadata at the top of `fields` or under `fields.metadata`.
  const { metadata, text: _text, ...top } = row.fields as Record<string, unknown>;
  const meta: Record<string, unknown> = { ...(metadata as Record<string, unknown> | undefined), ...top };
  return {
    id: String(field(row, spec.idKey) ?? row.id),
    title: String(field(row, spec.titleKey) ?? ""),
    sub: spec.subKeys
      .map((k) => field(row, k))
      .filter((v) => v !== undefined && v !== "")
      .join(" · "),
    sim: similarity(row.score),
    text: String(field(row, "text") ?? ""),
    meta,
  };
}

export function UseCaseDemo({ spec }: { spec: UseCaseSpec }) {
  const [seedMsg, setSeedMsg] = useState(`Checking ${spec.noun}…`);
  const [seeded, setSeeded] = useState(false);
  const [query, setQuery] = useState(spec.scenarios[0].q);
  const [values, setValues] = useState<FilterValues>(spec.scenarios[0].filters ?? {});
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<UseCaseHit[]>([]);
  const [ranQuery, setRanQuery] = useState("");
  const [runId, setRunId] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const rowRefs = useRef<Record<string, HTMLLIElement | null>>({});

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await ensureSeeded(spec.bucket, spec.docs, spec.seedVersion, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding ${spec.noun} ${d}/${t}…`);
        });
        if (cancelled) return;
        setSeeded(true);
        setSeedMsg(r.written > 0 ? `Seeded ${r.written} ${spec.noun}.` : `${r.total} ${spec.noun} ready.`);
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [spec]);

  const run = async () => {
    const q = query.trim();
    if (!q) return;
    setErr(null);
    setBusy(true);
    const sql = buildSql(spec, q, values);
    const trace: PipelineStep[] = [{ label: "Retrieval (SQL filters + vector)", detail: sql, status: "running" }];
    setSteps([...trace]);
    try {
      const r = await runSql(sql);
      const raw = r.rows.map((row) => toHit(spec, row)).sort((a, b) => b.sim - a.sim);
      const norm = minMax(raw.map((h) => h.sim));
      trace[0] = { ...trace[0], rowsOut: raw.length, ms: r.tookMs, status: "ok" };
      setSteps([...trace]);
      setHits(raw.map((h) => ({ ...h, rel: norm(h.sim) })));
      setRanQuery(q);
      setSelected(null);
      setRunId((n) => n + 1);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const getContext = useCallback(
    async (): Promise<ContextDoc[]> =>
      hits.slice(0, spec.chat?.topK ?? 6).map((h) => ({
        id: h.id,
        label: h.id,
        bucket: spec.bucket,
        text: compact(h.text),
        metadata: { title: h.title },
      })),
    [hits, spec]
  );

  const mapHits = useMemo<MapHit[]>(() => {
    const m = spec.map;
    if (!m) return [];
    return hits.flatMap((h, i) => {
      const lat = Number(h.meta[m.latKey]);
      const lng = Number(h.meta[m.lngKey]);
      if (!Number.isFinite(lat) || !Number.isFinite(lng)) return [];
      return [{ id: h.id, rank: i + 1, lat, lng, label: m.label(h), title: h.title }];
    });
  }, [hits, spec]);

  const selectFromMap = (id: string) => {
    setSelected(id);
    rowRefs.current[id]?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  };

  const Insight = spec.Insight;
  const seedFailed = seedMsg.startsWith("Seeding failed");
  const filterLabel = useMemo(
    () => (spec.filters ?? []).map((f) => f.label.toLowerCase()).join(" / "),
    [spec]
  );

  return (
    <div className="space-y-4">
      {spec.blueprint && <ArchitecturePanel id={spec.blueprint} />}
      <Panel
        title={spec.title}
        subtitle={spec.subtitle}
        action={
          <span className="metachip">
            <span className={`dot ${seeded ? "dot-ok" : seedFailed ? "dot-bad" : "dot-warn animate-pulseok"}`} />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-1.5 mb-3">
          <span className="eyebrow self-center mr-1">Scenarios</span>
          {spec.scenarios.map((s) => (
            <button
              key={s.label}
              type="button"
              onClick={() => {
                setQuery(s.q);
                setValues(s.filters ?? {});
              }}
              className={`rounded-full border px-2.5 py-1 text-xs transition-colors ${
                query === s.q
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent/60"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">{spec.queryLabel}</span>
          <textarea
            className="code-input w-full h-24"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={spec.placeholder}
          />
        </label>
        <div className="flex flex-wrap gap-3 items-end">
          {(spec.filters ?? []).map((f) => (
            <label key={f.key} className="text-xs">
              <span className="block font-medium mb-1">{f.label}</span>
              {f.kind === "select" ? (
                <select
                  className="code-input !py-1.5 !text-sm"
                  value={values[f.key] ?? ""}
                  onChange={(e) => setValues((v) => ({ ...v, [f.key]: e.target.value }))}
                >
                  <option value="">any</option>
                  {f.options.map((o) => (
                    <option key={o} value={o}>
                      {o}
                    </option>
                  ))}
                </select>
              ) : (
                <input
                  type="number"
                  step={f.step ?? 1}
                  className="code-input !py-1.5 !text-sm w-28"
                  placeholder={f.kind === "max" ? "no max" : "no min"}
                  value={values[f.key] ?? ""}
                  onChange={(e) => setValues((v) => ({ ...v, [f.key]: e.target.value }))}
                />
              )}
            </label>
          ))}
          <button className="btn" disabled={busy || !seeded || !query.trim()} onClick={() => void run()}>
            {busy ? <Spinner label="searching…" /> : "Run"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {runId > 0 && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
          <Panel title={spec.resultsTitle} subtitle={`${hits.length} matches · ranked by semantic similarity`}>
            {hits.length === 0 ? (
              <p className="text-sm text-gray-500 dark:text-muted">
                No hits{filterLabel ? ` — loosen the ${filterLabel} filter` : ""}.
              </p>
            ) : (
              <>
              {spec.map && <ResultsMap hits={mapHits} selected={selected} onSelect={selectFromMap} />}
              <ul className={`space-y-2 ${spec.map ? "max-h-[28rem] overflow-y-auto pr-1" : ""}`}>
                {hits.map((h, i) => (
                  <li
                    key={h.id}
                    ref={(el) => {
                      rowRefs.current[h.id] = el;
                    }}
                    onClick={spec.map ? () => setSelected(h.id) : undefined}
                    className={`rounded-md border px-3 py-2 transition-colors ${
                      spec.map ? "cursor-pointer" : ""
                    } ${
                      selected === h.id
                        ? "border-accent bg-accent/10"
                        : "border-gray-200 dark:border-edge"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <div className="min-w-0">
                        <div className="text-sm font-medium">
                          {spec.map && <span className="text-accent mr-1">{i + 1}.</span>}
                          {h.id} · {h.title}
                        </div>
                        <div className="text-xs text-gray-500 dark:text-muted">{h.sub}</div>
                      </div>
                      <Stat label="sim" value={`${Math.round(h.rel * 100)}%`} />
                    </div>
                    <p className="mt-2 text-xs text-gray-600 dark:text-muted line-clamp-3">{h.text}</p>
                  </li>
                ))}
              </ul>
              </>
            )}
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </Panel>
          {spec.chat ? (
            <GroundedChat
              title={spec.chat.title}
              subtitle={spec.chat.subtitle}
              system={spec.chat.system}
              getContext={getContext}
              resetKey={String(runId)}
              disabledReason={hits.length === 0 ? "Run a search with at least one hit first." : undefined}
              suggestions={spec.chat.suggestions}
              placeholder={spec.chat.placeholder}
            />
          ) : (
            Insight && (
              <Panel title={spec.insightTitle ?? "Insight"} subtitle={spec.insightSubtitle}>
                <Insight hits={hits} query={ranQuery} />
              </Panel>
            )
          )}
        </div>
      )}
    </div>
  );
}

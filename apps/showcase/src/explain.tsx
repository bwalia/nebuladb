import { useState, type ReactNode } from "react";
import type { Explain, ExplainStage, HitExplain } from "./api";
import { JsonView, Panel, Stat } from "./components";
import { JsonTree } from "./tree";

/**
 * Renders an `Explain` — how a SQL / search / RAG query arrived at its
 * result. The server's analogue of Postgres `EXPLAIN ANALYZE`:
 *
 * - Pipeline: every stage in order with rows in → out, a time bar
 *   proportional to its share of the total, and its structured attrs.
 * - Why ranked: per-result breakdown — vector distance/similarity, BM25
 *   term contributions, fused score, SQL filter checks.
 * - Plan / Prompt / Text: the typed SQL plan tree, the exact LLM prompt,
 *   and the psql-style text rendering (copyable).
 *
 * `embedded` drops the card chrome so the panel can nest inside another
 * card (the RAG turn view).
 */
export type ExplainView = "pipeline" | "hits" | "plan" | "prompt" | "text";

export function ExplainPanel({
  explain,
  embedded = false,
  initialView = "pipeline",
}: {
  explain: Explain;
  embedded?: boolean;
  /** Tab shown first. Falls back to Pipeline if that tab has no data. */
  initialView?: ExplainView;
}) {
  type View = ExplainView;
  const views: Array<{ id: View; label: string; show: boolean }> = [
    { id: "pipeline", label: "Pipeline", show: true },
    { id: "hits", label: `Why ranked (${explain.hits.length})`, show: explain.hits.length > 0 },
    { id: "plan", label: "Plan", show: explain.plan !== null && explain.plan !== undefined },
    { id: "prompt", label: "Prompt", show: !!explain.prompt },
    { id: "text", label: "Text", show: explain.text.length > 0 },
  ];
  const [view, setView] = useState<View>(() =>
    views.some((v) => v.id === initialView && v.show) ? initialView : "pipeline"
  );

  const title = explain.analyzed ? "EXPLAIN ANALYZE" : "EXPLAIN";
  const subtitle = explain.analyzed
    ? `How this ${explain.kind === "sql" ? "query" : explain.kind} arrived at its result — the query ran, timings are real`
    : "Plan only — nothing was executed, so there are no row counts or timings";

  const body = (
    <div className="space-y-3">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <p className="text-sm text-gray-800 dark:text-ink max-w-3xl">{explain.summary}</p>
        <div className="flex gap-3 shrink-0">
          {explain.analyzed && <Stat label="total" value={formatUs(explain.total_us)} />}
          <Stat label="stages" value={explain.stages.length} />
        </div>
      </div>

      {explain.notes.length > 0 && (
        <div className="space-y-1.5">
          {explain.notes.map((n, i) => (
            <NoteBanner key={i} level={n.level}>
              {n.message}
            </NoteBanner>
          ))}
        </div>
      )}

      <div className="flex flex-wrap border-b border-gray-200 dark:border-edge" role="tablist">
        {views
          .filter((v) => v.show)
          .map((v) => (
            <button
              key={v.id}
              role="tab"
              aria-selected={view === v.id}
              className={`tab-btn !py-1.5 !text-xs ${view === v.id ? "tab-btn-active" : ""}`}
              onClick={() => setView(v.id)}
            >
              {v.label}
            </button>
          ))}
      </div>

      {view === "pipeline" && (
        <Pipeline stages={explain.stages} totalUs={explain.total_us} analyzed={explain.analyzed} />
      )}
      {view === "hits" && <HitsTable hits={explain.hits} />}
      {view === "plan" && <JsonView value={explain.plan} />}
      {view === "prompt" && explain.prompt && (
        <CopyBlock text={explain.prompt} caption={`${explain.prompt.length.toLocaleString()} chars sent to the LLM`} wrap />
      )}
      {view === "text" && <CopyBlock text={explain.text.join("\n")} caption="psql-style EXPLAIN output" />}
    </div>
  );

  if (embedded) return body;
  return (
    <Panel title={title} subtitle={subtitle}>
      {body}
    </Panel>
  );
}

// ---- Pipeline -------------------------------------------------------------

function Pipeline({
  stages,
  totalUs,
  analyzed,
}: {
  stages: ExplainStage[];
  totalUs: number;
  analyzed: boolean;
}) {
  if (stages.length === 0) {
    return <p className="text-xs text-gray-500 dark:text-muted">No stages reported.</p>;
  }
  return (
    <ol className="space-y-2">
      {stages.map((s, i) => (
        <StageRow key={i} index={i + 1} stage={s} totalUs={totalUs} analyzed={analyzed} />
      ))}
    </ol>
  );
}

function StageRow({
  index,
  stage,
  totalUs,
  analyzed,
}: {
  index: number;
  stage: ExplainStage;
  totalUs: number;
  analyzed: boolean;
}) {
  const [open, setOpen] = useState(false);
  const attrKeys = Object.keys(stage.attrs ?? {});
  const share = totalUs > 0 ? stage.took_us / totalUs : 0;
  const hasRows = stage.rows_in !== null || stage.rows_out !== null;
  const dropped =
    stage.rows_in !== null && stage.rows_out !== null && stage.rows_out < stage.rows_in
      ? stage.rows_in - stage.rows_out
      : 0;

  return (
    <li className="rounded-lg border border-gray-200 dark:border-edge px-3 py-2">
      <div className="flex flex-wrap items-baseline gap-x-3 gap-y-1">
        <span className="font-mono text-[10px] text-gray-400 dark:text-faint tabular-nums w-5">
          {index}.
        </span>
        <span className="text-sm font-medium text-gray-900 dark:text-ink">{stage.label}</span>
        <span className="font-mono text-[10px] uppercase tracking-wider text-gray-400 dark:text-faint">
          {stage.name}
        </span>
        <span className="flex-1" />
        {hasRows && (
          <span className="font-mono text-xs tabular-nums text-gray-700 dark:text-muted">
            {fmtRows(stage.rows_in)} → {fmtRows(stage.rows_out)}
            {/* Neutral, not amber: narrowing is what most stages are for
                (HNSW picks 32 of millions). Real problems — e.g. a filter
                starving LIMIT — arrive as server notes. */}
            {dropped > 0 && (
              <span className="text-gray-400 dark:text-faint"> (−{dropped.toLocaleString()})</span>
            )}
          </span>
        )}
        {analyzed && (
          <span className="font-mono text-xs tabular-nums text-gray-900 dark:text-ink w-16 text-right">
            {formatUs(stage.took_us)}
          </span>
        )}
      </div>

      {analyzed && (
        <div className="mt-1.5 ml-8 spectrum-track" title={`${(share * 100).toFixed(1)}% of total`}>
          <div
            className="spectrum-fill"
            style={{ width: `${stage.took_us > 0 ? Math.max(1, share * 100) : 0}%` }}
          />
        </div>
      )}

      <p className="mt-1.5 ml-8 text-xs text-gray-600 dark:text-muted">{stage.detail}</p>

      {attrKeys.length > 0 && (
        <div className="ml-8 mt-1">
          <button
            className="text-[11px] text-gray-500 hover:text-gray-900 dark:hover:text-ink underline"
            onClick={() => setOpen((v) => !v)}
          >
            {open ? "hide" : "show"} {attrKeys.length} detail{attrKeys.length === 1 ? "" : "s"}
          </button>
          {open && (
            <dl className="mt-1 grid grid-cols-[max-content_1fr] gap-x-4 gap-y-0.5 text-xs">
              {attrKeys.map((k) => (
                <AttrRow key={k} name={k} value={stage.attrs[k]} />
              ))}
            </dl>
          )}
        </div>
      )}
    </li>
  );
}

function AttrRow({ name, value }: { name: string; value: unknown }) {
  return (
    <>
      <dt className="font-mono text-gray-500 dark:text-faint">{name}</dt>
      <dd className="font-mono text-gray-900 dark:text-ink break-all min-w-0">
        <AttrValue value={value} />
      </dd>
    </>
  );
}

function AttrValue({ value }: { value: unknown }) {
  if (value === null || value === undefined) return <span className="text-gray-400">null</span>;
  if (typeof value === "number") return <span className="tabular-nums">{fmtNum(value)}</span>;
  if (typeof value === "string" || typeof value === "boolean") return <>{String(value)}</>;
  // Short arrays of primitives read better inline than as a tree.
  if (
    Array.isArray(value) &&
    value.length <= 8 &&
    value.every((v) => v === null || ["string", "number", "boolean"].includes(typeof v))
  ) {
    return <>[{value.map((v) => (typeof v === "number" ? fmtNum(v) : String(v))).join(", ")}]</>;
  }
  return <JsonTree value={value} defaultExpanded={false} />;
}

// ---- Why ranked -----------------------------------------------------------

const SCORE_HINT: Record<HitExplain["score_kind"], string> = {
  distance: "cosine distance · lower is closer",
  fused: "fused hybrid score · higher is better",
  bm25: "BM25 weight · higher is better",
};

function HitsTable({ hits }: { hits: HitExplain[] }) {
  const [openTerms, setOpenTerms] = useState<Set<number>>(new Set());
  const kinds = new Set(hits.map((h) => h.score_kind));
  const hasVector = hits.some((h) => h.vector);
  const hasBm25 = hits.some((h) => h.bm25);
  const hasFilters = hits.some((h) => h.filters && h.filters.length > 0);
  const colCount = 3 + (hasVector ? 1 : 0) + (hasBm25 ? 1 : 0) + (hasFilters ? 1 : 0);

  const toggle = (i: number) =>
    setOpenTerms((prev) => {
      const next = new Set(prev);
      if (next.has(i)) next.delete(i);
      else next.add(i);
      return next;
    });

  return (
    <div className="space-y-2">
      <p className="text-xs text-gray-500 dark:text-muted">
        score = {[...kinds].map((k) => SCORE_HINT[k]).join("; ")}
      </p>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead className="bg-gray-100 dark:bg-carbon-950 text-gray-600 dark:text-muted">
            <tr>
              <th className="text-left px-2 py-1 font-medium">#</th>
              <th className="text-left px-2 py-1 font-medium">id</th>
              <th className="text-left px-2 py-1 font-medium">score</th>
              {hasVector && <th className="text-left px-2 py-1 font-medium">vector</th>}
              {hasBm25 && <th className="text-left px-2 py-1 font-medium">BM25 (keywords)</th>}
              {hasFilters && <th className="text-left px-2 py-1 font-medium">filters</th>}
            </tr>
          </thead>
          <tbody>
            {hits.map((h, i) => (
              <HitRows
                key={i}
                hit={h}
                hasVector={hasVector}
                hasBm25={hasBm25}
                hasFilters={hasFilters}
                colCount={colCount}
                termsOpen={openTerms.has(i)}
                onToggleTerms={() => toggle(i)}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function HitRows({
  hit,
  hasVector,
  hasBm25,
  hasFilters,
  colCount,
  termsOpen,
  onToggleTerms,
}: {
  hit: HitExplain;
  hasVector: boolean;
  hasBm25: boolean;
  hasFilters: boolean;
  colCount: number;
  termsOpen: boolean;
  onToggleTerms: () => void;
}) {
  const terms = hit.bm25?.terms ?? [];
  return (
    <>
      <tr className="border-t border-gray-200 dark:border-edge align-top">
        <td className="px-2 py-1.5 text-gray-500 tabular-nums">{hit.rank}</td>
        <td className="px-2 py-1.5 font-mono break-all">{hit.id}</td>
        <td className="px-2 py-1.5 font-mono tabular-nums text-accent">{fmtNum(hit.final_score)}</td>
        {hasVector && (
          <td className="px-2 py-1.5 font-mono tabular-nums whitespace-nowrap">
            {hit.vector ? (
              <StageMath
                parts={[
                  ["dist", hit.vector.distance],
                  ...(hit.score_kind === "fused"
                    ? ([["sim", hit.vector.similarity]] as Array<[string, number]>)
                    : []),
                ]}
                normalized={hit.vector.normalized}
                weight={hit.vector.weight}
                weighted={hit.vector.weighted}
                rank={hit.vector.rank}
              />
            ) : (
              <Missing why="not in the vector candidates" />
            )}
          </td>
        )}
        {hasBm25 && (
          <td className="px-2 py-1.5 font-mono tabular-nums whitespace-nowrap">
            {hit.bm25 ? (
              <div className="space-y-0.5">
                <StageMath
                  parts={[["bm25", hit.bm25.score]]}
                  normalized={hit.bm25.normalized}
                  weight={hit.bm25.weight}
                  weighted={hit.bm25.weighted}
                  rank={hit.bm25.rank}
                />
                {terms.length > 0 && (
                  <button
                    className="ml-2 text-[11px] text-gray-500 hover:text-gray-900 dark:hover:text-ink underline font-sans"
                    onClick={onToggleTerms}
                  >
                    {termsOpen ? "hide" : "show"} {terms.length} term{terms.length === 1 ? "" : "s"}
                  </button>
                )}
              </div>
            ) : (
              <Missing why="no query keyword matched" />
            )}
          </td>
        )}
        {hasFilters && (
          <td className="px-2 py-1.5">
            {hit.filters && hit.filters.length > 0 ? (
              <ul className="space-y-0.5">
                {hit.filters.map((f, j) => (
                  <li key={j} className="font-mono">
                    <span className={f.passed ? "text-ok" : "text-bad"}>{f.passed ? "✓" : "✗"}</span>{" "}
                    {f.predicate}{" "}
                    <span className="text-gray-500 dark:text-faint">
                      (actual: {f.actual === undefined ? "missing" : JSON.stringify(f.actual)})
                    </span>
                  </li>
                ))}
              </ul>
            ) : (
              <span className="text-gray-400 dark:text-faint">—</span>
            )}
          </td>
        )}
      </tr>
      {termsOpen && terms.length > 0 && (
        <tr className="bg-gray-50 dark:bg-carbon-950">
          <td />
          <td colSpan={colCount - 1} className="px-2 py-2">
            <table className="text-xs font-mono">
              <thead className="text-gray-500 dark:text-faint">
                <tr>
                  <th className="text-left pr-4 font-medium">term</th>
                  <th className="text-right pr-4 font-medium" title="occurrences in this doc">tf</th>
                  <th className="text-right pr-4 font-medium" title="live docs containing the term">df</th>
                  <th className="text-right pr-4 font-medium" title="inverse document frequency — rarer terms weigh more">idf</th>
                  <th className="text-right font-medium">contribution</th>
                </tr>
              </thead>
              <tbody>
                {terms.map((t, k) => (
                  <tr key={k}>
                    <td className="pr-4">{t.term}</td>
                    <td className="pr-4 text-right tabular-nums">{t.tf}</td>
                    <td className="pr-4 text-right tabular-nums">{t.df.toLocaleString()}</td>
                    <td className="pr-4 text-right tabular-nums">{fmtNum(t.idf)}</td>
                    <td className="text-right tabular-nums text-accent">{fmtNum(t.contribution)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </td>
        </tr>
      )}
    </>
  );
}

/**
 * `dist 0.1234 → sim 0.8901 → norm 0.950 × 0.70 = 0.665  #3`.
 */
function StageMath({
  parts,
  normalized,
  weight,
  weighted,
  rank,
}: {
  parts: Array<[string, number]>;
  normalized: number | null;
  weight: number | null;
  weighted: number | null;
  rank: number | null;
}) {
  return (
    <span>
      {parts.map(([label, v], i) => (
        <span key={label}>
          {i > 0 && <span className="text-gray-400"> → </span>}
          <span className="text-gray-500 dark:text-faint">{label} </span>
          {fmtNum(v)}
        </span>
      ))}
      {normalized !== null && (
        <>
          <span className="text-gray-400"> → </span>
          <span className="text-gray-500 dark:text-faint">norm </span>
          {fmtNum(normalized)}
        </>
      )}
      {weighted !== null && (
        <>
          {weight !== null && <span className="text-gray-500 dark:text-faint"> × {fmtNum(weight)}</span>}
          <span className="text-gray-400"> = </span>
          <span className="text-accent">{fmtNum(weighted)}</span>
        </>
      )}
      {rank !== null && <span className="text-gray-400 dark:text-faint"> #{rank}</span>}
    </span>
  );
}

function Missing({ why }: { why: string }) {
  return <span className="text-gray-400 dark:text-faint font-sans">— {why}</span>;
}

// ---- Shared bits -----------------------------------------------------------

function NoteBanner({ level, children }: { level: "info" | "warn"; children: ReactNode }) {
  const cls =
    level === "warn"
      ? "border-amber-300/70 bg-amber-50 text-amber-900 dark:border-warn/40 dark:bg-warn/10 dark:text-warn"
      : "border-gray-200 bg-gray-50 text-gray-700 dark:border-edge dark:bg-carbon-950 dark:text-muted";
  return (
    <div className={`rounded-md border px-3 py-2 text-xs flex gap-2 ${cls}`}>
      <span className="font-mono uppercase tracking-wider text-[10px] pt-px">{level}</span>
      <span>{children}</span>
    </div>
  );
}

function CopyBlock({ text, caption, wrap = false }: { text: string; caption: string; wrap?: boolean }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      /* clipboard blocked (insecure context / permissions) — the text is selectable anyway */
    }
  };
  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between gap-3">
        <span className="text-xs text-gray-500 dark:text-muted">{caption}</span>
        <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={copy}>
          {copied ? "copied" : "copy"}
        </button>
      </div>
      <pre
        className={`text-xs font-mono rounded-md border border-gray-200 bg-gray-50 p-3 overflow-x-auto max-h-[28rem] dark:border-edge dark:bg-carbon-950 ${
          wrap ? "whitespace-pre-wrap break-words" : "whitespace-pre"
        }`}
      >
        {text}
      </pre>
    </div>
  );
}

/** µs → human: `840µs`, `3.27ms`, `41.2ms`, `1.20s`. */
export function formatUs(us: number): string {
  if (us < 1000) return `${Math.round(us)}µs`;
  if (us < 10_000) return `${(us / 1000).toFixed(2)}ms`;
  if (us < 1_000_000) return `${(us / 1000).toFixed(1)}ms`;
  return `${(us / 1_000_000).toFixed(2)}s`;
}

function fmtRows(n: number | null): string {
  return n === null ? "·" : n.toLocaleString();
}

/** Scores span 1e-4 (distances) to 1e2 (BM25) — keep ~4 significant digits. */
function fmtNum(n: number): string {
  if (!Number.isFinite(n)) return String(n);
  if (Number.isInteger(n)) return n.toLocaleString();
  const abs = Math.abs(n);
  if (abs >= 100) return n.toFixed(1);
  if (abs >= 1) return n.toFixed(3);
  return n.toFixed(4);
}

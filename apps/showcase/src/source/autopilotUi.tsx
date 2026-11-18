/**
 * Visual pieces shared by the three "autopilot" apps built from the
 * Source Supplies discovery call (Retail POD, Stock, Report Studio):
 * the business-benefit strip, KPI tiles, a sparkline with forecast, and
 * a horizontal bar chart. Inline SVG only, theme-aware via currentColor.
 */
import type { ReactNode } from "react";

export interface BenefitRow {
  label: string;
  before: string;
  after: string;
}

/** "Before Jordan / with Jordan" strip with a headline figure — the business case in one glance. */
export function BenefitStrip({ headline, sub, rows, quote }: { headline: string; sub: string; rows: BenefitRow[]; quote?: string }) {
  return (
    <section className="card !p-0 overflow-hidden" data-tour="benefit">
      <div className="grid gap-0 md:grid-cols-[minmax(0,15rem)_1fr]">
        <div className="bg-ok/10 border-b md:border-b-0 md:border-r border-ok/30 p-4 flex flex-col justify-center">
          <div className="eyebrow text-ok">Business benefit</div>
          <div className="font-display text-2xl font-semibold text-gray-900 dark:text-ink mt-1">{headline}</div>
          <div className="text-xs text-gray-600 dark:text-muted mt-1">{sub}</div>
        </div>
        <div className="p-4">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-[11px] uppercase tracking-wider text-gray-500 dark:text-muted">
                <th className="pb-1.5 pr-3 font-medium"> </th>
                <th className="pb-1.5 pr-3 font-medium">Today (manual)</th>
                <th className="pb-1.5 font-medium text-ok">With Jordan</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.label} className="border-t border-gray-100 dark:border-edge/60">
                  <td className="py-1.5 pr-3 text-gray-600 dark:text-muted">{r.label}</td>
                  <td className="py-1.5 pr-3 text-gray-500 dark:text-faint line-through decoration-bad/50">{r.before}</td>
                  <td className="py-1.5 font-medium text-gray-900 dark:text-ink">{r.after}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {quote && <p className="mt-2 text-xs italic text-gray-500 dark:text-muted">“{quote}” — Dave, discovery call</p>}
        </div>
      </div>
    </section>
  );
}

export function Kpi({ label, value, tone, hint }: { label: string; value: ReactNode; tone?: "ok" | "warn" | "bad"; hint?: string }) {
  const color = tone === "ok" ? "text-ok" : tone === "warn" ? "text-warn" : tone === "bad" ? "text-bad" : "text-gray-900 dark:text-ink";
  return (
    <div className="rounded-lg border border-gray-200 dark:border-edge px-3 py-2 min-w-0">
      <div className="text-[11px] uppercase tracking-wider text-gray-500 dark:text-muted truncate">{label}</div>
      <div className={`font-display text-xl font-semibold tabular-nums ${color}`}>{value}</div>
      {hint && <div className="text-[11px] text-gray-500 dark:text-faint truncate">{hint}</div>}
    </div>
  );
}

/** History (solid) + forecast (dashed, accent) + optional one-off markers. */
export function Sparkline({
  history,
  forecast = [],
  naive,
  marks = [],
  width = 220,
  height = 48,
}: {
  history: number[];
  forecast?: number[];
  naive?: number;
  marks?: number[];
  width?: number;
  height?: number;
}) {
  const all = [...history, ...forecast, ...(naive ? [naive] : [])];
  const max = Math.max(...all, 1) * 1.08;
  const n = history.length + forecast.length;
  const x = (i: number) => (i / Math.max(1, n - 1)) * (width - 4) + 2;
  const y = (v: number) => height - 2 - (v / max) * (height - 4);
  const hPath = history.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  const fStart = history.length - 1;
  const fPath = [history[fStart], ...forecast].map((v, i) => `${i ? "L" : "M"}${x(fStart + i).toFixed(1)},${y(v).toFixed(1)}`).join(" ");
  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} className="block" role="img" aria-label="demand history and forecast">
      {naive !== undefined && (
        <line x1={x(fStart)} x2={x(n - 1)} y1={y(naive)} y2={y(naive)} className="stroke-gray-400 dark:stroke-faint" strokeDasharray="2 3" strokeWidth={1} />
      )}
      <path d={hPath} fill="none" className="stroke-gray-500 dark:stroke-muted" strokeWidth={1.4} />
      {forecast.length > 0 && <path d={fPath} fill="none" className="stroke-accent" strokeWidth={2} strokeDasharray="4 2" />}
      {marks.map((i) => (
        <circle key={i} cx={x(i)} cy={y(history[i])} r={3} className="fill-warn" />
      ))}
    </svg>
  );
}

export function Bars({ rows, format }: { rows: { label: string; value: number; sub?: string }[]; format: (v: number) => string }) {
  const max = Math.max(...rows.map((r) => r.value), 1);
  return (
    <div className="space-y-1.5">
      {rows.map((r) => (
        <div key={r.label} className="grid grid-cols-[minmax(0,11rem)_1fr_auto] items-center gap-2 text-xs">
          <div className="truncate text-gray-700 dark:text-muted" title={r.label}>
            {r.label}
          </div>
          <div className="h-3 rounded bg-gray-100 dark:bg-carbon-800 overflow-hidden">
            <div className="h-full rounded bg-accent/80 animate-rise" style={{ width: `${(r.value / max) * 100}%` }} />
          </div>
          <div className="tabular-nums text-gray-900 dark:text-ink text-right min-w-[5rem]">
            {format(r.value)}
            {r.sub && <span className="ml-1 text-gray-500 dark:text-faint">{r.sub}</span>}
          </div>
        </div>
      ))}
    </div>
  );
}

/** Small status pill. */
export function Pill({ tone, children }: { tone: "ok" | "warn" | "bad" | "idle" | "accent"; children: ReactNode }) {
  const cls = {
    ok: "border-ok/40 text-ok bg-ok/10",
    warn: "border-warn/40 text-warn bg-warn/10",
    bad: "border-bad/40 text-bad bg-bad/10",
    idle: "border-gray-300 dark:border-edge text-gray-500 dark:text-muted",
    accent: "border-accent/40 text-accent bg-accent/10",
  }[tone];
  return <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[11px] font-medium whitespace-nowrap ${cls}`}>{children}</span>;
}

export function download(name: string, text: string, type = "text/csv") {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

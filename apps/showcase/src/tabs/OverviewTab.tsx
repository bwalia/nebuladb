import { useEffect, useRef, useState } from "react";
import { api, type AuditEntry, type BucketStats, type StatsSnapshot } from "../api";
import { Panel, Stat } from "../components";
import { FreshnessPill, useFreshness } from "../freshness";

/**
 * Overview / cluster home. Polls the admin endpoints every 1s and
 * keeps a rolling 60-sample ring for the sparklines — one full minute
 * of history at 1s/tick. All charts are inline SVG so we pay zero
 * chart-library bytes for a demo dashboard.
 *
 * A `FreshnessPill` in the header makes stalled polls visible at a
 * glance: green "Xs ago" on fresh data, amber when more than 3s
 * since the last successful tick.
 */

interface TickSample {
  ts: number;
  stats: StatsSnapshot;
}

const RING_SIZE = 60;

export function OverviewTab({ onNavigate }: { onNavigate: (id: string) => void }) {
  const [stats, setStats] = useState<StatsSnapshot | null>(null);
  const [buckets, setBuckets] = useState<BucketStats[] | null>(null);
  const [audit, setAudit] = useState<AuditEntry[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  // Ring buffer held in a ref so updates don't rerender — we only
  // force a rerender when `stats` changes, which is enough for the
  // chart's latest tick to flow in via `historyRef.current`.
  const historyRef = useRef<TickSample[]>([]);
  const [historyRev, setHistoryRev] = useState(0);
  const { bump, pill } = useFreshness(3000);

  useEffect(() => {
    let cancelled = false;
    // 1s cadence so the sparkline actually feels live when bulk
    // loads are running. The three admin endpoints served from
    // in-memory state are < 1ms each; they're not the bottleneck.
    const tick = async () => {
      try {
        // Fetch everything in parallel; stats is the dominant
        // signal, buckets/audit are secondary.
        const [s, b, a] = await Promise.all([
          api.stats(),
          api.buckets().catch(() => [] as BucketStats[]),
          api.audit(20).catch(() => [] as AuditEntry[]),
        ]);
        if (cancelled) return;
        setStats(s);
        setBuckets(b);
        setAudit(a);
        setErr(null);
        bump();
        const ring = historyRef.current;
        ring.push({ ts: Date.now(), stats: s });
        if (ring.length > RING_SIZE) ring.shift();
        setHistoryRev((r) => r + 1);
      } catch (e) {
        if (!cancelled) setErr((e as Error).message);
      }
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  const deltas = computeDeltas(historyRef.current);
  const _ = historyRev; // keep the rerender tied to ring updates
  void _;

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-3">
        <PageHeader
          title="Cluster overview"
          subtitle="Single-pane summary — refreshes every 1s"
        />
        <FreshnessPill {...pill} />
      </div>

      {err && (
        <div className="rounded-md border border-red-200 bg-red-50 dark:border-red-800 dark:bg-red-950/30 text-red-800 dark:text-red-300 px-3 py-2 text-sm">
          {err}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Documents" value={stats?.total_docs_live ?? "…"} />
        <StatCard label="Buckets" value={buckets?.length ?? "…"} />
        <StatCard label="Requests" value={stats?.requests_total ?? "…"} />
        <StatCard
          label="Errors"
          value={stats?.requests_errors ?? "…"}
          tone={stats && stats.requests_errors > 0 ? "warn" : "ok"}
        />
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <Panel
          title="Request rate"
          subtitle="Per-second, rolling 60s"
          action={<Stat label="now" value={`${fmt(deltas.reqs_per_sec.last)}/s`} />}
        >
          <Sparkline values={deltas.reqs_per_sec.series} stroke="#2563eb" />
        </Panel>
        <Panel
          title="Semantic + vector search rate"
          subtitle="Requests per second"
          action={<Stat label="now" value={`${fmt(deltas.search_per_sec.last)}/s`} />}
        >
          <Sparkline values={deltas.search_per_sec.series} stroke="#10b981" />
        </Panel>
      </div>

      <div className="grid md:grid-cols-3 gap-4">
        <Panel
          title="Cache hit ratio"
          subtitle="In-proc embedding cache · last minute"
        >
          <CacheGauge
            hits={deltas.cache_hits_window}
            misses={deltas.cache_misses_window}
          />
        </Panel>
        <Panel title="Buckets" subtitle={`${buckets?.length ?? 0} active`} >
          {buckets && buckets.length > 0 ? (
            <ul className="space-y-1 text-xs">
              {buckets.slice(0, 6).map((b) => (
                <li key={b.bucket} className="flex items-center justify-between">
                  <span className="font-mono">{b.bucket}</span>
                  <span className="text-gray-500 dark:text-gray-400">
                    {b.docs} docs · {b.parent_docs} parents
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-gray-500 dark:text-gray-400">
              No buckets yet.{" "}
              <button
                className="underline"
                onClick={() => onNavigate("documents")}
              >
                Go to Documents
              </button>{" "}
              or run <code>./scripts/seed.sh</code>.
            </p>
          )}
        </Panel>
        <Panel title="Recent activity" subtitle="Write-path audit trail">
          {audit && audit.length > 0 ? (
            <ul className="space-y-1 text-xs">
              {audit.slice(0, 6).map((a, i) => (
                <li
                  key={i}
                  className="flex items-center justify-between gap-2"
                >
                  <span className="font-mono truncate">{a.method} {a.path}</span>
                  <span
                    className={`font-mono ${
                      a.status >= 500
                        ? "text-red-600 dark:text-red-400"
                        : a.status >= 400
                        ? "text-orange-600 dark:text-orange-400"
                        : "text-green-700 dark:text-green-400"
                    }`}
                  >
                    {a.status}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-gray-500 dark:text-gray-400">
              No writes recorded yet.
            </p>
          )}
        </Panel>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard
          label="Rate limited"
          value={stats?.rate_limited ?? "…"}
          tone={stats && stats.rate_limited > 0 ? "warn" : "ok"}
        />
        <StatCard
          label="Auth failures"
          value={stats?.auth_failures ?? "…"}
          tone={stats && stats.auth_failures > 0 ? "warn" : "ok"}
        />
        <StatCard label="RAG calls" value={stats?.rag_requests ?? "…"} />
        <StatCard
          label="Semantic searches"
          value={stats?.searches_semantic ?? "…"}
        />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers & primitives
// ---------------------------------------------------------------------------

function PageHeader({ title, subtitle }: { title: string; subtitle?: string }) {
  return (
    <div>
      <h2 className="text-xl font-semibold">{title}</h2>
      {subtitle && (
        <p className="text-xs text-gray-500 dark:text-gray-400">{subtitle}</p>
      )}
    </div>
  );
}

function StatCard({
  label,
  value,
  tone = "default",
}: {
  label: string;
  value: number | string;
  tone?: "default" | "ok" | "warn";
}) {
  const toneClass =
    tone === "warn"
      ? "text-orange-600 dark:text-orange-400"
      : tone === "ok"
      ? "text-green-700 dark:text-green-400"
      : "text-gray-900 dark:text-gray-100";
  return (
    <div className="card">
      <div className="text-xs text-gray-500 dark:text-gray-400">{label}</div>
      <div className={`text-2xl font-semibold mt-1 ${toneClass}`}>{value}</div>
    </div>
  );
}

/**
 * Minimal SVG sparkline. Values are rescaled into a 0..1 range across
 * the window so the shape is visible even when absolute values are
 * tiny. A flat line means "no activity" and renders as a horizontal
 * line at y=max (bottom of the chart) so it doesn't look broken.
 */
function Sparkline({ values, stroke }: { values: number[]; stroke: string }) {
  const W = 320;
  const H = 80;
  if (values.length < 2) {
    return (
      <div className="h-20 flex items-center justify-center text-xs text-gray-400">
        collecting samples…
      </div>
    );
  }
  const max = Math.max(...values, 1);
  const min = Math.min(...values, 0);
  const span = max - min || 1;
  const step = W / (values.length - 1);
  const points = values
    .map((v, i) => {
      const x = (i * step).toFixed(1);
      const y = (H - ((v - min) / span) * (H - 8) - 4).toFixed(1);
      return `${x},${y}`;
    })
    .join(" ");
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-20" preserveAspectRatio="none">
      <polyline
        fill="none"
        stroke={stroke}
        strokeWidth="2"
        strokeLinejoin="round"
        strokeLinecap="round"
        points={points}
      />
    </svg>
  );
}

function CacheGauge({ hits, misses }: { hits: number; misses: number }) {
  const total = hits + misses;
  if (total === 0) {
    return (
      <div className="text-xs text-gray-500 dark:text-gray-400">
        No cache traffic in the last minute.
      </div>
    );
  }
  const ratio = hits / total;
  const pct = Math.round(ratio * 100);
  const color =
    ratio >= 0.7 ? "#16a34a" : ratio >= 0.3 ? "#f97316" : "#dc2626";
  return (
    <div>
      <div className="flex items-baseline gap-2">
        <span className="text-3xl font-semibold" style={{ color }}>
          {pct}%
        </span>
        <span className="text-xs text-gray-500 dark:text-gray-400">
          {hits} hits / {total} lookups · last 60s
        </span>
      </div>
      <div className="h-2 rounded bg-gray-200 dark:bg-gray-800 mt-2 overflow-hidden">
        <div
          className="h-full transition-all"
          style={{ width: `${pct}%`, background: color }}
        />
      </div>
    </div>
  );
}

interface Deltas {
  reqs_per_sec: { last: number; series: number[] };
  search_per_sec: { last: number; series: number[] };
  cache_hits_window: number;
  cache_misses_window: number;
}

/**
 * Turn cumulative counters into rate-per-second across adjacent
 * samples. Classic Prometheus-style `rate()` logic done on the
 * client so we don't need an extra derivative endpoint.
 */
function computeDeltas(ring: TickSample[]): Deltas {
  const empty: Deltas = {
    reqs_per_sec: { last: 0, series: [] },
    search_per_sec: { last: 0, series: [] },
    cache_hits_window: 0,
    cache_misses_window: 0,
  };
  if (ring.length < 2) return empty;

  const reqSeries: number[] = [];
  const searchSeries: number[] = [];
  for (let i = 1; i < ring.length; i++) {
    const dtSec = Math.max((ring[i].ts - ring[i - 1].ts) / 1000, 0.001);
    reqSeries.push(
      Math.max(0, (ring[i].stats.requests_total - ring[i - 1].stats.requests_total) / dtSec)
    );
    searchSeries.push(
      Math.max(
        0,
        (ring[i].stats.searches_semantic +
          ring[i].stats.searches_vector -
          ring[i - 1].stats.searches_semantic -
          ring[i - 1].stats.searches_vector) /
          dtSec
      )
    );
  }
  const first = ring[0].stats;
  const last = ring[ring.length - 1].stats;
  return {
    reqs_per_sec: { last: reqSeries[reqSeries.length - 1] ?? 0, series: reqSeries },
    search_per_sec: { last: searchSeries[searchSeries.length - 1] ?? 0, series: searchSeries },
    cache_hits_window: Math.max(0, last.embed_cache_hits - first.embed_cache_hits),
    cache_misses_window: Math.max(0, last.embed_cache_misses - first.embed_cache_misses),
  };
}

function fmt(n: number): string {
  if (n >= 100) return n.toFixed(0);
  if (n >= 10) return n.toFixed(1);
  return n.toFixed(2);
}

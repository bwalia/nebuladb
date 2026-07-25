import { useEffect, useRef, useState } from "react";
import { api, type AuditEntry, type BucketStats, type StatsSnapshot } from "../api";
import { Panel, Stat } from "../components";
import { FreshnessPill, useFreshness } from "../freshness";

/**
 * Overview / cluster home — styled after the Ring Promoter control
 * plane: KPI tiles, live rate charts, and status-dot service rows +
 * an activity feed. Polls the admin endpoints every 1s and keeps a
 * rolling 60-sample ring for the sparklines. Charts are inline SVG so
 * we pay zero chart-library bytes.
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
  const historyRef = useRef<TickSample[]>([]);
  const [historyRev, setHistoryRev] = useState(0);
  const { bump, pill } = useFreshness(3000);

  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      try {
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
  const _ = historyRev;
  void _;

  const bucketCount = buckets?.length ?? 0;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-end">
        <FreshnessPill {...pill} />
      </div>

      {err && (
        <div className="rounded-xl border border-red-300/60 bg-red-50 dark:border-bad/30 dark:bg-bad/10 text-bad px-4 py-2.5 text-sm font-mono">
          {err}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <StatCard label="Documents" value={stats?.total_docs_live ?? "…"} />
        <StatCard label="Buckets" value={bucketCount || "…"} />
        <StatCard label="Requests" value={stats?.requests_total ?? "…"} />
        <StatCard
          label="Errors"
          value={stats?.requests_errors ?? "…"}
          tone={stats && stats.requests_errors > 0 ? "bad" : "ok"}
        />
      </div>

      <div className="grid md:grid-cols-2 gap-4">
        <Panel
          title="Request rate"
          subtitle="Per-second, rolling 60s"
          action={<Stat label="now" value={`${fmt(deltas.reqs_per_sec.last)}/s`} />}
        >
          <Sparkline values={deltas.reqs_per_sec.series} stroke="#4C8DFF" />
        </Panel>
        <Panel
          title="Search rate"
          subtitle="Semantic + vector, per second"
          action={<Stat label="now" value={`${fmt(deltas.search_per_sec.last)}/s`} />}
        >
          <Sparkline values={deltas.search_per_sec.series} stroke="#3FB950" />
        </Panel>
      </div>

      <div className="grid md:grid-cols-3 gap-4">
        <Panel title="Cache hit ratio" subtitle="Embedding cache · last minute">
          <CacheGauge hits={deltas.cache_hits_window} misses={deltas.cache_misses_window} />
        </Panel>

        {/* Buckets as a Ring-Promoter "rings" list: a health dot + row. */}
        <Panel
          title="Buckets"
          subtitle={bucketCount > 0 ? `${bucketCount} healthy` : "none deployed"}
          action={
            <span className="metachip">
              <span className={`dot ${bucketCount > 0 ? "dot-ok" : "dot-idle"}`} />
              {bucketCount}/{bucketCount} rings
            </span>
          }
        >
          {buckets && buckets.length > 0 ? (
            <div className="-mx-1">
              {buckets.slice(0, 6).map((b) => (
                <button
                  key={b.bucket}
                  onClick={() => onNavigate("search")}
                  className="listrow w-full rounded-lg text-left"
                >
                  <span className="dot dot-ok" />
                  <span className="font-medium text-black dark:text-ink truncate flex-1">
                    {b.bucket}
                  </span>
                  <span className="font-mono text-xs text-gray-500 dark:text-muted">
                    {b.docs} docs
                  </span>
                  <span className="text-gray-400 dark:text-faint">›</span>
                </button>
              ))}
            </div>
          ) : (
            <p className="text-sm text-gray-500 dark:text-muted">
              No buckets yet.{" "}
              <button className="text-black dark:text-ink underline" onClick={() => onNavigate("documents")}>
                Ingest documents
              </button>{" "}
              to deploy your first ring.
            </p>
          )}
        </Panel>

        {/* Recent activity — the write-path audit trail as a feed. */}
        <Panel title="Recent activity" subtitle="Write-path audit trail">
          {audit && audit.length > 0 ? (
            <div className="-mx-1">
              {audit.slice(0, 6).map((a, i) => {
                const bad = a.status >= 500;
                const warn = a.status >= 400 && a.status < 500;
                return (
                  <div key={i} className="listrow rounded-lg">
                    <span className={`dot ${bad ? "dot-bad" : warn ? "dot-warn" : "dot-ok"}`} />
                    <span className="font-mono text-xs text-black dark:text-ink truncate flex-1">
                      <span className="text-gray-500 dark:text-muted">{a.method}</span> {a.path}
                    </span>
                    <span
                      className={`font-mono text-xs ${
                        bad ? "text-bad" : warn ? "text-warn" : "text-ok"
                      }`}
                    >
                      {a.status}
                    </span>
                  </div>
                );
              })}
            </div>
          ) : (
            <p className="text-sm text-gray-500 dark:text-muted">No writes recorded yet.</p>
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
        <StatCard label="Semantic searches" value={stats?.searches_semantic ?? "…"} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Helpers & primitives
// ---------------------------------------------------------------------------

function StatCard({
  label,
  value,
  tone = "default",
}: {
  label: string;
  value: number | string;
  tone?: "default" | "ok" | "warn" | "bad";
}) {
  const toneClass =
    tone === "bad"
      ? "text-bad"
      : tone === "warn"
      ? "text-warn"
      : tone === "ok"
      ? "text-ok"
      : "text-black dark:text-ink";
  return (
    <div className="instrument !p-4">
      <div className="eyebrow">{label}</div>
      <div className={`text-2xl font-semibold mt-1.5 tabular-nums ${toneClass}`}>{value}</div>
    </div>
  );
}

/**
 * Minimal SVG sparkline. Values are rescaled into a 0..1 range across
 * the window so the shape is visible even when absolute values are tiny.
 */
function Sparkline({ values, stroke }: { values: number[]; stroke: string }) {
  const W = 320;
  const H = 80;
  if (values.length < 2) {
    return (
      <div className="h-20 flex items-center justify-center text-xs text-gray-400 dark:text-faint">
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
  const areaPoints = `0,${H} ${points} ${W},${H}`;
  const gid = `spark-${stroke.replace("#", "")}`;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-20" preserveAspectRatio="none">
      <defs>
        <linearGradient id={gid} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={stroke} stopOpacity="0.22" />
          <stop offset="100%" stopColor={stroke} stopOpacity="0" />
        </linearGradient>
      </defs>
      <polygon fill={`url(#${gid})`} points={areaPoints} />
      <polyline
        fill="none"
        stroke={stroke}
        strokeWidth="1.75"
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
      <div className="text-sm text-gray-500 dark:text-muted">
        No cache traffic in the last minute.
      </div>
    );
  }
  const ratio = hits / total;
  const pct = Math.round(ratio * 100);
  const color = ratio >= 0.7 ? "#3FB950" : ratio >= 0.3 ? "#D29922" : "#F85149";
  return (
    <div>
      <div className="flex items-baseline gap-2">
        <span className="text-3xl font-semibold tabular-nums" style={{ color }}>
          {pct}%
        </span>
        <span className="text-xs text-gray-500 dark:text-muted">
          {hits} hits / {total} lookups · 60s
        </span>
      </div>
      <div className="h-1.5 rounded-full bg-gray-200 dark:bg-carbon-800 mt-2.5 overflow-hidden">
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, background: color }} />
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
 * samples — client-side Prometheus-style `rate()`.
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

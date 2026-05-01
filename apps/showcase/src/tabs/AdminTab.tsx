import { useCallback, useEffect, useState } from "react";
import {
  api,
  ApiError,
  type AuditEntry,
  type BucketStats,
  type DurabilityInfo,
  type QueryPlan,
  type SlowQueryEntry,
} from "../api";
import { ErrorBanner, JsonView, Panel, Spinner, Stat } from "../components";
import { clearHistory, loadHistory, type HistoryEntry } from "../utils";
import { FreshnessPill, useFreshness } from "../freshness";

/**
 * Admin / observability tab. Four sub-panels:
 *
 * - Buckets — live per-bucket stats from `/api/v1/admin/buckets`.
 * - Query history — replay + EXPLAIN a previous SQL statement.
 * - EXPLAIN — paste arbitrary SQL, see the typed plan tree.
 * - Audit log — most-recent-first view of mutating requests.
 *
 * The whole tab polls in the background (10 s interval) so leaving
 * it open while you run tests elsewhere shows live activity.
 */
export function AdminTab() {
  return (
    <div className="grid gap-4">
      <DurabilityPanel />
      <BucketsPanel />
      <ExplainPanel />
      <HistoryPanel />
      <SlowQueriesPanel />
      <AuditPanel />
    </div>
  );
}

// ---------- Buckets ----------

function BucketsPanel() {
  const [data, setData] = useState<BucketStats[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const { bump, pill } = useFreshness(6000);
  const refresh = useCallback(async () => {
    setBusy(true);
    setErr(null);
    try {
      setData(await api.buckets());
      bump();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [bump]);

  useEffect(() => {
    void refresh();
    // 3s poll — fast enough to feel alive under bulk load, slow
    // enough that a bucket-scan every tick doesn't matter.
    const id = setInterval(refresh, 3_000);
    return () => clearInterval(id);
  }, [refresh]);

  return (
    <Panel
      title="Buckets"
      subtitle="Live doc counts and metadata-key frequency — refreshes every 3s"
      action={
        <div className="flex items-center gap-2">
          <FreshnessPill {...pill} />
          <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={refresh} disabled={busy}>
            {busy ? "…" : "Refresh"}
          </button>
        </div>
      }
    >
      <ErrorBanner err={err} />
      {data === null && !err && <Spinner />}
      {data && data.length === 0 && (
        <p className="text-sm text-gray-500 dark:text-gray-400">
          No documents indexed yet. Try the Documents tab or run <code>./scripts/seed.sh</code>.
        </p>
      )}
      {data && data.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-gray-100 dark:bg-gray-950 text-gray-600 dark:text-gray-400">
              <tr>
                <th className="text-left px-2 py-1 font-medium">bucket</th>
                <th className="text-left px-2 py-1 font-medium">docs</th>
                <th className="text-left px-2 py-1 font-medium">parent docs</th>
                <th className="text-left px-2 py-1 font-medium">top metadata keys</th>
                <th className="text-right px-2 py-1 font-medium">actions</th>
              </tr>
            </thead>
            <tbody>
              {data.map((b) => (
                <tr key={b.bucket} className="border-t border-gray-200 dark:border-gray-800">
                  <td className="px-2 py-1 font-mono">{b.bucket}</td>
                  <td className="px-2 py-1 font-mono">{b.docs}</td>
                  <td className="px-2 py-1 font-mono">{b.parent_docs}</td>
                  <td className="px-2 py-1">
                    <div className="flex flex-wrap gap-1">
                      {b.metadata_keys.length === 0 && (
                        <span className="text-gray-400">—</span>
                      )}
                      {b.metadata_keys.map(([k, n]) => (
                        <span
                          key={k}
                          className="rounded bg-brand-50 text-brand-700 dark:bg-brand-600/20 dark:text-brand-500 px-1.5 py-0.5"
                        >
                          {k} · {n}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="px-2 py-1 text-right">
                    <button
                      className="text-xs text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 underline"
                      onClick={async () => {
                        // Confirmation: empties are idempotent but
                        // destructive. A single-click wipe would be
                        // an operator footgun.
                        if (
                          !confirm(
                            `Empty bucket "${b.bucket}"? This tombstones ${b.docs} document(s).`
                          )
                        ) {
                          return;
                        }
                        try {
                          await api.emptyBucket(b.bucket);
                          await refresh();
                        } catch (e) {
                          alert(`Failed: ${(e as Error).message}`);
                        }
                      }}
                    >
                      Empty
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

// ---------- Explain ----------

function ExplainPanel() {
  const [sql, setSql] = useState(
    "SELECT region, COUNT(*) AS n FROM docs WHERE semantic_match(content, 'zero trust') GROUP BY region"
  );
  const [plan, setPlan] = useState<QueryPlan | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const explain = async () => {
    setErr(null);
    setBusy(true);
    try {
      setPlan(await api.explain(sql));
    } catch (e) {
      setPlan(null);
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Panel
      title="EXPLAIN"
      subtitle="Parse + plan without running the query. See which semantic clause is picked, how WHERE splits, and what top_k the retrieval will ask for."
    >
      <textarea
        className="input font-mono text-xs min-h-[6rem] resize-y"
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        onKeyDown={(e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === "Enter") void explain();
        }}
        spellCheck={false}
      />
      <button className="btn" onClick={explain} disabled={busy}>
        {busy ? <Spinner label="planning…" /> : "Explain"}
      </button>
      <ErrorBanner err={err} />
      {plan !== null && <JsonView value={plan} />}
    </Panel>
  );
}

// ---------- History ----------

function HistoryPanel() {
  const [history, setHistory] = useState<HistoryEntry[]>(() => loadHistory());

  const reload = () => setHistory(loadHistory());
  const wipe = () => {
    clearHistory();
    setHistory([]);
  };

  return (
    <Panel
      title="Query history"
      subtitle={`${history.length} entries, stored locally in your browser`}
      action={
        <div className="flex gap-2">
          <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={reload}>
            Reload
          </button>
          <button
            className="btn-secondary !py-1 !px-2 !text-xs"
            onClick={wipe}
            disabled={history.length === 0}
          >
            Clear
          </button>
        </div>
      }
    >
      {history.length === 0 && (
        <p className="text-sm text-gray-500 dark:text-gray-400">
          Run a query in the SQL tab — it lands here with timing and row counts.
        </p>
      )}
      {history.length > 0 && (
        <ul className="space-y-2">
          {history.map((h, i) => (
            <li
              key={i}
              className="border border-gray-200 dark:border-gray-800 rounded-md p-2 text-xs"
            >
              <div className="flex items-center justify-between gap-2">
                <span
                  className={`font-mono px-1.5 py-0.5 rounded ${
                    h.ok
                      ? "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300"
                      : "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300"
                  }`}
                >
                  {h.ok ? "ok" : "err"}
                </span>
                <span className="flex items-center gap-3 text-gray-500 dark:text-gray-400">
                  {h.ok && typeof h.took_ms === "number" && (
                    <Stat label="took" value={`${h.took_ms}ms`} />
                  )}
                  {h.ok && typeof h.rows === "number" && <Stat label="rows" value={h.rows} />}
                  <span>{new Date(h.ts).toLocaleTimeString()}</span>
                </span>
              </div>
              <pre className="mt-1 whitespace-pre-wrap break-words font-mono">{h.sql}</pre>
              {!h.ok && h.error && (
                <div className="text-red-700 dark:text-red-300 mt-1">{h.error}</div>
              )}
            </li>
          ))}
        </ul>
      )}
    </Panel>
  );
}

// ---------- Audit ----------

function AuditPanel() {
  const [entries, setEntries] = useState<AuditEntry[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const { bump, pill } = useFreshness(4000);

  const refresh = useCallback(async () => {
    setBusy(true);
    setErr(null);
    try {
      setEntries(await api.audit(100));
      bump();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [bump]);

  useEffect(() => {
    void refresh();
    // 2s poll — under bulk load the user wants to see writes land
    // as they happen; 5s felt sluggish.
    const id = setInterval(refresh, 2_000);
    return () => clearInterval(id);
  }, [refresh]);

  return (
    <Panel
      title="Audit log"
      subtitle="Mutating requests only. Newest first, capped at 1024 entries in memory."
      action={
        <div className="flex items-center gap-2">
          <FreshnessPill {...pill} />
          <button className="btn-secondary !py-1 !px-2 !text-xs" onClick={refresh} disabled={busy}>
            {busy ? "…" : "Refresh"}
          </button>
        </div>
      }
    >
      <ErrorBanner err={err} />
      {entries === null && !err && <Spinner />}
      {entries && entries.length === 0 && (
        <p className="text-sm text-gray-500 dark:text-gray-400">
          No writes recorded yet.
        </p>
      )}
      {entries && entries.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-gray-100 dark:bg-gray-950 text-gray-600 dark:text-gray-400">
              <tr>
                <th className="text-left px-2 py-1 font-medium">time</th>
                <th className="text-left px-2 py-1 font-medium">status</th>
                <th className="text-left px-2 py-1 font-medium">method</th>
                <th className="text-left px-2 py-1 font-medium">path</th>
                <th className="text-left px-2 py-1 font-medium">principal</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e, i) => (
                <tr key={i} className="border-t border-gray-200 dark:border-gray-800">
                  <td className="px-2 py-1 font-mono text-gray-500">
                    {new Date(e.ts_ms).toLocaleTimeString()}
                  </td>
                  <td className="px-2 py-1 font-mono">
                    <span
                      className={
                        e.status >= 500
                          ? "text-red-600 dark:text-red-400"
                          : e.status >= 400
                          ? "text-orange-600 dark:text-orange-400"
                          : "text-green-700 dark:text-green-400"
                      }
                    >
                      {e.status}
                    </span>
                  </td>
                  <td className="px-2 py-1 font-mono">{e.method}</td>
                  <td className="px-2 py-1 font-mono">{e.path}</td>
                  <td className="px-2 py-1 font-mono text-gray-500 dark:text-gray-400">
                    {e.principal}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

// ---------- Slow queries ----------

/**
 * Slowest SQL queries seen since boot. Not a rolling window — the
 * server keeps a fixed-capacity priority queue of worst offenders,
 * so operators can answer "what's the dumbest query we've been
 * running?" without scrolling audit logs.
 *
 * Only queries crossing the server's `min_threshold_ms` (default
 * 10ms) are recorded; MockEmbedder noise stays out.
 */
function SlowQueriesPanel() {
  const [entries, setEntries] = useState<SlowQueryEntry[] | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const { bump, pill } = useFreshness(6000);

  const refresh = useCallback(async () => {
    setBusy(true);
    setErr(null);
    try {
      setEntries(await api.slow());
      bump();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [bump]);

  useEffect(() => {
    void refresh();
    // 3s poll — matches the Buckets panel cadence. Slow-log
    // changes are rare (only when a query crosses the threshold)
    // but the reader wants to see a freshly-slow query surface
    // quickly during testing.
    const id = setInterval(refresh, 3_000);
    return () => clearInterval(id);
  }, [refresh]);

  return (
    <Panel
      title="Slow queries"
      subtitle="Top slowest SQL queries since boot (>10ms). Sorted worst-first."
      action={
        <div className="flex items-center gap-2">
          <FreshnessPill {...pill} />
          <button
            className="btn-secondary !py-1 !px-2 !text-xs"
            onClick={refresh}
            disabled={busy}
          >
            {busy ? "…" : "Refresh"}
          </button>
        </div>
      }
    >
      <ErrorBanner err={err} />
      {entries === null && !err && <Spinner />}
      {entries && entries.length === 0 && (
        <p className="text-sm text-gray-500 dark:text-gray-400">
          No queries have crossed the 10ms threshold yet. Try running a SQL
          query in the SQL tab — it'll show up here if it's slow enough.
        </p>
      )}
      {entries && entries.length > 0 && (
        <div className="overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead className="bg-gray-100 dark:bg-gray-950 text-gray-600 dark:text-gray-400">
              <tr>
                <th className="text-right px-2 py-1 font-medium">took</th>
                <th className="text-right px-2 py-1 font-medium">rows</th>
                <th className="text-left px-2 py-1 font-medium">ok</th>
                <th className="text-left px-2 py-1 font-medium">when</th>
                <th className="text-left px-2 py-1 font-medium">sql</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((e, i) => (
                <tr
                  key={i}
                  className="border-t border-gray-200 dark:border-gray-800 align-top"
                >
                  <td className="px-2 py-1 font-mono text-right">
                    <span
                      className={
                        e.took_ms >= 500
                          ? "text-red-600 dark:text-red-400"
                          : e.took_ms >= 100
                          ? "text-orange-600 dark:text-orange-400"
                          : "text-gray-900 dark:text-gray-100"
                      }
                    >
                      {e.took_ms}ms
                    </span>
                  </td>
                  <td className="px-2 py-1 font-mono text-right">{e.rows}</td>
                  <td className="px-2 py-1 font-mono">
                    <span
                      className={
                        e.ok
                          ? "text-green-700 dark:text-green-400"
                          : "text-red-600 dark:text-red-400"
                      }
                    >
                      {e.ok ? "ok" : "err"}
                    </span>
                  </td>
                  <td className="px-2 py-1 font-mono text-gray-500">
                    {new Date(e.ts_ms).toLocaleTimeString()}
                  </td>
                  <td className="px-2 py-1 font-mono whitespace-pre-wrap break-words">
                    {e.sql}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Panel>
  );
}

// ---------- Durability ----------

/**
 * Durability panel: shows WAL/snapshot state and offers the two
 * safe operator actions — take a snapshot (always safe; blocks
 * writers briefly while we copy state) and compact the WAL
 * (always safe; never removes the current segment).
 *
 * When the server is running in-memory (no NEBULA_DATA_DIR), the
 * panel explains that clearly instead of showing a dead dashboard.
 */
function DurabilityPanel() {
  const [info, setInfo] = useState<DurabilityInfo | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [lastAction, setLastAction] = useState<string | null>(null);
  const { bump, pill } = useFreshness(6000);

  const refresh = useCallback(async () => {
    setBusy(true);
    setErr(null);
    try {
      setInfo(await api.durability());
      bump();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }, [bump]);

  useEffect(() => {
    void refresh();
    const id = setInterval(refresh, 3_000);
    return () => clearInterval(id);
  }, [refresh]);

  const takeSnapshot = async () => {
    setLastAction(null);
    setErr(null);
    try {
      const r = await api.takeSnapshot();
      setLastAction(
        `Snapshot at WAL seq ${r.wal_seq_captured} → ${shortPath(r.path)}`
      );
      await refresh();
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    }
  };

  const compactWal = async () => {
    setLastAction(null);
    setErr(null);
    try {
      const r = await api.compactWal();
      setLastAction(`Removed ${r.removed_segments} old WAL segment(s)`);
      await refresh();
    } catch (e) {
      if (e instanceof ApiError) setErr(`${e.code}: ${e.body}`);
      else setErr((e as Error).message);
    }
  };

  return (
    <Panel
      title="Durability"
      subtitle="WAL + snapshot state. Green = every write is durable before it returns."
      action={
        <div className="flex items-center gap-2">
          <FreshnessPill {...pill} />
          <button
            className="btn-secondary !py-1 !px-2 !text-xs"
            onClick={refresh}
            disabled={busy}
          >
            {busy ? "…" : "Refresh"}
          </button>
        </div>
      }
    >
      <ErrorBanner err={err} />
      {info === null && !err && <Spinner />}

      {info && !info.persistent && (
        <div className="text-sm text-gray-600 dark:text-gray-400">
          <p className="mb-2">
            <span className="font-mono px-2 py-0.5 rounded bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300">
              in-memory
            </span>{" "}
            This server was booted without{" "}
            <code>NEBULA_DATA_DIR</code>, so mutations are not persisted. A
            restart will lose every write.
          </p>
          <p className="text-xs">
            To turn durability on, set <code>NEBULA_DATA_DIR=/path/to/data</code>{" "}
            on the server and restart. Then this panel will show WAL + snapshot
            state and offer the action buttons.
          </p>
        </div>
      )}

      {info && info.persistent && (
        <div className="space-y-3">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <DurabilityStat label="Mode" value="persistent" tone="ok" />
            <DurabilityStat
              label="WAL segments"
              value={info.wal?.segment_count ?? "—"}
            />
            <DurabilityStat
              label="WAL size"
              value={formatBytes(info.wal?.total_bytes ?? 0)}
            />
            <DurabilityStat
              label="Data dir"
              value={info.data_dir ? shortPath(info.data_dir) : "—"}
              mono
            />
          </div>
          <div className="flex flex-wrap gap-2">
            <button
              className="btn !py-1 !px-3 !text-xs"
              onClick={takeSnapshot}
              title="Capture a consistent snapshot of the in-memory state to disk"
            >
              Take snapshot
            </button>
            <button
              className="btn-secondary !py-1 !px-3 !text-xs"
              onClick={compactWal}
              title="Drop WAL segments that are fully superseded by the latest snapshot"
            >
              Compact WAL
            </button>
          </div>
          {lastAction && (
            <div className="text-xs text-green-700 dark:text-green-400">
              {lastAction}
            </div>
          )}
        </div>
      )}
    </Panel>
  );
}

function DurabilityStat({
  label,
  value,
  tone = "default",
  mono = false,
}: {
  label: string;
  value: string | number;
  tone?: "default" | "ok";
  mono?: boolean;
}) {
  const toneClass =
    tone === "ok"
      ? "text-green-700 dark:text-green-400"
      : "text-gray-900 dark:text-gray-100";
  return (
    <div>
      <div className="text-xs text-gray-500 dark:text-gray-400">{label}</div>
      <div
        className={`${toneClass} ${
          mono ? "font-mono text-xs" : "text-lg font-semibold"
        } truncate`}
        title={String(value)}
      >
        {value}
      </div>
    </div>
  );
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function shortPath(p: string): string {
  // Keep the last 2 path components — avoids scrolling a long
  // absolute path but still shows the bucket-ish name.
  const parts = p.split("/").filter(Boolean);
  return parts.length > 2 ? ".../" + parts.slice(-2).join("/") : p;
}

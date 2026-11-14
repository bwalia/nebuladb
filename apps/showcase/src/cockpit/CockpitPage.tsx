/**
 * NebulaDB Mission Control Cockpit — NASA-style ops deck modelled on
 * NebulaCB Mission Control (nebulacb.fictionally.org).
 *
 * Live tiles only: /healthz, /admin/stats, /admin/durability,
 * /admin/cluster/nodes, /admin/replication, /admin/audit, peer healthz,
 * /admin/embed/migrate, /admin/reliability. Controls hit real admin
 * endpoints (snapshot, WAL compact, promote). Served at /cockpit under
 * the same nginx OAuth / basic-auth gate as the rest of the showcase.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  api,
  ApiError,
  type AuditEntry,
  type ClusterTopology,
  type DurabilityInfo,
  type Health,
  type ReplicationInfo,
  type StatsSnapshot,
} from "../api";
import "./cockpit.css";

const POLL_MS = 4000;

type Sev = "info" | "warn" | "error";
type MissionHealth = "green" | "amber" | "red";

interface CockpitAlert {
  id: string;
  sev: Sev;
  title: string;
  detail: string;
  at: number;
}

interface EmbedMigrateStatus {
  state: string;
  source_model?: string;
  source_dim?: number;
  target_model?: string;
  target_dim?: number;
  dumped?: number;
  total?: number;
  drained?: number;
  error?: string | null;
}

interface ReliabilitySnap {
  mode?: string;
  memory_used_bytes?: number;
  memory_limit_bytes?: number;
  [k: string]: unknown;
}

interface PeerProbe {
  id: "follower" | "region-b";
  label: string;
  health: Health | null;
  err: string | null;
}

interface LogLine {
  id: string;
  ts: number;
  src: string;
  sev: Sev;
  msg: string;
}

const PHASES = [
  { id: "boot", label: "Boot" },
  { id: "heal", label: "Health" },
  { id: "repl", label: "Replicate" },
  { id: "integrity", label: "Integrity" },
  { id: "embed", label: "Embed" },
  { id: "nominal", label: "Nominal" },
] as const;

function fmt(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 10_000) return `${(n / 1_000).toFixed(1)}k`;
  return n.toLocaleString();
}

function fmtBytes(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—";
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MiB`;
  return `${(n / 1024 ** 3).toFixed(2)} GiB`;
}

function clock(ts: number): string {
  return new Date(ts).toLocaleTimeString([], { hour12: false });
}

function sparkFrom(history: number[], slots = 24): number[] {
  if (history.length === 0) return Array.from({ length: slots }, () => 0.08);
  const slice = history.slice(-slots);
  const max = Math.max(...slice, 1);
  return Array.from({ length: slots }, (_, i) => {
    const v = slice[i - (slots - slice.length)] ?? slice[0] ?? 0;
    return Math.max(0.08, v / max);
  });
}

export function CockpitPage({
  onLeave,
}: {
  /** Navigate back into the normal showcase shell. */
  onLeave?: () => void;
}) {
  const [health, setHealth] = useState<Health | null>(null);
  const [stats, setStats] = useState<StatsSnapshot | null>(null);
  const [dur, setDur] = useState<DurabilityInfo | null>(null);
  const [topo, setTopo] = useState<ClusterTopology | null>(null);
  const [repl, setRepl] = useState<ReplicationInfo | null>(null);
  const [audit, setAudit] = useState<AuditEntry[]>([]);
  const [migrate, setMigrate] = useState<EmbedMigrateStatus | null>(null);
  const [reliability, setReliability] = useState<ReliabilitySnap | null>(null);
  const [follower, setFollower] = useState<PeerProbe>({
    id: "follower",
    label: "Follower",
    health: null,
    err: null,
  });
  const [regionB, setRegionB] = useState<PeerProbe>({
    id: "region-b",
    label: "Region-B",
    health: null,
    err: null,
  });
  const [err, setErr] = useState<string | null>(null);
  const [cmdMsg, setCmdMsg] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [logs, setLogs] = useState<LogLine[]>([]);
  const [reqHistory, setReqHistory] = useState<number[]>([]);
  const lastStats = useRef<StatsSnapshot | null>(null);
  const logId = useRef(0);

  const pushLog = useCallback((src: string, sev: Sev, msg: string) => {
    const line: LogLine = {
      id: `l${++logId.current}`,
      ts: Date.now(),
      src,
      sev,
      msg,
    };
    setLogs((prev) => [...prev.slice(-199), line]);
  }, []);

  const refresh = useCallback(async () => {
    if (paused) return;
    const failed: string[] = [];
    const one = <T,>(name: string, p: Promise<T>, set: (v: T) => void) =>
      p.then(
        (v) => set(v),
        () => {
          failed.push(name);
        }
      );

    await Promise.all([
      one("health", api.health(), setHealth),
      one("stats", api.stats(), (s) => {
        setStats(s);
        const prev = lastStats.current;
        lastStats.current = s;
        const delta =
          prev != null ? Math.max(0, s.requests_total - prev.requests_total) : 0;
        setReqHistory((h) => [...h.slice(-47), delta]);
      }),
      one("durability", api.durability(), setDur),
      one("cluster", api.clusterNodes(), setTopo),
      one("replication", api.replication(), setRepl),
      one("audit", api.audit(40), setAudit),
      one(
        "migrate",
        api.embedMigrate(),
        setMigrate as (v: EmbedMigrateStatus) => void
      ),
      one(
        "reliability",
        api.reliability(),
        setReliability as (v: ReliabilitySnap) => void
      ),
      api
        .peerHealth("follower")
        .then((h) => setFollower((p) => ({ ...p, health: h, err: null })))
        .catch((e) =>
          setFollower((p) => ({
            ...p,
            health: null,
            err: (e as Error).message,
          }))
        ),
      api
        .peerHealth("region-b")
        .then((h) => setRegionB((p) => ({ ...p, health: h, err: null })))
        .catch((e) =>
          setRegionB((p) => ({
            ...p,
            health: null,
            err: (e as Error).message,
          }))
        ),
    ]);
    setErr(failed.length ? `Partial refresh: ${failed.join(", ")}` : null);
  }, [paused]);

  useEffect(() => {
    void refresh();
    const id = setInterval(() => void refresh(), POLL_MS);
    return () => clearInterval(id);
  }, [refresh]);

  // Derive event / log stream from live signals.
  useEffect(() => {
    if (!health) return;
    pushLog(
      "leader",
      "info",
      `docs=${health.docs} model=${health.model} dim=${health.dim} v=${health.version ?? "?"}`
    );
  }, [health?.docs, health?.model, health?.dim, health?.version, pushLog]);

  useEffect(() => {
    if (!repl) return;
    const lag = repl.lag_bytes;
    if (lag != null && lag > 0) {
      pushLog("xdcr", "warn", `follower lag ${fmtBytes(lag)}`);
    }
    for (const r of repl.remotes ?? []) {
      if (!r.healthy || r.consecutive_errors > 0) {
        pushLog(
          "xdcr",
          "error",
          `${r.region}: ${r.last_error || "unhealthy"} (errors=${r.consecutive_errors})`
        );
      } else {
        pushLog(
          "xdcr",
          "info",
          `${r.region} applied=${r.applied_records} ok`
        );
      }
    }
  }, [
    repl?.lag_bytes,
    repl?.remotes?.map((r) => `${r.region}:${r.healthy}:${r.consecutive_errors}:${r.applied_records}`).join("|"),
    pushLog,
  ]);

  useEffect(() => {
    if (!migrate) return;
    if (migrate.state && migrate.state !== "idle") {
      pushLog(
        "embed",
        migrate.error ? "error" : migrate.state === "draining" ? "warn" : "info",
        `migrate ${migrate.state} drained=${migrate.drained ?? 0}/${migrate.total ?? 0}${
          migrate.error ? ` err=${migrate.error}` : ""
        }`
      );
    }
  }, [migrate?.state, migrate?.drained, migrate?.total, migrate?.error, pushLog]);

  const alerts = useMemo(() => {
    const out: CockpitAlert[] = [];
    const now = Date.now();
    if (err) {
      out.push({ id: "poll", sev: "warn", title: "Poll degraded", detail: err, at: now });
    }
    if (follower.err) {
      out.push({
        id: "follower",
        sev: "error",
        title: "Follower unreachable",
        detail: follower.err,
        at: now,
      });
    }
    if (regionB.err) {
      out.push({
        id: "region-b",
        sev: "warn",
        title: "Region-B unreachable",
        detail: regionB.err,
        at: now,
      });
    }
    if (repl?.behind) {
      out.push({
        id: "lag",
        sev: "warn",
        title: "Replication behind",
        detail: `lag ${fmtBytes(repl.lag_bytes)}`,
        at: now,
      });
    }
    for (const r of repl?.remotes ?? []) {
      if (!r.healthy) {
        out.push({
          id: `remote-${r.region}`,
          sev: "error",
          title: `XDCR ${r.region}`,
          detail: r.last_error || "unhealthy",
          at: now,
        });
      }
    }
    if (migrate?.error) {
      out.push({
        id: "migrate",
        sev: "error",
        title: "Embed migrate",
        detail: migrate.error,
        at: now,
      });
    } else if (migrate && migrate.state === "draining") {
      out.push({
        id: "migrate-drain",
        sev: "warn",
        title: "Embed drain in progress",
        detail: `${migrate.drained ?? 0}/${migrate.total ?? 0}`,
        at: now,
      });
    }
    if (reliability?.mode && reliability.mode !== "normal") {
      out.push({
        id: "rel",
        sev: "warn",
        title: `Mode ${reliability.mode}`,
        detail: reliability.memory_used_bytes
          ? `mem ${fmtBytes(reliability.memory_used_bytes)}`
          : "resource pressure",
        at: now,
      });
    }
    return out.slice(0, 8);
  }, [err, follower.err, regionB.err, repl, migrate, reliability]);

  const leaderDocs = health?.docs ?? 0;
  const followerDocs = follower.health?.docs ?? null;
  const regionDocs = regionB.health?.docs ?? null;
  const deltaFollow =
    followerDocs != null ? Math.abs(leaderDocs - followerDocs) : null;
  const deltaRegion =
    regionDocs != null ? Math.abs(leaderDocs - regionDocs) : null;
  const maxDelta = Math.max(deltaFollow ?? 0, deltaRegion ?? 0);

  const mission: MissionHealth = useMemo(() => {
    if (!health) return "amber";
    if (alerts.some((a) => a.sev === "error")) return "red";
    if (alerts.some((a) => a.sev === "warn") || maxDelta > 0) return "amber";
    return "green";
  }, [health, alerts, maxDelta]);

  const missionLabel =
    mission === "green" ? "NOMINAL" : mission === "amber" ? "DEGRADED" : "CRITICAL";

  const xdcrState = useMemo(() => {
    const remotes = repl?.remotes ?? [];
    if (remotes.length === 0) return "idle";
    if (remotes.every((r) => r.healthy)) return "running";
    if (remotes.some((r) => r.healthy)) return "degraded";
    return "down";
  }, [repl]);

  const phaseIndex = useMemo(() => {
    if (!health) return 0;
    if (mission === "red") return 1;
    if (migrate && ["dumping", "awaiting_restart", "draining"].includes(migrate.state))
      return 4;
    if (maxDelta > 0 || repl?.behind) return 3;
    if ((repl?.remotes?.length ?? 0) > 0 || (topo?.peers?.length ?? 0) > 0) return 5;
    return 2;
  }, [health, mission, migrate, maxDelta, repl, topo]);

  const migratePct =
    migrate && migrate.total && migrate.total > 0
      ? Math.min(100, Math.round(((migrate.drained ?? 0) / migrate.total) * 100))
      : mission === "green"
        ? 100
        : Math.round(((phaseIndex + 1) / PHASES.length) * 100);

  const spark = sparkFrom(reqHistory);
  const healthyPeers = (topo?.peers ?? []).filter((p) => p.healthy === true).length;

  const runCmd = async (id: string, fn: () => Promise<unknown>, ok: string) => {
    setBusy(id);
    setCmdMsg(null);
    try {
      await fn();
      setCmdMsg(ok);
      pushLog("ops", "info", ok);
      await refresh();
    } catch (e) {
      const msg =
        e instanceof ApiError ? `${e.status}: ${e.body || e.code}` : (e as Error).message;
      setCmdMsg(`Failed: ${msg}`);
      pushLog("ops", "error", msg);
    } finally {
      setBusy(null);
    }
  };

  const goShowcase = () => {
    if (onLeave) onLeave();
    else window.location.assign("/");
  };

  return (
    <div className="nebula-cockpit">
      <div className="ck-shell">
        <div className="ck-status-bar">
          <div className={`ck-pill ${mission}`}>
            <span className="ck-pill-dot" />
            Mission status · {missionLabel}
          </div>
          <div className="ck-meta">
            <span>
              <strong>{(topo?.peers?.length ?? 0) + 1}</strong> nodes
            </span>
            <span>
              peers healthy <strong className={healthyPeers > 0 ? "good" : "warn"}>{healthyPeers}</strong>
            </span>
            <span>
              XDCR:{" "}
              <strong className={xdcrState === "running" ? "good" : xdcrState === "down" ? "bad" : "warn"}>
                {xdcrState.toUpperCase()}
              </strong>
            </span>
            <span>
              Δ docs:{" "}
              <strong className={maxDelta === 0 ? "good" : "warn"}>{fmt(maxDelta)}</strong>
            </span>
            <span>
              env <strong>{health?.env ?? "—"}</strong>
            </span>
          </div>
          <div className="ml-auto flex items-center gap-2 z-[1]">
            <button
              type="button"
              className="ck-btn"
              style={{ padding: "6px 10px" }}
              onClick={() => setPaused((p) => !p)}
            >
              {paused ? "Resume" : "Pause"}
            </button>
            <button
              type="button"
              className="ck-btn"
              style={{ padding: "6px 10px" }}
              onClick={goShowcase}
              title="Back to showcase demos"
            >
              ← Showcase
            </button>
          </div>
        </div>

        <div className="ck-top-strip">
          <div className="ck-tile">
            <div className="ck-legend">Source · Leader</div>
            <span className="ck-role leader">{topo?.role ?? "Leader"}</span>
            <div className="ck-title">{topo?.self_id ?? "nebuladb"}</div>
            <div className="ck-sub">
              {health
                ? `${health.model} · dim ${health.dim} · ${health.image_tag ?? health.git_commit ?? "—"}`
                : "Waiting for /healthz…"}
            </div>
            <div className="ck-metrics">
              <div>
                <div className="ck-metric-label">Documents</div>
                <div className="ck-metric-value">{fmt(leaderDocs)}</div>
              </div>
              <div>
                <div className="ck-metric-label">Region</div>
                <div className="ck-metric-value">{repl?.region ?? "—"}</div>
              </div>
              <div>
                <div className="ck-metric-label">Requests</div>
                <div className="ck-metric-value">{fmt(stats?.requests_total)}</div>
              </div>
              <div>
                <div className="ck-metric-label">Searches</div>
                <div className="ck-metric-value">
                  {fmt((stats?.searches_vector ?? 0) + (stats?.searches_semantic ?? 0))}
                </div>
              </div>
            </div>
          </div>

          <div className="ck-tile">
            <div className="ck-legend">Target · Within-region</div>
            <span className="ck-role follower">Follower</span>
            <div className="ck-title">nebuladb-follower</div>
            <div className="ck-sub">
              {follower.health
                ? `${follower.health.model} · ${follower.health.status}`
                : follower.err
                  ? "Unreachable via /peer/follower/healthz"
                  : "Probing…"}
            </div>
            <div className="ck-metrics">
              <div>
                <div className="ck-metric-label">Documents</div>
                <div className="ck-metric-value">{fmt(followerDocs)}</div>
              </div>
              <div>
                <div className="ck-metric-label">Δ vs leader</div>
                <div className={`ck-metric-value ${deltaFollow === 0 ? "" : "warn"}`}>
                  {fmt(deltaFollow)}
                </div>
              </div>
              <div>
                <div className="ck-metric-label">WAL lag</div>
                <div className="ck-metric-value">{fmtBytes(repl?.lag_bytes)}</div>
              </div>
              <div>
                <div className="ck-metric-label">Behind</div>
                <div className="ck-metric-value">{repl?.behind ? "YES" : "no"}</div>
              </div>
            </div>
          </div>

          <div className="ck-tile">
            <div className="ck-legend">Cross-region · XDCR</div>
            <div className="ck-flow">
              <div className="ck-flow-node">
                <div className="ck-metric-label">Leader</div>
                <div className="ck-metric-value">{fmt(leaderDocs)}</div>
              </div>
              <div className="ck-flow-arrow">→</div>
              <div className="ck-flow-node">
                <div className="ck-metric-label">Region-B</div>
                <div className="ck-metric-value">{fmt(regionDocs)}</div>
              </div>
            </div>
            <div className="ck-metrics">
              <div>
                <div className="ck-metric-label">Remotes</div>
                <div className="ck-metric-value">{repl?.remotes?.length ?? 0}</div>
              </div>
              <div>
                <div className="ck-metric-label">Applied</div>
                <div className="ck-metric-value">
                  {fmt(
                    (repl?.remotes ?? []).reduce((a, r) => a + (r.applied_records || 0), 0)
                  )}
                </div>
              </div>
              <div>
                <div className="ck-metric-label">State</div>
                <div className="ck-metric-value">{xdcrState}</div>
              </div>
              <div>
                <div className="ck-metric-label">Region-B Δ</div>
                <div className="ck-metric-value">{fmt(deltaRegion)}</div>
              </div>
            </div>
            {(repl?.remotes ?? []).slice(0, 2).map((r) => (
              <div key={r.region} className="ck-sub" style={{ marginTop: 8, marginBottom: 0 }}>
                <span className="ck-role region">{r.region}</span>{" "}
                {r.healthy ? "healthy" : r.last_error || "down"} · {r.grpc_url}
              </div>
            ))}
          </div>

          <div className="ck-tile ck-tile-stack">
            <div>
              <div className="ck-legend">Load · Request delta / poll</div>
              <div className="ck-metrics">
                <div>
                  <div className="ck-metric-label">RAG</div>
                  <div className="ck-metric-value">{fmt(stats?.rag_requests)}</div>
                </div>
                <div>
                  <div className="ck-metric-label">Errors</div>
                  <div className="ck-metric-value">{fmt(stats?.requests_errors)}</div>
                </div>
              </div>
              <div className="ck-spark" aria-hidden>
                {spark.map((h, i) => (
                  <span key={i} style={{ height: `${Math.round(h * 100)}%` }} />
                ))}
              </div>
            </div>
            <div>
              <div className="ck-legend">Alerts · {alerts.length}</div>
              {alerts.length === 0 ? (
                <div className="ck-sub" style={{ margin: 0 }}>
                  No active alerts — annunciators quiet.
                </div>
              ) : (
                alerts.slice(0, 4).map((a) => (
                  <div key={a.id} className="ck-alert-row">
                    <span className={`ck-sev ${a.sev}`}>{a.sev}</span>
                    <div>
                      <div style={{ fontWeight: 600 }}>{a.title}</div>
                      <div className="ck-mono" style={{ color: "var(--ck-muted)", fontSize: "0.68rem" }}>
                        {a.detail}
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        <div className="ck-timeline">
          <div className="ck-timeline-header">
            <div className="ck-timeline-title">Cluster readiness</div>
            <div className="ck-timeline-versions">
              {health?.version ? `v${health.version}` : "—"}
              <span className="arrow">→</span>
              {health?.image_tag ?? health?.git_commit?.slice(0, 7) ?? "live"}
            </div>
            <span className="ck-phase-badge">
              {migrate && migrate.state !== "idle"
                ? `EMBED · ${migrate.state}`
                : `PHASE · ${PHASES[phaseIndex]?.label ?? "—"}`}
            </span>
            <span className="ck-mono" style={{ color: "var(--ck-muted)", fontSize: "0.72rem" }}>
              {migratePct}%
            </span>
          </div>
          <div className="ck-phase-rail">
            {PHASES.map((p, i) => (
              <div
                key={p.id}
                className={`ck-phase-step ${i < phaseIndex ? "done" : ""} ${i === phaseIndex ? "active" : ""}`}
              >
                <div className="ck-phase-dot" />
                <div className="ck-phase-label">{p.label}</div>
              </div>
            ))}
          </div>
          <div className="ck-progress">
            <i style={{ width: `${migratePct}%` }} />
          </div>
          <div className="ck-events">
            {audit.slice(0, 8).map((a, i) => (
              <div
                key={`${a.ts_ms}-${i}`}
                className={`ck-event ${a.status >= 500 ? "error" : a.status >= 400 ? "warn" : ""}`}
              >
                <span className="ck-event-title">
                  {a.method} {a.status}
                </span>
                <span className="ck-event-detail">{a.path}</span>
                <span className="ck-event-time">{clock(a.ts_ms)}</span>
              </div>
            ))}
            {audit.length === 0 && (
              <div className="ck-sub" style={{ textAlign: "center", padding: 12 }}>
                Waiting for audit events…
              </div>
            )}
          </div>
        </div>

        <div className="ck-bottom">
          <div className="ck-col">
            <div className="ck-col-head">
              <div className="ck-legend" style={{ margin: 0 }}>
                Live signal log
              </div>
              <span className="ck-role" style={{ marginLeft: "auto" }}>
                {logs.length} lines
              </span>
            </div>
            <div className="ck-col-body">
              <div className="ck-log-tail">
                {[...logs].reverse().map((l) => (
                  <div key={l.id} className={`ck-log-line ${l.sev}`}>
                    <span className="ts">{clock(l.ts)}</span>
                    <span className="src">{l.src}</span>
                    <span className="msg">{l.msg}</span>
                  </div>
                ))}
                {logs.length === 0 && (
                  <div style={{ color: "var(--ck-faint)", padding: 12 }}>Tailing derived ops signals…</div>
                )}
              </div>
            </div>
          </div>

          <div className="ck-col">
            <div className="ck-col-head">
              <div className="ck-legend" style={{ margin: 0 }}>
                Data integrity
              </div>
            </div>
            <div className="ck-col-body">
              <div className="ck-proof-grid">
                <div className="ck-proof-cell">
                  <div className="ck-metric-label">Leader docs</div>
                  <div className="ck-metric-value">{fmt(leaderDocs)}</div>
                </div>
                <div className="ck-proof-cell">
                  <div className="ck-metric-label">Follower docs</div>
                  <div className="ck-metric-value">{fmt(followerDocs)}</div>
                </div>
                <div className="ck-proof-cell">
                  <div className="ck-metric-label">Region-B docs</div>
                  <div className="ck-metric-value">{fmt(regionDocs)}</div>
                </div>
                <div className="ck-proof-cell">
                  <div className="ck-metric-label">Max Δ</div>
                  <div className="ck-metric-value">{fmt(maxDelta)}</div>
                </div>
                <div className="ck-proof-cell">
                  <div className="ck-metric-label">WAL segments</div>
                  <div className="ck-metric-value">{fmt(dur?.wal?.segment_count)}</div>
                </div>
                <div className="ck-proof-cell">
                  <div className="ck-metric-label">WAL bytes</div>
                  <div className="ck-metric-value">{fmtBytes(dur?.wal?.total_bytes)}</div>
                </div>
              </div>
              <div className="ck-sub" style={{ marginBottom: 10 }}>
                Persistent: {dur?.persistent ? "yes" : "ephemeral"}
                {dur?.data_dir ? ` · ${dur.data_dir}` : ""}
                {reliability?.mode ? ` · mode ${reliability.mode}` : ""}
              </div>
              <div
                className={`ck-verdict ${
                  maxDelta === 0 && mission !== "red" ? "ok" : mission === "red" ? "bad" : "warn"
                }`}
              >
                {maxDelta === 0 && mission !== "red"
                  ? "Zero-delta · peers aligned"
                  : mission === "red"
                    ? "Integrity caution · investigate alerts"
                    : "Convergence in progress"}
              </div>
            </div>
          </div>

          <div className="ck-col">
            <div className="ck-col-head">
              <div className="ck-legend" style={{ margin: 0 }}>
                Mission controls
              </div>
            </div>
            <div className="ck-col-body">
              <div className="ck-controls">
                <button
                  type="button"
                  className="ck-btn ok"
                  disabled={!!busy}
                  onClick={() =>
                    void runCmd("snap", () => api.takeSnapshot(), "Snapshot captured")
                  }
                >
                  {busy === "snap" ? "…" : "Snapshot"}
                </button>
                <button
                  type="button"
                  className="ck-btn"
                  disabled={!!busy}
                  onClick={() =>
                    void runCmd("wal", () => api.compactWal(), "WAL compact requested")
                  }
                >
                  {busy === "wal" ? "…" : "Compact WAL"}
                </button>
                <button
                  type="button"
                  className="ck-btn danger"
                  disabled={!!busy}
                  onClick={() => {
                    if (
                      !confirm(
                        "Promote the within-region follower to leader? Only when the current leader is down or you are practising failover."
                      )
                    )
                      return;
                    void runCmd(
                      "promote",
                      () => api.promoteOnPeer("follower"),
                      "Promote issued on follower"
                    );
                  }}
                >
                  {busy === "promote" ? "…" : "Promote follower"}
                </button>
                <button
                  type="button"
                  className="ck-btn"
                  disabled={!!busy}
                  onClick={() => void refresh()}
                >
                  Refresh now
                </button>
                <button
                  type="button"
                  className="ck-btn"
                  onClick={() => window.location.assign("/#/tab/cluster")}
                >
                  Cluster tab
                </button>
                <button
                  type="button"
                  className="ck-btn"
                  onClick={() => window.location.assign("/#/tab/rebalance")}
                >
                  Rebalance
                </button>
                <button
                  type="button"
                  className="ck-btn"
                  onClick={() => window.location.assign("/#/tab/admin")}
                >
                  Admin
                </button>
                <button
                  type="button"
                  className="ck-btn"
                  onClick={() => window.location.assign("/#/tab/metrics")}
                >
                  Metrics
                </button>
              </div>
              <div className="ck-cmd-msg">{cmdMsg ?? " "}</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export function isCockpitPath(pathname = window.location.pathname, hash = window.location.hash): boolean {
  if (pathname === "/cockpit" || pathname.endsWith("/cockpit")) return true;
  return hash === "#/cockpit" || hash.startsWith("#/cockpit?");
}

/** Canonicalise hash deep-links onto the login-gated /cockpit path. */
export function ensureCockpitPath(): void {
  if (window.location.hash.startsWith("#/cockpit") && window.location.pathname !== "/cockpit") {
    window.history.replaceState(null, "", "/cockpit");
  }
}

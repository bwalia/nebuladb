/**
 * Live cluster / XDCR / replication lag (build brief sections 20–22).
 *
 * Bound only to GET /admin/cluster/nodes and GET /admin/replication.
 * Promote hits the follower via /peer/follower/… (nginx). No simulated
 * multi-region map or failover drill.
 */
import { useCallback, useEffect, useState } from "react";
import {
  api,
  ApiError,
  type ClusterPeer,
  type ClusterTopology,
  type ReplicationInfo,
  type RemoteRegionStatus,
} from "../api";
import { Panel, ErrorBanner } from "../components";
import { OriginBadge } from "../components/Provenance";
import { UnderTheHood } from "../components/UnderTheHood";

const POLL_MS = 4000;

export function ClusterTab() {
  const [topo, setTopo] = useState<ClusterTopology | null>(null);
  const [repl, setRepl] = useState<ReplicationInfo | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [promoteMsg, setPromoteMsg] = useState<string | null>(null);
  const [promoting, setPromoting] = useState(false);

  const refresh = useCallback(async () => {
    try {
      const [n, r] = await Promise.all([api.clusterNodes(), api.replication()]);
      setTopo(n);
      setRepl(r);
      setErr(null);
    } catch (e) {
      setErr((e as Error).message);
    }
  }, []);

  useEffect(() => {
    void refresh();
    const id = setInterval(() => void refresh(), POLL_MS);
    return () => clearInterval(id);
  }, [refresh]);

  const onPromote = async () => {
    if (
      !confirm(
        "Promote the within-region follower to leader? Only use when the current leader is down or you are practising failover."
      )
    ) {
      return;
    }
    setPromoting(true);
    setPromoteMsg(null);
    try {
      const res = await api.promoteOnPeer("follower");
      setPromoteMsg(
        res.promoted
          ? `Promoted — node role is now ${res.role}.`
          : `No-op — peer already role=${res.role}.`
      );
      await refresh();
    } catch (e) {
      const msg =
        e instanceof ApiError ? `${e.status}: ${e.body || e.code}` : (e as Error).message;
      setPromoteMsg(`Promote failed: ${msg}`);
    } finally {
      setPromoting(false);
    }
  };

  const peers = topo?.peers ?? [];
  const healthyPeers = peers.filter((p) => p.healthy === true).length;
  const remotes = repl?.remotes ?? [];

  return (
    <div className="space-y-5">
      <Panel
        title="Cluster topology"
        subtitle={
          topo
            ? `${topo.self_id ?? "self"} · role ${topo.role ?? "?"} · ${peers.length} peers · ${healthyPeers} healthy`
            : "Loading live membership…"
        }
        action={
          <div className="flex items-center gap-2">
            <OriginBadge origin="live" from="GET /api/v1/admin/cluster/nodes" />
            <button
              className="btn-secondary !text-xs"
              onClick={() => void onPromote()}
              disabled={promoting}
              title="POST /peer/follower/api/v1/admin/promote"
            >
              {promoting ? "Promoting…" : "Promote follower"}
            </button>
          </div>
        }
      >
        <ErrorBanner err={err} />
        {promoteMsg && (
          <p className="mb-3 font-mono text-[11px] text-gray-600 dark:text-muted">{promoteMsg}</p>
        )}
        {!topo && !err && <p className="text-xs text-gray-500 dark:text-muted">loading…</p>}
        {topo && (
          <ul className="space-y-2">
            <SelfCard
              id={topo.self_id ?? "self"}
              role={topo.role ?? "unknown"}
              region={repl?.region ?? undefined}
              lagBytes={repl?.lag_bytes}
              behind={repl?.behind}
              localNewest={repl?.local_newest}
            />
            {peers.map((p) => (
              <PeerCard key={p.id} peer={p} />
            ))}
          </ul>
        )}
      </Panel>

      <Panel
        title="XDCR"
        subtitle={
          remotes.length
            ? `${remotes.length} remote region(s) · live CrossRegionReplicationService`
            : "No cross-region remotes (single-region or XDCR not configured)"
        }
        action={<OriginBadge origin="live" from="GET /api/v1/admin/replication → remotes" />}
      >
        {remotes.length === 0 ? (
          <p className="text-xs text-gray-500 dark:text-muted">
            Enable <code className="font-mono">ha.crossRegion</code> and deploy region-b to see
            bidirectional WAL tails here.
          </p>
        ) : (
          <ul className="space-y-2">
            {remotes.map((r) => (
              <RemoteCard key={r.region} remote={r} />
            ))}
          </ul>
        )}
      </Panel>

      <Panel
        title="Replication lag"
        subtitle="Follower byte lag and whether this node is behind the leader tip"
        action={<OriginBadge origin="live" from="lag_bytes / behind on /admin/replication" />}
      >
        <div className="grid gap-2 sm:grid-cols-3">
          <Metric
            label="lag_bytes"
            value={
              repl?.lag_bytes == null ? "—" : fmtBytes(repl.lag_bytes)
            }
            tone={repl?.behind ? "warn" : "ok"}
          />
          <Metric
            label="behind"
            value={
              repl?.behind == null ? "n/a" : repl.behind ? "yes" : "no"
            }
            tone={repl?.behind ? "warn" : "ok"}
          />
          <Metric
            label="local region"
            value={repl?.region ?? "—"}
          />
        </div>
        {repl?.follower_applied && (
          <p className="mt-3 font-mono text-[10px] text-gray-500 dark:text-muted">
            follower_applied seg={repl.follower_applied.segment_seq} off=
            {repl.follower_applied.byte_offset}
            {repl.leader_newest_probed &&
              ` · leader_tip seg=${repl.leader_newest_probed.segment_seq} off=${repl.leader_newest_probed.byte_offset}`}
          </p>
        )}
      </Panel>

      <UnderTheHood filter={["/admin/cluster", "/admin/replication", "/admin/promote"]} />
    </div>
  );
}

function SelfCard({
  id,
  role,
  region,
  lagBytes,
  behind,
  localNewest,
}: {
  id: string;
  role: string;
  region?: string;
  lagBytes?: number | null;
  behind?: boolean | null;
  localNewest?: { segment_seq: number; byte_offset: number } | null;
}) {
  return (
    <li className="rounded-md border border-accent/30 bg-accent/5 p-2.5 dark:border-accent/40">
      <div className="flex flex-wrap items-center gap-2">
        <span className="dot dot-ok" />
        <span className="font-mono text-[11px] font-semibold text-gray-900 dark:text-ink">{id}</span>
        <RoleBadge role={role} />
        {region && (
          <span className="rounded border border-gray-200 px-1 font-mono text-[9px] text-gray-500 dark:border-edge dark:text-muted">
            {region}
          </span>
        )}
        <span className="ml-auto font-mono text-[10px] text-gray-500 dark:text-muted">this node</span>
      </div>
      <div className="mt-2 font-mono text-[10px] text-gray-500 dark:text-muted">
        {localNewest
          ? `WAL tip seg=${localNewest.segment_seq} off=${localNewest.byte_offset}`
          : "no local WAL tip"}
        {lagBytes != null && ` · lag ${fmtBytes(lagBytes)}`}
        {behind != null && ` · behind=${behind}`}
      </div>
    </li>
  );
}

function PeerCard({ peer }: { peer: ClusterPeer }) {
  const ok = peer.healthy === true;
  const sick = peer.healthy === false;
  const dot = ok ? "dot-ok" : sick ? "dot-bad" : "dot-idle";
  return (
    <li
      className={`rounded-md border p-2.5 ${
        sick
          ? "border-bad/40 bg-bad/5"
          : "border-gray-200 bg-white dark:border-edge dark:bg-carbon-900"
      }`}
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className={`dot ${dot}`} />
        <span className="font-mono text-[11px] font-semibold text-gray-900 dark:text-ink">
          {peer.id}
        </span>
        <span className="ml-auto font-mono text-[10px] text-gray-500 dark:text-muted">
          {peer.docs != null ? `${peer.docs.toLocaleString()} docs` : "docs —"}
          {peer.probe_ms != null ? ` · ${peer.probe_ms} ms` : ""}
        </span>
      </div>
      <div className="mt-1 truncate font-mono text-[10px] text-gray-400 dark:text-faint">
        {peer.base_url}
        {peer.healthy == null && " · unreachable"}
        {sick && " · unhealthy"}
      </div>
    </li>
  );
}

function RemoteCard({ remote }: { remote: RemoteRegionStatus }) {
  const dot = remote.healthy ? "dot-ok" : "dot-bad";
  return (
    <li
      className={`rounded-md border p-2.5 ${
        remote.healthy
          ? "border-gray-200 bg-white dark:border-edge dark:bg-carbon-900"
          : "border-bad/40 bg-bad/5"
      }`}
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className={`dot ${dot}`} />
        <span className="font-mono text-[11px] font-semibold text-gray-900 dark:text-ink">
          {remote.region}
        </span>
        <span className="rounded border border-accent/40 bg-accent/10 px-1 font-mono text-[9px] text-accent">
          XDCR
        </span>
        <span className="ml-auto font-mono text-[10px] text-gray-500 dark:text-muted">
          {remote.applied_records.toLocaleString()} applied
          {remote.consecutive_errors > 0 && ` · ${remote.consecutive_errors} errs`}
        </span>
      </div>
      <div className="mt-1 font-mono text-[10px] text-gray-400 dark:text-faint">
        {remote.grpc_url}
        {remote.last_applied_segment != null &&
          ` · cursor seg=${remote.last_applied_segment} off=${remote.last_applied_byte_offset ?? 0}`}
      </div>
      {remote.last_error && (
        <p className="mt-1 font-mono text-[10px] text-bad">{remote.last_error}</p>
      )}
    </li>
  );
}

function RoleBadge({ role }: { role: string }) {
  const primary = role === "leader" || role === "primary";
  return (
    <span
      className={`rounded border px-1 font-mono text-[9px] ${
        primary
          ? "border-accent/40 bg-accent/10 text-accent"
          : "border-gray-200 text-gray-500 dark:border-edge dark:text-muted"
      }`}
    >
      {role}
    </span>
  );
}

function Metric({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "ok" | "warn";
}) {
  return (
    <div className="rounded-md border border-gray-200 bg-white p-2.5 dark:border-edge dark:bg-carbon-900">
      <div className="eyebrow">{label}</div>
      <div
        className={`mt-0.5 font-mono text-sm font-semibold tabular-nums ${
          tone === "warn" ? "text-warn" : "text-gray-900 dark:text-ink"
        }`}
      >
        {value}
      </div>
    </div>
  );
}

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

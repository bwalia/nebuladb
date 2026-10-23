/**
 * Live rebalance primitives: bucket export/import, home-region, promote.
 *
 * Composes real admin APIs (no simulated phase machine). Cross-region
 * import targets region-b via /peer/region-b/… when HA is configured.
 */
import { useCallback, useEffect, useState } from "react";
import {
  api,
  ApiError,
  type BucketExport,
  type HomeRegionInfo,
  type ReplicationInfo,
} from "../api";
import { Panel, ErrorBanner } from "../components";
import { OriginBadge } from "../components/Provenance";
import { UnderTheHood } from "../components/UnderTheHood";

type PeerTarget = "primary" | "region-b";

export function RebalanceTab() {
  const [buckets, setBuckets] = useState<string[]>([]);
  const [bucket, setBucket] = useState("");
  const [home, setHome] = useState<HomeRegionInfo | null>(null);
  const [repl, setRepl] = useState<ReplicationInfo | null>(null);
  const [target, setTarget] = useState<PeerTarget>("region-b");
  const [log, setLog] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [exported, setExported] = useState<BucketExport | null>(null);

  const push = (line: string) =>
    setLog((prev) => [`${new Date().toISOString().slice(11, 19)}  ${line}`, ...prev].slice(0, 40));

  const refreshMeta = useCallback(async () => {
    try {
      const [b, r] = await Promise.all([api.buckets(), api.replication()]);
      setRepl(r);
      const names = b.map((x) => x.bucket).filter(Boolean);
      setBuckets(names);
      if (!bucket && names.length) setBucket(names[0]!);
      setErr(null);
    } catch (e) {
      setErr((e as Error).message);
    }
  }, [bucket]);

  useEffect(() => {
    void refreshMeta();
  }, [refreshMeta]);

  useEffect(() => {
    if (!bucket) {
      setHome(null);
      return;
    }
    let dead = false;
    (async () => {
      try {
        const h = await api.homeRegion(bucket);
        if (!dead) setHome(h);
      } catch {
        if (!dead) setHome(null);
      }
    })();
    return () => {
      dead = true;
    };
  }, [bucket]);

  const onExport = async () => {
    if (!bucket) return;
    setBusy(true);
    setErr(null);
    try {
      push(`export ${bucket} from primary…`);
      const body = await api.exportBucket(bucket);
      setExported(body);
      const n = Array.isArray(body.docs) ? body.docs.length : 0;
      push(`exported ${n} docs (dim=${body.dim ?? "?"})`);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body || e.message : (e as Error).message;
      setErr(msg);
      push(`export failed: ${msg}`);
    } finally {
      setBusy(false);
    }
  };

  const onImport = async () => {
    if (!bucket || !exported) return;
    setBusy(true);
    setErr(null);
    try {
      const peer = target === "region-b" ? "region-b" : undefined;
      push(`import ${bucket} → ${target}…`);
      const res = await api.importBucket(bucket, exported, peer);
      push(`imported ${res.imported} docs onto ${target}`);
      await refreshMeta();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body || e.message : (e as Error).message;
      setErr(msg);
      push(`import failed: ${msg}`);
    } finally {
      setBusy(false);
    }
  };

  const onSeedHome = async (region: string) => {
    if (!bucket) return;
    if (
      !confirm(
        `Write home_region=${region} seed doc into bucket "${bucket}" on the primary? Wrong-home writes will then be rejected elsewhere.`
      )
    ) {
      return;
    }
    setBusy(true);
    setErr(null);
    try {
      push(`seed home_region=${region} on ${bucket}…`);
      await api.upsertDoc(bucket, "__nebuladb_operator_seed__", "nebuladb operator seed", {
        kind: "nebuladb-operator-seed",
        bucket,
        home_region: region,
        home_epoch: 1,
        replicated_to: region === "us-east-1" ? ["us-west-2"] : ["us-east-1"],
      });
      const h = await api.homeRegion(bucket);
      setHome(h);
      push(`home_region now ${h.home_region ?? "unset"} epoch=${h.home_epoch ?? "?"}`);
    } catch (e) {
      const msg = e instanceof ApiError ? e.body || e.message : (e as Error).message;
      setErr(msg);
      push(`seed failed: ${msg}`);
    } finally {
      setBusy(false);
    }
  };

  const onPromote = async () => {
    if (!confirm("Promote follower → leader via POST /peer/follower/api/v1/admin/promote?")) return;
    setBusy(true);
    try {
      const res = await api.promoteOnPeer("follower");
      push(
        res.promoted
          ? `promoted — role=${res.role}`
          : `promote no-op — role=${res.role}`
      );
      await refreshMeta();
    } catch (e) {
      const msg = e instanceof ApiError ? e.body || e.message : (e as Error).message;
      setErr(msg);
      push(`promote failed: ${msg}`);
    } finally {
      setBusy(false);
    }
  };

  const remotes = repl?.remotes ?? [];

  return (
    <div className="space-y-5">
      <Panel
        title="Live replication"
        subtitle="Same signals the Cluster tab uses — lag and XDCR remotes"
        action={<OriginBadge origin="live" from="GET /api/v1/admin/replication" />}
      >
        <div className="grid gap-2 sm:grid-cols-3">
          <Metric label="lag_bytes" value={repl?.lag_bytes == null ? "—" : fmtBytes(repl.lag_bytes)} />
          <Metric
            label="behind"
            value={repl?.behind == null ? "n/a" : repl.behind ? "yes" : "no"}
            tone={repl?.behind ? "warn" : undefined}
          />
          <Metric label="XDCR remotes" value={String(remotes.length)} />
        </div>
        {remotes.length > 0 && (
          <ul className="mt-3 space-y-1 font-mono text-[10px] text-gray-500 dark:text-muted">
            {remotes.map((r) => (
              <li key={r.region}>
                {r.region}: {r.healthy ? "healthy" : "unhealthy"} · {r.applied_records} applied
                {r.last_error ? ` · ${r.last_error}` : ""}
              </li>
            ))}
          </ul>
        )}
      </Panel>

      <Panel
        title="Bucket swap (export → import)"
        subtitle="gap #4 primitive — move a bucket’s docs to another region node"
        action={<OriginBadge origin="live" from="GET/POST /admin/bucket/:b/export|import" />}
      >
        <ErrorBanner err={err} />
        <div className="flex flex-wrap items-end gap-3">
          <label className="text-xs text-gray-600 dark:text-muted">
            Bucket
            <select
              className="mt-1 block min-w-[10rem] rounded border border-gray-200 bg-white px-2 py-1 font-mono text-[11px] dark:border-edge dark:bg-carbon-900"
              value={bucket}
              onChange={(e) => setBucket(e.target.value)}
              disabled={busy}
            >
              {buckets.length === 0 && <option value="">(no buckets)</option>}
              {buckets.map((b) => (
                <option key={b} value={b}>
                  {b}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs text-gray-600 dark:text-muted">
            Import target
            <select
              className="mt-1 block rounded border border-gray-200 bg-white px-2 py-1 font-mono text-[11px] dark:border-edge dark:bg-carbon-900"
              value={target}
              onChange={(e) => setTarget(e.target.value as PeerTarget)}
              disabled={busy}
            >
              <option value="primary">primary</option>
              <option value="region-b">region-b (/peer/region-b)</option>
            </select>
          </label>
          <button className="btn !text-xs" disabled={busy || !bucket} onClick={() => void onExport()}>
            Export
          </button>
          <button
            className="btn-secondary !text-xs"
            disabled={busy || !exported}
            onClick={() => void onImport()}
          >
            Import
          </button>
        </div>
        {home && (
          <p className="mt-3 font-mono text-[11px] text-gray-600 dark:text-muted">
            home_region={home.home_region ?? "(unset)"} epoch={home.home_epoch ?? "—"}
            {home.replicated_to?.length ? ` · replicated_to=[${home.replicated_to.join(", ")}]` : ""}
          </p>
        )}
        {exported && (
          <p className="mt-1 font-mono text-[10px] text-gray-400 dark:text-faint">
            staged export: {Array.isArray(exported.docs) ? exported.docs.length : 0} docs
          </p>
        )}
      </Panel>

      <Panel
        title="Home region + promote"
        subtitle="Active-active write affinity and within-region failover"
        action={<OriginBadge origin="live" from="seed doc + POST /admin/promote" />}
      >
        <div className="flex flex-wrap gap-2">
          <button
            className="btn-secondary !text-xs"
            disabled={busy || !bucket}
            onClick={() => void onSeedHome("us-east-1")}
          >
            Home bucket → us-east-1
          </button>
          <button
            className="btn-secondary !text-xs"
            disabled={busy || !bucket}
            onClick={() => void onSeedHome("us-west-2")}
          >
            Home bucket → us-west-2
          </button>
          <button className="btn !text-xs" disabled={busy} onClick={() => void onPromote()}>
            Promote follower
          </button>
        </div>
        <p className="mt-3 text-[11px] leading-relaxed text-gray-500 dark:text-muted">
          Writes are only accepted in a bucket’s home region. XDCR tails the home WAL into
          peers so reads stay local. Promote is for within-region failover when the leader is
          down — it calls the follower peer, not this page’s simulation.
        </p>
      </Panel>

      {log.length > 0 && (
        <Panel title="Activity" subtitle="Most recent first">
          <ol className="space-y-1 font-mono text-[11px] text-gray-700 dark:text-muted">
            {log.map((l, i) => (
              <li key={i}>{l}</li>
            ))}
          </ol>
        </Panel>
      )}

      <UnderTheHood
        filter={["/admin/bucket", "/admin/promote", "/admin/replication", "/peer/"]}
      />
    </div>
  );
}

function Metric({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "warn";
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

/**
 * Cluster, multi-region and failover (build brief sections 20-22).
 *
 * Two layers, kept visibly distinct:
 *   - What this server actually reports: /admin/cluster/nodes and
 *     /admin/replication. On a standalone deployment that is one node,
 *     and the page says so rather than pretending otherwise.
 *   - A simulated six-node, two-region topology used to demonstrate
 *     shard placement, XDCR lag and a failover drill. Every number in
 *     that layer is badged SIMULATED.
 *
 * The failover drill is a state machine in demo/simulation.ts. It never
 * touches the real cluster — clicking "Simulate node failure" cannot
 * affect a running NebulaDB.
 */
import { useEffect, useRef, useState } from "react";
import { api, type ClusterNode, type ReplicationInfo } from "../api";
import {
  buildCluster,
  tickCluster,
  initialFailover,
  startFailover,
  tickFailover,
  type SimCluster,
  type SimNode,
  type FailoverState,
} from "../demo/simulation";
import { Panel, ErrorBanner, JsonView } from "../components";
import { OriginBadge, SimulationNotice } from "../components/Provenance";
import { UnderTheHood } from "../components/UnderTheHood";

const TICK_MS = 700;

export function ClusterTab() {
  const [cluster, setCluster] = useState<SimCluster>(() => buildCluster());
  const [failover, setFailover] = useState<FailoverState>(initialFailover);
  const [tick, setTick] = useState(0);
  const [realNodes, setRealNodes] = useState<ClusterNode[] | null>(null);
  const [repl, setRepl] = useState<ReplicationInfo | null>(null);
  const [realErr, setRealErr] = useState<string | null>(null);
  const clusterRef = useRef(cluster);
  clusterRef.current = cluster;

  // Real admin surface. Both endpoints exist; on a standalone node they
  // return a minimal shape, which we render verbatim.
  useEffect(() => {
    let dead = false;
    (async () => {
      try {
        const [n, r] = await Promise.all([api.clusterNodes(), api.replication()]);
        if (dead) return;
        setRealNodes(n);
        setRepl(r);
      } catch (e) {
        if (!dead) setRealErr((e as Error).message);
      }
    })();
    return () => {
      dead = true;
    };
  }, []);

  // Drive both simulators from one clock.
  useEffect(() => {
    const id = setInterval(() => {
      setTick((t) => t + 1);
      setCluster((c) => tickCluster(c, tick));
      setFailover((f) => tickFailover(f, TICK_MS, clusterRef.current));
    }, TICK_MS);
    return () => clearInterval(id);
  }, [tick]);

  // Reflect the drill's effect on node health in the topology view.
  useEffect(() => {
    if (!failover.targetNodeId) return;
    setCluster((c) => ({
      ...c,
      regions: c.regions.map((reg) => ({
        ...reg,
        nodes: reg.nodes.map((n) => {
          if (n.id === failover.targetNodeId) {
            const down = ["failure", "detection", "promotion", "routing"].includes(failover.phase);
            return { ...n, health: down ? "down" : failover.phase === "recovery" ? "joining" : "healthy" };
          }
          if (n.id === failover.promotedNodeId && failover.phase !== "healthy") {
            return { ...n, role: "primary" as const };
          }
          return n;
        }),
      })),
    }));
  }, [failover.phase, failover.targetNodeId, failover.promotedNodeId]);

  const allNodes = cluster.regions.flatMap((r) => r.nodes);
  const healthy = allNodes.filter((n) => n.health === "healthy").length;

  const runDrill = () => {
    const victim = allNodes.find((n) => n.role === "primary" && n.health === "healthy") ?? allNodes[0];
    setFailover(startFailover(victim.id));
  };

  return (
    <div className="space-y-5">
      {/* Real first, so nobody mistakes the simulation for the cluster */}
      <Panel
        title="This server"
        subtitle="What the live admin API reports about cluster membership"
        action={<OriginBadge origin="live" from="GET /api/v1/admin/cluster/nodes" />}
      >
        <ErrorBanner err={realErr} />
        {!realErr && (
          <div className="grid gap-3 lg:grid-cols-2">
            <div>
              <div className="eyebrow mb-1">cluster/nodes</div>
              {realNodes ? <JsonView value={realNodes} /> : <p className="text-xs text-gray-500 dark:text-muted">loading…</p>}
            </div>
            <div>
              <div className="eyebrow mb-1">replication</div>
              {repl ? <JsonView value={repl} /> : <p className="text-xs text-gray-500 dark:text-muted">loading…</p>}
            </div>
          </div>
        )}
        <p className="text-[11px] leading-relaxed text-gray-500 dark:text-muted">
          A standalone deployment reports a single node here. The topology below is a
          simulated six-node cluster used to demonstrate shard placement, cross-region
          replication and failover.
        </p>
      </Panel>

      <SimulationNotice
        what="Topology, XDCR and the failover drill below are simulated"
        why="NebulaDB has no multi-region topology or failover-drill endpoint. This section runs a deterministic state machine in the browser so the behaviour can be demonstrated safely. Clicking 'Simulate node failure' cannot affect any real cluster."
      />

      {/* Topology --------------------------------------------------- */}
      <Panel
        title="Multi-region topology"
        subtitle={`${allNodes.length} nodes · ${cluster.shardCount} shards · RF ${cluster.replicationFactor} · ${healthy} healthy`}
        action={
          <button className="btn-secondary !text-xs" onClick={runDrill} disabled={failover.phase !== "healthy"}>
            {failover.phase === "healthy" ? "Simulate node failure" : "Drill running…"}
          </button>
        }
      >
        <div className="grid gap-4 lg:grid-cols-2">
          {cluster.regions.map((region, i) => (
            <div key={region.name} className="space-y-2">
              <div className="flex items-center gap-2">
                <span className="text-xs font-semibold text-gray-900 dark:text-ink">{region.label}</span>
                <span className="ml-auto font-mono text-[10px] text-gray-500 dark:text-muted">
                  lag {region.lagMs}ms · {region.throughputDocsSec.toLocaleString()} docs/s
                  {region.conflicts > 0 && ` · ${region.conflicts} conflicts`}
                </span>
              </div>
              <ul className="space-y-1.5">
                {region.nodes.map((n) => (
                  <NodeCard key={n.id} node={n} />
                ))}
              </ul>
              {i === 0 && (
                <div className="flex items-center justify-center gap-2 py-1 font-mono text-[10px] text-gray-400 dark:text-faint">
                  <span className="h-px w-8 bg-gray-300 dark:bg-edge" />
                  XDCR
                  <span className="h-px w-8 bg-gray-300 dark:bg-edge" />
                </div>
              )}
            </div>
          ))}
        </div>
      </Panel>

      {/* Failover timeline ------------------------------------------ */}
      {failover.phase !== "healthy" && (
        <Panel title="Failover drill" subtitle={`Phase: ${failover.phase}`}>
          <div className="space-y-3">
            <div className="flex flex-wrap gap-1.5">
              {["failure", "detection", "promotion", "routing", "recovery"].map((p) => {
                const order = ["failure", "detection", "promotion", "routing", "recovery"];
                const done = order.indexOf(p) < order.indexOf(failover.phase);
                const now = p === failover.phase;
                return (
                  <span
                    key={p}
                    className={`rounded border px-2 py-0.5 font-mono text-[10px] transition-colors
                      ${
                        now
                          ? "border-warn/50 bg-warn/15 text-warn"
                          : done
                            ? "border-ok/40 bg-ok/10 text-ok"
                            : "border-gray-200 text-gray-400 dark:border-edge dark:text-faint"
                      }`}
                  >
                    {p}
                  </span>
                );
              })}
            </div>
            <ol className="space-y-1">
              {failover.log.map((l, i) => (
                <li key={i} className="flex gap-3 font-mono text-[11px]">
                  <span className="shrink-0 tabular-nums text-gray-400 dark:text-faint">
                    +{(l.atMs / 1000).toFixed(1)}s
                  </span>
                  <span className="text-gray-700 dark:text-muted">{l.message}</span>
                </li>
              ))}
            </ol>
            {failover.phase === "recovery" && (
              <button className="btn-secondary !text-xs" onClick={() => setFailover(initialFailover())}>
                Reset drill
              </button>
            )}
          </div>
        </Panel>
      )}

      <UnderTheHood filter={["/admin/cluster", "/admin/replication"]} />
    </div>
  );
}

function NodeCard({ node }: { node: SimNode }) {
  const dot =
    node.health === "healthy"
      ? "dot-ok"
      : node.health === "down"
        ? "dot-bad"
        : node.health === "joining"
          ? "dot-warn"
          : "dot-idle";
  return (
    <li
      className={`rounded-md border p-2.5 transition-colors
        ${node.health === "down" ? "border-bad/40 bg-bad/5" : "border-gray-200 bg-white dark:border-edge dark:bg-carbon-900"}`}
    >
      <div className="flex items-center gap-2">
        <span className={`dot ${dot}`} />
        <span className="font-mono text-[11px] font-semibold text-gray-900 dark:text-ink">{node.id}</span>
        <span
          className={`rounded border px-1 font-mono text-[9px]
            ${
              node.role === "primary"
                ? "border-accent/40 bg-accent/10 text-accent"
                : "border-gray-200 text-gray-500 dark:border-edge dark:text-muted"
            }`}
        >
          {node.role}
        </span>
        <span className="ml-auto font-mono text-[10px] text-gray-400 dark:text-faint">
          {node.shards.length} shards
        </span>
      </div>
      <div className="mt-2 grid grid-cols-4 gap-2 font-mono text-[10px] text-gray-500 dark:text-muted">
        <Meter label="cpu" pct={node.cpuPct} />
        <Meter label="mem" pct={node.memPct} />
        <span className="tabular-nums">{node.storageGb} GB</span>
        <span className="tabular-nums">{node.qps.toLocaleString()} q/s</span>
      </div>
    </li>
  );
}

function Meter({ label, pct }: { label: string; pct: number }) {
  const tone = pct > 85 ? "bg-bad" : pct > 65 ? "bg-warn" : "bg-ok";
  return (
    <span className="flex items-center gap-1">
      <span>{label}</span>
      <span className="h-1 w-8 overflow-hidden rounded-full bg-gray-200 dark:bg-carbon-800">
        <span className={`block h-full ${tone}`} style={{ width: `${pct}%` }} />
      </span>
    </span>
  );
}

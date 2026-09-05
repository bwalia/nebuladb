/**
 * Executive Demo (build brief section 3).
 *
 * The five-minute story: enterprise data enters NebulaDB, becomes SQL +
 * JSON + vectors, feeds retrieval and RAG, and drives agents — all on
 * one highly available cluster.
 *
 * Live tiles come from /healthz, /admin/stats, /admin/buckets and
 * /admin/durability. Tiles the server has no endpoint for (QPS across a
 * multi-node cluster, replication lag, active agents) come from the
 * deterministic simulator and are badged SIMULATED. Nothing on this
 * screen presents a simulated number as a measurement.
 */
import { useEffect, useState } from "react";
import { api, type Health, type StatsSnapshot, type BucketStats, type DurabilityInfo } from "../api";
import { live, derived, type Sourced } from "../demo/provenance";
import { buildCluster, tickCluster, simExecSlice, type SimCluster } from "../demo/simulation";
import { SourcedStat, OriginBadge } from "../components/Provenance";
import { UnderTheHood } from "../components/UnderTheHood";
import { Panel, ErrorBanner } from "../components";

const FLOW = [
  { label: "Enterprise Data", detail: "documents, tickets, runbooks, policies" },
  { label: "NebulaDB", detail: "one engine, three protocols" },
  { label: "SQL + JSON + Vector", detail: "shared corpus, one index" },
  { label: "AI Retrieval", detail: "hybrid BM25 + HNSW, reranked" },
  { label: "RAG", detail: "grounded answers with citations" },
  { label: "AI Agents", detail: "tool calls over MCP" },
  { label: "Enterprise Applications", detail: "support, SRE, analytics" },
];

export function ExecutiveTab({ onNavigate }: { onNavigate?: (t: string) => void }) {
  const [health, setHealth] = useState<Health | null>(null);
  const [stats, setStats] = useState<StatsSnapshot | null>(null);
  const [buckets, setBuckets] = useState<BucketStats[] | null>(null);
  const [dur, setDur] = useState<DurabilityInfo | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [cluster, setCluster] = useState<SimCluster>(() => buildCluster());
  const [tick, setTick] = useState(0);

  // Live poll. Each of these is a real endpoint; failures surface
  // rather than silently falling back to invented numbers.
  useEffect(() => {
    let dead = false;
    const load = async () => {
      try {
        const [h, s, b] = await Promise.all([api.health(), api.stats(), api.buckets()]);
        if (dead) return;
        setHealth(h);
        setStats(s);
        setBuckets(b);
        setErr(null);
      } catch (e) {
        if (!dead) setErr((e as Error).message);
      }
      try {
        const d = await api.durability();
        if (!dead) setDur(d);
      } catch {
        /* durability is optional — absent on ephemeral nodes */
      }
    };
    load();
    const id = setInterval(load, 5000);
    return () => {
      dead = true;
      clearInterval(id);
    };
  }, []);

  // Simulator clock, deliberately faster than the live poll so the
  // cluster panel feels alive between API refreshes.
  useEffect(() => {
    const id = setInterval(() => {
      setTick((t) => t + 1);
      setCluster((c) => tickCluster(c, tick));
    }, 1500);
    return () => clearInterval(id);
  }, [tick]);

  const sim = simExecSlice(cluster, tick);

  const docs: Sourced<number> = live(health?.docs ?? 0, "GET /healthz");
  const vectors: Sourced<number> = derived(
    (health?.docs ?? 0) * 1,
    "one vector per indexed chunk (/healthz docs)"
  );
  const embeddings: Sourced<number> = live(
    (stats?.embed_cache_hits ?? 0) + (stats?.embed_cache_misses ?? 0),
    "GET /api/v1/admin/stats"
  );
  const ragQueries: Sourced<number> = live(stats?.rag_requests ?? 0, "GET /api/v1/admin/stats");
  const cacheRatio =
    stats && stats.embed_cache_hits + stats.embed_cache_misses > 0
      ? stats.embed_cache_hits / (stats.embed_cache_hits + stats.embed_cache_misses)
      : 0;

  return (
    <div className="space-y-6">
      <ErrorBanner err={err} />

      {/* Hero ------------------------------------------------------- */}
      <section className="card !p-6 bg-gradient-to-br from-white to-gray-50 dark:from-carbon-900 dark:to-carbon-950">
        <div className="eyebrow">Executive demo</div>
        <h2 className="mt-2 font-display text-2xl font-semibold tracking-tight text-gray-900 dark:text-ink sm:text-3xl">
          One Database for Enterprise AI
        </h2>
        <p className="mt-2 max-w-2xl text-sm leading-relaxed text-gray-600 dark:text-muted">
          Store, search, understand and automate enterprise data with SQL, JSON,
          vectors, RAG and AI agents — on a single highly available cluster.
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          {[
            ["rag", "Explore RAG"],
            ["agents", "Try an AI agent"],
            ["sql", "Explore SQL"],
            ["cluster", "View cluster"],
            ["mcp", "Explore MCP"],
          ].map(([id, label]) => (
            <button key={id} onClick={() => onNavigate?.(id)} className="btn-secondary !text-xs">
              {label}
            </button>
          ))}
        </div>
      </section>

      {/* Live statistics -------------------------------------------- */}
      <div>
        <div className="mb-2 flex items-center gap-2">
          <h3 className="font-display text-sm font-semibold text-gray-900 dark:text-ink">
            Live statistics
          </h3>
          <span className="text-[10px] text-gray-400 dark:text-faint">
            refreshed every 5s · each tile shows its own origin
          </span>
        </div>
        <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-3 lg:grid-cols-4">
          <SourcedStat label="Documents" sourced={docs} format={fmtInt} />
          <SourcedStat label="Vectors" sourced={vectors} format={fmtInt} />
          <SourcedStat label="Embeddings computed" sourced={embeddings} format={fmtInt} />
          <SourcedStat label="RAG queries" sourced={ragQueries} format={fmtInt} />
          <SourcedStat label="Queries / sec" sourced={sim.qps} format={fmtInt} />
          <SourcedStat label="Vector queries / sec" sourced={sim.vectorQps} format={fmtInt} />
          <SourcedStat label="P95 latency" sourced={sim.p95Ms} format={(v) => `${v} ms`} />
          <SourcedStat label="Active agents" sourced={sim.activeAgents} format={fmtInt} />
          <SourcedStat label="Replication lag" sourced={sim.replicationLagMs} format={(v) => `${v} ms`} />
          <SourcedStat label="Cluster nodes" sourced={sim.clusterNodes} format={fmtInt} />
          <SourcedStat label="Regions" sourced={sim.regions} format={fmtInt} />
          <SourcedStat label="Storage" sourced={sim.storageGb} format={(v) => `${v} GB`} />
        </div>
      </div>

      {/* Story flow -------------------------------------------------- */}
      <Panel
        title="How the pieces fit"
        subtitle="The same corpus serves structured queries, retrieval and agents"
      >
        <ol className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
          {FLOW.map((f, i) => (
            <li
              key={f.label}
              className="rounded-md border border-gray-200 bg-white p-3 dark:border-edge dark:bg-carbon-900"
            >
              <div className="flex items-center gap-2">
                <span className="grid h-5 w-5 place-items-center rounded-full border border-accent/40 bg-accent/10 font-mono text-[10px] text-accent">
                  {i + 1}
                </span>
                <span className="text-xs font-semibold text-gray-900 dark:text-ink">{f.label}</span>
              </div>
              <p className="mt-1.5 text-[11px] leading-relaxed text-gray-500 dark:text-muted">
                {f.detail}
              </p>
            </li>
          ))}
        </ol>
      </Panel>

      {/* Corpus + durability ---------------------------------------- */}
      <div className="grid gap-4 lg:grid-cols-2">
        <Panel title="Corpus" subtitle="Buckets and live document counts" action={<OriginBadge origin="live" from="GET /api/v1/admin/buckets" />}>
          {!buckets ? (
            <p className="text-xs text-gray-500 dark:text-muted">loading…</p>
          ) : buckets.length === 0 ? (
            <p className="text-xs text-gray-500 dark:text-muted">
              No buckets yet — load the enterprise dataset from the Ingestion page.
            </p>
          ) : (
            <ul className="space-y-1.5">
              {buckets.slice(0, 8).map((b) => (
                <li key={b.bucket} className="flex items-center gap-3 text-xs">
                  <span className="font-mono text-gray-800 dark:text-ink">{b.bucket}</span>
                  <span className="ml-auto font-mono tabular-nums text-gray-500 dark:text-muted">
                    {fmtInt(b.docs)} chunks
                  </span>
                </li>
              ))}
            </ul>
          )}
        </Panel>

        <Panel title="Durability" subtitle="WAL and persistence" action={<OriginBadge origin="live" from="GET /api/v1/admin/durability" />}>
          {!dur ? (
            <p className="text-xs text-gray-500 dark:text-muted">loading…</p>
          ) : (
            <dl className="grid grid-cols-2 gap-2 text-xs">
              <Row k="Persistent" v={dur.persistent ? "yes" : "no (ephemeral)"} />
              <Row k="Data dir" v={dur.data_dir ?? "—"} />
              <Row k="WAL segments" v={dur.wal ? String(dur.wal.segment_count) : "—"} />
              <Row k="WAL bytes" v={dur.wal ? fmtBytes(dur.wal.total_bytes) : "—"} />
              <Row k="Seq range" v={dur.wal ? `${dur.wal.oldest_seq} → ${dur.wal.newest_seq}` : "—"} />
              <Row k="Embed cache hit" v={`${Math.round(cacheRatio * 100)}%`} />
            </dl>
          )}
        </Panel>
      </div>

      <UnderTheHood filter={["/healthz", "/admin/"]} />
    </div>
  );
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <>
      <dt className="text-gray-500 dark:text-muted">{k}</dt>
      <dd className="truncate font-mono text-gray-900 dark:text-ink" title={v}>
        {v}
      </dd>
    </>
  );
}

const fmtInt = (v: number | string) => Number(v).toLocaleString();

function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 ** 2) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 ** 3) return `${(n / 1024 ** 2).toFixed(1)} MB`;
  return `${(n / 1024 ** 3).toFixed(2)} GB`;
}

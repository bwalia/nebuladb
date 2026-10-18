/**
 * Engineering blog — short posts plus long-form tech notes from running
 * NebulaDB in production. Posts are static content; deep links use
 * `#/blog/<slug>` so a post can be shared or opened from the landing page.
 *
 * Every number in these posts comes from the prod run it describes
 * (loader logs, /admin/reliability, kubectl). Nothing is simulated.
 */
import { useEffect, useState, type ReactNode } from "react";

export interface PostMeta {
  slug: string;
  kind: "Blog" | "Tech note";
  title: string;
  date: string;
  readMins: number;
  summary: string;
}

export const POSTS: PostMeta[] = [
  {
    slug: "48gi-without-a-restart",
    kind: "Blog",
    title: "We gave a live NebulaDB leader 16 GB more RAM, without a restart",
    date: "2026-10-13",
    readMins: 3,
    summary:
      "Loading the UK Companies House register pushed our prod leader to 34.1 of 34.4 GB. An in-place pod resize took it to 48Gi with zero restarts, and ingest throughput doubled. Update: two more resizes, to 64Gi and 96Gi, carried it through all 5.17M active companies.",
  },
  {
    slug: "in-place-memory-resize-tech-note",
    kind: "Tech note",
    title: "Tech note: memory, HNSW and an in-place resize on a multi-region NebulaDB cluster",
    date: "2026-10-13",
    readMins: 14,
    summary:
      "The full story: where the memory goes, the memory-aware bulk loader, the near-OOM, why we did not helm upgrade, the exact commands, and what is still open.",
  },
];

const HASH_PREFIX = "#/blog/";

export function blogHref(slug: string) {
  return `${HASH_PREFIX}${slug}`;
}

function slugFromHash(): string | null {
  const h = window.location.hash;
  if (!h.startsWith(HASH_PREFIX)) return null;
  const slug = h.slice(HASH_PREFIX.length);
  return POSTS.some((p) => p.slug === slug) ? slug : null;
}

export function BlogTab() {
  const [slug, setSlug] = useState<string | null>(() => slugFromHash());

  useEffect(() => {
    const onHash = () => setSlug(slugFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const open = (s: string | null) => {
    window.location.hash = s ? blogHref(s) : "#/blog";
    setSlug(s);
    document.querySelector("main")?.scrollTo({ top: 0 });
  };

  const post = POSTS.find((p) => p.slug === slug);
  if (!post) return <PostList onOpen={open} />;

  return (
    <article className="space-y-6">
      <button onClick={() => open(null)} className="btn-secondary !text-xs">
        ← All posts
      </button>
      <PostHeader post={post} />
      {post.slug === "48gi-without-a-restart" ? (
        <BlogPost onOpen={open} />
      ) : (
        <TechNote />
      )}
    </article>
  );
}

function PostList({ onOpen }: { onOpen: (slug: string) => void }) {
  return (
    <div className="grid gap-4 md:grid-cols-2">
      {POSTS.map((p) => (
        <button
          key={p.slug}
          onClick={() => onOpen(p.slug)}
          className="instrument text-left"
        >
          <div className="flex items-center gap-2">
            <KindBadge kind={p.kind} />
            <span className="eyebrow">{p.date} · {p.readMins} min read</span>
          </div>
          <h3 className="mt-3 text-base font-semibold leading-snug text-gray-900 dark:text-ink">
            {p.title}
          </h3>
          <p className="mt-2 text-sm leading-relaxed text-gray-600 dark:text-muted">{p.summary}</p>
          <span className="mt-3 inline-block text-xs font-medium text-accent">Read →</span>
        </button>
      ))}
    </div>
  );
}

function KindBadge({ kind }: { kind: PostMeta["kind"] }) {
  return (
    <span
      className={`rounded-full border px-2 py-0.5 font-mono text-[10px] uppercase tracking-wider ${
        kind === "Blog"
          ? "border-accent/40 bg-accent/10 text-accent"
          : "border-ok/40 bg-ok/10 text-ok"
      }`}
    >
      {kind}
    </span>
  );
}

function PostHeader({ post }: { post: PostMeta }) {
  return (
    <header className="card !p-6 bg-gradient-to-br from-white to-gray-50 dark:from-carbon-900 dark:to-carbon-950">
      <div className="flex items-center gap-2">
        <KindBadge kind={post.kind} />
        <span className="eyebrow">{post.date} · {post.readMins} min read · NebulaDB engineering</span>
      </div>
      <h1 className="mt-3 text-2xl font-semibold tracking-tight text-gray-900 dark:text-ink sm:text-3xl">
        {post.title}
      </h1>
      <p className="mt-3 max-w-3xl text-sm leading-relaxed text-gray-600 dark:text-muted">
        {post.summary}
      </p>
    </header>
  );
}

/* ------------------------------------------------------------------ */
/* Prose primitives                                                    */
/* ------------------------------------------------------------------ */

function Prose({ children }: { children: ReactNode }) {
  return (
    <div className="card !p-6 space-y-4 text-[15px] leading-7 text-gray-700 dark:text-gray-300 max-w-none">
      {children}
    </div>
  );
}

function H2({ id, children }: { id?: string; children: ReactNode }) {
  return (
    <h2 id={id} className="pt-4 text-xl font-semibold tracking-tight text-gray-900 dark:text-ink">
      {children}
    </h2>
  );
}

function H3({ children }: { children: ReactNode }) {
  return <h3 className="pt-2 text-base font-semibold text-gray-900 dark:text-ink">{children}</h3>;
}

function C({ children }: { children: ReactNode }) {
  return (
    <code className="rounded bg-gray-100 px-1 py-0.5 font-mono text-[13px] text-gray-900 dark:bg-carbon-800 dark:text-ink">
      {children}
    </code>
  );
}

function Code({ title, children }: { title?: string; children: string }) {
  return (
    <figure className="overflow-hidden rounded-lg border border-gray-200 dark:border-edge">
      {title && (
        <figcaption className="border-b border-gray-200 bg-gray-50 px-3 py-1.5 font-mono text-[11px] text-gray-500 dark:border-edge dark:bg-carbon-800 dark:text-faint">
          {title}
        </figcaption>
      )}
      <pre className="overflow-x-auto bg-gray-50 p-3 font-mono text-[12.5px] leading-relaxed text-gray-800 dark:bg-carbon-950 dark:text-ink">
        {children}
      </pre>
    </figure>
  );
}

function Callout({ tone = "info", title, children }: { tone?: "info" | "warn" | "ok"; title: string; children: ReactNode }) {
  const cls =
    tone === "warn"
      ? "border-warn/40 bg-warn/5"
      : tone === "ok"
        ? "border-ok/40 bg-ok/5"
        : "border-accent/40 bg-accent/5";
  return (
    <aside className={`rounded-lg border p-4 ${cls}`}>
      <div className="text-sm font-semibold text-gray-900 dark:text-ink">{title}</div>
      <div className="mt-1 text-sm leading-relaxed text-gray-700 dark:text-gray-300">{children}</div>
    </aside>
  );
}

function Figure({ n, caption, children }: { n: number; caption: string; children: ReactNode }) {
  return (
    <figure className="my-2 rounded-lg border border-gray-200 bg-white p-4 dark:border-edge dark:bg-carbon-950">
      {children}
      <figcaption className="mt-3 text-xs leading-relaxed text-gray-500 dark:text-muted">
        <span className="font-semibold text-gray-700 dark:text-ink">Figure {n}.</span> {caption}
      </figcaption>
    </figure>
  );
}

function Table({ head, rows }: { head: string[]; rows: (string | ReactNode)[][] }) {
  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-edge">
      <table className="w-full text-left text-sm">
        <thead className="bg-gray-50 text-xs uppercase tracking-wide text-gray-500 dark:bg-carbon-800 dark:text-faint">
          <tr>{head.map((h) => <th key={h} className="px-3 py-2 font-medium">{h}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i} className="border-t border-gray-100 dark:border-edge/60">
              {r.map((c, j) => (
                <td key={j} className={`px-3 py-2 ${j === 0 ? "text-gray-600 dark:text-muted" : "font-mono text-gray-900 dark:text-ink"}`}>
                  {c}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Stat({ label, before, after }: { label: string; before: string; after: string }) {
  return (
    <div className="rounded-lg border border-gray-200 p-3 dark:border-edge">
      <div className="eyebrow">{label}</div>
      <div className="mt-1 flex items-baseline gap-2 font-mono">
        <span className="text-sm text-gray-400 line-through dark:text-faint">{before}</span>
        <span className="text-lg font-semibold text-gray-900 dark:text-ink">{after}</span>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Figures (inline SVG — theme-aware via currentColor + Tailwind)       */
/* ------------------------------------------------------------------ */

/** Figure: prod topology — leader, within-region follower, cross-region peer. */
function TopologyFigure() {
  const box = "fill-white stroke-gray-300 dark:fill-carbon-900 dark:stroke-edge";
  const text = "fill-gray-800 dark:fill-ink";
  const sub = "fill-gray-500 dark:fill-muted";
  return (
    <svg viewBox="0 0 760 330" className="h-auto w-full" role="img" aria-label="NebulaDB prod topology">
      <defs>
        <marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" className="fill-gray-400 dark:fill-faint" />
        </marker>
      </defs>
      {/* region frames */}
      <rect x="10" y="10" width="470" height="310" rx="14" className="fill-transparent stroke-gray-300 dark:stroke-edge" strokeDasharray="5 5" />
      <text x="26" y="34" className={`${sub} font-mono`} fontSize="11">REGION us-east-1 · k3s namespace prod</text>
      <rect x="500" y="10" width="250" height="310" rx="14" className="fill-transparent stroke-gray-300 dark:stroke-edge" strokeDasharray="5 5" />
      <text x="516" y="34" className={`${sub} font-mono`} fontSize="11">REGION us-west-2</text>

      {/* clients */}
      <rect x="30" y="52" width="190" height="52" rx="10" className={box} />
      <text x="125" y="75" textAnchor="middle" className={text} fontSize="13" fontWeight="600">showcase + nginx</text>
      <text x="125" y="93" textAnchor="middle" className={sub} fontSize="11">CH writes pinned to leader</text>

      <rect x="260" y="52" width="200" height="52" rx="10" className={box} />
      <text x="360" y="75" textAnchor="middle" className={text} fontSize="13" fontWeight="600">bulk loader</text>
      <text x="360" y="93" textAnchor="middle" className={sub} fontSize="11">500 docs / POST, memory-aware</text>

      {/* leader */}
      <rect x="110" y="150" width="270" height="78" rx="12" className="fill-accent/10 stroke-accent" strokeWidth="1.5" />
      <text x="245" y="176" textAnchor="middle" className={text} fontSize="14" fontWeight="700">nebuladb (leader)</text>
      <text x="245" y="196" textAnchor="middle" className={sub} fontSize="11">node debworker00 · HNSW dim 384</text>
      <text x="245" y="214" textAnchor="middle" className="fill-accent font-mono" fontSize="11">limit 32Gi → 48Gi (in place)</text>

      {/* follower */}
      <rect x="110" y="258" width="270" height="50" rx="12" className={box} />
      <text x="245" y="280" textAnchor="middle" className={text} fontSize="13" fontWeight="600">nebuladb-follower</text>
      <text x="245" y="297" textAnchor="middle" className={sub} fontSize="11">within-region mirror · debian002 · 32Gi</text>

      {/* region b */}
      <rect x="525" y="150" width="200" height="78" rx="12" className={box} />
      <text x="625" y="176" textAnchor="middle" className={text} fontSize="13" fontWeight="600">nebuladb-region-b</text>
      <text x="625" y="196" textAnchor="middle" className={sub} fontSize="11">peer leader · debian004</text>
      <text x="625" y="214" textAnchor="middle" className={`${sub} font-mono`} fontSize="11">gRPC :50051</text>

      {/* arrows */}
      <path d="M125 104 L200 148" className="stroke-gray-400 dark:stroke-faint" fill="none" markerEnd="url(#arr)" />
      <path d="M360 104 L300 148" className="stroke-gray-400 dark:stroke-faint" fill="none" markerEnd="url(#arr)" />
      <path d="M245 228 L245 256" className="stroke-gray-400 dark:stroke-faint" fill="none" markerEnd="url(#arr)" />
      <text x="252" y="246" className={sub} fontSize="10">WAL tail</text>
      <path d="M380 189 L523 189" className="stroke-gray-400 dark:stroke-faint" fill="none" markerEnd="url(#arr)" />
      <text x="452" y="182" textAnchor="middle" className={sub} fontSize="10">cross-region</text>
    </svg>
  );
}

/**
 * Leader memory while loading, bucketed per 20k docs: trough (min) and
 * peak (max) reading across the batches in that bucket. Source: loader
 * logs (`mem=` is /admin/reliability memory_used_bytes before each batch).
 */
const MEM_SERIES: [number, number, number][] = [
  [0, 15.3, 25.4], [20, 16.0, 24.4], [40, 15.9, 24.5], [60, 15.9, 23.1], [80, 16.6, 23.4],
  [100, 16.3, 24.2], [120, 16.6, 23.7], [140, 16.7, 24.7], [160, 16.7, 24.3], [180, 17.2, 25.0],
  [200, 17.2, 24.0], [220, 17.5, 24.0], [240, 17.4, 24.3], [260, 17.7, 24.9], [280, 16.3, 24.2],
  [300, 17.1, 24.8], [320, 15.5, 24.1], [340, 17.0, 24.4], [360, 16.9, 24.9], [380, 16.0, 24.2],
  [400, 17.3, 23.4], [420, 17.1, 24.5], [440, 17.2, 24.0], [460, 17.1, 23.8], [480, 19.0, 24.1],
  [500, 17.3, 24.4], [520, 19.5, 25.0], [540, 17.7, 25.0], [560, 17.7, 24.6], [580, 18.5, 23.9],
  [600, 18.7, 24.4], [620, 18.0, 24.7], [640, 17.7, 24.8], [660, 18.1, 24.5], [680, 18.8, 24.8],
  [700, 19.2, 26.8], [720, 19.4, 26.9], [740, 20.2, 26.4], [760, 19.8, 26.2], [780, 20.0, 26.7],
  [800, 19.3, 26.7], [820, 21.3, 26.8], [840, 20.6, 26.8], [860, 20.6, 26.3], [880, 21.2, 27.0],
  [900, 20.0, 26.7], [920, 19.5, 26.7], [940, 19.0, 26.0], [960, 18.2, 26.9], [980, 18.4, 26.9],
  [1000, 20.9, 29.5],
];

function MemoryChart() {
  const W = 760, H = 300, L = 48, R = 16, T = 16, B = 40;
  const xMax = 1000, yMin = 10, yMax = 36;
  const x = (k: number) => L + (k / xMax) * (W - L - R);
  const y = (g: number) => T + ((yMax - g) / (yMax - yMin)) * (H - T - B);
  const band =
    MEM_SERIES.map(([k, , hi]) => `${x(k)},${y(hi)}`).join(" ") +
    " " +
    [...MEM_SERIES].reverse().map(([k, lo]) => `${x(k)},${y(lo)}`).join(" ");
  const trough = MEM_SERIES.map(([k, lo]) => `${x(k)},${y(lo)}`).join(" ");
  const grid = "stroke-gray-200 dark:stroke-edge";
  const label = "fill-gray-500 dark:fill-muted";
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" role="img" aria-label="Leader memory during bulk load">
      {[10, 15, 20, 25, 30, 35].map((g) => (
        <g key={g}>
          <line x1={L} x2={W - R} y1={y(g)} y2={y(g)} className={grid} />
          <text x={L - 8} y={y(g) + 4} textAnchor="end" fontSize="11" className={label}>{g}</text>
        </g>
      ))}
      {[0, 200, 400, 600, 800, 1000].map((k) => (
        <text key={k} x={x(k)} y={H - B + 18} textAnchor="middle" fontSize="11" className={label}>
          {k === 0 ? "0" : `${k}k`}
        </text>
      ))}
      <text x={(W + L) / 2} y={H - 6} textAnchor="middle" fontSize="11" className={label}>Companies House docs loaded</text>
      <text x={14} y={T + 4} fontSize="11" className={label}>GB</text>

      {/* old hard limit */}
      <line x1={L} x2={W - R} y1={y(34.4)} y2={y(34.4)} className="stroke-bad" strokeDasharray="6 4" />
      <text x={W - R - 4} y={y(34.4) - 6} textAnchor="end" fontSize="11" className="fill-bad">old limit 32Gi = 34.4 GB</text>
      {/* pause thresholds */}
      <line x1={x(0)} x2={x(700)} y1={y(25)} y2={y(25)} className="stroke-warn" strokeDasharray="3 4" />
      <line x1={x(700)} x2={x(1000)} y1={y(27)} y2={y(27)} className="stroke-warn" strokeDasharray="3 4" />
      <text x={x(4)} y={y(25) - 6} fontSize="11" className="fill-warn">loader pauses at 25 GB → 27 GB</text>

      <polygon points={band} className="fill-accent/20" />
      <polyline points={trough} fill="none" className="stroke-accent" strokeWidth="2" />

      {/* idle spike annotation */}
      <circle cx={x(1000)} cy={y(34.1)} r="4" className="fill-bad" />
      <text x={x(1000) - 8} y={y(34.1) + 16} textAnchor="end" fontSize="11" className="fill-bad">34.1 GB idle, no writes</text>
    </svg>
  );
}

/** Figure: decision — helm upgrade (Recreate) vs in-place resize. */
function ResizeFlowFigure() {
  const box = "fill-white stroke-gray-300 dark:fill-carbon-900 dark:stroke-edge";
  const t = "fill-gray-800 dark:fill-ink";
  const s = "fill-gray-500 dark:fill-muted";
  const Step = ({ x0, y0, w, title, sub, cls = box }: { x0: number; y0: number; w: number; title: string; sub: string; cls?: string }) => (
    <g>
      <rect x={x0} y={y0} width={w} height="52" rx="10" className={cls} />
      <text x={x0 + w / 2} y={y0 + 22} textAnchor="middle" className={t} fontSize="12.5" fontWeight="600">{title}</text>
      <text x={x0 + w / 2} y={y0 + 40} textAnchor="middle" className={s} fontSize="11">{sub}</text>
    </g>
  );
  const arrow = "stroke-gray-400 dark:stroke-faint";
  return (
    <svg viewBox="0 0 760 250" className="h-auto w-full" role="img" aria-label="helm upgrade versus in-place resize">
      <defs>
        <marker id="arr2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">
          <path d="M0,0 L10,5 L0,10 z" className="fill-gray-400 dark:fill-faint" />
        </marker>
      </defs>
      <text x="10" y="22" className="fill-bad font-mono" fontSize="11">A · helm upgrade (strategy: Recreate, RWO local-path PVC)</text>
      <Step x0={10} y0={34} w={160} title="new pod template" sub="limits.memory 48Gi" />
      <Step x0={200} y0={34} w={160} title="old pod killed" sub="index dropped from RAM" cls="fill-bad/10 stroke-bad" />
      <Step x0={390} y0={34} w={170} title="cold recovery" sub="snapshot replay ≈ 45 min" cls="fill-bad/10 stroke-bad" />
      <Step x0={590} y0={34} w={160} title="serving again" sub="504s meanwhile" />
      {[170, 360, 560].map((x0) => <path key={x0} d={`M${x0} 60 L${x0 + 28} 60`} className={arrow} markerEnd="url(#arr2)" />)}

      <text x="10" y="142" className="fill-ok font-mono" fontSize="11">B · kubectl patch pod --subresource resize (K8s 1.36, in-place)</text>
      <Step x0={10} y0={154} w={160} title="patch resize" sub="limits.memory 48Gi" />
      <Step x0={200} y0={154} w={160} title="kubelet → cgroup" sub="memory.max raised" cls="fill-ok/10 stroke-ok" />
      <Step x0={390} y0={154} w={170} title="same process" sub="restartCount 0" cls="fill-ok/10 stroke-ok" />
      <Step x0={590} y0={154} w={160} title="server sees 51.5 GB" sub="/admin/reliability" />
      {[170, 360, 560].map((x0) => <path key={x0} d={`M${x0} 180 L${x0 + 28} 180`} className={arrow} markerEnd="url(#arr2)" />)}
    </svg>
  );
}

/** Figure: ingest throughput and pauses before vs after the resize. */
function ThroughputFigure() {
  const rows = [
    { label: "Before (32Gi limit)", rate: 10, color: "fill-gray-400 dark:fill-faint", note: "9–11 docs/s · 88 pauses · median wait 7 min" },
    { label: "After (48Gi, in place)", rate: 21, color: "fill-accent", note: "21 docs/s · 0 pauses" },
  ];
  const max = 25, L = 180, W = 760;
  return (
    <svg viewBox={`0 0 ${W} 130`} className="h-auto w-full" role="img" aria-label="Ingest throughput before and after">
      {rows.map((r, i) => {
        const yy = 18 + i * 54;
        const w = (r.rate / max) * (W - L - 200);
        return (
          <g key={r.label}>
            <text x={L - 10} y={yy + 18} textAnchor="end" fontSize="12" className="fill-gray-700 dark:fill-ink">{r.label}</text>
            <rect x={L} y={yy} width={w} height="28" rx="6" className={r.color} />
            <text x={L + w + 8} y={yy + 19} fontSize="12" fontWeight="600" className="fill-gray-900 dark:fill-ink">{r.rate} docs/s</text>
            <text x={L + w + 80} y={yy + 19} fontSize="11" className="fill-gray-500 dark:fill-muted">{r.note}</text>
          </g>
        );
      })}
    </svg>
  );
}

/* ------------------------------------------------------------------ */
/* Post 1: blog item                                                    */
/* ------------------------------------------------------------------ */

function BlogPost({ onOpen }: { onOpen: (slug: string) => void }) {
  return (
    <>
      <div className="grid gap-2.5 sm:grid-cols-4">
        <Stat label="Leader memory limit" before="32Gi" after="48Gi" />
        <Stat label="Pod restarts" before="—" after="0" />
        <Stat label="Ingest rate" before="~10/s" after="21/s" />
        <Stat label="Loader pauses" before="88" after="0" />
      </div>
      <Prose>
        <p>
          This week we loaded the UK Companies House register into the NebulaDB showcase cluster. The
          October 2026 snapshot has 5.7 million companies, 5.17 million of them active, and every one
          becomes a searchable document with a 384-dimension vector in the HNSW index.
        </p>
        <p>
          By the first million documents the production leader was in trouble. With no writes in flight,
          it reached <strong>34.1 GB of its 34.4 GB (32Gi) memory limit</strong>, and the server's own
          reliability monitor flagged memory as <C>critical</C>. One more spike and Kubernetes would have
          OOM-killed it. The last time that happened, the showcase served 504s for about 45 minutes while
          the leader replayed its snapshot.
        </p>
        <H3>The obvious fix would have caused the outage</H3>
        <p>
          The usual move is to edit <C>values-prod.yaml</C> and <C>helm upgrade</C>. But the leader
          deployment uses the <C>Recreate</C> strategy on a ReadWriteOnce volume, so any template change
          kills the pod first. A routine redeploy would have caused the same 45 minutes of downtime we
          were trying to avoid.
        </p>
        <H3>What we did: resize the running pod</H3>
        <p>
          Our k3s cluster runs Kubernetes 1.36, where in-place pod resize is generally available. A single{" "}
          <C>kubectl patch --subresource resize</C> raised the leader's memory limit from 32Gi to 48Gi.
          The kubelet updated the cgroup underneath the running process: no restart and no failover. The
          leader, its within-region follower and the us-west-2 peer leader all stayed up. Seconds later,
          NebulaDB's <C>/admin/reliability</C> endpoint reported the new 51.5 GB ceiling and dropped back
          to <C>normal</C> mode.
        </p>
        <H3>What it bought us</H3>
        <p>
          Before the resize, the bulk loader had to pause 88 times to let memory recover, waiting a median
          of 7 minutes each time, and averaged about 10 docs/s. Since the resize it has run at
          <strong> 21 docs/s with zero pauses</strong>. Same hardware, same code, twice the ingest
          throughput, and zero downtime.
        </p>
        <p>
          We then committed the new limit to <C>values-prod.yaml</C> so the next deploy keeps it instead of
          quietly reverting it.
        </p>
        <H3>Update, 17 October: the whole register is in</H3>
        <p>
          48Gi was not the end of it. At about 2.75 million documents, the leader's periodic snapshot
          (every ~23 minutes, ~8 minutes long) was pushing an otherwise idle leader to 43–48 GB of its
          51.5 GB ceiling: snapshot peaks grow with the corpus. We repeated the same in-place resize twice,
          to <strong>64Gi</strong> and then <strong>96Gi</strong> (103.1 GB), and committed each value to
          the chart.
        </p>
        <p>
          With that headroom the loader finished the remaining 2.41 million companies in one 29.6-hour run
          at a steady 22–23 docs/s. The prod bucket now holds all <strong>5,166,057 active companies</strong>.
          The leader settles at 54–57 GB, and its restart count is still 0: the same process that started
          on 5 October served the showcase throughout all three resizes.
        </p>
        <Callout tone="info" title="Want the full story?">
          The long-form tech note covers where HNSW memory goes, the memory-aware loader, why we did not{" "}
          <C>helm upgrade</C>, the exact commands and checks, and the work that is still open.{" "}
          <button className="font-medium text-accent underline" onClick={() => onOpen("in-place-memory-resize-tech-note")}>
            Read the tech note →
          </button>
        </Callout>
      </Prose>
    </>
  );
}

/* ------------------------------------------------------------------ */
/* Post 2: long tech note                                               */
/* ------------------------------------------------------------------ */

const TOC = [
  ["tldr", "TL;DR"],
  ["workload", "1. The workload"],
  ["topology", "2. The topology"],
  ["memory", "3. Where the memory goes"],
  ["loader", "4. A memory-aware bulk loader"],
  ["incident", "5. The near-OOM"],
  ["options", "6. Options, and why not helm upgrade"],
  ["resize", "7. The in-place resize, step by step"],
  ["persist", "8. Making it stick: Helm values and drift"],
  ["results", "9. Results"],
  ["finish", "10. Update: 64Gi, 96Gi and the full register"],
  ["open", "11. What is still open"],
  ["checklist", "12. Runbook checklist"],
] as const;

function TechNote() {
  return (
    <div className="grid gap-6 lg:grid-cols-[1fr_220px]">
      <Prose>
        <H2 id="tldr">TL;DR</H2>
        <ul className="list-disc space-y-1 pl-5">
          <li>Bulk-loading about 1M UK Companies House records drove the prod leader to 34.1 / 34.4 GB, idle, with memory marked <C>critical</C>.</li>
          <li>A <C>helm upgrade</C> would have recreated the pod: about 45 minutes of cold snapshot replay and 504s.</li>
          <li>Instead we used Kubernetes 1.36 <strong>in-place pod resize</strong> to take the leader from 32Gi to 48Gi: restart count 0, no failover, multi-region topology untouched.</li>
          <li>Ingest throughput went from about 10 to 21 docs/s, and loader pauses went from 88 to 0.</li>
          <li>The limit is committed to <C>values-prod.yaml</C> so the next rollout keeps it. Reclaiming deleted-vector memory is still the real long-term fix.</li>
        </ul>

        <H2 id="workload">1. The workload</H2>
        <p>
          Companies House publishes the whole register as one CSV, <C>BasicCompanyDataAsOneFile</C>. The
          2026-10-01 file is 2.8 GB, with 5,704,712 rows and 55 columns. We loaded the 5,166,057
          companies with <C>CompanyStatus = Active</C> into the <C>companies_house</C> bucket. That bucket
          also holds per-company docs written by the live Companies House API integration (
          <C>{"{number}-profile"}</C>, <C>-officers</C>, <C>-psc</C>, <C>-filings</C>).
        </p>
        <p>Each CSV row becomes one document:</p>
        <Code title="one register document">{`{
  "id": "08209948-register",
  "text": "! LTD (company number 08209948) is a Private Limited Company registered
           with UK Companies House. Status: Active. Incorporated: 11/09/2012.
           Registered office: 9 PRINCES SQUARE, HARROGATE, ENGLAND, HG1 1ND.
           Nature of business (SIC): 99999 - Dormant Company. ...",
  "metadata": {
    "source": "ch_bulk_2026-10-01", "kind": "register_basic",
    "company_number": "08209948", "company_name": "! LTD",
    "status": "Active", "category": "Private Limited Company",
    "incorporation_date": "11/09/2012", "post_town": "HARROGATE",
    "postcode": "HG1 1ND", "sic_codes": ["99999 - Dormant Company"]
  }
}`}</Code>
        <p>
          The ID is deterministic (<C>{"{number}-register"}</C>), so re-sending a batch overwrites rather
          than duplicates. That property is what makes it safe to stop and resume the loader from any row.
          Documents go in through <C>POST /api/v1/bucket/companies_house/docs/bulk</C> in batches of 500
          (the server caps a batch at 1,000).
        </p>

        <H2 id="topology">2. The topology</H2>
        <Figure n={1} caption="Prod runs a leader on debworker00, a within-region follower that tails the leader's WAL, and a cross-region peer leader in us-west-2. The showcase nginx pins Companies House routes to the leader, and the bulk loader writes there too. Only the leader's limit changed.">
          <TopologyFigure />
        </Figure>
        <p>
          All three NebulaDB pods are ordinary Deployments on our k3s cluster, managed by one Helm release
          (<C>nebuladb</C>, namespace <C>prod</C>). The resize touched only the leader.
        </p>

        <H2 id="memory">3. Where the memory goes</H2>
        <p>NebulaDB keeps its HNSW vector index fully in RAM. Per document, the resident cost is roughly:</p>
        <ul className="list-disc space-y-1 pl-5">
          <li><strong>The vector</strong>: 384 dimensions × 4 bytes = 1.5 KB.</li>
          <li><strong>Graph links</strong>: neighbour lists on each HNSW layer.</li>
          <li><strong>Text, metadata and the BM25 postings</strong> that hybrid search needs.</li>
          <li>
            <strong>Tombstones</strong>: deletes are soft. A deleted node keeps its arena slot, so resident
            memory only ever grows. That is also why "empty the bucket" does not give memory back; only a
            restart does.
          </li>
        </ul>
        <p>
          On top of that steady state there are transient peaks: the allocator holds freed pages for a
          while, and a snapshot adds about 5 GB at its peak. In the chart below, the line traces the
          trough (the steady-state footprint); the shaded band shows how far each 20k-doc window spiked
          above it.
        </p>
        <Figure n={2} caption="Leader memory across the first 1M documents, bucketed per 20k docs. Line: lowest reading in the bucket; band: lowest to highest. The trough rose from about 15 GB to about 21 GB (roughly 6 KB of steady-state memory per document), while write spikes reached the loader's pause threshold over and over. At 1M docs the leader hit 34.1 GB with nothing writing. Source: loader logs, /admin/reliability memory_used_bytes.">
          <MemoryChart />
        </Figure>

        <H2 id="loader">4. A memory-aware bulk loader</H2>
        <p>
          An early probe showed that batch-sized spikes would trip the hard limit long before the steady
          state did. So the loader reads the leader's memory before every batch and backs off: it pauses
          above a high-water mark, waits for memory to fall below a lower resume mark, and stops entirely
          if memory does not recover within a time budget. An unreadable gauge counts as "high", so it
          fails safe.
        </p>
        <Code title="load_ch.py: the guard that runs before every POST">{`mem = leader_mem_gb()        # GET /api/v1/admin/reliability → memory_used_bytes
if mem > args.max_mem_gb:    # pause threshold (25 → 27 → 40 GB over the run)
    waited = 0
    while mem > args.resume_mem_gb:
        if waited >= args.max_wait_s:
            raise SystemExit(f"ABORT ... resume_skip={row}")   # prints where to restart
        time.sleep(60); waited += 60
        mem = leader_mem_gb()
post(batch)                  # idempotent: ids are {company_number}-register`}</Code>
        <p>
          It worked. Across the first million documents the loader paused 88 times and never let the
          leader cross its limit. But the cost grew: the median pause was 7 minutes and the longest was
          41, which pulled throughput down to about 6–10 docs/s. Pausing was hiding the real problem: the
          leader simply did not have enough room.
        </p>

        <H2 id="incident">5. The near-OOM</H2>
        <p>
          At about 1,003,500 documents the loader hit its 60-minute budget and stopped. With no writes at
          all, the leader kept climbing anyway:
        </p>
        <Code title="GET /api/v1/admin/reliability (abridged)">{`{"mode":"memory_pressure",
 "levels":{"memory":"critical","disk":"ok","cpu":"ok"},
 "sample":{"memory_used_bytes":34100000000,"memory_limit_bytes":34359738368}}`}</Code>
        <p>
          NebulaDB's reliability monitor moves between <C>normal</C> and <C>memory_pressure</C> (and
          <C> cpu_pressure</C>) based on cgroup watermarks, and sheds load with a <C>503 service
          temporarily unavailable</C> while under pressure. That shedding protects reads, but it cannot
          shrink an index that is already resident. We had about 300 MB of headroom.
        </p>

        <H2 id="options">6. Options, and why not helm upgrade</H2>
        <Table
          head={["Option", "Downtime", "Effect"]}
          rows={[
            ["Delete / empty the bucket", "none", "No relief: deletes are tombstones, memory stays resident"],
            ["Trigger a snapshot to compact", "none", "Makes it worse: about +5 GB at peak, likely the OOM itself"],
            ["Restart the leader", "≈ 45 min of 504s", "Frees memory now, but returns to the same footprint after reload"],
            ["helm upgrade with a bigger limit", "≈ 45 min of 504s", "Strategy is Recreate on an RWO local-path PVC, so the pod is killed first"],
            ["In-place pod resize to 48Gi", "none", "Raises the cgroup limit under the running process"],
          ]}
        />
        <Figure n={3} caption="Top: a Helm rollout recreates the pod and pays for a full cold recovery. Bottom: an in-place resize changes only the cgroup, so the process, its in-memory index and its connections all survive.">
          <ResizeFlowFigure />
        </Figure>
        <p>
          The node had room: <C>debworker00</C> was using about 42 GB of its RAM, about 32% of the node.
          The follower and the region-b peer hold far smaller working sets, so they kept their 32Gi limits.
        </p>

        <H2 id="resize">7. The in-place resize, step by step</H2>
        <p>
          Kubernetes 1.36 (we run <C>v1.36.2+k3s1</C>) supports changing a running container's resources
          through the pod's <C>resize</C> subresource. Raising a memory limit needs no restart. The patch
          must target only the field being changed; a merge patch on the containers list fails with{" "}
          <C>resource requests cannot be removed</C>, so we used a JSON patch:
        </p>
        <Code title="1 · resize the running leader">{`# against the prod cluster context
POD=$(kubectl -n prod get pod \\
       -l app.kubernetes.io/name=nebuladb,app.kubernetes.io/component=server \\
       -o jsonpath='{.items[0].metadata.name}')   # leader only

kubectl -n prod patch pod "$POD" --subresource resize --type json -p \\
  '[{"op":"replace","path":"/spec/containers/0/resources/limits/memory","value":"48Gi"}]'`}</Code>
        <Code title="2 · verify the kubelet applied it, and nothing restarted">{`kubectl -n prod get pod "$POD" -o jsonpath=\\
'{.status.containerStatuses[0].resources.limits}{"\\n"}{.status.containerStatuses[0].restartCount}'
# {"cpu":"4","memory":"48Gi"}
# 0`}</Code>
        <Code title="3 · verify the database sees the new ceiling">{`curl -s https://showcase.nebuladb.net/api/v1/admin/reliability \\
  | jq '{mode, used: .sample.memory_used_bytes, limit: .sample.memory_limit_bytes}'
# { "mode": "normal", "used": 26200000000, "limit": 51539607552 }`}</Code>
        <Callout tone="ok" title="Check status, not spec">
          <C>spec.containers[].resources</C> changes the moment the patch is accepted.{" "}
          <C>status.containerStatuses[].resources</C> changes only once the kubelet has actually resized
          the cgroup. In our case both read 48Gi within seconds, and NebulaDB's own reading of the cgroup
          (51.5 GB = 48 GiB) confirmed it from inside the process.
        </Callout>

        <H2 id="persist">8. Making it stick: Helm values and drift</H2>
        <p>
          A resized pod drifts from its Deployment template. The next rollout, Helm rollback or{" "}
          <C>helm upgrade --reuse-values</C> would rebuild the pod from the template at 32Gi. So we
          committed the new limit to the chart's prod values. Merging to <C>main</C> deploys only to the
          int environment; prod deploys are a manual dispatch, so the commit itself restarts nothing.
        </p>
        <Code title="deploy/helm/nebuladb/values-prod.yaml">{`  resources:
    limits:
      cpu: "4"
-     memory: "32Gi"
+     memory: "48Gi"
    requests:
      cpu: "500m"
      memory: "8Gi"`}</Code>
        <p>
          The next planned prod deploy will recreate the pod at 48Gi. That one still pays for a cold
          recovery, but at a moment we choose, not one the OOM killer chooses.
        </p>

        <H2 id="results">9. Results</H2>
        <Figure n={4} caption="Bulk-ingest throughput into the leader, measured by the loader: averaged over the pre-resize runs, and over the first run after the resize.">
          <ThroughputFigure />
        </Figure>
        <Table
          head={["Metric", "Before", "After"]}
          rows={[
            ["Leader memory limit", "32Gi (34.4 GB)", "48Gi (51.5 GB)"],
            ["Peak memory / limit", "34.1 GB / 34.4 GB (99%)", "≈ 27 GB / 51.5 GB (52%)"],
            ["Reliability mode", "memory_pressure · memory critical", "normal"],
            ["Pod restarts for the change", "—", "0"],
            ["Downtime", "—", "0 s"],
            ["Bulk ingest rate", "≈ 10 docs/s", "21 docs/s"],
            ["Loader pauses (memory)", "88 (median 7 min)", "0"],
          ]}
        />
        <p>
          The performance gain is real but specific. Bulk ingest doubled because the leader no longer
          spends its time in <C>memory_pressure</C> shedding load and waiting for memory to recover. Query
          latency was not the bottleneck here, and we are not claiming a change in it.
        </p>

        <H2 id="finish">10. Update: 64Gi, 96Gi and the full register</H2>
        <p>
          The 48Gi limit carried the load to about 2.75 million register documents. By then the problem
          had moved: memory between writes was fine, but the periodic snapshot (every ~23 minutes, running
          ~8 minutes) added a spike that grows with the corpus, and an idle leader was touching 43–48 GB of
          51.5 GB. Our earlier estimate of ~5 GB for snapshot overhead was far too low at this size.
        </p>
        <p>
          We applied the same procedure twice more: 48Gi → 64Gi on 15 October, then 64Gi → 96Gi to give
          the final 2.4 million documents room for a projected ~53 GB settled plus ~30 GB of snapshot peak.{" "}
          <C>debworker00</C> has about 125 GiB allocatable, so 96Gi still leaves the node headroom. Each step
          was a single resize patch, verified the same way, and committed to <C>values-prod.yaml</C>.
        </p>
        <Table
          head={["Final run", "Value"]}
          rows={[
            ["Documents loaded", "2,412,057 (rows 2,754,001 – 5,166,057)"],
            ["Duration", "106,689 s ≈ 29.6 h"],
            ["Ingest rate", "22–23 docs/s throughout"],
            ["Loader pauses", "3, each 60–120 s, all from a briefly unreadable memory gauge"],
            ["Leader memory, settled at the end", "54–57 GB of 103.1 GB"],
            ["Highest loader reading", "64.3 GB (during a snapshot)"],
            ["Leader restarts across all three resizes", "0 (same pod since 5 October)"],
            ["companies_house bucket", "5,166,063 docs (5,166,057 register + 6 enrichment)"],
          ]}
        />
        <p>
          The loader's memory guard (pause at 78 GB, resume at 68 GB) never fired on a real reading. It
          treats an unreadable gauge as high, which accounts for all three pauses: cautious, and cheap.
        </p>

        <H2 id="open">11. What is still open</H2>
        <ul className="list-disc space-y-1 pl-5">
          <li>
            <strong>Tombstone reclamation.</strong> HNSW deletes never free their slots, so resident memory
            only grows. Extra RAM buys headroom, not a fix; compaction is the real fix.
          </li>
          <li>
            <strong>Footprint per doc.</strong> The full register fits, but only with a 96Gi leader.
            Quantised vectors, or keeping the register as SQL rows with only a subset embedded, would
            bring that back down.
          </li>
          <li>
            <strong>Semantic quality.</strong> Prod still uses the <C>mock-384</C> embedder. Exact lookups
            and hybrid (BM25) search work today; meaning-based search needs a real embedder and a
            re-embed.
          </li>
          <li>
            <strong>Follower sizing.</strong> If the follower is ever promoted, it has to fit the leader's
            working set, so its limit has to follow the leader's. It is still at 32Gi and has not caught
            up with the full register.
          </li>
        </ul>

        <H2 id="checklist">12. Runbook checklist</H2>
        <ol className="list-decimal space-y-1 pl-5">
          <li>Confirm the server version supports in-place resize (<C>kubectl version</C> ≥ 1.33; GA in our 1.36).</li>
          <li>Confirm node headroom: <C>kubectl top node &lt;node&gt;</C> has room for the new limit.</li>
          <li>Patch the <strong>limit only</strong> through <C>--subresource resize</C> with a JSON patch.</li>
          <li>Verify <C>status.containerStatuses[].resources</C> and <C>restartCount</C>, not just spec.</li>
          <li>Verify from inside the app (<C>/admin/reliability</C> <C>memory_limit_bytes</C>).</li>
          <li>Commit the same value to <C>values-&lt;env&gt;.yaml</C> before anyone runs Helm again.</li>
          <li>Raise followers or peers that may be promoted to the same size.</li>
        </ol>
      </Prose>

      <nav className="hidden lg:block">
        <div className="sticky top-4 card !p-4">
          <div className="eyebrow mb-2">On this page</div>
          <ul className="space-y-1.5 text-xs">
            {TOC.map(([id, label]) => (
              <li key={id}>
                <button
                  onClick={() => document.getElementById(id)?.scrollIntoView({ behavior: "smooth" })}
                  className="text-left text-gray-600 hover:text-black dark:text-muted dark:hover:text-ink"
                >
                  {label}
                </button>
              </li>
            ))}
          </ul>
        </div>
      </nav>
    </div>
  );
}

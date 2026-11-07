import { useEffect, useState } from "react";
import { api, type BucketStats } from "../../api";
import { ErrorBanner } from "../../components";
import { overlapAt, percentile } from "../engine/core";
import { Banner, Bar, fmtPct } from "../shell";
import type { LabProps } from "./props";

const EFS = [16, 32, 64, 128, 256, 512];
const REF_EF = 512;

interface Row {
  ef: number;
  p50: number;
  p95: number;
  server50: number;
  agree: number;
}

interface Sweep {
  bucket: string;
  docs: number;
  rows: Row[];
  queries: number;
}

export function ScaleLab({ pack }: LabProps) {
  const [buckets, setBuckets] = useState<BucketStats[]>([]);
  const [target, setTarget] = useState<string>("");
  const [sweeps, setSweeps] = useState<Sweep[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    api
      .buckets()
      .then((b) => {
        const sorted = [...b].sort((x, y) => y.docs - x.docs);
        setBuckets(sorted);
        const big = sorted.find((x) => x.bucket !== pack.bucket);
        if (big) setTarget(big.bucket);
      })
      .catch((e: Error) => setErr(e.message));
  }, [pack.bucket]);

  const queries = pack.questions.slice(0, 8).map((q) => q.q);

  const sweep = async (bucket: string): Promise<Sweep> => {
    const docs = buckets.find((b) => b.bucket === bucket)?.docs ?? pack.docs.length;
    // Reference results and a warm-up pass.
    const ref = new Map<string, string[]>();
    for (const q of queries) ref.set(q, (await api.searchEx({ query: q, top_k: 10, bucket, ef: REF_EF })).hits.map((h) => h.id));
    const rows: Row[] = [];
    for (const ef of EFS) {
      setBusy(`${bucket}: ef ${ef}`);
      const client: number[] = [];
      const server: number[] = [];
      const agree: number[] = [];
      for (const q of queries) {
        const t0 = performance.now();
        const r = await api.searchEx({ query: q, top_k: 10, bucket, ef });
        client.push(performance.now() - t0);
        server.push(r.took_ms);
        agree.push(overlapAt(r.hits.map((h) => h.id), ref.get(q) ?? [], 10));
      }
      rows.push({
        ef,
        p50: percentile(client, 50),
        p95: percentile(client, 95),
        server50: percentile(server, 50),
        agree: agree.reduce((a, b) => a + b, 0) / agree.length,
      });
    }
    return { bucket, docs, rows, queries: queries.length };
  };

  const run = async () => {
    setErr(null);
    setSweeps([]);
    try {
      const out: Sweep[] = [];
      for (const b of [pack.bucket, target].filter(Boolean)) {
        out.push(await sweep(b));
        setSweeps([...out]);
      }
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(null);
    }
  };

  const maxMs = Math.max(1, ...sweeps.flatMap((s) => s.rows.map((r) => r.p95)));

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <p className="text-sm text-gray-600 dark:text-muted">
          {queries.length} of {pack.name}'s questions, run at each HNSW <code>ef</code> setting against the {pack.docs.length}-doc pack and against
          a large bucket on this server. Relevance isn't scored here — only speed and how much each setting agrees with ef {REF_EF}.
        </p>
        <div className="flex flex-wrap items-end gap-3">
          <label className="text-xs text-gray-500 dark:text-muted">
            <span className="eyebrow block mb-1">Compare with</span>
            <select className="input !w-auto !py-1.5 max-w-xs" value={target} onChange={(e) => setTarget(e.target.value)}>
              {buckets
                .filter((b) => b.bucket !== pack.bucket)
                .map((b) => (
                  <option key={b.bucket} value={b.bucket}>
                    {b.bucket} — {b.docs.toLocaleString()} docs
                  </option>
                ))}
            </select>
          </label>
          <button className="btn" onClick={run} disabled={!!busy}>
            {busy ? `Measuring ${busy}…` : "Run the sweep"}
          </button>
        </div>
        {buckets.length > 0 && buckets[0].docs < 10_000 && (
          <Banner kind="info">
            The largest bucket on this server has {buckets[0].docs.toLocaleString()} docs. On a big deployment (millions of docs) the gap
            between low and high ef gets much wider — run this lab against prod-sized data to see it.
          </Banner>
        )}
      </div>
      <ErrorBanner err={err} />

      {sweeps.map((s) => (
        <div key={s.bucket} className="card space-y-2 overflow-x-auto">
          <div className="flex items-baseline justify-between">
            <div className="font-semibold text-sm text-black dark:text-ink">
              {s.bucket} · {s.docs.toLocaleString()} docs
            </div>
            <span className="text-[11px] text-gray-500 dark:text-muted">{s.queries} queries per setting · measured {new Date().toLocaleTimeString()}</span>
          </div>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-left eyebrow">
                <th className="py-1 pr-3 w-14">ef</th>
                <th className="py-1 pr-3">Latency p50 / p95 (browser, incl. network)</th>
                <th className="py-1 pr-3 w-24">Server p50</th>
                <th className="py-1 w-56">Agreement with ef {REF_EF}</th>
              </tr>
            </thead>
            <tbody>
              {s.rows.map((r) => (
                <tr key={r.ef} className="border-t border-gray-100 dark:border-edge">
                  <td className="py-1.5 pr-3 font-mono">{r.ef}</td>
                  <td className="py-1.5 pr-3">
                    <Bar value={r.p95 / maxMs} label={`${Math.round(r.p50)}/${Math.round(r.p95)}`} tone="faint" />
                  </td>
                  <td className="py-1.5 pr-3 font-mono">{r.server50} ms</td>
                  <td className="py-1.5">
                    <Bar value={r.agree} label={fmtPct(r.agree)} tone={r.agree >= 0.95 ? "ok" : r.agree >= 0.8 ? "accent" : "warn"} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}

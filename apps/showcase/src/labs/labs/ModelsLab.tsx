import { useMemo, useState } from "react";
import { api } from "../../api";
import { ErrorBanner } from "../../components";
import { Bm25, cosine, percentile, score, type Score } from "../engine/core";
import { BROWSER_MODEL_LABEL, embedInBrowser, loadBrowserModel } from "../engine/browserEmbed";
import { Banner, Bar, fmtPct } from "../shell";
import type { LabelledQuestion } from "../types";
import type { LabProps } from "./props";

type RetrieverId = "bm25" | "server" | "hybrid" | "browser";

interface Run {
  q: LabelledQuestion;
  ranked: string[];
  ms: number;
}

interface Result {
  id: RetrieverId;
  label: string;
  runs: Run[];
}

/** Run labelled questions through each retriever with bounded concurrency. */
async function runAll(qs: LabelledQuestion[], f: (q: string) => Promise<string[]>, onTick: () => void): Promise<Run[]> {
  const out: Run[] = new Array(qs.length);
  let i = 0;
  const worker = async () => {
    while (i < qs.length) {
      const idx = i++;
      const t0 = performance.now();
      const ranked = await f(qs[idx].q);
      out[idx] = { q: qs[idx], ranked, ms: performance.now() - t0 };
      onTick();
    }
  };
  await Promise.all([worker(), worker(), worker(), worker()]);
  return out;
}

export function ModelsLab({ pack, env }: LabProps) {
  const [results, setResults] = useState<Result[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [progress, setProgress] = useState(0);
  const [err, setErr] = useState<string | null>(null);
  const [browser, setBrowser] = useState(false);
  const [dl, setDl] = useState<number | null>(null);
  const bm25 = useMemo(() => new Bm25(pack.docs), [pack]);
  const qs = pack.questions;

  const run = async () => {
    setErr(null);
    setResults([]);
    const out: Result[] = [];
    const tick = () => setProgress((p) => p + 1);
    try {
      setProgress(0);
      setBusy("BM25 (browser)");
      out.push({ id: "bm25", label: "BM25 keywords (browser)", runs: await runAll(qs, async (q) => bm25.search(q, 10).map((h) => h.id), tick) });
      setResults([...out]);
      setBusy(`${env?.embedModel ?? "server"} (NebulaDB vector)`);
      out.push({
        id: "server",
        label: `${env?.embedModel ?? "server embedder"} · vector`,
        runs: await runAll(qs, async (q) => (await api.searchEx({ query: q, top_k: 10, bucket: pack.bucket })).hits.map((h) => h.id), tick),
      });
      setResults([...out]);
      setBusy("NebulaDB hybrid");
      out.push({
        id: "hybrid",
        label: `${env?.embedModel ?? "server embedder"} · hybrid`,
        runs: await runAll(
          qs,
          async (q) => (await api.searchEx({ query: q, top_k: 10, bucket: pack.bucket, hybrid: true })).hits.map((h) => h.id),
          tick
        ),
      });
      setResults([...out]);
      if (browser) {
        setBusy("MiniLM in your browser: downloading model");
        await loadBrowserModel((p) => setDl(Math.round(p)));
        setBusy("MiniLM in your browser: embedding the pack");
        const docVecs = await embedInBrowser(pack.docs.map((d) => d.text));
        const runs: Run[] = [];
        for (const q of qs) {
          const t0 = performance.now();
          const [qv] = await embedInBrowser([q.q]);
          const ranked = pack.docs
            .map((d, i) => ({ id: d.id, s: cosine(qv, docVecs[i]) }))
            .sort((a, b) => b.s - a.s)
            .slice(0, 10)
            .map((x) => x.id);
          runs.push({ q, ranked, ms: performance.now() - t0 });
          tick();
        }
        out.push({ id: "browser", label: BROWSER_MODEL_LABEL, runs });
        setResults([...out]);
      }
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(null);
    }
  };

  const scored = results.map((r) => ({ ...r, s: score(r.runs.map((x) => ({ ranked: x.ranked, expect: x.q.expect }))), p50: percentile(r.runs.map((x) => x.ms), 50) }));
  const best = [...scored].sort((a, b) => b.s.mrr - a.s.mrr)[0];
  const groups = useMemo(() => {
    const kinds = [...new Set(qs.map((q) => q.kind))];
    const langs = [...new Set(qs.map((q) => q.lang).filter(Boolean))] as string[];
    return { kinds, langs };
  }, [qs]);
  const sub = (r: Result, pred: (q: LabelledQuestion) => boolean): Score =>
    score(r.runs.filter((x) => pred(x.q)).map((x) => ({ ranked: x.ranked, expect: x.q.expect })));

  const total = qs.length * (3 + (browser ? 1 : 0));
  const exportJson = () => {
    const blob = new Blob(
      [
        JSON.stringify(
          {
            lab: "models",
            pack: pack.id,
            docs: pack.docs.length,
            questions: qs.length,
            embedder: env?.embedModel,
            measured_at: new Date().toISOString(),
            retrievers: scored.map((r) => ({ id: r.id, label: r.label, ...r.s, p50_ms: Math.round(r.p50 * 10) / 10 })),
          },
          null,
          2
        ),
      ],
      { type: "application/json" }
    );
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `nebula-lab-models-${pack.id}.json`;
    a.click();
  };

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <p className="text-sm text-gray-600 dark:text-muted">
          {qs.length} labelled questions for {pack.name}:{" "}
          {groups.kinds.map((k) => `${qs.filter((q) => q.kind === k).length} ${k}`).join(", ")}
          {groups.langs.length > 1 && ` · languages: ${groups.langs.join(", ")}`}.
        </p>
        <label className="flex items-start gap-2 text-sm text-gray-700 dark:text-ink">
          <input type="checkbox" className="mt-1" checked={browser} onChange={(e) => setBrowser(e.target.checked)} />
          <span>
            Also run {BROWSER_MODEL_LABEL}
            <span className="block text-xs text-gray-500 dark:text-muted">
              Downloads about 23 MB of model weights from Hugging Face once, then runs on your device. Your questions don't leave the browser for
              this retriever.
            </span>
          </span>
        </label>
        <div className="flex items-center gap-3">
          <button className="btn" onClick={run} disabled={!!busy}>
            {busy ? "Running…" : "Run the shoot-out"}
          </button>
          {busy && (
            <span className="text-xs text-gray-500 dark:text-muted">
              {busy}
              {dl !== null && busy.includes("download") && ` ${dl}%`} · {progress}/{total}
            </span>
          )}
          {scored.length > 0 && !busy && (
            <button className="btn-secondary" onClick={exportJson}>
              Export JSON
            </button>
          )}
        </div>
      </div>
      <ErrorBanner err={err} />

      {scored.length > 0 && (
        <div className="card space-y-4 overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left eyebrow">
                <th className="py-1.5 pr-3">Retriever</th>
                <th className="py-1.5 pr-3 w-40">Hit@1</th>
                <th className="py-1.5 pr-3 w-40">Hit@5</th>
                <th className="py-1.5 pr-3 w-40">MRR</th>
                <th className="py-1.5 pr-3">Recall@10</th>
                <th className="py-1.5">Median latency</th>
              </tr>
            </thead>
            <tbody>
              {scored.map((r) => (
                <tr key={r.id} className="border-t border-gray-100 dark:border-edge">
                  <td className="py-2 pr-3 font-medium text-black dark:text-ink">
                    {r.label}
                    {best?.id === r.id && scored.length > 1 && <span className="ml-2 text-[10px] font-mono text-ok">best MRR</span>}
                  </td>
                  <td className="py-2 pr-3">
                    <Bar value={r.s.hit1} label={fmtPct(r.s.hit1)} />
                  </td>
                  <td className="py-2 pr-3">
                    <Bar value={r.s.hit5} label={fmtPct(r.s.hit5)} />
                  </td>
                  <td className="py-2 pr-3">
                    <Bar value={r.s.mrr} label={r.s.mrr.toFixed(2)} tone="ok" />
                  </td>
                  <td className="py-2 pr-3 font-mono text-xs">{fmtPct(r.s.recall10)}</td>
                  <td className="py-2 font-mono text-xs">{r.p50.toFixed(1)} ms</td>
                </tr>
              ))}
            </tbody>
          </table>

          <div>
            <div className="eyebrow mb-1.5">MRR by question type{groups.langs.length > 1 ? " and language" : ""}</div>
            <table className="text-xs">
              <thead>
                <tr className="text-left text-gray-500 dark:text-muted">
                  <th className="pr-4 py-1">Retriever</th>
                  {groups.kinds.map((k) => (
                    <th key={k} className="pr-4 py-1 font-mono">
                      {k}
                    </th>
                  ))}
                  {groups.langs.length > 1 &&
                    groups.langs.map((l) => (
                      <th key={l} className="pr-4 py-1 font-mono">
                        {l}
                      </th>
                    ))}
                </tr>
              </thead>
              <tbody>
                {scored.map((r) => (
                  <tr key={r.id} className="border-t border-gray-100 dark:border-edge">
                    <td className="pr-4 py-1 text-gray-700 dark:text-ink">{r.label}</td>
                    {groups.kinds.map((k) => (
                      <td key={k} className="pr-4 py-1 font-mono tabular-nums">
                        {sub(r, (q) => q.kind === k).mrr.toFixed(2)}
                      </td>
                    ))}
                    {groups.langs.length > 1 &&
                      groups.langs.map((l) => (
                        <td key={l} className="pr-4 py-1 font-mono tabular-nums">
                          {sub(r, (q) => q.lang === l).mrr.toFixed(2)}
                        </td>
                      ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {!busy && scored.length > 1 && <Recommendation scored={scored} kinds={groups.kinds} sub={sub} packName={pack.name} mock={!!env?.mockEmbedder} />}
        </div>
      )}
    </div>
  );
}

function Recommendation({
  scored,
  kinds,
  sub,
  packName,
  mock,
}: {
  scored: (Result & { s: Score; p50: number })[];
  kinds: string[];
  sub: (r: Result, pred: (q: LabelledQuestion) => boolean) => Score;
  packName: string;
  mock: boolean;
}) {
  const ranked = [...scored].sort((a, b) => b.s.mrr - a.s.mrr);
  const [first, second] = ranked;
  const margin = first.s.mrr - (second?.s.mrr ?? 0);
  const perKind = kinds
    .map((k) => {
      const w = [...scored].sort((a, b) => sub(b, (q) => q.kind === k).mrr - sub(a, (q) => q.kind === k).mrr)[0];
      return { k, w, v: sub(w, (q) => q.kind === k).mrr };
    })
    .filter((x) => x.w.id !== first.id);
  return (
    <Banner kind="info">
      <div className="space-y-1 text-sm">
        <div>
          <b>For {packName}:</b> {first.label} ranks the right document highest on average (MRR {first.s.mrr.toFixed(2)}
          {second ? `, ${margin >= 0.05 ? `${margin.toFixed(2)} ahead of` : "level with"} ${second.label}` : ""}).
          {margin < 0.05 && " The gap is within noise for this many questions — pick on cost and latency."}
        </div>
        {perKind.length > 0 && (
          <div>
            But {perKind.map((x) => `${x.w.label} wins on ${x.k} questions (${x.v.toFixed(2)})`).join("; ")} — a hybrid of the two is
            usually the safest default.
          </div>
        )}
        {mock && <div className="text-warn">The server embedder is the mock hash, so its row is a floor, not a real model score.</div>}
      </div>
    </Banner>
  );
}

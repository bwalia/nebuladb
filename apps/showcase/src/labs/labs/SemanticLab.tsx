import { useCallback, useEffect, useMemo, useState } from "react";
import { api, type Hit, type HitExplain } from "../../api";
import { ErrorBanner } from "../../components";
import { Bm25, type Bm25Hit } from "../engine/core";
import { Bar, QuestionBox } from "../shell";
import type { LabProps } from "./props";

interface Col {
  key: "keyword" | "vector" | "hybrid";
  title: string;
  sub: string;
  hits: { id: string; text: string; score: number; why: string }[];
  ms: number;
}

/** Meaning vs words: BM25 (browser) | vector (NebulaDB) | hybrid (NebulaDB) on one question. */
export function SemanticLab({ pack, state, update }: LabProps) {
  // Open on a labelled paraphrase so the expected answer is highlighted from the start.
  const q = state.q ?? pack.questions.find((x) => x.kind === "paraphrase")?.q ?? pack.sample;
  const k = state.k ?? 5;
  const [cols, setCols] = useState<Col[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const bm25 = useMemo(() => new Bm25(pack.docs), [pack]);
  const byId = useMemo(() => new Map(pack.docs.map((d) => [d.id, d])), [pack]);
  const label = pack.questions.find((x) => x.q === q);

  const run = useCallback(
    async (question: string) => {
      setBusy(true);
      setErr(null);
      try {
        const t0 = performance.now();
        const kw: Bm25Hit[] = bm25.search(question, k);
        const kwMs = performance.now() - t0;
        const [vec, hyb] = await Promise.all([
          api.searchEx({ query: question, top_k: k, bucket: pack.bucket }),
          api.searchEx({ query: question, top_k: k, bucket: pack.bucket, hybrid: true, explain: true }),
        ]);
        const explainOf = new Map<string, HitExplain>((hyb.explain?.hits ?? []).map((h) => [h.id, h]));
        const textOf = (h: Hit) => byId.get(h.id)?.text ?? h.text;
        setCols([
          {
            key: "keyword",
            title: "Words (BM25)",
            sub: "exact terms, weighted by rarity",
            ms: Math.round(kwMs * 10) / 10,
            hits: kw.map((h) => ({
              id: h.id,
              text: byId.get(h.id)?.text ?? "",
              score: h.score,
              why: Object.entries(h.terms)
                .sort((a, b) => b[1] - a[1])
                .map(([t, c]) => `${t} ${c.toFixed(2)}`)
                .join(" · "),
            })),
          },
          {
            key: "vector",
            title: "Meaning (vectors)",
            sub: "embedding + HNSW in NebulaDB",
            ms: vec.took_ms,
            // Vector hits carry a cosine *distance* (lower = closer); show similarity = 1 − distance.
            hits: vec.hits.map((h) => ({ id: h.id, text: textOf(h), score: 1 - h.score, why: `cosine distance ${h.score.toFixed(3)} → similarity ${(1 - h.score).toFixed(3)}` })),
          },
          {
            key: "hybrid",
            title: "Both (hybrid)",
            sub: "BM25 + vector, fused in NebulaDB",
            ms: hyb.took_ms,
            hits: hyb.hits.map((h) => {
              const e = explainOf.get(h.id);
              const parts = [
                e?.vector?.weighted != null ? `vector ${e.vector.weighted.toFixed(3)}` : null,
                e?.bm25?.weighted != null ? `words ${e.bm25.weighted.toFixed(3)}` : null,
              ].filter(Boolean);
              return { id: h.id, text: textOf(h), score: h.score, why: parts.length ? parts.join(" + ") : `fused ${h.score.toFixed(3)}` };
            }),
          },
        ]);
      } catch (e) {
        setErr((e as Error).message);
      } finally {
        setBusy(false);
      }
    },
    [bm25, byId, k, pack.bucket]
  );

  useEffect(() => {
    void run(q);
    // re-run when pack or question changes
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pack.id, q, k]);

  const samples = [
    ...pack.questions.filter((x) => x.kind === "paraphrase").slice(0, 3),
    ...pack.questions.filter((x) => x.kind === "keyword").slice(0, 3),
  ].map((x) => x.q);

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <QuestionBox value={q} samples={samples} busy={busy} onSubmit={(nq) => update({ q: nq })} />
        <div className="flex flex-wrap items-center gap-4 text-xs text-gray-500 dark:text-muted">
          <label className="inline-flex items-center gap-2">
            <span className="eyebrow">Top</span>
            <select className="input !w-auto !py-1" value={k} onChange={(e) => update({ k: Number(e.target.value) })}>
              {[3, 5, 10].map((n) => (
                <option key={n}>{n}</option>
              ))}
            </select>
          </label>
          {label ? (
            <span>
              Labelled question ({label.kind}). Expected answer: <code>{label.expect.join(", ")}</code> — highlighted green below.
            </span>
          ) : (
            <span>Your own question — no labelled answer, so judge the results yourself.</span>
          )}
        </div>
      </div>
      <ErrorBanner err={err} />
      {cols && (
        <div className="grid gap-3 lg:grid-cols-3">
          {cols.map((c) => (
            <div key={c.key} className="card !p-4 space-y-2">
              <div className="flex items-baseline justify-between gap-2">
                <div>
                  <div className="font-semibold text-sm text-black dark:text-ink">{c.title}</div>
                  <div className="text-[11px] text-gray-500 dark:text-muted">{c.sub}</div>
                </div>
                <span className="font-mono text-[11px] text-gray-500 dark:text-muted tabular-nums">{c.ms} ms</span>
              </div>
              {c.hits.length === 0 && <p className="text-xs text-gray-500 dark:text-muted">No document shares a word with the question.</p>}
              <ol className="space-y-2">
                {c.hits.map((h, i) => {
                  const good = label?.expect.includes(h.id);
                  const top = c.hits[0]?.score || 1;
                  return (
                    <li
                      key={h.id}
                      className={`rounded-md border px-2.5 py-2 ${good ? "border-ok/60 bg-ok/5" : "border-gray-200 dark:border-edge"}`}
                    >
                      <div className="flex items-center gap-2 text-[11px] font-mono text-gray-500 dark:text-muted">
                        <span>{i + 1}</span>
                        <span className="truncate">{h.id}</span>
                        {good && <span className="ml-auto text-ok">expected ✓</span>}
                      </div>
                      <p className="mt-1 text-xs text-gray-700 dark:text-ink line-clamp-3">{h.text}</p>
                      <div className="mt-1.5">
                        <Bar value={h.score / top} label={h.score.toFixed(3)} tone={good ? "ok" : "accent"} />
                      </div>
                      <div className="mt-1 text-[10px] font-mono text-gray-400 dark:text-faint truncate" title={h.why}>
                        why: {h.why || "—"}
                      </div>
                    </li>
                  );
                })}
              </ol>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

import { useMemo, useState } from "react";
import { api } from "../../api";
import { ErrorBanner } from "../../components";
import { Bm25, docPasses } from "../engine/core";
import { matchesSafety } from "../validate";
import { Banner, Bar, fmtPct } from "../shell";
import type { Pack, RefuseKind } from "../types";
import type { LabProps } from "./props";

type Expect = RefuseKind | "answer";

interface Probe {
  id: string;
  q: string;
  expect: Expect;
  persona?: string;
  /** Measured once per run; classification below is recomputed live from the thresholds. */
  rule: string | null;
  bm25Top: number;
  vecTopSim: number;
  topId: string | null;
}

/** The guard order every pack app uses: rule in code → access filter → relevance. */
function classify(p: Probe, pack: Pack, t: { bm25: number; vec: number; useVec: boolean }): { got: Expect; why: string } {
  if (p.rule) return { got: "safety", why: `rule “${p.rule}” matched — fixed card, no model` };
  if (p.persona && p.topId) {
    const persona = pack.personas.find((x) => x.id === p.persona);
    const doc = pack.docs.find((d) => d.id === p.topId);
    if (persona && doc && !docPasses(pack, doc, {}, persona.filter)) {
      return { got: "acl", why: `best match ${p.topId} is outside ${persona.label}'s data — the SQL filter never returns it` };
    }
  }
  const lexical = p.bm25Top >= t.bm25;
  const semantic = t.useVec && p.vecTopSim >= t.vec;
  if (!lexical && !semantic) {
    return {
      got: "out_of_scope",
      why: `nothing relevant: BM25 ${p.bm25Top.toFixed(2)} < ${t.bm25}${t.useVec ? `, similarity ${p.vecTopSim.toFixed(2)} < ${t.vec}` : ""}`,
    };
  }
  return { got: "answer", why: lexical ? `BM25 ${p.bm25Top.toFixed(2)} ≥ ${t.bm25}` : `similarity ${p.vecTopSim.toFixed(2)} ≥ ${t.vec}` };
}

const LABEL: Record<Expect, string> = { safety: "safety card", acl: "access blocked", out_of_scope: "not in the documents", answer: "answer" };

export function RefuseLab({ pack, env }: LabProps) {
  const [probes, setProbes] = useState<Probe[] | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [bm25T, setBm25T] = useState(2);
  const [vecT, setVecT] = useState(0.55);
  const useVec = !env?.mockEmbedder;
  const bm25 = useMemo(() => new Bm25(pack.docs), [pack]);

  const run = async () => {
    setBusy(true);
    setErr(null);
    try {
      const items: { id: string; q: string; expect: Expect; persona?: string }[] = [
        ...pack.refuse.map((r) => ({ id: r.id, q: r.q, expect: r.expect as Expect, persona: r.persona })),
        // Controls: questions the app SHOULD answer, so over-refusal shows up too.
        ...pack.questions.slice(0, 12).map((x) => ({ id: x.id, q: x.q, expect: "answer" as Expect })),
      ];
      const out: Probe[] = [];
      let i = 0;
      const worker = async () => {
        while (i < items.length) {
          const it = items[i++];
          const rule = pack.safety.find((r) => matchesSafety(it.q, r.triggers))?.label ?? null;
          const kw = bm25.search(it.q, 1)[0];
          // Vector top-1 for the relevance threshold (cosine distance → similarity);
          // hybrid top-1 for "where would the answer come from", which keyword overlap anchors.
          const [v, h] = await Promise.all([
            api.searchEx({ query: it.q, top_k: 1, bucket: pack.bucket }),
            api.searchEx({ query: it.q, top_k: 1, bucket: pack.bucket, hybrid: true }),
          ]);
          const top = v.hits[0];
          out.push({ ...it, rule, bm25Top: kw?.score ?? 0, vecTopSim: top ? 1 - top.score : 0, topId: h.hits[0]?.id ?? top?.id ?? kw?.id ?? null });
        }
      };
      await Promise.all([worker(), worker(), worker(), worker()]);
      const order = new Map(items.map((x, j) => [x.id, j]));
      setProbes(out.sort((a, b) => order.get(a.id)! - order.get(b.id)!));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const rows = probes?.map((p) => ({ p, ...classify(p, pack, { bm25: bm25T, vec: vecT, useVec }) })) ?? [];
  const correct = rows.filter((r) => r.got === r.p.expect).length;
  const falseRefusals = rows.filter((r) => r.p.expect === "answer" && r.got !== "answer").length;
  const missed = rows.filter((r) => r.p.expect !== "answer" && r.got === "answer").length;
  const kinds: Expect[] = ["safety", "acl", "out_of_scope", "answer"];

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <p className="text-sm text-gray-600 dark:text-muted">
          {pack.refuse.length} questions {pack.name}'s assistant must not answer from the model, plus 12 it should answer, so over-refusal
          counts too. Safety rules for this pack: {pack.safety.map((s) => s.label).join(", ")}.
        </p>
        <div className="flex flex-wrap items-end gap-5">
          <button className="btn" onClick={run} disabled={busy}>
            {busy ? "Probing…" : probes ? "Run again" : "Run the guards"}
          </button>
          <label className="text-xs text-gray-500 dark:text-muted">
            <span className="eyebrow block mb-1">Keyword threshold (BM25) {bm25T.toFixed(1)}</span>
            <input type="range" min={0} max={8} step={0.5} value={bm25T} onChange={(e) => setBm25T(Number(e.target.value))} />
          </label>
          <label className={`text-xs text-gray-500 dark:text-muted ${useVec ? "" : "opacity-40"}`}>
            <span className="eyebrow block mb-1">Meaning threshold (similarity) {vecT.toFixed(2)}</span>
            <input type="range" min={0.2} max={0.9} step={0.05} value={vecT} disabled={!useVec} onChange={(e) => setVecT(Number(e.target.value))} />
          </label>
        </div>
        {!useVec && (
          <Banner kind="warn">
            With the mock embedder, similarity scores are meaningless, so only the keyword threshold decides relevance. Paraphrased questions
            will look out of scope — expect false refusals until a real embedder is configured.
          </Banner>
        )}
      </div>
      <ErrorBanner err={err} />

      {probes && (
        <>
          <div className="grid gap-3 sm:grid-cols-3">
            <div className="card !p-4">
              <div className="eyebrow">Handled correctly</div>
              <div className="mt-1 text-2xl font-semibold text-black dark:text-ink">{fmtPct(correct / rows.length)}</div>
              <Bar value={correct / rows.length} label={`${correct}/${rows.length}`} tone="ok" />
            </div>
            <div className="card !p-4">
              <div className="eyebrow">Answered when it shouldn't</div>
              <div className={`mt-1 text-2xl font-semibold ${missed ? "text-bad" : "text-ok"}`}>{missed}</div>
              <p className="text-[11px] text-gray-500 dark:text-muted">the dangerous failure</p>
            </div>
            <div className="card !p-4">
              <div className="eyebrow">Refused when it should answer</div>
              <div className={`mt-1 text-2xl font-semibold ${falseRefusals ? "text-warn" : "text-ok"}`}>{falseRefusals}</div>
              <p className="text-[11px] text-gray-500 dark:text-muted">the annoying failure</p>
            </div>
          </div>

          <div className="card overflow-x-auto">
            <div className="eyebrow mb-2">Expected (rows) vs what the guards did (columns)</div>
            <table className="text-xs mb-4">
              <thead>
                <tr>
                  <th />
                  {kinds.map((k) => (
                    <th key={k} className="px-3 py-1 font-mono text-gray-500 dark:text-muted">
                      {LABEL[k]}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {kinds.map((e) => (
                  <tr key={e} className="border-t border-gray-100 dark:border-edge">
                    <td className="pr-3 py-1 font-mono text-gray-500 dark:text-muted">{LABEL[e]}</td>
                    {kinds.map((g) => {
                      const n = rows.filter((r) => r.p.expect === e && r.got === g).length;
                      return (
                        <td key={g} className={`px-3 py-1 text-center font-mono tabular-nums ${n ? (e === g ? "text-ok font-semibold" : "text-bad font-semibold") : "text-gray-300 dark:text-faint"}`}>
                          {n}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
            <table className="w-full text-xs">
              <thead>
                <tr className="text-left eyebrow">
                  <th className="py-1 pr-3">Question</th>
                  <th className="py-1 pr-3">Expected</th>
                  <th className="py-1 pr-3">Got</th>
                  <th className="py-1">Why</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(({ p, got, why }) => (
                  <tr key={p.id} className="border-t border-gray-100 dark:border-edge align-top">
                    <td className="py-1.5 pr-3 text-gray-800 dark:text-ink max-w-md">
                      {p.q}
                      {p.persona && <span className="block text-[10px] text-gray-400 dark:text-faint">asked as {pack.personas.find((x) => x.id === p.persona)?.label}</span>}
                    </td>
                    <td className="py-1.5 pr-3 font-mono text-gray-500 dark:text-muted">{LABEL[p.expect]}</td>
                    <td className={`py-1.5 pr-3 font-mono ${got === p.expect ? "text-ok" : "text-bad"}`}>{LABEL[got]}</td>
                    <td className="py-1.5 text-gray-500 dark:text-muted">{why}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}

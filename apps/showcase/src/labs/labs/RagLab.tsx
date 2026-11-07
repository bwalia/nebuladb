import { useState } from "react";
import { api, type AiChatResponse } from "../../api";
import { ErrorBanner } from "../../components";
import { PipelineTrace, type PipelineStep } from "../../rag/PipelineTrace";
import { claims, filterSql, ragPrompt, unsupportedNumbers } from "../engine/core";
import { matchesSafety } from "../validate";
import { Banner, ModelPicker, PersonaPicker, QuestionBox, personaOf } from "../shell";
import type { LabProps } from "./props";

interface Chunk {
  id: string;
  text: string;
  score: number;
}

const TOKEN_BUDGET = 1500;
const approxTokens = (s: string) => Math.ceil(s.length / 4);

export function RagLab({ pack, state, update, env }: LabProps) {
  const q = state.q ?? pack.sample;
  const k = state.k ?? 5;
  const mode = state.mode === "vector" ? "vector" : "hybrid";
  const provider = state.models?.[0];
  const persona = personaOf(pack, state.persona);

  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [chunks, setChunks] = useState<Chunk[]>([]);
  const [bare, setBare] = useState<AiChatResponse | null>(null);
  const [grounded, setGrounded] = useState<AiChatResponse | null>(null);
  const [card, setCard] = useState<string | null>(null);
  const [focus, setFocus] = useState<number | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [asked, setAsked] = useState<string | null>(null);

  const run = async (question: string) => {
    update({ q: question });
    setAsked(question);
    setBusy(true);
    setErr(null);
    setBare(null);
    setGrounded(null);
    setChunks([]);
    setFocus(null);
    setCard(null);
    const s: PipelineStep[] = [];
    const push = (st: PipelineStep) => {
      s.push(st);
      setSteps([...s]);
    };
    try {
      // 0. Safety rules run in code, before any retrieval or model call.
      const rule = pack.safety.find((r) => matchesSafety(question, r.triggers));
      if (rule) {
        push({ label: `Safety rule “${rule.label}” matched — fixed response, no model call`, status: "ok" });
        setCard(rule.card);
        return;
      }
      push({ label: "Safety rules checked: none matched", status: "ok", detail: pack.safety.map((r) => r.label).join(" · ") });

      // 1. Retrieve.
      const t0 = performance.now();
      let got: Chunk[];
      if (persona) {
        const sql = filterSql(pack, question, {}, k, persona.filter);
        const r = await api.sql(sql);
        got = r.rows.map((x) => ({ id: x.id, text: String(x.fields.text ?? pack.docs.find((d) => d.id === x.id)?.text ?? ""), score: x.score }));
        push({ label: `Retrieve as ${persona.label} (filter inside the SQL)`, detail: sql, rowsOut: got.length, ms: Math.round(performance.now() - t0) });
      } else {
        const r = await api.searchEx({ query: question, top_k: k, bucket: pack.bucket, hybrid: mode === "hybrid" });
        got = r.hits.map((h) => ({ id: h.id, text: pack.docs.find((d) => d.id === h.id)?.text ?? h.text, score: h.score }));
        push({ label: `Retrieve top ${k} (${mode})`, detail: `POST /api/v1/ai/search {bucket: ${pack.bucket}, hybrid: ${mode === "hybrid"}}`, rowsOut: got.length, ms: r.took_ms });
      }

      // 2. Assemble context within a token budget.
      const kept: Chunk[] = [];
      let used = 0;
      for (const c of got) {
        const t = approxTokens(c.text) + 8;
        if (used + t > TOKEN_BUDGET) break;
        kept.push(c);
        used += t;
      }
      setChunks(kept);
      push({ label: `Assemble ${kept.length} numbered sources (~${used} of ${TOKEN_BUDGET} tokens)`, rowsIn: got.length, rowsOut: kept.length });

      // 3. Two answers from the same model.
      const prompt = ragPrompt(question, kept, pack.system);
      const t1 = performance.now();
      const [a, b] = await Promise.all([
        api.aiChat({ message: question, system: `Answer the question briefly. ${pack.system}`, provider, temperature: 0.2, max_tokens: 400 }),
        api.aiChat({ message: prompt.user, system: prompt.system, provider, temperature: 0.1, max_tokens: 500 }),
      ]);
      setBare(a);
      setGrounded(b);
      push({
        label: `Generate twice with ${b.model} (${b.provider}) — without and with sources`,
        detail: `system: ${prompt.system}\n\n${prompt.user}`,
        ms: Math.round(performance.now() - t1),
      });
    } catch (e) {
      setErr((e as Error).message);
      push({ label: "Failed", status: "error", detail: (e as Error).message });
    } finally {
      setBusy(false);
    }
  };

  const gc = grounded ? claims(grounded.answer) : [];
  const badNums = grounded && asked ? unsupportedNumbers(grounded.answer, chunks.map((c) => c.text), asked) : [];
  const uncited = gc.filter((c) => !c.cites.length && !/not in the documents/i.test(c.text)).length;
  const bareNums = bare && asked ? unsupportedNumbers(bare.answer, chunks.map((c) => c.text), asked) : [];

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <QuestionBox value={q} busy={busy} cta="Ask" samples={[pack.sample, ...pack.questions.filter((x) => x.kind !== "keyword").map((x) => x.q)]} onSubmit={run} />
        <div className="flex flex-wrap items-center gap-3">
          <ModelPicker env={env} value={provider} onChange={(p) => update({ models: [p] })} />
          <label className="inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
            <span className="eyebrow">Retrieval</span>
            <select className="input !w-auto !py-1.5" value={mode} onChange={(e) => update({ mode: e.target.value })} disabled={!!persona}>
              <option value="hybrid">hybrid</option>
              <option value="vector">vector</option>
            </select>
          </label>
          <label className="inline-flex items-center gap-2 text-xs text-gray-500 dark:text-muted">
            <span className="eyebrow">Sources</span>
            <select className="input !w-auto !py-1.5" value={k} onChange={(e) => update({ k: Number(e.target.value) })}>
              {[3, 5, 8].map((n) => (
                <option key={n}>{n}</option>
              ))}
            </select>
          </label>
          <PersonaPicker pack={pack} value={state.persona} onChange={(id) => update({ persona: id })} />
        </div>
      </div>
      <ErrorBanner err={err} />
      {steps.length > 0 && (
        <div className="card space-y-2">
          <div className="eyebrow">Pipeline</div>
          <PipelineTrace steps={steps} />
        </div>
      )}

      {card && (
        <div className="card border-warn/50">
          <div className="eyebrow text-warn">Fixed response (rule in code — the model was not asked)</div>
          <p className="mt-2 whitespace-pre-wrap text-sm text-black dark:text-ink">{card}</p>
        </div>
      )}

      {grounded && bare && (
        <div className="grid gap-4 lg:grid-cols-2">
          <div className="card space-y-2">
            <div className="flex items-baseline justify-between">
              <div className="font-semibold text-sm text-black dark:text-ink">Without retrieval</div>
              <span className="text-[11px] text-gray-500 dark:text-muted">model memory only</span>
            </div>
            <p className="whitespace-pre-wrap text-sm text-gray-700 dark:text-ink">{bare.answer}</p>
            <Banner kind="warn">
              No sources, so nothing here can be checked against {pack.name}'s documents.
              {bareNums.length > 0 && ` Numbers not found in the retrieved passages: ${bareNums.join(", ")}.`}
            </Banner>
          </div>
          <div className="card space-y-2">
            <div className="flex items-baseline justify-between">
              <div className="font-semibold text-sm text-black dark:text-ink">With retrieval</div>
              <span className="text-[11px] text-gray-500 dark:text-muted">{chunks.length} sources</span>
            </div>
            <div className="space-y-1.5 text-sm">
              {gc.map((c, i) => (
                <p key={i} className={c.cites.length || /not in the documents/i.test(c.text) ? "text-gray-800 dark:text-ink" : "text-gray-800 dark:text-ink decoration-warn underline decoration-dotted underline-offset-4"}>
                  {c.text.replace(/\[(\d+(?:\s*,\s*\d+)*)\]/g, "")}{" "}
                  {c.cites.map((n) => (
                    <button
                      key={n}
                      onClick={() => setFocus(n)}
                      className={`mx-0.5 rounded px-1 font-mono text-[10px] ${chunks[n - 1] ? "bg-accent/15 text-accent" : "bg-bad/15 text-bad"}`}
                      title={chunks[n - 1] ? chunks[n - 1].id : "cites a source that doesn't exist"}
                    >
                      {n}
                    </button>
                  ))}
                  {!c.cites.length && !/not in the documents/i.test(c.text) && <span className="ml-1 text-[10px] text-warn">no source</span>}
                </p>
              ))}
            </div>
            <div className="flex flex-wrap gap-3 text-[11px] text-gray-500 dark:text-muted">
              <span>{gc.length - uncited}/{gc.length} sentences cited</span>
              <span className={badNums.length ? "text-bad" : "text-ok"}>
                {badNums.length ? `numbers not in any source: ${badNums.join(", ")}` : "every number appears in a source"}
              </span>
            </div>
          </div>
        </div>
      )}

      {chunks.length > 0 && (
        <div className="card space-y-2">
          <div className="eyebrow">Sources sent to the model</div>
          <ol className="space-y-1.5">
            {chunks.map((c, i) => (
              <li
                key={c.id}
                className={`rounded-md border px-2.5 py-2 text-xs transition-colors ${focus === i + 1 ? "border-accent bg-accent/5" : "border-gray-200 dark:border-edge"}`}
              >
                <span className="font-mono text-accent">[{i + 1}]</span> <span className="font-mono text-gray-500 dark:text-muted">{c.id}</span>
                <p className="mt-1 text-gray-700 dark:text-ink">{c.text}</p>
              </li>
            ))}
          </ol>
        </div>
      )}
    </div>
  );
}

import { useEffect, useState } from "react";
import { api } from "../../api";
import { ErrorBanner } from "../../components";
import { checkSql, extractJson, flatFields, unsupportedNumbers } from "../engine/core";
import { Banner, ModelPicker } from "../shell";
import type { Pack, ReasonStep } from "../types";
import type { LabProps } from "./props";

interface StepRun {
  step: ReasonStep;
  status: "pending" | "running" | "ok" | "error";
  rows?: Record<string, unknown>[];
  ms?: number;
  error?: string;
}

/** A compact, model-friendly rendering of a step's output (also the evidence for the number check). */
function stepOutput(r: StepRun, i: number): string {
  if (r.status !== "ok" || !r.rows) return `[S${i + 1}] (no output)`;
  const lines = r.rows.slice(0, 25).map((row) =>
    Object.entries(row)
      .filter(([k]) => k !== "seed_version")
      .map(([k, v]) => `${k}=${Array.isArray(v) ? v.join("|") : String(v)}`)
      .join("; ")
  );
  return `[S${i + 1}] ${r.step.kind === "sql" ? r.step.sql.replace(/\s+/g, " ") : `search "${r.step.query}"`}\n${lines.join("\n") || "(0 rows)"}`;
}

function schemaFor(pack: Pack): string {
  const fields = new Map<string, Set<string>>();
  for (const d of pack.docs)
    for (const [k, v] of Object.entries(d.metadata)) {
      const s = fields.get(k) ?? new Set<string>();
      if (s.size < 6) s.add(Array.isArray(v) ? `[${v.slice(0, 2).join(",")}]` : JSON.stringify(v));
      fields.set(k, s);
    }
  return [...fields].map(([k, vs]) => `- ${k}: e.g. ${[...vs].join(", ")}`).join("\n");
}

function planPrompt(pack: Pack, question: string): { system: string; user: string } {
  const system = `You plan read-only data lookups for a question about ${pack.name} (${pack.industry}). Reply with JSON only:
{"steps":[{"kind":"sql","why":"…","sql":"SELECT …"},{"kind":"search","why":"…","query":"…","k":5}]}
Use 2–4 steps. SQL rules (NebulaDB): SELECT from ${pack.bucket} only; WHERE must contain exactly one semantic_match(text, '…'); other predicates are metadata filters (=, <=, >=, array_contains(field, 'v')); no OR, JOIN, subquery or CTE; GROUP BY needs explicit columns with COUNT(*), SUM, AVG, MIN or MAX; always end with LIMIT 200; escape ' as ''. Use "search" for policy or guidance text.`;
  const user = `Metadata fields:\n${schemaFor(pack)}\n\nQuestion: ${question}`;
  return { system, user };
}

export function ReasonLab({ pack, state, update, env }: LabProps) {
  const task = pack.reason.find((t) => t.id === state.mode) ?? pack.reason[0];
  const [question, setQuestion] = useState(state.q ?? task.q);
  const [source, setSource] = useState<"reference" | "model">("reference");
  const [runs, setRuns] = useState<StepRun[]>([]);
  const [planNote, setPlanNote] = useState<string | null>(null);
  const [answer, setAnswer] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [edit, setEdit] = useState<number | null>(null);
  const provider = state.models?.[0];

  useEffect(() => {
    setQuestion(state.q ?? task.q);
    setRuns([]);
    setAnswer(null);
    setPlanNote(null);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pack.id, task.id]);

  const execute = async (plan: ReasonStep[], from = 0, prev: StepRun[] = []) => {
    const out: StepRun[] = [...prev.slice(0, from), ...plan.slice(from).map((step) => ({ step, status: "pending" as const }))];
    setRuns([...out]);
    setAnswer(null);
    for (let i = from; i < out.length; i++) {
      out[i] = { ...out[i], status: "running" };
      setRuns([...out]);
      const t0 = performance.now();
      try {
        const st = out[i].step;
        if (st.kind === "sql") {
          const problems = checkSql(st.sql, pack.bucket);
          if (problems.length) throw new Error(problems.join("; "));
          const r = await api.sql(st.sql);
          out[i] = { ...out[i], status: "ok", rows: r.rows.map((x) => ({ id: x.id, ...flatFields(x.fields) })), ms: Math.round(performance.now() - t0) };
        } else {
          const r = await api.searchEx({ query: st.query, top_k: Math.min(10, st.k || 5), bucket: pack.bucket, hybrid: true });
          out[i] = {
            ...out[i],
            status: "ok",
            rows: r.hits.map((h) => ({ id: h.id, text: h.text || pack.docs.find((d) => d.id === h.id)?.text || "" })),
            ms: Math.round(performance.now() - t0),
          };
        }
      } catch (e) {
        out[i] = { ...out[i], status: "error", error: (e as Error).message, ms: Math.round(performance.now() - t0) };
      }
      setRuns([...out]);
    }
    // Final answer from the step outputs only.
    const evidence = out.map(stepOutput);
    const r = await api.aiChat({
      provider,
      temperature: 0.1,
      max_tokens: 500,
      system: `Answer the question using ONLY the step outputs below. Quote numbers exactly as they appear. After each sentence cite the step it uses, like [S1]. If the outputs don't answer it, say what is missing. ${pack.system}`,
      message: `Step outputs:\n${evidence.join("\n\n")}\n\nQuestion: ${question}`,
    });
    setAnswer(r.answer);
    return out;
  };

  const run = async () => {
    setBusy(true);
    setErr(null);
    setPlanNote(null);
    try {
      update({ q: question, mode: task.id });
      let plan: ReasonStep[] = question === task.q ? task.plan : [];
      if (source === "model" || !plan.length) {
        const p = planPrompt(pack, question);
        const r = await api.aiChat({ provider, temperature: 0, max_tokens: 700, system: p.system, message: p.user });
        try {
          const parsed = extractJson(r.answer) as { steps?: ReasonStep[] };
          const steps = (parsed.steps ?? []).filter((s) => s && (s.kind === "sql" || s.kind === "search")).slice(0, 5);
          if (!steps.length) throw new Error("the plan had no usable steps");
          plan = steps;
          setPlanNote(`Plan written by ${r.model} (${r.provider}). Invalid SQL is shown as a failed step rather than silently fixed.`);
        } catch (e) {
          if (!task.plan.length || question !== task.q) throw new Error(`The model's plan couldn't be used (${(e as Error).message}). Try the reference plan.`);
          plan = task.plan;
          setPlanNote(`The model's plan couldn't be parsed (${(e as Error).message}), so the pack's reference plan ran instead.`);
        }
      } else {
        setPlanNote("Reference plan written for this pack by hand. Switch to “Model plans” to let the LLM write its own.");
      }
      await execute(plan);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const evidence = runs.map(stepOutput);
  const bad = answer ? unsupportedNumbers(answer, evidence, question) : [];

  return (
    <div className="space-y-4">
      <div className="card space-y-3">
        <div className="flex flex-wrap gap-1.5">
          {pack.reason.map((t) => (
            <button
              key={t.id}
              onClick={() => update({ mode: t.id, q: t.q })}
              className={`rounded-full border px-2.5 py-0.5 text-[11px] ${t.id === task.id ? "border-accent text-accent" : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent"}`}
            >
              {t.q.length > 80 ? t.q.slice(0, 78) + "…" : t.q}
            </button>
          ))}
        </div>
        <textarea className="input min-h-[3.5rem]" value={question} onChange={(e) => setQuestion(e.target.value)} aria-label="Question" />
        <div className="flex flex-wrap items-center gap-3">
          <div className="inline-flex rounded-lg border border-gray-200 dark:border-edge p-0.5 text-xs">
            {(["reference", "model"] as const).map((s) => (
              <button key={s} onClick={() => setSource(s)} className={`rounded-md px-2.5 py-1 ${source === s ? "bg-black text-white dark:bg-ink dark:text-black" : "text-gray-600 dark:text-muted"}`}>
                {s === "reference" ? "Reference plan" : "Model plans"}
              </button>
            ))}
          </div>
          <ModelPicker env={env} value={provider} onChange={(p) => update({ models: [p] })} />
          <button className="btn" onClick={run} disabled={busy || !question.trim()}>
            {busy ? "Running…" : "Plan and run"}
          </button>
        </div>
        {env?.mockLlmOnly && source === "model" && (
          <Banner kind="warn">Only the mock LLM is configured, so “Model plans” will fall back to the reference plan.</Banner>
        )}
      </div>
      <ErrorBanner err={err} />
      {planNote && <p className="text-xs text-gray-500 dark:text-muted">{planNote}</p>}

      {runs.length > 0 && (
        <ol className="space-y-3">
          {runs.map((r, i) => (
            <li key={i} className="card !p-4 space-y-2">
              <div className="flex flex-wrap items-center gap-2 text-xs">
                <span className={`dot ${r.status === "ok" ? "dot-ok" : r.status === "error" ? "dot-bad" : r.status === "running" ? "dot-warn animate-pulseok" : "dot-idle"}`} />
                <span className="font-mono text-gray-400 dark:text-faint">S{i + 1}</span>
                <span className="font-medium text-black dark:text-ink">{r.step.kind === "sql" ? "SQL" : "Search"}</span>
                <span className="text-gray-500 dark:text-muted">{r.step.why}</span>
                <span className="ml-auto font-mono text-[11px] text-gray-500 dark:text-muted">
                  {r.rows ? `${r.rows.length} rows · ` : ""}
                  {r.ms !== undefined ? `${r.ms} ms` : ""}
                </span>
                <button className="text-[11px] text-accent hover:underline" onClick={() => setEdit(edit === i ? null : i)} disabled={busy}>
                  {edit === i ? "Cancel" : "Edit"}
                </button>
              </div>
              {edit === i ? (
                <StepEditor
                  step={r.step}
                  onRun={(s) => {
                    setEdit(null);
                    setBusy(true);
                    const plan = runs.map((x, j) => (j === i ? s : x.step));
                    void execute(plan, i, runs)
                      .catch((e) => setErr((e as Error).message))
                      .finally(() => setBusy(false));
                  }}
                />
              ) : (
                <pre className="rounded-md bg-gray-50 dark:bg-carbon-950 p-2 text-[11px] font-mono whitespace-pre-wrap text-gray-700 dark:text-ink">
                  {r.step.kind === "sql" ? r.step.sql : `hybrid search: "${r.step.query}" (top ${r.step.k})`}
                </pre>
              )}
              {r.error && <p className="text-xs text-bad">{r.error}</p>}
              {r.rows && r.rows.length > 0 && <Rows rows={r.rows} />}
            </li>
          ))}
        </ol>
      )}

      {answer !== null && (
        <div className="card space-y-2">
          <div className="eyebrow">Answer from the step outputs</div>
          <p className="whitespace-pre-wrap text-sm text-black dark:text-ink">{answer}</p>
          <p className={`text-xs ${bad.length ? "text-bad" : "text-ok"}`}>
            {bad.length
              ? `Numbers not found in any step output: ${bad.join(", ")} — treat them as invented.`
              : "Every number in the answer appears in a step output or the question."}
          </p>
        </div>
      )}
    </div>
  );
}

function StepEditor({ step, onRun }: { step: ReasonStep; onRun: (s: ReasonStep) => void }) {
  const [text, setText] = useState(step.kind === "sql" ? step.sql : step.query);
  return (
    <div className="space-y-2">
      <textarea className="code-input min-h-[5rem] text-[12px]" value={text} onChange={(e) => setText(e.target.value)} />
      <button className="btn-secondary !py-1" onClick={() => onRun(step.kind === "sql" ? { ...step, sql: text } : { ...step, query: text })}>
        Re-run from this step
      </button>
    </div>
  );
}

function Rows({ rows }: { rows: Record<string, unknown>[] }) {
  const cols = [...new Set(rows.slice(0, 20).flatMap((r) => Object.keys(r)))].filter((c) => c !== "seed_version").slice(0, 8);
  return (
    <div className="overflow-x-auto max-h-64">
      <table className="text-[11px] font-mono">
        <thead>
          <tr className="text-left text-gray-500 dark:text-muted">
            {cols.map((c) => (
              <th key={c} className="pr-3 py-0.5">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 20).map((r, i) => (
            <tr key={i} className="border-t border-gray-100 dark:border-edge align-top">
              {cols.map((c) => (
                <td key={c} className="pr-3 py-0.5 text-gray-700 dark:text-ink max-w-[28rem] truncate" title={String(r[c] ?? "")}>
                  {Array.isArray(r[c]) ? (r[c] as unknown[]).join(", ") : String(r[c] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 20 && <p className="text-[11px] text-gray-500 dark:text-muted">…and {rows.length - 20} more rows</p>}
    </div>
  );
}

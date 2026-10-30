/**
 * Policy Desk — ask HR / security / IT / finance policy questions;
 * NebulaDB retrieves the relevant handbook clauses; chat answers with citations.
 */
import { useCallback, useEffect, useMemo, useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner, Stat } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ensureSeeded } from "../rag/seed";
import { field, lit, minMax, runSql, semanticText, similarity } from "../rag/sql";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import {
  DEPARTMENTS,
  POLICIES,
  POLICY_BUCKET,
  SAMPLE_QUESTIONS,
  SEED_VERSION,
  type Department,
} from "../rag/data/policy";
import { ArchitecturePanel } from "../components/RagArchitecture";

interface Hit {
  id: string;
  title: string;
  department: string;
  owner: string;
  sim: number;
  text: string;
}

function compact(s: string, n = 1400) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

export function PolicyDeskTab() {
  const [seedMsg, setSeedMsg] = useState("Checking policy corpus…");
  const [seeded, setSeeded] = useState(false);
  const [question, setQuestion] = useState(SAMPLE_QUESTIONS[0].q);
  const [dept, setDept] = useState<Department | "">("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<Hit[]>([]);
  const [runId, setRunId] = useState(0);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await ensureSeeded(POLICY_BUCKET, POLICIES, SEED_VERSION, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding policies ${d}/${t}…`);
        });
        if (cancelled) return;
        setSeeded(true);
        setSeedMsg(r.written > 0 ? `Seeded ${r.written} policies.` : `${r.total} policies ready.`);
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const search = async () => {
    const q = question.trim();
    if (!q) return;
    setErr(null);
    setBusy(true);
    const sq = lit(semanticText(q));
    let sql = `SELECT * FROM ${POLICY_BUCKET}\nWHERE semantic_match(text, ${sq})`;
    if (dept) sql += `\n  AND department = ${lit(dept)}`;
    sql += `\nLIMIT 12`;
    const trace: PipelineStep[] = [
      { label: "Policy retrieval (SQL + vector)", detail: sql, status: "running" },
    ];
    setSteps([...trace]);
    try {
      const r = await runSql(sql);
      const rows: Hit[] = r.rows
        .map((row) => ({
          id: String(field(row, "policy_id") ?? row.id),
          title: String(field(row, "title") ?? ""),
          department: String(field(row, "department") ?? ""),
          owner: String(field(row, "owner") ?? ""),
          sim: similarity(row.score),
          text: String(field(row, "text") ?? ""),
        }))
        .sort((a, b) => b.sim - a.sim);
      trace[0] = { ...trace[0], rowsOut: rows.length, ms: r.tookMs, status: "ok" };
      setSteps([...trace]);
      setHits(rows);
      setRunId((n) => n + 1);
    } catch (e) {
      const msg = e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message;
      setErr(msg);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const norm = useMemo(() => minMax(hits.map((h) => h.sim)), [hits]);

  const getContext = useCallback(async (): Promise<ContextDoc[]> => {
    return hits.slice(0, 6).map((h) => ({
      id: h.id,
      label: h.id,
      bucket: POLICY_BUCKET,
      text: compact(h.text),
      metadata: { department: h.department, title: h.title },
    }));
  }, [hits]);

  const system = useMemo(
    () =>
      `You are Northline's internal policy advisor. Answer ONLY from the cited policy excerpts. ` +
      `If the policies do not cover the question, say what is missing and who to ask (People Ops, Security, Finance, Legal). ` +
      `Cite clauses as [n]. This is not legal advice — flag when Legal must review.`,
    []
  );

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="policy" />
      <Panel
        title="Policy Desk"
        subtitle="Employee handbook, security and finance policies — semantic retrieval with optional department filter, then grounded Q&A"
        action={
          <span className="metachip">
            <span
              className={`dot ${seeded ? "dot-ok" : seedMsg.startsWith("Seeding failed") ? "dot-bad" : "dot-warn animate-pulseok"}`}
            />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-1.5 mb-3">
          <span className="eyebrow self-center mr-1">Scenarios</span>
          {SAMPLE_QUESTIONS.map((s) => (
            <button
              key={s.label}
              type="button"
              onClick={() => {
                setQuestion(s.q);
                setDept(s.dept ?? "");
              }}
              className={`rounded-full border px-2.5 py-1 text-xs transition-colors ${
                question === s.q
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent/60"
              }`}
            >
              {s.label}
            </button>
          ))}
        </div>
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">Question</span>
          <textarea
            className="code-input w-full h-24"
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            placeholder="Ask about remote work, expenses, lost laptops…"
          />
        </label>
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Department</span>
            <select
              className="code-input !py-1.5 !text-sm"
              value={dept}
              onChange={(e) => setDept(e.target.value as Department | "")}
            >
              <option value="">any</option>
              {DEPARTMENTS.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </label>
          <button className="btn" disabled={busy || !seeded || !question.trim()} onClick={() => void search()}>
            {busy ? <Spinner label="searching…" /> : "Find policies"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {runId > 0 && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
          <Panel title="Matched policies" subtitle={`${hits.length} clauses · ranked by semantic similarity`}>
            {hits.length === 0 ? (
              <p className="text-sm text-gray-500 dark:text-muted">No hits — widen the department filter.</p>
            ) : (
              <ul className="space-y-2">
                {hits.map((h) => (
                  <li key={h.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
                    <div className="flex items-center justify-between gap-2">
                      <div>
                        <div className="text-sm font-medium">
                          {h.id} · {h.title}
                        </div>
                        <div className="text-xs text-gray-500 dark:text-muted">
                          {h.department} · {h.owner}
                        </div>
                      </div>
                      <Stat label="sim" value={`${Math.round(norm(h.sim) * 100)}%`} />
                    </div>
                    <p className="mt-2 text-xs text-gray-600 dark:text-muted line-clamp-3">{h.text}</p>
                  </li>
                ))}
              </ul>
            )}
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </Panel>
          <GroundedChat
            title="Policy advisor"
            subtitle="Answers only from the matched clauses"
            system={system}
            getContext={getContext}
            resetKey={String(runId)}
            disabledReason={hits.length === 0 ? "Run a search with at least one hit first." : undefined}
            suggestions={[
              "Summarise the approval steps I need.",
              "What happens if I miss the deadline?",
              "Who owns this policy?",
            ]}
            placeholder="Ask a follow-up about these policies…"
          />
        </div>
      )}
    </div>
  );
}

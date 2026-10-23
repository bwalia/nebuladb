/**
 * Onboarding Concierge — day-one checklist + role-filtered handbook RAG
 * for new hires (and managers).
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
  DAY_ONE,
  DOCS,
  ONBOARD_BUCKET,
  ROLES,
  SAMPLE_QUESTIONS,
  SEED_VERSION,
  type Role,
} from "../rag/data/onboarding";

interface Hit {
  id: string;
  title: string;
  role: string;
  sim: number;
  text: string;
}

function compact(s: string, n = 1400) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

export function OnboardingTab() {
  const [seedMsg, setSeedMsg] = useState("Checking onboarding corpus…");
  const [seeded, setSeeded] = useState(false);
  const [role, setRole] = useState<Role>("engineering");
  const [question, setQuestion] = useState(SAMPLE_QUESTIONS[2].q);
  const [done, setDone] = useState<Record<string, boolean>>({});
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<Hit[]>([]);
  const [runId, setRunId] = useState(0);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await ensureSeeded(ONBOARD_BUCKET, DOCS, SEED_VERSION, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding handbook ${d}/${t}…`);
        });
        if (cancelled) return;
        setSeeded(true);
        setSeedMsg(r.written > 0 ? `Seeded ${r.written} docs.` : `${r.total} onboarding docs ready.`);
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const checklist = useMemo(
    () => DAY_ONE.filter((c) => !c.role || c.role === role || c.role === "all"),
    [role]
  );

  const search = async () => {
    const q = question.trim();
    if (!q) return;
    setErr(null);
    setBusy(true);
    const sq = lit(semanticText(q));
    // Role-specific + company-wide ("all"). Use IN — OR in WHERE is
    // unsupported (ANN has no well-defined disjunction).
    const sql =
      `SELECT * FROM ${ONBOARD_BUCKET}\nWHERE semantic_match(text, ${sq})\n` +
      `  AND role IN (${lit(role)}, ${lit("all")})\nLIMIT 10`;
    const trace: PipelineStep[] = [
      { label: "Handbook retrieval", detail: sql, status: "running" },
    ];
    setSteps([...trace]);
    try {
      const r = await runSql(sql);
      const rows: Hit[] = r.rows
        .map((row) => ({
          id: String(field(row, "doc_id") ?? row.id),
          title: String(field(row, "title") ?? ""),
          role: String(field(row, "role") ?? ""),
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
  const progress = checklist.filter((c) => done[c.id]).length;

  const getContext = useCallback(async (): Promise<ContextDoc[]> => {
    return hits.slice(0, 6).map((h) => ({
      id: h.id,
      label: h.id,
      bucket: ONBOARD_BUCKET,
      text: compact(h.text),
      metadata: { role: h.role, title: h.title },
    }));
  }, [hits]);

  const system = useMemo(
    () =>
      `You are Northline's onboarding concierge for a new ${role} hire. ` +
      `Answer from the handbook excerpts only; cite [n]. Be concrete about tools, SLAs and who to ping. ` +
      `If something is missing, say which team owns it (#help-it, People Ops, Sales Ops).`,
    [role]
  );

  return (
    <div className="space-y-4">
      <Panel
        title="Onboarding Concierge"
        subtitle="Day-one checklist plus role-filtered handbook RAG — same pattern enterprises use for new-hire copilots"
        action={
          <span className="metachip">
            <span
              className={`dot ${seeded ? "dot-ok" : seedMsg.startsWith("Seeding failed") ? "dot-bad" : "dot-warn animate-pulseok"}`}
            />
            {seedMsg}
          </span>
        }
      >
        <div className="flex flex-wrap gap-3 items-end mb-3">
          <label className="text-xs">
            <span className="block font-medium mb-1">Your role</span>
            <select
              className="code-input !py-1.5 !text-sm"
              value={role}
              onChange={(e) => setRole(e.target.value as Role)}
            >
              {ROLES.filter((r) => r !== "all").map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </label>
          <Stat label="checklist" value={`${progress}/${checklist.length}`} />
        </div>
        <ul className="grid gap-1.5 sm:grid-cols-2 mb-4">
          {checklist.map((c) => (
            <li key={c.id}>
              <label className="flex items-start gap-2 rounded-md border border-gray-200 dark:border-edge px-2.5 py-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  className="mt-0.5"
                  checked={!!done[c.id]}
                  onChange={(e) => setDone((d) => ({ ...d, [c.id]: e.target.checked }))}
                />
                <span>
                  <span className="font-medium">{c.title}</span>
                  <span className="block text-xs text-gray-500 dark:text-muted">{c.id}</span>
                </span>
              </label>
            </li>
          ))}
        </ul>
        <div className="flex flex-wrap gap-1.5 mb-3">
          <span className="eyebrow self-center mr-1">Ask</span>
          {SAMPLE_QUESTIONS.filter((s) => !s.role || s.role === role).map((s) => (
            <button
              key={s.label}
              type="button"
              onClick={() => setQuestion(s.q)}
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
            className="code-input w-full h-20"
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
          />
        </label>
        <button className="btn" disabled={busy || !seeded || !question.trim()} onClick={() => void search()}>
          {busy ? <Spinner label="searching…" /> : "Search handbook"}
        </button>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {runId > 0 && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
          <Panel title="Handbook hits" subtitle={`Filtered to ${role} + company-wide`}>
            <ul className="space-y-2">
              {hits.map((h) => (
                <li key={h.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
                  <div className="flex justify-between gap-2">
                    <div>
                      <div className="text-sm font-medium">
                        {h.id} · {h.title}
                      </div>
                      <div className="text-xs text-gray-500 dark:text-muted">audience: {h.role}</div>
                    </div>
                    <Stat label="sim" value={`${Math.round(norm(h.sim) * 100)}%`} />
                  </div>
                </li>
              ))}
            </ul>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </Panel>
          <GroundedChat
            title="Ask the concierge"
            subtitle="Grounded on the handbook hits for your role"
            system={system}
            getContext={getContext}
            resetKey={`${role}-${runId}`}
            disabledReason={hits.length === 0 ? "Search the handbook first." : undefined}
            suggestions={["What should I do before lunch on day one?", "Who do I ping if Okta fails?"]}
            placeholder="Ask about setup, benefits, norms…"
          />
        </div>
      )}
    </div>
  );
}

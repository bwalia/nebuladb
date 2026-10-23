/**
 * RFP Responder — pick a buyer question, retrieve capability sheets + past
 * proposal excerpts, draft a cited answer for the response matrix.
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
  CAPABILITIES,
  CAPABILITY_BUCKET,
  PROPOSALS,
  PROPOSAL_BUCKET,
  RFP_QUESTIONS,
  SEED_VERSION,
  SEGMENTS,
  type Segment,
} from "../rag/data/rfp";

interface Hit {
  id: string;
  kind: string;
  title: string;
  segment: string;
  outcome?: string;
  customer?: string;
  sim: number;
  text: string;
  bucket: string;
}

function compact(s: string, n = 1400) {
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

export function RfpResponderTab() {
  const [seedMsg, setSeedMsg] = useState("Checking RFP corpus…");
  const [seeded, setSeeded] = useState(false);
  const [qid, setQid] = useState(RFP_QUESTIONS[0].id);
  const [segment, setSegment] = useState<Segment | "">("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [hits, setHits] = useState<Hit[]>([]);
  const [runId, setRunId] = useState(0);

  const question = RFP_QUESTIONS.find((q) => q.id === qid) ?? RFP_QUESTIONS[0];

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const a = await ensureSeeded(CAPABILITY_BUCKET, CAPABILITIES, SEED_VERSION, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding capabilities ${d}/${t}…`);
        });
        const b = await ensureSeeded(PROPOSAL_BUCKET, PROPOSALS, SEED_VERSION, (d, t) => {
          if (!cancelled) setSeedMsg(`Seeding proposals ${d}/${t}…`);
        });
        if (cancelled) return;
        setSeeded(true);
        setSeedMsg(
          a.written + b.written > 0
            ? `Seeded ${a.written} capabilities + ${b.written} proposals.`
            : `${a.total} capabilities + ${b.total} past proposals ready.`
        );
      } catch (e) {
        if (!cancelled) setSeedMsg(`Seeding failed: ${(e as Error).message}`);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const retrieve = async () => {
    setErr(null);
    setBusy(true);
    const q = lit(semanticText(question.text));
    const seg = segment || question.segment;
    const capSql =
      `SELECT * FROM ${CAPABILITY_BUCKET}\nWHERE semantic_match(text, ${q})` +
      (seg ? `\n  AND segment = ${lit(seg)}` : "") +
      `\nLIMIT 8`;
    const propSql =
      `SELECT * FROM ${PROPOSAL_BUCKET}\nWHERE semantic_match(text, ${q})` +
      (seg ? `\n  AND segment = ${lit(seg)}` : "") +
      `\nLIMIT 8`;
    const trace: PipelineStep[] = [
      { label: "Capability sheets", detail: capSql, status: "running" },
    ];
    setSteps([...trace]);
    try {
      const caps = await runSql(capSql);
      trace[0] = { ...trace[0], rowsOut: caps.rows.length, ms: caps.tookMs, status: "ok" };
      trace.push({ label: "Past proposal excerpts", detail: propSql, status: "running" });
      setSteps([...trace]);
      const props = await runSql(propSql);
      trace[1] = { ...trace[1], rowsOut: props.rows.length, ms: props.tookMs, status: "ok" };
      setSteps([...trace]);

      const mapped: Hit[] = [
        ...caps.rows.map((row) => ({
          id: String(field(row, "doc_id") ?? row.id),
          kind: "capability",
          title: String(field(row, "title") ?? ""),
          segment: String(field(row, "segment") ?? ""),
          sim: similarity(row.score),
          text: String(field(row, "text") ?? ""),
          bucket: CAPABILITY_BUCKET,
        })),
        ...props.rows.map((row) => ({
          id: String(field(row, "doc_id") ?? row.id),
          kind: "proposal",
          title: String(field(row, "title") ?? ""),
          segment: String(field(row, "segment") ?? ""),
          outcome: String(field(row, "outcome") ?? ""),
          customer: String(field(row, "customer") ?? ""),
          sim: similarity(row.score),
          text: String(field(row, "text") ?? ""),
          bucket: PROPOSAL_BUCKET,
        })),
      ].sort((a, b) => b.sim - a.sim);
      setHits(mapped);
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
    const docs: ContextDoc[] = [
      {
        id: "rfp-q",
        label: question.id,
        text: `RFP QUESTION ${question.id} (${question.title}):\n${question.text}`,
      },
    ];
    for (const h of hits.slice(0, 8)) {
      docs.push({
        id: h.id,
        label: h.id,
        bucket: h.bucket,
        text: compact(h.text),
        metadata: { kind: h.kind, outcome: h.outcome },
      });
    }
    return docs;
  }, [hits, question]);

  const system = useMemo(
    () =>
      `You are a proposal writer for NebulaStack. Draft a concise RFP matrix answer (150–250 words) ` +
      `using ONLY the provided capability sheets and past proposal notes. Cite sources as [n]. ` +
      `Call out gaps honestly (e.g. lost deals on CMK). Do not invent certifications or customers. ` +
      `End with a one-line "Evidence" list of the doc ids you relied on.`,
    []
  );

  return (
    <div className="space-y-4">
      <Panel
        title="RFP Responder"
        subtitle="Buyer question → capability sheets + past wins/losses → cited draft for the response matrix"
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
          {RFP_QUESTIONS.map((q) => (
            <button
              key={q.id}
              type="button"
              onClick={() => {
                setQid(q.id);
                setSegment(q.segment);
              }}
              className={`rounded-full border px-2.5 py-1 text-xs transition-colors ${
                qid === q.id
                  ? "border-accent text-accent bg-accent/10"
                  : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted hover:border-accent/60"
              }`}
            >
              {q.id} · {q.title}
            </button>
          ))}
        </div>
        <div className="rounded-md border border-gray-200 dark:border-edge bg-gray-50 dark:bg-panel/40 px-3 py-2 text-sm mb-3">
          <div className="text-xs uppercase tracking-wide text-gray-500 dark:text-muted mb-1">
            {question.id} · {question.segment}
          </div>
          {question.text}
        </div>
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Segment filter</span>
            <select
              className="code-input !py-1.5 !text-sm"
              value={segment}
              onChange={(e) => setSegment(e.target.value as Segment | "")}
            >
              <option value="">any</option>
              {SEGMENTS.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
          <button className="btn" disabled={busy || !seeded} onClick={() => void retrieve()}>
            {busy ? <Spinner label="retrieving…" /> : "Retrieve evidence"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {runId > 0 && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
          <Panel title="Evidence" subtitle="Capabilities and prior proposal language">
            <ul className="space-y-2">
              {hits.map((h) => (
                <li key={`${h.bucket}-${h.id}`} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
                  <div className="flex justify-between gap-2">
                    <div>
                      <div className="text-sm font-medium">
                        {h.id}{" "}
                        <span className="text-xs font-normal text-gray-500">
                          · {h.kind}
                          {h.outcome ? ` · ${h.outcome}` : ""}
                          {h.customer ? ` · ${h.customer}` : ""}
                        </span>
                      </div>
                      <div className="text-xs text-gray-500 dark:text-muted">{h.title}</div>
                    </div>
                    <Stat label="sim" value={`${Math.round(norm(h.sim) * 100)}%`} />
                  </div>
                </li>
              ))}
            </ul>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </Panel>
          <GroundedChat
            title="Draft response"
            subtitle="Proposal language grounded on the evidence set"
            system={system}
            getContext={getContext}
            resetKey={`${qid}-${runId}`}
            disabledReason={hits.length === 0 ? "Retrieve evidence first." : undefined}
            suggestions={[
              "Draft the matrix answer.",
              "List residual risks we should disclose.",
              "What did we lose on last time in this segment?",
            ]}
            placeholder="Ask to draft or refine the RFP answer…"
          />
        </div>
      )}
    </div>
  );
}

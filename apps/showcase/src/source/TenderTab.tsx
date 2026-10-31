/**
 * App F — Tender and Questionnaire Responder (the RfpResponderTab idea,
 * one query per buyer question).
 *
 * Per question: semantic_match over ss_tender_kb with sector IN (sector,
 * 'all') and, optionally, won = TRUE. A coverage score (topic match +
 * words in common: deterministic, so it means the same with the mock
 * embedder) marks weak questions "needs a human". Strong ones get a
 * cited draft within a word limit; everything exports as Markdown.
 */
import { useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { Markdown } from "../rag/Markdown";
import type { ContextDoc } from "../rag/GroundedChat";
import { lit, runSql, semanticText, similarity } from "../rag/sql";
import { B, exhaustiveLimit } from "./data/common";
import { TENDER_DOCS, TENDER_SECTORS, TENDER_SECTOR_LABEL, type TenderSector, type Topic } from "./data/tender";
import { Chips, DemoNote, SeedChip, draftAnswer, metaOf, overlap, str, useSeeds } from "./common";

const KB_LIMIT = exhaustiveLimit(TENDER_DOCS.length);
const NEEDS_HUMAN = 0.5;

const SETS: Array<{ label: string; sector: TenderSector; questions: string }> = [
  {
    label: "Academy trust tender",
    sector: "education",
    questions: [
      "Describe how you will help the trust reduce carbon emissions from cleaning products and deliveries.",
      "What health and safety accreditations do you hold, and how do you support COSHH on our sites?",
      "Describe your delivery service levels and the ways schools can place orders.",
      "What social value will you deliver to the local community?",
    ].join("\n"),
  },
  {
    label: "NHS trust questionnaire",
    sector: "healthcare",
    questions: [
      "Describe your quality management system and how complaints are handled.",
      "How do you make safety data sheets available and support COSHH assessments?",
      "What product training do you offer to our domestic staff?",
      "Describe your environmental management and carbon reduction plan.",
    ].join("\n"),
  },
  {
    label: "Council framework",
    sector: "public",
    questions: [
      "Explain your pricing model and how you control cost over the contract.",
      "What social value commitments will you make?",
      "Describe how you deliver to multiple council sites.",
      "How will you help the council meet its net zero target?",
    ].join("\n"),
  },
  {
    label: "University tender",
    sector: "higher-education",
    questions: [
      "Describe your sustainability credentials, including any certifications.",
      "Describe your washroom hygiene services and how you guarantee visits happen.",
      "What training do you provide for new cleaning staff across many buildings?",
      "Do you hold Cyber Essentials Plus, and how do you protect our data?",
    ].join("\n"),
  },
];

const TOPIC_RE: Array<[Topic, RegExp]> = [
  ["sustainability", /\b(carbon|co2|emissions?|environment\w*|sustainab\w*|green\w*|net zero|eco)\b/i],
  ["health-safety", /\b(health and safety|h&s|coshh|safety data|45001|safe)\b/i],
  ["quality", /\b(quality|9001|complaints?|account manag\w*)\b/i],
  ["delivery", /\b(deliver\w*|orders?|ordering|service levels?|lead times?|sites)\b/i],
  ["social-value", /\b(social value|community|local)\b/i],
  ["pricing", /\b(pric\w*|costs?|rebates?|value for money)\b/i],
  ["training", /\b(train\w*|induction)\b/i],
  ["washroom", /\b(washroom|sanitary|hygiene services?|nappy)\b/i],
];

function topicsOf(q: string): Topic[] {
  return TOPIC_RE.filter(([, re]) => re.test(q)).map(([t]) => t);
}

interface Evidence {
  id: string;
  kbId: string;
  title: string;
  kind: string;
  topic: string;
  won?: boolean;
  text: string;
  sim: number;
  score: number;
}

interface QA {
  q: string;
  topics: Topic[];
  evidence: Evidence[];
  coverage: number;
  answer: string;
  trimmed: boolean;
  drafting: boolean;
  error?: string;
}

function trimWords(s: string, n: number): { text: string; trimmed: boolean } {
  const parts = s.split(/(\s+)/);
  let count = 0;
  let out = "";
  for (const p of parts) {
    if (/\S/.test(p)) count++;
    if (count > n) return { text: out.trimEnd() + " …", trimmed: true };
    out += p;
  }
  return { text: out, trimmed: false };
}

function wordCount(s: string): number {
  return s.split(/\s+/).filter(Boolean).length;
}

function system(limit: number): string {
  return (
    "You are a bid writer for Source Supplies, a UK B2B supplier of sustainable cleaning and hygiene products and washroom services. " +
    `Write a tender answer of at most ${limit} words using ONLY the knowledge-base entries provided. Cite each fact as [n]. ` +
    "Do not invent certifications, figures, customers or commitments. Keep any '(demo figure)' marker next to a number. " +
    "If the entries do not answer part of the question, say that part needs input from the bid team. Use UK English, plain words and short sentences."
  );
}

export function TenderTab() {
  const seed = useSeeds(["tender"]);
  const [setLabel, setSetLabel] = useState(SETS[0].label);
  const [questions, setQuestions] = useState(SETS[0].questions);
  const [sector, setSector] = useState<TenderSector>(SETS[0].sector);
  const [wonOnly, setWonOnly] = useState(false);
  const [limit, setLimit] = useState(120);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [qas, setQas] = useState<QA[]>([]);

  const retrieve = async () => {
    const qs = questions.split("\n").map((s) => s.trim()).filter(Boolean).slice(0, 12);
    if (qs.length === 0) return;
    setErr(null);
    setBusy(true);
    const trace: PipelineStep[] = [];
    const out: QA[] = [];
    try {
      for (const q of qs) {
        const sql =
          `SELECT * FROM ss_tender_kb\n WHERE semantic_match(text, ${lit(semanticText(q))})` +
          `\n   AND sector IN (${[sector, "all"].map(lit).join(", ")})` +
          (wonOnly ? `\n   AND won = ${lit(true)}` : "") +
          `\n LIMIT ${KB_LIMIT}`;
        trace.push({ label: `Q${out.length + 1}: ${q.slice(0, 70)}${q.length > 70 ? "…" : ""}`, detail: sql, status: "running" });
        setSteps([...trace]);
        const r = await runSql(sql);
        const topics = topicsOf(q);
        const ev: Evidence[] = r.rows
          .map((row) => {
            const m = metaOf(row);
            const text = str(m.text);
            const sim = similarity(row.score);
            const topicHit = topics.includes(str(m.topic) as Topic) ? 1 : 0;
            return {
              id: row.id,
              kbId: str(m.kb_id),
              title: str(m.title),
              kind: str(m.kind),
              topic: str(m.topic),
              won: m.won === undefined ? undefined : m.won === true,
              text,
              sim,
              score: 0.5 * topicHit + 0.5 * Math.min(1, overlap(q, text) * 2) + 0.1 * sim,
            };
          })
          .sort((a, b) => b.score - a.score)
          .slice(0, 5);
        const coverage = Math.min(1, ev[0]?.score ?? 0);
        out.push({ q, topics, evidence: ev, coverage, answer: "", trimmed: false, drafting: false });
        trace[trace.length - 1] = { ...trace[trace.length - 1], rowsIn: r.rows.length, rowsOut: ev.length, ms: r.tookMs, status: "ok" };
        setSteps([...trace]);
      }
      trace.push({
        label: "Coverage per question (in the browser)",
        detail: `score = 0.5 × topic match + 0.5 × min(1, 2 × share of question words found) + 0.1 × similarity · below ${NEEDS_HUMAN} → "needs a human"`,
        status: "ok",
      });
      setSteps([...trace]);
      setQas(out);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const draftOne = async (i: number) => {
    const qa = qas[i];
    if (!qa) return;
    setQas((prev) => prev.map((x, j) => (j === i ? { ...x, drafting: true, error: undefined } : x)));
    try {
      const ctx: ContextDoc[] = qa.evidence.map((e) => ({ id: e.id, label: e.kbId, bucket: B.tender, text: e.text }));
      const raw = await draftAnswer(qa.q, ctx, system(limit));
      const { text, trimmed } = trimWords(raw, limit);
      setQas((prev) => prev.map((x, j) => (j === i ? { ...x, answer: text, trimmed, drafting: false } : x)));
    } catch (e) {
      setQas((prev) => prev.map((x, j) => (j === i ? { ...x, drafting: false, error: (e as Error).message } : x)));
    }
  };

  const draftAll = async () => {
    setBusy(true);
    for (let i = 0; i < qas.length; i++) if (qas[i].coverage >= NEEDS_HUMAN) await draftOne(i);
    setBusy(false);
  };

  const markdown = () =>
    `# Tender response draft — ${TENDER_SECTOR_LABEL[sector]}\n\n` +
    `_Demo data — not real Source Supplies records. Draft for the bid team to check._\n\n` +
    qas
      .map((qa, i) => {
        const cites = qa.evidence.map((e, n) => `[${n}] ${e.kbId} — ${e.title}`).join("\n");
        const body =
          qa.coverage < NEEDS_HUMAN && !qa.answer
            ? "**Needs a human:** the knowledge base has no strong answer for this."
            : qa.answer || "_Not drafted yet._";
        return `## Q${i + 1}. ${qa.q}\n\nCoverage: ${Math.round(qa.coverage * 100)}%${qa.coverage < NEEDS_HUMAN ? " (needs a human)" : ""}\n\n${body}\n\nSources:\n${cites}\n`;
      })
      .join("\n");

  const download = () => {
    const blob = new Blob([markdown()], { type: "text/markdown" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `tender-draft-${sector}.md`;
    a.click();
    URL.revokeObjectURL(a.href);
  };

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="ss_tender" />
      <DemoNote extra="Figures marked (demo figure) are invented." />
      <Panel title="Tender and Questionnaire Responder" subtitle="Paste the buyer's questions, one per line. Each gets its evidence, a coverage score and a cited draft." action={<SeedChip seed={seed} />}>
        <Chips
          items={SETS}
          active={setLabel}
          onPick={(s) => {
            setSetLabel(s.label);
            setQuestions(s.questions);
            setSector(s.sector);
            setQas([]);
          }}
        />
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">Buyer questions (one per line)</span>
          <textarea className="code-input w-full h-28" value={questions} onChange={(e) => setQuestions(e.target.value)} />
        </label>
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Buyer sector</span>
            <select className="code-input !py-1.5 !text-sm" value={sector} onChange={(e) => setSector(e.target.value as TenderSector)}>
              {TENDER_SECTORS.map((s) => (
                <option key={s} value={s}>
                  {TENDER_SECTOR_LABEL[s]}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs flex items-center gap-1.5 pb-2" title="won = TRUE only matches past answers, so capability statements drop out">
            <input type="checkbox" checked={wonOnly} onChange={(e) => setWonOnly(e.target.checked)} />
            Only past answers from won tenders
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Word limit per answer</span>
            <input type="number" min={30} max={500} className="code-input !py-1.5 !text-sm w-24" value={limit} onChange={(e) => setLimit(Math.max(30, Math.min(500, Number(e.target.value) || 120)))} />
          </label>
          <button className="btn" disabled={busy || !seed.ready || !questions.trim()} onClick={() => void retrieve()}>
            {busy && qas.length === 0 ? <Spinner label="finding evidence…" /> : "Find evidence"}
          </button>
          {qas.length > 0 && (
            <>
              <button className="btn-secondary" disabled={busy} onClick={() => void draftAll()}>
                {busy ? <Spinner label="drafting…" /> : "Draft strong answers"}
              </button>
              <button className="btn-secondary" onClick={download}>
                Export Markdown
              </button>
              <button className="btn-secondary" onClick={() => void navigator.clipboard?.writeText(markdown())}>
                Copy Markdown
              </button>
            </>
          )}
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {qas.map((qa, i) => (
        <Panel key={i} title={`Q${i + 1}. ${qa.q}`} subtitle={qa.topics.length ? `Topics: ${qa.topics.join(", ")}` : "No known topic"}>
          <div className="flex items-center gap-3">
            <div className="h-2 w-40 rounded bg-gray-100 dark:bg-carbon-800">
              <div className={`h-2 rounded ${qa.coverage >= NEEDS_HUMAN ? "bg-ok" : "bg-warn"}`} style={{ width: `${Math.round(qa.coverage * 100)}%` }} />
            </div>
            <span className="text-xs font-mono">{Math.round(qa.coverage * 100)}% coverage</span>
            {qa.coverage < NEEDS_HUMAN && <span className="rounded border border-warn/60 bg-warn/10 px-1.5 text-xs font-semibold text-warn">needs a human</span>}
            <button className="btn-secondary !py-1 !px-2 !text-xs ml-auto" disabled={qa.drafting} onClick={() => void draftOne(i)}>
              {qa.drafting ? <Spinner label="drafting…" /> : qa.answer ? "Redraft" : qa.coverage < NEEDS_HUMAN ? "Draft anyway" : "Draft"}
            </button>
          </div>
          <ol className="space-y-1">
            {qa.evidence.map((e, n) => (
              <li key={e.id} className="text-xs rounded border border-gray-200 dark:border-edge px-2 py-1">
                <span className="font-mono text-accent mr-1.5">[{n}]</span>
                <span className="font-medium">{e.kbId}</span> · {e.title}
                <span className="text-gray-500 dark:text-muted">
                  {" "}
                  · {e.kind} · {e.topic}
                  {e.won !== undefined ? (e.won ? " · won" : " · lost") : ""}
                </span>
              </li>
            ))}
          </ol>
          {qa.answer && (
            <div className="rounded-md border border-gray-200 dark:border-edge px-3 py-2 text-sm">
              <Markdown text={qa.answer} citeLabel={(n) => qa.evidence[n]?.kbId} />
              <div className="mt-1 text-[11px] font-mono text-gray-500 dark:text-muted">
                {wordCount(qa.answer)} / {limit} words{qa.trimmed ? " · trimmed to the limit" : ""}
              </div>
            </div>
          )}
          {qa.error && <p className="text-xs font-mono text-bad">{qa.error}</p>}
        </Panel>
      ))}
      <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
    </div>
  );
}

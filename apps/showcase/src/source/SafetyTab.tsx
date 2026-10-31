/**
 * App B — COSHH & Safety Data Assistant.
 *
 * Safety rules run in code before the model (see ./safety.ts):
 * emergencies get a fixed first-aid card, mixing questions get the fixed
 * "do not mix" answer with Section 10, and anything the SDS does not
 * cover gets "This is not in the safety data sheet." Only the rest is
 * answered by the model, grounded on the cited SDS sections and guides.
 */
import { useEffect, useRef, useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { Markdown } from "../rag/Markdown";
import { lit, runSql, semanticText } from "../rag/sql";
import type { SqlRow } from "../api";
import { sseStream } from "../sse";
import { exhaustiveLimit } from "./data/common";
import { SDS_DOCS, SDS_PROFILES } from "./data/sds";
import { GUIDE_DOCS } from "./data/guides";
import {
  COSHH_DRAFT_NOTE,
  EMERGENCY_LINE,
  NOT_IN_SDS,
  citeLabel,
  draftCoshh,
  firstAidCard,
  mixAnswer,
  planQuestion,
  sdsCovers,
  sectionBody,
  type CoshhDraft,
  type FirstAidCard,
  type MixAnswer,
  type Plan,
  type SdsSection,
} from "./safety";
import { Chips, DemoNote, SeedChip, compact, metaOf, num, str, useSeeds } from "./common";

const SDS_LIMIT = exhaustiveLimit(SDS_DOCS.length);
const GUIDES_LIMIT = exhaustiveLimit(GUIDE_DOCS.length);

const SCENARIOS = [
  { label: "PPE for the descaler", q: "Which PPE do cleaners need for DS-200?", sku: "DS-200" },
  { label: "Can I mix with bleach?", q: "Can I mix DS-200 with bleach to make it stronger?", sku: "DS-200" },
  { label: "Splashed in the eye", q: "A cleaner splashed MR-160 in her eye. What do we do?", sku: "MR-160" },
  { label: "Storing the degreaser", q: "How should we store KD-130 in the cleaners' cupboard?", sku: "KD-130" },
];
const ALSO = ["What does a case cost?", "Can I use it on marble?", "How do I deal with a spill?", "How do we dispose of empty containers?"];

const SYSTEM =
  "You are a safety assistant helping a cleaner or site manager understand a DEMO safety data sheet. " +
  "Answer ONLY from the SDS sections and guides provided. Every fact must cite its source as [n], and say which section it came from, e.g. 'Section 8: Exposure controls / PPE [0]'. " +
  `If the answer is not in the sections, reply exactly: "${NOT_IN_SDS}" and nothing else. ` +
  "Never guess about mixing products, exposure limits or medical treatment. Never advise mixing products. " +
  "Use short sentences, UK English and plain words. Remind the reader this is a demo SDS for a fictional product.";

interface Turn {
  q: string;
  sku: string;
  plan: Plan;
  card?: FirstAidCard;
  mix?: MixAnswer & { unknown: string[] };
  notInSds?: boolean;
  answer: string;
  context: Array<{ label: string; text: string }>;
  done: boolean;
  error?: string;
}

function toSection(r: SqlRow): SdsSection {
  const m = metaOf(r);
  return {
    id: r.id,
    sku: str(m.sku),
    product_name: str(m.product_name),
    section: num(m.section),
    section_title: str(m.section_title),
    text: str(m.text),
  };
}

export function SafetyTab() {
  const seed = useSeeds(["products", "sds", "guides"]);
  const [sku, setSku] = useState(SCENARIOS[0].sku);
  const [q, setQ] = useState(SCENARIOS[0].q);
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [turns, setTurns] = useState<Turn[]>([]);
  const [coshh, setCoshh] = useState<CoshhDraft | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const product = SDS_PROFILES.find((p) => p.sku === sku);
  useEffect(() => () => abortRef.current?.abort(), []);

  const ask = async (question: string) => {
    const text = question.trim();
    if (!text || busy) return;
    setErr(null);
    setBusy(true);
    const trace: PipelineStep[] = [];
    const push = (s: PipelineStep) => {
      trace.push(s);
      setSteps([...trace]);
      return trace.length - 1;
    };
    const done = (i: number, patch: Partial<PipelineStep>) => {
      trace[i] = { ...trace[i], ...patch, status: patch.status ?? "ok" };
      setSteps([...trace]);
    };
    const plan = planQuestion(text, sku);
    const idx = turns.length;
    setTurns((t) => [...t, { q: text, sku, plan, answer: "", context: [], done: false }]);
    const update = (mut: (t: Turn) => Turn) => setTurns((prev) => prev.map((t, i) => (i === idx ? mut(t) : t)));
    push({
      label: "Safety rules (code, before any model)",
      detail:
        plan.kind === "emergency"
          ? `emergency words found (${plan.route}) → fixed first-aid card from Section 4, no model`
          : plan.kind === "mix"
            ? `mixing question (${plan.skus.join(", ")}) → fixed answer + Section 10, no model`
            : `normal question → topic sections: ${plan.sections.length ? plan.sections.join(", ") : "none matched, search all"}`,
      status: "ok",
    });
    try {
      if (plan.kind === "emergency") {
        const sql =
          `SELECT * FROM ss_sds\n WHERE semantic_match(text, ${lit("first-aid measures")})\n   AND sku = ${lit(sku)}\n   AND section = ${lit(4)}\n LIMIT ${SDS_LIMIT}`;
        const i = push({ label: "Section 4 of this SDS", detail: sql, status: "running" });
        const r = await runSql(sql);
        done(i, { rowsOut: r.rows.length, ms: r.tookMs });
        const card = firstAidCard(plan.route, r.rows.map(toSection)[0]);
        update((t) => ({ ...t, card, done: true }));
        return;
      }
      if (plan.kind === "mix") {
        const sql =
          `SELECT * FROM ss_sds\n WHERE semantic_match(text, ${lit("stability and reactivity, do not mix")})` +
          `\n   AND sku IN (${plan.skus.map(lit).join(", ")})\n   AND section = ${lit(10)}\n LIMIT ${SDS_LIMIT}`;
        const i = push({ label: "Section 10 (stability and reactivity) for each product", detail: sql, status: "running" });
        const r = await runSql(sql);
        done(i, { rowsOut: r.rows.length, ms: r.tookMs });
        const secs = r.rows.map(toSection);
        const found = new Set(secs.map((s) => s.sku));
        update((t) => ({ ...t, mix: { ...mixAnswer(secs), unknown: plan.skus.filter((s) => !found.has(s)) }, done: true }));
        return;
      }

      // Normal lookup: SDS sections for this SKU (narrowed to the topic's sections when one matched).
      const sql =
        `SELECT * FROM ss_sds\n WHERE semantic_match(text, ${lit(semanticText(text))})\n   AND sku = ${lit(sku)}` +
        (plan.sections.length ? `\n   AND section IN (${plan.sections.map(lit).join(", ")})` : "") +
        `\n LIMIT ${SDS_LIMIT}`;
      let i = push({ label: "SDS sections for this product", detail: sql, status: "running" });
      const r = await runSql(sql);
      const secs = r.rows.map(toSection);
      done(i, { rowsOut: secs.length, ms: r.tookMs });
      const cover = sdsCovers(text, secs);
      i = push({
        label: "Is it in the SDS? (code check)",
        detail: cover.covered ? `yes → ${cover.sections.slice(0, 4).map(citeLabel).join("; ")}` : `no → "${NOT_IN_SDS}" and stop`,
        status: "ok",
      });
      if (!cover.covered) {
        update((t) => ({ ...t, notInSds: true, done: true }));
        return;
      }
      const gSql =
        `SELECT * FROM ss_guides\n WHERE semantic_match(text, ${lit(semanticText(`${text} ${product?.name ?? ""}`))})` +
        `\n   AND array_contains(skus, ${lit(sku)})\n LIMIT ${GUIDES_LIMIT}`;
      i = push({ label: "Safe-use steps from the guides", detail: gSql, status: "running" });
      const g = await runSql(gSql);
      done(i, { rowsOut: g.rows.length, ms: g.tookMs });

      const context = [
        ...cover.sections.slice(0, 4).map((s) => ({ id: s.id, label: citeLabel(s), bucket: "ss_sds", text: s.text })),
        ...g.rows.slice(0, 2).map((row) => {
          const m = metaOf(row);
          return { id: row.id, label: `Guide: ${str(m.title)}`, bucket: "ss_guides", text: compact(str(m.text), 900) };
        }),
      ];
      update((t) => ({ ...t, context: context.map((c) => ({ label: c.label, text: c.text })) }));
      i = push({ label: "Grounded answer (LLM over the cited sections)", detail: `POST /api/v1/ai/rag · ${context.length} context docs`, status: "running" });
      const started = performance.now();
      const ctrl = new AbortController();
      abortRef.current = ctrl;
      for await (const frame of sseStream(
        "/api/v1/ai/rag",
        { query: text, stream: true, system: SYSTEM, context: context.map((c) => ({ id: c.id, text: c.text, bucket: c.bucket, metadata: {} })) },
        ctrl.signal
      )) {
        if (frame.event === "answer_delta") update((t) => ({ ...t, answer: t.answer + frame.data }));
        else if (frame.event === "error") throw new Error(frame.data);
        else if (frame.event === "done") break;
      }
      done(i, { ms: Math.round(performance.now() - started) });
      update((t) => ({ ...t, done: true }));
    } catch (e) {
      const msg = e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message;
      setErr(msg);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
      update((t) => ({ ...t, done: true, error: msg }));
    } finally {
      setBusy(false);
    }
  };

  const buildCoshh = async () => {
    setErr(null);
    setBusy(true);
    const sql = `SELECT * FROM ss_sds\n WHERE semantic_match(text, ${lit("safety data sheet")})\n   AND sku = ${lit(sku)}\n LIMIT ${SDS_LIMIT}`;
    const trace: PipelineStep[] = [{ label: "All 16 SDS sections for the COSHH draft", detail: sql, status: "running" }];
    setSteps(trace);
    try {
      const r = await runSql(sql);
      setSteps([{ ...trace[0], rowsOut: r.rows.length, ms: r.tookMs, status: "ok" }]);
      setCoshh(draftCoshh(r.rows.map(toSection)));
    } catch (e) {
      setErr((e as Error).message);
      setSteps([{ ...trace[0], status: "error" }]);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="ss_safety" />
      <DemoNote extra="Safety data sheets here are for fictional products: never use them for real work." />
      <Panel
        title="COSHH & Safety Data Assistant"
        subtitle="Ask about a product's safety data sheet. Every answer says which section it came from."
        action={<SeedChip seed={seed} />}
      >
        <Chips
          items={SCENARIOS}
          active={scenario}
          onPick={(s) => {
            setScenario(s.label);
            setQ(s.q);
            setSku(s.sku);
            setCoshh(null);
          }}
        />
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Product</span>
            <select className="code-input !py-1.5 !text-sm" value={sku} onChange={(e) => { setSku(e.target.value); setCoshh(null); }}>
              {SDS_PROFILES.map((p) => (
                <option key={p.sku} value={p.sku}>
                  {p.name}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs flex-1 min-w-[16rem]">
            <span className="block font-medium mb-1">Your question</span>
            <input className="input w-full" value={q} onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === "Enter" && void ask(q)} />
          </label>
          <button className="btn" disabled={busy || !seed.ready || !q.trim()} onClick={() => void ask(q)}>
            {busy ? <Spinner label="checking…" /> : "Ask"}
          </button>
          <button className="btn-secondary" disabled={busy || !seed.ready} onClick={() => void buildCoshh()}>
            Draft COSHH assessment
          </button>
        </div>
        <div className="flex flex-wrap gap-1.5 mt-2">
          <span className="eyebrow self-center mr-1">Also try</span>
          {ALSO.map((a) => (
            <button
              key={a}
              type="button"
              disabled={busy || !seed.ready}
              onClick={() => {
                setQ(a);
                void ask(a);
              }}
              className="rounded-full border border-gray-200 dark:border-edge px-2.5 py-1 text-xs text-gray-600 dark:text-muted hover:border-accent/60"
            >
              {a}
            </button>
          ))}
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {coshh && (
        <Panel title={`Draft COSHH assessment · ${product?.name ?? sku}`} subtitle="Filled only from the cited SDS sections">
          <div className="rounded-md border border-warn/50 bg-warn/10 px-3 py-1.5 text-xs font-semibold text-warn mb-2">{coshh.note}</div>
          <table className="w-full text-sm">
            <tbody>
              {coshh.fields.map((f) => (
                <tr key={f.label} className="border-t border-gray-100 dark:border-edge align-top">
                  <td className="py-1.5 pr-3 font-medium w-40">{f.label}</td>
                  <td className="py-1.5 pr-3">{f.value}</td>
                  <td className="py-1.5 text-[11px] font-mono text-gray-500 dark:text-muted w-56">{f.cite}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <button
            className="btn-secondary !text-xs mt-2"
            onClick={() =>
              void navigator.clipboard?.writeText(
                `${COSHH_DRAFT_NOTE}\n${product?.name ?? sku}\n` + coshh.fields.map((f) => `${f.label}: ${f.value} (${f.cite})`).join("\n")
              )
            }
          >
            Copy draft
          </button>
        </Panel>
      )}

      {turns.length > 0 && (
        <Panel title="Answers" subtitle="Rule-based answers are fixed text. Model answers only use the cited sections.">
          <div className="space-y-4">
            {[...turns].reverse().map((t, i) => (
              <TurnView key={turns.length - i} t={t} />
            ))}
          </div>
        </Panel>
      )}
      <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
    </div>
  );
}

function TurnView({ t }: { t: Turn }) {
  return (
    <div className="space-y-2">
      <div className="flex justify-end">
        <div className="max-w-[85%] rounded-xl rounded-br-sm bg-gray-900 text-white dark:bg-carbon-800 dark:text-ink px-3 py-2 text-sm">
          <span className="font-mono text-[10px] opacity-70 mr-1.5">{t.sku}</span>
          {t.q}
        </div>
      </div>
      {t.card && (
        <div className="rounded-md border-2 border-bad/60 bg-red-50 dark:bg-bad/10 px-3 py-2 text-sm">
          <div className="font-semibold text-bad">First aid — {t.card.heading}</div>
          <p className="mt-1">{t.card.steps}</p>
          <p className="mt-1 font-semibold">{EMERGENCY_LINE}</p>
          <p className="mt-1 text-[11px] font-mono text-gray-500 dark:text-muted">From {t.card.cite} (demo SDS)</p>
        </div>
      )}
      {t.mix && (
        <div className="rounded-md border-2 border-warn/60 bg-warn/10 px-3 py-2 text-sm space-y-1.5">
          <div className="font-semibold">{t.mix.rule}</div>
          {t.mix.cites.map((c) => (
            <div key={c.id}>
              <div className="text-[11px] font-mono text-gray-500 dark:text-muted">{c.label}</div>
              <div>{c.text}</div>
            </div>
          ))}
          {t.mix.unknown.length > 0 && (
            <div className="text-xs">No demo SDS for {t.mix.unknown.join(", ")}, so it cannot be shown to be safe. Do not mix.</div>
          )}
        </div>
      )}
      {t.notInSds && <div className="rounded-md border border-gray-300 dark:border-edge px-3 py-2 text-sm font-medium">{NOT_IN_SDS}</div>}
      {t.plan.kind === "lookup" && !t.notInSds && (
        <div className="rounded-xl rounded-bl-sm border border-gray-200 dark:border-edge px-3 py-2.5 text-sm">
          {t.answer ? (
            <Markdown text={t.answer} citeLabel={(n) => t.context[n]?.label} />
          ) : !t.done ? (
            <Spinner label="reading the safety data sheet…" />
          ) : null}
          {t.context.length > 0 && (
            <ol className="mt-2 space-y-1">
              {t.context.map((c, n) => (
                <li key={n} className="rounded border border-gray-200 dark:border-edge px-2 py-1 text-xs">
                  <span className="font-mono text-accent mr-1.5">[{n}]</span>
                  <span className="font-medium">{c.label}</span>
                  <span className="text-gray-500 dark:text-muted"> — {sectionBody({ id: "", sku: "", product_name: "", section: 0, section_title: "", text: c.text }).slice(0, 160)}…</span>
                </li>
              ))}
            </ol>
          )}
        </div>
      )}
      {t.error && <p className="text-xs font-mono text-bad">{t.error}</p>}
    </div>
  );
}

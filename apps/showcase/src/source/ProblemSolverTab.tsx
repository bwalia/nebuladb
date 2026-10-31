/**
 * App C — Cleaning Problem Solver.
 *
 *   1. semantic_match over ss_guides, filtered by surface and sector.
 *   2. The SKUs those guides name → ss_products with sku IN (...) AND
 *      in_stock = TRUE (the kit list with stock status).
 *   3. SDS sections 4/7/8/10 for any kit item that has a demo SDS.
 *   4. "Add all to Quick Order" writes a *draft* to ss_order_drafts.
 *   5. Grounded chat over the cited guides and SDS sections only.
 */
import { useCallback, useState } from "react";
import { ApiError, api } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { GroundedChat, type ContextDoc } from "../rag/GroundedChat";
import { lit, runSql, semanticText, similarity } from "../rag/sql";
import { B, SECTORS, SECTOR_LABEL, exhaustiveLimit, type Sector } from "./data/common";
import { GUIDE_DOCS, SURFACES } from "./data/guides";
import { SDS_DOCS } from "./data/sds";
import { SITES } from "./data/accounts";
import type { Product } from "./data/products";
import { PRODUCTS_LIMIT } from "./finder";
import { Chips, DemoNote, SeedChip, gbp, metaOf, num, overlap, productOf, str, useSeeds } from "./common";

const GUIDES_LIMIT = exhaustiveLimit(GUIDE_DOCS.length);
const SDS_LIMIT = exhaustiveLimit(SDS_DOCS.length);

const SCENARIOS = [
  { label: "Black mould in student rooms", q: "Black mould keeps coming back on the walls of student bedrooms every winter.", surface: "painted walls", sector: "higher-education" as Sector },
  { label: "Gum on a school playground", q: "The school playground is covered in old chewing gum. How do we get it off?", surface: "tarmac", sector: "education" as Sector },
  { label: "Care-home laundry smells", q: "There is a bad smell in the care home laundry room that will not go away.", surface: "laundry drains", sector: "care" as Sector },
  { label: "Worn acrylic sports court", q: "Our acrylic sports court looks dull and players say it is slippery.", surface: "sports floor", sector: "leisure" as Sector },
];

interface GuideChunk {
  id: string;
  guideId: string;
  title: string;
  heading: string;
  chunk: number;
  skus: string[];
  text: string;
  sim: number;
  words: number;
}

interface KitItem {
  sku: string;
  product?: Product;
}

const SYSTEM =
  "You are a Source Supplies account manager helping a cleaning supervisor with a tough job. " +
  "Answer ONLY from the cited guide chunks and demo SDS sections, citing each fact as [n]. " +
  "Give steps as a short numbered list. Put any PPE or 'do not mix' warning from an SDS first. " +
  "If the guides do not say (for example how long before a room can be used), say the guides do not cover it. " +
  "Use UK English and plain words. You do not place orders: the kit list goes into a draft for a person to check.";

export function ProblemSolverTab() {
  const seed = useSeeds(["products", "guides", "sds", "accounts"]);
  const [q, setQ] = useState(SCENARIOS[0].q);
  const [surface, setSurface] = useState(SCENARIOS[0].surface);
  const [sector, setSector] = useState<Sector | "">(SCENARIOS[0].sector);
  const [scenario, setScenario] = useState(SCENARIOS[0].label);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [chunks, setChunks] = useState<GuideChunk[]>([]);
  const [kit, setKit] = useState<KitItem[]>([]);
  const [sds, setSds] = useState<ContextDoc[]>([]);
  const [runId, setRunId] = useState(0);
  const [siteKey, setSiteKey] = useState(`${SITES[4].account_id}/${SITES[4].site}`);
  const [saved, setSaved] = useState<string | null>(null);

  const run = async () => {
    const text = q.trim();
    if (!text) return;
    setErr(null);
    setSaved(null);
    setBusy(true);
    const trace: PipelineStep[] = [];
    const push = (s: PipelineStep) => {
      trace.push(s);
      setSteps([...trace]);
      return trace.length - 1;
    };
    const done = (i: number, patch: Partial<PipelineStep>) => {
      trace[i] = { ...trace[i], ...patch, status: "ok" };
      setSteps([...trace]);
    };
    try {
      const gSql =
        `SELECT * FROM ss_guides\n WHERE semantic_match(text, ${lit(semanticText(text))})` +
        (surface ? `\n   AND surface = ${lit(surface)}` : "") +
        (sector ? `\n   AND sector = ${lit(sector)}` : "") +
        `\n LIMIT ${GUIDES_LIMIT}`;
      let i = push({ label: "Find guides: meaning + surface + sector", detail: gSql, status: "running" });
      const g = await runSql(gSql);
      const all: GuideChunk[] = g.rows.map((r) => {
        const m = metaOf(r);
        return {
          id: r.id,
          guideId: str(m.guide_id),
          title: str(m.title),
          heading: str(m.heading),
          chunk: num(m.chunk),
          skus: Array.isArray(m.skus) ? (m.skus as string[]) : [],
          text: str(m.text),
          sim: similarity(r.score),
          words: overlap(text, str(m.text)),
        };
      });
      // Pick the best guide (words in common + similarity), then keep all its chunks in order.
      const byGuide = new Map<string, GuideChunk[]>();
      for (const c of all) byGuide.set(c.guideId, [...(byGuide.get(c.guideId) ?? []), c]);
      const ranked = [...byGuide.values()]
        .map((cs) => ({ cs, score: Math.max(...cs.map((c) => c.words * 2 + c.sim)) }))
        .sort((a, b) => b.score - a.score);
      const picked = ranked.slice(0, 1).flatMap((r) => r.cs).sort((a, b) => a.chunk - b.chunk);
      done(i, { rowsOut: all.length, ms: g.tookMs, detail: gSql + `\n→ best guide: ${picked[0]?.title ?? "none"}` });

      const skus = [...new Set(picked.flatMap((c) => c.skus))];
      let kitItems: KitItem[] = [];
      let sdsDocs: ContextDoc[] = [];
      if (skus.length) {
        const pSql =
          `SELECT * FROM ss_products\n WHERE semantic_match(text, ${lit(semanticText(text))})` +
          `\n   AND sku IN (${skus.map(lit).join(", ")})\n   AND in_stock = ${lit(true)}\n LIMIT ${PRODUCTS_LIMIT}`;
        i = push({ label: "Kit list: products the guide names, in stock", detail: pSql, status: "running" });
        const p = await runSql(pSql);
        const inStock = new Map(p.rows.map((r) => [str(metaOf(r).sku), productOf(r)]));
        kitItems = skus.map((s) => ({ sku: s, product: inStock.get(s) }));
        done(i, { rowsIn: skus.length, rowsOut: p.rows.length, ms: p.tookMs });

        const sSql =
          `SELECT * FROM ss_sds\n WHERE semantic_match(text, ${lit("PPE, storage, first aid, do not mix")})` +
          `\n   AND sku IN (${skus.map(lit).join(", ")})\n   AND section IN (${[4, 7, 8, 10].map(lit).join(", ")})\n LIMIT ${SDS_LIMIT}`;
        i = push({ label: "Safety: demo SDS sections for the kit", detail: sSql, status: "running" });
        const s = await runSql(sSql);
        sdsDocs = s.rows.map((r) => {
          const m = metaOf(r);
          return { id: r.id, label: `${str(m.sku)} · Section ${num(m.section)}`, bucket: B.sds, text: str(m.text) };
        });
        done(i, { rowsOut: s.rows.length, ms: s.tookMs });
      }
      setChunks(picked);
      setKit(kitItems);
      setSds(sdsDocs);
      setRunId((n) => n + 1);
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.code}: ${e.body}` : (e as Error).message);
      setSteps(trace.map((s) => (s.status === "running" ? { ...s, status: "error" } : s)));
    } finally {
      setBusy(false);
    }
  };

  const saveDraft = async () => {
    const site = SITES.find((s) => `${s.account_id}/${s.site}` === siteKey);
    if (!site) return;
    const lines = kit.filter((k) => k.product).map((k) => ({ sku: k.sku, qty: 1, price_gbp: k.product?.price_gbp ?? 0 }));
    const id = `draft-solver-${site.account_id}-${Date.now()}`;
    try {
      await api.upsertDoc(
        B.drafts,
        id,
        `Draft Quick Order basket for ${site.account_name}, ${site.site}: kit for "${chunks[0]?.title ?? "cleaning problem"}". ` +
          lines.map((l) => `1 × ${l.sku}`).join(", ") +
          ". Draft only: not ordered until a person confirms.",
        {
          account_id: site.account_id,
          site: site.site,
          status: "draft",
          created_by: "problem-solver",
          guide_id: chunks[0]?.guideId ?? "",
          lines,
          total_gbp: Math.round(lines.reduce((n, l) => n + l.price_gbp, 0) * 100) / 100,
        }
      );
      setSaved(id);
    } catch (e) {
      setErr((e as Error).message);
    }
  };

  const getContext = useCallback(
    async (): Promise<ContextDoc[]> => [
      ...chunks.map((c) => ({ id: c.id, label: `${c.title} · ${c.heading}`, bucket: B.guides, text: c.text })),
      ...sds.slice(0, 12),
    ],
    [chunks, sds]
  );

  const inStockKit = kit.filter((k) => k.product);

  return (
    <div className="space-y-4">
      <ArchitecturePanel id="ss_solver" />
      <DemoNote />
      <Panel title="Cleaning Problem Solver" subtitle="Describe the tough job. Get a method, the kit with stock status, and the guides behind it." action={<SeedChip seed={seed} />}>
        <Chips
          items={SCENARIOS}
          active={scenario}
          onPick={(s) => {
            setScenario(s.label);
            setQ(s.q);
            setSurface(s.surface);
            setSector(s.sector);
          }}
        />
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">What is the problem?</span>
          <textarea className="code-input w-full h-16" value={q} onChange={(e) => setQ(e.target.value)} />
        </label>
        <div className="flex flex-wrap gap-3 items-end">
          <label className="text-xs">
            <span className="block font-medium mb-1">Surface</span>
            <select className="code-input !py-1.5 !text-sm" value={surface} onChange={(e) => setSurface(e.target.value)}>
              <option value="">any</option>
              {SURFACES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Sector</span>
            <select className="code-input !py-1.5 !text-sm" value={sector} onChange={(e) => setSector(e.target.value as Sector | "")}>
              <option value="">any</option>
              {SECTORS.map((s) => (
                <option key={s} value={s}>
                  {SECTOR_LABEL[s]}
                </option>
              ))}
            </select>
          </label>
          <button className="btn" disabled={busy || !seed.ready || !q.trim()} onClick={() => void run()}>
            {busy ? <Spinner label="solving…" /> : "Solve it"}
          </button>
        </div>
        <ErrorBanner err={err} />
        <PipelineTrace steps={steps} />
      </Panel>

      {runId > 0 && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
          <div className="space-y-4">
            <Panel title={chunks[0] ? `Method · ${chunks[0].title}` : "Method"} subtitle="Step by step, from the cited guide">
              {chunks.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-muted">No guide matches that surface and sector. Try "any" for one of them.</p>
              ) : (
                <ol className="space-y-2">
                  {chunks.map((c, n) => (
                    <li key={c.id} className="rounded-md border border-gray-200 dark:border-edge px-3 py-2">
                      <div className="text-sm font-medium">
                        <span className="font-mono text-accent mr-1.5">[{n}]</span>
                        {c.heading}
                      </div>
                      <p className="mt-1 text-sm text-gray-700 dark:text-muted">{c.text.replace(/^.*? — .*?\. /, "")}</p>
                    </li>
                  ))}
                </ol>
              )}
            </Panel>
            <Panel title="Kit list" subtitle="Products the guide names, checked against stock">
              {kit.length === 0 ? (
                <p className="text-sm text-gray-500 dark:text-muted">This guide names no products.</p>
              ) : (
                <>
                  <ul className="space-y-1 text-sm">
                    {kit.map((k) => (
                      <li key={k.sku} className="flex flex-wrap items-baseline gap-x-2">
                        <span className={`dot ${k.product ? "dot-ok" : "dot-warn"}`} />
                        <span className="font-mono text-xs">{k.sku}</span>
                        {k.product ? (
                          <>
                            <span>{k.product.name}</span>
                            <span className="text-xs text-gray-500 dark:text-muted">
                              {k.product.pack} · {gbp(k.product.price_gbp)} ex VAT · {k.product.lead_days} day delivery
                            </span>
                          </>
                        ) : (
                          <span className="text-xs text-warn">Not in stock today: ask your account manager for the back-in date</span>
                        )}
                      </li>
                    ))}
                  </ul>
                  <div className="mt-3 flex flex-wrap items-end gap-2">
                    <label className="text-xs">
                      <span className="block font-medium mb-1">Site</span>
                      <select className="code-input !py-1.5 !text-sm" value={siteKey} onChange={(e) => setSiteKey(e.target.value)}>
                        {SITES.map((s) => (
                          <option key={`${s.account_id}/${s.site}`} value={`${s.account_id}/${s.site}`}>
                            {s.account_name} · {s.site}
                          </option>
                        ))}
                      </select>
                    </label>
                    <button className="btn !text-xs" disabled={inStockKit.length === 0} onClick={() => void saveDraft()}>
                      Add all to Quick Order (draft)
                    </button>
                  </div>
                  {saved && (
                    <p className="mt-2 text-xs text-ok">
                      Draft saved to ss_order_drafts as <span className="font-mono">{saved}</span>. Nothing has been ordered: a person checks it in Quick Order.
                    </p>
                  )}
                </>
              )}
            </Panel>
            <UnderTheHood filter={["/api/v1/query", "/api/v1/ai/rag", "/api/v1/bucket"]} />
          </div>
          <GroundedChat
            title="Follow-up questions"
            subtitle="Only from the cited guide and demo SDS sections"
            system={SYSTEM}
            getContext={getContext}
            resetKey={String(runId)}
            disabledReason={chunks.length === 0 ? "Find a guide first." : undefined}
            suggestions={["How long before the room can be used?", "What PPE do we need?", "Can we do this during the school day?"]}
            placeholder="Ask about timings, PPE or method…"
          />
        </div>
      )}
    </div>
  );
}

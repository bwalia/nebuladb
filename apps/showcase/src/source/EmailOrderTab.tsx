/**
 * Email Order Intake — from the Source Supplies discovery call.
 *
 * ~90% of orders come through the website → Merlin. The remaining emails
 * still need a person to re-key into Merlin. Dave asked: could AI put the
 * order on from the email?
 *
 * Demo: paste a customer email → match lines to ss_products → draft Merlin
 * sales order. A person confirms before anything is written.
 */
import { useState } from "react";
import { ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { UnderTheHood } from "../components/UnderTheHood";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { lit, runSql, similarity } from "../rag/sql";
import { B, exhaustiveLimit } from "./data/common";
import { PRODUCT_DOCS } from "./data/products";
import { guessCategory } from "./reorder";
import { Chips, DemoNote, SeedChip, gbp, metaOf, overlap, productOf, str, useSeeds } from "./common";
import type { Product } from "./data/products";

const PRODUCTS_LIMIT = exhaustiveLimit(PRODUCT_DOCS.length);

const EMAILS = [
  {
    label: "Brightway FM — Reading",
    from: "ops@brightway-fm.demo",
    subject: "PO BW-4418 — Reading Gateway restock",
    body: `Hi Source,

Please supply for Reading Gateway (next Tuesday delivery):

- 6 packs recycled jumbo toilet roll
- 4 packs hand towels
- 2 × floor cleaner concentrate
- 3 × multi-purpose cleaner

PO: BW-4418
Thanks,
Ops — Brightway FM`,
  },
  {
    label: "Ashby — Farnborough",
    from: "buyer@ashby-cc.demo",
    subject: "Urgent: gloves and bin liners Farnborough Depot",
    body: `Source team,

Need asap for Farnborough Depot:
12 boxes nitrile gloves
8 packs heavy-duty bin liners
2 kitchen degreaser

Charge Ashby Contract Cleaning / Farnborough Depot.
Ref ACC-9021`,
  },
  {
    label: "Vague one-liner",
    from: "site@harbour-care.demo",
    subject: "soap please",
    body: `Can we have some more hand soap for Seaview House? Same as last time if possible.`,
  },
];

interface DraftLine {
  raw: string;
  qty: number;
  product: Product | null;
  sim: number;
  note: string;
}

function parseQtyLines(body: string): Array<{ raw: string; qty: number; phrase: string }> {
  const out: Array<{ raw: string; qty: number; phrase: string }> = [];
  for (const line of body.split(/\n/)) {
    const t = line.replace(/^[-*•]\s*/, "").trim();
    if (!t || /^(hi|hello|please|thanks|regards|charge|po:|ref |from:|subject:)/i.test(t)) continue;
    const m = t.match(/^(\d+)\s*[×x]?\s*(?:packs?|boxes?|of)?\s*(.+)$/i) || t.match(/^(\d+)\s+(.+)$/);
    if (m) out.push({ raw: t, qty: Number(m[1]), phrase: m[2].replace(/\s+/g, " ").trim() });
    else if (/soap|towel|roll|cleaner|glove|liner|degreaser|mop/i.test(t)) {
      out.push({ raw: t, qty: 1, phrase: t });
    }
  }
  return out;
}

export function EmailOrderTab() {
  const seed = useSeeds(["products", "accounts"]);
  const [scenario, setScenario] = useState(EMAILS[0].label);
  const [from, setFrom] = useState(EMAILS[0].from);
  const [subject, setSubject] = useState(EMAILS[0].subject);
  const [body, setBody] = useState(EMAILS[0].body);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [draft, setDraft] = useState<DraftLine[]>([]);
  const [merlinDraft, setMerlinDraft] = useState<string | null>(null);

  const run = async () => {
    setBusy(true);
    setErr(null);
    setMerlinDraft(null);
    setDraft([]);
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
      const parsed = parseQtyLines(body);
      push({
        label: "Parse email into order lines",
        detail: `${parsed.length} candidate lines from subject/body (no Merlin write yet)`,
        rowsOut: parsed.length,
        status: "ok",
      });

      const lines: DraftLine[] = [];
      for (const p of parsed) {
        // Route to the catalogue category first, pull the whole category,
        // then rank mostly on words in common: robust with any embedder,
        // and a real embedder sharpens the meaning part.
        const cat = guessCategory(p.phrase);
        const sql =
          `SELECT * FROM ${B.products}\n WHERE semantic_match(text, ${lit(p.phrase)})` +
          `\n   AND in_stock = ${lit(true)}${cat ? `\n   AND category = ${lit(cat)}` : ""}\n LIMIT ${Math.max(PRODUCTS_LIMIT, PRODUCT_DOCS.length)}`;
        const i = push({ label: `Match “${p.phrase}”${cat ? ` (${cat})` : ""}`, detail: sql, status: "running" });
        const res = await runSql(sql);
        const ranked = res.rows
          .map((r) => {
            const prod = productOf(r);
            const words = overlap(p.phrase.replace(/-/g, " "), `${prod.name} ${str(metaOf(r).text)}`.replace(/-/g, " "));
            const sim = 0.7 * words + 0.3 * similarity(r.score);
            return { prod, sim, words };
          })
          .sort((a, b) => b.sim - a.sim);
        const best = ranked[0];
        done(i, { rowsOut: res.rows.length, ms: res.tookMs });
        lines.push({
          raw: p.raw,
          qty: p.qty,
          product: best?.prod ?? null,
          sim: best?.sim ?? 0,
          note: best
            ? best.sim >= 0.45
              ? "Matched"
              : "Weak match — check before confirming"
            : "No product match — needs a human",
        });
      }
      setDraft(lines);

      const ok = lines.filter((l) => l.product && l.sim >= 0.45);
      const weak = lines.length - ok.length;
      const total = ok.reduce((s, l) => s + l.qty * (l.product?.price_gbp ?? 0), 0);
      setMerlinDraft(
        [
          "SOURCE SUPPLIES — DRAFT MERLIN SALES ORDER (demo)",
          `From: ${from}`,
          `Subject: ${subject}`,
          `Matched ${ok.length}/${lines.length} lines · ${gbp(total)} ex VAT · ${weak} need a human`,
          "",
          ...ok.map(
            (l) =>
              `${String(l.qty).padStart(3)} × ${l.product!.sku.padEnd(8)}  ${l.product!.name}  @ ${gbp(l.product!.price_gbp)}`
          ),
          "",
          weak
            ? `Unresolved:\n${lines
                .filter((l) => !l.product || l.sim < 0.45)
                .map((l) => `  · ${l.raw}`)
                .join("\n")}`
            : "All lines matched.",
          "",
          "Status: draft. A person confirms before Jordan writes to Merlin.",
        ].join("\n")
      );
      push({
        label: "Build Merlin draft",
        detail: `${ok.length} ready · ${weak} flagged · human confirm required`,
        rowsOut: ok.length,
        status: "ok",
      });
    } catch (e) {
      setErr(e instanceof ApiError ? `${e.status}: ${e.body || e.code}` : (e as Error).message);
      if (trace.length) {
        trace[trace.length - 1] = { ...trace[trace.length - 1], status: "error" };
        setSteps([...trace]);
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <DemoNote extra="The ~10% of orders that still arrive by email — AI drafts Merlin, a person confirms." />
      <ArchitecturePanel id="ss_email" />
      <Panel
        title="Email Order Intake"
        subtitle="Paste a customer email → match catalogue → draft Merlin sales order."
        action={<SeedChip seed={seed} />}
      >
        <Chips
          items={EMAILS}
          active={scenario}
          onPick={(e) => {
            setScenario(e.label);
            setFrom(e.from);
            setSubject(e.subject);
            setBody(e.body);
          }}
        />
        <div className="grid gap-3 sm:grid-cols-2 mb-3">
          <label className="text-xs">
            <span className="block font-medium mb-1">From</span>
            <input className="input w-full" value={from} onChange={(e) => setFrom(e.target.value)} />
          </label>
          <label className="text-xs">
            <span className="block font-medium mb-1">Subject</span>
            <input className="input w-full" value={subject} onChange={(e) => setSubject(e.target.value)} />
          </label>
        </div>
        <label className="block mb-3">
          <span className="block text-xs font-medium mb-1">Email body</span>
          <textarea className="code-input w-full h-40" value={body} onChange={(e) => setBody(e.target.value)} />
        </label>
        <button type="button" className="btn" disabled={busy || !seed.ready} onClick={() => void run()}>
          {busy ? <Spinner /> : null}
          {busy ? "Matching…" : "Draft Merlin order"}
        </button>
      </Panel>

      <ErrorBanner err={err} />
      {steps.length > 0 && <PipelineTrace steps={steps} />}

      {draft.length > 0 && (
        <Panel title="Matched lines" subtitle="Weak matches stay with a human">
          <div className="overflow-x-auto -mx-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-xs uppercase tracking-wider text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge">
                  <th className="py-2 pr-3">Email line</th>
                  <th className="py-2 pr-3">Qty</th>
                  <th className="py-2 pr-3">SKU</th>
                  <th className="py-2 pr-3">Product</th>
                  <th className="py-2">Note</th>
                </tr>
              </thead>
              <tbody>
                {draft.map((l, i) => (
                  <tr key={i} className="border-b border-gray-100 dark:border-edge/60">
                    <td className="py-2 pr-3 text-xs">{l.raw}</td>
                    <td className="py-2 pr-3 font-mono">{l.qty}</td>
                    <td className="py-2 pr-3 font-mono text-xs">{l.product?.sku ?? "—"}</td>
                    <td className="py-2 pr-3">{l.product?.name ?? "—"}</td>
                    <td className={`py-2 text-xs ${l.sim < 0.45 ? "text-warn" : "text-gray-600 dark:text-muted"}`}>
                      {l.note}
                      {l.product ? ` · ${gbp(l.qty * l.product.price_gbp)}` : ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {merlinDraft && (
        <Panel title="Draft Merlin sales order" subtitle="Confirm before write — Jordan never places alone">
          <pre className="code-input !p-3 text-xs whitespace-pre-wrap">{merlinDraft}</pre>
        </Panel>
      )}

      <UnderTheHood filter={["/api/v1/query", "ss_products", "email"]} />
    </div>
  );
}

/**
 * Retail POD Autopilot — discovery-call pain #1 ("this is the biggest pain").
 *
 * A national retailer (~600 small orders a month) orders through its own
 * facilities portal. Today a person keys each order into Merlin, then for
 * every delivery finds the Merlin scan POD AND the courier photo POD and
 * uploads both to the portal. Dave: "probably costing 20 grand a year".
 *
 * Jordan runs the day: portal orders → Merlin drafts (retailer item codes
 * mapped, duplicates dropped), then every despatch is matched to its two
 * PODs by PO → consignment → fuzzy (NebulaDB holds the whole mailbox).
 * Clean matches attach themselves; the rest become one-line exceptions.
 * A person's correction is saved to `ss_retail_rules`, so the same case
 * never needs a person again — "the staff don't even know they're
 * training AI".
 *
 * Demo: "ValueMax Stores" and "ParcelLine" are fictional. Nothing is sent
 * to a real portal, courier or Merlin.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api, ApiError } from "../api";
import { ErrorBanner, Panel, Spinner } from "../components";
import { ArchitecturePanel } from "../components/RagArchitecture";
import { PipelineTrace, type PipelineStep } from "../rag/PipelineTrace";
import { lit, runSql } from "../rag/sql";
import { B, exhaustiveLimit } from "./data/common";
import {
  ADDRESS_ALIAS,
  BUCKET,
  COURIER,
  DESPATCH_DAY,
  RETAILER,
  RETAILER_ITEMS,
  RETAIL_DOCS,
  RULES_BUCKET,
  UNMAPPED_ITEMS,
  type CourierPod,
  type MerlinDespatch,
  type PortalOrder,
} from "./data/retail";
import { PRODUCT_BY_SKU } from "./data/products";
import { guessCategory } from "./reorder";
import {
  ITEM_THRESHOLD,
  ROI,
  intakeOrders,
  manualMinutesPerOrder,
  matchPods,
  roiFor,
  type CatalogueItem,
  type IntakeResult,
  type PodMatch,
  type RetailRule,
} from "./retailPilot";
import { BenefitStrip, Kpi, Pill } from "./autopilotUi";
import { DemoNote, SeedChip, gbp, metaOf, num, str, useSeeds } from "./common";

/** LIMIT caps rows returned as well as the candidate pool, so cover the whole mailbox. */
const INBOX_LIMIT = Math.max(exhaustiveLimit(RETAIL_DOCS.length), RETAIL_DOCS.length);
const KNOWN = Object.fromEntries(RETAILER_ITEMS.map((i) => [i.code, i.sku]));

type Filter = "all" | "attached" | "exception" | "waiting";

function routeCategory(desc: string) {
  return /\bsign/i.test(desc) ? "equipment" : guessCategory(desc);
}

function parseJson<T>(v: unknown, d: T): T {
  try {
    return typeof v === "string" ? (JSON.parse(v) as T) : d;
  } catch {
    return d;
  }
}

const STATUS: Record<PodMatch["status"], { tone: "ok" | "warn" | "bad" | "idle"; label: string }> = {
  attached: { tone: "ok", label: "Attached" },
  attached_followup: { tone: "ok", label: "Attached + follow-up" },
  exception: { tone: "bad", label: "Needs a person" },
  waiting: { tone: "idle", label: "Waiting courier" },
};

export function ServiceChannelTab() {
  const seed = useSeeds(["products", "retail"]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [steps, setSteps] = useState<PipelineStep[]>([]);
  const [intake, setIntake] = useState<IntakeResult[]>([]);
  const [pods, setPods] = useState<PodMatch[]>([]);
  const [shown, setShown] = useState(0);
  const [rules, setRules] = useState<RetailRule[]>([]);
  const [resolved, setResolved] = useState<Record<string, string>>({});
  const [filter, setFilter] = useState<Filter>("all");
  const [selected, setSelected] = useState<string | null>(null);
  const [log, setLog] = useState<string[]>([]);
  const timer = useRef<number | null>(null);

  const loadRules = useCallback(async (): Promise<RetailRule[]> => {
    try {
      const r = await runSql(`SELECT * FROM ${RULES_BUCKET} WHERE semantic_match(text, 'learned rule') LIMIT 40`);
      return r.rows.map((row) => {
        const m = metaOf(row);
        return m.kind === "address_alias"
          ? { kind: "address_alias" as const, store_no: str(m.store_no), postcode: str(m.postcode) }
          : { kind: "item_alias" as const, code: str(m.code), sku: str(m.sku) };
      });
    } catch {
      return []; // bucket not created until the first lesson
    }
  }, []);

  useEffect(() => {
    void loadRules().then(setRules);
    return () => {
      if (timer.current) window.clearInterval(timer.current);
    };
  }, [loadRules]);

  const run = async () => {
    setBusy(true);
    setErr(null);
    setResolved({});
    setSelected(null);
    setShown(0);
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
      const kindSql = (kind: string, hint: string, extra = "") =>
        `SELECT * FROM ${BUCKET}\n WHERE semantic_match(text, ${lit(hint)})\n   AND kind = ${lit(kind)}${extra}\n LIMIT ${INBOX_LIMIT}`;

      let i = push({ label: "Read today's portal work orders from the shared mailbox", detail: kindSql("portal_order", "retail portal work order"), status: "running" });
      const pr = await runSql(kindSql("portal_order", "retail portal work order"));
      const portal: PortalOrder[] = pr.rows.map((row) => {
        const m = metaOf(row);
        return { ...(m as unknown as PortalOrder), lines: parseJson(m.lines_json, []) };
      }).sort((a, b) => a.received.localeCompare(b.received));
      done(i, { rowsOut: portal.length, ms: pr.tookMs });

      const mSql = kindSql("merlin_despatch", "Merlin delivery ticket scanned POD", `\n   AND despatched = ${lit(DESPATCH_DAY)}`);
      i = push({ label: "Merlin delivery tickets (scan PODs) for Wednesday's despatches", detail: mSql, status: "running" });
      const mr = await runSql(mSql);
      const despatches: MerlinDespatch[] = mr.rows
        .map((row) => {
          const m = metaOf(row);
          return { ...(m as unknown as MerlinDespatch), lines: parseJson(m.lines_json, []) };
        })
        .sort((a, b) => a.so.localeCompare(b.so));
      done(i, { rowsOut: despatches.length, ms: mr.tookMs });

      const cSql = kindSql("courier_pod", `${COURIER} proof of delivery photo signed`);
      i = push({ label: `${COURIER} proof-of-delivery emails`, detail: cSql, status: "running" });
      const cr = await runSql(cSql);
      const courier: CourierPod[] = cr.rows.map((row) => metaOf(row) as unknown as CourierPod);
      done(i, { rowsOut: courier.length, ms: cr.tookMs });

      i = push({ label: "Learned rules (corrections staff made before)", detail: `SELECT * FROM ${RULES_BUCKET} … LIMIT 40`, status: "running" });
      const learned = await loadRules();
      setRules(learned);
      done(i, { rowsOut: learned.length });

      // Unknown retailer item codes → catalogue candidates from NebulaDB.
      const unknown = [...new Set(portal.flatMap((o) => o.lines).filter((l) => !KNOWN[l.code]).map((l) => l.desc))];
      const cands = new Map<string, CatalogueItem[]>();
      for (const desc of unknown) {
        const cat = routeCategory(desc);
        const sql = `SELECT * FROM ${B.products}\n WHERE semantic_match(text, ${lit(desc)})${cat ? `\n   AND category = ${lit(cat)}` : ""}\n LIMIT 39`;
        const j = push({ label: `Find catalogue match for new item “${desc}”`, detail: sql, status: "running" });
        const r = await runSql(sql);
        cands.set(desc, r.rows.map((row) => {
          const m = metaOf(row);
          return { sku: str(m.sku), name: str(m.name), text: str(m.text) };
        }));
        done(j, { rowsOut: r.rows.length, ms: r.tookMs });
      }

      const existingPos = new Set(despatches.map((d) => d.po));
      const intakeRes = intakeOrders(portal, KNOWN, learned, (d) => cands.get(d) ?? [], existingPos);
      push({
        label: "Portal → Merlin sales-order drafts",
        detail: `known retailer codes → SKU; learned aliases; new codes scored against catalogue (≥ ${ITEM_THRESHOLD}); duplicate PO check`,
        rowsIn: portal.length,
        rowsOut: intakeRes.filter((r) => r.status === "drafted").length,
        status: "ok",
      });
      const matches = matchPods(despatches, courier, learned);
      push({
        label: "Match two PODs per delivery: PO → consignment → fuzzy",
        detail: "fuzzy = 0.55·consignment edit distance + 0.30·postcode (incl. learned aliases) + 0.15·date; ≥ 0.70 to auto-match",
        rowsIn: despatches.length,
        rowsOut: matches.filter((m) => m.courier).length,
        status: "ok",
      });
      setIntake(intakeRes);
      setPods(matches);
      setLog([]);
      // Animate the board filling in, one delivery at a time.
      if (timer.current) window.clearInterval(timer.current);
      let k = 0;
      timer.current = window.setInterval(() => {
        k++;
        setShown(k);
        if (k >= matches.length && timer.current) {
          window.clearInterval(timer.current);
          timer.current = null;
        }
      }, 90);
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : (e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const teachAddress = async (m: PodMatch) => {
    if (!m.courier) return;
    const id = `ss-rule-addr-${m.store_no}-${m.courier.postcode.replace(/\s+/g, "")}`;
    await api.upsertDoc(RULES_BUCKET, id, `Learned rule: ${RETAILER} store ${m.store_no} ${m.store} also delivers to ${m.courier.postcode} (${m.courier.address}).`, {
      kind: "address_alias",
      store_no: m.store_no,
      postcode: m.courier.postcode,
      taught_by: "order desk",
    });
    setResolved((r) => ({ ...r, [m.so]: "Confirmed same store — attached; Jordan will remember this address" }));
    setRules((r) => [...r, { kind: "address_alias", store_no: m.store_no, postcode: m.courier!.postcode }]);
    setLog((l) => [`Learned: store ${m.store_no} ↔ ${m.courier!.postcode}. Saved to ${RULES_BUCKET}.`, ...l]);
  };

  const teachItem = async (code: string, sku: string, desc: string) => {
    await api.upsertDoc(RULES_BUCKET, `ss-rule-item-${code}`, `Learned rule: ${RETAILER} item ${code} “${desc}” is Source SKU ${sku}.`, {
      kind: "item_alias",
      code,
      sku,
      taught_by: "order desk",
    });
    setRules((r) => [...r, { kind: "item_alias", code, sku }]);
    setIntake((prev) =>
      prev.map((o) => {
        const lines = o.lines.map((l) => (l.code === code ? { ...l, sku, how: "learned" as const, score: 1 } : l));
        const review = lines.some((l) => l.how === "suggested" || l.how === "unresolved");
        return o.status === "duplicate" ? o : { ...o, lines, status: review ? "needs_review" : "drafted", note: review ? o.note : "All lines mapped — Merlin SO drafted." };
      })
    );
    setLog((l) => [`Learned: ${code} → ${sku}. Next time it maps with no review.`, ...l]);
  };

  const resetLessons = async () => {
    const ids = [
      `ss-rule-addr-${ADDRESS_ALIAS.store_no}-${ADDRESS_ALIAS.postcode.replace(/\s+/g, "")}`,
      ...UNMAPPED_ITEMS.map((u) => `ss-rule-item-${u.code}`),
    ];
    for (const id of ids) await api.deleteDoc(RULES_BUCKET, id).catch(() => undefined);
    setRules([]);
    setLog((l) => ["Learned rules cleared — the next run starts from scratch.", ...l]);
  };

  const effective = useMemo(
    () => pods.map((m) => (resolved[m.so] && m.status === "exception" ? { ...m, status: "attached" as const, action: resolved[m.so] } : m)),
    [pods, resolved]
  );
  const visible = effective.slice(0, shown);
  const roi = roiFor(effective);
  const liveRoi = roiFor(visible.length ? visible : effective);
  const filtered = visible.filter((m) =>
    filter === "all" ? true : filter === "attached" ? m.status === "attached" || m.status === "attached_followup" : m.status === filter
  );
  const sel = effective.find((m) => m.so === selected) ?? null;
  const manual = manualMinutesPerOrder();
  const drafted = intake.filter((o) => o.status === "drafted").length;

  return (
    <div className="space-y-4">
      <DemoNote extra={`Fictional retailer “${RETAILER}” and courier “${COURIER}”. Nothing reaches a real portal, courier or Merlin.`} />
      <BenefitStrip
        headline={pods.length ? `≈ ${gbp(roi.poundsPerYear).replace(".00", "")} / year` : "≈ £20k / year"}
        sub={pods.length ? `${roi.hoursSavedMonth} staff hours a month back · ${roi.fteFreed} of a person freed` : "Dave’s estimate for the dual-POD loop"}
        rows={[
          { label: "Minutes per retail order", before: `${manual} min`, after: pods.length ? `${roi.autoMinutes} min` : "≈ 1 min" },
          { label: "Orders keyed into Merlin by hand", before: "30 a day", after: "only new item codes, once" },
          { label: "PODs found and uploaded by hand", before: "2 per delivery", after: pods.length ? `${roi.exceptions} exceptions today` : "exceptions only" },
          { label: "When a case is new", before: "same manual work next time", after: "one click teaches Jordan" },
        ]}
        quote="That whole process… it’s probably costing 20 grand a year"
      />
      <ArchitecturePanel id="ss_retail_pod" />

      <Panel
        title="Retail POD Autopilot"
        subtitle={`Friday 30 Oct · ${RETAILER} portal → Merlin → ${COURIER} → attach Merlin scan + courier photo back to the portal`}
        action={<SeedChip seed={seed} />}
      >
        <div className="flex flex-wrap items-center gap-2">
          <button type="button" className="btn" onClick={run} disabled={busy || !seed.ready} data-tour="run">
            {busy ? <Spinner label="Running the day…" /> : "▶ Run today’s batch"}
          </button>
          <button type="button" className="btn-secondary text-xs" onClick={resetLessons} disabled={busy}>
            Reset learned rules ({rules.length})
          </button>
          <span className="text-xs text-gray-500 dark:text-muted">
            ~{ROI.ordersPerMonth} orders a month · {manual} manual minutes each (accept, key, find 2 PODs, upload)
          </span>
        </div>
        <ErrorBanner err={err} />
      </Panel>

      {steps.length > 0 && <PipelineTrace steps={steps} />}

      {pods.length > 0 && (
        <div className="grid grid-cols-2 md:grid-cols-5 gap-2" data-tour="kpis">
          <Kpi label="Deliveries processed" value={`${visible.length}/${effective.length}`} />
          <Kpi label="Straight-through" value={`${Math.round(liveRoi.stp * 100)}%`} tone="ok" hint="both PODs attached, no person" />
          <Kpi label="Need a person" value={visible.filter((m) => m.status === "exception").length} tone="warn" hint="each with a one-line action" />
          <Kpi label="Merlin drafts" value={`${drafted}/${intake.length}`} hint="1 duplicate PO dropped" />
          <Kpi label="Saved per year" value={gbp(roi.poundsPerYear).replace(".00", "")} tone="ok" hint={`at £${ROI.hourlyCost}/h, ${ROI.ordersPerMonth}/month`} />
        </div>
      )}

      {intake.length > 0 && (
        <Panel title="1 · Portal orders → Merlin drafts" subtitle="Retailer item codes mapped to Source SKUs; new codes matched against the catalogue in NebulaDB">
          <div className="overflow-x-auto -mx-1">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-[11px] uppercase tracking-wider text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge">
                  <th className="py-1.5 pr-3">Work order</th>
                  <th className="py-1.5 pr-3">Store</th>
                  <th className="py-1.5 pr-3">Lines → SKU</th>
                  <th className="py-1.5">Status</th>
                </tr>
              </thead>
              <tbody>
                {intake.map((o) => (
                  <tr key={o.wo} className="border-b border-gray-100 dark:border-edge/60 align-top" data-tour={o.status === "needs_review" ? "intake-review" : undefined}>
                    <td className="py-1.5 pr-3 font-mono text-xs">
                      {o.wo}
                      <div className="text-[11px] text-gray-500 dark:text-faint">PO {o.po}</div>
                    </td>
                    <td className="py-1.5 pr-3 text-xs">{o.store}</td>
                    <td className="py-1.5 pr-3 text-xs space-y-1">
                      {o.lines.map((l) => (
                        <div key={l.code} className="flex flex-wrap items-center gap-1.5">
                          <span>
                            {l.qty} × <span className="text-gray-500 dark:text-muted">{l.desc}</span> →
                          </span>
                          {l.sku ? <span className="font-mono">{l.sku}</span> : <span className="text-bad">no match</span>}
                          {l.how === "learned" && <Pill tone="accent">learned</Pill>}
                          {l.how === "suggested" && (
                            <>
                              <Pill tone="warn">new code · {Math.round(l.score * 100)}% match</Pill>
                              <span className="text-gray-500 dark:text-faint">{PRODUCT_BY_SKU[l.sku!]?.name}</span>
                              <button type="button" className="btn text-[11px] !py-0.5 !px-2" onClick={() => teachItem(l.code, l.sku!, l.desc)}>
                                Confirm &amp; remember
                              </button>
                            </>
                          )}
                        </div>
                      ))}
                      {o.status === "duplicate" && <span className="text-gray-500 dark:text-muted">{o.note}</span>}
                    </td>
                    <td className="py-1.5">
                      {o.status === "drafted" && <Pill tone="ok">Merlin SO drafted</Pill>}
                      {o.status === "needs_review" && <Pill tone="warn">Confirm 1 line</Pill>}
                      {o.status === "duplicate" && <Pill tone="idle">Duplicate — skipped</Pill>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>
      )}

      {pods.length > 0 && (
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_22rem]">
          <Panel title="2 · Proof of delivery — two PODs per order" subtitle="Click a row to see the attach pack">
            <div className="flex flex-wrap gap-1.5 mb-2">
              {(["all", "attached", "exception", "waiting"] as Filter[]).map((f) => (
                <button
                  key={f}
                  type="button"
                  onClick={() => setFilter(f)}
                  data-tour={`filter-${f}`}
                  className={`rounded-full border px-2.5 py-1 text-xs ${filter === f ? "border-accent text-accent bg-accent/10" : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted"}`}
                >
                  {f === "all" ? "All" : f === "attached" ? "Attached" : f === "exception" ? "Needs a person" : "Waiting"} (
                  {f === "all" ? visible.length : visible.filter((m) => (f === "attached" ? m.status.startsWith("attached") : m.status === f)).length})
                </button>
              ))}
            </div>
            <div className="overflow-x-auto -mx-1 max-h-[30rem] overflow-y-auto">
              <table className="w-full text-xs">
                <thead className="sticky top-0 bg-white dark:bg-carbon-900">
                  <tr className="text-left text-[11px] uppercase tracking-wider text-gray-500 dark:text-muted border-b border-gray-200 dark:border-edge">
                    <th className="py-1.5 pr-2">Store</th>
                    <th className="py-1.5 pr-2">PO</th>
                    <th className="py-1.5 pr-2">Matched by</th>
                    <th className="py-1.5 pr-2">PODs</th>
                    <th className="py-1.5">Status</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((m) => (
                    <tr
                      key={m.so}
                      onClick={() => setSelected(m.so)}
                      data-tour={`pod-${m.status}`}
                      className={`border-b border-gray-100 dark:border-edge/60 cursor-pointer animate-rise ${selected === m.so ? "bg-accent/10" : "hover:bg-gray-50 dark:hover:bg-carbon-800"}`}
                    >
                      <td className="py-1.5 pr-2">{m.store.replace(`${RETAILER} `, "")}</td>
                      <td className="py-1.5 pr-2 font-mono">{m.po}</td>
                      <td className="py-1.5 pr-2">
                        {m.via === "po" && "PO number"}
                        {m.via === "consignment" && "consignment"}
                        {m.via === "fuzzy" && <Pill tone="accent">AI fuzzy {Math.round(m.confidence * 100)}%</Pill>}
                        {!m.via && "—"}
                      </td>
                      <td className="py-1.5 pr-2 whitespace-nowrap">
                        <span className={`dot ${m.merlin.scan_pod ? "dot-ok" : "dot-idle"}`} /> Merlin{" "}
                        <span className={`dot ${m.courier?.status === "Delivered" ? "dot-ok" : m.courier ? "dot-bad" : "dot-idle"}`} /> Courier
                      </td>
                      <td className="py-1.5">
                        <Pill tone={STATUS[m.status].tone}>{resolved[m.so] ? "Attached (person)" : STATUS[m.status].label}</Pill>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>

          <Panel title={sel ? `Attach pack · PO ${sel.po}` : "Attach pack"} subtitle={sel ? sel.store : "Select a delivery"}>
            {!sel && <p className="text-xs text-gray-500 dark:text-muted">Pick a row. Exceptions explain exactly what a person needs to do.</p>}
            {sel && (
              <div className="space-y-3 text-xs" data-tour="attach-pack">
                <div className="grid grid-cols-2 gap-2">
                  <div className="rounded-md border border-gray-200 dark:border-edge p-2">
                    <div className="eyebrow">Merlin scan POD</div>
                    <div className="font-mono mt-1">{sel.merlin.so}</div>
                    <div className="text-gray-500 dark:text-muted">{sel.merlin.lines.map((l) => `${l.qty}× ${l.sku}${l.owed ? ` (${l.owed} owed)` : ""}`).join(", ")}</div>
                    <div className="text-gray-500 dark:text-faint truncate">{sel.merlin.scan_pod}</div>
                  </div>
                  <div className="rounded-md border border-gray-200 dark:border-edge p-2">
                    <div className="eyebrow">{COURIER} photo POD</div>
                    {sel.courier ? (
                      <>
                        <div className="font-mono mt-1">{sel.courier.consignment}</div>
                        <div className="text-gray-500 dark:text-muted">
                          {sel.courier.status} {sel.courier.delivered_at.replace("T", " ")}
                        </div>
                        <div className="text-gray-500 dark:text-faint">{sel.courier.note}</div>
                      </>
                    ) : (
                      <div className="text-gray-500 dark:text-muted mt-1">Not back yet</div>
                    )}
                  </div>
                </div>
                {sel.via === "fuzzy" && sel.courier && (
                  <p className="rounded-md bg-accent/10 border border-accent/30 p-2">
                    Courier email had <b>no PO</b> and consignment <span className="font-mono">{sel.courier.consignment}</span> vs Merlin{" "}
                    <span className="font-mono">{sel.merlin.consignment}</span> (two digits swapped). Matched on near-identical consignment + same postcode + date:{" "}
                    {Math.round(sel.confidence * 100)}% confidence.
                  </p>
                )}
                <ul className="space-y-0.5">
                  {sel.checks.map((c) => (
                    <li key={c.label}>
                      <span className={c.ok ? "text-ok" : "text-bad"}>{c.ok ? "✓" : "✗"}</span> {c.label}
                    </li>
                  ))}
                </ul>
                <p className="font-medium text-gray-900 dark:text-ink">{resolved[sel.so] ?? sel.action}</p>
                {sel.status === "exception" && !resolved[sel.so] && (
                  <div className="flex flex-wrap gap-2">
                    {sel.checks.some((c) => c.label.startsWith("Postcode")) && (
                      <button type="button" className="btn text-xs" onClick={() => teachAddress(sel)} data-tour="teach-address">
                        Same store — attach &amp; remember
                      </button>
                    )}
                    {sel.courier?.status === "Delivered" && !sel.courier.signed_by && (
                      <button type="button" className="btn text-xs" onClick={() => setResolved((r) => ({ ...r, [sel.so]: "Store confirmed receipt by phone — attached." }))}>
                        Store confirmed — attach
                      </button>
                    )}
                    {sel.courier?.status === "Failed" && (
                      <button type="button" className="btn text-xs" onClick={() => setResolved((r) => ({ ...r, [sel.so]: "Rebooked for Monday; store emailed (draft)." }))}>
                        Rebook courier
                      </button>
                    )}
                  </div>
                )}
              </div>
            )}
          </Panel>
        </div>
      )}

      {log.length > 0 && (
        <Panel title="What Jordan learned" subtitle={`Saved to ${RULES_BUCKET} — used on every future run`}>
          <ul className="space-y-1 text-xs text-gray-700 dark:text-muted">
            {log.map((l, i) => (
              <li key={i}>· {l}</li>
            ))}
          </ul>
        </Panel>
      )}
      <p className="text-[11px] text-gray-500 dark:text-faint">
        Figures: {manual} manual minutes per order (accept 2, key into Merlin 4, find Merlin POD 2, find courier POD 3, upload both 3) vs{" "}
        {ROI.autopilotMinutes.straightThrough} min to spot-check a straight-through order and {ROI.autopilotMinutes.exception} min per exception, at £
        {ROI.hourlyCost}/hour loaded. Demo figures, tuned to Dave’s “~£20k a year”. {num(rules.length)} learned rule(s) active.
      </p>
    </div>
  );
}

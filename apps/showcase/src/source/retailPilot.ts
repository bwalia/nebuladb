/**
 * Retail POD Autopilot — the deterministic engine behind the
 * `ssretail` tab. No model and no runtime imports beyond data types,
 * so `node --test` loads it directly.
 *
 * Two jobs from the discovery call:
 *   1. Intake: portal work orders → Merlin sales-order drafts (retailer
 *      item codes mapped to Source SKUs, duplicates caught).
 *   2. Proof of delivery: for every despatched order, find the Merlin
 *      scan POD and the courier photo POD, check them, and either attach
 *      both to the portal or raise a clear exception for a person.
 *
 * Matching order: PO number (exact) → consignment (exact) → fuzzy
 * (consignment edit distance + postcode + date). Learned rules (address
 * aliases, item-code mappings) come from `ss_retail_rules`.
 */
import type { CourierPod, MerlinDespatch, PortalOrder } from "./data/retail.ts";

export interface AddressRule {
  kind: "address_alias";
  store_no: string;
  postcode: string;
}
export interface ItemRule {
  kind: "item_alias";
  code: string;
  sku: string;
}
export type RetailRule = AddressRule | ItemRule;

export type PodStatus = "attached" | "attached_followup" | "exception" | "waiting";

export interface Check {
  label: string;
  ok: boolean;
}

export interface PodMatch {
  so: string;
  po: string;
  store: string;
  store_no: string;
  status: PodStatus;
  via: "po" | "consignment" | "fuzzy" | null;
  confidence: number;
  merlin: MerlinDespatch;
  courier?: CourierPod;
  checks: Check[];
  /** One line a person can act on. */
  action: string;
}

export function editDistance(a: string, b: string): number {
  const dp = Array.from({ length: a.length + 1 }, (_, i) => [i, ...Array(b.length).fill(0)]);
  for (let j = 1; j <= b.length; j++) dp[0][j] = j;
  for (let i = 1; i <= a.length; i++)
    for (let j = 1; j <= b.length; j++) {
      dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
      // Adjacent transposition (Damerau) — the most common keying slip.
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) dp[i][j] = Math.min(dp[i][j], dp[i - 2][j - 2] + 1);
    }
  return dp[a.length][b.length];
}

const norm = (pc: string) => pc.replace(/\s+/g, "").toUpperCase();

function postcodeOk(store_no: string, storePc: string, podPc: string, rules: RetailRule[]): boolean {
  if (norm(storePc) === norm(podPc)) return true;
  return rules.some((r) => r.kind === "address_alias" && r.store_no === store_no && norm(r.postcode) === norm(podPc));
}

/** Score a courier POD as a fuzzy candidate for a despatch (0..1). */
export function fuzzyScore(d: MerlinDespatch, p: CourierPod, rules: RetailRule[]): number {
  const dist = editDistance(d.consignment, p.consignment);
  const cons = dist <= 2 ? 1 - dist / 4 : 0;
  const pc = postcodeOk(d.store_no, d.postcode, p.postcode, rules) ? 1 : norm(d.postcode).slice(0, 3) === norm(p.postcode).slice(0, 3) ? 0.6 : 0;
  const day = p.delivered_at.slice(0, 10) >= d.despatched ? 1 : 0;
  return Math.round((0.55 * cons + 0.3 * pc + 0.15 * day) * 100) / 100;
}

export const FUZZY_THRESHOLD = 0.7;

export function matchPods(despatches: MerlinDespatch[], pods: CourierPod[], rules: RetailRule[] = []): PodMatch[] {
  const byRef = new Map(pods.filter((p) => p.customer_ref).map((p) => [p.customer_ref, p]));
  const byCons = new Map(pods.map((p) => [p.consignment, p]));
  const claimed = new Set<string>();
  const out: PodMatch[] = [];
  const pending: MerlinDespatch[] = [];

  const finish = (d: MerlinDespatch, p: CourierPod | undefined, via: PodMatch["via"], confidence: number): PodMatch => {
    if (!p) {
      return {
        so: d.so, po: d.po, store: d.store, store_no: d.store_no, status: "waiting", via: null, confidence: 0, merlin: d,
        checks: [{ label: "Merlin scan POD on file", ok: Boolean(d.scan_pod) }, { label: "Courier POD received", ok: false }],
        action: "Courier POD not back yet — Jordan will re-check at 14:00.",
      };
    }
    claimed.add(p.consignment);
    const delivered = p.status === "Delivered";
    const signed = Boolean(p.signed_by);
    const pcOk = postcodeOk(d.store_no, d.postcode, p.postcode, rules);
    const owed = d.lines.filter((l) => l.owed > 0);
    const checks: Check[] = [
      { label: "Merlin scan POD on file", ok: Boolean(d.scan_pod) },
      { label: `Courier status: ${p.status}`, ok: delivered },
      { label: signed ? `Signed by ${p.signed_by}` : "Signature", ok: signed },
      { label: pcOk ? "Delivery postcode matches store" : `Postcode ${p.postcode} ≠ store ${d.postcode}`, ok: pcOk },
      { label: owed.length ? `${owed.length} line owed on Merlin ticket` : "All lines delivered", ok: owed.length === 0 },
    ];
    let status: PodStatus = "attached";
    let action = `Attach ${d.so} scan + ${p.consignment} photo to portal PO ${d.po}.`;
    if (!delivered) {
      status = "exception";
      action = `Delivery failed (${p.note}) — rebook ${p.consignment} and tell the store.`;
    } else if (!signed) {
      status = "exception";
      action = "Photo only, no signature — ask the store to confirm receipt before attaching.";
    } else if (!pcOk) {
      status = "exception";
      action = `Courier address ${p.postcode} differs from the store record — confirm it is the same store.`;
    } else if (owed.length) {
      status = "attached_followup";
      action = `Attach both PODs and raise a follow-up for ${owed.map((l) => `${l.owed} × ${l.sku}`).join(", ")} owed.`;
    }
    return { so: d.so, po: d.po, store: d.store, store_no: d.store_no, status, via, confidence, merlin: d, courier: p, checks, action };
  };

  for (const d of despatches) {
    const viaRef = byRef.get(d.po);
    if (viaRef) out.push(finish(d, viaRef, "po", 0.99));
    else if (byCons.get(d.consignment)) out.push(finish(d, byCons.get(d.consignment), "consignment", 0.97));
    else pending.push(d);
  }
  // Fuzzy pass only over PODs no exact match claimed.
  for (const d of pending) {
    let best: { p: CourierPod; s: number } | null = null;
    for (const p of pods) {
      if (claimed.has(p.consignment)) continue;
      const s = fuzzyScore(d, p, rules);
      if (!best || s > best.s) best = { p, s };
    }
    if (best && best.s >= FUZZY_THRESHOLD) out.push(finish(d, best.p, "fuzzy", best.s));
    else out.push(finish(d, undefined, null, 0));
  }
  const order = new Map(despatches.map((d, i) => [d.so, i]));
  return out.sort((a, b) => order.get(a.so)! - order.get(b.so)!);
}

/* ---------------- Intake: portal orders → Merlin drafts ---------------- */

export interface CatalogueItem {
  sku: string;
  name: string;
  text: string;
}

const SYNONYMS: Record<string, string[]> = {
  "blue roll": ["centrefeed", "blue"],
  sign: ["signs", "wet", "floor"],
  tissue: ["toilet", "roll"],
  sacks: ["refuse", "sacks"],
  liners: ["bin", "liners"],
};

function tokens(s: string): string[] {
  return s.toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/).filter((w) => w.length > 2 && !/^x?\d+$/.test(w));
}

/** Words-in-common score of a portal description against a catalogue item (0..1). */
export function itemScore(desc: string, item: CatalogueItem): number {
  const lower = desc.toLowerCase();
  const q = new Set(tokens(desc));
  for (const [k, extra] of Object.entries(SYNONYMS)) if (lower.includes(k)) extra.forEach((w) => q.add(w));
  if (q.size === 0) return 0;
  const t = new Set(tokens(`${item.name} ${item.text}`));
  let hit = 0;
  for (const w of q) if (t.has(w) || t.has(w.replace(/s$/, "")) || t.has(`${w}s`)) hit++;
  return Math.round((hit / q.size) * 100) / 100;
}

export interface IntakeLine {
  code: string;
  desc: string;
  qty: number;
  sku: string | null;
  how: "known" | "learned" | "suggested" | "unresolved";
  score: number;
  candidates: { sku: string; name: string; score: number }[];
}

export interface IntakeResult {
  wo: string;
  po: string;
  store: string;
  status: "drafted" | "needs_review" | "duplicate";
  lines: IntakeLine[];
  note: string;
}

export const ITEM_THRESHOLD = 0.6;

/**
 * `known` maps retailer codes Source already set up; `candidatesFor`
 * returns catalogue rows for a description (from NebulaDB in the app).
 */
export function intakeOrders(
  orders: PortalOrder[],
  known: Record<string, string>,
  rules: RetailRule[],
  candidatesFor: (desc: string) => CatalogueItem[],
  existingPos: Set<string> = new Set()
): IntakeResult[] {
  const learned = Object.fromEntries(rules.filter((r): r is ItemRule => r.kind === "item_alias").map((r) => [r.code, r.sku]));
  const seen = new Set<string>();
  return orders.map((o) => {
    if (seen.has(o.po) || existingPos.has(o.po)) {
      return { wo: o.wo, po: o.po, store: o.store, status: "duplicate", lines: [], note: `PO ${o.po} already keyed — portal re-send ignored.` };
    }
    seen.add(o.po);
    const lines: IntakeLine[] = o.lines.map((l) => {
      if (known[l.code]) return { ...l, sku: known[l.code], how: "known", score: 1, candidates: [] };
      if (learned[l.code]) return { ...l, sku: learned[l.code], how: "learned", score: 1, candidates: [] };
      const cands = candidatesFor(l.desc)
        .map((c) => ({ sku: c.sku, name: c.name, score: itemScore(l.desc, c) }))
        .sort((a, b) => b.score - a.score)
        .slice(0, 3);
      const top = cands[0];
      if (top && top.score >= ITEM_THRESHOLD) return { ...l, sku: top.sku, how: "suggested", score: top.score, candidates: cands };
      return { ...l, sku: null, how: "unresolved", score: top?.score ?? 0, candidates: cands };
    });
    const review = lines.some((l) => l.how === "suggested" || l.how === "unresolved");
    return {
      wo: o.wo,
      po: o.po,
      store: o.store,
      status: review ? "needs_review" : "drafted",
      lines,
      note: review ? "New retailer item code — confirm the SKU once and Jordan remembers it." : "All lines mapped — Merlin SO drafted.",
    };
  });
}

/* ---------------- ROI ---------------- */

/** Demo figures, from the call: ~30 orders a day, ~600 a month, "probably £20k a year". */
export const ROI = {
  ordersPerMonth: 600,
  manualMinutes: { accept: 2, keyMerlin: 4, findMerlinPod: 2, findCourierPod: 3, uploadBoth: 3 },
  autopilotMinutes: { straightThrough: 0.5, exception: 4 },
  hourlyCost: 14,
};

export function manualMinutesPerOrder(): number {
  return Object.values(ROI.manualMinutes).reduce((a, b) => a + b, 0);
}

export function roiFor(matches: PodMatch[]) {
  const total = matches.length || 1;
  const straight = matches.filter((m) => m.status === "attached" || m.status === "attached_followup").length;
  const exceptions = matches.filter((m) => m.status === "exception").length;
  const stp = straight / total;
  const manual = manualMinutesPerOrder();
  const auto = stp * ROI.autopilotMinutes.straightThrough + (1 - stp) * ROI.autopilotMinutes.exception;
  const hoursSavedMonth = (ROI.ordersPerMonth * (manual - auto)) / 60;
  return {
    straight,
    exceptions,
    stp,
    manualMinutes: manual,
    autoMinutes: Math.round(auto * 10) / 10,
    hoursSavedMonth: Math.round(hoursSavedMonth),
    poundsPerYear: Math.round((hoursSavedMonth * 12 * ROI.hourlyCost) / 100) * 100,
    fteFreed: Math.round((hoursSavedMonth / 140) * 10) / 10,
  };
}

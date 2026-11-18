/**
 * `ss_demand` — 24 months (Nov 2024 – Oct 2026) of company-wide monthly
 * despatches for Source Supplies' core lines, plus the estimated stock
 * position on the demo "today" (31 Oct 2026).
 *
 * DEMO DATA. Volumes, suppliers, lead times and stock are invented, but
 * shaped like the business on the discovery call: ~95% repeat lines,
 * school-holiday dips, a winter hygiene peak, ~12% growth a year, a new
 * contract that steps one line up, and one big one-off order that a
 * plain 3-month average would wrongly chase.
 *
 * Merlin has no live stock, so `on_hand` is an *estimate*: last count +
 * goods in − despatches. That is exactly what Jordan would compute.
 */
import { B, rng, type SeedDoc } from "./common.ts";
import { PRODUCT_BY_SKU } from "./products.ts";

export const SEED_VERSION = "ss-demand-v1";
export const BUCKET = B.demand;

export const DEMAND_MONTHS: string[] = (() => {
  const out: string[] = [];
  for (let i = 0; i < 24; i++) {
    const d = new Date(Date.UTC(2024, 10 + i, 1));
    out.push(d.toISOString().slice(0, 7));
  }
  return out;
})();

export type Family = "paper" | "hygiene" | "waste" | "chemicals" | "ppe";

/** Seasonal shape by calendar month (Jan … Dec). */
const SEASON: Record<Family, number[]> = {
  paper: [1.06, 1.02, 1.0, 0.98, 0.97, 0.95, 0.92, 0.78, 1.05, 1.07, 1.14, 0.86],
  hygiene: [1.24, 1.18, 1.04, 0.94, 0.88, 0.83, 0.8, 0.78, 0.96, 1.12, 1.22, 1.14],
  waste: [0.95, 0.92, 0.95, 1.0, 1.04, 1.1, 1.12, 1.06, 0.98, 1.0, 1.08, 1.28],
  chemicals: [1.0, 0.98, 1.02, 1.05, 1.0, 0.98, 0.95, 0.84, 1.08, 1.05, 1.03, 0.9],
  ppe: [1.02, 1.0, 1.0, 1.0, 0.99, 0.99, 0.98, 0.97, 1.01, 1.01, 1.02, 1.0],
};

export interface Supplier {
  id: string;
  name: string;
  lead_days: number;
}

export const SUPPLIERS: Record<string, Supplier> = {
  tissue: { id: "tissue", name: "Northfold Tissue Mills (demo)", lead_days: 14 },
  chem: { id: "chem", name: "Greenline Chemicals (demo)", lead_days: 7 },
  poly: { id: "poly", name: "PolyPak Sacks (demo)", lead_days: 10 },
  ppe: { id: "ppe", name: "SafeHands PPE (demo)", lead_days: 5 },
  soap: { id: "soap", name: "Clearwash Hygiene (demo)", lead_days: 12 },
};

export interface CoreLine {
  sku: string;
  family: Family;
  supplier: keyof typeof SUPPLIERS;
  /** Packs per month in Nov 2024 before season/growth. */
  base: number;
  moq: number;
  /** Estimated cover on hand today, in months of the naive average (sets the story). */
  cover: number;
  on_order: number;
  event?: "step_up" | "one_off";
}

export const CORE: CoreLine[] = [
  { sku: "JR-400", family: "paper", supplier: "tissue", base: 820, moq: 40, cover: 1.15, on_order: 0 },
  { sku: "JR-410", family: "paper", supplier: "tissue", base: 610, moq: 40, cover: 0.4, on_order: 0, event: "step_up" },
  { sku: "TR-425", family: "paper", supplier: "tissue", base: 380, moq: 40, cover: 1.4, on_order: 0 },
  { sku: "HT-300", family: "paper", supplier: "tissue", base: 540, moq: 40, cover: 0.85, on_order: 120 },
  { sku: "HT-310", family: "paper", supplier: "tissue", base: 470, moq: 40, cover: 0.45, on_order: 0 },
  { sku: "CR-350", family: "paper", supplier: "tissue", base: 300, moq: 30, cover: 1.6, on_order: 0 },
  { sku: "HS-060", family: "hygiene", supplier: "soap", base: 260, moq: 24, cover: 0.9, on_order: 0 },
  { sku: "HS-270", family: "hygiene", supplier: "soap", base: 340, moq: 24, cover: 0.45, on_order: 0 },
  { sku: "HS-275", family: "hygiene", supplier: "soap", base: 190, moq: 24, cover: 0.65, on_order: 48 },
  { sku: "BG-610", family: "waste", supplier: "poly", base: 640, moq: 50, cover: 1.2, on_order: 0 },
  { sku: "BG-625", family: "waste", supplier: "poly", base: 360, moq: 50, cover: 1.0, on_order: 0, event: "one_off" },
  { sku: "FC-110", family: "chemicals", supplier: "chem", base: 150, moq: 12, cover: 1.3, on_order: 0 },
  { sku: "WR-220", family: "chemicals", supplier: "chem", base: 170, moq: 12, cover: 0.95, on_order: 0 },
  { sku: "MP-150", family: "chemicals", supplier: "chem", base: 140, moq: 12, cover: 1.8, on_order: 0 },
  { sku: "GV-800", family: "ppe", supplier: "ppe", base: 420, moq: 20, cover: 1.1, on_order: 0 },
  { sku: "CL-915", family: "ppe", supplier: "ppe", base: 210, moq: 20, cover: 0.9, on_order: 0 },
];

/** "Yes company" one-off lines: special orders that must not drive core POs. */
export const ONE_OFF_SKUS = ["OV-255", "LD-265", "FS-825"];

export interface DemandRow {
  sku: string;
  month: string;
  packs: number;
  /** Share of the month's packs that came from the single largest order. */
  top_order_share: number;
  note?: string;
}

function build(): DemandRow[] {
  const rand = rng(20261101);
  const rows: DemandRow[] = [];
  for (const c of CORE) {
    DEMAND_MONTHS.forEach((m, t) => {
      const cal = Number(m.slice(5, 7)) - 1;
      let v = c.base * SEASON[c.family][cal] * Math.pow(1.12, t / 12) * (0.95 + 0.1 * rand());
      let note: string | undefined;
      let share = 0.03 + 0.06 * rand();
      if (c.event === "step_up" && m >= "2026-09") {
        v *= 1.35;
        note = "New contract: Meridian Student Living added 11 halls (demo)";
      }
      if (c.event === "one_off" && m === "2026-10") {
        v *= 1.85;
        share = 0.46;
        note = "One-off: Castlegate Arena event clean-up order (demo)";
      }
      rows.push({ sku: c.sku, month: m, packs: Math.round(v), top_order_share: Math.round(share * 100) / 100, note });
    });
  }
  for (const sku of ONE_OFF_SKUS) {
    DEMAND_MONTHS.forEach((m) => {
      if (rand() < 0.22) rows.push({ sku, month: m, packs: 1 + Math.floor(rand() * 6), top_order_share: 1, note: "Special order for one customer" });
    });
  }
  return rows;
}

export const DEMAND: DemandRow[] = build();

export interface StockPosition {
  sku: string;
  on_hand: number;
  on_order: number;
  /** ETA for on_order, ISO date. */
  eta: string | null;
  last_count: string;
}

export const STOCK: StockPosition[] = CORE.map((c) => {
  const last3 = DEMAND.filter((r) => r.sku === c.sku).slice(-3);
  const avg = last3.reduce((a, r) => a + r.packs, 0) / 3;
  return {
    sku: c.sku,
    on_hand: Math.round(avg * c.cover),
    on_order: c.on_order,
    eta: c.on_order ? "2026-11-06" : null,
    last_count: "2026-10-03",
  };
});

/** Unit cost to Source (demo: 62% of list price ex VAT). */
export function unitCost(sku: string): number {
  const p = PRODUCT_BY_SKU[sku];
  return Math.round((p?.price_gbp ?? 10) * 0.62 * 100) / 100;
}

export const DEMAND_DOCS: SeedDoc[] = [
  ...DEMAND.map((r) => {
    const p = PRODUCT_BY_SKU[r.sku];
    const core = CORE.find((c) => c.sku === r.sku);
    return {
      id: `ss-dem-${r.sku}-${r.month}`,
      text: `Despatched ${r.packs} packs of ${r.sku} ${p?.name ?? ""} in ${r.month}.${r.note ? ` ${r.note}.` : ""} Demo demand record.`,
      metadata: {
        kind: "demand",
        sku: r.sku,
        month: r.month,
        packs: r.packs,
        top_order_share: r.top_order_share,
        family: core?.family ?? "special",
        core: Boolean(core),
        note: r.note ?? "",
      },
    };
  }),
  ...STOCK.map((s) => {
    const c = CORE.find((x) => x.sku === s.sku)!;
    const sup = SUPPLIERS[c.supplier];
    return {
      id: `ss-stk-${s.sku}`,
      text: `Estimated stock for ${s.sku} ${PRODUCT_BY_SKU[s.sku]?.name ?? ""}: ${s.on_hand} on hand, ${s.on_order} on order. Supplier ${sup.name}, lead time ${sup.lead_days} days, MOQ ${c.moq}. Demo stock record.`,
      metadata: {
        kind: "stock",
        ...s,
        eta: s.eta ?? "",
        family: c.family,
        supplier: sup.name,
        lead_days: sup.lead_days,
        moq: c.moq,
        unit_cost: unitCost(s.sku),
      },
    };
  }),
];

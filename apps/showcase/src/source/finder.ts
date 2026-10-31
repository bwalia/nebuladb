/**
 * Pure logic for the Green Product Finder: SQL building, near-miss
 * reasons, weighted ranking and the eco-swap sums. No React, no I/O, so
 * it is easy to read next to the SQL it explains.
 */
import { lit, semanticText } from "../rag/sql";
import { BADGES, SECTOR_LABEL, exhaustiveLimit, type Badge, type Sector } from "./data/common";
import { PRODUCT_DOCS, costPerUse, type Product } from "./data/products";

export interface FinderFilters {
  sector: Sector | "";
  badge: Badge | "";
  maxPrice: string;
  concentrateOnly: boolean;
  inStockOnly: boolean;
}

export const NO_FILTERS: FinderFilters = { sector: "", badge: "", maxPrice: "", concentrateOnly: false, inStockOnly: false };

/** LIMIT large enough that LIMIT × 4 candidates covers the whole products bucket. */
export const PRODUCTS_LIMIT = exhaustiveLimit(PRODUCT_DOCS.length);

export function finderSql(query: string, f: FinderFilters): string {
  let sql = `SELECT * FROM ss_products\n WHERE semantic_match(text, ${lit(semanticText(query))})`;
  if (f.badge) sql += `\n   AND array_contains(badges, ${lit(f.badge)})`;
  if (f.sector) sql += `\n   AND array_contains(sectors, ${lit(f.sector)})`;
  if (f.concentrateOnly) sql += `\n   AND concentrate = ${lit(true)}`;
  if (f.inStockOnly) sql += `\n   AND in_stock = ${lit(true)}`;
  const max = Number(f.maxPrice);
  if (f.maxPrice !== "" && Number.isFinite(max)) sql += `\n   AND price_gbp <= ${lit(max)}`;
  return sql + `\n LIMIT ${PRODUCTS_LIMIT}`;
}

/** The same meaning, no hard filters: the pool near misses come from. */
export function nearMissSql(query: string): string {
  return `SELECT * FROM ss_products\n WHERE semantic_match(text, ${lit(semanticText(query))})\n LIMIT 40`;
}

/** Why a product failed the hard filters, in plain words. Empty = it passes. */
export function missReasons(p: Product, f: FinderFilters): string[] {
  const out: string[] = [];
  if (f.badge && !p.badges.includes(f.badge)) out.push(`No ${BADGES[f.badge]} badge`);
  if (f.sector && !p.sectors.includes(f.sector)) out.push(`Not listed for ${SECTOR_LABEL[f.sector].toLowerCase()}`);
  if (f.inStockOnly && !p.in_stock) out.push(`Out of stock, back in ${p.lead_days} day${p.lead_days === 1 ? "" : "s"}`);
  if (f.concentrateOnly && !p.concentrate) out.push("Not a concentrate");
  const max = Number(f.maxPrice);
  if (f.maxPrice !== "" && Number.isFinite(max) && p.price_gbp > max) out.push(`£${p.price_gbp.toFixed(2)} is over your £${max} limit`);
  return out;
}

export interface Weights {
  semantic: number;
  eco: number;
  cost: number;
  lead: number;
}

export const DEFAULT_WEIGHTS: Weights = { semantic: 40, eco: 25, cost: 20, lead: 15 };

export const FACETS: Array<{ key: keyof Weights; label: string; hint: string }> = [
  { key: "semantic", label: "Fit", hint: "How close the product text is to what you asked for (semantic_match score, spread 0–1)" },
  { key: "eco", label: "Eco", hint: "Number of eco badges, against the best in this list" },
  { key: "cost", label: "Cost per use", hint: "Price ÷ uses per pack (litres in use for chemicals). Cheaper is better" },
  { key: "lead", label: "Lead time", hint: "Days until it can arrive. Sooner is better" },
];

export interface Scored {
  product: Product;
  sim: number;
  facets: Record<keyof Weights, number>;
  total: number;
}

function spread(values: number[]): (v: number) => number {
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  if (!Number.isFinite(lo) || hi - lo < 1e-9) return () => 1;
  return (v) => (v - lo) / (hi - lo);
}

/** Score each hit on the four facets (0..1, higher is better). */
export function scoreHits(hits: Array<{ product: Product; sim: number }>): Omit<Scored, "total">[] {
  if (hits.length === 0) return [];
  const sem = spread(hits.map((h) => h.sim));
  const maxBadges = Math.max(1, ...hits.map((h) => h.product.badges.length));
  // Cost per use spans 0.02 £/L to thousands per machine: compare on a log scale.
  const logCost = hits.map((h) => Math.log10(Math.max(1e-4, costPerUse(h.product))));
  const cost = spread(logCost);
  const lead = spread(hits.map((h) => h.product.lead_days));
  return hits.map((h, i) => ({
    product: h.product,
    sim: h.sim,
    facets: {
      semantic: sem(h.sim),
      eco: h.product.badges.length / maxBadges,
      cost: 1 - cost(logCost[i]),
      lead: 1 - lead(h.product.lead_days),
    },
  }));
}

export function composite(s: Omit<Scored, "total">, w: Weights): number {
  const sum = w.semantic + w.eco + w.cost + w.lead || 1;
  return (w.semantic * s.facets.semantic + w.eco * s.facets.eco + w.cost * s.facets.cost + w.lead * s.facets.lead) / sum;
}

export interface Swap {
  from: Product;
  to: Product;
  /** Packs of `from` bought per year at the current volume. */
  packsPerYear: number;
  /** Whole packs of `to` needed for the same amount of use. */
  newPacks: number;
  savingGbp: number;
  savingKg: number;
  volumeSource: "orders" | "assumed";
}

/** Swap `from` → `to` at the same amount of use (litres in use, rolls, sheets…). */
export function swapSaving(from: Product, to: Product, packsPerYear: number, volumeSource: Swap["volumeSource"]): Swap {
  const use = packsPerYear * from.uses_per_pack;
  const newPacks = packsPerYear === 0 ? 0 : Math.max(1, Math.ceil(use / Math.max(1e-9, to.uses_per_pack)));
  return {
    from,
    to,
    packsPerYear,
    newPacks,
    savingGbp: packsPerYear * from.price_gbp - newPacks * to.price_gbp,
    savingKg: packsPerYear * from.co2e_kg_per_unit - newPacks * to.co2e_kg_per_unit,
    volumeSource,
  };
}

/** Default volume when no account is picked: one pack a month. */
export const ASSUMED_PACKS_PER_YEAR = 12;

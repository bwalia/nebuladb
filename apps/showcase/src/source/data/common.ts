/**
 * Shared constants for the Source Supplies demo corpus.
 *
 * Everything under `source/data` is DEMO DATA: products, SKUs, prices,
 * stock, customers, sites, orders, tickets, tenders and safety data
 * sheets are all invented. Partner brand names appear only as a
 * `brand_family` label on paper and dispenser lines (ground rule 4).
 *
 * Files here use `import type` only, so Node can load them directly
 * (`node --test`) for the consistency checks.
 */
import type { SeedDoc } from "../../rag/seed";

export type { SeedDoc };

/** Bucket names. Every Source Supplies bucket is prefixed `ss_`. */
export const B = {
  products: "ss_products",
  sds: "ss_sds",
  guides: "ss_guides",
  accounts: "ss_accounts",
  orders: "ss_orders",
  washroom: "ss_washroom_jobs",
  tender: "ss_tender_kb",
  drafts: "ss_order_drafts",
} as const;

/**
 * The demo calendar is fixed so "last 30 days" and "next visit due"
 * read the same on every run. Orders cover May–October 2026.
 */
export const DEMO_TODAY = "2026-10-31";
export const ORDER_MONTHS = ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"] as const;
export const NEXT_MONTH = "2026-11";

export const SECTORS = [
  "education",
  "higher-education",
  "care",
  "healthcare",
  "office",
  "industrial",
  "leisure",
] as const;
export type Sector = (typeof SECTORS)[number];

export const SECTOR_LABEL: Record<Sector, string> = {
  education: "Schools",
  "higher-education": "Colleges and universities",
  care: "Care homes",
  healthcare: "Healthcare",
  office: "Offices",
  industrial: "Industrial",
  leisure: "Stadia and leisure",
};

/** Eco badge slugs, matching the shop's filter list. */
export const BADGES = {
  "b-corp": "B Corp",
  "carbon-neutral-gold": "Carbon Neutral Zone Gold",
  "cruelty-free": "Cruelty Free International",
  ecocert: "ECOCERT",
  "eu-ecolabel": "EU Flower (EU Ecolabel)",
  fsc: "FSC",
  "global-greentag": "Global GreenTag",
  "made-in-britain": "Made in Britain",
  pefc: "PEFC",
  "planet-mark": "Planet Mark",
  "nordic-swan": "Nordic Swan",
  vegan: "Vegan Society",
} as const;
export type Badge = keyof typeof BADGES;
export const BADGE_SLUGS = Object.keys(BADGES) as Badge[];

export const CATEGORIES = ["chemicals", "paper", "washroom", "waste", "machines", "PPE", "equipment"] as const;
export type Category = (typeof CATEGORIES)[number];

/**
 * Filters run after retrieval over LIMIT × 4 candidates (at least 32).
 * For an exhaustive filtered lookup — "every order line for this site" —
 * LIMIT × 4 must cover the whole bucket, or rows are silently missed.
 */
export function exhaustiveLimit(bucketSize: number): number {
  return Math.ceil(bucketSize / 4) + 8;
}

/** Small deterministic PRNG (mulberry32) so generated demo data is stable. */
export function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function slug(s: string): string {
  return s
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

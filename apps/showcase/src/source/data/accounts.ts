/**
 * `ss_accounts` and `ss_orders` — six invented trade accounts with 3–5
 * sites each, and six months of order lines per site.
 *
 * None of these are real Source Supplies customers. Names were made up
 * for the demo. Orders are generated from a fixed seed, so every run
 * produces the same lines, and only use SKUs that exist in
 * `ss_products`.
 */
import { B, ORDER_MONTHS, rng, slug, type SeedDoc, type Sector } from "./common.ts";
import { PRODUCT_BY_SKU } from "./products.ts";

export const SEED_VERSION = "ss-accounts-v1";
export const ACCOUNTS_BUCKET = B.accounts;
export const ORDERS_BUCKET = B.orders;

export type Service = "sanitary" | "nappy" | "air-care" | "dryer" | "refills";
export const SERVICES: Service[] = ["sanitary", "nappy", "air-care", "dryer", "refills"];

export interface Site {
  account_id: string;
  account_name: string;
  site: string;
  sector: Sector;
  postcode_area: string;
  delivery_day: "Monday" | "Tuesday" | "Wednesday" | "Thursday" | "Friday";
  plan: string;
  /** Relative size: scales order volumes. */
  size: number;
  washroom_services: Service[];
  /** Days between Source Washroom visits under the contract. */
  washroom_cycle_days: number;
}

interface AccountDef {
  id: string;
  name: string;
  sector: Sector;
  plan: string;
  /** SKU → packs per month at size 1. */
  basket: Record<string, number>;
  sites: Array<[string, string, Site["delivery_day"], number, Service[], number]>;
}

const ACCOUNT_DEFS: AccountDef[] = [
  {
    id: "BRIGHTWAY",
    name: "Brightway FM",
    sector: "office",
    plan: "FM contract",
    basket: { "FC-010": 2, "WR-020": 4, "MP-050": 4, "JR-400": 6, "HT-300": 5, "HS-060": 2 },
    sites: [
      ["Reading Gateway", "RG", "Tuesday", 3, ["sanitary", "air-care", "dryer"], 28],
      ["Basingstoke Park", "RG", "Tuesday", 2, ["sanitary", "air-care"], 28],
      ["Woking Central", "GU", "Thursday", 2, ["sanitary", "dryer"], 28],
      ["Guildford Riverside", "GU", "Thursday", 1, ["sanitary"], 28],
    ],
  },
  {
    id: "KINGSMEAD",
    name: "Kingsmead Academy Trust",
    sector: "education",
    plan: "Education framework",
    basket: { "FC-110": 2, "WR-220": 2, "JR-410": 8, "HT-300": 6, "HS-270": 3, "BG-610": 3 },
    sites: [
      ["North site", "SL", "Monday", 3, ["sanitary", "nappy", "refills"], 14],
      ["South site", "SL", "Monday", 2, ["sanitary", "refills"], 14],
      ["St Mary's Primary", "SL", "Wednesday", 1, ["sanitary", "nappy"], 28],
      ["Sixth Form Centre", "SL", "Wednesday", 1, ["sanitary", "dryer"], 28],
    ],
  },
  {
    id: "HARBOUR",
    name: "Harbour Care Homes",
    sector: "care",
    plan: "Care group contract",
    basket: { "SA-197": 2, "LD-260": 1, "EN-180": 1, "TR-425": 2, "HT-310": 3, "AP-830": 3 },
    sites: [
      ["Seaview House", "PO", "Wednesday", 2, ["sanitary", "nappy", "air-care"], 14],
      ["Elm Lodge", "PO", "Wednesday", 1, ["sanitary", "air-care"], 14],
      ["Quay Court", "SO", "Friday", 2, ["sanitary", "nappy"], 14],
    ],
  },
  {
    id: "MERIDIAN",
    name: "Meridian Student Living",
    sector: "higher-education",
    plan: "Accommodation contract",
    basket: { "MR-160": 2, "WR-020": 3, "FC-110": 1, "TR-425": 3, "HT-300": 3, "BG-600": 3 },
    sites: [
      ["Canal Halls", "KT", "Monday", 3, ["sanitary", "refills"], 28],
      ["Park Halls", "KT", "Monday", 2, ["sanitary"], 28],
      ["Riverside Studios", "TW", "Tuesday", 1, ["sanitary"], 28],
      ["Station Court", "TW", "Tuesday", 2, ["sanitary", "dryer"], 28],
      ["Abbey Halls", "KT", "Friday", 1, ["sanitary"], 28],
    ],
  },
  {
    id: "CASTLEGATE",
    name: "Castlegate Leisure",
    sector: "leisure",
    plan: "Leisure contract",
    basket: { "FC-150": 2, "WR-220": 3, "JR-400": 6, "HT-310": 5, "HS-270": 3, "GR-170": 1 },
    sites: [
      ["Castlegate Arena", "GU", "Friday", 3, ["sanitary", "nappy", "dryer", "air-care"], 14],
      ["Northfield Leisure Centre", "GU", "Thursday", 2, ["sanitary", "nappy", "dryer"], 14],
      ["Westbrook Pool", "RG", "Thursday", 1, ["sanitary", "nappy"], 14],
    ],
  },
  {
    id: "ASHBY",
    name: "Ashby Contract Cleaning",
    sector: "industrial",
    plan: "Contract cleaner",
    basket: { "FC-150": 2, "KD-130": 1, "BR-360": 3, "BG-625": 3, "GV-800": 6, "CR-350": 3 },
    sites: [
      ["Farnborough Depot", "GU", "Tuesday", 2, ["sanitary"], 28],
      ["Slough Distribution Hub", "SL", "Wednesday", 3, ["sanitary", "dryer"], 28],
      ["Bracknell Works", "RG", "Wednesday", 1, ["sanitary"], 28],
      ["Camberley Yard", "GU", "Friday", 1, ["sanitary"], 28],
    ],
  },
];

export const SITES: Site[] = ACCOUNT_DEFS.flatMap((a) =>
  a.sites.map(([site, postcode_area, delivery_day, size, washroom_services, washroom_cycle_days]) => ({
    account_id: a.id,
    account_name: a.name,
    site,
    sector: a.sector,
    postcode_area,
    delivery_day,
    plan: a.plan,
    size,
    washroom_services,
    washroom_cycle_days,
  }))
);

export const ACCOUNTS = ACCOUNT_DEFS.map((a) => ({ id: a.id, name: a.name, sector: a.sector }));

export function siteKey(accountId: string, site: string): string {
  return `${accountId}/${site}`;
}

export const ACCOUNT_DOCS: SeedDoc[] = SITES.map((s) => ({
  id: `ss-acct-${s.account_id}-${slug(s.site)}`,
  text:
    `Trade account ${s.account_name} (${s.account_id}), site ${s.site}. Sector: ${s.sector}. ` +
    `Postcode area ${s.postcode_area}. Delivery day: ${s.delivery_day}. Plan: ${s.plan}. ` +
    `Source Washroom services: ${s.washroom_services.join(", ")} every ${s.washroom_cycle_days} days. Demo account record.`,
  metadata: { ...s },
}));

export interface OrderLine {
  account_id: string;
  account_name: string;
  site: string;
  sku: string;
  qty: number;
  month: string;
  po_ref: string;
}

function buildOrders(): OrderLine[] {
  const rand = rng(20261031);
  const out: OrderLine[] = [];
  for (const a of ACCOUNT_DEFS) {
    a.sites.forEach(([site, , , size], si) => {
      for (const month of ORDER_MONTHS) {
        const po = `PO-${a.id.slice(0, 3)}-${month.replace("-", "")}-${si + 1}`;
        for (const [sku, base] of Object.entries(a.basket)) {
          let qty = Math.max(1, Math.round(base * size * (0.85 + 0.3 * rand())));
          // A visible spike for the "why did toilet roll go up?" story.
          if (a.id === "KINGSMEAD" && site === "North site" && sku === "JR-410" && month === "2026-10") {
            qty = Math.round(qty * 1.7);
          }
          out.push({ account_id: a.id, account_name: a.name, site, sku, qty, month, po_ref: po });
        }
      }
    });
  }
  return out;
}

export const ORDERS: OrderLine[] = buildOrders();

export const ORDER_DOCS: SeedDoc[] = ORDERS.map((o) => {
  const p = PRODUCT_BY_SKU[o.sku];
  return {
    id: `ss-ord-${o.account_id}-${slug(o.site)}-${o.month}-${o.sku}`,
    text:
      `Order line ${o.po_ref}: ${o.account_name}, ${o.site}, ${o.month}. ` +
      `${o.qty} × ${o.sku} ${p?.name ?? ""} (${p?.pack ?? ""}). Demo order record.`,
    metadata: { ...o },
  };
});

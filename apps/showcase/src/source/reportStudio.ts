/**
 * Jordan Report Studio — the engine behind `ssreports`.
 *
 * Discovery call: "Merlin… you can't get hold of them… a real simple
 * report… third attempt… three or four days apart." and "can you just
 * send me the report for [customer] for the last 3 months combined".
 *
 * Plain English → a typed report plan (shown to the user, so nothing is
 * hidden) → ONE NebulaDB SQL query → figures computed here in code, never
 * by a model → insights, CSV for Excel, an email draft and a schedule.
 * Follow-ups ("now just paper", "by month") refine the previous plan.
 *
 * Pure functions: `node --test` loads this file directly.
 */
import { B, ORDER_MONTHS, exhaustiveLimit, type Category } from "./data/common.ts";
import { ORDER_DOCS } from "./data/accounts.ts";
import { PRODUCTS, PRODUCT_BY_SKU } from "./data/products.ts";

/** Same quoting as rag/sql `lit()` (inlined so node --test can load this file). */
function lit(v: string): string {
  return `'${v.replace(/'/g, "''")}'`;
}

export type GroupBy = "site" | "product" | "month" | "category";
export type Measure = "spend" | "qty";

export interface Account {
  id: string;
  name: string;
}

export interface ReportPlan {
  accountId: string;
  accountName: string;
  months: string[];
  periodLabel: string;
  prevMonths: string[];
  groupBy: GroupBy;
  category: Category | null;
  top: number | null;
  measure: Measure;
}

export interface Parsed {
  plan: ReportPlan | null;
  /** What Jordan understood, one chip each. */
  understood: string[];
  /** Why it could not build a plan. */
  missing: string | null;
}

const MONTHS = [...ORDER_MONTHS] as string[];
const MONTH_NAMES = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];
const GENERIC = new Set(["contract", "cleaning", "homes", "home", "academy", "trust", "living", "leisure", "care", "student", "fm", "the", "and"]);

const CATEGORY_WORDS: [RegExp, Category][] = [
  [/\b(paper|toilet roll|tissue|towels?|centrefeed|blue roll)\b/, "paper"],
  [/\b(chemicals?|cleaners?|cleaning fluids?|detergents?|sanitisers?)\b/, "chemicals"],
  [/\b(washroom|soap|dispensers?|air fresheners?)\b/, "washroom"],
  [/\b(waste|bin liners?|sacks|bags)\b/, "waste"],
  [/\b(ppe|gloves|aprons|masks|goggles)\b/, "PPE"],
  [/\b(machines?|scrubbers?|vacuums?)\b/, "machines"],
  [/\b(equipment|mops?|buckets?|cloths?)\b/, "equipment"],
];

function ymName(ym: string): string {
  return new Date(`${ym}-01T00:00:00Z`).toLocaleDateString("en-GB", { month: "short", year: "numeric", timeZone: "UTC" });
}

function span(months: string[]): string {
  if (months.length === 0) return "—";
  return months.length === 1 ? ymName(months[0]) : `${ymName(months[0])} – ${ymName(months[months.length - 1])}`;
}

function prevOf(months: string[]): string[] {
  const i = MONTHS.indexOf(months[0]);
  if (i < months.length) return [];
  return MONTHS.slice(i - months.length, i);
}

function parsePeriod(q: string): { months: string[]; label: string } | null {
  const last = MONTHS.length - 1;
  if (/\b(last|previous) quarter\b|\bq3\b/.test(q)) return { months: ["2026-07", "2026-08", "2026-09"], label: "Last quarter (Q3)" };
  if (/\b(this|current) quarter\b|\bq4\b/.test(q)) return { months: ["2026-10"], label: "This quarter to date (Q4)" };
  if (/\b(last|past) (6|six) months\b|\bhalf[- ]year\b|\ball (time|history)\b|\beverything\b/.test(q)) return { months: [...MONTHS], label: "Last 6 months" };
  if (/\b(last|past) (3|three) months\b|\b(3|three) months\b/.test(q)) return { months: MONTHS.slice(-3), label: "Last 3 months" };
  if (/\b(last|past) (2|two) months\b/.test(q)) return { months: MONTHS.slice(-2), label: "Last 2 months" };
  if (/\blast month\b/.test(q)) return { months: [MONTHS[last - 1]], label: "Last month" };
  if (/\bthis month\b/.test(q)) return { months: [MONTHS[last]], label: "This month" };
  const since = /\bsince (\w+)/.exec(q);
  if (since) {
    const mi = MONTH_NAMES.indexOf(since[1]);
    const i = MONTHS.findIndex((m) => Number(m.slice(5, 7)) - 1 === mi);
    if (i >= 0) return { months: MONTHS.slice(i), label: `Since ${ymName(MONTHS[i])}` };
  }
  const named = MONTH_NAMES.map((n, mi) => ({ n, mi })).filter(({ n }) => new RegExp(`\\b${n}\\b`).test(q));
  if (named.length) {
    const picked = MONTHS.filter((m) => named.some(({ mi }) => Number(m.slice(5, 7)) - 1 === mi));
    if (picked.length) return { months: picked, label: span(picked) };
  }
  return null;
}

export function matchAccount(q: string, accounts: Account[]): Account | null {
  for (const a of accounts) {
    if (q.includes(a.id.toLowerCase())) return a;
    const words = a.name.toLowerCase().split(/\s+/).filter((w) => !GENERIC.has(w));
    if (words.some((w) => w.length > 3 && new RegExp(`\\b${w}`).test(q))) return a;
  }
  return null;
}

export function parseRequest(text: string, accounts: Account[], prev?: ReportPlan | null): Parsed {
  const q = text.toLowerCase();
  const understood: string[] = [];
  const acct = matchAccount(q, accounts);
  if (!acct && !prev) return { plan: null, understood, missing: "Which customer? Try a name like Ashby, Kingsmead or Brightway." };
  const account = acct ?? { id: prev!.accountId, name: prev!.accountName };
  understood.push(`Customer: ${account.name}${acct ? "" : " (same as before)"}`);

  const period = parsePeriod(q) ?? (prev ? { months: prev.months, label: prev.periodLabel } : { months: MONTHS.slice(-3), label: "Last 3 months" });
  understood.push(`Period: ${period.label} (${span(period.months)})`);

  let groupBy: GroupBy = prev?.groupBy ?? "site";
  if (/\b(by|per|each|every) (site|location|building)s?\b|\beach of them\b|\bsites?\b.*\beach\b/.test(q)) groupBy = "site";
  else if (/\b(by|per) (product|sku|item|line)s?\b|\bcombined\b|\bwhat (products|items)\b|\btop \d+ (products|items|lines)\b/.test(q)) groupBy = "product";
  else if (/\b(by|per) month\b|\bmonthly\b|\btrend\b|\bmonth by month\b/.test(q)) groupBy = "month";
  else if (/\b(by|per) (category|type)\b/.test(q)) groupBy = "category";
  understood.push(`Grouped by ${groupBy}`);

  let category: Category | null = prev?.category ?? null;
  if (/\b(all|every) (products|categories|lines)\b|\beverything\b/.test(q)) category = null;
  for (const [re, c] of CATEGORY_WORDS)
    if (re.test(q)) {
      category = c;
      break;
    }
  if (category) understood.push(`Only ${category}`);

  const topM = /\btop (\d{1,2})\b/.exec(q);
  const top = topM ? Number(topM[1]) : acct || parsePeriod(q) ? null : prev?.top ?? null;
  if (top) understood.push(`Top ${top}`);

  const measure: Measure = /\b(how many|quantit(y|ies)|units|packs|cases|volume)\b/.test(q) ? "qty" : prev && !acct ? prev.measure : "spend";
  understood.push(measure === "spend" ? "Measure: spend £ ex VAT" : "Measure: packs");

  const prevMonths = prevOf(period.months);
  if (prevMonths.length) understood.push(`Compared with ${span(prevMonths)}`);

  return {
    plan: { accountId: account.id, accountName: account.name, months: period.months, periodLabel: period.label, prevMonths, groupBy, category, top, measure },
    understood,
    missing: null,
  };
}

export const ORDERS_LIMIT = exhaustiveLimit(ORDER_DOCS.length);

export function categorySkus(c: Category): string[] {
  return PRODUCTS.filter((p) => p.category === c).map((p) => p.sku);
}

/** The one SQL query a plan needs (current + comparison months together). */
export function planSql(p: ReportPlan): string {
  const months = [...p.prevMonths, ...p.months].map(lit).join(", ");
  const hint = `order lines ${p.accountName}${p.category ? ` ${p.category}` : ""}`;
  let sql = `SELECT * FROM ${B.orders}\n WHERE semantic_match(text, ${lit(hint)})\n   AND account_id = ${lit(p.accountId)}\n   AND month IN (${months})`;
  if (p.category) sql += `\n   AND sku IN (${categorySkus(p.category).map(lit).join(", ")})`;
  return `${sql}\n LIMIT ${ORDERS_LIMIT}`;
}

/* ---------------- Building the report ---------------- */

export interface OrderRow {
  site: string;
  sku: string;
  month: string;
  qty: number;
  po_ref: string;
}

export interface Group {
  key: string;
  label: string;
  spend: number;
  qty: number;
  prevSpend: number;
  prevQty: number;
  share: number;
}

export interface Report {
  plan: ReportPlan;
  totals: { spend: number; qty: number; orders: number; sites: number; skus: number; prevSpend: number; deltaPct: number | null };
  groups: Group[];
  byMonth: { month: string; value: number }[];
  insights: string[];
  co2SavingKg: number;
}

const price = (sku: string) => PRODUCT_BY_SKU[sku]?.price_gbp ?? 0;
const round2 = (n: number) => Math.round(n * 100) / 100;
const gbp = (n: number) => `£${n.toLocaleString("en-GB", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

function groupKey(r: OrderRow, g: GroupBy): [string, string] {
  if (g === "site") return [r.site, r.site];
  if (g === "month") return [r.month, ymName(r.month)];
  if (g === "category") {
    const c = PRODUCT_BY_SKU[r.sku]?.category ?? "other";
    return [c, c];
  }
  return [r.sku, `${r.sku} ${PRODUCT_BY_SKU[r.sku]?.name ?? ""}`.trim()];
}

export function buildReport(p: ReportPlan, rows: OrderRow[]): Report {
  const cur = rows.filter((r) => p.months.includes(r.month));
  const prv = rows.filter((r) => p.prevMonths.includes(r.month));
  const val = (r: OrderRow) => (p.measure === "spend" ? r.qty * price(r.sku) : r.qty);

  const map = new Map<string, Group>();
  const touch = (r: OrderRow) => {
    const [key, label] = groupKey(r, p.groupBy);
    const g = map.get(key) ?? { key, label, spend: 0, qty: 0, prevSpend: 0, prevQty: 0, share: 0 };
    map.set(key, g);
    return g;
  };
  for (const r of cur) {
    const g = touch(r);
    g.spend += r.qty * price(r.sku);
    g.qty += r.qty;
  }
  if (p.groupBy !== "month")
    for (const r of prv) {
      const g = touch(r);
      g.prevSpend += r.qty * price(r.sku);
      g.prevQty += r.qty;
    }
  const spend = cur.reduce((a, r) => a + r.qty * price(r.sku), 0);
  const prevSpend = prv.reduce((a, r) => a + r.qty * price(r.sku), 0);
  const measureTotal = cur.reduce((a, r) => a + val(r), 0) || 1;
  let groups = [...map.values()]
    .filter((g) => g.qty > 0)
    .map((g) => ({ ...g, spend: round2(g.spend), prevSpend: round2(g.prevSpend), share: (p.measure === "spend" ? g.spend : g.qty) / measureTotal }));
  groups.sort(p.groupBy === "month" ? (a, b) => a.key.localeCompare(b.key) : (a, b) => (p.measure === "spend" ? b.spend - a.spend : b.qty - a.qty));
  if (p.top) groups = groups.slice(0, p.top);

  const byMonth = p.months.map((m) => ({ month: m, value: round2(cur.filter((r) => r.month === m).reduce((a, r) => a + val(r), 0)) }));
  const deltaPct = prevSpend > 0 ? Math.round(((spend - prevSpend) / prevSpend) * 1000) / 10 : null;

  /* insights — all computed, each one sentence */
  const insights: string[] = [];
  const g0 = groups[0];
  if (g0 && p.groupBy !== "month") insights.push(`${g0.label} is ${Math.round(g0.share * 100)}% of the total (${p.measure === "spend" ? gbp(g0.spend) : `${g0.qty} packs`}).`);
  if (deltaPct !== null) insights.push(`Spend is ${deltaPct >= 0 ? "up" : "down"} ${Math.abs(deltaPct)}% on the previous ${p.months.length} month${p.months.length > 1 ? "s" : ""} (${gbp(prevSpend)} → ${gbp(spend)}).`);
  // Biggest SKU mover vs previous period.
  if (prv.length) {
    const d = new Map<string, number>();
    for (const r of cur) d.set(r.sku, (d.get(r.sku) ?? 0) + r.qty * price(r.sku));
    for (const r of prv) d.set(r.sku, (d.get(r.sku) ?? 0) - r.qty * price(r.sku));
    const mover = [...d.entries()].sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))[0];
    if (mover && Math.abs(mover[1]) > 1) insights.push(`Biggest mover: ${mover[0]} ${PRODUCT_BY_SKU[mover[0]]?.name ?? ""} (${mover[1] >= 0 ? "+" : "−"}${gbp(Math.abs(mover[1]))}).`);
  }
  // Spikes: a site × SKU month ≥ 50% above its other months in the window.
  const all = [...prv, ...cur];
  const bySiteSku = new Map<string, OrderRow[]>();
  for (const r of all) {
    const k = `${r.site}|${r.sku}`;
    bySiteSku.set(k, [...(bySiteSku.get(k) ?? []), r]);
  }
  for (const list of bySiteSku.values()) {
    for (const r of list) {
      if (!p.months.includes(r.month)) continue;
      const others = list.filter((x) => x !== r);
      if (others.length < 2) continue;
      const avg = others.reduce((a, x) => a + x.qty, 0) / others.length;
      if (r.qty >= avg * 1.5 && r.qty - avg >= 3) {
        insights.push(`Spike: ${r.site} took ${r.qty} × ${r.sku} in ${ymName(r.month)}, ${Math.round((r.qty / avg - 1) * 100)}% above its usual ${Math.round(avg)} — worth a call.`);
      }
    }
  }
  // Greener swaps on what they actually bought.
  let co2 = 0;
  let swapSpend = 0;
  const swapSkus = new Set<string>();
  for (const r of cur) {
    const greener = PRODUCTS.find((x) => x.replaces_sku === r.sku);
    const old = PRODUCT_BY_SKU[r.sku];
    if (greener && old) {
      co2 += (old.co2e_kg_per_unit - greener.co2e_kg_per_unit) * r.qty;
      swapSpend += r.qty * old.price_gbp;
      swapSkus.add(r.sku);
    }
  }
  if (co2 > 1) insights.push(`Greener swaps exist for ${swapSkus.size} line${swapSkus.size > 1 ? "s" : ""} (${gbp(swapSpend)} of spend) — switching would save about ${Math.round(co2)} kg CO2e for this period.`);

  return {
    plan: p,
    totals: {
      spend: round2(spend),
      qty: cur.reduce((a, r) => a + r.qty, 0),
      orders: new Set(cur.map((r) => r.po_ref)).size,
      sites: new Set(cur.map((r) => r.site)).size,
      skus: new Set(cur.map((r) => r.sku)).size,
      prevSpend: round2(prevSpend),
      deltaPct,
    },
    groups,
    byMonth,
    insights,
    co2SavingKg: Math.round(co2),
  };
}

/* ---------------- Outputs ---------------- */

function csvCell(v: string | number): string {
  const s = String(v);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/** Excel-ready CSV of the grouped figures plus totals. */
export function toCsv(r: Report): string {
  const head = [r.plan.groupBy, "Spend GBP ex VAT", "Packs", "Share %"];
  if (r.plan.prevMonths.length && r.plan.groupBy !== "month") head.push("Previous spend GBP", "Change %");
  const lines = [head.map(csvCell).join(",")];
  for (const g of r.groups) {
    const row: (string | number)[] = [g.label, g.spend.toFixed(2), g.qty, (g.share * 100).toFixed(1)];
    if (head.length > 4) row.push(g.prevSpend.toFixed(2), g.prevSpend ? (((g.spend - g.prevSpend) / g.prevSpend) * 100).toFixed(1) : "");
    lines.push(row.map(csvCell).join(","));
  }
  lines.push(["Total", r.totals.spend.toFixed(2), r.totals.qty, "100.0"].map(csvCell).join(","));
  return lines.join("\n");
}

export function emailDraft(r: Report): string {
  const p = r.plan;
  const top = r.groups.slice(0, 5).map((g) => `  • ${g.label}: ${gbp(g.spend)} (${g.qty} packs)`).join("\n");
  return [
    `Subject: ${p.accountName} — purchasing report, ${p.periodLabel.toLowerCase()} (${span(p.months)})`,
    "",
    `Hi,`,
    "",
    `Here is your purchasing summary for ${span(p.months)}${p.category ? `, ${p.category} only` : ""}.`,
    `Total spend ${gbp(r.totals.spend)} ex VAT across ${r.totals.sites} site${r.totals.sites === 1 ? "" : "s"} and ${r.totals.orders} order${r.totals.orders === 1 ? "" : "s"}${r.totals.deltaPct !== null ? ` (${r.totals.deltaPct >= 0 ? "+" : ""}${r.totals.deltaPct}% on the previous period)` : ""}.`,
    "",
    `By ${p.groupBy}:`,
    top,
    "",
    ...r.insights.slice(-1).filter((i) => i.startsWith("Greener")).map((i) => `${i}\n`),
    `The full breakdown is attached as a spreadsheet.`,
    "",
    `Kind regards,`,
    `Source Supplies`,
  ].join("\n");
}

/** Last Friday of the month after `iso` — when a monthly report would go out. */
export function nextLastFriday(iso: string): string {
  const d = new Date(`${iso}T00:00:00Z`);
  for (let k = 0; k < 2; k++) {
    const end = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1 + k, 0));
    while (end.getUTCDay() !== 5) end.setUTCDate(end.getUTCDate() - 1);
    if (end.toISOString().slice(0, 10) > iso) return end.toISOString().slice(0, 10);
  }
  return iso;
}

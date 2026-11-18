/**
 * Stock Autopilot — the forecasting and ordering engine behind `ssstock`.
 * Pure functions (no React, no fetch) so `node --test` can load it.
 *
 * Dave's current method (from the call): paste three months of Merlin
 * sales into ChatGPT and order the average. Jordan's method, explained
 * in every row's "why":
 *   1. Clean one-offs: a month far above expectation where one order is
 *      most of the volume is a one-off ("yes company" special or event)
 *      and is not baked into the forecast.
 *   2. Detect step-ups: two months in a row well above the run-rate that
 *      are NOT one-offs mean a new contract — the level moves up.
 *   3. Seasonality from every month of history (school holidays, winter
 *      hygiene peak, Christmas waste), plus year-on-year growth.
 *   4. Safety stock from the model's own forecast error and the supplier
 *      lead time, so popular lines stop running out.
 *   5. Estimated stock position (Merlin has no live stock), stock-out
 *      date, MOQ-rounded order, grouped into one draft PO per supplier.
 */

export const Z_95 = 1.65;
export const REVIEW_DAYS = 30;
/** Forecast error never assumed below ±6% (supplier and data noise). */
export const SIGMA_FLOOR = 0.06;

export interface Cleaned {
  values: number[];
  outliers: { index: number; packs: number; expected: number; share: number }[];
}

export interface SkuHistory {
  sku: string;
  months: string[];
  packs: number[];
  /** Share of each month's packs from the single largest order (0..1). */
  topShare: number[];
}

const mean = (a: number[]) => (a.length ? a.reduce((x, y) => x + y, 0) / a.length : 0);
const cal = (ym: string) => Number(ym.slice(5, 7)) - 1;
const median = (a: number[]) => {
  const s = [...a].sort((x, y) => x - y);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};

/** Replace one-off spikes (one big order, far above expectation) with the expected value. */
export function cleanOneOffs(h: SkuHistory): Cleaned {
  const v = [...h.packs];
  const outliers: Cleaned["outliers"] = [];
  for (let t = 1; t < v.length; t++) {
    const expected = t >= 12 ? h.packs[t - 12] * 1.12 : median(h.packs.slice(Math.max(0, t - 3), t));
    if (expected <= 0) continue;
    if (h.packs[t] / expected > 1.4 && (h.topShare[t] ?? 0) > 0.35) {
      outliers.push({ index: t, packs: h.packs[t], expected: Math.round(expected), share: h.topShare[t] });
      v[t] = Math.round(expected);
    }
  }
  return { values: v, outliers };
}

/** Seasonal index per calendar month (mean 1), from a 12-month moving window. */
export function seasonalIndex(values: number[], months: string[]): number[] {
  const n = values.length;
  const sums = Array(12).fill(0);
  const counts = Array(12).fill(0);
  if (n < 12) return Array(12).fill(1);
  for (let t = 0; t < n; t++) {
    const start = Math.min(Math.max(0, t - 6), n - 12);
    const base = mean(values.slice(start, start + 12));
    if (base <= 0) continue;
    sums[cal(months[t])] += values[t] / base;
    counts[cal(months[t])]++;
  }
  const raw = sums.map((s, i) => (counts[i] ? s / counts[i] : 1));
  const m = mean(raw);
  return raw.map((x) => x / m);
}

export interface Forecast {
  /** Next `horizon` months. */
  months: string[];
  values: number[];
  naive: number;
  level: number;
  growth: number;
  index: number[];
  sigmaRel: number;
  stepUp: { from: string; pct: number } | null;
  cleaned: Cleaned;
}

function addMonths(ym: string, k: number): string {
  const d = new Date(Date.UTC(Number(ym.slice(0, 4)), Number(ym.slice(5, 7)) - 1 + k, 1));
  return d.toISOString().slice(0, 7);
}

export function forecast(h: SkuHistory, horizon = 3): Forecast {
  const n = h.packs.length;
  const cleaned = cleanOneOffs(h);
  const raw = cleaned.values;
  const naive = mean(h.packs.slice(-3));

  // Step-up via year-on-year ratios (seasonality cancels out): the last two
  // months both ≥ 20% above the YoY run-rate of the three before them.
  let stepUp: Forecast["stepUp"] = null;
  if (n >= 17) {
    const yoy = (t: number) => raw[t] / raw[t - 12];
    const before = mean([yoy(n - 5), yoy(n - 4), yoy(n - 3)]);
    const last2 = [yoy(n - 2), yoy(n - 1)];
    if (before > 0 && last2.every((x) => x / before > 1.2)) {
      stepUp = { from: h.months[n - 2], pct: Math.round((mean(last2) / before - 1) * 100) };
    }
  }
  const lift = stepUp ? 1 + stepUp.pct / 100 : 1;
  // Fit seasonality and growth on the series with the step taken out.
  const v = raw.map((x, t) => (stepUp && t >= n - 2 ? x / lift : x));
  const idx = seasonalIndex(v, h.months);
  const d = v.map((x, t) => x / idx[cal(h.months[t])]);
  let growth = 0.1;
  if (n >= 15) {
    const recent = mean(d.slice(n - 3, n));
    const yearAgo = mean(d.slice(n - 15, n - 12));
    if (yearAgo > 0) growth = Math.min(0.5, Math.max(-0.3, recent / yearAgo - 1));
  }
  const level = mean(d.slice(n - 3)) * lift;
  const last = h.months[n - 1];
  const months = Array.from({ length: horizon }, (_, k) => addMonths(last, k + 1));
  const values = months.map((m, k) => Math.round(level * Math.pow(1 + growth, (k + 2) / 12) * idx[cal(m)]));

  // In-sample one-step error over the last 12 months (relative).
  const errs: number[] = [];
  for (let t = Math.max(3, n - 12); t < n; t++) {
    const pred = mean(d.slice(t - 3, t)) * Math.pow(1 + growth, 2 / 12) * idx[cal(h.months[t])];
    if (pred > 0) errs.push(v[t] / pred - 1);
  }
  const mu = mean(errs);
  const sigmaRel = Math.max(SIGMA_FLOOR, Math.sqrt(mean(errs.map((e) => (e - mu) ** 2))));
  return { months, values, naive: Math.round(naive), level, growth, index: idx, sigmaRel, stepUp, cleaned };
}

/* ---------------- Plan for today ---------------- */

export interface SkuInput extends SkuHistory {
  name: string;
  supplier: string;
  lead_days: number;
  moq: number;
  unit_cost: number;
  on_hand: number;
  on_order: number;
  eta: string | null;
}

export type Risk = "urgent" | "watch" | "ok" | "overstock";

export interface SkuPlan {
  input: SkuInput;
  fc: Forecast;
  safety: number;
  reorderPoint: number;
  daysCover: number;
  stockoutDate: string | null;
  risk: Risk;
  orderQty: number;
  naiveOrderQty: number;
  orderValue: number;
  why: string[];
}

function daysIn(ym: string): number {
  return new Date(Date.UTC(Number(ym.slice(0, 4)), Number(ym.slice(5, 7)), 0)).getUTCDate();
}

function addDays(iso: string, k: number): string {
  const d = new Date(`${iso}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + k);
  return d.toISOString().slice(0, 10);
}

export function plan(input: SkuInput, today: string): SkuPlan {
  const fc = forecast(input);
  const daily = (iso: string) => {
    const m = iso.slice(0, 7);
    const i = fc.months.indexOf(m);
    const monthly = i >= 0 ? fc.values[i] : fc.values[fc.values.length - 1];
    return monthly / daysIn(m);
  };
  // Walk forward day by day from tomorrow.
  let stock = input.on_hand;
  let stockoutDate: string | null = null;
  let demandLT = 0;
  let demandCover = 0;
  const lt = input.lead_days;
  for (let k = 1; k <= 90; k++) {
    const day = addDays(today, k);
    if (input.eta && day === input.eta) stock += input.on_order;
    const dd = daily(day);
    stock -= dd;
    if (k <= lt) demandLT += dd;
    if (k <= lt + REVIEW_DAYS) demandCover += dd;
    if (stock < 0 && !stockoutDate) stockoutDate = day;
  }
  const monthly = fc.values[0];
  const safety = Math.round(Z_95 * fc.sigmaRel * monthly * Math.sqrt((lt + REVIEW_DAYS) / 30));
  const reorderPoint = Math.round(demandLT + safety);
  const position = input.on_hand + input.on_order;
  const target = demandCover + safety;
  const orderQty = Math.max(0, Math.ceil((target - position) / input.moq) * input.moq);
  const naiveTarget = (fc.naive * (lt + REVIEW_DAYS)) / 30;
  const naiveOrderQty = Math.max(0, Math.ceil((naiveTarget - position) / input.moq) * input.moq);
  const daysCover = Math.round(input.on_hand / daily(addDays(today, 1)));

  let risk: Risk = "ok";
  if (stockoutDate && stockoutDate <= addDays(today, lt)) risk = "urgent";
  else if (position < reorderPoint) risk = "watch";
  else if (position > target * 1.6) risk = "overstock";

  const pct = (x: number) => `${x >= 0 ? "+" : ""}${Math.round(x * 100)}%`;
  const why: string[] = [];
  why.push(`3-month average says ${fc.naive.toLocaleString("en-GB")} a month; Jordan forecasts ${fc.values.map((x) => x.toLocaleString("en-GB")).join(" → ")} for ${fc.months.map(shortMonth).join(", ")}.`);
  const si = fc.index[Number(fc.months[0].slice(5, 7)) - 1];
  if (Math.abs(si - 1) >= 0.04) why.push(`${shortMonth(fc.months[0])} is usually ${pct(si - 1)} vs an average month for this line (seasonality from 24 months).`);
  why.push(`Growth ${pct(fc.growth)} year on year.`);
  if (fc.stepUp) why.push(`Step-up detected from ${shortMonth(fc.stepUp.from)} (${pct(fc.stepUp.pct / 100)}) — looks like a new contract, so the level moves up.`);
  for (const o of fc.cleaned.outliers)
    why.push(`${shortMonth(input.months[o.index])} spike ignored: ${o.packs.toLocaleString("en-GB")} vs ~${o.expected.toLocaleString("en-GB")} expected, ${Math.round(o.share * 100)}% from one order (one-off).`);
  why.push(`Safety stock ${safety.toLocaleString("en-GB")} (95% service, forecast error ±${Math.round(fc.sigmaRel * 100)}%, ${lt}-day lead time).`);
  if (stockoutDate) why.push(`At this rate, stock runs out on ${ukDay(stockoutDate)}${risk === "urgent" ? ` — before a ${lt}-day order could land` : ""}.`);
  if (orderQty !== naiveOrderQty)
    why.push(`Order ${orderQty.toLocaleString("en-GB")} (MOQ ${input.moq}); the 3-month-average method would order ${naiveOrderQty.toLocaleString("en-GB")}.`);

  return {
    input,
    fc,
    safety,
    reorderPoint,
    daysCover,
    stockoutDate,
    risk,
    orderQty,
    naiveOrderQty,
    orderValue: Math.round(orderQty * input.unit_cost * 100) / 100,
    why,
  };
}

export function shortMonth(ym: string): string {
  return new Date(`${ym}-01T00:00:00Z`).toLocaleDateString("en-GB", { month: "short", timeZone: "UTC" });
}
export function ukDay(iso: string): string {
  return new Date(`${iso}T00:00:00Z`).toLocaleDateString("en-GB", { weekday: "short", day: "numeric", month: "short", timeZone: "UTC" });
}

/* ---------------- Supplier POs ---------------- */

export interface DraftPo {
  supplier: string;
  lines: { sku: string; name: string; qty: number; value: number; risk: Risk }[];
  total: number;
  deliverBy: string;
  urgent: boolean;
}

export function draftPos(plans: SkuPlan[], today: string): DraftPo[] {
  const by = new Map<string, DraftPo>();
  for (const p of plans) {
    if (p.orderQty <= 0) continue;
    const s = p.input.supplier;
    const po = by.get(s) ?? { supplier: s, lines: [], total: 0, deliverBy: addDays(today, p.input.lead_days), urgent: false };
    po.lines.push({ sku: p.input.sku, name: p.input.name, qty: p.orderQty, value: p.orderValue, risk: p.risk });
    po.total = Math.round((po.total + p.orderValue) * 100) / 100;
    po.urgent ||= p.risk === "urgent";
    by.set(s, po);
  }
  return [...by.values()].sort((a, b) => Number(b.urgent) - Number(a.urgent) || b.total - a.total);
}

/* ---------------- Backtest: last 12 months, both methods ---------------- */

export interface PolicyResult {
  stockoutMonths: number;
  shortPacks: number;
  avgStockValue: number;
}

export interface Backtest {
  months: string[];
  naive: PolicyResult;
  jordan: PolicyResult;
}

/** Buffer a sensible buyer adds on top of the plain average. */
export const NAIVE_BUFFER = 1.1;

export function backtest(input: SkuInput, months = 12): Backtest {
  const n = input.packs.length;
  const run = (policy: "naive" | "jordan"): PolicyResult => {
    let leftover = 0;
    let stockoutMonths = 0;
    let shortPacks = 0;
    let held = 0;
    for (let t = n - months; t < n; t++) {
      const hist: SkuHistory = { sku: input.sku, months: input.months.slice(0, t), packs: input.packs.slice(0, t), topShare: input.topShare.slice(0, t) };
      let target: number;
      if (policy === "naive") target = mean(hist.packs.slice(-3)) * NAIVE_BUFFER;
      else {
        const f = forecast(hist, 1);
        target = f.values[0] * (1 + Z_95 * f.sigmaRel);
      }
      const available = Math.max(leftover, target);
      const actual = input.packs[t];
      const short = Math.max(0, actual - available);
      if (short > 0) stockoutMonths++;
      shortPacks += short;
      leftover = Math.max(0, available - actual);
      held += leftover;
    }
    return { stockoutMonths, shortPacks: Math.round(shortPacks), avgStockValue: Math.round((held / months) * input.unit_cost) };
  };
  return { months: input.months.slice(n - months), naive: run("naive"), jordan: run("jordan") };
}

/** Demo figures for turning short packs into money. */
export const SHORTFALL = { packsPerRedelivery: 5, redeliveryCost: 28, marginPerPack: 4.2 };

export function shortfallCost(shortPacks: number): { redeliveries: number; pounds: number } {
  const redeliveries = Math.ceil(shortPacks / SHORTFALL.packsPerRedelivery);
  return { redeliveries, pounds: Math.round(redeliveries * SHORTFALL.redeliveryCost + shortPacks * SHORTFALL.marginPerPack) };
}

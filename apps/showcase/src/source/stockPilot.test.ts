import { test } from "node:test";
import assert from "node:assert/strict";
import { CORE, DEMAND, DEMAND_MONTHS, STOCK, SUPPLIERS, unitCost } from "./data/demand.ts";
import { PRODUCT_BY_SKU } from "./data/products.ts";
import { backtest, draftPos, forecast, plan, seasonalIndex, type SkuInput } from "./stockPilot.ts";

function input(sku: string): SkuInput {
  const c = CORE.find((x) => x.sku === sku)!;
  const rows = DEMAND.filter((r) => r.sku === sku);
  const st = STOCK.find((s) => s.sku === sku)!;
  const sup = SUPPLIERS[c.supplier];
  return {
    sku, months: DEMAND_MONTHS, packs: rows.map((r) => r.packs), topShare: rows.map((r) => r.top_order_share),
    name: PRODUCT_BY_SKU[sku].name, supplier: sup.name, lead_days: sup.lead_days, moq: c.moq, unit_cost: unitCost(sku),
    on_hand: st.on_hand, on_order: st.on_order, eta: st.eta,
  };
}

test("seasonal index has mean 1 and sees the August school-holiday dip in paper", () => {
  const i = input("JR-400");
  const idx = seasonalIndex(i.packs, i.months);
  assert.ok(Math.abs(idx.reduce((a, b) => a + b, 0) / 12 - 1) < 1e-9);
  assert.ok(idx[7] < 0.85, `Aug index ${idx[7]}`);
});

test("a new contract is a step-up, a one-big-order month is a one-off", () => {
  const step = forecast(input("JR-410"));
  assert.ok(step.stepUp && step.stepUp.from === "2026-09");
  const oneOff = forecast(input("BG-625"));
  assert.equal(oneOff.stepUp, null);
  assert.equal(oneOff.cleaned.outliers.length, 1);
  assert.ok(oneOff.values[0] < oneOff.naive, "one-off must not inflate the forecast");
});

test("today's plan: the three short lines are urgent, orders are MOQ multiples, POs group by supplier", () => {
  const plans = CORE.map((c) => plan(input(c.sku), "2026-10-31"));
  const urgent = plans.filter((p) => p.risk === "urgent").map((p) => p.input.sku).sort();
  assert.deepEqual(urgent, ["HS-270", "HT-310", "JR-410"]);
  for (const p of plans) assert.equal(p.orderQty % p.input.moq, 0);
  const pos = draftPos(plans, "2026-10-31");
  assert.equal(new Set(pos.map((p) => p.supplier)).size, pos.length);
  assert.ok(pos[0].urgent);
});

test("backtest: Jordan has fewer stock-out months and holds less stock than the 3-month average", () => {
  let n = { s: 0, v: 0 }, j = { s: 0, v: 0 };
  for (const c of CORE) {
    const b = backtest(input(c.sku));
    n = { s: n.s + b.naive.stockoutMonths, v: n.v + b.naive.avgStockValue };
    j = { s: j.s + b.jordan.stockoutMonths, v: j.v + b.jordan.avgStockValue };
  }
  assert.ok(j.s < n.s / 2, `stockouts ${j.s} vs ${n.s}`);
  assert.ok(j.v < n.v, `stock held ${j.v} vs ${n.v}`);
});

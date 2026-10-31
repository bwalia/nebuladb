import { test } from "node:test";
import assert from "node:assert/strict";
import { daysUntil, forecastLines, guessCategory, makesDelivery, matchSite, nextDelivery, parseSwitches } from "./reorder.ts";
import { ORDERS, SITES } from "./data/accounts.ts";
import { ORDER_MONTHS } from "./data/common.ts";

test("forecast is the 3-month average rounded up, and flags a >50% spike", () => {
  const months = ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"];
  const lines = [10, 10, 10, 10, 10, 18].map((qty, i) => ({ sku: "JR-410", month: months[i], qty }));
  const [row] = forecastLines(lines, months);
  assert.equal(row.forecast, Math.ceil((10 + 10 + 18) / 3));
  assert.equal(row.spike, true);
  assert.match(row.reason, /80% above/);
  const [flat] = forecastLines([10, 10, 10, 10, 10, 11].map((qty, i) => ({ sku: "X", month: months[i], qty })), months);
  assert.equal(flat.spike, false);
});

test("the seeded Kingsmead North toilet-roll spike is flagged", () => {
  const lines = ORDERS.filter((o) => o.account_id === "KINGSMEAD" && o.site === "North site");
  const rows = forecastLines(lines, ORDER_MONTHS);
  assert.equal(rows.find((r) => r.sku === "JR-410")?.spike, true);
  assert.equal(rows.filter((r) => r.spike).length, 1);
});

test("switch requests are parsed into phrases with a category", () => {
  const p = parseSwitches("Same as last month for Kingsmead, but switch to recycled hand towels.");
  assert.deepEqual(p, ["recycled hand towels"]);
  assert.equal(guessCategory(p[0]), "paper");
  assert.deepEqual(parseSwitches("swap the floor cleaner for a concentrate and switch to recycled bin liners"), ["concentrate", "recycled bin liners"]);
  assert.equal(guessCategory("concentrate"), "chemicals");
  assert.deepEqual(parseSwitches("same as last month"), []);
});

test("account and site are matched from free text, including apostrophes", () => {
  assert.deepEqual(matchSite("Draft next month's order for Kingsmead Academy, North site.", SITES), { account: "KINGSMEAD", site: "North site" });
  assert.deepEqual(matchSite("same as last month for St Mary's Primary", SITES), { account: "KINGSMEAD", site: "St Mary's Primary" });
  assert.deepEqual(matchSite("Brightway please", SITES), { account: "BRIGHTWAY" });
  assert.deepEqual(matchSite("nothing here", SITES), {});
});

test("delivery day split", () => {
  assert.equal(nextDelivery("2026-10-31", "Monday"), "2026-11-02");
  assert.equal(nextDelivery("2026-10-31", "Saturday"), "2026-11-07");
  assert.equal(daysUntil("2026-10-31", "2026-11-02"), 2);
  assert.equal(makesDelivery({ in_stock: true, stock_qty: 50, lead_days: 1 }, 10, 2), true);
  assert.equal(makesDelivery({ in_stock: true, stock_qty: 50, lead_days: 3 }, 10, 2), false);
  assert.equal(makesDelivery({ in_stock: true, stock_qty: 5, lead_days: 1 }, 10, 2), false);
  assert.equal(makesDelivery({ in_stock: false, stock_qty: 0, lead_days: 1 }, 1, 2), false);
});

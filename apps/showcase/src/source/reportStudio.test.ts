import { test } from "node:test";
import assert from "node:assert/strict";
import { ACCOUNTS, ORDERS } from "./data/accounts.ts";
import { buildReport, emailDraft, nextLastFriday, parseRequest, planSql, toCsv, type OrderRow } from "./reportStudio.ts";

const rowsFor = (id: string): OrderRow[] => ORDERS.filter((o) => o.account_id === id);

test("Dave's request: a contract cleaner, last 3 months combined", () => {
  const { plan, understood } = parseRequest("Send me the report for Ashby Contract Cleaning for the last 3 months combined", ACCOUNTS);
  assert.ok(plan);
  assert.equal(plan.accountId, "ASHBY");
  assert.deepEqual(plan.months, ["2026-08", "2026-09", "2026-10"]);
  assert.equal(plan.groupBy, "product");
  assert.deepEqual(plan.prevMonths, ["2026-05", "2026-06", "2026-07"]);
  assert.ok(understood.some((u) => u.startsWith("Customer: Ashby")));
  const sql = planSql(plan);
  assert.match(sql, /account_id = 'ASHBY'/);
  assert.match(sql, /semantic_match\(text, /);
});

test("follow-ups refine the previous plan", () => {
  const first = parseRequest("What did Kingsmead buy last quarter by site?", ACCOUNTS).plan!;
  assert.deepEqual(first.months, ["2026-07", "2026-08", "2026-09"]);
  const paper = parseRequest("now just paper, by month", ACCOUNTS, first).plan!;
  assert.equal(paper.accountId, "KINGSMEAD");
  assert.equal(paper.category, "paper");
  assert.equal(paper.groupBy, "month");
  assert.deepEqual(paper.months, first.months);
  assert.equal(parseRequest("hello", ACCOUNTS).plan, null);
});

test("report figures add up, and the Kingsmead North toilet-roll spike is called out", () => {
  const plan = parseRequest("Kingsmead last 3 months by site", ACCOUNTS).plan!;
  const r = buildReport(plan, rowsFor("KINGSMEAD"));
  const sum = r.groups.reduce((a, g) => a + g.spend, 0);
  assert.ok(Math.abs(sum - r.totals.spend) < 0.05);
  assert.ok(r.insights.some((i) => /Spike: North site .*JR-410/.test(i)), r.insights.join("\n"));
  assert.match(toCsv(r), /^site,Spend GBP ex VAT/);
  assert.match(emailDraft(r), /Subject: Kingsmead/);
});

test("monthly schedule runs on the last Friday", () => {
  assert.equal(nextLastFriday("2026-10-31"), "2026-11-27");
  assert.equal(nextLastFriday("2026-11-01"), "2026-11-27");
});

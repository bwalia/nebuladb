import { test } from "node:test";
import assert from "node:assert/strict";
import { DAY, EXCEPTIONS, ADDRESS_ALIAS, RETAILER_ITEMS, UNMAPPED_ITEMS } from "./data/retail.ts";
import { editDistance, intakeOrders, matchPods, roiFor, type RetailRule } from "./retailPilot.ts";

test("edit distance counts an adjacent transposition as one slip", () => {
  assert.equal(editDistance("PL482105516", "PL482150516"), 1);
  assert.equal(editDistance("abc", "abc"), 0);
});

test("a demo day: 30 despatches, the fixed messy cases land where expected", () => {
  const m = matchPods(DAY.despatches, DAY.pods);
  assert.equal(m.length, 30);
  assert.equal(m[EXCEPTIONS.fuzzy].via, "fuzzy");
  assert.equal(m[EXCEPTIONS.fuzzy].status, "attached");
  assert.equal(m[EXCEPTIONS.owed].status, "attached_followup");
  assert.equal(m[EXCEPTIONS.failed].status, "exception");
  assert.equal(m[EXCEPTIONS.safePlace].status, "exception");
  assert.equal(m[EXCEPTIONS.address].status, "exception");
  assert.equal(m[EXCEPTIONS.inTransit].status, "waiting");
  const clean = m.filter((x) => x.status === "attached" && x.via === "po").length;
  assert.equal(clean, 24);
});

test("teaching one address alias clears that exception on the next run", () => {
  const rules: RetailRule[] = [{ kind: "address_alias", ...ADDRESS_ALIAS }];
  const m = matchPods(DAY.despatches, DAY.pods, rules);
  assert.equal(m[EXCEPTIONS.address].status, "attached");
  const r = roiFor(m);
  assert.equal(r.exceptions, 2);
  assert.ok(r.poundsPerYear > 15000 && r.poundsPerYear < 30000, String(r.poundsPerYear));
});

test("intake maps known codes, suggests a SKU for new codes and drops the duplicate PO", () => {
  const known = Object.fromEntries(RETAILER_ITEMS.map((i) => [i.code, i.sku]));
  const catalogue = [
    { sku: "CR-350", name: "Blue Centrefeed Roll 2-ply", text: "blue centrefeed roll for wiping" },
    { sku: "WF-935", name: "Wet Floor Signs", text: "yellow wet floor warning signs" },
    { sku: "BR-360", name: "Industrial Wiper Roll", text: "wiper roll" },
  ];
  const res = intakeOrders(DAY.portal, known, [], () => catalogue);
  assert.equal(res.length, 13);
  assert.equal(res.filter((r) => r.status === "duplicate").length, 1);
  const flat = res.flatMap((r) => r.lines);
  for (const u of UNMAPPED_ITEMS) assert.equal(flat.find((l) => l.code === u.code)?.sku, u.expectSku);
  const learned = intakeOrders(DAY.portal, known, UNMAPPED_ITEMS.map((u) => ({ kind: "item_alias" as const, code: u.code, sku: u.expectSku })), () => []);
  assert.equal(learned.filter((r) => r.status === "needs_review").length, 0);
});

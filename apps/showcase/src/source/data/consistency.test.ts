/**
 * Consistency checks for the Source Supplies demo corpus.
 * Run with `npm test` (Node's built-in runner, no extra dependencies).
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { PRODUCTS, PRODUCT_BY_SKU, PRODUCT_DOCS } from "./products.ts";
import { SDS_DOCS, SDS_PROFILES, DEMO_BANNER } from "./sds.ts";
import { GUIDES, GUIDE_DOCS } from "./guides.ts";
import { SITES, ORDERS, ORDER_DOCS, ACCOUNT_DOCS, siteKey } from "./accounts.ts";
import { JOBS, JOB_DOCS, MISSED } from "./washroom.ts";
import { KB, TENDER_DOCS, TOPICS } from "./tender.ts";
import { SEEDS } from "./index.ts";
import { BADGE_SLUGS, ORDER_MONTHS, exhaustiveLimit } from "./common.ts";

const siteKeys = new Set(SITES.map((s) => siteKey(s.account_id, s.site)));

test("bucket sizes are in the ranges the spec asks for", () => {
  assert.ok(PRODUCTS.length >= 110 && PRODUCTS.length <= 130, `products: ${PRODUCTS.length}`);
  assert.equal(SDS_PROFILES.length, 10);
  assert.equal(SDS_DOCS.length, 160);
  assert.ok(GUIDE_DOCS.length >= 38 && GUIDE_DOCS.length <= 45, `guides: ${GUIDE_DOCS.length}`);
  const accounts = new Set(SITES.map((s) => s.account_id));
  assert.equal(accounts.size, 6);
  for (const a of accounts) {
    const n = SITES.filter((s) => s.account_id === a).length;
    assert.ok(n >= 3 && n <= 5, `${a} has ${n} sites`);
  }
  assert.ok(JOBS.length >= 75 && JOBS.length <= 90, `jobs: ${JOBS.length}`);
  assert.equal(KB.length, 60);
});

test("SKUs are unique and every cross-reference points at a real SKU", () => {
  assert.equal(new Set(PRODUCTS.map((p) => p.sku)).size, PRODUCTS.length);
  for (const p of PRODUCTS) {
    if (p.replaces_sku) {
      assert.ok(PRODUCT_BY_SKU[p.replaces_sku], `${p.sku} replaces unknown ${p.replaces_sku}`);
      assert.notEqual(p.replaces_sku, p.sku);
    }
    for (const b of p.badges) assert.ok(BADGE_SLUGS.includes(b), `${p.sku}: unknown badge ${b}`);
    assert.equal(p.in_stock, p.stock_qty > 0, `${p.sku}: in_stock must follow stock_qty`);
  }
  for (const o of ORDERS) assert.ok(PRODUCT_BY_SKU[o.sku], `order uses unknown SKU ${o.sku}`);
  for (const g of GUIDES) for (const c of g.chunks) for (const s of c.skus) assert.ok(PRODUCT_BY_SKU[s], `${g.id} names unknown SKU ${s}`);
  for (const p of SDS_PROFILES) assert.ok(PRODUCT_BY_SKU[p.sku], `SDS for unknown SKU ${p.sku}`);
});

test("orders and washroom jobs only use real accounts and sites", () => {
  for (const o of ORDERS) assert.ok(siteKeys.has(siteKey(o.account_id, o.site)), `order for unknown site ${o.account_id}/${o.site}`);
  for (const j of JOBS) assert.ok(siteKeys.has(siteKey(j.account_id, j.site)), `job for unknown site ${j.account_id}/${j.site}`);
  for (const k of Object.keys(MISSED)) assert.ok(siteKeys.has(k), `missed visit for unknown site ${k}`);
  for (const o of ORDERS) assert.ok((ORDER_MONTHS as readonly string[]).includes(o.month));
  // Every site orders every month.
  for (const s of SITES) {
    const months = new Set(ORDERS.filter((o) => o.account_id === s.account_id && o.site === s.site).map((o) => o.month));
    assert.equal(months.size, ORDER_MONTHS.length, `${s.site} is missing months`);
  }
});

test("brand names only label paper and dispenser lines, with no claims attached", () => {
  const replaced = new Set(PRODUCTS.map((p) => p.replaces_sku).filter(Boolean));
  for (const p of PRODUCTS) {
    if (!p.brand_family) continue;
    assert.ok(p.category === "paper" || (p.category === "washroom" && p.sku.startsWith("DP-")), `${p.sku} brand on a non-paper, non-dispenser line`);
    assert.equal(p.badges.length, 0, `${p.sku}: branded line must carry no badges`);
    assert.equal(p.replaces_sku, undefined, `${p.sku}: branded line must not be a swap`);
    assert.ok(!replaced.has(p.sku), `${p.sku}: branded line must not be swapped out`);
    assert.ok(!SDS_PROFILES.some((s) => s.sku === p.sku), `${p.sku}: no safety data next to a brand`);
  }
});

test("every SDS section is marked as a demo and each product has all 16 sections", () => {
  for (const p of SDS_PROFILES) {
    const sections = SDS_DOCS.filter((d) => d.metadata.sku === p.sku).map((d) => d.metadata.section);
    assert.deepEqual([...sections].sort((a, b) => Number(a) - Number(b)), Array.from({ length: 16 }, (_, i) => i + 1));
  }
  for (const d of SDS_DOCS) assert.ok(d.text.startsWith(DEMO_BANNER), `${d.id} lacks the demo banner`);
  // No real-looking hazard or transport codes.
  for (const d of SDS_DOCS) assert.ok(!/\b[HP]\d{3}\b|\bUN ?\d{4}\b/.test(d.text), `${d.id} has a real-looking code`);
});

test("tender KB covers every topic, with won and lost past answers", () => {
  for (const t of TOPICS) assert.ok(KB.some((k) => k.topic === t), `no KB entry for ${t}`);
  const past = KB.filter((k) => k.kind === "past-answer");
  assert.ok(past.some((k) => k.won) && past.some((k) => k.won === false));
});

test("doc ids are unique per bucket and metadata has no nulls", () => {
  for (const s of Object.values(SEEDS)) {
    assert.equal(new Set(s.docs.map((d) => d.id)).size, s.docs.length, `${s.bucket} has duplicate ids`);
    for (const d of s.docs) {
      for (const [k, v] of Object.entries(d.metadata)) assert.ok(v !== null && v !== undefined, `${d.id}.${k} is empty`);
      assert.ok(d.text.length > 20);
    }
  }
  assert.equal(PRODUCT_DOCS.length, PRODUCTS.length);
  assert.equal(ACCOUNT_DOCS.length, SITES.length);
  assert.equal(ORDER_DOCS.length, ORDERS.length);
  assert.equal(JOB_DOCS.length, JOBS.length);
  assert.equal(TENDER_DOCS.length, KB.length);
});

test("exhaustive lookups cover the whole bucket (filters run over LIMIT × 4 candidates)", () => {
  for (const s of Object.values(SEEDS)) {
    assert.ok(exhaustiveLimit(s.docs.length) * 4 >= s.docs.length, s.bucket);
  }
});

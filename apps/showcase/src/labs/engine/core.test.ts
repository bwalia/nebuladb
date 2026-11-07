import { test } from "node:test";
import assert from "node:assert/strict";
import {
  Bm25,
  claims,
  filterSql,
  labHash,
  lit,
  numbersIn,
  overlapAt,
  parseLabHash,
  pca,
  reciprocalRank,
  score,
  tokenize,
  unsupportedNumbers,
} from "./core.ts";
import type { Pack } from "../types.ts";

test("tokenize keeps codes and non-Latin scripts", () => {
  assert.deepEqual(tokenize("ENG2041 failed: E_RATE_LIMIT!"), ["eng2041", "failed", "e_rate_limit"]);
  assert.deepEqual(tokenize("गेहूं में पीला रोग"), ["गेहूं", "में", "पीला", "रोग"]);
  assert.deepEqual(tokenize("ਕਣਕ ਦੀ ਬਿਜਾਈ"), ["ਕਣਕ", "ਦੀ", "ਬਿਜਾਈ"]);
});

test("bm25 ranks the exact-code doc first and explains terms", () => {
  const idx = new Bm25([
    { id: "a", text: "Module ENG2041 covers fluid dynamics." },
    { id: "b", text: "Fluid dynamics is also taught in physics." },
    { id: "c", text: "Gardening in spring." },
  ]);
  const hits = idx.search("ENG2041");
  assert.equal(hits[0].id, "a");
  assert.ok(hits[0].terms["eng2041"] > 0);
  assert.equal(idx.search("unrelated words").length, 0);
});

test("metrics", () => {
  assert.equal(reciprocalRank(["x", "y", "a"], ["a"]), 1 / 3);
  const s = score([
    { ranked: ["a", "b"], expect: ["a"] },
    { ranked: ["b", "c"], expect: ["a"] },
  ]);
  assert.equal(s.hit1, 0.5);
  assert.equal(s.mrr, 0.5);
  assert.equal(overlapAt(["a", "b", "c"], ["a", "c", "d"], 3), 2 / 3);
});

test("lab URL state round-trips, including apostrophes and filters", () => {
  const s = { lab: "filters" as const, pack: "carehome", q: "St Brigid's falls", k: 10, ef: 128, persona: "carer-elm", f: { home: "St Brigid's House" } };
  const back = parseLabHash(labHash(s));
  assert.deepEqual(back, { ...s, models: undefined, mode: undefined });
  assert.equal(parseLabHash("#/blog/x"), null);
  assert.equal(parseLabHash("#/labs")?.lab, null);
});

test("filter SQL escapes literals and uses the right operator per filter", () => {
  const pack = {
    bucket: "lab_x",
    filters: [
      { field: "home", label: "Home", op: "eq", values: ["St Brigid's House", "Elm Lodge"] },
      { field: "tags", label: "Tags", op: "contains", values: ["falls", "skin"] },
      { field: "price", label: "Max price", op: "max" },
      { field: "in_stock", label: "In stock", op: "bool" },
    ],
  } as unknown as Pack;
  const sql = filterSql(pack, "what's new", { home: "St Brigid's House", tags: "falls", price: "30", in_stock: "true" }, 200);
  assert.match(sql, /semantic_match\(text, 'what''s new'\)/);
  assert.match(sql, /home = 'St Brigid''s House'/);
  assert.match(sql, /array_contains\(tags, 'falls'\)/);
  assert.match(sql, /price <= 30/);
  assert.match(sql, /in_stock = TRUE/);
  assert.match(sql, /LIMIT 200$/);
  assert.equal(filterSql(pack, "q", { price: "30; DROP" }, 5).includes("DROP"), false);
  assert.equal(lit("O'Neill"), "'O''Neill'");
});

test("numbers in an answer must come from the evidence", () => {
  assert.deepEqual(numbersIn("£1,250.50 and 92% [1] [2, 3]"), ["1250.5", "92"]);
  const evidence = ["Average modal price 5,412 INR; min 5300.", "Rain 12.4 mm on Thursday"];
  assert.deepEqual(unsupportedNumbers("Average 5412 INR, min 5300, rain 12 mm [1].", evidence), []);
  assert.deepEqual(unsupportedNumbers("Average 5500 INR [1].", evidence), ["5500"]);
  assert.deepEqual(unsupportedNumbers("Step 2: wait.", evidence), []);
});

test("claims split sentences and parse citations", () => {
  const c = claims("Use the falls policy [1]. Call the GP within 24 hours [2,3]. Be kind.");
  assert.deepEqual(c.map((x) => x.cites), [[1], [2, 3], []]);
});

test("pca projects to two dims and separates two clusters", () => {
  const rows = [
    [1, 0, 0],
    [0.9, 0.1, 0],
    [0, 0, 1],
    [0, 0.1, 0.9],
  ];
  const p = pca(rows, 2);
  assert.equal(p.length, 4);
  assert.equal(p[0].length, 2);
  assert.ok(Math.sign(p[0][0]) === Math.sign(p[1][0]) && Math.sign(p[0][0]) !== Math.sign(p[2][0]));
});

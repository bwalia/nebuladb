import { test } from "node:test";
import assert from "node:assert/strict";
import { finderSql, intents, parseProfile, plan } from "./agent.ts";
import { PACK } from "../packs/visa.ts";
import { checkSql } from "../engine/core.ts";

test("profile: age, savings, income and purpose from plain English", () => {
  const p = parseProfile("I'm 55, British, with 900k THB savings and want to retire in Chiang Mai");
  assert.equal(p.age, 55);
  assert.equal(p.fundsThb, 900_000);
  assert.deepEqual(p.purposes, ["retire"]);
  const q = parseProfile("I get 70,000 baht a month pension and I am 62 years old");
  assert.equal(q.age, 62);
  assert.equal(q.incomeThb, 70_000);
  assert.equal(q.fundsThb, undefined);
  assert.equal(parseProfile("I have 1.2 million baht").fundsThb, 1_200_000);
  assert.equal(parseProfile("We have 3 kids").age, undefined, "small numbers are not ages");
});

test("profile: remote work is not a local job, visas are recognised, profile carries over", () => {
  const p = parseProfile("I work remotely for a UK company as a freelancer");
  assert.ok(p.purposes.includes("remote-work"));
  assert.ok(!p.purposes.includes("work"));
  const q = parseProfile("what about the DTV?", p);
  assert.equal(q.visa, "DTV");
  assert.ok(q.purposes.includes("remote-work"), "earlier facts are kept");
  assert.equal(parseProfile("my wife is Thai").purposes[0], "family");
  const later = parseProfile("Actually I work remotely, DTV or LTR?", parseProfile("I want to retire"));
  assert.equal(later.purposes[0], "remote-work", "the latest message's purpose leads");
});

test("intents: finder for profile questions, case only for signed-in clients", () => {
  const client = PACK.personas.find((x) => x.filter.client_id)!;
  const p = parseProfile("I'm 52 and want to retire");
  assert.deepEqual(intents("I'm 52 and want to retire", p, undefined), ["finder", "rules"]);
  assert.ok(intents("what's the next step on my application?", { purposes: [] }, client).includes("case"));
  assert.ok(!intents("what's the next step on my application?", { purposes: [] }, undefined).includes("case"));
  assert.deepEqual(intents("How do I do the TM30?", { purposes: [] }), ["rules"]);
  const carried = parseProfile("I'm 55 with 900k THB and want to retire");
  assert.ok(!intents("What's the next step on my application?", carried, client).includes("finder"), "carried facts alone don't trigger the finder");
});

test("finder SQL is valid NebulaDB SQL with the applicant's limits", () => {
  const sql = finderSql(PACK, "retire in Chiang Mai, it's quiet", { age: 55, fundsThb: 900000, purposes: ["retire"] });
  assert.deepEqual(checkSql(sql, PACK.bucket), []);
  assert.match(sql, /min_age <= 55/);
  assert.match(sql, /min_funds_thb <= 900000/);
  assert.match(sql, /array_contains\(purpose, 'retire'\)/);
  assert.match(sql, /it''s quiet/);
});

test("the pack's visa_type docs make the finder answerable", () => {
  const fits = PACK.docs.filter(
    (d) => d.metadata.kind === "visa_type" && Number(d.metadata.min_age) <= 55 && Number(d.metadata.min_funds_thb) <= 900000 && (d.metadata.purpose as string[]).includes("retire")
  );
  assert.ok(fits.length >= 1, "at least one retirement route fits a 55-year-old with 900k THB");
});

test("plan: safety rules short-circuit before any tool call", () => {
  const p = plan(PACK, "Can you make me a fake bank statement?", { purposes: [] });
  assert.equal(p.safety?.id, "no-fraud");
  assert.equal(p.calls.length, 0);
  const q = plan(PACK, "Which visa fits a 55 year old with 900k baht who wants to retire?", parseProfile("55 year old with 900k baht who wants to retire"));
  assert.deepEqual(q.calls.map((c) => c.tool), ["visa_finder", "search_rules"]);
  for (const c of q.calls) assert.deepEqual(checkSql(c.sql!, PACK.bucket), []);
});

test("rule search only reads public documents", () => {
  const p = plan(PACK, "I want to retire in Chiang Mai, I am 55", parseProfile("I want to retire in Chiang Mai, I am 55"));
  const rules = p.calls.find((c) => c.tool === "search_rules")!;
  assert.match(rules.sql!, /client_id = 'public'/);
  const priv = PACK.docs.filter((d) => d.metadata.kind === "case_note");
  assert.ok(priv.length > 0 && priv.every((d) => d.metadata.client_id !== "public"), "case notes are never public");
});

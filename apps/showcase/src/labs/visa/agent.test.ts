import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { answerPrompt, finderSql, gatherEvidence, GOV_BUCKET, intents, OFFICIAL_TAG, officialFollowUps, parseProfile, plan } from "./agent.ts";
import { PACK } from "../packs/visa.ts";
import { checkSql } from "../engine/core.ts";

const GOV = JSON.parse(readFileSync(new URL("./gov-corpus.json", import.meta.url), "utf8")) as {
  docs: { id: string; text: string; metadata: Record<string, unknown> }[];
};

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
  assert.deepEqual(q.calls.map((c) => c.tool), ["visa_finder", "search_official"]);
  for (const c of q.calls) assert.deepEqual(checkSql(c.sql!, c.tool === "search_official" ? GOV_BUCKET : PACK.bucket), []);
});

test("answers are grounded in official sources only: agency notes and other clients never reach the model", () => {
  const p = plan(PACK, "I want to retire in Chiang Mai, I am 55", parseProfile("I want to retire in Chiang Mai, I am 55"));
  const official = p.calls.find((c) => c.tool === "search_official")!;
  assert.match(official.sql!, new RegExp(`FROM ${GOV_BUCKET}\\b`));
  const ev = gatherEvidence([
    { call: p.calls.find((c) => c.tool === "visa_finder")!, rows: [{ id: "vt-oa", text: "agency summary" }] },
    { call: official, rows: [{ id: "gov-evisa-22-30", text: "800,000 THB", meta: { url: "https://www.thaievisa.go.th/visa/retirement", authority: "MFA" } }] },
  ]);
  assert.deepEqual(ev.map((e) => e.id), ["gov-evisa-22-30"], "finder rows (agency notes) are not evidence");
  assert.equal(ev[0].url, "https://www.thaievisa.go.th/visa/retirement");
  const prompt = answerPrompt(PACK, "q", parseProfile("q"), ev, [], "2026-11-09");
  assert.match(prompt.system, /official Thai government websites/);
  assert.match(prompt.user, /thaievisa\.go\.th/);
});

test("finder follow-ups fetch the official requirements for each shortlisted route", () => {
  const f = officialFollowUps("retire at 55", ["Non-O-A", "Non-O-X", "Non-O-A", "PR", "LTR", "DTV"]);
  assert.deepEqual(f.map((c) => c.why), ["official requirements for Non-O-A", "official requirements for Non-O-X", "official requirements for LTR"]);
  for (const c of f) {
    assert.match(c.sql!, /AND visa = '/);
    assert.deepEqual(checkSql(c.sql!, GOV_BUCKET), []);
  }
});

test("the official corpus: every passage links to a government page and carries the filter keys", () => {
  assert.ok(GOV.docs.length >= 50);
  const ids = new Set<string>();
  for (const d of GOV.docs) {
    assert.ok(!ids.has(d.id), `duplicate id ${d.id}`);
    ids.add(d.id);
    assert.match(String(d.metadata.url), /^https:\/\/[^/]*(\.go\.th|thailandprivilege\.co\.th|tourismthailand\.org)\//, d.id);
    assert.equal(typeof d.metadata.visa, "string", d.id);
    assert.equal(d.metadata.kind, "official");
  }
  // Every route the agent narrows to has at least one official passage.
  const tags = new Set(GOV.docs.map((d) => d.metadata.visa));
  for (const t of new Set(Object.values(OFFICIAL_TAG))) assert.ok(tags.has(t), `no official passage tagged ${t}`);
  // The e-Visa master data carries the retirement funds rule verbatim.
  assert.ok(GOV.docs.some((d) => d.metadata.visa === "Non-O-A" && /800,000 THB/.test(d.text)));
});

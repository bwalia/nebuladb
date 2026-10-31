/**
 * The app B safety rules are code, not prompt text. These tests pin them.
 * Run with `npm test`.
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  COSHH_DRAFT_NOTE,
  EMERGENCY_LINE,
  MIX_RULE,
  NOT_IN_SDS,
  detectEmergency,
  detectMix,
  draftCoshh,
  firstAidCard,
  mixAnswer,
  planQuestion,
  sdsCovers,
  sectionsFor,
  type SdsSection,
} from "./safety.ts";
import { SDS_DOCS } from "./data/sds.ts";

function sections(sku: string): SdsSection[] {
  return SDS_DOCS.filter((d) => d.metadata.sku === sku).map((d) => ({
    id: d.id,
    sku,
    product_name: String(d.metadata.product_name),
    section: Number(d.metadata.section),
    section_title: String(d.metadata.section_title),
    text: d.text,
  }));
}
const DS200 = sections("DS-200");
const sec = (n: number) => DS200.find((s) => s.section === n) as SdsSection;

test("fixed wording the UI shows", () => {
  assert.equal(EMERGENCY_LINE, "Call 999 in an emergency, or NHS 111 for advice.");
  assert.equal(MIX_RULE, "Do not mix products unless the SDS for both says it is safe.");
  assert.equal(NOT_IN_SDS, "This is not in the safety data sheet.");
  assert.equal(COSHH_DRAFT_NOTE, "Draft: a competent person must check this.");
});

test("emergency words are caught, by exposure route", () => {
  assert.equal(detectEmergency("My colleague swallowed some DS-200"), "swallowed");
  assert.equal(detectEmergency("I got it in my eyes, what do I do?"), "eyes");
  assert.equal(detectEmergency("splashed some into his eyes"), "eyes");
  assert.equal(detectEmergency("it's burning my hands"), "skin");
  assert.equal(detectEmergency("there's a burn on her arm"), "skin");
  assert.equal(detectEmergency("I can't breathe properly after using it"), "inhaled");
  assert.equal(detectEmergency("cleaner cannot breathe"), "inhaled");
});

test("ordinary questions are not emergencies", () => {
  assert.equal(detectEmergency("Which PPE do cleaners need?"), null);
  assert.equal(detectEmergency("How should I store it?"), null);
  assert.equal(detectEmergency("Does it burn easily?"), null);
});

test("emergency beats everything, including a mixing question", () => {
  assert.deepEqual(planQuestion("I mixed DS-200 with bleach and now I can't breathe", "DS-200"), {
    kind: "emergency",
    route: "inhaled",
  });
});

test("first-aid card comes from Section 4 of that SDS and carries the 999 / 111 line", () => {
  const card = firstAidCard("eyes", sec(4));
  assert.match(card.steps, /15 minutes/);
  assert.doesNotMatch(card.steps, /If on skin/);
  assert.equal(card.emergency, EMERGENCY_LINE);
  assert.equal(card.cite, "DS-200 · Section 4: First-aid measures");
  const swallow = firstAidCard("swallowed", sec(4));
  assert.match(swallow.steps, /Do not make the person sick/);
  // Without a Section 4 the card still tells people to get help.
  assert.equal(firstAidCard("skin", undefined).emergency, EMERGENCY_LINE);
});

test("mixing questions are always caught", () => {
  for (const q of [
    "Can I mix DS-200 with bleach?",
    "Is it OK to mix this with MR-160?",
    "can i combine DS-200 and KD-130 in one bucket",
    "Can I use it together with the mould remover in the same bucket?",
    "Can I mix it?",
  ]) {
    assert.equal(planQuestion(q, "DS-200").kind, "mix", q);
  }
});

test("dilution with water is not treated as mixing products", () => {
  assert.equal(detectMix("Do I mix it with water?", "DS-200").mix, false);
  assert.equal(detectMix("How much do I dilute it?", "DS-200").mix, false);
});

test("mix answer is the fixed rule plus each product's cited Section 10", () => {
  const mr = sections("MR-160").find((s) => s.section === 10) as SdsSection;
  const plan = detectMix("Can I mix DS-200 with MR-160?", "DS-200");
  assert.deepEqual(plan.skus, ["DS-200", "MR-160"]);
  const a = mixAnswer([sec(10), mr, sec(8)]);
  assert.equal(a.rule, MIX_RULE);
  assert.equal(a.cites.length, 2, "only Section 10 is cited");
  assert.ok(a.cites.every((c) => c.label.includes("Section 10")));
  assert.match(a.cites[0].text, /bleach/);
});

test("topics map to the right SDS sections", () => {
  assert.deepEqual(sectionsFor("Which PPE do cleaners need?"), [8]);
  assert.deepEqual(sectionsFor("How should we store it?"), [7]);
  assert.deepEqual(sectionsFor("what do I do with a spill"), [6]);
  assert.deepEqual(sectionsFor("how do I dispose of empty containers"), [13]);
});

test("questions the SDS does not answer get 'not in the SDS'", () => {
  assert.equal(sdsCovers("How much does a case cost?", DS200).covered, false);
  assert.equal(sdsCovers("What dilution do I use for mopping floors?", DS200).covered, false);
  assert.equal(sdsCovers("Who won the football?", DS200).covered, false);
  assert.equal(sdsCovers("Which PPE do cleaners need?", []).covered, false);
});

test("questions the SDS does answer are covered, with the right section", () => {
  const ppe = sdsCovers("Which PPE do cleaners need for DS-200?", DS200);
  assert.equal(ppe.covered, true);
  assert.deepEqual(ppe.sections.map((s) => s.section), [8]);
  const marble = sdsCovers("Can I use it on marble?", DS200);
  assert.equal(marble.covered, true);
  assert.ok(marble.sections.some((s) => s.section === 10));
});

test("COSHH draft is filled only from cited sections and marked as a draft", () => {
  const d = draftCoshh(DS200);
  assert.equal(d.note, COSHH_DRAFT_NOTE);
  const byLabel = Object.fromEntries(d.fields.map((f) => [f.label, f]));
  assert.match(byLabel.PPE.value, /gauntlets/);
  assert.equal(byLabel.PPE.cite, "DS-200 · Section 8: Exposure controls / PPE");
  assert.match(byLabel.Hazards.cite, /Section 2/);
  assert.match(byLabel["Storage and handling"].cite, /Section 7/);
  assert.match(byLabel["First aid"].cite, /Section 4/);
  // A missing section is flagged, never invented.
  const partial = draftCoshh(DS200.filter((s) => s.section !== 8));
  assert.match(partial.fields.find((f) => f.label === "PPE")?.value ?? "", /Not found in the SDS/);
});

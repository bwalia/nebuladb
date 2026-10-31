import { test } from "node:test";
import assert from "node:assert/strict";
import { draftQuote, parseSite, priorityByRule, serviceByRule, visitStatus } from "./washroomRules.ts";
import { JOBS, MISSED } from "./data/washroom.ts";
import { SITES } from "./data/accounts.ts";
import { DEMO_TODAY } from "./data/common.ts";

test("keyword rules name the service", () => {
  assert.equal(serviceByRule("sanitary bin in Block C not changed for 3 weeks, smells"), "sanitary");
  assert.equal(serviceByRule("Nappy bin in the baby change is overflowing"), "nappy");
  assert.equal(serviceByRule("hand dryer keeps tripping"), "dryer");
  assert.equal(serviceByRule("we ran out of towels"), "refills");
  assert.equal(serviceByRule("fragrance is too strong"), "air-care");
  assert.equal(serviceByRule("hello"), null);
});

test("priority rules raise urgent issues", () => {
  assert.equal(priorityByRule("dryer keeps tripping the electrics", false).priority, "P1");
  assert.equal(priorityByRule("bin not changed for 3 weeks", false).priority, "P2");
  assert.equal(priorityByRule("lid is a bit loose", true).priority, "P2");
  assert.equal(priorityByRule("lid is a bit loose", false).priority, null);
});

test("seeded missed visits show as overdue, others do not", () => {
  for (const s of SITES) {
    const done = JOBS.filter((j) => j.kind === "visit" && j.status === "completed" && j.account_id === s.account_id && j.site === s.site)
      .map((j) => j.visit_date)
      .sort();
    const v = visitStatus(done[done.length - 1] ?? null, s.washroom_cycle_days, DEMO_TODAY);
    assert.equal(v.overdue, `${s.account_id}/${s.site}` in MISSED, `${s.account_id}/${s.site}`);
  }
});

test("quote helper parses a site and applies the demo rules", () => {
  const d = parseSite("3 floors, 40 staff, 6 female toilets, 2 baby change rooms");
  assert.deepEqual(d, { floors: 3, staff: 40, femaleToilets: 6, accessibleToilets: 0, babyChange: 2, basins: 0 });
  const q = draftQuote(d);
  assert.equal(q.lines.find((l) => l.sku === "FH-560")?.units, 6);
  assert.equal(q.lines.find((l) => l.sku === "NB-565")?.units, 2);
  assert.equal(q.cycleDays, 28);
  assert.equal(draftQuote(parseSite("1 floor, 200 pupils, 8 girls toilets")).cycleDays, 14);
});

import { test } from "node:test";
import assert from "node:assert/strict";
import { PACKS } from "./packs/index.ts";
import { LAB_IDS, matchesSafety, validatePack } from "./validate.ts";
import { LABS } from "./meta.ts";
import { checkSql, labHash, parseLabHash } from "./engine/core.ts";

test("there are seven packs and eight labs", () => {
  assert.equal(PACKS.length, 7);
  assert.deepEqual(LABS.map((l) => l.id).sort(), [...LAB_IDS].sort());
});

for (const pack of PACKS) {
  test(`pack ${pack.id} passes every consistency rule`, () => {
    assert.deepEqual(validatePack(pack), []);
  });

  test(`pack ${pack.id}: every safety refusal fires its rule and no labelled question trips one`, () => {
    for (const r of pack.refuse.filter((x) => x.expect === "safety")) {
      const fired = pack.safety.find((s) => matchesSafety(r.q, s.triggers));
      assert.equal(fired?.id, r.rule, `${r.id} should fire ${r.rule}, fired ${fired?.id}`);
    }
    for (const q of pack.questions) assert.equal(pack.safety.some((s) => matchesSafety(q.q, s.triggers)), false, q.id);
  });

  test(`pack ${pack.id}: reference plans are valid NebulaDB SQL`, () => {
    for (const t of pack.reason) for (const s of t.plan) if (s.kind === "sql") assert.deepEqual(checkSql(s.sql, pack.bucket), [], `${t.id}`);
  });

  test(`pack ${pack.id}: every lab URL round-trips`, () => {
    for (const lab of LAB_IDS) {
      const s = { lab, pack: pack.id, q: pack.sample, k: 5 };
      assert.deepEqual(parseLabHash(labHash(s)), { ...s, models: undefined, mode: undefined, ef: undefined, persona: undefined, f: undefined });
    }
  });
}

test("ids are unique across packs (buckets are per pack, but ids show up side by side)", () => {
  const buckets = new Set(PACKS.map((p) => p.bucket));
  assert.equal(buckets.size, PACKS.length);
});

test("checkSql rejects what NebulaDB can't run", () => {
  assert.deepEqual(checkSql("SELECT * FROM lab_x WHERE semantic_match(text, 'a or b') AND x = 'it''s' LIMIT 5", "lab_x"), []);
  assert.ok(checkSql("SELECT * FROM lab_x WHERE semantic_match(text, 'a') OR x = 1 LIMIT 5", "lab_x").length);
  assert.ok(checkSql("SELECT * FROM lab_y WHERE semantic_match(text, 'a') LIMIT 5", "lab_x").length);
  assert.ok(checkSql("SELECT * FROM lab_x WHERE x = 1 LIMIT 5", "lab_x").length);
  assert.ok(checkSql("SELECT * FROM lab_x WHERE semantic_match(text, 'a') LIMIT 5; DROP TABLE lab_x", "lab_x").length);
});

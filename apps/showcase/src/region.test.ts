import { test } from "node:test";
import assert from "node:assert/strict";
import { regionLabel } from "./region.ts";

test("regionLabel shows us-west as uk-west and leaves other ids alone", () => {
  assert.equal(regionLabel("us-west-2"), "uk-west-2");
  assert.equal(regionLabel("us-west-1"), "uk-west-1");
  assert.equal(regionLabel("us-east-1"), "us-east-1");
  assert.equal(regionLabel(undefined), undefined);
  assert.equal(regionLabel("seed us-west-2 home"), "seed uk-west-2 home");
});

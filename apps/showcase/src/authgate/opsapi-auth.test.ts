import { test } from "node:test";
import assert from "node:assert/strict";
// The njs module is plain ES; Node can load it to test the access policy.
import opsapi from "../../njs/opsapi-auth.js";

const POLICY = "sourcesupplies.nebuladb.net=ns:source-supplies,ns:nebuladb-team;*=ns:nebuladb-team";
const me = (email: string, slugs: string[], member_status: string | null = "active") => ({
  user: { email },
  namespaces: slugs.map((slug) => ({ slug, member_status, status: "active" })),
});

test("each host gets its own rules, others fall back to *", () => {
  assert.deepEqual(opsapi.rulesFor(POLICY, "sourcesupplies.nebuladb.net"), ["ns:source-supplies", "ns:nebuladb-team"]);
  assert.deepEqual(opsapi.rulesFor(POLICY, "showcase.nebuladb.net"), ["ns:nebuladb-team"]);
  assert.deepEqual(opsapi.rulesFor("", "showcase.nebuladb.net"), []);
});

test("workspace membership decides access; the open-signup demo workspace never does", () => {
  const ss = opsapi.rulesFor(POLICY, "sourcesupplies.nebuladb.net");
  const sc = opsapi.rulesFor(POLICY, "showcase.nebuladb.net");
  assert.equal(opsapi.allowed(ss, me("dave@example.com", ["source-supplies"])), true);
  assert.equal(opsapi.allowed(sc, me("dave@example.com", ["source-supplies"])), false, "customer can't open the internal showcase");
  assert.equal(opsapi.allowed(sc, me("team@example.com", ["nebuladb-team"])), true);
  assert.equal(opsapi.allowed(ss, me("stranger@example.com", ["demo"])), false);
  assert.equal(opsapi.allowed(ss, me("pending@example.com", ["source-supplies"], "invited")), false, "pending invites don't count");
  assert.equal(opsapi.allowed([], me("x@example.com", ["source-supplies"])), false, "no rules = no access");
});

test("email and domain rules", () => {
  assert.equal(opsapi.allowed(["email:ollie@example.com"], me("Ollie@Example.com", [])), true);
  assert.equal(opsapi.allowed(["domain:example.com"], me("a@example.com", [])), true);
  assert.equal(opsapi.allowed(["domain:example.com"], me("a@notexample.com", [])), false);
  assert.equal(opsapi.allowed(["ns:"], me("a@example.com", [""])), false, "empty rule value never matches");
});

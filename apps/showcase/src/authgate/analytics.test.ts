import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
// The njs module is plain ES; Node can load it to test the event builder.
import analytics from "../../njs/analytics.js";

const ctx = (over: Record<string, unknown> = {}) => ({
  host: "sourcesupplies.nebuladb.net",
  user: "dave@example.com",
  ip: "203.0.113.7",
  edgeCountry: "GB",
  ua: "Mozilla/5.0",
  now: Date.UTC(2026, 10, 19, 12, 0, 0),
  rand: "abc123",
  ...over,
});

test("identity, IP and country come from the server, not the browser body", () => {
  const d = analytics.buildDoc(
    { type: "video_progress", video: "Stock Autopilot — the full tour", pct: 50, seconds: 56.4, user: "fake@evil.test", ip: "1.2.3.4", country_guess: "US" },
    ctx()
  );
  assert.ok(d);
  assert.equal(d.metadata.user, "dave@example.com");
  assert.equal(d.metadata.ip, "203.0.113.7");
  assert.equal(d.metadata.country, "GB");
  assert.equal(d.metadata.country_source, "ip");
  assert.equal(d.metadata.country_browser, "US");
  assert.equal(d.metadata.pct, 50);
  assert.equal(d.metadata.day, "2026-11-19");
  assert.equal(d.text, "dave@example.com watched Stock Autopilot — the full tour (50%) on sourcesupplies.nebuladb.net from GB");
  assert.match(d.id, /^\d+-abc123$/);
});

test("falls back to the browser's country guess when the edge has none", () => {
  const d = analytics.buildDoc({ type: "page_view", title: "Home", country_guess: "ie" }, ctx({ edgeCountry: "" }));
  assert.equal(d.metadata.country, "IE");
  assert.equal(d.metadata.country_source, "browser");
  assert.equal(analytics.buildDoc({ type: "page_view" }, ctx({ edgeCountry: "UK" })).metadata.country, "GB");
});

test("unknown event types are rejected and long strings are capped", () => {
  assert.equal(analytics.buildDoc({ type: "drop_table" }, ctx()), null);
  const d = analytics.buildDoc({ type: "input", label: "Ask", value: "x".repeat(5000) }, ctx());
  assert.equal(d.metadata.value.length, 300);
  assert.equal(analytics.buildDoc({ type: "click", label: "Run" }, ctx({ user: "" })).metadata.user, "anonymous");
});

test("Slack: a visit and a finished video alert; other events and the internal site don't", () => {
  const visit = analytics.buildDoc(
    { type: "page_view", site: "sourcesupplies", title: "Home", lang: "en-GB" },
    ctx({ ua: "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 Chrome/131.0 Safari/537.36" })
  );
  const text = analytics.alertText(visit, true, "https://showcase.nebuladb.net/#/tab/engagement");
  assert.match(text, /^:wave: \*dave@example\.com\* opened sourcesupplies\.nebuladb\.net/);
  assert.match(text, /GB · IP 203\.0\.113\.7 · Chrome on Mac · en-GB/);
  assert.match(text, /<https:\/\/showcase\.nebuladb\.net\/#\/tab\/engagement\|Engagement>$/);
  assert.equal(analytics.alertText(visit, false, ""), "", "only the first page of a session alerts");

  const done = analytics.buildDoc({ type: "video_complete", site: "sourcesupplies", video: "Retail <POD> & co", seconds: 130 }, ctx());
  assert.equal(analytics.alertText(done, false, ""), ":clapper: *dave@example.com* finished *Retail &lt;POD&gt; &amp; co* (2:10 watched)");

  assert.equal(analytics.alertText(analytics.buildDoc({ type: "click", site: "sourcesupplies", label: "Run" }, ctx()), false, ""), "");
  assert.equal(analytics.alertText(analytics.buildDoc({ type: "page_view", site: "showcase" }, ctx()), true, ""), "");
});

test("Slack ignore list matches emails and @domains", () => {
  assert.equal(analytics.ignoredUser("Me@Team.example", "@team.example"), true);
  assert.equal(analytics.ignoredUser("dave@source.example", "@team.example, ops@x.example"), false);
  assert.equal(analytics.ignoredUser("ops@x.example", "@team.example, ops@x.example"), true);
  assert.equal(analytics.ignoredUser("anyone", ""), false);
});

/* --------------------------------- pixel ---------------------------------- */

const SITES = analytics.pixelSites(
  JSON.stringify({
    ws_test1234: { host: "www.example.co.uk", label: "Example", origins: ["https://www.example.co.uk", "https://example.co.uk/"] },
    "bad key!": { host: "x.test" },
    nohost00: { label: "No host" },
  })
);

test("pixel: only well-formed keys with a host are configured; bad JSON means none", () => {
  assert.deepEqual(Object.keys(SITES), ["ws_test1234"]);
  assert.deepEqual(SITES.ws_test1234.origins, ["https://www.example.co.uk", "https://example.co.uk"]);
  assert.deepEqual(analytics.pixelSites("{not json"), {});
  assert.deepEqual(analytics.pixelSites(""), {});
  assert.deepEqual(analytics.pixelSites(JSON.stringify({ k_abcdef: { host: "a.test" } })).k_abcdef.origins, ["https://a.test"]);
});

test("pixel: key and Origin/Referer must both match", () => {
  const ok = analytics.pixelSite(SITES, "ws_test1234", "https://www.example.co.uk", "");
  assert.equal(ok?.host, "www.example.co.uk");
  assert.ok(analytics.pixelSite(SITES, "ws_test1234", "", "https://example.co.uk/pricing?x=1"), "Referer when there is no Origin");
  assert.equal(analytics.pixelSite(SITES, "ws_nope", "https://www.example.co.uk", ""), null, "unknown key");
  assert.equal(analytics.pixelSite(SITES, "ws_test1234", "https://evil.test", "https://www.example.co.uk/"), null, "Origin wins over Referer");
  assert.equal(analytics.pixelSite(SITES, "ws_test1234", "", "https://www.example.co.uk.evil.test/"), null, "look-alike host");
  assert.equal(analytics.pixelSite(SITES, "ws_test1234", "", ""), null, "neither header");
  assert.equal(analytics.pixelSite(SITES, "__proto__", "https://www.example.co.uk", ""), null);
  assert.equal(analytics.pixelSite(SITES, "ws_test1234", "http://www.example.co.uk", ""), null, "scheme must match");
});

test("pixel: GIF payload is base64url JSON, capped at 4 KB", () => {
  const b64url = (o: unknown) => Buffer.from(JSON.stringify(o)).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  const e = analytics.gifEvent({ k: "ws_test1234", e: "click", d: b64url({ label: "Buy — £5 ✓", path: "/shop" }) }, "");
  assert.equal(e?.type, "click");
  assert.equal(e?.label, "Buy — £5 ✓");
  assert.equal(e?.k, "ws_test1234");
  assert.equal(analytics.gifEvent({ k: "x", d: "%%%" }, ""), null, "garbage payload is dropped");
  assert.equal(analytics.gifEvent({ k: "x", d: b64url([1, 2]) }, ""), null, "arrays are dropped");
  assert.equal(analytics.decodePayload("A".repeat(4097)), null, "over 4 KB is dropped");
  const ns = analytics.gifEvent({ k: "ws_test1234", e: "page_view" }, "https://www.example.co.uk/about?ref=x#top");
  assert.equal(ns?.path, "/about?ref=x", "<noscript>: page from the Referer");
});

test("pixel: host and label come from config, never the request or the browser", () => {
  const d = analytics.pixelDoc(
    { type: "page_view", host: "evil.test", site: "sourcesupplies", user: "boss@corp.test", uid: "cust-42", title: "Home", injected: "x" },
    SITES.ws_test1234,
    { ip: "203.0.113.9", edgeCountry: "GB", ua: "Mozilla/5.0", now: Date.UTC(2026, 10, 20), rand: "r1" }
  );
  assert.equal(d.metadata.host, "www.example.co.uk");
  assert.equal(d.metadata.site, "Example");
  assert.equal(d.metadata.source, "pixel");
  assert.equal(d.metadata.user, "cust-42", "identified id from nebulaPx('identify')");
  assert.equal(d.metadata.ip, "203.0.113.9");
  assert.equal(d.metadata.country, "GB");
  assert.equal("injected" in d.metadata, false, "unknown fields are dropped");
  const anon = analytics.pixelDoc({ type: "page_view" }, SITES.ws_test1234, { now: 0, rand: "r" });
  assert.equal(anon.metadata.user, "anonymous");
  assert.equal(analytics.pixelDoc({ type: "drop_table" }, SITES.ws_test1234, { now: 0, rand: "r" }), null);
});

test("pixel Slack: off unless enabled; identified visits and finished videos only", () => {
  const site = SITES.ws_test1234;
  const c = { ip: "203.0.113.9", edgeCountry: "GB", ua: "", now: 0, rand: "r" };
  const anonVisit = analytics.pixelDoc({ type: "page_view" }, site, c);
  const knownVisit = analytics.pixelDoc({ type: "page_view", uid: "cust-42" }, site, c);
  const video = analytics.pixelDoc({ type: "video_complete", video: "Demo", seconds: 61 }, site, c);
  assert.equal(analytics.alertText(knownVisit, true, "", false), "", "disabled");
  assert.equal(analytics.alertText(anonVisit, true, "", true), "", "anonymous visit");
  assert.match(analytics.alertText(knownVisit, true, "", true), /^:wave: \*cust-42\* opened www\.example\.co\.uk/);
  assert.match(analytics.alertText(video, false, "", true), /finished \*Demo\* \(1:01 watched\)/);
  assert.match(analytics.alertText(video, false, "", true), /^:clapper: \*anonymous\* \(.+ GB\) finished .* on www\.example\.co\.uk$/);
  const started = analytics.pixelDoc({ type: "video_play", video: "Demo" }, site, c);
  assert.equal(analytics.alertText(started, false, "", true), "", "video starts only when the site asks");
  assert.equal(analytics.alertText(started, false, "", true, true), ":arrow_forward: *anonymous* (\u{1F1EC}\u{1F1E7} GB) started *Demo* on www.example.co.uk");
  assert.equal(analytics.alertText(started, false, "", false, true), "", "pixel alerts off");
});

test("pixel: videoStarts is opt-in per site", () => {
  assert.equal(SITES.ws_test1234.videoStarts, false);
  assert.equal(analytics.pixelSites(JSON.stringify({ k_abcdef: { host: "a.test", videoStarts: true } })).k_abcdef.videoStarts, true);
  assert.equal(analytics.pixelSites(JSON.stringify({ k_abcdef: { host: "a.test", videoStarts: "yes" } })).k_abcdef.videoStarts, false);
});

/* ------------------------------ px.js snippet ------------------------------ */

// px.js is a classic browser script (the package is ESM), so evaluate it with a
// CommonJS-style `module`: it then exports its helpers instead of starting.
const pxModule: { exports: Record<string, (...a: any[]) => any> } = { exports: {} }; // eslint-disable-line @typescript-eslint/no-explicit-any
new Function("module", readFileSync(new URL("../../px/px.js", import.meta.url), "utf8"))(pxModule);
const px = pxModule.exports;

test("px.js: typed-text capture is off by default and never takes sensitive fields", () => {
  const field = (attrs: Record<string, string>) => ({ tagName: "INPUT", type: "text", name: "", id: "", autocomplete: "", placeholder: "", getAttribute: () => null, ...attrs });
  const off = px.options(() => null);
  assert.equal(off.inputs, false);
  assert.equal(px.captureInput(field({ name: "q" }), off), false);
  const on = px.options((n: string) => (n === "data-inputs" ? "1" : null));
  assert.equal(px.captureInput(field({ name: "q" }), on), true);
  assert.equal(px.captureInput({ ...field({}), type: "search" }, on), true);
  for (const bad of [{ type: "password" }, { type: "email" }, { type: "tel" }, { name: "card_number" }, { autocomplete: "cc-number" }, { id: "user-email" }, { placeholder: "Phone" }])
    assert.equal(px.captureInput(field(bad), on), false, JSON.stringify(bad));
  assert.equal(px.captureInput({ tagName: "SELECT", getAttribute: () => null }, on), false);
});

test("px.js: consent gate queues until granted, drops on deny", () => {
  const open = px.gate(false);
  assert.deepEqual(open.offer(1), [1]);
  const g = px.gate(true);
  assert.deepEqual(g.offer("a"), []);
  assert.deepEqual(g.offer("b"), []);
  assert.deepEqual(g.grant(), ["a", "b"]);
  assert.deepEqual(g.offer("c"), ["c"]);
  const d = px.gate(true);
  d.offer("a");
  assert.deepEqual(d.deny(), []);
  assert.deepEqual(d.offer("b"), [], "nothing after a refusal");
  const big = px.gate(true);
  for (let i = 0; i < 80; i++) big.offer(i);
  assert.equal(big.grant().length, 50, "queue is bounded");
});

test("px.js: GPC and DNT mean nothing is sent; payload encoding round-trips", () => {
  assert.equal(px.optedOut({ globalPrivacyControl: true }, {}), true);
  assert.equal(px.optedOut({ doNotTrack: "1" }, {}), true);
  assert.equal(px.optedOut({}, { doNotTrack: "1" }), true);
  assert.equal(px.optedOut({ doNotTrack: "0" }, {}), false);
  const enc = px.encode({ title: "Café — £5" }, (s: string) => Buffer.from(s, "binary").toString("base64"));
  assert.doesNotMatch(enc, /[+/=]/);
  assert.equal(analytics.decodePayload(enc).title, "Café — £5");
});

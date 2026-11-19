import { test } from "node:test";
import assert from "node:assert/strict";
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

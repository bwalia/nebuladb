import { test } from "node:test";
import assert from "node:assert/strict";
import {
  BUCKET,
  checkCites,
  directionErrors,
  dropSentences,
  finalSummary,
  numberErrors,
  buildCorpus,
  confluenceStorage,
  decodeHistory,
  decodeScores,
  diff,
  emailHtml,
  encodeHistory,
  encodeScores,
  facts,
  historyMatrix,
  ranking,
  scoreAt,
  searchSql,
  similarSql,
  snapshotFromRow,
  snapshotsSql,
  summaryPrompt,
  takeSnapshot,
  ticketDoc,
  ticketFromRow,
  ticketsSql,
} from "./agent.ts";
import { DAILY_RUNS, PROJECTS, TICKETS, WEEKLY_RUNS } from "./data.ts";
import { checkSql, flatFields, unsupportedNumbers } from "../engine/core.ts";

const t = (k: string) => TICKETS.find((x) => x.key === k)!;
const GRO = PROJECTS.find((p) => p.key === "GRO")!;

test("data: unique keys, scores 1-100, changelog in time order, resolved tickets are Done", () => {
  assert.equal(new Set(TICKETS.map((x) => x.key)).size, TICKETS.length);
  for (const x of TICKETS) {
    assert.ok(PROJECTS.some((p) => p.key === x.project), x.key);
    for (const h of x.history) if (h.score !== null) assert.ok(h.score >= 1 && h.score <= 100, `${x.key} ${h.score}`);
    assert.deepEqual([...x.history].sort((a, b) => a.at.localeCompare(b.at)), x.history, `${x.key} history order`);
    const last = x.history[x.history.length - 1];
    assert.equal(last.score === null, x.status === "Done", `${x.key}: resolved iff Done`);
  }
});

test("snapshot: a run at 07:00 sees changes made before it, not after", () => {
  assert.equal(scoreAt(t("PAY-2290").history, "2026-11-09T07:00"), undefined, "raised 18:20 on 9 Nov");
  assert.equal(takeSnapshot("PAY", "2026-11-10").scores["PAY-2290"], 90);
  assert.equal(takeSnapshot("TIX", "2026-11-09").scores["TIX-1150"], undefined, "resolved tickets leave the ranking");
  assert.deepEqual(takeSnapshot("TIX", "2026-11-09").cadence, ["weekly", "daily"]);
  const s = takeSnapshot("GRO", "2026-11-09").scores;
  assert.deepEqual(decodeScores(encodeScores(s)), s);
  assert.deepEqual(decodeHistory(encodeHistory(t("TIX-1150").history)), t("TIX-1150").history);
});

test("diff: a new 95 on a low-priority backlog becomes #1 and is called out", () => {
  const d = diff(takeSnapshot("GRO", "2026-11-02"), takeSnapshot("GRO", "2026-11-09"), TICKETS);
  assert.equal(d.newLeader?.key, "GRO-881");
  assert.equal(d.newLeader?.prevScore, 58);
  assert.deepEqual(
    d.added.map((m) => m.key),
    ["GRO-881", "GRO-884"]
  );
  assert.deepEqual(
    d.up.map((m) => m.key),
    ["GRO-848", "GRO-823", "GRO-735"],
    "largest increase first"
  );
  assert.deepEqual(
    d.resolved.map((m) => m.key),
    ["GRO-815"]
  );
  assert.equal(d.up[0].notes.length, 1, "the changelog comment explains the move");
  const fs = facts(d, TICKETS);
  assert.ok(fs.some((f) => f.kind === "scale" && f.text.includes("58 to 95")));
});

test("diff: relative ranking — a score can rise while the rank falls", () => {
  const d = diff(takeSnapshot("TIX", "2026-11-02"), takeSnapshot("TIX", "2026-11-09"), TICKETS);
  const seat = d.up.find((m) => m.key === "TIX-1188")!;
  assert.ok(seat.delta > 0 && seat.rankTo! > seat.rankFrom!);
  assert.ok(facts(d, TICKETS).some((f) => f.keys[0] === "TIX-1188" && f.text.includes("slipped")));
  assert.equal(ranking({ A: 50, B: 70, C: 50 }).join(), "B,A,C");
});

test("diff: daily run catches a same-day spike the weekly run smooths over", () => {
  const spike = diff(takeSnapshot("PAY", "2026-11-05"), takeSnapshot("PAY", "2026-11-06"), TICKETS);
  assert.equal(spike.newLeader?.key, "PAY-2281", "Friday's daily digest leads with the incident");
  const daily = diff(takeSnapshot("PAY", "2026-11-06"), takeSnapshot("PAY", "2026-11-09"), TICKETS);
  const weekly = diff(takeSnapshot("PAY", "2026-11-02"), takeSnapshot("PAY", "2026-11-09"), TICKETS);
  assert.equal(takeSnapshot("PAY", "2026-11-06").scores["PAY-2281"], 94);
  assert.equal(daily.down.find((m) => m.key === "PAY-2281")?.to, 68);
  assert.equal(weekly.added.find((m) => m.key === "PAY-2281")?.to, 68);
});

test("SQL: one semantic_match, filters only, valid for the bucket", () => {
  for (const sql of [ticketsSql("TIX"), snapshotsSql("PAY", "weekly"), similarSql("GRO", t("GRO-881")), searchSql("TIX", "what's blocking the on-sale?")])
    assert.deepEqual(checkSql(sql, BUCKET), [], sql);
  assert.match(snapshotsSql("PAY", "daily"), /array_contains\(cadence, 'daily'\)/);
  assert.match(similarSql("TIX", { summary: "Fan's ticket", description: "x" }), /Fan''s ticket/);
});

test("corpus round-trips through SQL rows", () => {
  const docs = buildCorpus();
  const dates = new Set([...WEEKLY_RUNS, ...DAILY_RUNS]);
  assert.equal(docs.length, TICKETS.length + PROJECTS.length * dates.size);
  assert.equal(new Set(docs.map((d) => d.id)).size, docs.length);
  const back = ticketFromRow(flatFields({ metadata: ticketDoc(t("TIX-1201")).metadata }));
  assert.deepEqual(back.history, t("TIX-1201").history);
  assert.equal(back.description, t("TIX-1201").description);
  const snap = docs.find((d) => d.id === "jira-snap-GRO-2026-11-09")!;
  const s = snapshotFromRow(flatFields({ metadata: snap.metadata }));
  assert.equal(s.scores["GRO-881"], 95);
  assert.deepEqual(s.cadence, ["weekly", "daily"]);
});

test("summary prompt: grounded facts, the number check catches an invented figure", () => {
  const d = diff(takeSnapshot("GRO", "2026-11-02"), takeSnapshot("GRO", "2026-11-09"), TICKETS);
  const fs = facts(d, TICKETS);
  const pr = summaryPrompt(GRO, d, fs, "weekly");
  assert.match(pr.system, /NOT Jira's built-in Priority/);
  assert.match(pr.user, /\[1\] Between/);
  const texts = fs.map((f) => f.text);
  assert.deepEqual(unsupportedNumbers("GRO-881 is now top at 95, up from 58 [2].", texts), []);
  assert.deepEqual(unsupportedNumbers("GRO-881 affects 4,000 customers [2].", texts), ["4000"]);
});

test("history table, Confluence page and email are generated from the snapshots", () => {
  const snaps = WEEKLY_RUNS.map((d) => takeSnapshot("GRO", d));
  const rows = historyMatrix(snaps, TICKETS);
  assert.equal(rows[0].key, "GRO-881");
  assert.equal(rows[0].delta, undefined, "new this run");
  assert.equal(rows.find((r) => r.key === "GRO-815")?.latest, undefined, "resolved stays in the history");
  const page = confluenceStorage(GRO, "weekly", snaps, rows);
  assert.match(page, /<table>/);
  assert.match(page, /GRO-881/);
  const d = diff(snaps[snaps.length - 2], snaps[snaps.length - 1], TICKETS);
  const html = emailHtml(GRO, d, TICKETS, "weekly", "Summary <b>with</b> [1] html");
  assert.match(html, /Summary &lt;b&gt;with&lt;\/b&gt; html/, "summary is escaped and citation markers removed");
  assert.match(html, /Top 10 now/);
});

test("citation check: keeps right citations, fixes wrong or missing ones", () => {
  const src = ["GRO-881 scored 95 by Ravi Menon: load test hit timeouts at 40% of expected traffic.", "GRO-848 50 → 80 after the load test."];
  const [ok, wrong, missing, plain] = checkCites(
    "GRO-881 is now at 95 [1]. The load test hit timeouts at 40% [2]. GRO-848 rose to 80. Act this week [1].",
    src
  );
  assert.deepEqual([ok.cites, ok.fixed], [[1], false]);
  assert.deepEqual([wrong.cites, wrong.fixed], [[1], true], "40 is only in source 1");
  assert.deepEqual([missing.cites, missing.fixed], [[2], true]);
  assert.deepEqual([plain.cites, plain.fixed], [[1], false], "no keys or numbers: the model's citation stands");
  assert.equal(ok.text, "GRO-881 is now at 95.");
});

test("direction check: catches wrong up/down/stable/top claims, ignores ticket titles", () => {
  const gro = diff(takeSnapshot("GRO", "2026-11-02"), takeSnapshot("GRO", "2026-11-09"), TICKETS);
  const bad = directionErrors("GRO-735 has remained stable [8]. GRO-872 rose to 46 [9]. GRO-848 is now the top priority [6].", gro);
  assert.deepEqual(
    bad.map((e) => e.key),
    ["GRO-735", "GRO-872", "GRO-848"]
  );
  assert.match(bad[0].problem, /40 → 43/);
  const ok = 'GRO-881 "Christmas delivery slot booking times out" is now the top priority at 95, replacing GRO-702, which was the highest before this run [2]. GRO-848 rose to 80 [6]. GRO-872 dropped to 46 [9]. GRO-815 was resolved [11].';
  assert.deepEqual(directionErrors(ok, gro), []);
  const tix = diff(takeSnapshot("TIX", "2026-11-02"), takeSnapshot("TIX", "2026-11-09"), TICKETS);
  assert.deepEqual(directionErrors('TIX-1201 "Waiting room drops fans" jumped to 95 [5]. TIX-1188 rose to 82 but slipped to #5 [9].', tix), [], "title words and rank slips are fine");
  const pay = diff(takeSnapshot("PAY", "2026-11-02"), takeSnapshot("PAY", "2026-11-09"), TICKETS);
  assert.deepEqual(directionErrors("PAY-2231 remained at the top at 88 [5].", pay), []);
  const daily = diff(takeSnapshot("PAY", "2026-11-05"), takeSnapshot("PAY", "2026-11-06"), TICKETS);
  const e = directionErrors("PAY-2281 is the new top ticket at 94 [2]. PAY-2231 eased off from 71 to 88 [4].", daily);
  assert.deepEqual(e.map((x) => x.key), ["PAY-2231"]);
  assert.equal(dropSentences("PAY-2281 is the new top ticket at 94 [2]. PAY-2231 eased off from 71 to 88 [4].", e), "PAY-2281 is the new top ticket at 94 [2].");
});

test("direction check: a score claim is checked against the score, not the rank; fallback to facts", () => {
  const d = diff(takeSnapshot("PAY", "2026-11-09"), takeSnapshot("PAY", "2026-11-10"), TICKETS);
  assert.deepEqual(directionErrors("PAY-2231 dropped to #2 [2].", d), [], "rank fell from #1 to #2");
  assert.equal(directionErrors("PAY-2231 was top before, but its score has dropped [2].", d).length, 1, "score is unchanged at 88");
  const g = diff(takeSnapshot("GRO", "2026-11-09"), takeSnapshot("GRO", "2026-11-10"), TICKETS);
  const draft = "GRO-790 is now the top priority at 86 [2]. This ticket moved from #6 to #2 [2].";
  const errs = directionErrors(draft, g);
  assert.equal(dropSentences(draft, errs), "", "the follow-on 'This ticket' sentence goes too");
  const fin = finalSummary(draft, errs, facts(g, TICKETS));
  assert.ok(fin.fallback);
  assert.match(fin.text, /UP GRO-790 .*57 → 86/);
});

test("direction check: 'with a score of 50' is not a score move; leftover invented numbers are caught per sentence", () => {
  const g = diff(takeSnapshot("GRO", "2026-11-02"), takeSnapshot("GRO", "2026-11-09"), TICKETS);
  assert.deepEqual(directionErrors("GRO-823 has slipped to #8 with a score of 50 [7].", g), []);
  assert.equal(directionErrors("GRO-823's score has dropped to 50 [7].", g).length, 1);
  const fs = facts(g, TICKETS).map((f) => f.text);
  const e = numberErrors("GRO-881 is top at 95 [2]. It affects 85 stores [2].", fs);
  assert.deepEqual(e.map((x) => x.problem), ["85 is not in any fact"]);
});

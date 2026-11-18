#!/usr/bin/env node
/**
 * Narrated UI tours of the Source Supplies demos: the three "top 3" apps
 * plus a short teaser for every other demo and Labs (all recorded inside
 * the Source Supplies customer site shell).
 *
 * Drives the real showcase (Vite dev server + nebula-server) with
 * Playwright, records the screen, narrates each segment with macOS `say`
 * (en-GB voice), overlays captions and title cards, then muxes with ffmpeg
 * into public/tours/source-supplies/*.mp4.
 *
 *   NEBULA_TARGET=http://127.0.0.1:8080 npx vite        # showcase on :5173
 *   ./target/release/nebula-server                      # NebulaDB on :8080
 *   PLAYWRIGHT_DIR=/path/with/playwright node tours/record-source-top3.mjs [retail stock reports email reorder washroom finder safety solver tender labs]
 *
 * Needs: macOS `say`, ffmpeg/ffprobe, Playwright (+ a Chromium; set CHROME to a binary to reuse one).
 */
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync, existsSync, readdirSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.resolve(here, "../public/tours/source-supplies");
const WORK = process.env.TOUR_WORK || path.resolve(here, "../.tour-work");
const BASE = process.env.SHOWCASE_URL || "http://localhost:5173";
const VOICE = process.env.TOUR_VOICE || "Daniel";
const RATE = process.env.TOUR_RATE || "178";
const W = 1440;
const H = 900;

const req = createRequire(path.join(process.env.PLAYWRIGHT_DIR || here, "noop.js"));
const { chromium } = req("playwright");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/* ------------------------------ overlay ------------------------------ */
const OVERLAY = `
(() => {
  const css = \`
  #tour-cap{position:fixed;left:50%;bottom:28px;transform:translateX(-50%);max-width:1100px;z-index:99999;
    background:rgba(8,10,14,.92);color:#fff;border:1px solid rgba(76,141,255,.55);border-radius:14px;padding:12px 20px;
    font:500 19px/1.4 Poppins,Helvetica,Arial,sans-serif;box-shadow:0 10px 40px rgba(0,0,0,.5);transition:opacity .3s;opacity:0}
  #tour-cap b{color:#3FB950;font-weight:600}
  #tour-card{position:fixed;inset:0;z-index:100000;display:flex;align-items:center;justify-content:center;
    background:radial-gradient(1200px 600px at 30% 20%,#13233f,#05070b);color:#EDEDEE;transition:opacity .5s;opacity:0;pointer-events:none}
  #tour-card .k{font:600 14px/1 "JetBrains Mono",monospace;letter-spacing:.25em;color:#4C8DFF;text-transform:uppercase}
  #tour-card h1{font:700 56px/1.1 Unbounded,Poppins,sans-serif;margin:18px 0 14px}
  #tour-card p{font:400 22px/1.5 Poppins,sans-serif;color:#b9bcc4;max-width:900px;margin:0 auto}
  #tour-card .stat{display:inline-block;margin:28px 14px 0;padding:14px 22px;border:1px solid #2a3a55;border-radius:14px;background:rgba(63,185,80,.08)}
  #tour-card .stat div:first-child{font:700 34px/1.1 Unbounded,sans-serif;color:#3FB950}
  #tour-card .stat div:last-child{font:400 15px/1.3 Poppins,sans-serif;color:#9aa0aa;margin-top:6px}
  .tour-hl{outline:3px solid #4C8DFF !important;outline-offset:4px;border-radius:10px;transition:outline-color .3s}\`;
  const boot = () => {
    if (document.getElementById("tour-cap")) return;
    const s = document.createElement("style"); s.textContent = css; document.head.appendChild(s);
    const c = document.createElement("div"); c.id = "tour-cap"; document.body.appendChild(c);
    const k = document.createElement("div"); k.id = "tour-card"; document.body.appendChild(k);
  };
  window.__tour = {
    cap(html){ boot(); const c=document.getElementById("tour-cap"); if(!html){c.style.opacity=0;return;} c.innerHTML=html; c.style.opacity=1; },
    card(html){ boot(); const k=document.getElementById("tour-card"); if(!html){k.style.opacity=0;return;} k.innerHTML='<div style="text-align:center;padding:40px">'+html+'</div>'; k.style.opacity=1; },
    hl(sel){ document.querySelectorAll(".tour-hl").forEach(e=>e.classList.remove("tour-hl")); if(!sel) return; const e=document.querySelector(sel); if(e){ e.classList.add("tour-hl"); } },
  };
  if (document.readyState !== "loading") boot(); else document.addEventListener("DOMContentLoaded", boot);
})();`;

function card({ kicker, title, sub, stats = [] }) {
  return (
    `<div class="k">${kicker}</div><h1>${title}</h1><p>${sub}</p>` +
    stats.map(([v, l]) => `<div class="stat"><div>${v}</div><div>${l}</div></div>`).join("")
  );
}

/* ------------------------------ helpers ------------------------------ */
function makeHelpers(page) {
  const scrollTo = async (sel, block = "center") => {
    await page.locator(sel).first().evaluate((e, b) => e.scrollIntoView({ behavior: "smooth", block: b }), block);
    await sleep(700);
  };
  return {
    page,
    scrollTo,
    cap: (html) => page.evaluate((h) => window.__tour.cap(h), html),
    card: (html) => page.evaluate((h) => window.__tour.card(h), html),
    hl: (sel) => page.evaluate((s) => window.__tour.hl(s), sel),
    click: async (sel) => {
      await scrollTo(sel);
      const box = await page.locator(sel).first().boundingBox();
      if (box) await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2, { steps: 12 });
      await sleep(250);
      await page.locator(sel).first().click();
    },
    type: async (sel, text) => {
      await scrollTo(sel);
      await page.locator(sel).first().fill("");
      await page.locator(sel).first().type(text, { delay: 35 });
    },
    waitRun: () => page.waitForSelector('[data-tour="run"]:not([disabled])', { timeout: 60000 }),
    /** Click the first button in the page body whose text contains `text`. */
    clickBtn: async (text) => {
      const b = page.locator("main button", { hasText: text }).first();
      await b.evaluate((e) => e.scrollIntoView({ behavior: "smooth", block: "center" }));
      await sleep(500);
      const box = await b.boundingBox();
      if (box) await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2, { steps: 12 });
      await sleep(200);
      await page.waitForFunction((el) => !el.disabled, await b.elementHandle(), { timeout: 60000 });
      await b.click();
    },
    scrollBy: async (px) => {
      await page.evaluate((y) => document.querySelector("main")?.scrollBy({ top: y, behavior: "smooth" }), px);
      await sleep(900);
    },
    top: () => page.evaluate(() => document.querySelector("main")?.scrollTo({ top: 0, behavior: "smooth" })),
  };
}

/* ------------------------------ tours ------------------------------ */
const TOURS = {
  retail: {
    file: "retail-pod-autopilot",
    tab: "ssretail",
    setup: async (h) => {
      await h.waitRun();
      await h.page.locator("button", { hasText: "Reset learned rules" }).click();
      await sleep(400);
    },
    segments: [
      {
        say: "This is the problem Dave called the biggest pain. A national retailer sends about six hundred small orders a month through its own portal. Each one is keyed into Merlin by hand. Then, for every delivery, someone hunts down two proofs of delivery and uploads both. Dave put the cost at around twenty thousand pounds a year.",
        act: async (h) =>
          h.card(card({ kicker: "Source Supplies × Jordan AI · Top 3 · #1", title: "Retail POD Autopilot", sub: "Retail portal orders → Merlin → courier → two proofs of delivery back to the portal. Automated end to end, with people only on exceptions." })),
      },
      {
        say: "Here's the business case up front. Fourteen minutes of manual work per order today. With Jordan, about one minute, because people only touch the exceptions.",
        cap: "Today: <b>14 min</b> per order by hand → with Jordan: <b>≈1 min</b>",
        act: async (h) => {
          await h.card(null);
          await h.hl('[data-tour="benefit"]');
        },
      },
      {
        say: "Let's run Friday's batch. Jordan reads the shared mailbox in NebulaDB: today's portal work orders, the Merlin delivery tickets, and the courier's proof of delivery emails. Every step is a real query you can see. No black box.",
        cap: "One click runs the day — every step is a real, visible NebulaDB query",
        act: async (h) => {
          await h.hl(null);
          await h.click('[data-tour="run"]');
          await sleep(2500);
          await h.scrollTo('[data-tour="kpis"]', "end");
        },
      },
      {
        say: "First, intake. Thirteen portal orders. Known retailer item codes map straight to Source products, and Merlin sales orders are drafted. A re-sent duplicate is caught and skipped, so nothing is keyed twice.",
        cap: "Portal orders → <b>Merlin drafts</b> · duplicate PO caught automatically",
        act: async (h) => h.scrollTo('[data-tour="intake-review"]', "center"),
      },
      {
        say: "Two lines use item codes Jordan has never seen. It finds the closest product in the catalogue, and a person confirms it once. From then on, that code maps by itself.",
        cap: "New item code? Jordan suggests the product — <b>confirm once, remembered forever</b>",
        act: async (h) => {
          await h.hl('[data-tour="intake-review"]');
          await sleep(2500);
          await h.click('[data-tour="intake-review"] button');
          await sleep(900);
          await h.click('[data-tour="intake-review"] button');
          await h.hl(null);
        },
      },
      {
        say: "Now proof of delivery. Thirty deliveries, each needing the Merlin scan and the courier photo. Twenty six are matched, checked and attached with nobody touching them. That's eighty seven percent straight through.",
        cap: "<b>26 of 30</b> deliveries: both PODs matched, checked and attached — no person",
        act: async (h) => {
          await h.scrollTo('[data-tour="kpis"]', "start");
          await h.hl('[data-tour="kpis"]');
          await sleep(3500);
          await h.hl(null);
          await h.click('[data-tour="filter-attached"]');
        },
      },
      {
        say: "This one is clever. The courier's email had no P O number, and two digits of the consignment were swapped. Jordan still matched it, using the near identical consignment, the store postcode, and the delivery date.",
        cap: "No PO, mistyped consignment → <b>still matched</b> (consignment + postcode + date)",
        act: async (h) => {
          const fuzzy = h.page.locator('tr[data-tour^="pod-"]', { hasText: "AI fuzzy" }).first();
          await fuzzy.evaluate((e) => e.scrollIntoView({ behavior: "smooth", block: "center" }));
          await sleep(600);
          await fuzzy.click();
          await sleep(500);
          await h.hl('[data-tour="attach-pack"]');
        },
      },
      {
        say: "Three deliveries need a person, and each one says exactly why. Here, the courier used a different postcode for the Leeds store. One click confirms it's the same store.",
        cap: "Exceptions explain themselves — <b>one click</b> to resolve",
        act: async (h) => {
          await h.hl(null);
          await h.click('[data-tour="filter-exception"]');
          await sleep(500);
          const leeds = h.page.locator('tr[data-tour="pod-exception"]', { hasText: "Leeds White Rose" }).first();
          await leeds.click();
          await sleep(600);
          await h.hl('[data-tour="attach-pack"]');
        },
      },
      {
        say: "And Jordan remembers. The staff don't even know they're training it.",
        cap: "Every correction becomes a rule — <b>staff train Jordan just by working</b>",
        act: async (h) => {
          await h.hl(null);
          await h.click('[data-tour="teach-address"]');
        },
      },
      {
        say: "Run the batch again, and that exception has gone. The longer the team works with it, the fewer exceptions there are.",
        cap: "Re-run: the Leeds exception is gone — <b>fewer exceptions every week</b>",
        act: async (h) => {
          await h.click('[data-tour="run"]');
          await sleep(4000);
          await h.scrollTo('[data-tour="kpis"]', "start");
          await h.hl('[data-tour="kpis"]');
        },
      },
      {
        say: "The result: around twenty two thousand pounds a year back, and close to a full person's time freed up for growth, without hiring. That is the business value of A I automation.",
        act: async (h) => {
          await h.hl(null);
          await h.cap(null);
          await h.card(card({ kicker: "Business benefit", title: "≈ £22k a year back", sub: "Retail orders keyed and proven automatically. People handle only the exceptions, and every fix teaches Jordan.", stats: [["14 → 1 min", "per retail order"], ["87%+", "straight-through"], ["≈ 0.9 FTE", "freed for growth"]] }));
        },
      },
    ],
  },

  stock: {
    file: "stock-autopilot",
    tab: "ssstock",
    setup: async (h) => h.waitRun(),
    segments: [
      {
        say: "Problem number two: stock control. Merlin doesn't hold live stock, and the popular lines, the ones that make up ninety five percent of the business, still run out. Dave has been pasting three months of sales into Chat G P T to work out an average.",
        act: async (h) =>
          h.card(card({ kicker: "Source Supplies × Jordan AI · Top 3 · #2", title: "Stock Autopilot", sub: "A nightly forecast with seasonality, growth, one-off cleaning and safety stock, then supplier POs ready to approve." })),
      },
      {
        say: "Jordan does this every night, and does it properly. The headline: sixty percent fewer stock outs, and less cash tied up in the warehouse.",
        cap: "<b>60% fewer stock-outs</b> — and less cash sitting in stock",
        act: async (h) => {
          await h.card(null);
          await h.hl('[data-tour="benefit"]');
        },
      },
      {
        say: "Jordan reads twenty four months of despatches from NebulaDB, plus an estimated stock position: the last count, plus goods in, minus what went out. Special orders for single customers are kept out of the core forecast.",
        cap: "24 months of Merlin despatches + estimated stock — <b>specials kept out</b>",
        act: async (h) => {
          await h.hl(null);
          await h.click('[data-tour="run"]');
          await sleep(2500);
          await h.scrollTo('[data-tour="kpis"]', "start");
        },
      },
      {
        say: "Sixteen core lines checked. Three will run out before a normal order could arrive, and five supplier purchase orders are already drafted.",
        cap: "<b>3 lines</b> will run out before a normal order lands · <b>5 POs</b> drafted",
        act: async (h) => {
          await h.hl('[data-tour="kpis"]');
          await sleep(4000);
          await h.hl(null);
        },
      },
      {
        say: "Take recycled jumbo toilet roll. A plain average says about nine hundred and forty a month. But Jordan spotted a step up in September, a new contract, and November is usually busier. So it forecasts nearly twelve hundred, and warns that stock runs out on the tenth of November.",
        cap: "Average says 939 · Jordan: <b>new contract + November peak → 1,173</b>, runs out 10 Nov",
        act: async (h) => {
          await h.click('[data-tour="row-JR-410"]');
          await h.hl('[data-tour="why"]');
        },
      },
      {
        say: "Now the opposite case. Heavy duty sacks had a huge October, but almost half of it came from one event order. Jordan treats it as a one off, so we don't end up with a warehouse full of sacks.",
        cap: "One big event order? <b>Ignored as a one-off</b> — no overstock",
        act: async (h) => {
          await h.hl(null);
          await h.click('[data-tour="row-BG-625"]');
          await sleep(300);
          await h.scrollTo('[data-tour="why"]', "center");
          await h.hl('[data-tour="why"]');
        },
      },
      {
        say: "Purchase orders are grouped by supplier, rounded to minimum order quantities, with the urgent ones first. A buyer approves with one click.",
        cap: "One PO per supplier · MOQ-rounded · <b>one-click approve</b>",
        act: async (h) => {
          await h.hl(null);
          await h.scrollTo('[data-tour="pos"]', "center");
          await h.hl('[data-tour="pos"]');
          await sleep(2500);
          await h.page.locator('[data-tour="pos"] button', { hasText: "Approve" }).first().click();
        },
      },
      {
        say: "And here's the proof. We replayed the last twelve months both ways. The three month average ran short forty three times. Jordan, seventeen. That's over three hundred fewer re-deliveries, about fifteen thousand pounds saved, and three thousand pounds less stock sitting on the shelves.",
        cap: "12 months replayed: stock-outs <b>43 → 17</b> · re-deliveries <b>520 → 206</b>",
        act: async (h) => {
          await h.hl(null);
          await h.scrollTo('[data-tour="backtest"]', "center");
          await h.hl('[data-tour="backtest"]');
        },
      },
      {
        say: "Fewer let downs for customers, fewer extra van runs, and a buyer who spends minutes on this, not half a day.",
        act: async (h) => {
          await h.hl(null);
          await h.cap(null);
          await h.card(card({ kicker: "Business benefit", title: "Never run out of the lines that matter", sub: "Seasonal forecasts, new-contract step-ups and one-offs handled every night, with POs ready to approve.", stats: [["−60%", "stock-out months"], ["≈ £15k", "shortfall cost avoided / yr"], ["−£3k", "average stock held"]] }));
        },
      },
    ],
  },

  reports: {
    file: "report-studio",
    tab: "ssreports",
    setup: async (h) => h.waitRun(),
    segments: [
      {
        say: "Problem three: customer reports. Big customers want regular reports, but a new Merlin report means a support ticket, or someone who writes S Q L. Dave's last one took three attempts, days apart.",
        act: async (h) =>
          h.card(card({ kicker: "Source Supplies × Jordan AI · Top 3 · #3", title: "Jordan Report Studio", sub: "Ask for any customer report in plain English. Jordan writes the query, checks the figures and schedules it." })),
      },
      {
        say: "With Jordan, anyone in the office just asks.",
        cap: "From a <b>4-week Merlin ticket</b> to <b>seconds</b>",
        act: async (h) => {
          await h.card(null);
          await h.hl('[data-tour="benefit"]');
        },
      },
      {
        say: "Here's Dave's own example: the report for a contract cleaner, for the last three months combined.",
        cap: "“Send me the report for Ashby Contract Cleaning for the last 3 months combined”",
        act: async (h) => {
          await h.hl(null);
          await h.type('[data-tour="ask"]', "Send me the report for Ashby Contract Cleaning for the last 3 months combined");
          await h.click('[data-tour="run"]');
        },
      },
      {
        say: "Jordan shows what it understood: the customer, the period, the grouping, and what to compare against. Then it writes the S Q L itself. One query against NebulaDB, back in milliseconds.",
        cap: "Jordan shows what it understood — then <b>writes the SQL itself</b>",
        act: async (h) => {
          await h.scrollTo('[data-tour="understood"]', "start");
          await h.hl('[data-tour="understood"]');
        },
      },
      {
        say: "The report is ready straight away. Spend, the change on the previous quarter, orders, sites and product lines. Every figure is calculated from the data, never made up by a model.",
        cap: "Every figure calculated from the data — <b>never invented</b>",
        act: async (h) => {
          await h.hl(null);
          await h.scrollTo('[data-tour="kpis"]', "start");
          await h.hl('[data-tour="kpis"]');
        },
      },
      {
        say: "Jordan also adds what an account manager would want to know: the biggest line, the biggest mover, and greener swaps that would cut this customer's carbon. That's a sales conversation, ready to go.",
        cap: "Insights included: biggest mover · <b>greener swaps = a sales conversation</b>",
        act: async (h) => {
          await h.hl(null);
          await h.scrollTo('[data-tour="insights"]', "center");
          await h.hl('[data-tour="insights"]');
        },
      },
      {
        say: "Now a different customer, by site.",
        cap: "“What did Kingsmead buy in the last 3 months, by site?”",
        act: async (h) => {
          await h.hl(null);
          await h.type('[data-tour="ask"]', "What did Kingsmead buy in the last 3 months, by site?");
          await h.click('[data-tour="run"]');
        },
      },
      {
        say: "It flags a spike. One school site took nearly twice as much toilet roll as usual. Worth a call.",
        cap: "Spikes flagged automatically — <b>worth a call</b>",
        act: async (h) => {
          await h.scrollTo('[data-tour="insights"]', "center");
          await h.hl('[data-tour="insights"]');
        },
      },
      {
        say: "Follow ups just work. Now just paper, by month. No new ticket.",
        cap: "Follow-up: “Now just paper, by month” — <b>no new ticket</b>",
        act: async (h) => {
          await h.hl(null);
          await h.type('[data-tour="ask"]', "Now just paper, by month");
          await h.click('[data-tour="run"]');
          await sleep(1200);
          await h.scrollTo('[data-tour="kpis"]', "start");
        },
      },
      {
        say: "Export it to Excel, draft the customer email, and schedule it to go out on the last Friday of every month, automatically.",
        cap: "Excel · email draft · <b>scheduled for the last Friday of every month</b>",
        act: async (h) => {
          await h.click('[data-tour="email"]');
          await sleep(600);
          await h.click('[data-tour="schedule"]');
          await sleep(800);
          await h.scrollTo('[data-tour="email-draft"]', "center");
          await h.hl('[data-tour="email-draft"]');
          await sleep(2500);
          await h.scrollTo('[data-tour="schedules"]', "center");
          await h.hl('[data-tour="schedules"]');
        },
      },
      {
        say: "From weeks of waiting, to seconds. That's hours back every week, and a better service for your biggest customers.",
        act: async (h) => {
          await h.hl(null);
          await h.cap(null);
          await h.card(card({ kicker: "Business benefit", title: "Customer reports in seconds", sub: "No SQL, no vendor tickets. Anyone asks in plain English, follow-ups refine, and monthly reports send themselves.", stats: [["4 weeks → secs", "per new report"], ["0", "SQL written by staff"], ["Monthly", "reports run unattended"]] }));
        },
      },
    ],
  },
};


/** A short teaser: title card → steps → benefit card. */
function teaser({ file, tab, hash, n, title, sub, steps, end }) {
  return {
    file,
    tab,
    hash,
    setup: async (h) => {
      await h.page.waitForSelector("main button.btn", { timeout: 60000 }).catch(() => undefined);
      await sleep(1500);
    },
    segments: [
      { say: steps[0].say, act: async (h) => h.card(card({ kicker: `Source Supplies × Jordan AI · ${n}`, title, sub })) },
      ...steps.slice(1).map((st, i) => ({
        say: st.say,
        cap: st.cap,
        act: async (h) => {
          if (i === 0) await h.card(null);
          await st.act(h);
        },
      })),
      {
        say: end.say,
        act: async (h) => {
          await h.cap(null);
          await h.card(card({ kicker: "Business benefit", title: end.title, sub: end.sub, stats: end.stats }));
        },
      },
    ],
  };
}

const TEASERS = {
  email: teaser({
    file: "email-order", tab: "ssemail", n: "Orders", title: "Email Order Intake", sub: "Emailed orders drafted into Merlin, not re-keyed.",
    steps: [
      { say: "Most orders come through your website. The rest arrive by email, and someone re-keys every one into Merlin." },
      { say: "Here's a customer's email. Jordan pulls out each line and quantity, and matches it to your catalogue.", cap: "Email in → every line matched to the catalogue", act: async (h) => { await h.clickBtn("Brightway FM"); await h.clickBtn("Draft Merlin order"); await sleep(2500); await h.scrollBy(450); } },
      { say: "Clear matches go onto a draft Merlin sales order for the right account. Nothing goes into Merlin until a person confirms.", cap: "Draft Merlin order — <b>a person confirms</b>", act: async (h) => h.scrollBy(450) },
      { say: "When an email is vague, Jordan doesn't guess. Weak matches stay with a person.", cap: "Vague email? <b>Jordan doesn't guess</b>", act: async (h) => { await h.clickBtn("Vague one-liner"); await h.clickBtn("Draft Merlin order"); await sleep(2500); await h.scrollBy(450); } },
    ],
    end: { say: "Every emailed order drafted in seconds. Your team just checks and confirms.", title: "No more re-keying", sub: "Emailed orders become Merlin drafts in seconds.", stats: [["Seconds", "per emailed order"], ["0", "lines re-typed"], ["100%", "checked by a person"]] },
  }),
  reorder: teaser({
    file: "smart-reorder", tab: "ssreorder", n: "Orders", title: "Smart Reorder", sub: "“Same as last month, but…” — for every site.",
    steps: [
      { say: "Multi-site customers re-key the same order every month, site by site." },
      { say: "The buyer types one sentence: same as last month for Kingsmead North site, but switch to recycled hand towels.", cap: "“Same as last month… but switch to recycled hand towels”", act: async (h) => { await h.clickBtn("Kingsmead North"); await h.clickBtn("Draft basket"); await sleep(2500); await h.scrollBy(400); } },
      { say: "Jordan reads six months of that site's orders, forecasts each line, and flags a spike in toilet roll, so nobody over-orders by mistake.", cap: "Forecast per line · <b>spikes flagged</b>", act: async (h) => h.scrollBy(450) },
      { say: "The towels are swapped for the recycled version at the same amount of use, and every line is checked against stock and the site's delivery day.", cap: "Greener swap · stock and delivery-day checked", act: async (h) => h.scrollBy(450) },
    ],
    end: { say: "A month's order for a site, in seconds, ready to approve.", title: "Repeat orders in one sentence", sub: "Forecast, swaps and delivery checks for every site.", stats: [["10 min", "saved per site per month"], ["Greener", "swaps built in"]] },
  }),
  washroom: teaser({
    file: "washroom-desk", tab: "sswashroom", n: "Operations", title: "Washroom Service Desk", sub: "The right priority, a missed-visit check and a reply, in one go.",
    steps: [
      { say: "Your washroom team services sanitary bins, air care and dryers on four and eight week cycles. Every issue needs the right priority and a quick, correct reply." },
      { say: "A school reports a sanitary bin not changed for three weeks. Jordan compares it with similar past tickets to suggest the service and the priority.", cap: "Similar past tickets → <b>service + priority</b>", act: async (h) => { await h.clickBtn("Sanitary bin not changed"); await h.clickBtn("Triage"); await sleep(2500); await h.scrollBy(400); } },
      { say: "It checks the site's contract and visit history, flags a missed visit when the last one is older than the cycle, and drafts the reply.", cap: "Missed visit flagged · <b>reply drafted</b>", act: async (h) => h.scrollBy(500) },
      { say: "New site? Describe it in one line, and Jordan drafts the units and visit frequency, for a person to price.", cap: "One line → a draft washroom quote", act: async (h) => { await h.clickBtn("uote"); await sleep(600); await h.clickBtn("Office, 3 floors"); await h.clickBtn("Draft quote"); await sleep(2500); await h.scrollBy(400); } },
    ],
    end: { say: "Faster, consistent answers for every washroom customer.", title: "Every ticket handled right", sub: "Priority, missed visits and replies in seconds; quotes drafted from one line.", stats: [["10 min", "saved per ticket"], ["0", "missed visits unnoticed"]] },
  }),
  finder: teaser({
    file: "green-finder", tab: "ssfinder", n: "Sales", title: "Green Product Finder", sub: "The greener choice that's in stock, on budget and right for the site.",
    steps: [
      { say: "Buyers ask: what should I use? And more and more, they want the greener option." },
      { say: "A school wants a low odour floor cleaner for the canteen. One search mixes the meaning of the request with hard rules: eco badges, the education sector, in stock, and on budget.", cap: "Meaning + eco badges + sector + stock + price — <b>one search</b>", act: async (h) => { await h.clickBtn("Low-odour canteen"); await h.clickBtn("Find products"); await sleep(3000); await h.scrollBy(450); } },
      { say: "Results are ranked on fit, eco badges, cost per use and lead time. Near misses say exactly which rule they failed.", cap: "Ranked by fit, badges, cost per use · <b>near misses explained</b>", act: async (h) => h.scrollBy(500) },
      { say: "Greener swaps are priced at this customer's real order volume, so your account manager can show the saving, not just claim it.", cap: "Greener swaps priced at <b>their real volume</b>", act: async (h) => h.scrollBy(500) },
    ],
    end: { say: "Greener sales conversations, in seconds.", title: "Sell the greener option", sub: "The right product, with proof of the saving.", stats: [["15 min", "saved per product question"], ["Real", "volumes, real savings"]] },
  }),
  safety: teaser({
    file: "coshh-safety", tab: "sssafety", n: "Compliance", title: "COSHH & Safety Data", sub: "Safety answers from the data sheet, with the section cited.",
    steps: [
      { say: "Cleaners on site need safety answers from the data sheet. Fast, and right." },
      { say: "A cleaner has splashed mould remover in her eye. Emergencies never go to the AI. Jordan shows a fixed first aid card from section four, and says call nine nine nine, or one one one.", cap: "Emergency → <b>fixed first-aid card</b>, never AI text", act: async (h) => { await h.clickBtn("Splashed in the eye"); await h.clickBtn("Ask"); await sleep(2500); await h.scrollBy(350); } },
      { say: "Mixing products? Always the same safe answer: don't mix unless both data sheets say it's safe, with section ten cited.", cap: "Mixing → the same safe rule, <b>section 10 cited</b>", act: async (h) => { await h.clickBtn("Can I mix with bleach"); await h.clickBtn("Ask"); await sleep(2500); await h.scrollBy(350); } },
      { say: "And a draft COSHH assessment is built from the cited sections in one click, marked for a competent person to check.", cap: "Draft COSHH assessment — <b>one click</b>", act: async (h) => { await h.clickBtn("Draft COSHH assessment"); await sleep(2500); await h.scrollBy(450); } },
    ],
    end: { say: "Safer sites, and COSHH paperwork in minutes.", title: "Safety answers you can trust", sub: "Cited from the data sheet. Emergencies and mixing handled by fixed rules.", stats: [["20 min", "saved per COSHH draft"], ["100%", "answers cited"]] },
  }),
  solver: teaser({
    file: "problem-solver", tab: "sssolver", n: "Sales", title: "Cleaning Problem Solver", sub: "The method and the kit for tough jobs.",
    steps: [
      { say: "Tough cleaning jobs, mould, gum, smells, worn floors, need the right method and the right kit." },
      { say: "Black mould keeps coming back in student bedrooms. Jordan finds the right guide for painted walls in student housing.", cap: "The right guide for <b>this surface and sector</b>", act: async (h) => { await h.clickBtn("Black mould"); await h.clickBtn("Solve it"); await sleep(3000); await h.scrollBy(450); } },
      { say: "It sets out the method, the kit list with what's in stock, and the safety points from the data sheets.", cap: "Method · kit with stock · safety points", act: async (h) => h.scrollBy(500) },
      { say: "Add the kit to a quick order as a draft. A thirty minute phone call becomes a two minute answer.", cap: "Kit → <b>draft quick order</b>", act: async (h) => h.scrollBy(400) },
    ],
    end: { say: "Expert answers for every customer, from every member of your team.", title: "Every problem, solved fast", sub: "Method, kit and safety in one answer, with a draft order.", stats: [["30 min", "saved per problem call"], ["Draft", "order ready"]] },
  }),
  tender: teaser({
    file: "tender-responder", tab: "sstender", n: "Sales", title: "Tender Responder", sub: "Evidence for every question, scored and cited.",
    steps: [
      { say: "School, NHS and council tenders ask the same questions in new words, and someone spends half a day finding the answers." },
      { say: "Paste the buyer's questions. For each one, Jordan finds evidence from your capability library and past winning answers.", cap: "Every question → <b>evidence from past wins</b>", act: async (h) => { await h.clickBtn("Academy trust tender"); await h.clickBtn("Find evidence"); await sleep(3000); await h.scrollBy(450); } },
      { say: "Every question gets a coverage score. Weak ones are marked for a human, never bluffed. Strong ones are drafted with sources and export in one click.", cap: "Coverage scored · weak ones <b>flagged, never bluffed</b>", act: async (h) => h.scrollBy(550) },
    ],
    end: { say: "Win more tenders, in a fraction of the time.", title: "Tenders in an hour, not a day", sub: "Cited answers from your own evidence.", stats: [["½ day", "saved per tender"], ["Cited", "every answer"]] },
  }),
  labs: teaser({
    file: "labs-source", tab: "labs", hash: "#/labs?pack=source", n: "Explore", title: "AI Labs on your data", sub: "See how the AI finds things, and when it says no.",
    steps: [
      { say: "Want to see how the AI actually works? The Labs run eight short experiments on Source Supplies' own demo data." },
      { say: "Search by meaning instead of keywords, filters that matter, answers built step by step, and when the AI should say no.", cap: "8 hands-on labs · <b>2 to 10 minutes each</b>", act: async (h) => h.scrollBy(500) },
      { say: "For example, meaning versus words: the same question answered by keyword search and by meaning, side by side.", cap: "Meaning vs words — <b>side by side</b>", act: async (h) => { await h.page.locator("main a", { hasText: "Meaning vs words" }).first().click(); await sleep(4000); await h.scrollBy(450); } },
    ],
    end: { say: "Hands on, with your data, so your team trusts what it sees.", title: "Understand it, then trust it", sub: "Eight labs on Source Supplies demo data.", stats: [["8", "labs"], ["Your", "demo data"]] },
  }),
};
Object.assign(TOURS, TEASERS);

/* ------------------------------ recorder ------------------------------ */
function tts(text, file) {
  execFileSync("say", ["-v", VOICE, "-r", RATE, "-o", file, text]);
  const d = execFileSync("ffprobe", ["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0", file]).toString();
  return Math.round(Number(d) * 1000);
}

async function record(key) {
  const t = TOURS[key];
  const dir = path.join(WORK, key);
  rmSync(dir, { recursive: true, force: true });
  mkdirSync(dir, { recursive: true });
  const segs = t.segments.map((s, i) => {
    const f = path.join(dir, `seg${i}.aiff`);
    return { ...s, audio: f, ms: tts(s.say, f) };
  });

  const browser = await chromium.launch({ executablePath: process.env.CHROME || undefined });
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, colorScheme: "dark", recordVideo: { dir, size: { width: W, height: H } } });
  await ctx.addInitScript(OVERLAY);
  const page = await ctx.newPage();
  const t0 = Date.now();
  await page.goto(`${BASE}/?site=sourcesupplies${t.hash ?? `#/tab/${t.tab}`}`);
  const h = makeHelpers(page);
  await page.evaluate(() => window.__tour.card('<div style="height:10px"></div>'));
  await t.setup(h);
  const starts = [];
  for (const s of segs) {
    const start = Date.now() - t0;
    starts.push(start);
    await h.cap(s.cap ?? null);
    await Promise.all([s.act(h).catch((e) => console.warn(`  [${key}] action warning: ${e.message.split("\n")[0]}`)), sleep(s.ms + 450)]);
  }
  await sleep(1200);
  const total = Date.now() - t0;
  await page.close();
  const video = await page.video().path();
  await ctx.close();
  await browser.close();

  const trim = Math.max(0, starts[0] - 200);
  const args = ["-y", "-ss", (trim / 1000).toFixed(3), "-i", video];
  segs.forEach((s) => args.push("-i", s.audio));
  const mix = segs.map((_, i) => `[${i + 1}:a]adelay=${starts[i] - trim}|${starts[i] - trim}[a${i}]`).join(";");
  const filter = `${mix};${segs.map((_, i) => `[a${i}]`).join("")}amix=inputs=${segs.length}:normalize=0,volume=1.6[aout];[0:v]scale=1280:-2,fps=25[vout]`;
  mkdirSync(OUT, { recursive: true });
  const out = path.join(OUT, `${t.file}.mp4`);
  args.push(
    "-filter_complex", filter, "-map", "[vout]", "-map", "[aout]",
    "-t", ((total - trim) / 1000).toFixed(3),
    "-c:v", "libx264", "-preset", "slow", "-crf", "27", "-pix_fmt", "yuv420p",
    "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart", out
  );
  execFileSync("ffmpeg", args, { stdio: ["ignore", "ignore", "pipe"] });
  mkdirSync(path.join(OUT, "posters"), { recursive: true });
  // Poster: a live-demo frame ~42% in (the UI under the title overlay the site draws).
  execFileSync("ffmpeg", ["-y", "-v", "error", "-ss", (((total - trim) / 1000) * 0.42).toFixed(1), "-i", out, "-frames:v", "1", "-vf", "scale=960:-2", "-q:v", "4", path.join(OUT, "posters", `${t.file}.jpg`)]);
  console.log(`✓ ${key}: ${out} (${((total - trim) / 1000).toFixed(1)} s)`);
  return out;
}

const keys = process.argv.slice(2).length ? process.argv.slice(2) : Object.keys(TOURS);
for (const k of keys) if (!TOURS[k]) throw new Error(`unknown tour '${k}' (have: ${Object.keys(TOURS).join(", ")})`);
const outs = [];
for (const k of keys) outs.push(await record(k));
if (["retail", "stock", "reports"].every((k) => keys.includes(k))) {
  const list = path.join(WORK, "concat.txt");
  writeFileSync(list, ["retail-pod-autopilot", "stock-autopilot", "report-studio"].map((f) => `file '${path.join(OUT, f)}.mp4'`).join("\n"));
  const all = path.join(OUT, "source-supplies-top3.mp4");
  execFileSync("ffmpeg", ["-y", "-f", "concat", "-safe", "0", "-i", list, "-c", "copy", "-movflags", "+faststart", all], { stdio: "ignore" });
  execFileSync("ffmpeg", ["-y", "-v", "error", "-ss", "60", "-i", all, "-frames:v", "1", "-vf", "scale=960:-2", "-q:v", "4", path.join(OUT, "posters", "source-supplies-top3.jpg")]);
  console.log(`✓ combined: ${all}`);
}
if (existsSync(WORK) && !process.env.TOUR_KEEP) for (const d of readdirSync(WORK)) rmSync(path.join(WORK, d), { recursive: true, force: true });

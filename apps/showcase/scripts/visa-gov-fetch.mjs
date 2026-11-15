#!/usr/bin/env node
/**
 * Build the Visa Desk's OFFICIAL corpus from Thai government websites.
 *
 * The Visa AI Agent answers only from these passages, so every fact the
 * model writes can be traced to a public government page. Two kinds of
 * source:
 *
 *  1. Thai e-Visa master data (Ministry of Foreign Affairs). The public
 *     JSON behind www.thaievisa.go.th lists every visa sub-type, the purposes
 *     of visit it covers and the documents each purpose needs (funds,
 *     insurance, age, …). One passage per purpose of visit.
 *  2. Official pages rendered in a headless browser (the sites are
 *     single-page apps): TDAC (Immigration Bureau), LTR and SMART (BOI),
 *     Thailand Privilege (a TAT company) and TAT's passport & visa pages.
 *
 * Only sites whose robots.txt allows the pages are fetched. We do not
 * fetch www.immigration.go.th: it answers automated clients with a bot
 * challenge, and we don't work around that. Pages are fetched once, a few
 * seconds apart, and only short passages with a link back are kept.
 *
 * Usage (playwright-core and a Chrome build are needed for step 2):
 *   cd <dir with playwright-core installed>
 *   CHROME=/path/to/chrome node <repo>/apps/showcase/scripts/visa-gov-fetch.mjs
 * Writes apps/showcase/src/labs/visa/gov-corpus.json. Re-run to refresh;
 * the agent re-seeds NebulaDB when the corpus version changes.
 */
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const OUT = join(dirname(fileURLToPath(import.meta.url)), "../src/labs/visa/gov-corpus.json");
const UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0 Safari/537.36";
const today = new Date().toISOString().slice(0, 10);
const docs = [];

const clean = (s) => s.replace(/ /g, " ").replace(/[ \t]+/g, " ").replace(/\s*\n\s*/g, "\n").trim();
const slug = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 40);

function add(id, text, meta) {
  docs.push({ id, text: clean(text), metadata: { kind: "official", fetched: today, lang: "en", ...meta } });
}

// ------------------------------------------------------------------ 1. e-Visa master data
async function json(url) {
  const r = await fetch(url, { headers: { "user-agent": UA, accept: "application/json" } });
  if (!r.ok) throw new Error(`${url}: HTTP ${r.status}`);
  return (await r.json()).data;
}

/** e-Visa sub-type → the public category page that describes it, and the agent's visa tag. */
function category(type, sub) {
  const c = sub.category;
  if (type === 5 && c === "MT") return ["medical-treatment", "medical"];
  if (type === 5) return ["tourist-visa", "TR"];
  if (type === 7) return ["transit-visa", "transit"];
  if (type === 8) return ["smart-visa", "SMART"];
  if (type === 9) return ["ltr-visa", "LTR"];
  if (type === 10) return ["dtv-visa", "DTV"];
  if (type === 11) return ["thailand-privilege-card", "Privilege"];
  if (type === 3) return ["courtesy-visa", "courtesy"];
  if (type === 1 || type === 2) return ["diploma-official-visa", "official"];
  if (sub.id === 30) return ["retirement", "Non-O-A"];
  if (sub.id === 31) return ["retirement", "Non-O-X"];
  if (c === "ED" && /^R/.test(sub.name)) return ["non-immigrant-r", "Non-R"];
  if (c === "ED" && /^RS/.test(sub.name)) return ["non-immigrant-rs", "Non-RS"];
  if (c === "ED") return ["non-immigrant-ed", "Non-ED"];
  if (c === "M") return ["non-immigrant-m", "Non-M"];
  if (c === "O" || c === "O(L-A)") return ["non-immigrant-o", "Non-O"];
  if (c === "F") return ["non-immigrant-f-o", "Non-F"];
  return ["business-investor-visa", "Non-B"];
}

async function evisa() {
  const ts = Date.now();
  const visa = await json(`https://thaievisa.go.th/ApiGateway/masterdata/visa?p=${ts}`);
  const all = await json(`https://thaievisa.go.th/ApiGateway/masterdata/all?p=${ts}`);
  const pov = await json(`https://thaievisa.go.th/ApiGateway/masterdata/individual/purposeofvisit?p=${ts}`);
  const types = new Map(visa.visaTypes.map((t) => [t.id, t.name]));
  const subs = new Map(visa.visaSubTypes.map((s) => [s.id, s]));
  const purposes = new Map(pov.purposeOfVisitsIndividual.map((p) => [p.id, p.name]));

  // Documents are listed per embassy; keep the generic list (no embassy code) and
  // drop near-duplicates (embassy-specific notes appended to the same item).
  const reqs = new Map();
  for (const d of all.documents) {
    if (d.consularCode || !purposes.has(d.purposeOfVisitId)) continue;
    const key = `${d.purposeOfVisitId}:${d.visaSubTypeId}`;
    const list = reqs.get(key) ?? [];
    const name = clean(d.name);
    if (!list.some((x) => x.slice(0, 50) === name.slice(0, 50))) list.push(name);
    reqs.set(key, list);
  }

  const seen = new Set();
  for (const m of visa.visaTypeMappings) {
    const purpose = purposes.get(m.purposeOfVisitId);
    const sub = subs.get(m.visaSubTypeId);
    const list = reqs.get(`${m.purposeOfVisitId}:${m.visaSubTypeId}`);
    if (!purpose || !sub || !list?.length || /IMF\/WBG|TECO|diplomat|official/i.test(`${sub.name} ${purpose}`)) continue;
    // The same purpose text appears under several ids (one per applicant group); keep one.
    const sig = `${sub.id}:${purpose}`;
    if (seen.has(sig)) continue;
    seen.add(sig);
    const [page, tag] = category(m.visaTypeId, sub);
    const vname = `${types.get(m.visaTypeId)}${sub.name && !types.get(m.visaTypeId).includes(sub.name) ? ` — ${sub.name}` : ""}`;
    const text = [
      `${vname}. Purpose of visit: ${purpose}.`,
      `Documents the Thai e-Visa system asks for:`,
      ...list.map((x, i) => `${i + 1}. ${x}`),
    ].join("\n");
    add(`gov-evisa-${m.purposeOfVisitId}-${sub.id}`, text.slice(0, 4000), {
      site: "thaievisa.go.th",
      authority: "Ministry of Foreign Affairs (Thai e-Visa)",
      title: `${vname}: ${purpose}`.slice(0, 160),
      url: `https://www.thaievisa.go.th/visa/${page}`,
      visa: tag,
    });
  }
}

// ------------------------------------------------------------------ 2. rendered official pages
const PAGES = [
  // Ministry of Foreign Affairs — Thai e-Visa
  ["thaievisa.go.th", "Ministry of Foreign Affairs (Thai e-Visa)", "https://www.thaievisa.go.th/", "general"],
  // Immigration Bureau — Thailand Digital Arrival Card
  ["tdac.immigration.go.th", "Immigration Bureau (TDAC)", "https://tdac.immigration.go.th/", "arrival"],
  // Board of Investment — LTR
  ["ltr.boi.go.th", "Board of Investment (LTR)", "https://ltr.boi.go.th/", "LTR"],
  ["ltr.boi.go.th", "Board of Investment (LTR)", "https://ltr.boi.go.th/page/required-documents.html", "LTR"],
  ["ltr.boi.go.th", "Board of Investment (LTR)", "https://ltr.boi.go.th/page/1-year-reporting.html", "LTR"],
  ["ltr.boi.go.th", "Board of Investment (LTR)", "https://ltr.boi.go.th/page/visa-issuance-info.html", "LTR"],
  ["ltr.boi.go.th", "Board of Investment (LTR)", "https://ltr.boi.go.th/page/faq.html", "LTR"],
  // Board of Investment — SMART visa
  ["smart-visa.boi.go.th", "Board of Investment (SMART Visa)", "https://smart-visa.boi.go.th/smart/index.html", "SMART"],
  ["smart-visa.boi.go.th", "Board of Investment (SMART Visa)", "https://smart-visa.boi.go.th/smart/pages/required-document.html", "SMART"],
  ["smart-visa.boi.go.th", "Board of Investment (SMART Visa)", "https://smart-visa.boi.go.th/smart/pages/how-to-manage.html", "SMART"],
  // Thailand Privilege Card Co. (Tourism Authority of Thailand)
  ["thailandprivilege.co.th", "Thailand Privilege (a TAT company)", "https://www.thailandprivilege.co.th/en", "Privilege"],
  ...["bronze", "gold", "platinum", "diamond", "reserve"].map((t) => ["thailandprivilege.co.th", "Thailand Privilege (a TAT company)", `https://www.thailandprivilege.co.th/membership/${t}`, "Privilege"]),
  ["thailandprivilege.co.th", "Thailand Privilege (a TAT company)", "https://www.thailandprivilege.co.th/living-in-thailand", "general"],
  ["thailandprivilege.co.th", "Thailand Privilege (a TAT company)", "https://www.thailandprivilege.co.th/living-in-thailand?title=stay", "general"],
  ["thailandprivilege.co.th", "Thailand Privilege (a TAT company)", "https://www.thailandprivilege.co.th/living-in-thailand?title=overstay", "general"],
  // Tourism Authority of Thailand
  ["tourismthailand.org", "Tourism Authority of Thailand", "https://www.tourismthailand.org/Articles/plan-your-trip-passportandvisa", "exemption"],
];

/** The route a passage is mainly about: the one it mentions first. */
function topic(text, fallback) {
  const hits = [
    [/visa exemption|\bVOA\b|on arrival/i, "exemption"],
    [/\bDTV\b|Destination Thailand/, "DTV"],
    [/Non-Immigrant ED|ED Plus/, "Non-ED"],
  ]
    .map(([re, tag]) => [text.search(re), tag])
    .filter(([i]) => i >= 0)
    .sort((a, b) => a[0] - b[0]);
  return hits[0]?.[1] ?? fallback;
}

const JUNK = /cookie policy|account number|rate this content|world's premier residency/i;

/** Split page text into ~1,100-character passages on line boundaries. */
function chunks(text, max = 1100) {
  const out = [];
  let cur = "";
  for (const line of text.split("\n")) {
    if (cur && cur.length + line.length > max) {
      out.push(cur);
      cur = "";
    }
    cur += (cur ? "\n" : "") + line;
  }
  if (cur.length > 80) out.push(cur);
  return out;
}

async function pages() {
  const require = createRequire(join(process.cwd(), "noop.js"));
  const { chromium } = require("playwright-core");
  const browser = await chromium.launch({ executablePath: process.env.CHROME });
  const ctx = await browser.newContext({ userAgent: UA, locale: "en-GB" });
  const texts = [];
  for (const [site, authority, url, visa] of PAGES) {
    const p = await ctx.newPage();
    try {
      const r = await p.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
      await p.waitForTimeout(6000);
      if (!r || r.status() >= 400) throw new Error(`HTTP ${r?.status()}`);
      // The element holding most of the page's text, minus navigation.
      const page = await p.evaluate(() => {
        document.querySelectorAll("nav, header, footer, script, style, noscript, [role=navigation], .navbar, .menu, .footer").forEach((e) => e.remove());
        // textContent (not innerText) so collapsed tabs and FAQ answers are kept; mark block ends as line breaks.
        document.querySelectorAll("p, div, li, h1, h2, h3, h4, h5, h6, tr, dd, dt, section, br, button, summary").forEach((e) => e.after("\n"));
        const len = (e) => (e.textContent ?? "").trim().length;
        const cands = [...document.querySelectorAll("main, article, [role=main], #content, .content")].sort((a, b) => len(b) - len(a));
        const best = cands[0] && len(cands[0]) > 400 ? cands[0] : document.body;
        const text = best.textContent ?? "";
        return { title: document.title.trim(), text };
      });
      texts.push({ site, authority, url, visa, ...page });
      console.error(`ok   ${url} (${page.text.length} chars)`);
    } catch (e) {
      console.error(`skip ${url}: ${e.message.split("\n")[0]}`);
    }
    await p.close();
    await new Promise((res) => setTimeout(res, 2000));
  }
  await browser.close();

  // Lines repeated on most pages of a site are menus and footers.
  const seenText = new Set();
  const bySite = new Map();
  for (const t of texts) bySite.set(t.site, [...(bySite.get(t.site) ?? []), t]);
  for (const [, list] of bySite) {
    const count = new Map();
    for (const t of list) for (const l of new Set(clean(t.text).split("\n"))) count.set(l, (count.get(l) ?? 0) + 1);
    for (const t of list) {
      const lines = clean(t.text)
        .split("\n")
        .filter((l) => l.length > 2 && !(list.length > 2 && count.get(l) > list.length / 2 && l.length < 200));
      chunks(lines.join("\n")).forEach((c, i) => {
        if (c.length < 150 || seenText.has(c) || JUNK.test(c)) return; // menus-only pages, the same tab under two URLs, cookie and payment boilerplate
        seenText.add(c);
        const dated = t.text.match(/\b\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{4}\b/)?.[0];
        add(`gov-${slug(t.site)}-${slug(new URL(t.url).pathname + new URL(t.url).search) || "home"}-${i + 1}`, `${t.title}${dated ? ` (page dated ${dated})` : ""}\n${c}`, {
          site: t.site,
          authority: t.authority,
          title: t.title.slice(0, 160),
          url: t.url,
          // TAT's one passport-and-visa page covers several routes; tag each passage by what it is about.
          visa: t.site === "tourismthailand.org" ? topic(c, t.visa) : t.visa,
          ...(dated ? { page_date: dated } : {}),
        });
      });
    }
  }
}

await evisa();
console.error(`e-Visa: ${docs.length} purpose passages`);
await pages();
const version = createHash("sha256").update(JSON.stringify(docs.map((d) => [d.id, d.text, d.metadata.visa, d.metadata.url]))).digest("hex").slice(0, 12);
writeFileSync(OUT, JSON.stringify({ fetched: today, version, sources: [...new Set(docs.map((d) => d.metadata.site))], docs }, null, 1) + "\n");
console.error(`wrote ${docs.length} passages (version ${version}) to ${OUT}`);

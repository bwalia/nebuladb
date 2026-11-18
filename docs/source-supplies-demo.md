# Source Supplies AI showcase

RAG apps and one agent team built for one business: Source Supplies Ltd, a UK B2B supplier of
sustainable commercial cleaning and hygiene products, cleaning machines and washroom services.
Four extra apps map to pain points from the Source Supplies discovery call; the top 3 (Retail POD
Autopilot, Stock Autopilot, Jordan Report Studio) are full automations with narrated video tours, and
Email Order Intake covers emailed orders → Merlin. The apps live in the showcase under **Source Supplies** in the
sidebar (`apps/showcase/src/source/`). The agents live in `examples/nebula-agents` (`--team source`).

> **Demo data only.** Every product, SKU, price, stock level, customer, site, order, ticket, tender
> answer and safety data sheet is invented. The safety data sheets are for fictional products, carry
> a "DEMO SDS" banner on every section and contain no real hazard codes, exposure limits or
> transport numbers. Real partner brands appear only as a `brand_family` label on paper and
> dispenser lines, and those lines carry no badges, swap claims or safety data. Prices are GBP
> excluding VAT. The model never places an order, sends an email or books a visit: it drafts, and
> a person confirms.

## Customer site: sourcesupplies.nebuladb.net

A separate, customer-facing site for Source Supplies — share this link with the customer; keep
`showcase.nebuladb.net` (everything combined) for our own testing. Same image and pods as the
showcase: `src/main.tsx` renders `src/source/site/SourceSite.tsx` instead of `<App/>` when the host
is `sourcesupplies.*` (`src/tenant.ts`; locally add `?site=sourcesupplies`, `?site=showcase` to go
back).

- Only the Source Supplies demos and Labs (pinned to the Source pack, picker hidden), grouped as
  Your top 3 · Orders & operations · Sales & compliance · Explore.
- Search in the header (every demo's name, problem, outcome and keywords) and a filter on the home
  page; a Video tours page; every demo page leads with the problem, what Jordan does, the win and a
  ▶ Watch button. Larger type for the embedded demo screens (scoped `.ss-site` CSS).
- Narrated tours for all 11 demos, recorded inside this shell, with posters:
  `public/tours/source-supplies/*.mp4` + `posters/*.jpg`; the catalogue is
  `src/source/site/catalog.ts`. Re-record: `apps/showcase/tours/record-source-top3.mjs`.
- Sign-in is the same as the showcase (Google + email/password); `login.html` brands itself per
  host. Prod sets `showcase.googleAuth.hostRedirect: true`, so oauth2-proxy uses the request host
  for its callback. **The Google OAuth client must list
  `https://sourcesupplies.nebuladb.net/oauth2/callback`** (email/password works without it).

Infra (no database restart): `values-prod.yaml` ingress host; `deploy-k3s.yml` showcase-only mode
adds missing customer hosts to the live ingress and swaps the OAuth arg in the same rolling update;
edge vhost `deploy/edge/wslproxy-server-sourcesupplies.json` + `.github/wslproxy/…/host:sourcesupplies.nebuladb.net.json`
and the shared prod rule; `register-edge-vhost.yml` ring `sourcesupplies` creates the wslproxy server
and the Cloudflare CNAME (→ `lon1.pop0.uk`).

To add another customer site: a host in `tenant.ts` + a shell, the ingress host, `CUSTOMER_HOSTS`
in `deploy-k3s.yml`, an edge server JSON + rule entry + ring, and its Google callback.

## Run it

```bash
./scripts/start.sh                       # NebulaDB + showcase (docker compose)
# or, without docker:
cargo run --release -p nebula-server     # :8080
cd apps/showcase && npm ci && npm run dev  # http://localhost:5173 → Source Supplies
```

Open **Source Supplies home** (or any Source Supplies tab). Each tab seeds the buckets it reads on first
open with the existing `ensureSeeded` helper (versioned, batched upserts keyed by id); opening it
again writes 0 documents, which the seed chip shows ("… demo docs ready (0 written)").

Tests for the data and the rules (no extra dependencies, Node's built-in runner):

```bash
cd apps/showcase && npm test
cd examples/nebula-agents && python3 -m unittest -v test_source_guards
```

With the default mock embedder (`mock-384`) semantic similarity is a hash, not meaning: the usual
"Mock embedder" banner shows on every Source Supplies tab. The hard filters (badges, sectors,
stock, price, SKU, account, site, month, section) still give the right rows, and the apps add a
deterministic "words in common" signal where a ranking would otherwise be noise. Point
`NEBULA_OPENAI_*` at `nomic-embed-text` (or any real embedder) for real semantic ranking.

## Buckets

| Bucket | Docs | One doc is… | Filtered on |
|---|---|---|---|
| `ss_products` | 123 | a product line | `sku`, `category`, `use_areas[]`, `sectors[]`, `badges[]`, `concentrate`, `in_stock`, `price_gbp`, `lead_days`, `replaces_sku`, `brand_family` |
| `ss_sds` | 160 | one section (1–16) of a demo SDS for 10 products | `sku`, `section`, `section_title`, `hazard_class`, `ppe[]`, `signal_word` |
| `ss_guides` | 41 | a how-to / problem-solving chunk | `problem`, `surface`, `sector`, `skus[]` |
| `ss_accounts` | 23 | a customer site (6 accounts) | `account_id`, `site`, `sector`, `postcode_area`, `delivery_day`, `washroom_cycle_days` |
| `ss_orders` | 828 | an order line (May–Oct 2026) | `account_id`, `site`, `sku`, `qty`, `month`, `po_ref` |
| `ss_washroom_jobs` | 82 | a planned visit or a resolved ticket | `kind`, `account_id`, `site`, `service`, `status`, `visit_date`, `priority` |
| `ss_tender_kb` | 60 | a capability or past tender answer | `kind`, `topic`, `cert`, `sector`, `won` |
| `ss_order_drafts` | — | a draft basket (apps and agents write here) | `account_id`, `site`, `status = 'draft'`, `created_by` |
| `ss_retail_inbox` | 72 | a portal order, Merlin delivery ticket or courier POD email | `kind`, `po`, `consignment`, `store_no`, `postcode`, `despatched`, `status` |
| `ss_retail_rules` | — | a learned correction (app writes) | `kind` (`address_alias` / `item_alias`), `store_no`, `postcode`, `code`, `sku` |
| `ss_demand` | 413 | a SKU-month of despatches, or a stock position | `kind`, `sku`, `month`, `packs`, `core`, `top_order_share`, `supplier`, `lead_days`, `moq` |
| `ss_report_schedules` | — | a scheduled customer report (app writes) | `account_id`, `cadence`, `next_run` |

Data files: `apps/showcase/src/source/data/*.ts`, each exporting its docs and `SEED_VERSION`.
`consistency.test.ts` checks that every SKU in orders, guides, SDS and swaps exists, that every
order and job uses a real account and site, and the brand-label rules above.

Demo accounts: Brightway FM (4 office sites), Kingsmead Academy Trust (4 sites, including
"St Mary's Primary", which exercises `''` escaping), Harbour Care Homes, Meridian Student Living,
Castlegate Leisure and Ashby Contract Cleaning. The demo calendar is fixed: today is 31 Oct 2026.

### One SQL rule worth knowing

NebulaDB applies metadata filters after retrieval, over `LIMIT × 4` candidates (at least 32). An
exhaustive filtered lookup ("every order line for this site") therefore uses a `LIMIT` large enough
that `LIMIT × 4` covers the bucket (`exhaustiveLimit()` in `data/common.ts`): 39 for products, 215
for orders, 48 for SDS sections. Every literal goes through `lit()`, so `site = 'St Mary''s Primary'`
is safe. One `semantic_match(text, …)` per query; no `OR`, subqueries or CTEs.

## The apps

### A. Green Product Finder (`ssfinder`)

A buyer says what they need; one query mixes meaning with hard filters, the browser ranks the hits
on fit / eco badges / cost per use / lead time with weight sliders, near misses say which filter
they failed, and greener swaps are priced at the account's real order volume.

```sql
SELECT * FROM ss_products
 WHERE semantic_match(text, 'low odour floor cleaner for a school canteen')
   AND array_contains(badges, 'nordic-swan')
   AND array_contains(sectors, 'education')
   AND in_stock = TRUE
   AND price_gbp <= 30
 LIMIT 39

-- near misses: same meaning, no filters
SELECT * FROM ss_products WHERE semantic_match(text, '…') LIMIT 40
-- swaps: what the hits replace, then the account's volume of those SKUs
SELECT * FROM ss_products WHERE semantic_match(text, '…') AND sku IN ('FC-010') LIMIT 39
SELECT * FROM ss_orders WHERE semantic_match(text, 'order lines')
   AND account_id = 'KINGSMEAD' AND sku IN ('FC-010') AND month >= '2026-05' LIMIT 215
```

Chat: answers only from the shortlist, always gives dilution and pack, never claims a badge the
product's complete badge list lacks, and suggests the closest near miss when nothing fits.

### B. COSHH & Safety Data Assistant (`sssafety`)

Questions about a demo SDS go through hard rules in code (`source/safety.ts`, tested in
`safety.test.ts`) before any model:

- Emergency words (swallowed, in eyes, burn, can't breathe…) → a fixed first-aid card from Section 4
  and "Call 999 in an emergency, or NHS 111 for advice". No model text.
- Mixing two products → always "Do not mix products unless the SDS for both says it is safe" plus
  the cited Section 10. No model text.
- Not covered by the retrieved sections → "This is not in the safety data sheet." and stop.

```sql
SELECT * FROM ss_sds WHERE semantic_match(text, 'Which PPE do cleaners need for DS-200?')
   AND sku = 'DS-200' AND section IN (8) LIMIT 48
SELECT * FROM ss_sds WHERE semantic_match(text, 'stability and reactivity, do not mix')
   AND sku IN ('DS-200', 'MR-160') AND section = 10 LIMIT 48
SELECT * FROM ss_guides WHERE semantic_match(text, '…') AND array_contains(skus, 'DS-200') LIMIT 19
```

"Draft COSHH assessment" fills substance, hazards, PPE, storage, first aid, spills and disposal from
the cited sections, marked "Draft: a competent person must check this".

### C. Cleaning Problem Solver (`sssolver`)

```sql
SELECT * FROM ss_guides WHERE semantic_match(text, 'black mould on student bedroom walls')
   AND surface = 'painted walls' AND sector = 'higher-education' LIMIT 19
SELECT * FROM ss_products WHERE semantic_match(text, '…')
   AND sku IN ('MR-165', 'MR-160', 'GV-810', …) AND in_stock = TRUE LIMIT 39
SELECT * FROM ss_sds WHERE semantic_match(text, 'PPE, storage, first aid, do not mix')
   AND sku IN (…) AND section IN (4, 7, 8, 10) LIMIT 48
```

Output: the method from the cited guide, the kit list with stock status, "Add all to Quick Order"
(a `status = 'draft'` doc in `ss_order_drafts`), and a chat limited to the cited guide and SDS.

### D. Smart Reorder (`ssreorder`)

"Same as last month for Kingsmead North site, but switch to recycled hand towels."

```sql
SELECT * FROM ss_accounts WHERE semantic_match(text, 'site delivery day')
   AND account_id = 'KINGSMEAD' AND site = 'North site' LIMIT 14
SELECT * FROM ss_orders WHERE semantic_match(text, 'order lines')
   AND account_id = 'KINGSMEAD' AND site = 'North site' AND month >= '2026-05' LIMIT 215
SELECT * FROM ss_products WHERE semantic_match(text, 'basket products') AND sku IN (…) LIMIT 39
SELECT * FROM ss_products WHERE semantic_match(text, 'recycled hand towels')
   AND category = 'paper' AND in_stock = TRUE LIMIT 39
```

The forecast is plain code (`source/reorder.ts`): average of the last 3 months rounded up to whole
packs, spikes over 50% flagged. Swaps keep the same amount of use; lines that cannot make the
site's next delivery day move to the following one. "Save draft" writes to `ss_order_drafts`. The
chat ("why did toilet roll go up?") is grounded on the order history and forecast numbers.

### E. Source Washroom Service Desk (`sswashroom`)

```sql
SELECT * FROM ss_washroom_jobs WHERE semantic_match(text, 'Sanitary bin in Block C not changed…')
   AND kind = 'ticket' LIMIT 24
SELECT * FROM ss_accounts WHERE semantic_match(text, 'washroom contract')
   AND account_id = 'KINGSMEAD' AND site = 'South site' LIMIT 14
SELECT * FROM ss_washroom_jobs WHERE semantic_match(text, 'planned sanitary visit')
   AND kind = 'visit' AND account_id = 'KINGSMEAD' AND site = 'South site' LIMIT 39
SELECT * FROM ss_guides WHERE semantic_match(text, '…') AND problem IN ('odour', 'washroom') LIMIT 19
```

A similarity-weighted vote (plus keyword rules) suggests service and priority; a site is flagged
for a missed visit when its last completed visit is older than the contract cycle. "Resolve & learn"
writes the outcome back to `ss_washroom_jobs`. The quote helper turns "3 floors, 40 staff, 6 female
toilets, 2 baby change rooms" into a draft unit list and visit frequency from the washroom rules in
`ss_tender_kb` (`topic = 'washroom'`) and past jobs, marked "Draft quote: a person confirms numbers
and price".

### F. Tender and Questionnaire Responder (`sstender`)

```sql
SELECT * FROM ss_tender_kb WHERE semantic_match(text, 'How will you help us reduce carbon?')
   AND sector IN ('education', 'all') [AND won = TRUE] LIMIT 23
```

One query per buyer question. Coverage = topic match + words in common; below 50% the question is
marked "needs a human" and not drafted by default. Drafts cite the knowledge-base entries, are
trimmed in code to the word limit and export as Markdown. Topics with strong demo answers:
sustainability and carbon (EV deliveries inside the M25, solar-powered site, B Corp, ISO 14001),
health and safety (ISO 45001, COSHH support and videos), quality (ISO 9001), delivery and service
levels, social value, and product training. Invented numbers are marked "(demo figure)".

### Top 3 from the discovery call

Dave's own ranking near the end of the call: the national-retailer portal + PODs, stock, then
customer reports. Each one is a full app with a tested engine (pure TypeScript, `node --test`), real
NebulaDB queries shown step by step, a "today vs with Jordan" benefit strip, and a narrated video tour
(`public/tours/source-supplies/*.mp4`, embedded on the Source Supplies home page).

| # | App | Engine | Tests | Video |
|---|---|---|---|---|
| 1 | Retail POD Autopilot (`ssretail`) | `source/retailPilot.ts` | `retailPilot.test.ts` | `retail-pod-autopilot.mp4` |
| 2 | Stock Autopilot (`ssstock`) | `source/stockPilot.ts` | `stockPilot.test.ts` | `stock-autopilot.mp4` |
| 3 | Jordan Report Studio (`ssreports`) | `source/reportStudio.ts` | `reportStudio.test.ts` | `report-studio.mp4` |

All three in one file: `source-supplies-top3.mp4`. Re-record with `apps/showcase/tours/record-source-top3.mjs`
(Playwright + macOS `say` en-GB voice + ffmpeg; instructions in the file header).

### G. Retail POD Autopilot (`ssretail`) — top-3 #1

Discovery call: ~30 small orders a day (~600 a month) from a national retailer's own facilities portal,
keyed into Merlin by hand; then for every delivery the Merlin scan POD *and* the courier photo POD are
found and uploaded back to the portal. "That whole process… it's probably costing 20 grand a year."
Demo retailer "ValueMax Stores" and courier "ParcelLine" are fictional.

Buckets: `ss_retail_inbox` (one demo Friday: 13 portal orders, 30 Merlin delivery tickets, 29 courier
POD emails — `kind` = `portal_order` / `merlin_despatch` / `courier_pod`) and `ss_retail_rules`
(corrections people make; created on first lesson).

```sql
SELECT * FROM ss_retail_inbox WHERE semantic_match(text, 'retail portal work order')
   AND kind = 'portal_order' LIMIT 72
SELECT * FROM ss_retail_inbox WHERE semantic_match(text, 'Merlin delivery ticket scanned POD')
   AND kind = 'merlin_despatch' AND despatched = '2026-10-28' LIMIT 72
SELECT * FROM ss_products WHERE semantic_match(text, 'Blue roll centrefeed 2ply x6')
   AND category = 'paper' LIMIT 39          -- unknown retailer item code → catalogue
```

1. **Intake**: retailer item codes → Source SKUs (known map, then learned aliases, then catalogue
   match ≥ 0.6 marked "confirm once"); duplicate PO re-sends are dropped.
2. **Two PODs per delivery**: match courier POD by PO → consignment → fuzzy (0.55·consignment edit
   distance incl. transpositions + 0.30·postcode incl. learned aliases + 0.15·date, ≥ 0.70). Checks:
   delivered, signed, postcode, owed lines. Outcome: attached, attached + follow-up, exception with a
   one-line action, or waiting.
3. **Learning**: "Same store — attach & remember" / "Confirm & remember" upsert a rule into
   `ss_retail_rules`; the next run uses it (the Leeds address exception disappears).

Demo day result: 26/30 straight-through (87%), 3 exceptions, 1 waiting; ≈ £21.9k a year at 14 manual
minutes per order vs 0.5 min spot-check / 4 min per exception, £14/h, 600 orders a month (demo figures
tuned to Dave's estimate).

### H. Stock Autopilot (`ssstock`) — top-3 #2

Discovery call: no live stock in Merlin, popular lines (~95% repeat) still run out, and Dave was
pasting three months of averages into ChatGPT to draft POs — "it's cleverer than just looking at an
average of 3 months… if Christmas is a busier period or quieter period".

Bucket `ss_demand`: 24 months (Nov 2024 – Oct 2026) of company-wide despatches for 16 core lines
(with `top_order_share`, the share from the single largest order), 3 "yes company" special lines, and
an estimated stock position per core line (last count + goods in − despatches), supplier, lead time, MOQ.

```sql
SELECT * FROM ss_demand WHERE semantic_match(text, 'monthly despatches core line')
   AND kind = 'demand' AND core = TRUE LIMIT 413
SELECT * FROM ss_demand WHERE semantic_match(text, 'estimated stock on hand supplier lead time')
   AND kind = 'stock' LIMIT 413
```

Engine (`stockPilot.ts`): one-off cleaning (> 1.4× expected and > 35% from one order), new-contract
step-ups from year-on-year ratios, seasonal index from a 12-month window, YoY growth, safety stock
= 1.65 · σ · forecast · √((lead + 30)/30) with σ ≥ 6%, a daily walk-forward to the stock-out date,
MOQ-rounded order-up-to, one draft PO per supplier. The 12-month backtest replays both methods:
stock-out months 43 → 17, short packs 2,598 → 1,027, average stock held £9.3k → £6.2k.

> `LIMIT` caps rows returned as well as the candidate pool, so these queries use
> `max(exhaustiveLimit(n), n)` — every one of the 384 history rows must come back.

### I. Jordan Report Studio (`ssreports`) — top-3 #3

Discovery call: Merlin custom reports need SQL or a vendor ticket ("third attempt… three or four days
apart"); "can you just send me the report for [customer] for the last 3 months combined".

Plain English → typed plan shown as chips (customer, period, grouping, category, top N, measure,
comparison period) → one query → figures computed in code → bars, insights (top share, change vs
previous period, biggest mover, site × SKU spikes ≥ 50%, greener swaps with CO2e), Excel CSV, customer
email draft, and a monthly schedule written to `ss_report_schedules` (last Friday). Follow-ups
("now just paper, by month", "top 5 products") refine the previous plan.

```sql
SELECT * FROM ss_orders WHERE semantic_match(text, 'order lines Ashby Contract Cleaning')
   AND account_id = 'ASHBY'
   AND month IN ('2026-05', '2026-06', '2026-07', '2026-08', '2026-09', '2026-10') LIMIT 215
```

### J. Email Order Intake (`ssemail`)

Discovery-call theme: ~90% of orders hit the website; the rest arrive by email and are re-keyed.
Paste an email → parse qty lines → `semantic_match` on `ss_products` → draft Merlin sales order.
Weak matches stay with a human; nothing writes to Merlin until confirmed.

## Agent team

`examples/nebula-agents/source_team.py`, run with `--team source`. Each agent gets only its tools,
and bucket limits are enforced in code by wrapping the MCP tools (`source_guards.py`, tested in
`test_source_guards.py`): the safety agent can only search `ss_sds` / `ss_guides`, the order agent
can only insert into `ss_order_drafts` with `status = 'draft'`, every SQL call must be a single
read-only `SELECT` over `ss_*` buckets, and the account assistant writes only to `agent_memory`.

| Agent | Key | Tools |
|---|---|---|
| Product Advisor | `product` | `execute_sql`, `semantic_search`, `get_document` |
| Safety Agent | `safety` | `semantic_search` (`ss_sds`, `ss_guides` only), `get_document` |
| Order Agent | `order` | `execute_sql`, `semantic_search`, `insert_document` (`ss_order_drafts`, `status = draft` only) |
| Washroom Agent | `washroom` | `execute_sql`, `semantic_search`, `get_document` |
| Sustainability Agent | `sustainability` | `execute_sql`, `get_document` |
| Account Assistant | `assistant` | `recall`, `remember`, `semantic_search`, `answer_question` |
| Supervisor | `supervisor` | `delegate` only |

```bash
cd examples/nebula-agents
python source_seed.py      # seeds the ss_* buckets (same ids and versions as the showcase)
python run.py --team source supervisor "Brightway FM wants to cut CO2 by 20% at their 4 office sites without spending more. What can we do this month?"
python run.py --team source order "Draft next month's order for Kingsmead Academy, North site."
python run.py --team source safety "Which PPE do cleaners need for the demo descaler DS-200?"
python run.py --team source washroom "Which sites missed a sanitary visit in the last 30 days?"
python run.py --team source assistant --user BRIGHTWAY "We are moving to 3 deliveries a week from January."
```

The showcase **AI agents** tab has a "Source Supplies" preset that replays the same story with
scripted plans and real tool calls in the browser.

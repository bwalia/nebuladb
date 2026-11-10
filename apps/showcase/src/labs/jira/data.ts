/**
 * Demo Jira data for the Jira Triage Agent — three fictional companies.
 *
 * Each ticket carries its Priority Score changelog (the custom 1–100 field,
 * not Jira's built-in Priority), the way Jira's `expand=changelog` returns it:
 * when the score changed, who changed it and the comment they left. A `null`
 * score means the ticket was resolved and left the ranking.
 *
 * The snapshots the agent stores in NebulaDB (one per project per run) are
 * computed from these changelogs, as if the agent had run every Monday at
 * 07:00 for eight weeks and every weekday morning for the last week.
 */

export interface ScoreChange {
  /** ISO timestamp, Europe/London local time. */
  at: string;
  /** Priority Score 1–100, or null when the ticket was resolved. */
  score: number | null;
  by: string;
  note: string;
}

export interface Ticket {
  key: string;
  project: string;
  type: "Bug" | "Story" | "Task" | "Incident";
  summary: string;
  description: string;
  component: string;
  team: string;
  assignee: string;
  /** Jira's built-in Priority. Shown for contrast; the agent ranks by Priority Score only. */
  jiraPriority: "Highest" | "High" | "Medium" | "Low";
  status: "To Do" | "In Progress" | "In Review" | "Done";
  history: ScoreChange[];
}

export interface Project {
  key: string;
  name: string;
  company: string;
  business: string;
  manager: string;
  managerEmail: string;
  confluenceSpace: string;
  scoreField: string;
}

export const PROJECTS: Project[] = [
  {
    key: "TIX",
    name: "Fan Platform",
    company: "Encore Tickets",
    business: "live-events ticket marketplace",
    manager: "Dana Okafor (Head of Engineering, Fan Platform)",
    managerEmail: "dana.okafor@encore-tickets.example",
    confluenceSpace: "FANPLAT",
    scoreField: "customfield_10342",
  },
  {
    key: "PAY",
    name: "Payments & Digital Banking",
    company: "Harbour Bank",
    business: "UK retail bank",
    manager: "Sam Whitfield (Engineering Manager, Payments)",
    managerEmail: "sam.whitfield@harbourbank.example",
    confluenceSpace: "PAYENG",
    scoreField: "customfield_10342",
  },
  {
    key: "GRO",
    name: "Online Grocery",
    company: "Freshway",
    business: "supermarket online shop and delivery",
    manager: "Ravi Menon (Delivery Lead, Online Grocery)",
    managerEmail: "ravi.menon@freshway.example",
    confluenceSpace: "ONLINEGRO",
    scoreField: "customfield_10342",
  },
];

type Raw = [
  key: string,
  type: Ticket["type"],
  summary: string,
  component: string,
  team: string,
  assignee: string,
  jiraPriority: Ticket["jiraPriority"],
  status: Ticket["status"],
  description: string,
  history: [at: string, score: number | null, by: string, note: string][],
];

const RAW: Raw[] = [
  // ------------------------------------------------------------------ Encore Tickets — Fan Platform
  ["TIX-1201", "Bug", "Waiting room drops fans when the queue token refreshes after 10 minutes", "Queue & Waiting Room", "Squad Falcon", "Priya Nair", "High", "In Progress",
    "Fans who wait longer than 10 minutes in the virtual waiting room lose their place when the queue token refreshes and are sent to the back of the queue.",
    [["2026-10-27T10:12", 62, "Priya Nair", "Seen in two Tuesday presales, about 0.4% of the queue affected."],
     ["2026-11-05T16:40", 95, "Dana Okafor", "Stadium tour presale on 12 Nov expects 1.2M fans in the queue; yesterday's load test lost 3% of tokens. Top priority until fixed."]]],
  ["TIX-1170", "Task", "Load test the queue at 1.5M concurrent sessions", "Platform", "Squad Core", "Tomasz Kowal", "High", "In Progress",
    "Prove the waiting room, checkout and seat map hold 1.5M concurrent sessions before the stadium tour on-sale.",
    [["2026-10-06T09:00", 58, "Tomasz Kowal", "Needed before the stadium tour on-sale."],
     ["2026-10-27T11:30", 80, "Dana Okafor", "Promoter go/no-go meeting is 10 Nov and needs the load test report."],
     ["2026-11-07T18:05", 91, "Tomasz Kowal", "Run 1 failed at 1.1M sessions (queue token service). Run 2 booked for 9 Nov."]]],
  ["TIX-1174", "Task", "Bot rules for presale codes shared on Telegram", "Fraud & Bots", "Squad Sentinel", "Grace Mensah", "High", "In Progress",
    "Presale codes meant for one fan are being shared and resold in Telegram groups. Bind each code to one account and rate-limit code redemption.",
    [["2026-09-24T14:00", 48, "Grace Mensah", "Small numbers of shared codes seen."],
     ["2026-10-20T09:45", 66, "Grace Mensah", "Presale codes resold for £40 each on Telegram for the October arena tour."],
     ["2026-11-06T12:20", 89, "Dana Okafor", "The stadium tour promoter requires one code per fan before the 12 Nov presale."]]],
  ["TIX-1209", "Bug", "Basket price excludes the service fee until the last checkout step", "Checkout", "Squad Ledger", "Hannah Brooks", "High", "To Do",
    "The basket shows the face value only; the per-ticket service fee is added on the final payment page.",
    [["2026-11-04T15:10", 84, "Legal (Ama Boateng)", "Drip pricing: the full price including mandatory fees must show from the first price. Legal wants this fixed before the tour on-sale."]]],
  ["TIX-1188", "Story", "Fast seat-map rendering for 90,000-capacity stadiums", "Seat Map", "Squad Orbit", "Leo Martins", "Medium", "In Progress",
    "Render the interactive seat map for stadium-size venues in under 2 seconds on mid-range phones.",
    [["2026-09-15T10:00", 55, "Leo Martins", "Stadium venues coming next season."],
     ["2026-10-13T16:00", 70, "Dana Okafor", "Stadium tour confirmed for three 90,000-seat venues."],
     ["2026-11-04T09:30", 82, "Leo Martins", "Seat map takes 9 seconds on a mid-range Android phone in testing."]]],
  ["TIX-1199", "Bug", "Gate scanners reject rotating barcodes when venue Wi-Fi drops", "Venue Scanning", "Squad Gate", "Femi Adeyemi", "High", "In Progress",
    "Handheld gate scanners cannot validate rotating barcodes offline, so fans are held at the gate when venue Wi-Fi drops.",
    [["2026-10-12T22:15", 70, "Femi Adeyemi", "Arena show in Manchester: 900 fans held at the gates for 8 minutes."],
     ["2026-10-22T10:00", 52, "Femi Adeyemi", "Offline grace window raised to 90 seconds as a mitigation."],
     ["2026-11-08T11:00", 76, "Dana Okafor", "Stadium gates use the same scanner firmware; the root fix must ship before 15 Nov."]]],
  ["TIX-1205", "Story", "Accessible seating: book a companion seat in the same flow", "Checkout", "Squad Atlas", "Mei Chen", "Medium", "In Progress",
    "Wheelchair users must call the box office to add a companion seat. Add it to the online flow and meet WCAG 2.2 AA.",
    [["2026-10-21T13:00", 50, "Mei Chen", "Accessibility review finding."],
     ["2026-11-04T17:20", 63, "Customer Care (Rob Hale)", "Formal complaint received under the Equality Act; we promised a date."]]],
  ["TIX-1162", "Story", "Resale price cap at face value plus 10%", "Resale Marketplace", "Squad Atlas", "Mei Chen", "Medium", "To Do",
    "Promoters can require resale listings to be capped at face value plus 10%. Enforce the cap at listing time.",
    [["2026-09-14T09:00", 60, "Mei Chen", "Requested by two promoters."],
     ["2026-10-19T09:00", 58, "Mei Chen", "No change in demand."],
     ["2026-11-03T15:00", 74, "Dana Okafor", "Stadium tour contract makes the cap mandatory; secondary ticketing is under regulator attention."]]],
  ["TIX-1196", "Bug", "Data deletion requests do not remove active resale listings", "Privacy", "Squad Sentinel", "Grace Mensah", "Medium", "In Progress",
    "When a fan asks for their data to be deleted, their active resale listings stay visible with their seller name.",
    [["2026-10-09T11:00", 61, "Grace Mensah", "Found in a privacy audit."],
     ["2026-10-30T10:30", 69, "DPO (Nadia Farouk)", "Regulator response deadline is 28 Nov."]]],
  ["TIX-1216", "Incident", "Checkout p95 latency at 8 seconds during payment provider slowdown", "Checkout", "Squad Ledger", "Hannah Brooks", "Highest", "In Progress",
    "Sunday evening: card authorisations slow at the payment provider; checkout p95 rose from 1.4 s to 8 s and conversion fell.",
    [["2026-11-09T21:05", 77, "On-call (Hannah Brooks)", "Provider status page confirms a degraded EU region. Need a second provider route before the on-sale."]]],
  ["TIX-1190", "Bug", "Transferred tickets missing from the Apple Wallet pass", "Mobile Wallet", "Squad Orbit", "Leo Martins", "Medium", "In Progress",
    "After a ticket transfer, the receiving fan's Apple Wallet pass does not update with the new ticket.",
    [["2026-10-01T10:00", 44, "Leo Martins", "Reported by support, about 30 cases a week."],
     ["2026-10-26T09:00", 51, "Leo Martins", "Cases rising with arena season."]]],
  ["TIX-1214", "Bug", "Mobile tickets do not appear in Apple Wallet after a friend transfers them", "Mobile Wallet", "Customer Support", "Unassigned", "Medium", "To Do",
    "Fans say tickets a friend sent them never show in Apple Wallet. 120 support contacts this week.",
    [["2026-11-07T14:30", 57, "Support (Kerry Walsh)", "Raised by support after 120 contacts this week."]]],
  ["TIX-1121", "Task", "Upgrade the Kafka cluster before vendor end of support", "Platform", "Squad Core", "Tomasz Kowal", "Low", "To Do",
    "The event streaming cluster runs a version leaving vendor support. Upgrade and test consumers.",
    [["2026-09-01T09:00", 35, "Tomasz Kowal", "Planned maintenance."],
     ["2026-10-14T09:00", 30, "Tomasz Kowal", "Vendor extended support into next year."],
     ["2026-11-02T09:00", 45, "Tomasz Kowal", "Vendor end of support moved forward to 31 Dec."]]],
  ["TIX-1140", "Task", "Automate refunds for postponed shows", "Refunds", "Squad Ledger", "Hannah Brooks", "Medium", "To Do",
    "Refunds for postponed shows are processed by hand from a spreadsheet.",
    [["2026-09-08T09:00", 66, "Hannah Brooks", "Four shows postponed; 18,000 refunds queued."],
     ["2026-10-05T09:00", 54, "Hannah Brooks", "Finance cleared half by hand."],
     ["2026-10-26T09:00", 38, "Hannah Brooks", "Backlog cleared manually."]]],
  ["TIX-1155", "Story", "Waitlist: tell fans when resale tickets appear", "Resale Marketplace", "Squad Atlas", "Mei Chen", "Low", "To Do",
    "Fans can join a waitlist for a sold-out show and get a push notification when a resale ticket is listed.",
    [["2026-09-16T09:00", 42, "Mei Chen", "Product idea from the fan survey."],
     ["2026-10-28T09:00", 28, "Dana Okafor", "Deferred to Q1."]]],
  ["TIX-1132", "Story", "Dark mode for the fan app", "Mobile App", "Squad Orbit", "Leo Martins", "Low", "To Do",
    "Add a dark theme to the iOS and Android fan apps.",
    [["2026-09-02T09:00", 22, "Leo Martins", "Popular request in app reviews."],
     ["2026-10-12T09:00", 18, "Leo Martins", "Behind stadium work."]]],
  ["TIX-1150", "Bug", "Cards double-charged when the 3-D Secure challenge times out", "Checkout", "Squad Ledger", "Hannah Brooks", "Highest", "Done",
    "If the bank's 3-D Secure challenge times out, a retry creates a second charge.",
    [["2026-09-10T09:00", 88, "Hannah Brooks", "34 complaints last weekend."],
     ["2026-10-08T09:00", 72, "Hannah Brooks", "Hotfix reduced it to 3 cases a week; root fix pending."],
     ["2026-10-29T09:00", 40, "Hannah Brooks", "Root fix in release 4.18; monitoring."],
     ["2026-11-06T09:00", null, "Hannah Brooks", "Resolved: no repeat for 7 days."]]],
  ["TIX-1180", "Bug", "Search ranks past events above upcoming ones", "Discovery", "Squad Orbit", "Leo Martins", "Low", "Done",
    "Searching an artist shows last year's shows first.",
    [["2026-09-29T09:00", 33, "Leo Martins", "Annoying but has a workaround (date filter)."],
     ["2026-11-02T09:00", null, "Leo Martins", "Resolved in release 4.17."]]],

  // ------------------------------------------------------------------ Harbour Bank — Payments & Digital Banking
  ["PAY-2231", "Story", "APP scam reimbursement: calculate the 50/50 split with the receiving bank", "Fraud Operations", "Squad Shield", "Aisha Rahman", "Medium", "In Progress",
    "Authorised push payment scam claims are reimbursed by hand. Calculate the split with the receiving bank and start the claim automatically.",
    [["2026-09-15T09:00", 46, "Aisha Rahman", "Claims team asked for automation."],
     ["2026-10-06T09:00", 58, "Aisha Rahman", "Claims backlog at 1,900."],
     ["2026-10-27T10:00", 71, "Sam Whitfield", "Regulator data return due 30 Nov needs accurate split figures."],
     ["2026-11-05T15:45", 88, "Sam Whitfield", "Backlog now 3,400 claims and the first 35-day deadline breaches have started."]]],
  ["PAY-2290", "Bug", "Card freeze toggle shows frozen but the card still authorises", "Cards", "Squad Tap", "Owen Price", "Highest", "In Progress",
    "In the mobile app, freezing a card updates the screen but the freeze is not sent to the card processor when the app is on a slow connection.",
    [["2026-11-09T18:20", 90, "Sam Whitfield", "Found by internal QA; two confirmed fraud losses on cards customers believed were frozen."]]],
  ["PAY-2281", "Incident", "Contactless transit taps declined during the morning peak", "Cards", "Squad Tap", "Owen Price", "Highest", "In Review",
    "Contactless card taps at transit gates were declined at about 3% between 07:40 and 09:10.",
    [["2026-11-05T08:30", 94, "On-call (Owen Price)", "Started 07:40; the transit operator is reporting declines at gates."],
     ["2026-11-06T11:00", 68, "Owen Price", "Mitigated: authorisation timeout raised to 1.2 s. Root-cause analysis still open."]]],
  ["PAY-2289", "Bug", "Biometric login fails on new Android devices after the OS update", "Mobile App", "Squad Pixel", "Lucy Zhang", "High", "In Progress",
    "Fingerprint and face login fail on phones that took this month's Android update; customers fall back to passcode.",
    [["2026-11-07T09:15", 82, "Lucy Zhang", "6% of Android logins failing since the OS update."]]],
  ["PAY-2270", "Story", "CHAPS enhanced data: structured remittance and LEI fields", "Payments Platform", "Squad Rail", "Marek Novak", "Medium", "In Progress",
    "Send structured remittance information and Legal Entity Identifiers on CHAPS payments in the new message format.",
    [["2026-10-01T09:00", 63, "Marek Novak", "Scheme change on the roadmap."],
     ["2026-11-04T14:00", 79, "Sam Whitfield", "Scheme readiness test slot booked for 12 Jan; build must finish by 15 Dec."]]],
  ["PAY-2262", "Bug", "Open Banking consent renewal loops on Android", "Open Banking API", "Squad Bridge", "Chidi Okeke", "High", "In Progress",
    "Customers renewing a third-party provider's access are sent back to the start of the consent journey on Android.",
    [["2026-10-14T09:00", 57, "Chidi Okeke", "Reported by two third-party providers."],
     ["2026-11-02T10:00", 73, "Chidi Okeke", "Third-party providers escalated 140 failed renewals."]]],
  ["PAY-2239", "Task", "PCI DSS evidence: quarterly authenticated vulnerability scans", "Security", "Squad Shield", "Aisha Rahman", "Medium", "To Do",
    "Collect authenticated scan evidence for all card data environment hosts for the annual assessment.",
    [["2026-09-20T09:00", 44, "Aisha Rahman", "Annual assessment prep."],
     ["2026-11-02T09:30", 66, "Security (Ben Carter)", "Assessor visit booked for 1 Dec."]]],
  ["PAY-2205", "Story", "Consumer Duty: one vulnerable-customer flag across app and branch", "Customer Platform", "Squad Care", "Fiona Grant", "Medium", "In Progress",
    "A customer's vulnerability flag set in a branch is not visible in the app or contact centre, so they get the wrong treatment.",
    [["2026-09-10T09:00", 55, "Fiona Grant", "Consumer Duty gap analysis."],
     ["2026-10-13T09:00", 55, "Fiona Grant", "No change."],
     ["2026-11-02T11:00", 61, "Sam Whitfield", "Board report draft due 15 Dec."]]],
  ["PAY-2244", "Task", "Confirmation of Payee: support building society roll numbers", "Payments Platform", "Squad Rail", "Marek Novak", "Medium", "To Do",
    "Name checks fail for payments to building society accounts that use a roll number as a secondary reference.",
    [["2026-09-22T09:00", 52, "Marek Novak", "Scheme expansion requirement."],
     ["2026-10-20T09:00", 60, "Marek Novak", "Complaints from customers paying mortgages."]]],
  ["PAY-2250", "Task", "DORA register of ICT third-party providers for the EU branch", "Risk & Resilience", "Squad Shield", "Aisha Rahman", "Medium", "To Do",
    "Build and keep current the register of ICT third-party service providers for the EU branch.",
    [["2026-10-02T09:00", 49, "Aisha Rahman", "EU branch compliance."],
     ["2026-10-30T09:00", 57, "Risk (Elena Rossi)", "Supervisor asked for the register in the next review."]]],
  ["PAY-2218", "Bug", "Statements show the old interest rate after a base rate change", "Statements", "Squad Care", "Fiona Grant", "High", "In Progress",
    "Savings statements generated after a base rate change print the previous interest rate.",
    [["2026-09-25T09:00", 69, "Fiona Grant", "41,000 statements affected after the base rate cut."],
     ["2026-10-09T09:00", 81, "Sam Whitfield", "Complaints rising; remediation letters needed."],
     ["2026-10-23T09:00", 50, "Fiona Grant", "Corrected statements re-issued."],
     ["2026-11-06T09:00", 30, "Fiona Grant", "Only archive copies remain to fix."]]],
  ["PAY-2212", "Task", "Retire the legacy SOAP payments gateway", "Platform", "Squad Rail", "Marek Novak", "Low", "To Do",
    "Move the last three internal callers off the SOAP gateway and switch it off.",
    [["2026-09-12T09:00", 41, "Marek Novak", "Tech debt."],
     ["2026-10-19T09:00", 34, "Marek Novak", "Behind regulatory work."]]],
  ["PAY-2275", "Story", "Raise the cheque imaging limit to £5,000", "Payments Platform", "Squad Bridge", "Chidi Okeke", "Low", "To Do",
    "Customers can pay in cheques by photo up to £1,000; raise the limit.",
    [["2026-10-15T09:00", 36, "Chidi Okeke", "Business banking request."]]],
  ["PAY-2160", "Story", "Round-up savings pots", "Mobile App", "Squad Pixel", "Lucy Zhang", "Low", "To Do",
    "Round card payments up to the nearest pound and move the difference into a savings pot.",
    [["2026-09-01T09:00", 38, "Lucy Zhang", "Roadmap item."],
     ["2026-10-12T09:00", 31, "Lucy Zhang", "Behind Android work."],
     ["2026-11-03T09:00", 25, "Sam Whitfield", "Deprioritised to the Q1 roadmap."]]],
  ["PAY-2190", "Bug", "Faster Payments stuck in pending after the core banking batch window", "Payments Platform", "Squad Rail", "Marek Novak", "Highest", "Done",
    "Outbound Faster Payments made during the overnight batch window stay pending until morning.",
    [["2026-09-18T23:40", 92, "On-call (Marek Novak)", "11,000 payments delayed on Friday night."],
     ["2026-09-25T09:00", 75, "Marek Novak", "Batch window moved; root cause is in message replay."],
     ["2026-10-16T09:00", 45, "Marek Novak", "Replay fix in testing."],
     ["2026-11-03T09:00", null, "Marek Novak", "Resolved: replay idempotency fix deployed."]]],
  ["PAY-2227", "Bug", "Direct debit collected twice when the mandate is amended the same day", "Payments Platform", "Squad Rail", "Marek Novak", "High", "Done",
    "Amending a direct debit mandate on collection day creates a second collection.",
    [["2026-09-29T09:00", 77, "Marek Novak", "620 customers collected twice."],
     ["2026-10-10T09:00", 60, "Marek Novak", "Refunds issued; fix in testing."],
     ["2026-11-04T09:00", null, "Marek Novak", "Resolved."]]],

  // ------------------------------------------------------------------ Freshway — Online Grocery
  ["GRO-881", "Bug", "Christmas delivery slot booking times out when slots open", "Delivery Slots", "Squad Basket", "Jordan Ellis", "High", "In Progress",
    "Booking a Christmas delivery slot times out under load; the slot is held but the order is not created.",
    [["2026-11-06T19:30", 95, "Ravi Menon", "Christmas slots open to Rewards members on 16 Nov. The peak load test hit timeouts at 40% of expected traffic."]]],
  ["GRO-848", "Task", "Peak readiness: Christmas traffic load test", "Platform", "Squad Core", "Kofi Asante", "Medium", "In Progress",
    "Load test the shop, slot booking and checkout at expected Christmas peak.",
    [["2026-10-20T09:00", 50, "Kofi Asante", "Annual peak readiness."],
     ["2026-11-06T20:10", 80, "Ravi Menon", "Load test exposed slot booking timeouts (GRO-881). Retest after the fix."]]],
  ["GRO-790", "Bug", "Allergen information missing on 212 own-brand product pages", "Product Content", "Squad Pantry", "Sofia Marques", "High", "In Progress",
    "Allergen information is missing from 212 own-brand product pages after the content migration.",
    [["2026-10-05T09:00", 54, "Sofia Marques", "Found in a content audit."],
     ["2026-10-15T09:00", 57, "Sofia Marques", "Count confirmed at 212 pages."],
     ["2026-11-09T10:00", 86, "Ravi Menon", "A customer reported an allergic reaction. Legal: allergen information must be available before purchase online."]]],
  ["GRO-884", "Bug", "Christmas slots page says 'sold out' before slots open", "Delivery Slots", "Squad Basket", "Jordan Ellis", "Medium", "To Do",
    "In staging, the Christmas slots page shows every slot as sold out before booking opens.",
    [["2026-11-07T15:00", 62, "Jordan Ellis", "Seen in staging; same release as the slot booking change."]]],
  ["GRO-702", "Story", "Substitution preferences per product line", "Substitutions", "Squad Pantry", "Sofia Marques", "Medium", "In Progress",
    "Let customers say 'no substitute' or pick a preferred substitute for each item.",
    [["2026-09-08T09:00", 52, "Sofia Marques", "Top complaint theme in customer feedback."],
     ["2026-10-12T09:00", 58, "Ravi Menon", "Top customer complaint in the September survey."]]],
  ["GRO-859", "Bug", "Age-restricted items can be booked to collection lockers", "Click & Collect", "Squad Route", "Ethan Cole", "High", "To Do",
    "Orders with alcohol can choose a locker collection, where no age check happens.",
    [["2026-10-22T09:00", 58, "Ethan Cole", "Found in a store audit."],
     ["2026-11-02T09:00", 58, "Ethan Cole", "Workaround: lockers hidden for alcohol orders by a manual rule."]]],
  ["GRO-760", "Task", "Rewards price not applied to bundle offers", "Promotions", "Squad Offers", "Zara Hussain", "Medium", "In Progress",
    "Members' Rewards prices are not applied when the item is part of a bundle offer.",
    [["2026-09-24T09:00", 47, "Zara Hussain", "Customer complaints."],
     ["2026-10-29T09:00", 55, "Zara Hussain", "Trading standards query on 3 products."]]],
  ["GRO-823", "Story", "Rewards: personalised coupons in the app wallet", "Rewards", "Squad Offers", "Zara Hussain", "Medium", "To Do",
    "Show personalised coupons in the app wallet and apply them at checkout.",
    [["2026-10-12T09:00", 45, "Zara Hussain", "Marketing roadmap."],
     ["2026-11-04T09:00", 50, "Zara Hussain", "Wanted for the Christmas campaign."]]],
  ["GRO-745", "Bug", "Picker app crashes when scanning multi-buy barcodes", "Picking App", "Squad Aisle", "Nina Petrova", "Medium", "In Progress",
    "The in-store picking app crashes when a picker scans a multi-buy pack barcode.",
    [["2026-09-21T09:00", 49, "Nina Petrova", "Pickers in 14 stores affected."],
     ["2026-10-14T09:00", 41, "Nina Petrova", "Workaround: scan single items."]]],
  ["GRO-872", "Bug", "Duplicate charge when an order is edited after cut-off", "Checkout", "Squad Basket", "Jordan Ellis", "High", "In Progress",
    "Editing an order after the cut-off time takes a second payment.",
    [["2026-10-30T09:00", 53, "Jordan Ellis", "Found by customer care."],
     ["2026-11-04T09:00", 46, "Jordan Ellis", "Only 9 cases; refunds automated."]]],
  ["GRO-735", "Task", "Upgrade Postgres 13 to 16 before end of life", "Platform", "Squad Core", "Kofi Asante", "Low", "To Do",
    "The order database runs Postgres 13, which is out of support.",
    [["2026-09-15T09:00", 40, "Kofi Asante", "Planned upgrade."],
     ["2026-11-02T09:00", 43, "Kofi Asante", "Security team flagged the version."]]],
  ["GRO-801", "Task", "Move van routing to the new route optimiser", "Delivery", "Squad Route", "Ethan Cole", "Medium", "To Do",
    "Switch delivery van routing to the new optimiser to cut miles per drop.",
    [["2026-10-07T09:00", 44, "Ethan Cole", "Fuel savings."],
     ["2026-11-03T09:00", 38, "Ravi Menon", "After Christmas: no routing changes during peak."]]],
  ["GRO-834", "Bug", "Scan & Go cannot read loose produce weights", "Scan & Go", "Squad Aisle", "Nina Petrova", "Medium", "To Do",
    "The Scan & Go app fails to read weighed loose produce labels.",
    [["2026-10-16T09:00", 39, "Nina Petrova", "Pilot stores only."]]],
  ["GRO-777", "Story", "Reminder email for abandoned slot bookings", "CRM", "Squad Offers", "Zara Hussain", "Low", "To Do",
    "Email customers who reserved a delivery slot but did not check out.",
    [["2026-09-30T09:00", 30, "Zara Hussain", "Growth idea."],
     ["2026-10-20T09:00", 26, "Zara Hussain", "Lower than coupons."]]],
  ["GRO-866", "Story", "Show CO2 per delivery slot (green slots)", "Delivery Slots", "Squad Basket", "Jordan Ellis", "Low", "To Do",
    "Highlight delivery slots where a van is already nearby.",
    [["2026-10-27T09:00", 28, "Jordan Ellis", "Sustainability team idea."]]],
  ["GRO-815", "Bug", "Collection lockers show the wrong slot after the clocks change", "Click & Collect", "Squad Route", "Ethan Cole", "Medium", "Done",
    "After the clocks went back, locker collection times were shown one hour out.",
    [["2026-10-26T09:00", 56, "Ethan Cole", "Clocks went back on 25 Oct; 1,100 orders showed the wrong slot."],
     ["2026-10-30T09:00", 33, "Ethan Cole", "Hotfix deployed."],
     ["2026-11-05T09:00", null, "Ethan Cole", "Resolved."]]],
];

export const TICKETS: Ticket[] = RAW.map(([key, type, summary, component, team, assignee, jiraPriority, status, description, history]) => ({
  key,
  project: key.split("-")[0],
  type,
  summary,
  description,
  component,
  team,
  assignee,
  jiraPriority,
  status,
  history: history.map(([at, score, by, note]) => ({ at, score, by, note })),
}));

/** The weekly runs (Mondays 07:00) and the daily runs (weekday mornings) the agent has made. */
export const WEEKLY_RUNS = ["2026-09-21", "2026-09-28", "2026-10-05", "2026-10-12", "2026-10-19", "2026-10-26", "2026-11-02", "2026-11-09"];
export const DAILY_RUNS = ["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-09", "2026-11-10"];
export const RUN_TIME = "07:00";
